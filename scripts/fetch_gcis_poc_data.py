#!/usr/bin/env python3
"""Fetch real company registration records from Taiwan MOEA GCIS Open Data API,
merge live TWSE (上市) & TPEx (上櫃) stock quotes and fundamentals by 8-digit BAN,
validate coordinates against TAIWAN_GEOJSON county polygons, classify by
industry/capital/age/cluster, and export structured JSON for the 3D PoC.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import random
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

WORKSPACE = Path("/Users/chenchunchih/Desktop/數據視覺化")
GEO_MAPS_JS = WORKSPACE / "js" / "universal_geo_maps.js"
OUTPUT_JSON = WORKSPACE / "sample_data" / "gcis_industry_poc_data.json"

# Verified GCIS Open Data API endpoints
OID_CAPITAL = "F0E8FB8D-E2FD-472E-886C-91C673641F31"
OID_KEYWORD = "6BBA2268-1367-4B42-9CCA-BC17499EBE8C"
OID_BAN = "5F64D864-61CB-4D0D-8AD9-492047CC1EA6"

# Live TWSE & TPEx OpenAPI endpoints
TWSE_INFO_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TWSE_DAY_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TWSE_VAL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"

TPEX_INFO_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TPEX_DAY_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
TPEX_VAL_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"

SSL_CTX = ssl._create_unverified_context()

# Representative Taiwan enterprises to include as curated anchor queries
ANCHOR_BANS = [
    "22099131",  # 台灣積體電路製造
    "84149961",  # 聯發科技
    "04541302",  # 鴻海精密工業
    "22822281",  # 廣達電腦
    "34051920",  # 台達電子工業
    "23638777",  # 華碩電腦
    "47217677",  # 聯華電子
    "84149738",  # 友達光電
    "22099548",  # 旺宏電子
    "30414175",  # 中國鋼鐵
    "73251209",  # 統一企業
    "20828393",  # 宏碁
    "86517510",  # 玉山商業銀行
    "03750168",  # 台北富邦商業銀行
    "22178368",  # 微星科技
    "27719048",  # 永豐餘工業用紙
    "28412550",  # 台灣世曦工程顧問
    "11880517",  # 葡萄王生技
]

KEYWORDS = [
    "大立光", "上銀科技", "日月光", "奇美實業", "漢翔航空", "長榮海運",
    "國泰金融", "富邦金融", "中華電信", "台灣大哥大", "智邦科技", "緯創資通",
    "緯穎科技", "信驊科技", "瑞昱半導體", "聯詠科技", "巨大機械", "研華",
    "和碩", "光寶", "半導體", "光電", "積體電路", "精密", "自動化", "AI",
    "人工智慧", "軟體", "資訊", "雲端", "數位", "科技", "生技", "醫藥",
    "綠能", "太陽能", "能源", "風電", "環保", "儲能", "潔淨", "永續",
    "金控", "創投", "證券", "航運", "物流", "工具機", "機械", "建設",
    "營造", "餐飲", "觀光", "海洋", "文創", "食品",
]

COUNTY_CENTROIDS: dict[str, tuple[float, float]] = {
    "臺北市": (121.545, 25.052),
    "新北市": (121.485, 25.012),
    "基隆市": (121.741, 25.128),
    "桃園市": (121.298, 24.989),
    "新竹市": (120.968, 24.804),
    "新竹縣": (121.028, 24.828),
    "苗栗縣": (120.822, 24.565),
    "臺中市": (120.665, 24.165),
    "彰化縣": (120.515, 24.051),
    "南投縣": (120.685, 23.915),
    "雲林縣": (120.431, 23.708),
    "嘉義市": (120.449, 23.480),
    "嘉義縣": (120.335, 23.458),
    "臺南市": (120.228, 23.018),
    "高雄市": (120.312, 22.632),
    "屏東縣": (120.488, 22.668),
    "宜蘭縣": (121.753, 24.752),
    "花蓮縣": (121.601, 23.987),
    "臺東縣": (121.143, 22.758),
    "澎湖縣": (119.579, 23.568),
    "金門縣": (118.318, 24.436),
    "連江縣": (119.951, 26.160),
}

DISTRICT_ANCHORS: dict[str, tuple[float, float, float]] = {
    "內湖區": (121.576, 25.078, 0.014),
    "南港區": (121.608, 25.056, 0.013),
    "松山區": (121.558, 25.054, 0.011),
    "汐止區": (121.648, 25.066, 0.015),
    "信義區": (121.567, 25.033, 0.011),
    "大安區": (121.543, 25.026, 0.011),
    "中山區": (121.533, 25.064, 0.012),
    "中正區": (121.519, 25.039, 0.010),
    "大同區": (121.513, 25.063, 0.009),
    "萬華區": (121.499, 25.033, 0.009),
    "士林區": (121.525, 25.093, 0.014),
    "北投區": (121.503, 25.132, 0.015),
    "文山區": (121.558, 24.992, 0.012),
    "板橋區": (121.462, 25.012, 0.013),
    "中和區": (121.495, 24.998, 0.013),
    "永和區": (121.516, 25.008, 0.009),
    "新店區": (121.540, 24.972, 0.016),
    "三重區": (121.486, 25.063, 0.012),
    "新莊區": (121.445, 25.038, 0.014),
    "五股區": (121.438, 25.083, 0.014),
    "土城區": (121.443, 24.972, 0.013),
    "樹林區": (121.421, 24.991, 0.014),
    "林口區": (121.375, 25.077, 0.015),
    "淡水區": (121.448, 25.172, 0.015),
    "蘆洲區": (121.474, 25.085, 0.010),
    "龜山區": (121.348, 25.025, 0.018),
    "桃園區": (121.301, 24.993, 0.015),
    "中壢區": (121.225, 24.962, 0.018),
    "蘆竹區": (121.288, 25.052, 0.016),
    "大園區": (121.202, 25.065, 0.018),
    "平鎮區": (121.215, 24.929, 0.016),
    "楊梅區": (121.146, 24.910, 0.018),
    "八德區": (121.286, 24.938, 0.014),
    "新竹科學園區": (121.007, 24.777, 0.015),
    "竹北市": (121.015, 24.835, 0.016),
    "竹東鎮": (121.088, 24.737, 0.016),
    "湖口鄉": (121.044, 24.902, 0.018),
    "新豐鄉": (120.985, 24.899, 0.015),
    "竹南鎮": (120.878, 24.692, 0.016),
    "頭份市": (120.909, 24.688, 0.016),
    "西屯區": (120.635, 24.178, 0.018),
    "南屯區": (120.625, 24.142, 0.016),
    "北屯區": (120.698, 24.182, 0.018),
    "大雅區": (120.646, 24.229, 0.016),
    "大里區": (120.678, 24.099, 0.015),
    "太平區": (120.718, 24.126, 0.016),
    "豐原區": (120.718, 24.252, 0.016),
    "潭子區": (120.705, 24.209, 0.014),
    "神岡區": (120.661, 24.257, 0.015),
    "沙鹿區": (120.566, 24.234, 0.016),
    "彰化市": (120.542, 24.072, 0.018),
    "員林市": (120.574, 23.959, 0.016),
    "新市區": (120.285, 23.095, 0.015),
    "善化區": (120.292, 23.128, 0.016),
    "永康區": (120.256, 23.028, 0.015),
    "安南區": (120.184, 23.047, 0.018),
    "仁德區": (120.252, 22.968, 0.015),
    "歸仁區": (120.293, 22.967, 0.016),
    "安平區": (120.165, 22.999, 0.012),
    "前鎮區": (120.313, 22.596, 0.014),
    "苓雅區": (120.312, 22.622, 0.012),
    "鼓山區": (120.281, 22.655, 0.013),
    "左營區": (120.298, 22.688, 0.014),
    "三民區": (120.321, 22.650, 0.014),
    "楠梓區": (120.298, 22.728, 0.016),
    "小港區": (120.355, 22.562, 0.018),
    "仁武區": (120.348, 22.701, 0.016),
    "岡山區": (120.295, 22.796, 0.018),
    "路竹區": (120.262, 22.856, 0.018),
    "大寮區": (120.402, 22.605, 0.018),
    "鳳山區": (120.356, 22.626, 0.015),
    "宜蘭市": (121.753, 24.752, 0.022),
    "羅東鎮": (121.767, 24.677, 0.018),
    "蘇澳鎮": (121.842, 24.594, 0.018),
    "花蓮市": (121.606, 23.987, 0.022),
    "吉安鄉": (121.564, 23.968, 0.020),
    "臺東市": (121.143, 22.758, 0.022),
}

# English address patterns for TPEx OTC companies
ENG_COUNTY_PATTERNS = [
    (r"NEW\s*TAI\w*", "新北市"),
    (r"SI\s*JHIH|XIZHI|ZHONGHE|CHUNG\s*-?\s*HO|YONGHE|BANQIAO|PANCHIAO|XINDIAN|HSIN\s*-?\s*TIEN|TUCHENG|SHULIN|SANCHONG|XINZHUANG|WUGU|LINKOU", "新北市"),
    (r"TAI\s*PEI|NEI\s*-?\s*HU|NANGANG|XINYI|DAAN|ZHONGSHAN|ZHONGZHENG|SONGSHAN|SHILIN|BEITOU", "臺北市"),
    (r"KEELUNG", "基隆市"),
    (r"TAO\s*-?\s*YUAN|CHUNG\s*-?\s*LI|ZHONGLI|YANG\s*MEI|KUEI\s*SHAN|GUISHAN|PING\s*-?\s*CHEN|LUZHU|DAYUAN", "桃園市"),
    (r"(?:ZHU|CHU)\s*-?\s*BEI|(?:ZHU|CHU)\s*-?\s*PEI|HU\s*-?\s*KOU|HSIN\s*-?\s*CHI?U\s+(?:COUNTY|HSIEN)", "新竹縣"),
    (r"HSIN\s*-?\s*CHU|SCIENCE[- ]BASED\s+INDUSTRIAL\s+PARK", "新竹市"),
    (r"MIAO\s*-?\s*LI|CHU\s*-?\s*NAN|ZHUNAN", "苗栗縣"),
    (r"TAI\s*-?\s*CHUNG|XITUN|NANTUN|BEITUN|DAYA|DALI|TANZI|FENGYUAN|SHENKANG", "臺中市"),
    (r"CHANG\s*-?\s*HUA|LUGANG|YUANLIN", "彰化縣"),
    (r"NAN\s*-?\s*TOU", "南投縣"),
    (r"YUN\s*-?\s*LIN|DOU\s*-?\s*L[EI]U", "雲林縣"),
    (r"CHIA\s*-?\s*YI.*(?:COUNTY|HSIEN|MINXIONG)|MINXIONG", "嘉義縣"),
    (r"CHIA\s*-?\s*YI", "嘉義市"),
    (r"TAI\s*-?\s*NAN|\bTAIN\b|XINSHI|SHANHUA|YONGKANG|ANNAN|RENDE|GUIREN", "臺南市"),
    (r"K[AO][OA]SH?IUNG|KAO\s*HSIUNG|QIANZHEN|NANZIH|NANZI|GUSHAN|LINGYA|SANMIN|ZUOYING|XIAOGANG|RENWU|LUZHU.*KAOH", "高雄市"),
    (r"PING\s*-?\s*TUNG", "屏東縣"),
    (r"Y?I\s*-?\s*LAN", "宜蘭縣"),
    (r"HUA\s*-?\s*LIEN", "花蓮縣"),
    (r"TAI\s*-?\s*TUNG", "臺東縣"),
    (r"PENG\s*-?\s*HU", "澎湖縣"),
    (r"KIN\s*-?\s*MEN", "金門縣"),
]

ENG_DISTRICT_PATTERNS = [
    (r"NEI\s*-?\s*HU", "內湖區"),
    (r"NAN\s*-?\s*GANG", "南港區"),
    (r"SONG\s*-?\s*SHAN", "松山區"),
    (r"SI\s*JHIH|XIZHI", "汐止區"),
    (r"XIN\s*-?\s*YI|HSIN\s*-?\s*YI", "信義區"),
    (r"DA\s*-?\s*AN|TA\s*-?\s*AN", "大安區"),
    (r"ZHONG\s*-?\s*SHAN|CHUNG\s*-?\s*SHAN", "中山區"),
    (r"ZHONG\s*-?\s*ZHENG|CHUNG\s*-?\s*CHENG", "中正區"),
    (r"SHI\s*-?\s*LIN|SHIH\s*-?\s*LIN", "士林區"),
    (r"BEI\s*-?\s*TOU|PEI\s*-?\s*TOU", "北投區"),
    (r"BAN\s*-?\s*QIAO|PAN\s*-?\s*CHIAO", "板橋區"),
    (r"ZHONG\s*-?\s*HE|CHUNG\s*-?\s*HO", "中和區"),
    (r"XIN\s*-?\s*DIAN|HSIN\s*-?\s*TIEN", "新店區"),
    (r"SAN\s*-?\s*CHONG|SAN\s*-?\s*CHUNG", "三重區"),
    (r"XIN\s*-?\s*ZHUANG|HSIN\s*-?\s*CHUANG", "新莊區"),
    (r"WU\s*-?\s*GU|WU\s*-?\s*KU", "五股區"),
    (r"TU\s*-?\s*CHENG", "土城區"),
    (r"SHU\s*-?\s*LIN", "樹林區"),
    (r"GUI\s*-?\s*SHAN|KUEI\s*-?\s*SHAN", "龜山區"),
    (r"CHUNG\s*-?\s*LI|ZHONG\s*-?\s*LI", "中壢區"),
    (r"YANG\s*-?\s*MEI", "楊梅區"),
    (r"PING\s*-?\s*ZHEN|PING\s*-?\s*CHEN", "平鎮區"),
    (r"SCIENCE[- ]BASED\s+INDUSTRIAL\s+PARK|HSINCHU\s+SCIENCE\s+PARK", "新竹科學園區"),
    (r"(?:ZHU|CHU)\s*-?\s*(?:BEI|PEI)", "竹北市"),
    (r"HU\s*-?\s*KOU", "湖口鄉"),
    (r"(?:ZHU|CHU)\s*-?\s*NAN", "竹南鎮"),
    (r"XI\s*-?\s*TUN|HSI\s*-?\s*TUN", "西屯區"),
    (r"NAN\s*-?\s*TUN", "南屯區"),
    (r"DA\s*-?\s*YA", "大雅區"),
    (r"TAN\s*-?\s*ZI", "潭子區"),
    (r"XIN\s*-?\s*SHI", "新市區"),
    (r"SHAN\s*-?\s*HUA", "善化區"),
    (r"YONG\s*-?\s*KANG|YUNG\s*-?\s*KANG", "永康區"),
    (r"AN\s*-?\s*NAN", "安南區"),
    (r"QIAN\s*-?\s*ZHEN|CHIEN\s*-?\s*CHEN", "前鎮區"),
    (r"NAN\s*-?\s*ZI|NAN\s*-?\s*TZU", "楠梓區"),
    (r"GU\s*-?\s*SHAN|KU\s*-?\s*SHAN", "鼓山區"),
    (r"ZUO\s*-?\s*YING|TSO\s*-?\s*YING", "左營區"),
]

# Official TWSE/TPEx industry code mapping into 8 PoC sectors
TWSE_INDUSTRY_MAP = {
    "24": "半導體與電子製造",  # 半導體業
    "25": "半導體與電子製造",  # 電腦及週邊設備業
    "26": "半導體與電子製造",  # 光電業
    "27": "半導體與電子製造",  # 通信網路業
    "28": "半導體與電子製造",  # 電子零組件業
    "29": "批發零售與國際貿易",  # 電子通路業
    "30": "AI與軟體資訊服務",   # 資訊服務業
    "31": "半導體與電子製造",  # 其他電子業
    "36": "AI與軟體資訊服務",   # 數位雲端
    "22": "生技醫療與綠能永續",  # 生技醫療業
    "35": "生技醫療與綠能永續",  # 綠能環保
    "17": "金融控股與投資",     # 金融保險業
    "14": "營造工程與不動產",    # 建材營造
    "16": "餐飲旅宿與生活文創",  # 觀光餐旅
    "04": "餐飲旅宿與生活文創",  # 食品工業
    "32": "餐飲旅宿與生活文創",  # 文化創意業
    "37": "餐飲旅宿與生活文創",  # 運動休閒
    "15": "批發零售與國際貿易",  # 航運業
    "18": "批發零售與國際貿易",  # 貿易百貨
    "38": "批發零售與國際貿易",  # 居家生活
    "01": "精密機械與傳統製造",  # 水泥工業
    "02": "餐飲旅宿與生活文創",  # 食品
    "03": "精密機械與傳統製造",  # 塑膠工業
    "05": "精密機械與傳統製造",  # 電機機械
    "06": "精密機械與傳統製造",  # 電器電纜
    "08": "精密機械與傳統製造",  # 玻璃陶瓷
    "09": "精密機械與傳統製造",  # 造紙工業
    "10": "精密機械與傳統製造",  # 鋼鐵工業
    "11": "精密機械與傳統製造",  # 橡膠工業
    "12": "精密機械與傳統製造",  # 汽車工業
    "19": "精密機械與傳統製造",  # 綜合
    "20": "精密機械與傳統製造",  # 其他
    "21": "精密機械與傳統製造",  # 化學工業
    "23": "精密機械與傳統製造",  # 油電燃氣業
    "33": "精密機械與傳統製造",  # 農林漁牧
    "34": "生技醫療與綠能永續",  # 環保綠能
}

CLUSTERS: list[dict[str, Any]] = [
    {
        "id": "all",
        "name": "全臺產業聚落總覽",
        "short": "全臺總覽",
        "en": "ALL TAIWAN",
        "center": [120.92, 23.75],
        "cam": {"yaw": -0.35, "pitch": 0.62, "zoom": 1.0},
        "tagline": "經濟部 GCIS 商工登記 × 證交所 TWSE & 櫃買中心 TPEx 台股即時行情",
        "narrative": "串聯經濟部實測商工登記與全臺上市櫃即時交易行情，呈現北部「金融與IC設計研發」、竹苗「半導體護國神山群」、中部「精密機械與光學」、南科「先進製程與綠能」至高雄「亞灣智慧港灣與封測S廊帶」的立體產業資本地圖。",
        "highlights": ["GCIS×TWSE雙引擎", "上市櫃即時股價", "竹科半導體心臟", "AI 3D對話導覽", "8大聚落座標"],
    },
    {
        "id": "taipei_tech",
        "name": "臺北內湖・南港軟體走廊",
        "short": "內湖・南港",
        "en": "NEIHU · NANGANG TECH",
        "center": [121.595, 25.068],
        "cam": {"yaw": -0.45, "pitch": 0.78, "zoom": 5.8},
        "tagline": "數位軟體、IC設計研發總部與生技新創密集走廊",
        "narrative": "串聯內湖科技園區、南港軟體工業園區與汐止經貿廊帶，聚集全臺最高密度的資通訊企業總部、軟體雲端服務商、電子通路與生技醫療研發中心。",
        "highlights": ["企業營運總部密集", "軟體與AI雲端重鎮", "南港生技廊帶", "上市櫃科技密集"],
    },
    {
        "id": "taipei_finance",
        "name": "臺北信義・中山金控商貿區",
        "short": "信義・中山金控",
        "en": "XINYI · CBD FINANCE",
        "center": [121.540, 25.042],
        "cam": {"yaw": 0.25, "pitch": 0.80, "zoom": 6.4},
        "tagline": "全臺資本運籌核心：金控總部、電信龍頭與跨國商貿",
        "narrative": "涵蓋信義計畫區、大安與中山松江金融軸線，實收資本額金字塔頂端（PR ≥ 90）密度全臺第一，聚集大型金控、電信、海運與跨國商貿決策總部。",
        "highlights": ["億級金控資本核心", "高殖利率權值股", "高資本PR90+集中區", "商貿與金融樞紐"],
    },
    {
        "id": "new_taipei_taoyuan",
        "name": "新北・桃園智慧製造與AI供應鏈",
        "short": "新北・桃園製造",
        "en": "NEW TAIPEI · TAOYUAN MFG",
        "center": [121.345, 25.015],
        "cam": {"yaw": -0.55, "pitch": 0.72, "zoom": 3.4},
        "tagline": "全球 AI 伺服器組裝、電源管理、PCB 與航空物流動脈",
        "narrative": "從中和、土城、五股、新店延伸至桃園龜山華亞科、中壢與大園航空城，匯聚鴻海、廣達、台達電、華碩與龐大 AI 伺服器、印刷電路板及國際海空物流供應鏈。",
        "highlights": ["AI 伺服器製造重鎮", "雙北桃園科技廊帶", "國際機場物流樞紐", "高產值上市製造群"],
    },
    {
        "id": "hsinchu_park",
        "name": "新竹竹科・竹北半導體心臟",
        "short": "新竹・竹科",
        "en": "HSINCHU SCIENCE PARK",
        "center": [121.005, 24.805],
        "cam": {"yaw": -0.60, "pitch": 0.76, "zoom": 5.2},
        "tagline": "全球半導體晶圓製造、IC設計與設備材料最高密度核心",
        "narrative": "以新竹科學園區為核心，向外擴散至竹北生醫與竹南先進封測基地。囊括台積電、聯發科、聯電、智邦、聯詠等高市值權值與千金股群聚。",
        "highlights": ["護國神山核心群聚", "IC設計千金股聚落", "資本PR中位數領先", "高度技術密集聚落"],
    },
    {
        "id": "taichung_precision",
        "name": "臺中中科・西屯精密機械走廊",
        "short": "臺中・中科精密",
        "en": "TAICHUNG PRECISION CORRIDOR",
        "center": [120.645, 24.165],
        "cam": {"yaw": -0.42, "pitch": 0.72, "zoom": 3.5},
        "tagline": "精密機械、光學鏡頭、航太與中臺灣製造冠軍",
        "narrative": "結合西屯七期商辦、大肚山精密機械黃金縱谷與中部科學園區，孕育大立光、上銀、漢翔與龐大的工具機、自行車與車用零組件上市櫃隱形冠軍。",
        "highlights": ["大肚山黃金縱谷", "精密工具機與光學", "中台灣企業總部群", "老牌隱形冠軍密集"],
    },
    {
        "id": "tainan_stsp",
        "name": "臺南南科・沙崙先進製程走廊",
        "short": "臺南・南科綠能",
        "en": "TAINAN STSP · GREEN TECH",
        "center": [120.265, 23.065],
        "cam": {"yaw": 0.35, "pitch": 0.72, "zoom": 3.8},
        "tagline": "先進半導體擴廠重鎮 × 食品化工與沙崙綠能城",
        "narrative": "以南部科學園區（新市、善化）為先進製程與光電設備核心，結合臺南永康、安平本土食品（統一）、特化材料與汽車零組件上市龍頭，新舊引擎並行。",
        "highlights": ["先進製程產值核心", "綠能光電與設備業", "南臺灣食品化工龍頭", "嘉南高成長聚落"],
    },
    {
        "id": "kaohsiung_bay",
        "name": "高雄亞灣・楠梓半導體S廊帶",
        "short": "高雄・亞灣S廊帶",
        "en": "KAOHSIUNG BAY · S-CORRIDOR",
        "center": [120.312, 22.645],
        "cam": {"yaw": -0.50, "pitch": 0.74, "zoom": 4.2},
        "tagline": "亞灣 5G AIoT 創新特區 × 楠梓封測與重工港灣",
        "narrative": "北接橋頭與楠梓半導體S廊帶（全球封測龍頭日月光等），南連亞洲新灣區 5G AIoT 特區及高雄港鋼鐵、石化與航運龍頭（中鋼等）。",
        "highlights": ["半導體南部S廊帶", "亞灣 5G AIoT 特區", "智慧港灣與重工", "大型企業資本升級"],
    },
    {
        "id": "east_offshore",
        "name": "宜花東・永續觀光與食農生技",
        "short": "宜花東・農創觀光",
        "en": "EAST TAIWAN · SUSTAINABLE",
        "center": [121.520, 23.850],
        "cam": {"yaw": -0.75, "pitch": 0.65, "zoom": 2.2},
        "tagline": "低密度高特色：太平洋觀光旅宿、食農創新與深層水生技",
        "narrative": "跨越蘭陽平原、花蓮縱谷至臺東海岸及離島，涵蓋東部綠色產業、觀光旅宿、石材綠建材、在地食農文創與生技代表企業。",
        "highlights": ["生態永續旅宿業", "在地食農與文創", "東部綠色製造", "生活風格創業聚落"],
    },
]

INDUSTRIES = [
    "半導體與電子製造",
    "AI與軟體資訊服務",
    "精密機械與傳統製造",
    "金融控股與投資",
    "批發零售與國際貿易",
    "生技醫療與綠能永續",
    "營造工程與不動產",
    "餐飲旅宿與生活文創",
]


def load_taiwan_geojson() -> dict[str, Any]:
    text = GEO_MAPS_JS.read_text(encoding="utf-8")
    m = re.search(r"const TAIWAN_GEOJSON\s*=\s*(\{.*?\});", text, re.S)
    if not m:
        raise RuntimeError("Could not find TAIWAN_GEOJSON in universal_geo_maps.js")
    return json.loads(m.group(1))


def point_in_ring(x: float, y: float, ring: list[list[float]]) -> bool:
    inside = False
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % n]
        if (y1 > y) != (y2 > y):
            x_intersect = (x2 - x1) * (y - y1) / (y2 - y1 + 1e-12) + x1
            if x < x_intersect:
                inside = not inside
    return inside


def point_in_feature(lon: float, lat: float, feat: dict[str, Any]) -> bool:
    geom = feat["geometry"]
    gtype = geom["type"]
    coords = geom["coordinates"]
    polys = [coords] if gtype == "Polygon" else coords
    for poly in polys:
        if poly and point_in_ring(lon, lat, poly[0]):
            in_hole = any(point_in_ring(lon, lat, hole) for hole in poly[1:])
            if not in_hole:
                return True
    return False


def build_county_poly_index(geojson: dict[str, Any]) -> dict[str, dict[str, Any]]:
    idx = {}
    for feat in geojson["features"]:
        name = feat["properties"].get("COUNTYNAME") or feat["properties"].get("name")
        if name:
            norm = name.replace("台", "臺")
            idx[norm] = feat
            idx[name] = feat
    return idx


def fetch_json(url: str, retries: int = 2, timeout: int = 16) -> list[dict[str, Any]]:
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace").strip()
                if not raw:
                    return []
                data = json.loads(raw)
                return data if isinstance(data, list) else []
        except Exception:
            if attempt < retries:
                time.sleep(0.4 * (attempt + 1))
    return []


def collect_live_gcis_records() -> tuple[dict[str, dict[str, Any]], set[str]]:
    tasks: list[tuple[str, str, str]] = []

    for ban in ANCHOR_BANS:
        flt = urllib.parse.quote(f"Business_Accounting_NO eq {ban}")
        url = f"https://data.gcis.nat.gov.tw/od/data/api/{OID_BAN}?$format=json&$filter={flt}"
        tasks.append(("ban", ban, url))

    bracket_skips = {
        "G": [0, 300, 900, 1800, 3000, 4800, 7200],
        "F": [0, 500, 1500, 3500, 6500],
        "E": [0, 800, 2200, 5000],
        "D": [0, 1000, 3000],
        "C": [0, 1200],
    }
    for code, skips in bracket_skips.items():
        for skip in skips:
            flt = urllib.parse.quote(f"Company_Status eq 01 and Capital_Stock_Amount eq {code}")
            url = (
                f"https://data.gcis.nat.gov.tw/od/data/api/{OID_CAPITAL}"
                f"?$format=json&$filter={flt}&$skip={skip}&$top=140"
            )
            tasks.append(("capital", f"{code}@{skip}", url))

    for kw in KEYWORDS:
        flt = urllib.parse.quote(f"Company_Name like {kw} and Company_Status eq 01")
        url = f"https://data.gcis.nat.gov.tw/od/data/api/{OID_KEYWORD}?$format=json&$filter={flt}&$skip=0&$top=80"
        tasks.append(("keyword", kw, url))

    records: dict[str, dict[str, Any]] = {}
    flagship_bans_verified: set[str] = set()
    anchor_kw_set = set(KEYWORDS[:20])
    print(f"[GCIS] Dispatching {len(tasks)} live API queries...")

    with ThreadPoolExecutor(max_workers=6) as pool:
        future_map = {pool.submit(fetch_json, url): (kind, tag) for kind, tag, url in tasks}
        done_count = 0
        for fut in as_completed(future_map):
            kind, tag = future_map[fut]
            done_count += 1
            rows = fut.result()
            for r in rows:
                ban = str(r.get("Business_Accounting_NO") or "").strip()
                if not ban:
                    continue
                if r.get("Company_Status") and str(r.get("Company_Status")) != "01":
                    continue
                cap = int(r.get("Paid_In_Capital_Amount") or r.get("Capital_Stock_Amount") or 0)
                if kind == "ban" or (kind == "keyword" and tag in anchor_kw_set and cap >= 500_000_000):
                    flagship_bans_verified.add(ban)
                if ban not in records or (kind == "ban"):
                    records[ban] = r
            if done_count % 15 == 0 or done_count == len(tasks):
                print(f"[GCIS] Progress {done_count}/{len(tasks)} -> {len(records)} unique active companies")

    return records, flagship_bans_verified


def load_or_collect_gcis_records(force_refresh: bool = False) -> tuple[dict[str, dict[str, Any]], set[str]]:
    if OUTPUT_JSON.exists() and not force_refresh:
        try:
            existing = json.loads(OUTPUT_JSON.read_text(encoding="utf-8"))
            comps = existing.get("companies") or []
            if len(comps) >= 3000:
                print(f"[GCIS] Reusing {len(comps)} verified base GCIS records from {OUTPUT_JSON.name} (pass --refresh-gcis to re-crawl GCIS)")
                records: dict[str, dict[str, Any]] = {}
                verified_flagships: set[str] = set(ANCHOR_BANS)
                for c in comps:
                    ban = str(c.get("ban") or "").strip()
                    if not ban:
                        continue
                    records[ban] = {
                        "Business_Accounting_NO": ban,
                        "Company_Name": c.get("name", ""),
                        "Responsible_Name": c.get("responsible", "—"),
                        "Paid_In_Capital_Amount": c.get("capital", 0),
                        "Capital_Stock_Amount": c.get("capital", 0),
                        "Company_Setup_Date": c.get("setup_date", ""),
                        "Company_Location": c.get("address", ""),
                        "Register_Organization_Desc": c.get("reg_org", ""),
                        "Company_Status": "01",
                    }
                    if c.get("flagship"):
                        verified_flagships.add(ban)
                return records, verified_flagships
        except Exception as exc:
            print(f"[GCIS] Cache read failed ({exc}), fetching live GCIS...")
    return collect_live_gcis_records()


def safe_float(val: Any) -> float | None:
    if val is None:
        return None
    s = str(val).strip().replace(",", "").replace("+", "")
    if not s or s in ("-", "--", "---", "----", "N/A", "NaN", "－"):
        return None
    if s.startswith("X"):
        s = s[1:].strip()
    try:
        v = float(s)
        return round(v, 2)
    except ValueError:
        return None


def safe_int(val: Any) -> int:
    if val is None:
        return 0
    s = re.sub(r"[^\d]", "", str(val))
    return int(s) if s else 0


def format_roc_yyyymmdd(raw_date: str) -> str:
    d = re.sub(r"\D", "", str(raw_date or ""))
    if len(d) == 7:
        y = 1911 + int(d[:3])
        return f"{y}-{d[3:5]}-{d[5:7]}"
    if len(d) == 8:
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return raw_date or ""


def fetch_twse_tpex_stocks() -> tuple[dict[str, dict[str, Any]], str]:
    """Fetch live TWSE (Listed) + TPEx (OTC) company info, daily quotes, and valuation metrics."""
    print("[TWSE/TPEx] Fetching live stock basic info, closing prices & valuation OpenAPI...")
    urls = {
        "twse_info": TWSE_INFO_URL,
        "twse_day": TWSE_DAY_URL,
        "twse_val": TWSE_VAL_URL,
        "tpex_info": TPEX_INFO_URL,
        "tpex_day": TPEX_DAY_URL,
        "tpex_val": TPEX_VAL_URL,
    }
    raw: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {pool.submit(fetch_json, u, 2, 18): k for k, u in urls.items()}
        for fut in as_completed(futs):
            k = futs[fut]
            raw[k] = fut.result()
            print(f"[TWSE/TPEx]   {k:10s}: {len(raw[k]):5d} rows")

    # Index daily quotes & valuation by ticker Code
    twse_quotes = {str(r.get("Code") or "").strip(): r for r in raw.get("twse_day", []) if r.get("Code")}
    twse_vals = {str(r.get("Code") or "").strip(): r for r in raw.get("twse_val", []) if r.get("Code")}

    tpex_quotes = {str(r.get("SecuritiesCompanyCode") or "").strip(): r for r in raw.get("tpex_day", []) if r.get("SecuritiesCompanyCode")}
    tpex_vals = {str(r.get("SecuritiesCompanyCode") or "").strip(): r for r in raw.get("tpex_val", []) if r.get("SecuritiesCompanyCode")}

    trade_date_str = ""
    if raw.get("twse_day"):
        trade_date_str = format_roc_yyyymmdd(str(raw["twse_day"][0].get("Date") or ""))
    elif raw.get("tpex_day"):
        trade_date_str = format_roc_yyyymmdd(str(raw["tpex_day"][0].get("Date") or ""))

    stocks_by_ban: dict[str, dict[str, Any]] = {}

    # 1. Process TWSE Listed (上市)
    for r in raw.get("twse_info", []):
        ban = re.sub(r"\D", "", str(r.get("營利事業統一編號") or ""))
        code = str(r.get("公司代號") or "").strip()
        if len(ban) != 8 or not code:
            continue
        q = twse_quotes.get(code, {})
        v = twse_vals.get(code, {})
        close_p = safe_float(q.get("ClosingPrice"))
        chg = safe_float(q.get("Change"))
        chg_pct = None
        if close_p is not None and chg is not None and (close_p - chg) > 0:
            chg_pct = round((chg / (close_p - chg)) * 100, 2)
        vol_shares = safe_int(q.get("TradeVolume"))

        stocks_by_ban[ban] = {
            "code": code,
            "abbr": str(r.get("公司簡稱") or "").strip(),
            "market": "上市",
            "ind_code": str(r.get("產業別") or "").strip().zfill(2),
            "close": close_p,
            "change": chg,
            "change_pct": chg_pct,
            "open": safe_float(q.get("OpeningPrice")),
            "high": safe_float(q.get("HighestPrice")),
            "low": safe_float(q.get("LowestPrice")),
            "vol_lots": vol_shares // 1000 if vol_shares > 0 else 0,
            "pe": safe_float(v.get("PEratio")),
            "yield_pct": safe_float(v.get("DividendYield")),
            "pb": safe_float(v.get("PBratio")),
            "date": format_roc_yyyymmdd(str(q.get("Date") or r.get("出表日期") or "")),
            # Fallback registration info
            "company_name": str(r.get("公司名稱") or "").strip(),
            "address": str(r.get("住址") or "").strip(),
            "responsible": str(r.get("董事長") or "—").strip(),
            "capital": safe_int(r.get("實收資本額")),
            "setup_date": str(r.get("成立日期") or "").strip(),
            "reg_org": "臺灣證券交易所（上市）",
        }

    # 2. Process TPEx OTC (上櫃)
    for r in raw.get("tpex_info", []):
        ban = re.sub(r"\D", "", str(r.get("UnifiedBusinessNo.") or ""))
        code = str(r.get("SecuritiesCompanyCode") or "").strip()
        if len(ban) != 8 or not code or ban in stocks_by_ban:
            continue
        q = tpex_quotes.get(code, {})
        v = tpex_vals.get(code, {})
        close_p = safe_float(q.get("Close"))
        chg = safe_float(q.get("Change"))
        chg_pct = None
        if close_p is not None and chg is not None and (close_p - chg) > 0:
            chg_pct = round((chg / (close_p - chg)) * 100, 2)
        vol_shares = safe_int(q.get("TradingShares"))

        stocks_by_ban[ban] = {
            "code": code,
            "abbr": str(r.get("CompanyAbbreviation") or "").strip(),
            "market": "上櫃",
            "ind_code": str(r.get("SecuritiesIndustryCode") or "").strip().zfill(2),
            "close": close_p,
            "change": chg,
            "change_pct": chg_pct,
            "open": safe_float(q.get("Open")),
            "high": safe_float(q.get("High")),
            "low": safe_float(q.get("Low")),
            "vol_lots": vol_shares // 1000 if vol_shares > 0 else 0,
            "pe": safe_float(v.get("PriceEarningRatio")),
            "yield_pct": safe_float(v.get("YieldRatio")),
            "pb": safe_float(v.get("PriceBookRatio")),
            "date": format_roc_yyyymmdd(str(q.get("Date") or r.get("Date") or "")),
            "company_name": str(r.get("CompanyName") or "").strip(),
            "address": str(r.get("Address") or "").strip(),
            "responsible": str(r.get("Chairman") or "—").strip(),
            "capital": safe_int(r.get("Paidin.Capital.NTDollars")),
            "setup_date": str(r.get("DateOfIncorporation") or "").strip(),
            "reg_org": "證券櫃檯買賣中心（上櫃）",
        }

    print(f"[TWSE/TPEx] Indexed {len(stocks_by_ban)} listed/OTC companies by 8-digit BAN (trade date: {trade_date_str})")
    return stocks_by_ban, trade_date_str


def parse_county_and_district(loc: str, reg_org: str) -> tuple[str, str]:
    loc_norm = (loc or "").replace("台", "臺").strip()
    reg_norm = (reg_org or "").replace("台", "臺").strip()
    loc_upper = (loc or "").upper()

    # Explicitly exclude Cayman Islands / foreign domiciles with no Taiwan address
    if any(k in loc_upper for k in ("CAYMAN", "SAMOA", "VIRGIN ISLANDS", "SINGAPORE", "RAYONG")) and not any(c in loc_norm for c in COUNTY_CENTROIDS):
        return "", ""

    county = ""
    for c in COUNTY_CENTROIDS:
        if c in loc_norm:
            county = c
            break
    if not county:
        if "新竹科學園區" in loc_norm or "新竹科學園區" in reg_norm:
            county = "新竹市"
        elif "中部科學園區" in loc_norm or "中部科學園區" in reg_norm:
            county = "臺中市"
        elif "南部科學園區" in loc_norm or "南部科學園區" in reg_norm:
            county = "臺南市"
        else:
            for c in COUNTY_CENTROIDS:
                if c[:2] in reg_norm:
                    county = c
                    break

    # Fallback: parse English romanized address (used by TPEx mopsfin_t187ap03_O)
    if not county and loc_upper:
        for pat, cn in ENG_COUNTY_PATTERNS:
            if re.search(pat, loc_upper):
                county = cn
                break

    if not county:
        return "", ""

    district = ""
    if "新竹科學園區" in loc_norm or "新竹科學園區" in reg_norm:
        district = "新竹科學園區"
    else:
        for d in DISTRICT_ANCHORS:
            if d in loc_norm:
                district = d
                break
        if not district:
            m = re.search(rf"{county[0]}.{{1,2}}[市縣](.{{1,3}}[區鄉鎮市])", loc_norm)
            if m:
                district = m.group(1)
        if not district and loc_upper:
            for pat, dname in ENG_DISTRICT_PATTERNS:
                if re.search(pat, loc_upper):
                    district = dname
                    break

    return county, district


def assign_cluster(county: str, district: str, loc: str) -> str:
    loc_u = (loc or "").upper()
    if county == "臺北市":
        if district in ("內湖區", "南港區", "松山區") or any(k in loc_u for k in ("內湖", "南港", "松山", "NEIHU", "NANGANG", "SONGSHAN")):
            return "taipei_tech"
        return "taipei_finance"
    if county == "新北市":
        if district == "汐止區" or "汐止" in loc_u or "SIJHIH" in loc_u or "XIZHI" in loc_u:
            return "taipei_tech"
        return "new_taipei_taoyuan"
    if county in ("桃園市", "基隆市"):
        return "new_taipei_taoyuan"
    if county in ("新竹市", "新竹縣", "苗栗縣"):
        return "hsinchu_park"
    if county in ("臺中市", "彰化縣", "南投縣"):
        return "taichung_precision"
    if county in ("臺南市", "嘉義市", "嘉義縣", "雲林縣"):
        return "tainan_stsp"
    if county in ("高雄市", "屏東縣"):
        return "kaohsiung_bay"
    return "east_offshore"


def classify_industry(name: str, reg_org: str = "", twse_ind_code: str = "") -> str:
    if twse_ind_code and twse_ind_code in TWSE_INDUSTRY_MAP:
        # Refine telecoms (中華電、台灣大、遠傳) & networking/cloud if needed
        if any(k in name for k in ("軟體", "資訊", "數位", "雲端", "AI", "人工智慧", "中華電信", "台灣大哥大", "遠傳")):
            return "AI與軟體資訊服務"
        return TWSE_INDUSTRY_MAP[twse_ind_code]

    if any(k in name for k in ("半導體", "積體電路", "晶圓", "光電", "光學", "電子", "面板", "封測", "元件", "電路板", "電腦", "聯電", "聯發", "宏碁", "和碩", "緯創", "緯穎", "台虹", "宏捷", "大立光", "科技", "通訊", "電通")):
        return "半導體與電子製造"
    if any(k in name for k in ("軟體", "資訊", "數位", "網路", "雲端", "AI", "人工智慧", "系統", "資通", "數據", "電信", "線上", "遊戲", "大哥大", "智邦")):
        return "AI與軟體資訊服務"
    if any(k in name for k in ("生技", "生物科技", "生醫", "醫藥", "醫療", "藥業", "製藥", "健康", "綠能", "太陽能", "能源", "風電", "環保", "儲能", "潔淨", "永續")):
        return "生技醫療與綠能永續"
    if any(k in name for k in ("金融", "金控", "銀行", "證券", "保險", "投資", "控股", "創投", "資產管理", "租賃", "票券", "期貨")):
        return "金融控股與投資"
    if any(k in name for k in ("機械", "精密", "工具機", "自動化", "五金", "鋼鐵", "金屬", "工業", "模具", "塑膠", "化學", "化工", "材料", "紡織", "電機", "航太", "漢翔", "奇美", "巨大", "造紙", "用紙", "水泥")):
        return "精密機械與傳統製造"
    if any(k in name for k in ("營造", "建設", "建築", "不動產", "地產", "工程顧問", "顧問", "開發", "機電")):
        return "營造工程與不動產"
    if any(k in name for k in ("餐飲", "食品", "咖啡", "烘焙", "觀光", "飯店", "旅宿", "旅館", "民宿", "文創", "娛樂", "休閒", "農", "茶", "統一企業")):
        return "餐飲旅宿與生活文創"
    if any(k in name for k in ("海運", "航運", "航空", "物流", "貨運", "倉儲", "通運", "運輸")):
        return "批發零售與國際貿易"
    if "科學園區" in reg_org:
        return "半導體與電子製造"
    return "批發零售與國際貿易"


def parse_setup_year(date_str: Any) -> int:
    s = str(date_str or "").strip()
    digits = re.sub(r"\D", "", s)
    if len(digits) == 8 and digits.startswith(("19", "20")):
        year = int(digits[:4])
        if 1945 <= year <= 2026:
            return year
    if len(digits) >= 6:
        roc_y = int(digits[:-4])
        year = 1911 + roc_y
        if 1945 <= year <= 2026:
            return year
    return 2015


def deterministic_coord(
    ban: str, county: str, district: str, county_polys: dict[str, dict[str, Any]]
) -> tuple[float, float]:
    h = hashlib.sha256(ban.encode("utf-8")).digest()
    rng = random.Random(int.from_bytes(h[:8], "big"))

    if district in DISTRICT_ANCHORS:
        base_lon, base_lat, spread = DISTRICT_ANCHORS[district]
    else:
        base_lon, base_lat = COUNTY_CENTROIDS.get(county, (120.95, 23.75))
        spread = 0.032

    feat = county_polys.get(county)
    for scale in (1.0, 0.65, 0.35, 0.15):
        for _ in range(18):
            lon = base_lon + rng.gauss(0, spread * scale)
            lat = base_lat + rng.gauss(0, spread * 0.85 * scale)
            if feat is None or point_in_feature(lon, lat, feat):
                return round(lon, 5), round(lat, 5)
    clon, clat = COUNTY_CENTROIDS.get(county, (base_lon, base_lat))
    return round(clon + rng.uniform(-0.004, 0.004), 5), round(clat + rng.uniform(-0.004, 0.004), 5)


def main() -> None:
    force_refresh = "--refresh-gcis" in sys.argv
    geojson = load_taiwan_geojson()
    county_polys = build_county_poly_index(geojson)
    raw_records, verified_flagships = load_or_collect_gcis_records(force_refresh=force_refresh)
    stocks_by_ban, twse_date = fetch_twse_tpex_stocks()

    # Merge TWSE/TPEx domestic companies that are not yet in GCIS sample
    added_from_twse = 0
    for ban, st in stocks_by_ban.items():
        if ban not in raw_records:
            county, _ = parse_county_and_district(st["address"], st["reg_org"])
            if not county:
                continue
            raw_records[ban] = {
                "Business_Accounting_NO": ban,
                "Company_Name": st["company_name"],
                "Responsible_Name": st["responsible"],
                "Paid_In_Capital_Amount": st["capital"],
                "Capital_Stock_Amount": st["capital"],
                "Company_Setup_Date": st["setup_date"],
                "Company_Location": st["address"],
                "Register_Organization_Desc": st["reg_org"],
                "Company_Status": "01",
            }
            added_from_twse += 1
        elif st["capital"] > 0 and int(raw_records[ban].get("Paid_In_Capital_Amount") or 0) <= 0:
            raw_records[ban]["Paid_In_Capital_Amount"] = st["capital"]
        # Prefer richer Chinese address if raw_records has empty/English address
        if ban in raw_records and (not raw_records[ban].get("Company_Location")) and st["address"]:
            raw_records[ban]["Company_Location"] = st["address"]

    print(f"[MERGE] Added {added_from_twse} additional domestic TWSE/TPEx listed/OTC companies (total pool: {len(raw_records)})")

    items: list[dict[str, Any]] = []
    current_year = 2026

    for ban, r in raw_records.items():
        name = str(r.get("Company_Name") or "").strip()
        loc = str(r.get("Company_Location") or "").strip()
        reg_org = str(r.get("Register_Organization_Desc") or "").strip()
        st = stocks_by_ban.get(ban)

        # If GCIS location is English or missing, try TWSE address too
        if not any("\u4e00" <= ch <= "\u9fff" for ch in loc) and st and st.get("address"):
            if any("\u4e00" <= ch <= "\u9fff" for ch in st["address"]):
                loc = st["address"]

        county, district = parse_county_and_district(loc, reg_org)
        if not name or not county:
            continue

        cap_stock = int(r.get("Capital_Stock_Amount") or 0)
        cap_paid = int(r.get("Paid_In_Capital_Amount") or 0)
        capital = cap_paid if cap_paid > 0 else cap_stock
        if st and st.get("capital", 0) > capital:
            capital = st["capital"]
        if capital <= 0:
            capital = 100000

        setup_year = parse_setup_year(r.get("Company_Setup_Date"))
        age = max(0, current_year - setup_year)

        if capital < 1_000_000:
            cap_tier = "< 100萬 微型新創"
            cap_tier_idx = 0
        elif capital < 10_000_000:
            cap_tier = "100萬–1,000萬 中小企業"
            cap_tier_idx = 1
        elif capital < 50_000_000:
            cap_tier = "1,000萬–5,000萬 成長中堅"
            cap_tier_idx = 2
        elif capital < 500_000_000:
            cap_tier = "5,000萬–5億 大型企業"
            cap_tier_idx = 3
        else:
            cap_tier = "≥ 5億 產業巨頭"
            cap_tier_idx = 4

        if age <= 5:
            age_tier = "新創動能 (≤5年)"
            age_tier_idx = 3
        elif age <= 15:
            age_tier = "成長擴張 (6–15年)"
            age_tier_idx = 2
        elif age <= 30:
            age_tier = "穩健中堅 (16–30年)"
            age_tier_idx = 1
        else:
            age_tier = "老牌傳承 (>30年)"
            age_tier_idx = 0

        twse_ind_code = st["ind_code"] if st else ""
        industry = classify_industry(name, reg_org, twse_ind_code)
        ind_idx = INDUSTRIES.index(industry)
        cluster = assign_cluster(county, district, loc)
        lon, lat = deterministic_coord(ban, county, district, county_polys)

        is_flagship = (
            (ban in verified_flagships)
            or (ban in set(ANCHOR_BANS))
            or (capital >= 10_000_000_000)
            or (bool(st and st.get("close") is not None and st["close"] >= 500))
        )

        if st and st.get("abbr"):
            short_name = st["abbr"]
        else:
            clean_short = re.sub(
                r"(股份有限公司|有限公司|商業銀行|金融控股|企業團)$", "", name
            ).strip()
            short_name = clean_short[:11] if clean_short else name[:11]

        compact_stock = None
        if st:
            compact_stock = {
                "code": st["code"],
                "abbr": st["abbr"],
                "market": st["market"],
                "close": st["close"],
                "change": st["change"],
                "change_pct": st["change_pct"],
                "open": st["open"],
                "high": st["high"],
                "low": st["low"],
                "vol_lots": st["vol_lots"],
                "pe": st["pe"],
                "yield_pct": st["yield_pct"],
                "pb": st["pb"],
                "date": st["date"],
            }

        items.append(
            {
                "ban": ban,
                "name": name,
                "short": short_name,
                "responsible": str(r.get("Responsible_Name") or "—").strip(),
                "capital": capital,
                "cap_tier": cap_tier,
                "cap_tier_i": cap_tier_idx,
                "setup_date": str(r.get("Company_Setup_Date") or "").strip(),
                "setup_year": setup_year,
                "age": age,
                "age_tier": age_tier,
                "age_tier_i": age_tier_idx,
                "county": county,
                "district": district or county,
                "address": loc,
                "reg_org": reg_org,
                "cluster": cluster,
                "industry": industry,
                "ind_i": ind_idx,
                "lon": lon,
                "lat": lat,
                "flagship": is_flagship,
                "stock": compact_stock,
            }
        )

    # Compute exact capital percentile PR (1..99)
    sorted_caps = sorted(x["capital"] for x in items)
    total_n = len(sorted_caps)
    for x in items:
        rank = bisect.bisect_right(sorted_caps, x["capital"])
        pr = min(99, max(1, int(round((rank / max(1, total_n)) * 99))))
        x["pr"] = pr

    # Sort flagships & listed giants first
    items.sort(
        key=lambda x: (
            1 if x["flagship"] else 0,
            1 if x.get("stock") and x["stock"].get("close") is not None else 0,
            x["capital"],
        ),
        reverse=True,
    )

    cluster_stats: dict[str, Any] = {}
    for c in CLUSTERS:
        cid = c["id"]
        sub = items if cid == "all" else [x for x in items if x["cluster"] == cid]
        prs = sorted(x["pr"] for x in sub)
        med_pr = prs[len(prs) // 2] if prs else 0
        billion_plus = sum(1 for x in sub if x["capital"] >= 100_000_000)
        recent_5y = sum(1 for x in sub if x["age"] <= 5)
        listed_cnt = sum(1 for x in sub if x.get("stock") is not None)
        ind_counts: dict[str, int] = {ind: 0 for ind in INDUSTRIES}
        for x in sub:
            ind_counts[x["industry"]] += 1
        top_inds = sorted(ind_counts.items(), key=lambda kv: kv[1], reverse=True)
        spotlights = [x["ban"] for x in sub[:6]]

        cluster_stats[cid] = {
            **c,
            "count": len(sub),
            "listed_count": listed_cnt,
            "median_pr": med_pr,
            "billion_plus": billion_plus,
            "startup_pct": round((recent_5y / max(1, len(sub))) * 100, 1),
            "total_capital_b": round(sum(x["capital"] for x in sub) / 1e8, 1),
            "top_industries": [
                {"name": k, "count": v, "pct": round((v / max(1, len(sub))) * 100, 1)}
                for k, v in top_inds
                if v > 0
            ],
            "spotlights": spotlights,
        }

    listed_total = sum(1 for x in items if x.get("stock") is not None)
    listed_twse = sum(1 for x in items if x.get("stock") and x["stock"]["market"] == "上市")
    listed_tpex = sum(1 for x in items if x.get("stock") and x["stock"]["market"] == "上櫃")

    payload = {
        "meta": {
            "source": "經濟部 GCIS 商工開放平臺 × 證交所 TWSE & 櫃買中心 TPEx OpenAPI",
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "twse_trade_date": twse_date,
            "total_records": len(items),
            "listed_count": listed_total,
            "twse_count": listed_twse,
            "tpex_count": listed_tpex,
            "counties_covered": len({x["county"] for x in items}),
            "clusters_count": len(CLUSTERS) - 1,
            "flagship_count": sum(1 for x in items if x["flagship"]),
        },
        "industries": INDUSTRIES,
        "clusters": [cluster_stats[c["id"]] for c in CLUSTERS],
        "companies": items,
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(
        f"[DONE] Saved {len(items)} companies ({listed_twse} 上市 + {listed_tpex} 上櫃 = {listed_total} 上市櫃) "
        f"across {payload['meta']['counties_covered']} counties -> {OUTPUT_JSON}"
    )
    for cid, st in cluster_stats.items():
        top_name = st["top_industries"][0]["name"] if st["top_industries"] else "-"
        print(
            f"  - {cid:18s}: {st['count']:4d} total ({st['listed_count']:4d} 上市櫃) "
            f"| median PR={st['median_pr']:2d} | top={top_name}"
        )


if __name__ == "__main__":
    main()
