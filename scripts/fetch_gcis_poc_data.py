#!/usr/bin/env python3
"""Fetch real company registration records from Taiwan MOEA GCIS Open Data API,
merge live TWSE (上市) & TPEx (上櫃) stock quotes and fundamentals by 8-digit BAN,
validate coordinates against TAIWAN_GEOJSON county polygons, classify by
industry/capital/age/cluster, and export structured JSON for the 3D PoC.
"""

from __future__ import annotations

import bisect
import csv
import hashlib
import io
import json
import random
import re
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

WORKSPACE = Path(__file__).resolve().parent.parent
GEO_MAPS_JS = WORKSPACE / "js" / "universal_geo_maps.js"
OUTPUT_JSON = WORKSPACE / "sample_data" / "gcis_industry_poc_data.json"
POC_HTML = WORKSPACE / "gcis_industry_3d_poc.html"
INDEX_HTML = WORKSPACE / "index.html"

# Verified GCIS Open Data API endpoints
OID_CAPITAL = "F0E8FB8D-E2FD-472E-886C-91C673641F31"
OID_KEYWORD = "6BBA2268-1367-4B42-9CCA-BC17499EBE8C"
OID_BAN = "5F64D864-61CB-4D0D-8AD9-492047CC1EA6"

# Official MOEA / GCIS bulk Open Data datasets for Central Taiwan manufacturing & active factories
OID_TAICHUNG_COMP_CMFG = "FC420109-8DF7-49F8-BD9F-BBB34289E7BD"
OID_CENTRAL_COMP_CMFG = "0D84DBC0-962E-48E7-9BE1-21549F0F38EB"
OID_TAICHUNG_BIZ_CMFG = "64F185DC-F414-4898-B3B2-7022B57A83E2"
IDA_FACTORY_INDEX_CSV = "https://www.ida.gov.tw/opendata/02/SDD6569.csv"
IDA_FACTORY_ZIP_FALLBACK = "https://serv.gcis.nat.gov.tw/RDownLoad/Data/statistical/%E7%94%9F%E7%94%A2%E4%B8%AD%E5%B7%A5%E5%BB%A0%E6%B8%85%E5%86%8A.zip"

CENTRAL_COUNTIES = {"臺中市", "彰化縣", "南投縣", "苗栗縣", "雲林縣"}
METAL_ELEC_MACH_2D = {"24", "25", "28", "29", "30", "31"}
TRAD_MFG_2D = {"08", "09", "10", "11", "12", "13", "14", "15", "16", "18", "19", "21", "22", "23", "32", "33"}
TARGET_CENTRAL_MFG_2D = METAL_ELEC_MACH_2D | TRAD_MFG_2D
ALL_MFG_SIC_2D = TARGET_CENTRAL_MFG_2D | {"17", "20", "26", "27"}
TWSE_TO_MFG_SIC = {
    "01": "23",  # 水泥
    "02": "08",  # 食品
    "03": "22",  # 塑膠
    "04": "11",  # 紡織纖維
    "05": "29",  # 電機機械
    "06": "28",  # 電器電纜
    "08": "23",  # 玻璃陶瓷
    "09": "15",  # 造紙
    "10": "24",  # 鋼鐵
    "11": "21",  # 橡膠
    "12": "30",  # 汽車
    "21": "18",  # 化學
    "22": "20",  # 生技醫療
    "23": "17",  # 油電燃氣/石油煤
    "24": "26",  # 半導體
    "25": "27",  # 電腦及週邊設備
    "26": "27",  # 光電
    "27": "27",  # 通信網路
    "28": "26",  # 電子零組件
    "31": "26",  # 其他電子
}

# Live TWSE & TPEx OpenAPI endpoints
TWSE_INFO_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TWSE_DAY_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TWSE_VAL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"

TPEX_INFO_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TPEX_DAY_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
TPEX_VAL_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"

SSL_CTX = ssl.create_default_context()
SSL_CTX_UNVERIFIED = ssl._create_unverified_context()
_TLS_WARN_LOCK = threading.Lock()
_TLS_WARNED_HOSTS: set[str] = set()

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

LEGACY_COUNTY_MAP: dict[str, str] = {
    "臺北縣": "新北市",
    "桃園縣": "桃園市",
    "臺中縣": "臺中市",
    "臺南縣": "臺南市",
    "高雄縣": "高雄市",
}

CAPITAL_CITY_COUNTY: dict[str, str] = {
    "南投市": "南投縣",
    "彰化市": "彰化縣",
    "苗栗市": "苗栗縣",
    "斗六市": "雲林縣",
    "太保市": "嘉義縣",
    "朴子市": "嘉義縣",
    "宜蘭市": "宜蘭縣",
    "花蓮市": "花蓮縣",
    "臺東市": "臺東縣",
    "馬公市": "澎湖縣",
    "竹北市": "新竹縣",
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
    "苗栗市": (120.819, 24.560, 0.016),
    "苑裡鎮": (120.654, 24.441, 0.016),
    "通霄鎮": (120.678, 24.489, 0.016),
    "後龍鎮": (120.786, 24.616, 0.016),
    "公館鄉": (120.828, 24.501, 0.015),
    "銅鑼鄉": (120.793, 24.482, 0.015),
    "三義鄉": (120.768, 24.398, 0.015),
    "造橋鄉": (120.864, 24.641, 0.014),
    "頭屋鄉": (120.847, 24.582, 0.014),
    "西屯區": (120.635, 24.178, 0.018),
    "南屯區": (120.625, 24.142, 0.016),
    "北屯區": (120.698, 24.182, 0.018),
    "大雅區": (120.646, 24.229, 0.016),
    "大里區": (120.678, 24.099, 0.016),
    "太平區": (120.718, 24.126, 0.018),
    "豐原區": (120.718, 24.252, 0.016),
    "潭子區": (120.705, 24.209, 0.015),
    "神岡區": (120.661, 24.257, 0.016),
    "烏日區": (120.624, 24.107, 0.016),
    "大甲區": (120.623, 24.348, 0.017),
    "龍井區": (120.545, 24.194, 0.016),
    "霧峰區": (120.700, 24.062, 0.017),
    "大肚區": (120.545, 24.154, 0.015),
    "梧棲區": (120.531, 24.254, 0.015),
    "后里區": (120.712, 24.305, 0.016),
    "清水區": (120.561, 24.268, 0.017),
    "外埔區": (120.654, 24.331, 0.014),
    "石岡區": (120.780, 24.275, 0.012),
    "東勢區": (120.828, 24.258, 0.015),
    "新社區": (120.809, 24.234, 0.015),
    "沙鹿區": (120.566, 24.234, 0.016),
    "彰化市": (120.542, 24.072, 0.018),
    "鹿港鎮": (120.435, 24.057, 0.018),
    "和美鎮": (120.498, 24.111, 0.017),
    "線西鄉": (120.464, 24.133, 0.014),
    "伸港鄉": (120.487, 24.151, 0.014),
    "秀水鄉": (120.503, 24.035, 0.015),
    "福興鄉": (120.443, 24.045, 0.015),
    "花壇鄉": (120.541, 24.025, 0.015),
    "埔鹽鄉": (120.467, 23.999, 0.015),
    "溪湖鎮": (120.479, 23.962, 0.016),
    "員林市": (120.574, 23.959, 0.017),
    "埔心鄉": (120.544, 23.953, 0.014),
    "大村鄉": (120.560, 23.993, 0.014),
    "社頭鄉": (120.583, 23.896, 0.015),
    "永靖鄉": (120.547, 23.924, 0.014),
    "田中鎮": (120.588, 23.861, 0.015),
    "北斗鎮": (120.525, 23.873, 0.015),
    "二林鎮": (120.371, 23.899, 0.018),
    "芳苑鄉": (120.320, 23.924, 0.017),
    "埤頭鄉": (120.461, 23.892, 0.015),
    "芬園鄉": (120.629, 24.014, 0.014),
    "田尾鄉": (120.525, 23.891, 0.013),
    "溪州鄉": (120.499, 23.851, 0.015),
    "南投市": (120.685, 23.915, 0.018),
    "草屯鎮": (120.684, 23.980, 0.017),
    "埔里鎮": (120.966, 23.966, 0.018),
    "竹山鎮": (120.683, 23.758, 0.017),
    "名間鄉": (120.703, 23.838, 0.015),
    "集集鎮": (120.784, 23.828, 0.013),
    "水里鄉": (120.855, 23.812, 0.014),
    "斗六市": (120.544, 23.709, 0.018),
    "斗南鎮": (120.479, 23.679, 0.016),
    "虎尾鎮": (120.434, 23.708, 0.017),
    "西螺鎮": (120.460, 23.799, 0.016),
    "麥寮鄉": (120.252, 23.752, 0.018),
    "北港鎮": (120.303, 23.575, 0.016),
    "土庫鎮": (120.391, 23.677, 0.015),
    "莿桐鄉": (120.498, 23.758, 0.014),
    "古坑鄉": (120.562, 23.642, 0.016),
    "大埤鄉": (120.431, 23.643, 0.014),
    "元長鄉": (120.312, 23.649, 0.014),
    "崙背鄉": (120.354, 23.759, 0.014),
    "二崙鄉": (120.413, 23.771, 0.014),
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

# County-scoped anchors for districts that exist in more than one county
COUNTY_DISTRICT_ANCHORS: dict[tuple[str, str], tuple[float, float, float]] = {
    ("臺北市", "中正區"): (121.519, 25.039, 0.010),
    ("基隆市", "中正區"): (121.758, 25.145, 0.012),
    ("臺北市", "中山區"): (121.533, 25.064, 0.012),
    ("基隆市", "中山區"): (121.732, 25.145, 0.012),
    ("臺北市", "信義區"): (121.567, 25.033, 0.011),
    ("基隆市", "信義區"): (121.766, 25.131, 0.012),
    ("臺北市", "大安區"): (121.543, 25.026, 0.011),
    ("臺中市", "大安區"): (120.587, 24.366, 0.014),
    ("臺中市", "中區"): (120.680, 24.144, 0.008),
    ("臺中市", "東區"): (120.697, 24.139, 0.012),
    ("臺中市", "西區"): (120.665, 24.142, 0.012),
    ("臺中市", "南區"): (120.662, 24.119, 0.013),
    ("臺中市", "北區"): (120.682, 24.161, 0.013),
}

DISTRICT_COUNTY_MAP: dict[str, tuple[str, ...]] = {
    "內湖區": ("臺北市",),
    "南港區": ("臺北市",),
    "松山區": ("臺北市",),
    "大同區": ("臺北市",),
    "萬華區": ("臺北市",),
    "士林區": ("臺北市",),
    "北投區": ("臺北市",),
    "文山區": ("臺北市",),
    "信義區": ("臺北市", "基隆市"),
    "大安區": ("臺北市", "臺中市"),
    "中山區": ("臺北市", "基隆市"),
    "中正區": ("臺北市", "基隆市"),
    "汐止區": ("新北市",),
    "板橋區": ("新北市",),
    "中和區": ("新北市",),
    "永和區": ("新北市",),
    "新店區": ("新北市",),
    "三重區": ("新北市",),
    "新莊區": ("新北市",),
    "五股區": ("新北市",),
    "土城區": ("新北市",),
    "樹林區": ("新北市",),
    "林口區": ("新北市",),
    "淡水區": ("新北市",),
    "蘆洲區": ("新北市",),
    "龜山區": ("桃園市",),
    "桃園區": ("桃園市",),
    "中壢區": ("桃園市",),
    "蘆竹區": ("桃園市",),
    "大園區": ("桃園市",),
    "平鎮區": ("桃園市",),
    "楊梅區": ("桃園市",),
    "八德區": ("桃園市",),
    "新竹科學園區": ("新竹市", "新竹縣"),
    "竹北市": ("新竹縣",),
    "竹東鎮": ("新竹縣",),
    "湖口鄉": ("新竹縣",),
    "新豐鄉": ("新竹縣",),
    "竹南鎮": ("苗栗縣",),
    "頭份市": ("苗栗縣",),
    "苗栗市": ("苗栗縣",),
    "苑裡鎮": ("苗栗縣",),
    "通霄鎮": ("苗栗縣",),
    "後龍鎮": ("苗栗縣",),
    "公館鄉": ("苗栗縣",),
    "銅鑼鄉": ("苗栗縣",),
    "三義鄉": ("苗栗縣",),
    "造橋鄉": ("苗栗縣",),
    "頭屋鄉": ("苗栗縣",),
    "西屯區": ("臺中市",),
    "南屯區": ("臺中市",),
    "北屯區": ("臺中市",),
    "大雅區": ("臺中市",),
    "大里區": ("臺中市",),
    "太平區": ("臺中市",),
    "豐原區": ("臺中市",),
    "潭子區": ("臺中市",),
    "神岡區": ("臺中市",),
    "烏日區": ("臺中市",),
    "大甲區": ("臺中市",),
    "龍井區": ("臺中市",),
    "霧峰區": ("臺中市",),
    "大肚區": ("臺中市",),
    "梧棲區": ("臺中市",),
    "后里區": ("臺中市",),
    "清水區": ("臺中市",),
    "外埔區": ("臺中市",),
    "石岡區": ("臺中市",),
    "東勢區": ("臺中市",),
    "新社區": ("臺中市",),
    "沙鹿區": ("臺中市",),
    "中區": ("臺中市",),
    "東區": ("臺中市", "新竹市", "嘉義市", "臺南市"),
    "西區": ("臺中市", "嘉義市"),
    "南區": ("臺中市", "臺南市"),
    "北區": ("臺中市", "新竹市", "臺南市"),
    "彰化市": ("彰化縣",),
    "鹿港鎮": ("彰化縣",),
    "和美鎮": ("彰化縣",),
    "線西鄉": ("彰化縣",),
    "伸港鄉": ("彰化縣",),
    "秀水鄉": ("彰化縣",),
    "福興鄉": ("彰化縣",),
    "花壇鄉": ("彰化縣",),
    "埔鹽鄉": ("彰化縣",),
    "溪湖鎮": ("彰化縣",),
    "員林市": ("彰化縣",),
    "埔心鄉": ("彰化縣",),
    "大村鄉": ("彰化縣",),
    "社頭鄉": ("彰化縣",),
    "永靖鄉": ("彰化縣",),
    "田中鎮": ("彰化縣",),
    "北斗鎮": ("彰化縣",),
    "二林鎮": ("彰化縣",),
    "芳苑鄉": ("彰化縣",),
    "埤頭鄉": ("彰化縣",),
    "芬園鄉": ("彰化縣",),
    "田尾鄉": ("彰化縣",),
    "溪州鄉": ("彰化縣",),
    "南投市": ("南投縣",),
    "草屯鎮": ("南投縣",),
    "埔里鎮": ("南投縣",),
    "竹山鎮": ("南投縣",),
    "名間鄉": ("南投縣",),
    "集集鎮": ("南投縣",),
    "水里鄉": ("南投縣",),
    "斗六市": ("雲林縣",),
    "斗南鎮": ("雲林縣",),
    "虎尾鎮": ("雲林縣",),
    "西螺鎮": ("雲林縣",),
    "麥寮鄉": ("雲林縣",),
    "北港鎮": ("雲林縣",),
    "土庫鎮": ("雲林縣",),
    "莿桐鄉": ("雲林縣",),
    "古坑鄉": ("雲林縣",),
    "大埤鄉": ("雲林縣",),
    "元長鄉": ("雲林縣",),
    "崙背鄉": ("雲林縣",),
    "二崙鄉": ("雲林縣",),
    "新市區": ("臺南市",),
    "善化區": ("臺南市",),
    "永康區": ("臺南市",),
    "安南區": ("臺南市",),
    "仁德區": ("臺南市",),
    "歸仁區": ("臺南市",),
    "安平區": ("臺南市",),
    "前鎮區": ("高雄市",),
    "苓雅區": ("高雄市",),
    "鼓山區": ("高雄市",),
    "左營區": ("高雄市",),
    "三民區": ("高雄市",),
    "楠梓區": ("高雄市",),
    "小港區": ("高雄市",),
    "仁武區": ("高雄市",),
    "岡山區": ("高雄市",),
    "路竹區": ("高雄市",),
    "大寮區": ("高雄市",),
    "鳳山區": ("高雄市",),
    "宜蘭市": ("宜蘭縣",),
    "羅東鎮": ("宜蘭縣",),
    "蘇澳鎮": ("宜蘭縣",),
    "花蓮市": ("花蓮縣",),
    "吉安鄉": ("花蓮縣",),
    "臺東市": ("臺東縣",),
}

_ROAD_FOLLOW_GUARD = r"(?!\s*(?:[NSEW]\.?\s*)?(?:RD|ROAD|ST|STREET|AVE|AVENUE|BLVD|LN|LANE|ALLEY|ALY|SEC|SECTION|VIL|VILLAGE)\b)"

_ROAD_PREFIX = r"(?:\b|(?<=RD)|(?<=ROAD)|(?<=ST)|(?<=STREET)|(?<=LN)|(?<=LANE)|(?<=AVE)|(?<=BLVD)|(?<=PARK)|(?<=DIST)|(?<=TOWNSHIP))"

# English address patterns for TPEx OTC companies:
# Pass 1: explicit full county/city names first; Pass 2: district/township tokens; Pass 3: bare city/park tokens.
ENG_COUNTY_PATTERNS: list[tuple[str, str]] = [
    # Pass 1: Explicit full county/city names
    (r"NEW\s*TAIP[EI]{1,3}|NEW\s*TAI\w*|TAIPEI\s*(?:COUNTY|HSIEN|HSIEH)", "新北市"),
    (r"HSIN\s*-?\s*CH[IU]\s+(?:COUNTY|HSIEN)", "新竹縣"),
    (r"HSIN\s*-?\s*CH[IU]\s+CITY", "新竹市"),
    (r"CHIA\s*-?\s*YI\s+(?:COUNTY|HSIEN)", "嘉義縣"),
    (r"CHIA\s*-?\s*YI\s+CITY", "嘉義市"),
    (r"TAO\s*-?\s*YUAN", "桃園市"),
    (r"K[AO][OA]SH?IUNG|KAO\s*HSIUNG|KAOSHSIUNG|KOAHSIUNG", "高雄市"),
    (r"TAI\s*-?\s*NAN|\bTAIN\b|\bTAIN\s*$", "臺南市"),
    (r"TAI\s*-?\s*CHUNG", "臺中市"),
    (r"MIAO\s*-?\s*LI", "苗栗縣"),
    (r"CHANG\s*-?\s*HUA", "彰化縣"),
    (r"NAN\s*-?\s*TOU", "南投縣"),
    (r"YUNE?\s*-?\s*LIN", "雲林縣"),
    (r"PING\s*-?\s*TUNG", "屏東縣"),
    (r"KEELUNG", "基隆市"),
    (rf"{_ROAD_PREFIX}Y?I\s*-?\s*LAN\b", "宜蘭縣"),
    (r"HUA\s*-?\s*LIEN", "花蓮縣"),
    (r"TAI\s*-?\s*TUNG", "臺東縣"),
    (r"PENG\s*-?\s*HU", "澎湖縣"),
    (r"KIN\s*-?\s*MEN", "金門縣"),
    (r"TAI\s*-?\s*PEI\s*CITY", "臺北市"),
    # Pass 2: Unambiguous district/township tokens (before bare TAIPEI / HSINCHU / parks)
    (
        rf"{_ROAD_PREFIX}(?:SI\s*JHIH|XIZHI|HSI\s*-?\s*CHIH|SHI\s*-?\s*JI|SIJHIH|ZHONGHE|CHUNG\s*-?\s*HO|JHONGHE|JUNGHE|YONGHE|BANQIAO|PANCHIAO|XINDIAN|HSIN\s*-?\s*TIEN|SIN\s*-?\s*DIAN|SINDIAN|TUCHENG|TO\s*-?\s*CHEN|SHULIN|SANCHONG|SANCHUNG|XINZHUANG|HSIN\s*-?\s*CHUANG|WUGU|WU\s*-?\s*KU|LINKOU|SHEN\s*-?\s*KENG|DANSHUI|TAMSUI|JUIFANG)\b{_ROAD_FOLLOW_GUARD}",
        "新北市",
    ),
    (
        rf"{_ROAD_PREFIX}(?:(?:ZHU|CHU|JHU|JU)\s*-?\s*[BP]EI|HU\s*-?\s*KOU|HSIN\s*-?\s*F[EUO]NG|CHU\s*-?\s*TUNG|CHIUNG\s*LIN|BAO\s*-?\s*SHAN)\b{_ROAD_FOLLOW_GUARD}",
        "新竹縣",
    ),
    (
        rf"{_ROAD_PREFIX}(?:NEI\s*-?\s*HU|NAN\s*-?\s*[GK]ANG|SONG\s*-?\s*SHAN|SHI\s*-?\s*LIN|SHIH\s*-?\s*LIN|SELIN|BEI\s*-?\s*TOU|PEI\s*-?\s*TOU|WAN\s*-?\s*HUA|DA\s*-?\s*TONG)\b{_ROAD_FOLLOW_GUARD}",
        "臺北市",
    ),
    (
        rf"{_ROAD_PREFIX}(?:CHUNG\s*-?\s*LI|ZHONG\s*-?\s*LI|YANG\s*-?\s*MEI|KUEI\s*-?\s*SHAN|GUI\s*-?\s*SHAN|GUEI\s*-?\s*SHAN|KWEI\s*-?\s*SHAN|PING\s*-?\s*CHEN|PING\s*-?\s*ZHEN|PING\s*-?\s*JHEN|DA\s*-?\s*YUAN|LUNG\s*-?\s*TAN|LONG\s*-?\s*TAN|BA\s*-?\s*DE|PA\s*-?\s*TE|GUAN\s*-?\s*YIN)\b{_ROAD_FOLLOW_GUARD}",
        "桃園市",
    ),
    (
        rf"{_ROAD_PREFIX}(?:XI\s*-?\s*TUN|HSI\s*-?\s*TUN|SI\s*-?\s*TUN|NAN\s*-?\s*TUN|NUN\s*-?\s*TUN|BEI\s*-?\s*TUN|DA\s*-?\s*YA|TA\s*-?\s*YA|DA\s*-?\s*LI(?!AO)|TAN\s*-?\s*ZI|TAN\s*-?\s*TZU|FENG\s*-?\s*YUAN|SHEN\s*-?\s*KANG|SHEN\s*-?\s*GANG|DA\s*-?\s*JIA|TA\s*-?\s*CHIA)\b{_ROAD_FOLLOW_GUARD}",
        "臺中市",
    ),
    (r"\b(?:MIN\s*-?\s*HSIUNG|MINXIONG|YI\s*-?\s*CHU)\b", "嘉義縣"),
    (rf"{_ROAD_PREFIX}DOU\s*-?\s*L[EI]U\b", "雲林縣"),
    (
        rf"{_ROAD_PREFIX}(?:XIN\s*-?\s*SHI|HSIN\s*-?\s*SHI|SHAN\s*-?\s*H[UW]A|YONG\s*-?\s*KANG|AN\s*-?\s*NAN|REN\s*-?\s*DE|GUI\s*-?\s*REN|SHIN\s*-?\s*YING|HSIN\s*-?\s*YING)\b{_ROAD_FOLLOW_GUARD}",
        "臺南市",
    ),
    (
        rf"{_ROAD_PREFIX}(?:QIAN\s*-?\s*ZHEN|CHIEN\s*-?\s*ZHEN|CIANJHEN|NAN\s*-?\s*ZI[H]?|NAN\s*-?\s*TZ[EU]|GU\s*-?\s*SHAN|LING\s*-?\s*YA|SAN\s*-?\s*MIN|ZUO\s*-?\s*YING|XIAO\s*-?\s*GANG|HSIAO\s*-?\s*KANG|REN\s*-?\s*WU|DA\s*-?\s*LIAO|TA\s*-?\s*FA)\b{_ROAD_FOLLOW_GUARD}",
        "高雄市",
    ),
    (r"\bLU\s*-?\s*GANG|LUGANG\b", "彰化縣"),
    (r"\b(?:CHU|ZHU)\s*-?\s*NAN\b", "苗栗縣"),
    # Pass 3: Bare city/county names and science parks
    (r"TAI\s*-?\s*PEI", "臺北市"),
    (rf"{_ROAD_PREFIX}HSIN\s*-?\s*CH[IU]\b|SCIENCE[- ]BASED\s+INDUSTRIAL\s+PARK|\bSBIP", "新竹市"),
    (r"\bCHIA\s*-?\s*YI\b", "嘉義市"),
]

_DIST_BOUNDARY = r"(?:\s*[-\s]?(?:DIST(?:RICT)?|TOWNSHIP|CITY|TOWN|HSIANG|CHIU)\b)"
_ROAD_DIST_BOUNDARY = r"(?:\s*[-\s]?(?:DIST(?:RICT)?|TOWNSHIP|HSIANG|CHIU)\b)"

# (pattern, district_name, allowed_counties_or_None)
# Street-prone names require explicit district/township/city boundary or are guarded against road suffixes.
ENG_DISTRICT_PATTERNS: list[tuple[str, str, tuple[str, ...] | None]] = [
    (rf"{_ROAD_PREFIX}(?:NEI\s*-?\s*HU)\b{_ROAD_FOLLOW_GUARD}", "內湖區", ("臺北市",)),
    (rf"{_ROAD_PREFIX}(?:NAN\s*-?\s*[GK]ANG)\b{_ROAD_FOLLOW_GUARD}", "南港區", ("臺北市",)),
    (rf"{_ROAD_PREFIX}(?:SONG\s*-?\s*SHAN)\b{_ROAD_FOLLOW_GUARD}", "松山區", ("臺北市",)),
    (rf"{_ROAD_PREFIX}(?:SI\s*JHIH|XIZHI|HSI\s*-?\s*CHIH|SIJHIH)\b{_ROAD_FOLLOW_GUARD}", "汐止區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:XIN\s*-?\s*YI|HSIN\s*-?\s*YI){_DIST_BOUNDARY}", "信義區", ("臺北市", "基隆市")),
    (rf"{_ROAD_PREFIX}(?:DA\s*-?\s*AN|TA\s*-?\s*AN){_DIST_BOUNDARY}", "大安區", ("臺北市", "臺中市")),
    (rf"{_ROAD_PREFIX}(?:Z?HONG\s*-?\s*SHAN|CHUNG\s*-?\s*SHAN){_DIST_BOUNDARY}", "中山區", ("臺北市", "基隆市")),
    (rf"{_ROAD_PREFIX}(?:ZHONG\s*-?\s*ZHENG|CHUNG\s*-?\s*CHENG|JHONG\s*-?\s*JHENG){_DIST_BOUNDARY}", "中正區", ("臺北市", "基隆市")),
    (rf"{_ROAD_PREFIX}(?:SHI\s*-?\s*LIN|SHIH\s*-?\s*LIN|SELIN)\b{_ROAD_FOLLOW_GUARD}", "士林區", ("臺北市",)),
    (rf"{_ROAD_PREFIX}(?:BEI\s*-?\s*TOU|PEI\s*-?\s*TOU)\b{_ROAD_FOLLOW_GUARD}", "北投區", ("臺北市",)),
    (rf"{_ROAD_PREFIX}(?:DA\s*-?\s*TONG){_DIST_BOUNDARY}", "大同區", ("臺北市",)),
    (rf"{_ROAD_PREFIX}(?:WAN\s*-?\s*HUA)\b{_ROAD_FOLLOW_GUARD}", "萬華區", ("臺北市",)),
    (rf"{_ROAD_PREFIX}(?:BAN\s*-?\s*QIAO|PAN\s*-?\s*CHIAO)\b{_ROAD_FOLLOW_GUARD}", "板橋區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:ZHONG\s*-?\s*HE|CHUNG\s*-?\s*HO|JHONG\s*-?\s*HE|JUNG\s*-?\s*HE){_DIST_BOUNDARY}", "中和區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:YONG\s*-?\s*HE)\b{_ROAD_FOLLOW_GUARD}", "永和區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:XIN\s*-?\s*DIAN|HSIN\s*-?\s*TIEN|SIN\s*-?\s*DIAN|SINDIAN)\b{_ROAD_FOLLOW_GUARD}", "新店區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:SAN\s*-?\s*CHONG|SAN\s*-?\s*CHUNG)\b{_ROAD_FOLLOW_GUARD}", "三重區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:XIN\s*-?\s*ZHUANG|HSIN\s*-?\s*CHUANG)\b{_ROAD_FOLLOW_GUARD}", "新莊區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:WU\s*-?\s*GU|WU\s*-?\s*KU)\b{_ROAD_FOLLOW_GUARD}", "五股區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:TU\s*-?\s*CHENG|TO\s*-?\s*CHEN)\b{_ROAD_FOLLOW_GUARD}", "土城區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:SHU\s*-?\s*LIN)\b{_ROAD_FOLLOW_GUARD}", "樹林區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:LIN\s*-?\s*KOU)\b{_ROAD_FOLLOW_GUARD}", "林口區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:DAN\s*-?\s*SHUI|TAM\s*-?\s*SUI)\b{_ROAD_FOLLOW_GUARD}", "淡水區", ("新北市",)),
    (rf"{_ROAD_PREFIX}(?:GUI\s*-?\s*SHAN|KUEI\s*-?\s*SHAN|GUEI\s*-?\s*SHAN|KWEI\s*-?\s*SHAN)\b{_ROAD_FOLLOW_GUARD}", "龜山區", ("桃園市",)),
    (rf"{_ROAD_PREFIX}(?:CHUNG\s*-?\s*LI|ZHONG\s*-?\s*LI)\b{_ROAD_FOLLOW_GUARD}", "中壢區", ("桃園市",)),
    (rf"{_ROAD_PREFIX}(?:YANG\s*-?\s*MEI)\b{_ROAD_FOLLOW_GUARD}", "楊梅區", ("桃園市",)),
    (rf"{_ROAD_PREFIX}(?:PING\s*-?\s*ZHEN|PING\s*-?\s*CHEN|PING\s*-?\s*JHEN)\b{_ROAD_FOLLOW_GUARD}", "平鎮區", ("桃園市",)),
    (rf"{_ROAD_PREFIX}(?:LU\s*-?\s*[CZ]HU|LUJHU)\b{_ROAD_FOLLOW_GUARD}", "蘆竹區", ("桃園市",)),
    (rf"{_ROAD_PREFIX}(?:LU\s*-?\s*[CZ]HU|LUJHU)\b{_ROAD_FOLLOW_GUARD}", "路竹區", ("高雄市",)),
    (rf"{_ROAD_PREFIX}(?:DA\s*-?\s*YUAN)\b{_ROAD_FOLLOW_GUARD}", "大園區", ("桃園市",)),
    (rf"{_ROAD_PREFIX}(?:GUAN\s*-?\s*YIN)\b{_ROAD_FOLLOW_GUARD}", "觀音區", ("桃園市",)),
    (rf"{_ROAD_PREFIX}(?:LUNG\s*-?\s*TAN|LONG\s*-?\s*TAN)\b{_ROAD_FOLLOW_GUARD}", "龍潭區", ("桃園市",)),
    (rf"{_ROAD_PREFIX}(?:BA\s*-?\s*DE|PA\s*-?\s*TE)\b{_ROAD_FOLLOW_GUARD}", "八德區", ("桃園市",)),
    (rf"{_ROAD_PREFIX}(?:TAO\s*-?\s*YUAN){_ROAD_DIST_BOUNDARY}", "桃園區", ("桃園市",)),
    (rf"\b(?:ZHU|CHU|JHU|JU)\s*-?\s*(?:BEI|PEI)\b{_ROAD_FOLLOW_GUARD}", "竹北市", ("新竹縣",)),
    (rf"\b(?:HU\s*-?\s*KOU)\b{_ROAD_FOLLOW_GUARD}", "湖口鄉", ("新竹縣",)),
    (rf"\b(?:HSIN\s*-?\s*F[EUO]NG)\b{_ROAD_FOLLOW_GUARD}", "新豐鄉", ("新竹縣",)),
    (rf"\b(?:ZHU|CHU)\s*-?\s*NAN\b{_ROAD_FOLLOW_GUARD}", "竹南鎮", ("苗栗縣",)),
    (rf"\b(?:TOU\s*-?\s*FEN)\b{_ROAD_FOLLOW_GUARD}", "頭份市", ("苗栗縣",)),
    (rf"\b(?:TONG\s*-?\s*LUO)\b{_ROAD_FOLLOW_GUARD}", "銅鑼鄉", ("苗栗縣",)),
    (rf"\b(?:XI\s*-?\s*TUN|HSI\s*-?\s*TUN|SI\s*-?\s*TUN)\b{_ROAD_FOLLOW_GUARD}", "西屯區", ("臺中市",)),
    (rf"\b(?:NAN\s*-?\s*TUN|NUN\s*-?\s*TUN)\b{_ROAD_FOLLOW_GUARD}", "南屯區", ("臺中市",)),
    (rf"\b(?:BEI\s*-?\s*TUN)\b{_ROAD_FOLLOW_GUARD}", "北屯區", ("臺中市",)),
    (rf"\b(?:DA\s*-?\s*YA|TA\s*-?\s*YA)\b{_ROAD_FOLLOW_GUARD}", "大雅區", ("臺中市",)),
    (rf"\b(?:DA\s*-?\s*LI)\b{_ROAD_FOLLOW_GUARD}", "大里區", ("臺中市",)),
    (rf"\b(?:TAI\s*-?\s*PING)\b{_ROAD_FOLLOW_GUARD}", "太平區", ("臺中市",)),
    (rf"\b(?:TAN\s*-?\s*ZI|TAN\s*-?\s*TZU)\b{_ROAD_FOLLOW_GUARD}", "潭子區", ("臺中市",)),
    (rf"\b(?:SHEN\s*-?\s*GANG|SHEN\s*-?\s*KANG)\b{_ROAD_FOLLOW_GUARD}", "神岡區", ("臺中市",)),
    (rf"\b(?:XIN\s*-?\s*SHI|HSIN\s*-?\s*SHI)\b{_ROAD_FOLLOW_GUARD}", "新市區", ("臺南市",)),
    (rf"\b(?:SHAN\s*-?\s*H[UW]A)\b{_ROAD_FOLLOW_GUARD}", "善化區", ("臺南市",)),
    (rf"\b(?:YONG\s*-?\s*KANG|YUNG\s*-?\s*KANG)\b{_ROAD_FOLLOW_GUARD}", "永康區", ("臺南市",)),
    (rf"\b(?:AN\s*-?\s*NAN)\b{_ROAD_FOLLOW_GUARD}", "安南區", ("臺南市",)),
    (rf"\b(?:REN\s*-?\s*DE)\b{_ROAD_FOLLOW_GUARD}", "仁德區", ("臺南市",)),
    (rf"\b(?:GUI\s*-?\s*REN)\b{_ROAD_FOLLOW_GUARD}", "歸仁區", ("臺南市",)),
    (rf"\b(?:AN\s*-?\s*PING)\b{_ROAD_FOLLOW_GUARD}", "安平區", ("臺南市",)),
    (rf"\b(?:QIAN\s*-?\s*ZHEN|CHIEN\s*-?\s*CHEN|CIAN\s*-?\s*JHEN)\b{_ROAD_FOLLOW_GUARD}", "前鎮區", ("高雄市",)),
    (rf"\b(?:LING\s*-?\s*YA)\b{_ROAD_FOLLOW_GUARD}", "苓雅區", ("高雄市",)),
    (rf"\b(?:GU\s*-?\s*SHAN|KU\s*-?\s*SHAN)\b{_ROAD_FOLLOW_GUARD}", "鼓山區", ("高雄市",)),
    (rf"\b(?:ZUO\s*-?\s*YING|TSO\s*-?\s*YING)\b{_ROAD_FOLLOW_GUARD}", "左營區", ("高雄市",)),
    (rf"\b(?:SAN\s*-?\s*MIN)\b{_ROAD_FOLLOW_GUARD}", "三民區", ("高雄市",)),
    (rf"\b(?:NAN\s*-?\s*ZI[H]?|NAN\s*-?\s*TZ[EU])\b{_ROAD_FOLLOW_GUARD}", "楠梓區", ("高雄市",)),
    (rf"\b(?:HSIAO\s*-?\s*KANG|XIAO\s*-?\s*GANG)\b{_ROAD_FOLLOW_GUARD}", "小港區", ("高雄市",)),
    (rf"\b(?:REN\s*-?\s*WUU?)\b{_ROAD_FOLLOW_GUARD}", "仁武區", ("高雄市",)),
    (rf"\b(?:KANG\s*-?\s*SHAN|GANG\s*-?\s*SHAN)\b{_ROAD_FOLLOW_GUARD}", "岡山區", ("高雄市",)),
    (rf"\b(?:DA\s*-?\s*LIAO)\b{_ROAD_FOLLOW_GUARD}", "大寮區", ("高雄市",)),
    (rf"\b(?:FENG\s*-?\s*SHAN)\b{_ROAD_FOLLOW_GUARD}", "鳳山區", ("高雄市",)),
    (rf"\b(?:SU\s*-?\s*AO)\b{_ROAD_FOLLOW_GUARD}", "蘇澳鎮", ("宜蘭縣",)),
]

# Official TWSE/TPEx industry code mapping into 9 PoC sectors
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
    "04": "傳統製造工業",       # 紡織纖維
    "32": "餐飲旅宿與生活文創",  # 文化創意業
    "37": "金屬電機與精密機械",  # 運動休閒（自行車、製鞋、運動器材製造為主；場館/健身另判）
    "15": "批發零售與國際貿易",  # 航運業（航太/造船另判金屬電機與精密機械）
    "18": "批發零售與國際貿易",  # 貿易百貨
    "38": "傳統製造工業",       # 居家生活
    "01": "傳統製造工業",       # 水泥工業
    "02": "傳統製造工業",       # 食品工業
    "03": "傳統製造工業",       # 塑膠工業
    "05": "金屬電機與精密機械",  # 電機機械
    "06": "金屬電機與精密機械",  # 電器電纜
    "08": "傳統製造工業",       # 玻璃陶瓷
    "09": "傳統製造工業",       # 造紙工業
    "10": "金屬電機與精密機械",  # 鋼鐵工業
    "11": "傳統製造工業",       # 橡膠工業
    "12": "金屬電機與精密機械",  # 汽車工業
    "19": "傳統製造工業",       # 綜合
    "20": "傳統製造工業",       # 其他
    "21": "傳統製造工業",       # 化學工業
    "23": "傳統製造工業",       # 油電燃氣業
    "33": "餐飲旅宿與生活文創",  # 農林漁牧
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
        "tagline": "經濟部 GCIS × 登記工廠名錄 × TWSE & TPEx 台股即時行情",
        "narrative": "串聯經濟部實測商工登記、全台中部生產中製造業工廠與全臺上市櫃即時交易行情，呈現北部「金融與IC設計研發」、竹苗「半導體護國神山群」、中部「中彰投金屬電機、精密機械與傳統製造聚落」、南科「先進製程與綠能」至高雄「亞灣智慧港灣與封測S廊帶」的立體產業地圖。",
        "highlights": ["GCIS×工廠名錄×TWSE", "中部金屬電機大聚落", "竹科半導體心臟", "AI 3D對話導覽", "8大聚落座標"],
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
        "name": "中彰投・精密機械・金屬電機與傳產走廊",
        "short": "中部・金屬機械傳產",
        "en": "CENTRAL TAIWAN · METAL & MFG",
        "center": [120.615, 24.115],
        "cam": {"yaw": -0.42, "pitch": 0.70, "zoom": 3.0},
        "tagline": "臺中神岡大里太平工具機 × 彰化水五金傳產 × 中部金屬電機大聚落",
        "narrative": "橫跨臺中大里、神岡、太平、豐原、烏日、西屯中科至彰化鹿港、和美、員林及南投南崗工業區，完整收錄金屬製品、手工具、螺絲、電力電機、工具機及傳統製造萬家生產中工廠與大立光、上銀、漢翔等隱形冠軍。",
        "highlights": ["萬家金屬電機工廠", "神岡大里太平工具機", "彰化水五金與傳產", "老牌製造隱形冠軍"],
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
    "金屬電機與精密機械",
    "金融控股與投資",
    "批發零售與國際貿易",
    "生技醫療與綠能永續",
    "營造工程與不動產",
    "餐飲旅宿與生活文創",
    "傳統製造工業",
]


def normalize_tw_text(s: str) -> str:
    out = (s or "").replace("\u5dff", "市").replace("台", "臺").strip()
    for old, new in LEGACY_COUNTY_MAP.items():
        out = out.replace(old, new)
    return out


def load_taiwan_geojson() -> dict[str, Any]:
    if GEO_MAPS_JS.exists():
        text = GEO_MAPS_JS.read_text(encoding="utf-8")
        m = re.search(r"const TAIWAN_GEOJSON\s*=\s*(\{.*?\});", text, re.S)
        if m:
            return json.loads(m.group(1))
    for html_path in (POC_HTML, INDEX_HTML):
        if html_path.exists():
            text = html_path.read_text(encoding="utf-8")
            m = re.search(r"window\.__TAIWAN_GEOJSON__\s*=\s*(\{.*?\});", text)
            if m:
                return json.loads(m.group(1))
    raise RuntimeError("Could not find TAIWAN_GEOJSON in js/universal_geo_maps.js or HTML files")


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
        props = feat.get("properties") or {}
        name = props.get("name") or props.get("COUNTYNAME")
        if name:
            norm = normalize_tw_text(name)
            idx[norm] = feat
            idx[name] = feat
            if props.get("COUNTYNAME"):
                raw_cn = str(props["COUNTYNAME"])
                idx[raw_cn] = feat
                idx[normalize_tw_text(raw_cn)] = feat
    return idx


def fetch_url_bytes(
    url: str, accept: str = "*/*", retries: int = 2, timeout: int = 20
) -> bytes:
    """Fetch URL bytes using strict TLS verification with logged SSLError fallback."""
    host = urllib.parse.urlsplit(url).netloc
    use_unverified = False
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                    "Accept": accept,
                },
            )
            ctx = SSL_CTX_UNVERIFIED if use_unverified else SSL_CTX
            with urllib.request.urlopen(req, context=ctx, timeout=timeout) as resp:
                return resp.read()
        except (ssl.SSLError, urllib.error.URLError) as exc:
            is_ssl = isinstance(exc, ssl.SSLError) or (
                isinstance(exc, urllib.error.URLError) and isinstance(exc.reason, ssl.SSLError)
            )
            if is_ssl and not use_unverified:
                with _TLS_WARN_LOCK:
                    if host not in _TLS_WARNED_HOSTS:
                        _TLS_WARNED_HOSTS.add(host)
                        print(f"[TLS] Verification failed for {host} ({exc}); falling back to unverified SSL context")
                use_unverified = True
                continue
            if attempt < retries:
                time.sleep(0.4 * (attempt + 1))
        except Exception:
            if attempt < retries:
                time.sleep(0.4 * (attempt + 1))
    return b""


def fetch_json(url: str, retries: int = 2, timeout: int = 16) -> list[dict[str, Any]]:
    raw_bytes = fetch_url_bytes(url, accept="application/json", retries=retries, timeout=timeout)
    if not raw_bytes:
        return []
    try:
        raw = raw_bytes.decode("utf-8", errors="replace").strip()
        if not raw:
            return []
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
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
                        "_fac_id": c.get("fac_id", ""),
                        "_fac_prod": c.get("prod", ""),
                        "_sic_codes": [s for s in str(c.get("sic") or "").split(",") if s],
                        "_cap_est": bool(c.get("cap_est")),
                    }
                    if c.get("flagship"):
                        verified_flagships.add(ban)
                return records, verified_flagships
        except Exception as exc:
            print(f"[GCIS] Cache read failed ({exc}), fetching live GCIS...")
    return collect_live_gcis_records()


def _parse_csv_bytes(raw_bytes: bytes) -> list[dict[str, str]]:
    if not raw_bytes:
        return []
    txt = raw_bytes.decode("utf-8-sig", errors="replace")
    return list(csv.DictReader(io.StringIO(txt)))


def _resolve_ida_factory_zip_url() -> str:
    raw = fetch_url_bytes(IDA_FACTORY_INDEX_CSV, retries=1, timeout=15)
    for row in _parse_csv_bytes(raw):
        link = (row.get("下載連結") or "").strip()
        if link.startswith("http"):
            return link
    return IDA_FACTORY_ZIP_FALLBACK


def _clean_fac_tags(ind_text: str, prod_text: str) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for seg in re.split(r"[、,，]+", f"{ind_text}、{prod_text}"):
        s = re.sub(r"^\d+\s*", "", seg.strip()).strip()
        if s and s not in seen and s not in ("未分類其他製造業",):
            seen.add(s)
            parts.append(s)
    return "・".join(parts)[:72]


def enrich_central_mfg_and_factories(raw_records: dict[str, dict[str, Any]]) -> None:
    """Download MOEA IDA active factories + GCIS Central Taiwan C-mfg CSVs and merge by BAN."""
    zip_url = _resolve_ida_factory_zip_url()
    urls = {
        "fac_zip": zip_url,
        "gcis_tc": f"https://data.gcis.nat.gov.tw/od/file?oid={OID_TAICHUNG_COMP_CMFG}",
        "gcis_ct": f"https://data.gcis.nat.gov.tw/od/file?oid={OID_CENTRAL_COMP_CMFG}",
        "biz_tc": f"https://data.gcis.nat.gov.tw/od/file?oid={OID_TAICHUNG_BIZ_CMFG}",
    }
    print("[CENTRAL] Fetching MOEA active factory registry + GCIS Central Taiwan C-mfg CSVs...")
    blobs: dict[str, bytes] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futs = {pool.submit(fetch_url_bytes, u, "*/*", 2, 45): k for k, u in urls.items()}
        for fut in as_completed(futs):
            k = futs[fut]
            blobs[k] = fut.result()

    # 1. Index GCIS Central Taiwan company & Taichung commercial registrations
    gcis_central_map: dict[str, dict[str, str]] = {}
    for key in ("gcis_tc", "gcis_ct"):
        for r in _parse_csv_bytes(blobs.get(key, b"")):
            if (r.get("登記狀態") or "").strip() != "核准設立":
                continue
            ban = (r.get("統一編號") or "").strip()
            if re.fullmatch(r"\d{8}", ban):
                gcis_central_map[ban] = r

    biz_tc_map: dict[str, dict[str, str]] = {}
    for r in _parse_csv_bytes(blobs.get("biz_tc", b"")):
        ban = (r.get("統一編號") or "").strip()
        if re.fullmatch(r"\d{8}", ban):
            biz_tc_map[ban] = r

    # 2. Parse MOEA active factory registry (生產中工廠清冊.zip)
    fac_bytes = blobs.get("fac_zip", b"")
    fac_rows: list[dict[str, str]] = []
    if fac_bytes:
        try:
            with zipfile.ZipFile(io.BytesIO(fac_bytes)) as zf:
                csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), zf.namelist()[0])
                fac_rows = _parse_csv_bytes(zf.read(csv_name))
        except Exception as exc:
            print(f"[CENTRAL] Factory ZIP parse failed: {exc}")

    # Aggregate Central Taiwan traditional & metal/electrical/machinery factories by BAN
    # Plus collect nationwide factory SIC codes for companies already present in raw_records
    central_fac_map: dict[str, dict[str, Any]] = {}
    nation_fac_by_ban: dict[str, dict[str, Any]] = {}
    for row in fac_rows:
        if (row.get("工廠登記狀態") or "").strip() != "生產中":
            continue
        ban = (row.get("統一編號") or "").strip()
        if not re.fullmatch(r"\d{8}", ban):
            continue
        ind_str = (row.get("產業類別") or "").strip()
        codes = re.findall(r"(?<!\d)(\d{2})(?=[^\d])", ind_str)
        fac_id = (row.get("工廠登記編號") or "").strip()
        prod_str = (row.get("主要產品") or "").strip()

        if ban in raw_records:
            nf = nation_fac_by_ban.setdefault(ban, {"codes": [], "fac_ids": [], "tag": ""})
            for cd in codes:
                if cd in ALL_MFG_SIC_2D and cd not in nf["codes"]:
                    nf["codes"].append(cd)
            if fac_id and fac_id not in nf["fac_ids"]:
                nf["fac_ids"].append(fac_id)
            if not nf["tag"]:
                nf["tag"] = _clean_fac_tags(ind_str, prod_str)

        addr = normalize_tw_text(row.get("工廠地址") or row.get("工廠市鎮鄉村里") or "")
        county_guess, _ = parse_county_and_district(addr, "")
        if county_guess not in CENTRAL_COUNTIES:
            continue
        if not any(cd in TARGET_CENTRAL_MFG_2D for cd in codes):
            continue

        fac_name = (row.get("工廠名稱") or "").strip()
        resp = (row.get("工廠負責人姓名") or "").strip()
        setup_d = (row.get("工廠設立核准日期") or row.get("工廠登記核准日期") or "").strip()

        entry = central_fac_map.get(ban)
        if entry is None:
            central_fac_map[ban] = {
                "fac_ids": [fac_id] if fac_id else [],
                "fac_name": fac_name,
                "addr": addr,
                "resp": resp,
                "setup_date": setup_d,
                "ind_codes": list(codes),
                "ind_str": ind_str,
                "prod_str": prod_str,
            }
        else:
            if fac_id and fac_id not in entry["fac_ids"]:
                entry["fac_ids"].append(fac_id)
            for cd in codes:
                if cd not in entry["ind_codes"]:
                    entry["ind_codes"].append(cd)
            if prod_str and prod_str not in entry["prod_str"]:
                entry["prod_str"] = f"{entry['prod_str']}、{prod_str}".strip("、")

    def _dgbas_list(row: dict[str, str]) -> list[str]:
        raw_c = row.get("行業代號（財政資訊中心匯入）") or ""
        return [c.strip()[:2] for c in raw_c.split(",") if c.strip() and c.strip()[:2].isdigit()]

    def _pick_cap(row: dict[str, str]) -> int:
        for k in ("實收資本額", "資本總額"):
            v = re.sub(r"\D", "", str(row.get(k) or ""))
            if v and int(v) > 0:
                return int(v)
        return 0

    added_fac = 0
    enriched_existing = 0
    for ban, fac in central_fac_map.items():
        g_row = gcis_central_map.get(ban)
        b_row = biz_tc_map.get(ban)
        fac_tag = _clean_fac_tags(fac["ind_str"], fac["prod_str"])
        fac_id_joined = ",".join(fac["fac_ids"][:3])

        if ban in raw_records:
            rec = raw_records[ban]
            rec["_fac_id"] = fac_id_joined or rec.get("_fac_id", "")
            rec["_fac_prod"] = fac_tag or rec.get("_fac_prod", "")
            rec["_fac_codes"] = fac["ind_codes"]
            if g_row:
                rec["_dgbas_codes"] = _dgbas_list(g_row)
                cap = _pick_cap(g_row)
                if cap > 0 and int(rec.get("Paid_In_Capital_Amount") or 0) <= 0:
                    rec["Paid_In_Capital_Amount"] = cap
                    rec["Capital_Stock_Amount"] = cap
                    rec["_cap_est"] = False
            elif b_row:
                rec["_dgbas_codes"] = _dgbas_list(b_row)
            enriched_existing += 1
            continue

        if g_row:
            cap = _pick_cap(g_row)
            g_addr = normalize_tw_text(g_row.get("公司地址") or "")
            g_county, _ = parse_county_and_district(g_addr, "")
            loc = g_addr if g_county in CENTRAL_COUNTIES else fac["addr"]
            name = (g_row.get("公司名稱") or fac["fac_name"]).strip()
            setup_date = (g_row.get("核准設立日期") or fac["setup_date"]).strip()
            dgbas = _dgbas_list(g_row)
            reg_org = "經濟部商工×工廠登記"
            cap_est = cap <= 0
        elif b_row:
            cap = _pick_cap(b_row)
            b_addr = normalize_tw_text(b_row.get("商業地址") or "")
            b_county, _ = parse_county_and_district(b_addr, "")
            loc = b_addr if b_county in CENTRAL_COUNTIES else fac["addr"]
            name = (b_row.get("商業名稱") or fac["fac_name"]).strip()
            setup_date = (b_row.get("核准設立日期") or fac["setup_date"]).strip()
            dgbas = _dgbas_list(b_row)
            reg_org = "臺中市政府(商業×工廠)"
            cap_est = cap <= 0
        else:
            name = re.sub(r"(公司)工廠$", r"\1", fac["fac_name"]).strip()
            loc = fac["addr"]
            cap = 0
            setup_date = fac["setup_date"]
            dgbas = []
            reg_org = "經濟部產業發展署(工廠登記)"
            cap_est = True

        raw_records[ban] = {
            "Business_Accounting_NO": ban,
            "Company_Name": name,
            "Responsible_Name": fac["resp"] or "—",
            "Paid_In_Capital_Amount": cap,
            "Capital_Stock_Amount": cap,
            "Company_Setup_Date": setup_date,
            "Company_Location": loc,
            "Register_Organization_Desc": reg_org,
            "Company_Status": "01",
            "_fac_id": fac_id_joined,
            "_fac_prod": fac_tag,
            "_fac_codes": fac["ind_codes"],
            "_dgbas_codes": dgbas,
            "_cap_est": cap_est,
        }
        added_fac += 1

    # 3. Also add extra Central Taiwan active GCIS C-mfg companies whose primary tax SIC is TARGET_CENTRAL_MFG_2D
    added_gcis_mfg = 0
    for ban, g_row in gcis_central_map.items():
        dgbas = _dgbas_list(g_row)
        if ban in raw_records:
            if dgbas and not raw_records[ban].get("_dgbas_codes"):
                raw_records[ban]["_dgbas_codes"] = dgbas
            continue
        if not dgbas or dgbas[0] not in TARGET_CENTRAL_MFG_2D:
            continue
        addr = normalize_tw_text(g_row.get("公司地址") or "")
        county_g, _ = parse_county_and_district(addr, "")
        if county_g not in CENTRAL_COUNTIES:
            continue
        cap = _pick_cap(g_row)
        raw_records[ban] = {
            "Business_Accounting_NO": ban,
            "Company_Name": (g_row.get("公司名稱") or "").strip(),
            "Responsible_Name": "—",
            "Paid_In_Capital_Amount": cap,
            "Capital_Stock_Amount": cap,
            "Company_Setup_Date": (g_row.get("核准設立日期") or "").strip(),
            "Company_Location": addr,
            "Register_Organization_Desc": "經濟部 GCIS(中部製造業)",
            "Company_Status": "01",
            "_fac_id": "",
            "_fac_prod": "",
            "_fac_codes": [],
            "_dgbas_codes": dgbas,
            "_cap_est": cap <= 0,
        }
        added_gcis_mfg += 1

    # 3b. Attach nationwide factory SIC codes & Taichung commercial DGBAS codes to existing companies
    for ban, nf in nation_fac_by_ban.items():
        rec = raw_records.get(ban)
        if not rec:
            continue
        if nf["codes"] and not rec.get("_fac_codes"):
            rec["_fac_codes"] = nf["codes"]
        if nf["fac_ids"] and not rec.get("_fac_id"):
            rec["_fac_id"] = ",".join(nf["fac_ids"][:3])
        if nf["tag"] and not rec.get("_fac_prod"):
            rec["_fac_prod"] = nf["tag"]
    for ban, b_row in biz_tc_map.items():
        rec = raw_records.get(ban)
        if rec and not rec.get("_dgbas_codes"):
            dgbas_b = _dgbas_list(b_row)
            if dgbas_b:
                rec["_dgbas_codes"] = dgbas_b

    print(
        f"[CENTRAL] Enriched {enriched_existing} existing + added {added_fac} Central active factories "
        f"+ {added_gcis_mfg} GCIS Central DGBAS manufacturers (total pool: {len(raw_records)})"
    )


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
    loc_norm = normalize_tw_text(loc)
    reg_norm = normalize_tw_text(reg_org)
    loc_upper = (loc or "").upper()

    # Explicitly exclude Cayman Islands / foreign domiciles with no Taiwan address
    foreign_tokens = (
        "CAYMAN",
        "SAMOA",
        "VIRGIN ISLANDS",
        "SINGAPORE",
        "RAYONG",
        "THAILAND",
        "MALAYSIA",
        "VIETNAM",
        "CALIFORNIA",
        "U.S.A",
        "UNITED STATES",
        "INDONESIA",
        "HONG KONG",
        "CAMBODIA",
        "PHILIPPINES",
        "GRAND PAVILION",
        "UGLAND HOUSE",
        "NORTH SOUND",
    )
    if (
        any(k in loc_upper for k in foreign_tokens)
        or re.search(r"\b(?:USA|MACAU|JAPAN|CHINA)\b", loc_upper)
    ) and not any(c in loc_norm for c in COUNTY_CENTROIDS):
        return "", ""

    county = ""
    for c in COUNTY_CENTROIDS:
        if c in loc_norm:
            county = c
            break

    loc_nopost = re.sub(r"^[\(\[（【]?\d{3,6}[\)\]）】]?\s*", "", loc_norm)
    if not county:
        if loc_nopost.startswith("北市"):
            county = "臺北市"
        elif loc_nopost.startswith("板橋區"):
            county = "新北市"
        else:
            for city_name, c_name in CAPITAL_CITY_COUNTY.items():
                if city_name in loc_norm:
                    county = c_name
                    break

    if not county:
        if "新竹科學工業園區" in loc_norm or "新竹科學園區" in loc_norm or "新竹科學園區" in reg_norm:
            county = "新竹市"
        elif "中部科學園區" in loc_norm or "中部科學園區" in reg_norm:
            county = "臺中市"
        elif "南部科學園區" in loc_norm or "南部科學園區" in reg_norm:
            county = "臺南市"
        else:
            for c in COUNTY_CENTROIDS:
                if c in reg_norm:
                    county = c
                    break
            if not county:
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
    # 1. County-scoped Chinese district anchors (excluding 新竹科學園區 until specific townships are checked)
    for d in DISTRICT_ANCHORS:
        if d == "新竹科學園區":
            continue
        if county in DISTRICT_COUNTY_MAP.get(d, ()) and d in loc_norm:
            district = d
            break

    if not district and loc_nopost.startswith("板橋區") and county == "新北市":
        district = "板橋區"

    if not district:
        for city_name, c_name in CAPITAL_CITY_COUNTY.items():
            if c_name == county and city_name in loc_norm:
                district = city_name
                break

    if not district:
        m = re.search(rf"{county[0]}.{{1,2}}[市縣]([^\d\s路街段巷弄號樓]{{1,3}}?[區鄉鎮市])", loc_norm)
        if m and m.group(1) not in ("科學園區", "工業區", "軟體園區"):
            district = m.group(1)

    # 2. County-scoped English district patterns
    if not district and loc_upper:
        for pat, dname, allowed_counties in ENG_DISTRICT_PATTERNS:
            if allowed_counties is not None and county not in allowed_counties:
                continue
            if re.search(pat, loc_upper):
                district = dname
                break

    # 3. Only fall back to 新竹科學園區 when county is 新竹市/新竹縣 and no specific township matched
    if not district and county in ("新竹市", "新竹縣"):
        if (
            "新竹科學園區" in loc_norm
            or "新竹科學工業園區" in loc_norm
            or "新竹科學園區" in reg_norm
            or "SCIENCE-BASED INDUSTRIAL PARK" in loc_upper
            or "HSINCHU SCIENCE PARK" in loc_upper
            or bool(re.search(r"\bSBIP", loc_upper))
        ):
            district = "新竹科學園區"

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


def classify_industry(
    name: str,
    reg_org: str = "",
    twse_ind_code: str = "",
    dgbas_codes: list[str] | None = None,
    fac_codes: list[str] | None = None,
    prod_hint: str = "",
) -> str:
    if twse_ind_code:
        if twse_ind_code == "37":
            if any(k in name for k in ("大魯閣", "柏文", "健身", "邁達康")):
                return "餐飲旅宿與生活文創"
            return "金屬電機與精密機械"
        if twse_ind_code == "15":
            if any(k in name for k in ("漢翔", "台船", "造船", "航太", "航空工業", "亞航")):
                return "金屬電機與精密機械"
            return "批發零售與國際貿易"
        if twse_ind_code in TWSE_INDUSTRY_MAP:
            # Refine telecoms (中華電、台灣大、遠傳) & networking/cloud if needed
            if (
                any(k in name for k in ("軟體", "資訊", "數位", "雲端", "人工智慧", "中華電信", "台灣大哥大", "遠傳"))
                or bool(re.search(r"(?<![A-Za-z])AI(?![A-Za-z])", name))
            ):
                return "AI與軟體資訊服務"
            return TWSE_INDUSTRY_MAP[twse_ind_code]

    # Official MOEA Factory & Finance Tax SIC classification (highest reliability for unlisted mfg)
    all_sic = list(fac_codes or []) + list(dgbas_codes or [])
    if all_sic:
        if any(cd in METAL_ELEC_MACH_2D for cd in all_sic):
            return "金屬電機與精密機械"
        if any(cd in ("26", "27") for cd in all_sic):
            return "半導體與電子製造"
        if any(cd in TRAD_MFG_2D for cd in all_sic):
            return "傳統製造工業"
    if prod_hint:
        if any(k in prod_hint for k in ("基本金屬", "金屬製品", "電力設備", "機械設備", "汽車及其零件", "其他運輸工具", "螺絲", "手工具", "刀具", "模具", "工具機", "自行車", "齒輪", "電線", "配線")):
            return "金屬電機與精密機械"
        if any(k in prod_hint for k in ("塑膠", "紡織", "食品", "非金屬", "家具", "木竹", "造紙", "印刷", "橡膠", "化學", "成衣", "皮革")):
            return "傳統製造工業"

    # Unlisted name heuristics: specific sectors BEFORE generic '科技' / '電子'
    if any(k in name for k in ("半導體", "積體電路", "晶圓", "封測", "電路板", "被動元件", "聯電", "聯發", "和碩", "緯創", "緯穎", "台虹", "宏捷", "大立光", "晶相", "記憶體")):
        return "半導體與電子製造"
    if any(k in name for k in ("生技", "生物科技", "生醫", "醫藥", "醫療", "藥業", "製藥", "健康", "疫苗", "基因", "綠能", "太陽能", "能源", "風電", "環保", "儲能", "潔淨", "永續", "水資源", "碳權")):
        return "生技醫療與綠能永續"
    if (
        any(k in name for k in ("軟體", "資訊", "數位", "網路", "雲端", "人工智慧", "系統", "資通", "數據", "電信", "線上", "遊戲", "大哥大", "智邦", "資安", "網通"))
        or bool(re.search(r"(?<![A-Za-z])AI(?![A-Za-z])", name))
    ):
        return "AI與軟體資訊服務"
    if any(k in name for k in ("光電", "光學", "面板", "電腦", "電源", "連接器", "宏碁")):
        return "半導體與電子製造"
    if any(k in name for k in ("金融", "金控", "銀行", "證券", "保險", "投資", "控股", "創投", "資產管理", "租賃", "票券", "期貨", "保經")):
        return "金融控股與投資"
    if any(k in name for k in ("機械", "精密", "工具機", "自動化", "五金", "鋼鐵", "金屬", "模具", "螺絲", "齒輪", "泵", "閥", "軸承", "電機", "鍛造", "鑄造", "沖壓", "自行車", "車床", "航太", "造船", "漢翔", "台船", "巨大", "美利達")):
        return "金屬電機與精密機械"
    if any(k in name for k in ("工業", "塑膠", "化學", "化工", "材料", "紡織", "纖維", "染整", "成衣", "製鞋", "寶成", "奇美", "造紙", "用紙", "水泥", "陶瓷", "玻璃", "木業", "家具", "橡膠", "壓克力")):
        return "傳統製造工業"
    if any(k in name for k in ("營造", "建設", "建築", "不動產", "地產", "工程顧問", "顧問", "開發", "機電")):
        return "營造工程與不動產"
    if any(k in name for k in ("餐飲", "食品", "咖啡", "烘焙", "觀光", "飯店", "旅宿", "旅館", "民宿", "文創", "娛樂", "休閒", "農", "茶", "統一企業")):
        return "餐飲旅宿與生活文創"
    if any(k in name for k in ("海運", "航運", "航空", "物流", "貨運", "倉儲", "通運", "運輸")):
        return "批發零售與國際貿易"
    if any(k in name for k in ("電子", "科技", "通訊", "電通", "元件")):
        return "半導體與電子製造"
    if "科學園區" in reg_org:
        return "半導體與電子製造"
    return "批發零售與國際貿易"


def parse_setup_year(date_str: Any, current_year: int | None = None) -> int | None:
    if current_year is None:
        current_year = time.localtime().tm_year
    s = str(date_str or "").strip()
    digits = re.sub(r"\D", "", s)
    if len(digits) == 8 and digits.startswith(("19", "20")):
        year = int(digits[:4])
        if 1900 <= year <= current_year:
            return year
    if len(digits) in (6, 7):
        roc_y = int(digits[:-4])
        year = 1911 + roc_y
        if 1900 <= year <= current_year:
            return year
    return None


def deterministic_coord(
    ban: str, county: str, district: str, county_polys: dict[str, dict[str, Any]]
) -> tuple[float, float]:
    h = hashlib.sha256(ban.encode("utf-8")).digest()
    rng = random.Random(int.from_bytes(h[:8], "big"))

    if (county, district) in COUNTY_DISTRICT_ANCHORS:
        base_lon, base_lat, spread = COUNTY_DISTRICT_ANCHORS[(county, district)]
    elif district in DISTRICT_ANCHORS and county in DISTRICT_COUNTY_MAP.get(district, ()):
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
    for _ in range(24):
        lon = clon + rng.uniform(-0.008, 0.008)
        lat = clat + rng.uniform(-0.008, 0.008)
        if feat is None or point_in_feature(lon, lat, feat):
            return round(lon, 5), round(lat, 5)
    return round(clon, 5), round(clat, 5)


def _default_short_name(name: str) -> str:
    clean_short = re.sub(
        r"(股份有限公司|有限公司|商業銀行|金融控股|企業團)$", "", name
    ).strip()
    return clean_short[:11] if clean_short else name[:11]


def pack_payload_for_html(payload: dict[str, Any]) -> str:
    """Strip derived/default fields for compact HTML embedding; JS hydrateGcisData restores them."""
    packed_comps: list[dict[str, Any]] = []
    for x in payload["companies"]:
        row: dict[str, Any] = {
            "ban": x["ban"],
            "name": x["name"],
            "capital": x["capital"],
            "cap_tier_i": x["cap_tier_i"],
            "age_tier_i": x["age_tier_i"],
            "county": x["county"],
            "address": x["address"],
            "reg_org": x["reg_org"],
            "cluster": x["cluster"],
            "ind_i": x["ind_i"],
            "lon": x["lon"],
            "lat": x["lat"],
            "pr": x["pr"],
        }
        if x.get("short") and x["short"] != _default_short_name(x["name"]):
            row["short"] = x["short"]
        if x.get("responsible") and x["responsible"] != "—":
            row["responsible"] = x["responsible"]
        if x.get("setup_date"):
            row["setup_date"] = x["setup_date"]
        if x.get("setup_year") is not None:
            row["setup_year"] = x["setup_year"]
        if x.get("district") and x["district"] != x["county"]:
            row["district"] = x["district"]
        if x.get("flagship"):
            row["flagship"] = True
        if x.get("stock") is not None:
            row["stock"] = x["stock"]
        if x.get("prod"):
            row["prod"] = x["prod"]
        if x.get("sic"):
            row["sic"] = x["sic"]
        if x.get("fac_id"):
            row["fac_id"] = x["fac_id"]
        if x.get("cap_est"):
            row["cap_est"] = True
        packed_comps.append(row)
    packed_payload = {
        "meta": payload["meta"],
        "industries": payload["industries"],
        "clusters": payload["clusters"],
        "companies": packed_comps,
    }
    return json.dumps(packed_payload, ensure_ascii=False, separators=(",", ":"))


def sync_payload_to_html(payload: dict[str, Any]) -> None:
    packed_json = pack_payload_for_html(payload)
    pattern = re.compile(r"^window\.__GCIS_DATA__\s*=\s*\{.*?\};[ \t]*$", re.MULTILINE)
    replacement = f"window.__GCIS_DATA__ = {packed_json};"
    for html_path in (POC_HTML, INDEX_HTML):
        if not html_path.exists():
            continue
        text = html_path.read_text(encoding="utf-8")
        if not pattern.search(text):
            continue
        new_text, count = pattern.subn(lambda _: replacement, text, count=1)
        if count:
            html_path.write_text(new_text, encoding="utf-8")
            print(f"[SYNC] Updated window.__GCIS_DATA__ in {html_path} ({len(packed_json)/1e6:.2f} MB payload)")


def main() -> None:
    force_refresh = "--refresh-gcis" in sys.argv
    refresh_central = "--refresh-central" in sys.argv
    skip_central = "--skip-central" in sys.argv
    sync_html = "--sync-html" in sys.argv
    geojson = load_taiwan_geojson()
    county_polys = build_county_poly_index(geojson)
    raw_records, verified_flagships = load_or_collect_gcis_records(force_refresh=force_refresh)
    if not skip_central and (refresh_central or force_refresh or not any(r.get("_fac_prod") for r in raw_records.values())):
        enrich_central_mfg_and_factories(raw_records)
    stocks_by_ban, twse_date = fetch_twse_tpex_stocks()

    # Merge TWSE/TPEx domestic companies that are not yet in GCIS sample
    added_from_twse = 0
    for ban, st in stocks_by_ban.items():
        if ban not in raw_records:
            addr = st["address"]
            reg_org = st["reg_org"]
            county, _ = parse_county_and_district(addr, reg_org)
            if not county and not addr.strip():
                # Fallback to GCIS BAN API when TWSE/TPEx has empty address (e.g. 3158 嘉實)
                flt = urllib.parse.quote(f"Business_Accounting_NO eq {ban}")
                gcis_rows = fetch_json(
                    f"https://data.gcis.nat.gov.tw/od/data/api/{OID_BAN}?$format=json&$filter={flt}"
                )
                if gcis_rows:
                    gr = gcis_rows[0]
                    addr = str(gr.get("Company_Location") or "").strip()
                    reg_org = str(gr.get("Register_Organization_Desc") or reg_org).strip()
                    county, _ = parse_county_and_district(addr, reg_org)
            if not county:
                continue
            raw_records[ban] = {
                "Business_Accounting_NO": ban,
                "Company_Name": st["company_name"],
                "Responsible_Name": st["responsible"],
                "Paid_In_Capital_Amount": st["capital"],
                "Capital_Stock_Amount": st["capital"],
                "Company_Setup_Date": st["setup_date"],
                "Company_Location": addr,
                "Register_Organization_Desc": reg_org,
                "Company_Status": "01",
            }
            added_from_twse += 1
        elif st["capital"] > 0 and int(raw_records[ban].get("Paid_In_Capital_Amount") or 0) <= 0:
            raw_records[ban]["Paid_In_Capital_Amount"] = st["capital"]
            raw_records[ban]["_cap_est"] = False
        # Prefer richer Chinese address if raw_records has empty/English address
        if ban in raw_records and (not raw_records[ban].get("Company_Location")) and st["address"]:
            raw_records[ban]["Company_Location"] = st["address"]

    print(f"[MERGE] Added {added_from_twse} additional domestic TWSE/TPEx listed/OTC companies (total pool: {len(raw_records)})")

    items: list[dict[str, Any]] = []
    current_year = time.localtime().tm_year

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
        cap_est = bool(r.get("_cap_est")) and capital <= 0
        if capital <= 0:
            capital = 0 if cap_est else 100000

        setup_year = parse_setup_year(r.get("Company_Setup_Date"), current_year=current_year)
        age = max(0, current_year - setup_year) if setup_year is not None else None

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

        if age is None:
            age_tier = "穩健中堅 (年代未詳)"
            age_tier_idx = 1
        elif age <= 5:
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
        industry = classify_industry(
            name,
            reg_org,
            twse_ind_code,
            dgbas_codes=r.get("_dgbas_codes"),
            fac_codes=r.get("_fac_codes"),
            prod_hint=str(r.get("_fac_prod") or ""),
        )
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
            short_name = _default_short_name(name)

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

        item: dict[str, Any] = {
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
        if r.get("_fac_prod"):
            item["prod"] = str(r["_fac_prod"])
        if r.get("_fac_id"):
            item["fac_id"] = str(r["_fac_id"])
        sic_list: list[str] = []
        for cd in list(r.get("_fac_codes") or []) + list(r.get("_dgbas_codes") or []) + list(r.get("_sic_codes") or []):
            s2 = str(cd).strip()[:2]
            if s2 in ALL_MFG_SIC_2D and s2 not in sic_list:
                sic_list.append(s2)
        if not sic_list and twse_ind_code in TWSE_TO_MFG_SIC:
            sic_list.append(TWSE_TO_MFG_SIC[twse_ind_code])
        if sic_list:
            item["sic"] = ",".join(sic_list)
        if cap_est:
            item["cap_est"] = True
        items.append(item)

    # Compute capital percentile PR (1..99) strictly over companies with known corporate capital > 0
    sorted_caps = sorted(x["capital"] for x in items if x["capital"] > 0)
    total_known_cap = len(sorted_caps)
    for x in items:
        if x["capital"] <= 0 or total_known_cap == 0:
            x["pr"] = 1
        else:
            rank = bisect.bisect_right(sorted_caps, x["capital"])
            x["pr"] = min(99, max(1, int(round((rank / total_known_cap) * 99))))

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
        recent_5y = sum(1 for x in sub if x["age"] is not None and x["age"] <= 5)
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
            "source": "經濟部 GCIS × 登記工廠名錄 × TWSE & TPEx OpenAPI",
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "data_year": current_year,
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
    compact_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    OUTPUT_JSON.write_text(compact_json, encoding="utf-8")
    if sync_html:
        sync_payload_to_html(payload)
    print(
        f"[DONE] Saved {len(items)} companies ({listed_twse} 上市 + {listed_tpex} 上櫃 = {listed_total} 上市櫃) "
        f"across {payload['meta']['counties_covered']} counties -> {OUTPUT_JSON} ({len(compact_json)/1e6:.2f} MB)"
    )
    for cid, st in cluster_stats.items():
        top_name = st["top_industries"][0]["name"] if st["top_industries"] else "-"
        print(
            f"  - {cid:18s}: {st['count']:4d} total ({st['listed_count']:4d} 上市櫃) "
            f"| median PR={st['median_pr']:2d} | top={top_name}"
        )


if __name__ == "__main__":
    main()
