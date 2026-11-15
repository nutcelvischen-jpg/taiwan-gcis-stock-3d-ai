# 全臺商工登記 (GCIS) × 台股上市櫃行情 × Claude Haiku 5.5 3D AI 飛越導覽

結合 **經濟部商工登記 Open Data API (GCIS)**、**經濟部登記工廠名錄**、**臺灣證券交易所 TWSE / 櫃買中心 TPEx 上市櫃股票行情** 與 **OpenRouter Claude Haiku 5.5 (`reasoning: { enabled: true }`)** 的互動式 3D 臺灣產業聚落視覺化專題。

## 主要功能

- **47,427 家企業 3D 地景**：涵蓋 22 縣市與 9 大產業聚落，支援 **Map（全臺聚落）**、**Industry（產業切片）**、**Capital（資本額階層）** 與 **Age（企業世代）** 四種 3D 切換視圖。
- **🧩 資料圖層**：三個來源互不重疊，可個別開關：

  | 圖層 | 家數 | 涵蓋範圍 |
  |---|---:|---|
  | 上市櫃公司 | 1,951 | 全臺 TWSE 上市 1,065・TPEx 上櫃 886，附行情 |
  | 全臺商工登記樣本 | 4,107 | 經濟部 GCIS 商工登記抽樣，22 縣市 |
  | 中部製造業名錄 | 41,369 | 工廠登記＋C 製造業商業登記，99% 位於中彰投苗雲 |

  中部製造業名錄數量遠多於其他來源，比較全臺分布或資本額 PR 時，建議用「只看全臺可比較資料」關閉此圖層。
- **上市櫃行情整合**：依統一編號合併股票代碼、收盤價、漲跌幅（紅漲綠跌）、本益比 (P/E)、殖利率、股價淨值比 (P/B)，行情日期標示於頁面；公司卡可單檔即時更新報價。
- **搜尋自動完成**：輸入公司名稱、股票代號或統一編號，下拉清單以 ↑↓ 選擇、Enter 直接飛越並開啟公司卡。
- **AI 產業與股價 3D 導演（自帶 OpenRouter Key）**：輸入您自己的 OpenRouter API Key（僅儲存於使用者瀏覽器 `localStorage`，本儲存庫不含任何金鑰）即可啟用 `anthropic/claude-haiku-5.5` 進行多輪連續思考（完整保留 `reasoning_details`），依自然語言問題自動規劃分鏡、3D 飛越至目標聚落或企業並彈出工商＋股價卡片與語音字幕導覽；未設定 Key 時亦可使用內建的本機智慧檢索分鏡功能。
- **手機版**：地圖優先，AI 導演與產業報告預設收合，報告以底部抽屜開啟。
- **🔗 分享視角**：網址會記錄目前的聚落、縣市、維度、圖層與選取的公司（例：`#c=hsinchu_park&m=capital&ban=22099131`），按「🔗 分享」即可複製連結。
- **新手導覽**：第一次開啟時有 4 步驟說明，之後可按「？」重看。

## 線上體驗

- GitHub Pages 首頁：`https://nutcelvischen-jpg.github.io/taiwan-gcis-stock-3d-ai/`

## 本機開啟

`index.html` 會另外載入 `data/gcis_data.js`，兩者需放在一起。可直接雙擊開啟，或在專案資料夾執行：

```bash
python3 -m http.server 8000   # 之後開啟 http://localhost:8000
```

## 檔案結構

| 檔案 | 說明 |
|---|---|
| `index.html` | 網頁本體（約 0.9 MB，含 three.js 與縣市邊界） |
| `data/gcis_data.js` | 企業資料（欄位式壓縮格式，約 12 MB，gzip 後約 2.3 MB） |
| `sample_data/gcis_industry_poc_data.json` | 完整原始資料（資料腳本輸出） |
| `scripts/fetch_gcis_poc_data.py` | 資料抓取與整合腳本 |

## 更新資料集

```bash
python3 scripts/fetch_gcis_poc_data.py --sync-html
```

- `--sync-html`：輸出 `data/gcis_data.js`，並更新 `index.html` 內的快取版本號。
- `--refresh-gcis`：重新向經濟部 GCIS API 拉取商工登記資料，並重新整合上市櫃每日行情。
- `--pack-only`：不連網，只把現有的 `sample_data/gcis_industry_poc_data.json` 重新打包成 `data/gcis_data.js`。
