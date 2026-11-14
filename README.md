# 全臺商工登記 (GCIS) × 台股上市櫃行情 × Claude Haiku 5.5 3D AI 飛越導覽

結合 **經濟部商工登記 Open Data API (GCIS)**、**臺灣證券交易所 TWSE / 櫃買中心 TPEx 上市櫃股票行情** 與 **OpenRouter Claude Haiku 5.5 (`reasoning: { enabled: true }`)** 的互動式 3D 臺灣產業聚落視覺化專題。

## 主要功能

- **全臺 6,478 家活躍企業 3D 地景**：涵蓋 22 縣市與 9 大產業聚落，支援 **Map（全臺聚落）**、**Industry（8 大產業切片）**、**Capital（資本額階層）** 與 **Age（企業世代）** 四種 3D 切換視圖。
- **1,928 家 TWSE 上市 / TPEx 上櫃即時行情整合**：依統一編號合併上市 1,044 家與上櫃 884 家股票代碼、收盤價、漲跌幅（紅漲綠跌）、本益比 (P/E)、殖利率、股價淨值比 (P/B)，支援點擊「📈 上市櫃」一鍵篩選及單檔即時報價更新。
- **AI 產業與股價 3D 導演（自帶 OpenRouter Key）**：輸入您自己的 OpenRouter API Key（僅儲存於使用者瀏覽器 `localStorage`，本儲存庫不含任何金鑰）即可啟用 `anthropic/claude-haiku-5.5` 進行多輪連續思考（完整保留 `reasoning_details`），依自然語言問題自動規劃分鏡、3D 飛越至目標聚落或企業並彈出工商＋股價卡片與語音字幕導覽；未設定 Key 時亦可使用內建的本機智慧檢索分鏡功能。

## 線上體驗

- GitHub Pages 首頁：`https://nutcelvischen-jpg.github.io/taiwan-gcis-stock-3d-ai/`

## 更新資料集

```bash
python3 scripts/fetch_gcis_poc_data.py
```

（加上 `--refresh-gcis` 可重新向經濟部 GCIS API 拉取商工登記資料並重新整合上市櫃每日行情。）
