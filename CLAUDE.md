# 實價登錄調查 — 專案規則

## 專案概述
查詢臺東縣不動產買賣成交紀錄。資料來源為內政部實價登錄開放資料（CSV），解析後存入 GCS，Cloud Run 服務從 GCS 讀取並提供搜尋/篩選/地圖顯示。

## 專案結構
```
real-estate-price/
├── app.py                  # Flask 後端（所有 API）
├── static/index.html       # 前端 UI（Leaflet 地圖 + 篩選面板）
├── update_price_data.py    # 本機更新腳本：解析 ZIP/CSV → 合併 → 上傳 GCS
├── parse_csv.py            # 舊版本機解析腳本（全台，備用）
├── Dockerfile
├── requirements.txt
├── .gitignore              # 含 price_data_v.json、price_data_tw.json
└── .dockerignore           # 同 .gitignore，避免大 JSON 打進 image
```

## 資料更新流程（本機操作）
```
1. 從內政部網站下載實價登錄 ZIP（臺東縣 v_lvr_land_a.csv）
   → https://lvr.land.moi.gov.tw/ → 不動產成交案件實際資訊資料供下載
2. 存到 ~/Downloads/實價登錄/

3. 執行更新腳本（需設 GCS_BUCKET 環境變數）：
   python3 update_price_data.py
   或指定路徑：
   python3 update_price_data.py ~/Downloads/實價登錄/20260121.zip

4. 腳本自動：解析 CSV → 合併舊資料（去重） → 備份舊版 → 上傳 GCS
5. Cloud Run 快取 1 小時，最多 1 小時後生效；若要立即更新可重新部署
```

## GCS 儲存路徑
- 主資料：`price/price_data_v.json`（臺東縣所有成交）
- 備份：`price/backups/price_data_v_YYYYMMDD_HHMMSS.json`
- GCS bucket：`real-estate-survey-data-0393195862`（與 Survey 共用 bucket）

## 核心 API 端點
| 端點 | 方法 | 用途 |
|------|------|------|
| `/` | GET | 未登入 → 導到 Portal；已登入 → 顯示 index.html |
| `/auth/portal-login` | GET/POST | Portal SSO token 驗證，建立 session |
| `/api/config` | GET | 回傳 `portal_url` 給前端 |
| `/api/me` | GET | 目前登入者 email/name |
| `/api/search` | POST | 查詢成交紀錄（keyword/district/type/price/ping/sort） |

## 資料欄位（JSON 格式）
| 欄位 | 說明 |
|------|------|
| `id` | md5 唯一 ID（地址+日期+總價） |
| `address` | 土地位置建物門牌 |
| `district` | 鄉鎮市區 |
| `transaction_type` | 交易標的（土地、建物+土地等） |
| `date` | 交易日期（西元 YYYY-MM-DD） |
| `total_price` | 總價（萬元） |
| `unit_price` | 單價（萬/坪） |
| `building_ping` | 建物移轉總坪數 |
| `building_type` | 建物型態 |
| `age` | 屋齡（年） |
| `lat`, `lng` | 座標（目前為 null，未來可加地理編碼） |

## 環境變數
| 變數 | 說明 |
|------|------|
| `FLASK_SECRET_KEY` | Session 加密金鑰 |
| `PORTAL_URL` | Portal 網址（登入跳轉用） |
| `GCS_BUCKET` | GCS bucket 名稱（**必須設定**，否則讀不到 GCS 資料） |
| `ADMIN_EMAILS` | 管理員 email（逗號分隔） |

## 部署
- **Cloud Run**：`gcloud run deploy real-estate-price --source . --region asia-east1 --allow-unauthenticated`
- **GitHub**：`a0911190009/real-estate-price`
- 透過 `sync-to-cloud-and-github.sh` 自動部署（已包含在腳本的 7 個工具列表中）

## 重要注意事項
- **`price_data_v.json` 不進 git / Docker**：已加進 `.gitignore` 和 `.dockerignore`，資料只放 GCS
- **前端登入順序**：先等 `/api/config` 回來才呼叫 `/api/me`，避免 race condition 跳錯 URL
- **GCS_BUCKET 必須部署時帶入**：`deploy_price()` 已補上 `GCS_BUCKET=$GCS_BUCKET`
- **ZIP 檔名大小寫**：內政部 ZIP 內為 `V_lvr_land_A.csv`（大寫），`update_price_data.py` 已用 `.lower()` 比對
- **資料快取 1 小時**：`_load_price_data()` 有快取機制，更新 GCS 後最多等 1 小時生效

## 合作習慣
- 修改完後直接執行 `cd ~/Projects && ./sync-to-cloud-and-github.sh "說明"` 部署
- 資料更新（新批次 ZIP）只需在本機執行 `update_price_data.py`，不需重新部署服務
