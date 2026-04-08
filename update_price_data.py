# -*- coding: utf-8 -*-
"""
實價登錄資料更新腳本
=====================
用法：
  python3 update_price_data.py                        # 掃描預設目錄（ZIP 和資料夾都支援）
  python3 update_price_data.py ~/Downloads/20260121.zip   # 指定單一 ZIP 檔
  python3 update_price_data.py ~/Downloads/新批次資料夾    # 指定單一解壓資料夾
  python3 update_price_data.py ~/Downloads/實價登錄        # 掃整個上層目錄（混合 ZIP+資料夾都可）

功能：
  1. 掃描 ~/Downloads/實價登錄/ 下所有 *_opendata 資料夾
  2. 解析臺東（v）不動產買賣 CSV
  3. 與 GCS 上現有資料合併（去除重複，以 id 為唯一鍵）
  4. 上傳更新後的 JSON 回 GCS
  5. 順便備份舊版本到 GCS（price/backups/）

執行前確認：
  - 已設定 GCS_BUCKET 環境變數（或在 ~/Projects/.env 裡）
  - 已安裝 google-cloud-storage（pip3 install google-cloud-storage）
"""

import csv
import json
import os
import glob
import sys
import hashlib
import datetime
import zipfile
import tempfile
import warnings

# 抑制 google-auth 等套件的 Python 3.9 過期警告，讓終端輸出簡潔
warnings.filterwarnings('ignore', category=FutureWarning, module='google.auth')
warnings.filterwarnings('ignore', category=FutureWarning, module='google.oauth2')
warnings.filterwarnings('ignore', category=FutureWarning, module='google.api_core')

# ── 嘗試載入 .env ─────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    _dir = os.path.dirname(os.path.abspath(__file__))
    for p in (os.path.join(_dir, '.env'), os.path.join(_dir, '..', '.env')):
        if os.path.isfile(p):
            load_dotenv(p, override=False)
            break
except Exception:
    pass

GCS_BUCKET = os.environ.get('GCS_BUCKET', '').strip()
PRICE_DATA_GCS_KEY = 'price/price_data_v.json'
BACKUP_GCS_PREFIX = 'price/backups/'
LOCAL_OUTPUT = os.path.join(os.path.dirname(__file__), 'price_data_v.json')

# 縣市代碼（目前只處理臺東）
TARGET_COUNTY = 'v'
COUNTY_NAME = '臺東縣'


# ── 工具函式（與 parse_csv.py 相同）─────────────────────────────────

def roc_to_ad(roc_str):
    s = str(roc_str).strip()
    if len(s) < 7:
        return ''
    try:
        year = int(s[:3]) + 1911
        month = s[3:5]
        day = s[5:7]
        return f'{year}-{month}-{day}'
    except Exception:
        return ''


def sqm_to_ping(sqm_str):
    try:
        v = float(sqm_str)
        return round(v / 3.30579, 2) if v > 0 else 0.0
    except Exception:
        return 0.0


def to_int(s):
    try:
        return int(str(s).strip())
    except Exception:
        return 0


def to_float(s):
    try:
        return float(str(s).strip())
    except Exception:
        return 0.0


def parse_floor(s):
    s = str(s).strip().replace('層', '').replace('第', '').strip()
    mapping = {
        '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
        '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
        '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15,
        '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
        '全': 0, '見使用執照': 0,
    }
    return mapping.get(s, 0)


def parse_land_file(filepath):
    """解析 _land.csv（土地明細），回傳 {移轉編號: {'sect': 段名, 'land_no': 地號}} 取面積最大那筆。"""
    land_map = {}
    try:
        with open(filepath, encoding='utf-8-sig') as f:
            rows = list(csv.reader(f))
    except UnicodeDecodeError:
        with open(filepath, encoding='big5', errors='replace') as f:
            rows = list(csv.reader(f))

    if len(rows) < 3:
        return land_map

    headers = rows[0]
    col = {name: i for i, name in enumerate(headers)}

    for row in rows[2:]:  # 第1列欄位名、第2列英文說明，從第3列開始才是資料
        if len(row) < 8:
            continue
        serial = row[col.get('編號', 0)].strip()
        sect = row[col.get('土地位置', 1)].strip()
        try:
            area = float(row[col.get('土地移轉面積平方公尺', 2)].strip() or 0)
        except Exception:
            area = 0.0
        land_no = row[col.get('地號', 7)].strip()

        if not serial or not land_no:
            continue

        # 同一筆交易可能有多筆地號，保留面積最大的那筆
        if serial not in land_map or area > land_map[serial]['area']:
            land_map[serial] = {'sect': sect, 'land_no': land_no, 'area': area}

    return land_map


def parse_csv_file(filepath, batch_label, land_dict=None):
    """解析單一 CSV 檔，回傳清洗後的紀錄 list。land_dict 為地號對照表（可選）。"""
    records = []
    try:
        with open(filepath, encoding='utf-8-sig') as f:
            rows = list(csv.reader(f))
    except UnicodeDecodeError:
        with open(filepath, encoding='big5', errors='replace') as f:
            rows = list(csv.reader(f))

    if len(rows) < 3:
        return records

    headers = rows[0]
    data_rows = rows[2:]
    col = {name: i for i, name in enumerate(headers)}

    for row in data_rows:
        if len(row) < 10:
            continue

        def get(field, default=''):
            idx = col.get(field, -1)
            if idx < 0 or idx >= len(row):
                return default
            return row[idx].strip()

        address = get('土地位置建物門牌')
        if not address:
            continue

        date_str = roc_to_ad(get('交易年月日'))
        if not date_str:
            continue

        total_price_ntd = to_int(get('總價元'))
        if total_price_ntd <= 0:
            continue

        total_price_wan = round(total_price_ntd / 10000, 1)
        unit_price_sqm = to_float(get('單價元平方公尺'))
        unit_price_ping = round(unit_price_sqm * 3.30579 / 10000, 1) if unit_price_sqm > 0 else 0.0

        building_sqm = to_float(get('建物移轉總面積平方公尺'))
        building_ping = sqm_to_ping(building_sqm)
        main_building_ping = sqm_to_ping(get('主建物面積'))
        land_ping = sqm_to_ping(get('土地移轉總面積平方公尺'))

        complete_date = get('建築完成年月')
        age = 0
        if complete_date and len(complete_date) >= 5:
            complete_ad = roc_to_ad(complete_date[:7] if len(complete_date) >= 7 else complete_date + '01')
            if complete_ad:
                try:
                    age = max(0, int(date_str[:4]) - int(complete_ad[:4]))
                except Exception:
                    pass

        park_price_ntd = to_int(get('車位總價元'))

        uid_src = f"{TARGET_COUNTY}_{address}_{date_str}_{total_price_ntd}"
        uid = hashlib.md5(uid_src.encode()).hexdigest()[:12]

        # 從 _land.csv 對照表取得段名與地號（用長 ID「編號」串接）
        serial_key = get('編號') or get('移轉編號')
        land_info = (land_dict or {}).get(serial_key, {})

        records.append({
            'id': uid,
            'batch': batch_label,
            'county': COUNTY_NAME,
            'county_code': TARGET_COUNTY,
            'district': get('鄉鎮市區'),
            'address': address,
            'transaction_type': get('交易標的'),
            'date': date_str,
            'total_price': total_price_wan,
            'unit_price': unit_price_ping,
            'building_ping': building_ping,
            'main_building_ping': main_building_ping,
            'land_ping': land_ping,
            'building_type': get('建物型態'),
            'main_use': get('主要用途'),
            'material': get('主要建材'),
            'floor': parse_floor(get('移轉層次')),
            'total_floor': parse_floor(get('總樓層數')),
            'age': age,
            'rooms': to_int(get('建物現況格局-房')),
            'halls': to_int(get('建物現況格局-廳')),
            'bathrooms': to_int(get('建物現況格局-衛')),
            'partitioned': get('建物現況格局-隔間'),
            'elevator': get('電梯'),
            'mgmt': get('有無管理組織'),
            'urban_zone': get('都市土地使用分區'),
            'non_urban_zone': get('非都市土地使用分區'),
            'non_urban_use': get('非都市土地使用編定'),
            'park_type': get('車位類別'),
            'park_price': round(park_price_ntd / 10000, 1) if park_price_ntd > 0 else 0,
            'note': get('備註'),
            'serial': serial_key,
            'land_sect': land_info.get('sect', ''),   # 段名，例如「順天段」
            'land_no': land_info.get('land_no', ''),  # 地號（8碼），例如「04260001」
            'lat': None,
            'lng': None,
        })

    return records


# ── GCS 操作 ──────────────────────────────────────────────────────────

def gcs_download(bucket_name, key):
    """從 GCS 下載 JSON，回傳 list。若不存在回傳空 list。"""
    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(key)
        if not blob.exists():
            print(f'  GCS 上尚無舊資料（{key}），將建立新檔。')
            return []
        content = blob.download_as_text(encoding='utf-8')
        return json.loads(content)
    except Exception as e:
        print(f'  ⚠ GCS 下載失敗：{e}')
        return []


def gcs_upload(bucket_name, key, data):
    """上傳 JSON 到 GCS。"""
    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(key)
        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False, indent=2),
            content_type='application/json'
        )
        print(f'  ✓ 已上傳到 GCS：gs://{bucket_name}/{key}')
        return True
    except Exception as e:
        print(f'  ✗ GCS 上傳失敗：{e}')
        return False


def gcs_backup(bucket_name, key):
    """把 GCS 上的現有資料備份到 backups/ 資料夾。"""
    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        src_blob = bucket.blob(key)
        if not src_blob.exists():
            return
        ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_key = f'{BACKUP_GCS_PREFIX}price_data_v_{ts}.json'
        bucket.copy_blob(src_blob, bucket, backup_key)
        print(f'  ✓ 備份舊資料到：gs://{bucket_name}/{backup_key}')
    except Exception as e:
        print(f'  ⚠ 備份失敗（不影響更新）：{e}')


# ── 主程式 ────────────────────────────────────────────────────────────

def find_sources(path):
    """
    輸入路徑可以是：
      - 單一 ZIP 檔
      - 單一解壓後的資料夾（*_opendata）
      - 包含多個 ZIP 或資料夾的上層目錄
    回傳：list of (batch_label, csv_path_or_zip_path, is_zip)
    """
    sources = []

    if os.path.isfile(path) and path.endswith('.zip'):
        # 單一 ZIP
        batch = os.path.basename(path).replace('.zip', '').replace('_opendata', '')
        sources.append((batch, path, True))
    elif os.path.isdir(path) and path.endswith('_opendata'):
        # 單一解壓資料夾
        batch = os.path.basename(path.rstrip('/')).replace('_opendata', '')
        csv_path = os.path.join(path, f'{TARGET_COUNTY}_lvr_land_a.csv')
        if os.path.isfile(csv_path):
            sources.append((batch, csv_path, False))
    else:
        # 上層目錄：掃 ZIP 和資料夾
        for item in sorted(os.listdir(path)):
            full = os.path.join(path, item)
            if item.endswith('.zip'):
                batch = item.replace('.zip', '').replace('_opendata', '')
                sources.append((batch, full, True))
            elif os.path.isdir(full) and item.endswith('_opendata'):
                batch = item.replace('_opendata', '')
                csv_path = os.path.join(full, f'{TARGET_COUNTY}_lvr_land_a.csv')
                if os.path.isfile(csv_path):
                    sources.append((batch, csv_path, False))

    return sources


def parse_source(batch, source_path, is_zip):
    """
    解析單一來源（ZIP 或 CSV 路徑）。
    ZIP 會解壓到暫存目錄，解析完自動清理。
    同時解析 _land.csv 取得地號資訊，串接到主檔紀錄。
    """
    if is_zip:
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(source_path, 'r') as zf:
                zf.extractall(tmpdir)
            # 找臺東買賣主檔（內政部 ZIP 可能是 V_lvr_land_A.csv 大寫）
            want_name = f'{TARGET_COUNTY}_lvr_land_a.csv'
            want_land = f'{TARGET_COUNTY}_lvr_land_a_land.csv'
            csv_path = None
            land_path = None
            for root, dirs, files in os.walk(tmpdir):
                for fname in files:
                    if fname.lower() == want_name.lower():
                        csv_path = os.path.join(root, fname)
                    if fname.lower() == want_land.lower():
                        land_path = os.path.join(root, fname)
            if not csv_path:
                print(f'  ⚠ ZIP 內找不到 {want_name}：{source_path}')
                return []
            land_dict = parse_land_file(land_path) if land_path else {}
            return parse_csv_file(csv_path, batch, land_dict)
    else:
        # source_path 是主檔路徑，同目錄下找 _land.csv
        land_path = source_path.replace('_a.csv', '_a_land.csv')
        land_dict = parse_land_file(land_path) if os.path.isfile(land_path) else {}
        return parse_csv_file(source_path, batch, land_dict)


def main():
    # 搜尋目錄或指定路徑（ZIP / 資料夾 / 上層目錄皆可）
    base_dir = os.path.expanduser('~/Downloads/實價登錄')
    search_path = sys.argv[1] if len(sys.argv) > 1 else base_dir

    sources = find_sources(search_path)
    if not sources:
        print(f'找不到任何 ZIP 或 *_opendata 資料夾：{search_path}')
        sys.exit(1)

    print('=' * 50)
    print('實價登錄資料更新工具')
    print('=' * 50)

    # ── Step 1：解析新批次 CSV ─────────────────────────────────────
    print('\n【Step 1】解析資料...')
    new_records = []
    new_ids = set()

    for batch, source_path, is_zip in sources:
        src_type = 'ZIP' if is_zip else '資料夾'
        records = parse_source(batch, source_path, is_zip)
        for r in records:
            if r['id'] not in new_ids:
                new_ids.add(r['id'])
                new_records.append(r)
        print(f'  {batch}（{src_type}）：{len(records)} 筆')

    print(f'  新批次合計：{len(new_records)} 筆')

    # ── Step 2：載入現有資料（GCS 或本地）────────────────────────
    print('\n【Step 2】載入現有資料...')
    if GCS_BUCKET:
        existing = gcs_download(GCS_BUCKET, PRICE_DATA_GCS_KEY)
        print(f'  GCS 現有資料：{len(existing)} 筆')
    elif os.path.isfile(LOCAL_OUTPUT):
        with open(LOCAL_OUTPUT, encoding='utf-8') as f:
            existing = json.load(f)
        print(f'  本地現有資料：{len(existing)} 筆')
    else:
        existing = []
        print('  尚無現有資料，建立新檔。')

    # ── Step 3：合併（新資料優先，舊資料補充）────────────────────
    print('\n【Step 3】合併資料（去除重複）...')
    merged = {r['id']: r for r in existing}  # 以 id 為 key 建立 dict
    added = 0
    updated = 0
    for r in new_records:
        if r['id'] not in merged:
            merged[r['id']] = r
            added += 1
        else:
            # 新資料覆蓋舊資料；保留已補好的座標，但 land_sect/land_no 優先用新版（可能舊版沒有）
            old = merged[r['id']]
            # 保留已有座標（不覆蓋補好的 lat/lng）
            if r.get('lat') is None and old.get('lat') is not None:
                r['lat'] = old['lat']
                r['lng'] = old['lng']
            # land_sect/land_no：新資料有就用新的，新資料沒有才保留舊的
            if not r.get('land_sect') and old.get('land_sect'):
                r['land_sect'] = old['land_sect']
                r['land_no'] = old['land_no']
            if old != r:  # 有實質變化才算更新
                merged[r['id']] = r
                updated += 1

    final = sorted(merged.values(), key=lambda x: x.get('date', ''), reverse=True)
    print(f'  新增：{added} 筆，更新：{updated} 筆，合計：{len(final)} 筆')

    # ── Step 4：儲存 ──────────────────────────────────────────────
    print('\n【Step 4】儲存資料...')

    # 先備份舊版本
    if GCS_BUCKET and existing:
        gcs_backup(GCS_BUCKET, PRICE_DATA_GCS_KEY)

    # 存本地
    with open(LOCAL_OUTPUT, 'w', encoding='utf-8') as f:
        json.dump(final, f, ensure_ascii=False, indent=2)
    size_kb = os.path.getsize(LOCAL_OUTPUT) / 1024
    print(f'  ✓ 本地儲存：{LOCAL_OUTPUT}（{size_kb:.1f} KB）')

    # 上傳 GCS
    if GCS_BUCKET:
        gcs_upload(GCS_BUCKET, PRICE_DATA_GCS_KEY, final)
    else:
        print('  ⚠ 未設定 GCS_BUCKET，僅儲存本地。')
        print('  提示：設定環境變數 GCS_BUCKET=real-estate-survey-data-0393195862 後再執行即可同步到雲端。')

    # ── Step 5：統計摘要 ──────────────────────────────────────────
    print('\n【Step 5】資料統計摘要')
    districts = {}
    types = {}
    batches = set()
    for r in final:
        d = r.get('district', '不明')
        districts[d] = districts.get(d, 0) + 1
        t = r.get('transaction_type', '其他')
        types[t] = types.get(t, 0) + 1
        batches.add(r.get('batch', ''))

    print(f'  總筆數：{len(final)} 筆')
    print(f'  批次數：{len(batches)} 期（{min(batches)} ～ {max(batches)}）')
    print('  鄉鎮分布（前5）：')
    for k, v in sorted(districts.items(), key=lambda x: -x[1])[:5]:
        print(f'    {k}: {v} 筆')
    print('  交易類型：')
    for k, v in sorted(types.items(), key=lambda x: -x[1]):
        print(f'    {k}: {v} 筆')

    print('\n✅ 更新完成！')
    if GCS_BUCKET:
        print('   Cloud Run 服務將在最多 1 小時內自動載入新資料。')
        print('   若要立即生效，可在 Cloud Run 主控台點「修訂版本」重新部署。')


if __name__ == '__main__':
    main()
