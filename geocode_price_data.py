# -*- coding: utf-8 -*-
"""
實價登錄資料補座標腳本
========================
使用 Google Geocoding API 對每筆紀錄的 address 解析經緯度，寫回 lat/lng。
同一地址只會呼叫一次 API（快取），已有座標的紀錄會略過。

用法：
  python3 geocode_price_data.py              # 從本地 price_data_v.json 讀取，處理後寫回並上傳 GCS
  python3 geocode_price_data.py --dry-run    # 只顯示會處理幾筆、不寫檔不上傳

環境變數：
  GOOGLE_MAPS_API_KEY  必填，Google Geocoding API 用（與 Survey 相同）
  GCS_BUCKET           選填，有則上傳更新後的 JSON 到 GCS
"""

import os
import sys
import json
import time
import urllib.parse
import urllib.request

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
API_KEY = (os.environ.get('GOOGLE_MAPS_API_KEY') or os.environ.get('GOOGLE_MAPS_API_KEY_002') or os.environ.get('GOOGLE_MAPS_API_KEY_001') or '').strip()
LOCAL_PATH = os.path.join(os.path.dirname(__file__), 'price_data_v.json')
PRICE_DATA_GCS_KEY = 'price/price_data_v.json'
# 每秒最多約 5 次請求，避免超過 Quota
DELAY_SEC = 0.22


def geocode_address(address, api_key):
    """
    呼叫 Google Geocoding API，回傳 (lat, lng) 或 (None, None)。
    """
    if not address or not api_key:
        return None, None
    # 台灣地址可加「台灣」提高準確度
    q = address if '台灣' in address or '臺東' in address else ('台灣' + address)
    url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + urllib.parse.quote(q) + '&key=' + api_key
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        print(f'  ⚠ Geocoding 請求失敗：{address[:30]}... → {e}')
        return None, None
    if data.get('status') != 'OK' or not data.get('results'):
        return None, None
    loc = data['results'][0]['geometry']['location']
    return loc.get('lat'), loc.get('lng')


def main():
    dry_run = '--dry-run' in sys.argv
    if not API_KEY:
        print('錯誤：請設定環境變數 GOOGLE_MAPS_API_KEY')
        sys.exit(1)
    if not os.path.isfile(LOCAL_PATH):
        print(f'錯誤：找不到 {LOCAL_PATH}，請先執行 update_price_data.py 產生資料')
        sys.exit(1)

    with open(LOCAL_PATH, encoding='utf-8') as f:
        records = json.load(f)

    # 依地址快取，避免重複呼叫 API
    cache = {}
    need_geocode = [r for r in records if r.get('lat') is None and r.get('lng') is None and (r.get('address') or '').strip()]
    unique_addresses = list(dict.fromkeys([r.get('address', '').strip() for r in need_geocode]))
    total_calls = len(unique_addresses)

    print(f'總筆數：{len(records)}')
    print(f'已有座標：{len(records) - len(need_geocode)} 筆')
    print(f'待補座標：{len(need_geocode)} 筆（不重複地址 {total_calls} 個）')
    if dry_run:
        print('--dry-run：不執行 Geocoding、不寫檔、不上傳')
        return

    if total_calls == 0:
        print('無需補座標，結束。')
        if GCS_BUCKET:
            print('若需重新上傳 GCS，請執行 update_price_data.py')
        return

    print(f'開始 Geocoding（每次請求間隔 {DELAY_SEC}s）...')
    updated = 0
    for i, addr in enumerate(unique_addresses):
        if addr in cache:
            lat, lng = cache[addr]
        else:
            lat, lng = geocode_address(addr, API_KEY)
            cache[addr] = (lat, lng)
            time.sleep(DELAY_SEC)
        if (i + 1) % 50 == 0:
            print(f'  已處理 {i + 1}/{total_calls} 個地址')

    # 寫回每筆紀錄
    for r in records:
        addr = (r.get('address') or '').strip()
        if r.get('lat') is not None and r.get('lng') is not None:
            continue
        if addr in cache:
            lat, lng = cache[addr]
            if lat is not None and lng is not None:
                r['lat'] = lat
                r['lng'] = lng
                updated += 1

    print(f'已寫入座標：{updated} 筆')

    with open(LOCAL_PATH, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f'已儲存：{LOCAL_PATH}')

    if GCS_BUCKET:
        try:
            from google.cloud import storage
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET)
            blob = bucket.blob(PRICE_DATA_GCS_KEY)
            blob.upload_from_string(
                json.dumps(records, ensure_ascii=False, indent=2),
                content_type='application/json'
            )
            print(f'已上傳 GCS：gs://{GCS_BUCKET}/{PRICE_DATA_GCS_KEY}')
        except Exception as e:
            print(f'⚠ GCS 上傳失敗：{e}')
    else:
        print('未設定 GCS_BUCKET，未上傳雲端。')

    print('✅ 完成。Cloud Run 最多 1 小時內會載入新資料。')


if __name__ == '__main__':
    main()
