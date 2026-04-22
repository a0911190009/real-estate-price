# -*- coding: utf-8 -*-
"""
實價登錄資料補座標腳本（地號版）
=================================
使用 Easymap（內政部地籍圖資）透過地號查詢 WGS84 座標，
比 Google Geocoding 更精準，且完全免費。

流程：
  1. 讀取 GCS（或本地）的 price_data_v.json
  2. 找出 lat/lng 為 null 且有 land_sect + land_no 的紀錄
  3. 以 (鄉鎮, 段名, 地號) 為 key，相同的只查一次（快取）
  4. 批次呼叫 Easymap，每筆間隔 1 秒避免被封
  5. 寫回 JSON 並上傳 GCS

用法：
  python3 geocode_price_data.py              # 正常執行
  python3 geocode_price_data.py --dry-run    # 只顯示待處理筆數，不查詢

環境變數：
  GCS_BUCKET  選填，有則從 GCS 讀寫；無則只處理本地 price_data_v.json
"""

import os
import sys
import json
import time
import re
import requests

# ── 載入 .env ─────────────────────────────────────────────────────────
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
LOCAL_PATH = os.path.join(os.path.dirname(__file__), 'price_data_v.json')
PRICE_DATA_GCS_KEY = 'price/price_data_v.json'
DELAY_SEC = 1.0  # 每筆查詢間隔，避免觸發 Easymap 封鎖


# ── Easymap 爬蟲 ──────────────────────────────────────────────────────

class EasymapCrawler:
    HOST = 'https://easymap.moi.gov.tw'

    def __init__(self):
        self.session = requests.Session()
        self.token = ''
        # 預載縣市 / 鄉鎮 / 段清單，避免每筆查詢都重複呼叫
        self._city_cache = None
        self._town_cache = {}    # {city_id: [town_list]}
        self._sect_cache = {}    # {city_id_town_id: [sect_list]}

    def init(self):
        """取得 session cookie 和 anti-bot token，每次呼叫前必須執行。"""
        self.session = requests.Session()
        self.session.get(f'{self.HOST}/Z10Web/', timeout=10)
        res = self.session.post(f'{self.HOST}/Z10Web/layout/setToken.jsp', timeout=10)
        match = re.search(r'name="token"\s+value=["\']([^"\']+)["\']', res.text)
        if not match:
            raise Exception('無法取得 Easymap token，可能被暫時封鎖')
        self.token = match.group(1)
        # 重置快取（token 換了，舊快取可能失效）
        self._city_cache = None
        self._town_cache = {}
        self._sect_cache = {}

    def _post(self, endpoint, params=None):
        """帶 token 的 POST 請求。"""
        data = {'struts.token.name': 'token', 'token': self.token}
        if params:
            data.update(params)
        res = self.session.post(f'{self.HOST}/Z10Web/{endpoint}', data=data, timeout=10)
        return res.json()

    @staticmethod
    def _norm(s):
        """台 → 臺，統一化縣市名稱。"""
        return s.replace('台', '臺')

    def _get_city(self, county):
        """查縣市清單，回傳符合 county 的 city dict。"""
        if self._city_cache is None:
            self._city_cache = self._post('City_json_getList')
        norm = self._norm(county)
        return next((c for c in self._city_cache if self._norm(c['name']) == norm), None)

    def _get_town(self, city_id, town):
        """查鄉鎮清單，回傳符合 town 的 town dict。"""
        if city_id not in self._town_cache:
            self._town_cache[city_id] = self._post('City_json_getTownList', {'cityCode': city_id})
        norm = self._norm(town)
        return next((t for t in self._town_cache[city_id] if self._norm(t['name']) == norm), None)

    def _get_sect(self, city_id, town_id, sect):
        """查段清單，回傳符合 sect 的 sect dict。"""
        key = f'{city_id}_{town_id}'
        if key not in self._sect_cache:
            self._sect_cache[key] = self._post('City_json_getSectionList',
                                                {'cityCode': city_id, 'townCode': town_id})
        return next((s for s in self._sect_cache[key] if s['name'] == sect), None)

    def get_coordinates(self, county, town, sect, land_no):
        """
        透過地號查詢 WGS84 座標。
        回傳 {'lat': float, 'lng': float} 或 None（查不到時）。
        """
        try:
            city = self._get_city(county)
            if not city:
                return None

            t = self._get_town(city['id'], town)
            if not t:
                return None

            s = self._get_sect(city['id'], t['id'], sect)
            if not s:
                return None

            res = self._post('Land_json_locate', {
                'sectNo': s['id'],
                'office': s['officeCode'],
                'landNo': land_no,
            })
            if res and res.get('X') and res.get('Y'):
                x, y = float(res['X']), float(res['Y'])
                # 若 Y > 1000 表示是 TWD97 投影座標（公尺），需轉換
                # 若 Y 在 20~26 之間則已是 WGS84 度數，直接使用
                if y < 100:
                    return {'lat': y, 'lng': x}
                else:
                    # TWD97 TM2 → WGS84 簡易轉換（台灣範圍內誤差 < 1 公尺）
                    lat, lng = _twd97_to_wgs84(x, y)
                    return {'lat': lat, 'lng': lng}
        except Exception as e:
            print(f'    ⚠ Easymap 查詢失敗：{e}')
        return None


def _twd97_to_wgs84(x, y):
    """
    TWD97 TM2 投影座標（公尺）→ WGS84 經緯度（度）。
    台灣中央子午線 121°，原點緯度 0°，E 加 250000m。
    """
    import math
    a = 6378137.0
    b = 6356752.3142
    e2 = (a**2 - b**2) / a**2
    k0 = 0.9999
    dx = 250000.0
    lon0 = math.radians(121)

    x -= dx
    M = y / k0
    mu = M / (a * (1 - e2/4 - 3*e2**2/64 - 5*e2**3/256))
    e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
    phi1 = mu + (3*e1/2 - 27*e1**3/32)*math.sin(2*mu) \
               + (21*e1**2/16 - 55*e1**4/32)*math.sin(4*mu) \
               + (151*e1**3/96)*math.sin(6*mu)
    N1 = a / math.sqrt(1 - e2 * math.sin(phi1)**2)
    T1 = math.tan(phi1)**2
    C1 = e2 / (1 - e2) * math.cos(phi1)**2
    R1 = a * (1 - e2) / (1 - e2 * math.sin(phi1)**2)**1.5
    D = x / (N1 * k0)
    lat = phi1 - (N1 * math.tan(phi1) / R1) * (
        D**2/2 - (5 + 3*T1 + 10*C1 - 4*C1**2 - 9*e2/(1-e2)) * D**4/24
        + (61 + 90*T1 + 298*C1 + 45*T1**2 - 252*e2/(1-e2) - 3*C1**2) * D**6/720
    )
    lng = lon0 + (D - (1 + 2*T1 + C1)*D**3/6
                    + (5 - 2*C1 + 28*T1 - 3*C1**2 + 8*e2/(1-e2) + 24*T1**2)*D**5/120) / math.cos(phi1)
    return round(math.degrees(lat), 8), round(math.degrees(lng), 8)


# ── GCS 工具 ──────────────────────────────────────────────────────────

def gcs_download(bucket_name, key):
    try:
        from google.cloud import storage
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(key)
        if not blob.exists():
            return []
        return json.loads(blob.download_as_text(encoding='utf-8'))
    except Exception as e:
        print(f'⚠ GCS 下載失敗：{e}')
        return []


def gcs_upload(bucket_name, key, data):
    try:
        from google.cloud import storage
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(key)
        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False, indent=2),
            content_type='application/json'
        )
        print(f'✓ 已上傳 GCS：gs://{bucket_name}/{key}')
    except Exception as e:
        print(f'⚠ GCS 上傳失敗：{e}')


# ── 地址解析工具 ──────────────────────────────────────────────────────

def _extract_land_from_addr(address):
    """
    從地址文字抽取段名和地號。
    「南清段226地號」→ ('南清段', '226')
    「豐榮段1032-3地號」→ ('豐榮段', '1032-3')
    若格式不符回傳 (None, None)。
    """
    m = re.match(r'^(.+?段)\s*(\d+(?:-\d+)?)\s*地號', address or '')
    if m:
        return m.group(1), m.group(2)
    return None, None


def _fmt_land_no(raw_no):
    """
    將地址文字中的地號轉成 Easymap 使用的 8 碼格式。
    '226'   → '02260000'  (主號 226，附號 0)
    '226-3' → '02260003'  (主號 226，附號 3)
    """
    if '-' in str(raw_no):
        parts = raw_no.split('-')
        main = int(parts[0])
        sub  = int(parts[1]) if len(parts) > 1 else 0
    else:
        main = int(raw_no)
        sub  = 0
    return f'{main:04d}{sub:04d}'


def _nominatim_geocode(address):
    """
    用 Nominatim（OpenStreetMap，免費無需 API key）查門牌地址座標。
    回傳 {'lat': float, 'lng': float} 或 None。
    """
    url = 'https://nominatim.openstreetmap.org/search'
    params = {
        'q': address,
        'format': 'json',
        'limit': 1,
        'countrycodes': 'tw',
    }
    headers = {'User-Agent': 'real-estate-price-geocoder/1.0'}
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        results = resp.json()
        if results:
            return {'lat': float(results[0]['lat']), 'lng': float(results[0]['lon'])}
    except Exception as e:
        print(f'    ⚠ Nominatim 查詢失敗：{e}')
    return None


# ── 主程式 ────────────────────────────────────────────────────────────

def main():
    dry_run  = '--dry-run' in sys.argv
    addr_mode = '--addr' in sys.argv   # 門牌地址模式（Nominatim）

    # 讀取資料
    if GCS_BUCKET:
        print('從 GCS 讀取資料...')
        records = gcs_download(GCS_BUCKET, PRICE_DATA_GCS_KEY)
    elif os.path.isfile(LOCAL_PATH):
        with open(LOCAL_PATH, encoding='utf-8') as f:
            records = json.load(f)
    else:
        print(f'錯誤：找不到 {LOCAL_PATH}，請先執行 update_price_data.py')
        sys.exit(1)

    if addr_mode:
        _run_addr_mode(records, dry_run)
    else:
        _run_land_mode(records, dry_run)


def _run_land_mode(records, dry_run):
    """
    Phase 1：地號模式（Easymap）。
    同時處理：
      (a) land_sect + land_no 已有欄位的紀錄
      (b) address 欄位內嵌地號文字（如「南清段226地號」）的紀錄
    """
    need = []
    for r in records:
        if r.get('lat') is not None:
            continue
        if r.get('land_sect', '').strip() and r.get('land_no', '').strip():
            # 欄位已有地號資料，直接加入
            need.append(r)
        else:
            # 嘗試從 address 文字抽取地號
            sect, raw_no = _extract_land_from_addr(r.get('address', ''))
            if sect and raw_no:
                r['_addr_sect']    = sect
                r['_addr_land_no'] = _fmt_land_no(raw_no)
                need.append(r)

    print(f'總筆數：{len(records)}')
    print(f'已有座標：{sum(1 for r in records if r.get("lat") is not None)} 筆（略過）')
    print(f'待補座標（地號）：{len(need)} 筆')
    addr_embedded = sum(1 for r in need if r.get('_addr_sect'))
    if addr_embedded:
        print(f'  其中從 address 欄位抽取地號：{addr_embedded} 筆')

    if dry_run:
        print('--dry-run：不執行查詢')
        keys = set()
        for r in need:
            sect    = r.get('land_sect') or r.get('_addr_sect', '')
            land_no = r.get('land_no')   or r.get('_addr_land_no', '')
            keys.add((r.get('district', ''), sect, land_no))
        print(f'不重複查詢組合：{len(keys)} 個')
        print(f'預估耗時：約 {len(keys) * DELAY_SEC / 60:.1f} 分鐘')
        return

    if not need:
        print('無需補座標，結束。')
        _save(records)
        return

    crawler = EasymapCrawler()
    print('\n初始化 Easymap session...')
    crawler.init()

    coord_cache = {}
    updated = 0
    failed  = 0

    print(f'開始補座標（每筆間隔 {DELAY_SEC}s）...\n')
    for i, r in enumerate(need):
        district = r.get('district', '')
        sect     = r.get('land_sect') or r.get('_addr_sect', '')
        land_no  = r.get('land_no')   or r.get('_addr_land_no', '')
        cache_key = (district, sect, land_no)

        if cache_key not in coord_cache:
            time.sleep(DELAY_SEC)
            coords = crawler.get_coordinates('臺東縣', district, sect, land_no)
            coord_cache[cache_key] = coords
            status = f'lat={coords["lat"]:.5f}, lng={coords["lng"]:.5f}' if coords else '查無座標'
            suffix = '（從 address 抽取）' if r.get('_addr_sect') else ''
            print(f'  [{i+1}/{len(need)}] {district} {sect} {land_no} → {status}{suffix}')
        else:
            coords = coord_cache[cache_key]

        if coords:
            r['lat'] = coords['lat']
            r['lng'] = coords['lng']
            # 若是從 address 抽取到的，順便補回欄位
            if r.get('_addr_sect'):
                r['land_sect'] = r.pop('_addr_sect')
                r['land_no']   = r.pop('_addr_land_no')
            updated += 1
        else:
            # 清除暫存欄位
            r.pop('_addr_sect', None)
            r.pop('_addr_land_no', None)
            failed += 1

        if (i + 1) % 50 == 0:
            print(f'\n  重新初始化 Easymap session（第 {i+1} 筆）...')
            try:
                crawler.init()
            except Exception as e:
                print(f'  ⚠ 重新初始化失敗：{e}，繼續使用舊 session')

    print(f'\n已補座標：{updated} 筆，查無結果：{failed} 筆')
    _save(records)


def _run_addr_mode(records, dry_run):
    """
    Phase 2：門牌地址模式（Nominatim，OpenStreetMap，免費）。
    用法：python3 geocode_price_data.py --addr
    處理仍無座標且地址含門牌號碼（含「號」）的紀錄。
    注意：Nominatim 每秒 1 次限制，與 DELAY_SEC 相同。
    """
    need = [r for r in records
            if r.get('lat') is None
            and re.search(r'\d+號', r.get('address', ''))]

    print(f'總筆數：{len(records)}')
    print(f'已有座標：{sum(1 for r in records if r.get("lat") is not None)} 筆（略過）')
    print(f'待補座標（門牌地址）：{len(need)} 筆')

    if dry_run:
        print('--dry-run：不執行查詢')
        print(f'預估耗時：約 {len(need) * DELAY_SEC / 60:.1f} 分鐘')
        return

    if not need:
        print('無需補門牌座標，結束。')
        return

    updated = 0
    failed  = 0
    print(f'\n開始補門牌座標（Nominatim，每筆間隔 {DELAY_SEC}s）...\n')
    for i, r in enumerate(need):
        addr = r.get('address', '')
        # 確保地址含縣市前綴，提高 Nominatim 準確率
        if '臺東' not in addr and '台東' not in addr:
            addr = f'臺東縣{r.get("district", "")}{addr}'
        time.sleep(DELAY_SEC)
        coords = _nominatim_geocode(addr)
        status = f'lat={coords["lat"]:.5f}, lng={coords["lng"]:.5f}' if coords else '查無座標'
        print(f'  [{i+1}/{len(need)}] {addr} → {status}')
        if coords:
            r['lat'] = coords['lat']
            r['lng'] = coords['lng']
            updated += 1
        else:
            failed += 1

    print(f'\n已補座標：{updated} 筆，查無結果：{failed} 筆')
    _save(records)


def _save(records):
    """儲存本地並上傳 GCS。"""
    with open(LOCAL_PATH, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f'已儲存本地：{LOCAL_PATH}')
    if GCS_BUCKET:
        gcs_upload(GCS_BUCKET, PRICE_DATA_GCS_KEY, records)
        print('✅ 完成。Cloud Run 最多 1 小時內會載入新資料。')
    else:
        print('（未設定 GCS_BUCKET，僅儲存本地）')


if __name__ == '__main__':
    main()
