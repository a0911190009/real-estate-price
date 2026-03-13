# -*- coding: utf-8 -*-
"""
實價登錄 CSV 解析腳本
用法：python3 parse_csv.py [資料夾路徑] [輸出 JSON 路徑]

預設：
  輸入 → ~/Downloads/實價登錄/ 下所有 opendata 資料夾
  輸出 → ./price_data_tw.json（全台）
        → ./price_data_v.json（臺東縣）

縣市代碼對照（檔名前綴）：
  a=臺北市 b=新北市 c=基隆市 d=桃園市 e=新竹市 f=新竹縣
  g=宜蘭縣 h=臺中市 i=苗栗縣 j=彰化縣 k=南投縣 m=雲林縣
  n=嘉義市 o=嘉義縣 p=臺南市 q=高雄市 t=屏東縣 u=花蓮縣
  v=臺東縣 w=澎湖縣 x=金門縣
"""

import csv
import json
import os
import glob
import sys
import hashlib

# ── 縣市代碼對照表 ────────────────────────────────────────────────────
COUNTY_MAP = {
    'a': '臺北市', 'b': '新北市', 'c': '基隆市', 'd': '桃園市',
    'e': '新竹市', 'f': '新竹縣', 'g': '宜蘭縣', 'h': '臺中市',
    'i': '苗栗縣', 'j': '彰化縣', 'k': '南投縣', 'm': '雲林縣',
    'n': '嘉義市', 'o': '嘉義縣', 'p': '臺南市', 'q': '高雄市',
    't': '屏東縣', 'u': '花蓮縣', 'v': '臺東縣', 'w': '澎湖縣',
    'x': '金門縣',
}

# ── 要解析的檔案類型（_a = 買賣，_b = 預售屋，_c = 租賃） ──────────
TARGET_SUFFIX = '_a'  # 只取不動產買賣


def roc_to_ad(roc_str):
    """民國年月日（如 1141205）轉西元（如 2025-12-05）"""
    s = str(roc_str).strip()
    if len(s) != 7:
        return ''
    try:
        year = int(s[:3]) + 1911
        month = s[3:5]
        day = s[5:7]
        return f'{year}-{month}-{day}'
    except Exception:
        return ''


def sqm_to_ping(sqm_str):
    """平方公尺轉坪（1坪 = 3.30579 平方公尺）"""
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
    """移轉層次中文轉數字（一層→1，十一層→11）"""
    s = str(s).strip()
    if not s:
        return 0
    mapping = {
        '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
        '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
        '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15,
        '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
        '全': 0, '見使用執照': 0,
    }
    # 嘗試移除「層」字
    s = s.replace('層', '').replace('第', '').strip()
    return mapping.get(s, 0)


def parse_csv_file(filepath, county_code, batch_label):
    """解析單一 CSV 檔，回傳清洗後的紀錄 list"""
    records = []
    county_name = COUNTY_MAP.get(county_code, county_code)

    try:
        with open(filepath, encoding='utf-8-sig') as f:
            rows = list(csv.reader(f))
    except UnicodeDecodeError:
        # 嘗試 Big5
        with open(filepath, encoding='big5', errors='replace') as f:
            rows = list(csv.reader(f))

    if len(rows) < 3:
        return records  # 無資料

    # 第1列是欄位名稱，第2列是英文說明，第3列起是資料
    headers = rows[0]
    data_rows = rows[2:]

    # 建立欄位索引
    col = {name: i for i, name in enumerate(headers)}

    for row in data_rows:
        if len(row) < 10:
            continue

        def get(field, default=''):
            idx = col.get(field, -1)
            if idx < 0 or idx >= len(row):
                return default
            return row[idx].strip()

        # ── 基本資訊 ──
        address = get('土地位置建物門牌')
        if not address:
            continue

        transaction_type = get('交易標的')  # 房地、土地、建物、車位…
        district = get('鄉鎮市區')
        date_str = roc_to_ad(get('交易年月日'))
        if not date_str:
            continue

        # ── 價格 ──
        total_price_ntd = to_int(get('總價元'))
        if total_price_ntd <= 0:
            continue
        total_price_wan = round(total_price_ntd / 10000, 1)  # 轉萬元

        unit_price_sqm = to_float(get('單價元平方公尺'))

        # ── 面積 ──
        building_sqm = to_float(get('建物移轉總面積平方公尺'))
        building_ping = sqm_to_ping(building_sqm)
        main_building_ping = sqm_to_ping(get('主建物面積'))
        land_sqm = to_float(get('土地移轉總面積平方公尺'))
        land_ping = sqm_to_ping(land_sqm)

        # ── 建物資訊 ──
        building_type = get('建物型態')
        main_use = get('主要用途')
        material = get('主要建材')
        floor_str = get('移轉層次')
        total_floor_str = get('總樓層數')
        floor_num = parse_floor(floor_str)
        total_floor_num = parse_floor(total_floor_str)

        # ── 屋齡 ──
        complete_date = get('建築完成年月')
        age = 0
        if complete_date and len(complete_date) >= 5:
            complete_ad = roc_to_ad(complete_date + '01' if len(complete_date) == 5 else complete_date[:7])
            if complete_ad:
                try:
                    complete_year = int(complete_ad[:4])
                    trans_year = int(date_str[:4])
                    age = max(0, trans_year - complete_year)
                except Exception:
                    pass

        # ── 格局 ──
        rooms = to_int(get('建物現況格局-房'))
        halls = to_int(get('建物現況格局-廳'))
        bathrooms = to_int(get('建物現況格局-衛'))
        partitioned = get('建物現況格局-隔間')

        # ── 車位 ──
        park_type = get('車位類別')
        park_price_ntd = to_int(get('車位總價元'))
        park_price_wan = round(park_price_ntd / 10000, 1) if park_price_ntd > 0 else 0

        # ── 其他 ──
        elevator = get('電梯')
        mgmt = get('有無管理組織')
        note = get('備註')
        serial = get('編號') or get('移轉編號')
        urban_zone = get('都市土地使用分區')
        non_urban_zone = get('非都市土地使用分區')
        non_urban_use = get('非都市土地使用編定')

        # ── 單價換算（元/平方公尺 → 萬/坪） ──
        unit_price_ping = 0.0
        if unit_price_sqm > 0:
            unit_price_ping = round(unit_price_sqm * 3.30579 / 10000, 1)

        # ── 唯一 ID（用地址+日期+總價 hash） ──
        uid_src = f"{county_code}_{address}_{date_str}_{total_price_ntd}"
        uid = hashlib.md5(uid_src.encode()).hexdigest()[:12]

        record = {
            'id': uid,
            'batch': batch_label,           # 資料批次（如 20260101）
            'county': county_name,          # 縣市
            'county_code': county_code,     # 縣市代碼
            'district': district,           # 鄉鎮市區
            'address': address,             # 門牌地址
            'transaction_type': transaction_type,  # 交易標的
            'date': date_str,              # 交易日期（西元）
            'total_price': total_price_wan,        # 總價（萬元）
            'unit_price': unit_price_ping,         # 單價（萬/坪）
            'building_ping': building_ping,        # 建物移轉面積（坪）
            'main_building_ping': main_building_ping,  # 主建物面積（坪）
            'land_ping': land_ping,                # 土地面積（坪）
            'building_type': building_type,        # 建物型態
            'main_use': main_use,                  # 主要用途
            'material': material,                  # 主要建材
            'floor': floor_num,                    # 移轉層次
            'total_floor': total_floor_num,        # 總樓層數
            'age': age,                            # 屋齡（估算）
            'rooms': rooms,                        # 房
            'halls': halls,                        # 廳
            'bathrooms': bathrooms,                # 衛
            'partitioned': partitioned,            # 隔間
            'elevator': elevator,                  # 電梯
            'mgmt': mgmt,                          # 管理組織
            'urban_zone': urban_zone,              # 都市使用分區
            'non_urban_zone': non_urban_zone,      # 非都市使用分區
            'non_urban_use': non_urban_use,        # 非都市使用編定
            'park_type': park_type,                # 車位類別
            'park_price': park_price_wan,          # 車位總價（萬）
            'note': note,                          # 備註
            'serial': serial,                      # 編號
            # 地理座標留空，後續可用地址 geocoding 補上
            'lat': None,
            'lng': None,
        }
        records.append(record)

    return records


def main():
    base_dir = os.path.expanduser('~/Downloads/實價登錄')
    output_all = os.path.join(os.path.dirname(__file__), 'price_data_all.json')
    output_v = os.path.join(os.path.dirname(__file__), 'price_data_v.json')

    # 命令列參數（可選）
    target_counties = sys.argv[1:] if len(sys.argv) > 1 else None  # 如 v（臺東）

    folders = sorted(glob.glob(os.path.join(base_dir, '*_opendata')))
    if not folders:
        print(f'找不到資料夾：{base_dir}/*_opendata')
        sys.exit(1)

    all_records = []
    seen_ids = set()  # 去除重複（跨批次同一筆）

    for folder in folders:
        batch = os.path.basename(folder).replace('_opendata', '')
        print(f'\n處理批次：{batch}')

        for county_code in COUNTY_MAP.keys():
            if target_counties and county_code not in target_counties:
                continue

            # 只處理不動產買賣主檔（_a.csv）
            csv_path = os.path.join(folder, f'{county_code}_lvr_land_a.csv')
            if not os.path.isfile(csv_path):
                continue

            records = parse_csv_file(csv_path, county_code, batch)

            # 去重
            new_count = 0
            for r in records:
                if r['id'] not in seen_ids:
                    seen_ids.add(r['id'])
                    all_records.append(r)
                    new_count += 1

            if new_count > 0:
                print(f'  {COUNTY_MAP[county_code]}（{county_code}）: {new_count} 筆')

    print(f'\n全部合計：{len(all_records)} 筆')

    # 輸出全台資料
    if not target_counties:
        with open(output_all, 'w', encoding='utf-8') as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)
        print(f'全台資料已儲存：{output_all}')

    # 輸出臺東資料
    v_records = [r for r in all_records if r['county_code'] == 'v']
    with open(output_v, 'w', encoding='utf-8') as f:
        json.dump(v_records, f, ensure_ascii=False, indent=2)
    print(f'臺東資料已儲存：{output_v}（{len(v_records)} 筆）')

    # 印出統計摘要
    print('\n=== 臺東資料摘要 ===')
    types = {}
    districts = {}
    for r in v_records:
        t = r.get('transaction_type', '其他')
        types[t] = types.get(t, 0) + 1
        d = r.get('district', '不明')
        districts[d] = districts.get(d, 0) + 1

    print('交易標的類型：')
    for k, v in sorted(types.items(), key=lambda x: -x[1]):
        print(f'  {k}: {v} 筆')
    print('鄉鎮市區分布：')
    for k, v in sorted(districts.items(), key=lambda x: -x[1])[:10]:
        print(f'  {k}: {v} 筆')


if __name__ == '__main__':
    main()
