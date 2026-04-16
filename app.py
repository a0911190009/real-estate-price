# -*- coding: utf-8 -*-
"""
房仲工具 — 實價登錄調查（real-estate-price）
查詢內政部實價登錄、附近成交價，與物件庫整合。
資料儲存在 GCS：price/price_data_v.json（臺東縣）
"""

import os
import io
import json
import time
import zipfile
import tempfile
import threading
from datetime import timedelta, datetime
from flask import Flask, request, session, redirect, jsonify, send_from_directory, Response, stream_with_context
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature

try:
    from dotenv import load_dotenv
    _dir = os.path.dirname(os.path.abspath(__file__))
    for p in (os.path.join(_dir, ".env"), os.path.join(_dir, "..", ".env")):
        if os.path.isfile(p):
            load_dotenv(p, override=False)
            break
except Exception:
    pass

app = Flask(__name__)
_secret = os.environ.get("FLASK_SECRET_KEY", "")
if not _secret:
    import logging
    logging.warning("FLASK_SECRET_KEY 未設定，使用預設 dev key。")
app.secret_key = _secret or "dev-only-insecure-key"
_is_production = bool(os.environ.get("K_SERVICE"))
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = _is_production
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=30)  # 手機瀏覽器會清除沒有到期日的 session cookie，設 30 天保持登入

# ─── 開發模式：自動模擬登入 ───
@app.before_request
def auto_login_dev():
    """本地開發時，SKIP_AUTH=true 會自動模擬登入，跳過 Portal token 驗證"""
    if os.getenv('SKIP_AUTH'):
        session.permanent = True  # 讓 cookie 帶 30 天到期日，手機不會被清除
        session['user_email'] = 'dev@test.com'
        session['user_name'] = '開發測試'

PORTAL_URL = (os.environ.get("PORTAL_URL") or "").strip()
ADMIN_EMAILS = [e.strip() for e in (os.environ.get("ADMIN_EMAILS") or "").split(",") if e.strip()]
GCS_BUCKET = (os.environ.get("GCS_BUCKET") or "").strip()
PRICE_DATA_GCS_KEY = "price/price_data_v.json"   # GCS 上的路徑
LOCAL_DATA_PATH = os.path.join(os.path.dirname(__file__), "price_data_v.json")

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
GENERAL_FEEDBACK_FILE = os.path.join(_APP_DIR, "general_feedback.json")

TOKEN_SERIALIZER = URLSafeTimedSerializer(app.secret_key)
TOKEN_MAX_AGE = 300

# ── 資料快取（從 GCS 或本地讀取，每小時重新整理）────────────────────
_data_cache = []
_data_cache_ts = 0.0
_data_lock = threading.Lock()


def _load_price_data():
    """從 GCS 或本地讀取實價登錄資料，附帶快取（1小時）。"""
    global _data_cache, _data_cache_ts

    with _data_lock:
        # 快取有效直接回傳
        if _data_cache and (time.time() - _data_cache_ts) < 3600:
            return _data_cache

        data = []

        if GCS_BUCKET:
            # Cloud Run 環境：從 GCS 讀取
            try:
                from google.cloud import storage
                client = storage.Client()
                bucket = client.bucket(GCS_BUCKET)
                blob = bucket.blob(PRICE_DATA_GCS_KEY)
                content = blob.download_as_text(encoding='utf-8')
                data = json.loads(content)
            except Exception as e:
                import logging
                logging.error(f'GCS 讀取失敗：{e}')
        else:
            # 本機開發：從本地 JSON 讀取
            if os.path.isfile(LOCAL_DATA_PATH):
                with open(LOCAL_DATA_PATH, encoding='utf-8') as f:
                    data = json.load(f)

        _data_cache = data
        _data_cache_ts = time.time()
        return data


def _is_admin(email):
    return email in ADMIN_EMAILS


def _load_general_feedback():
    """讀取通用反饋列表"""
    if GCS_BUCKET:
        try:
            from google.cloud import storage
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET)
            blob = bucket.blob("general_feedback.json")
            if blob.exists():
                content = blob.download_as_text(encoding='utf-8')
                return json.loads(content) if content else []
        except:
            pass
    # Fallback 至本地
    if os.path.exists(GENERAL_FEEDBACK_FILE):
        try:
            with open(GENERAL_FEEDBACK_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return []
    return []


def _atomic_write(fpath, data_str):
    """原子寫入：先寫 .tmp，fsync 後再 os.replace，讀取時永遠是完整檔案。"""
    os.makedirs(os.path.dirname(fpath), exist_ok=True)
    tmp = fpath + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data_str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, fpath)


def _gcs_write_feedback(data_str):
    """寫入通用反饋至 GCS 或本地"""
    if GCS_BUCKET:
        try:
            from google.cloud import storage
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET)
            blob = bucket.blob("general_feedback.json")
            blob.upload_from_string(data_str, content_type="application/json")
            return True
        except:
            pass
    # Fallback 至本地
    _atomic_write(GENERAL_FEEDBACK_FILE, data_str)
    return True


def _require_user():
    email = session.get("user_email")
    if not email:
        return None, ({"error": "未登入", "redirect": PORTAL_URL or "/"}, 401)
    return email, None


@app.route("/auth/portal-login", methods=["GET", "POST"])
def auth_portal_login():
    """Portal 跳轉過來時，驗證 token 建立 session。"""
    token = request.form.get("token") or request.args.get("token", "")
    if not token:
        return redirect(PORTAL_URL or "/")
    try:
        payload = TOKEN_SERIALIZER.loads(token, salt="portal-sso", max_age=TOKEN_MAX_AGE)
    except (SignatureExpired, BadSignature, Exception):
        return redirect(PORTAL_URL or "/")
    email = payload.get("email", "")
    if not email:
        return redirect(PORTAL_URL or "/")
    session.permanent = True  # 讓 cookie 帶 30 天到期日，手機不會被清除
    session["user_email"] = email
    session["user_name"] = payload.get("name", "")
    session["user_picture"] = payload.get("picture", "")
    session.modified = True
    return send_from_directory("static", "index.html")


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    session.clear()
    return jsonify({"redirect": PORTAL_URL or "/"})


@app.route("/api/config")
def api_config():
    """回傳前端需要的設定。"""
    return jsonify({"portal_url": PORTAL_URL or "/"})


@app.route("/api/me")
def api_me():
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]
    return jsonify({
        "email": email,
        "name": session.get("user_name", ""),
        "picture": session.get("user_picture", ""),
        "is_admin": _is_admin(email),
    })


@app.route("/api/theme", methods=["GET"])
def api_theme_get():
    """讀取主題（與 Portal 共用 Firestore system_settings/theme）。"""
    try:
        from google.cloud import firestore as _fs
        db = _fs.Client()
        doc = db.collection("system_settings").document("theme").get()
        if doc.exists:
            return jsonify(doc.to_dict())
    except Exception:
        pass
    return jsonify({})


@app.route("/api/theme", methods=["POST"])
def api_theme_set():
    """儲存主題（僅管理員）。"""
    email = session.get("user_email", "")
    admin_emails = [e.strip() for e in os.environ.get("ADMIN_EMAILS", "").split(",") if e.strip()]
    if email not in admin_emails:
        return jsonify({"error": "unauthorized"}), 403
    data = request.get_json() or {}
    try:
        from google.cloud import firestore as _fs
        db = _fs.Client()
        update = {k: data[k] for k in ("style", "mode") if k in data}
        if update:
            db.collection("system_settings").document("theme").set(update, merge=True)
    except Exception:
        pass
    return jsonify({"ok": True})


@app.route("/api/general-feedback", methods=["GET"])
def api_general_feedback_get():
    """列出所有通用反饋"""
    return jsonify(_load_general_feedback())


@app.route("/api/general-feedback", methods=["POST"])
def api_general_feedback():
    """通用反饋"""
    data = request.get_json() or {}
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"error": "請輸入意見內容"}), 400

    entries = _load_general_feedback()
    entries.append({
        "text": text,
        "category": data.get("category", ""),
        "created_at": datetime.now().isoformat(),
    })
    data_str = json.dumps(entries, ensure_ascii=False, indent=2)
    _gcs_write_feedback(data_str)

    return jsonify({"ok": True, "total": len(entries)})


@app.route("/")
def index():
    """未登入時導向 Portal；已登入由前端處理。"""
    if session.get("user_email"):
        return send_from_directory("static", "index.html")
    return redirect(PORTAL_URL or "/")


@app.route("/api/search", methods=["POST"])
def api_search():
    """
    查詢實價登錄成交紀錄。
    輸入：{
      "query": "地址或區域關鍵字",
      "district": "鄉鎮市區（可選）",
      "transaction_type": "交易標的類型（可選）",
      "min_price": 數字（萬，可選）,
      "max_price": 數字（萬，可選）,
      "min_ping": 數字（坪，可選）,
      "max_ping": 數字（坪，可選）,
      "sort": "date_desc|date_asc|price_desc|price_asc|unit_desc|unit_asc",
      "limit": 數字（預設 100）
    }
    """
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]

    body = request.get_json() or {}
    query = (body.get("query") or "").strip()
    district = (body.get("district") or "").strip()
    transaction_type = (body.get("transaction_type") or "").strip()
    min_price = body.get("min_price")   # 萬元
    max_price = body.get("max_price")
    min_ping = body.get("min_ping")     # 坪
    max_ping = body.get("max_ping")
    sort_by = (body.get("sort") or "date_desc").strip()
    limit = min(int(body.get("limit") or 100), 500)

    # 讀取資料
    all_data = _load_price_data()

    # ── 篩選 ──────────────────────────────────────────────────────────
    results = []
    for r in all_data:
        # 關鍵字（地址 / 地段）
        if query:
            addr = r.get("address", "")
            if query not in addr:
                continue

        # 鄉鎮市區
        if district and r.get("district") != district:
            continue

        # 交易標的類型
        if transaction_type and transaction_type not in r.get("transaction_type", ""):
            continue

        # 總價範圍（萬）
        total = r.get("total_price", 0)
        if min_price is not None and total < float(min_price):
            continue
        if max_price is not None and total > float(max_price):
            continue

        # 建物面積範圍（坪）
        ping = r.get("building_ping", 0)
        if min_ping is not None and ping < float(min_ping):
            continue
        if max_ping is not None and ping > float(max_ping):
            continue

        results.append(r)

    # ── 排序 ──────────────────────────────────────────────────────────
    sort_map = {
        "date_desc":  lambda x: x.get("date", ""),
        "date_asc":   lambda x: x.get("date", ""),
        "price_desc": lambda x: x.get("total_price", 0),
        "price_asc":  lambda x: x.get("total_price", 0),
        "unit_desc":  lambda x: x.get("unit_price", 0),
        "unit_asc":   lambda x: x.get("unit_price", 0),
    }
    reverse_map = {
        "date_desc": True, "date_asc": False,
        "price_desc": True, "price_asc": False,
        "unit_desc": True, "unit_asc": False,
    }
    key_fn = sort_map.get(sort_by, sort_map["date_desc"])
    reverse = reverse_map.get(sort_by, True)
    results.sort(key=key_fn, reverse=reverse)

    # 限制筆數
    total_count = len(results)
    results = results[:limit]

    # ── 統計摘要 ──────────────────────────────────────────────────────
    if results:
        # 只計算有建物面積的（排除純土地）
        with_unit = [r for r in results if r.get("unit_price", 0) > 0]
        prices = [r["total_price"] for r in results]
        summary = {
            "count": total_count,
            "shown": len(results),
            "avg_total": round(sum(prices) / len(prices), 1),
            "max_total": max(prices),
            "min_total": min(prices),
            "avg_unit": round(sum(r["unit_price"] for r in with_unit) / len(with_unit), 1) if with_unit else 0,
            "max_unit": max((r["unit_price"] for r in with_unit), default=0),
            "min_unit": min((r["unit_price"] for r in with_unit if r["unit_price"] > 0), default=0),
        }
    else:
        summary = {"count": 0, "shown": 0}

    # ── 可用的篩選選項（供前端下拉選單） ─────────────────────────────
    all_districts = sorted(set(r.get("district", "") for r in all_data if r.get("district")))
    all_types = sorted(set(r.get("transaction_type", "") for r in all_data if r.get("transaction_type")))

    return jsonify({
        "results": results,
        "summary": summary,
        "filter_options": {
            "districts": all_districts,
            "transaction_types": all_types,
        },
        "is_mock": False,
    })


# ── 管理員：雲端匯入實價登錄資料（SSE 串流進度）──────────────────────────

def _invalidate_cache():
    """清除資料快取，讓下次查詢立即從 GCS 讀取最新資料。"""
    global _data_cache, _data_cache_ts
    with _data_lock:
        _data_cache = []
        _data_cache_ts = 0.0


def _gcs_download_price():
    """從 GCS 下載實價登錄 JSON，回傳 list。"""
    if not GCS_BUCKET:
        return []
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(GCS_BUCKET).blob(PRICE_DATA_GCS_KEY)
    if not blob.exists():
        return []
    return json.loads(blob.download_as_text(encoding='utf-8'))


def _gcs_upload_price(data):
    """上傳實價登錄 JSON 到 GCS。"""
    if not GCS_BUCKET:
        return
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(GCS_BUCKET).blob(PRICE_DATA_GCS_KEY)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, indent=2),
        content_type='application/json'
    )


def _sse(obj):
    """把 dict 包成 SSE 格式字串。"""
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"


@app.route('/api/admin/import', methods=['POST'])
def api_admin_import():
    """
    管理員上傳 ZIP → 解析 CSV → 合併 → 補座標（Easymap）→ 存回 GCS。
    以 SSE（Server-Sent Events）串流回傳進度。
    ⚠️ Cloud Run 請求 timeout 預設 60 秒，若資料量大（>50 筆新紀錄）
       建議在 Cloud Run 主控台將 timeout 調高至 600 秒。
    """
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]
    if not _is_admin(email):
        return jsonify({'error': '無管理員權限'}), 403
    if not GCS_BUCKET:
        return jsonify({'error': '未設定 GCS_BUCKET，無法使用雲端匯入'}), 500

    f = request.files.get('file')
    if not f or not f.filename.lower().endswith('.zip'):
        return jsonify({'error': '請上傳 .zip 檔案'}), 400

    # 在請求 context 內先讀完，generator 裡不再需要 request
    file_bytes = f.read()
    batch_label = f.filename.lower().replace('.zip', '').replace('_opendata', '')

    def generate():
        added = updated = geocoded = geo_fail = 0
        try:
            # ── Step 1：解析 ZIP ────────────────────────────────────
            yield _sse({'type': 'log', 'msg': '📁 解析 ZIP...'})

            from update_price_data import parse_land_file, parse_csv_file

            TARGET_CSV  = 'v_lvr_land_a.csv'
            TARGET_LAND = 'v_lvr_land_a_land.csv'

            with tempfile.TemporaryDirectory() as tmpdir:
                with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
                    zf.extractall(tmpdir)

                csv_path = land_path = None
                for root, _, files in os.walk(tmpdir):
                    for fname in files:
                        if fname.lower() == TARGET_CSV:
                            csv_path = os.path.join(root, fname)
                        if fname.lower() == TARGET_LAND:
                            land_path = os.path.join(root, fname)

                if not csv_path:
                    yield _sse({'type': 'error', 'msg': f'ZIP 內找不到 {TARGET_CSV}，請確認是臺東縣資料'})
                    return

                land_dict = parse_land_file(land_path) if land_path else {}
                new_records = parse_csv_file(csv_path, batch_label, land_dict)

            yield _sse({'type': 'log', 'msg': f'✅ 解析完成：{len(new_records)} 筆'})

            # ── Step 2：載入 GCS 現有資料 ─────────────────────────
            yield _sse({'type': 'log', 'msg': '☁️ 載入 GCS 現有資料...'})
            existing = _gcs_download_price()
            yield _sse({'type': 'log', 'msg': f'   GCS 現有：{len(existing)} 筆'})

            # ── Step 3：合併 ──────────────────────────────────────
            merged = {r['id']: r for r in existing}
            for r in new_records:
                if r['id'] not in merged:
                    merged[r['id']] = r
                    added += 1
                else:
                    old = merged[r['id']]
                    if old.get('batch') != r['batch']:
                        # 保留已補好的座標和地號，不被新批次覆蓋
                        if r.get('lat') is None and old.get('lat') is not None:
                            r['lat'], r['lng'] = old['lat'], old['lng']
                        if not r.get('land_sect') and old.get('land_sect'):
                            r['land_sect'] = old['land_sect']
                            r['land_no']   = old['land_no']
                        merged[r['id']] = r
                        updated += 1

            final = sorted(merged.values(), key=lambda x: x.get('date', ''), reverse=True)
            yield _sse({'type': 'log', 'msg': f'🔀 合併：新增 {added} 筆，更新 {updated} 筆，合計 {len(final)} 筆'})

            # ── Step 4：上傳合併結果 ───────────────────────────────
            _gcs_upload_price(final)
            yield _sse({'type': 'log', 'msg': '☁️ 已儲存到 GCS'})

            # ── Step 5：補座標 ─────────────────────────────────────
            need = [r for r in final
                    if r.get('lat') is None
                    and r.get('land_sect', '').strip()
                    and r.get('land_no', '').strip()]

            if not need:
                yield _sse({'type': 'log', 'msg': '📍 無需補座標（全部已有座標）'})
            else:
                yield _sse({'type': 'log', 'msg': f'📍 開始補座標：{len(need)} 筆...'})
                yield _sse({'type': 'geocode_start', 'total': len(need)})

                from geocode_price_data import EasymapCrawler
                crawler = EasymapCrawler()
                crawler.init()
                coord_cache = {}

                for i, r in enumerate(need):
                    cache_key = (r.get('district', ''), r['land_sect'], r['land_no'])
                    if cache_key not in coord_cache:
                        time.sleep(1.0)
                        try:
                            coords = crawler.get_coordinates(
                                '臺東縣', r.get('district', ''), r['land_sect'], r['land_no'])
                        except Exception:
                            coords = None
                        coord_cache[cache_key] = coords
                    else:
                        coords = coord_cache[cache_key]

                    if coords:
                        r['lat'], r['lng'] = coords['lat'], coords['lng']
                        geocoded += 1
                    else:
                        geo_fail += 1

                    status = '✅' if coords else '❌'
                    yield _sse({'type': 'geocode_progress',
                                'current': i + 1, 'total': len(need),
                                'msg': f'{status} [{i+1}/{len(need)}] {r.get("district","")} {r["land_sect"]} {r["land_no"]}'})

                    # 每 50 筆重新初始化 session，防止 token 過期
                    if (i + 1) % 50 == 0:
                        try:
                            crawler.init()
                        except Exception:
                            pass

                # 補座標後再次上傳
                _gcs_upload_price(final)
                yield _sse({'type': 'log', 'msg': f'📍 補座標完成：✅ {geocoded} 筆，❌ {geo_fail} 筆'})
                yield _sse({'type': 'log', 'msg': '☁️ 已儲存到 GCS（含座標）'})

            # 清除 app 快取，讓查詢即時反映新資料
            _invalidate_cache()
            yield _sse({'type': 'done',
                        'added': added, 'updated': updated,
                        'geocoded': geocoded, 'geo_fail': geo_fail})

        except Exception as e:
            import traceback
            yield _sse({'type': 'error', 'msg': str(e)})

    return Response(
        stream_with_context(generate()),
        content_type='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


def _polygon_area_sqm(ring, center_lat):
    """
    Shoelace 公式計算多邊形面積（平方公尺）。
    ring：[[lng, lat], ...] 座標環（WGS84）
    center_lat：中心緯度（用來換算經度的公尺比例）
    """
    import math
    lat_m = 111320.0                                    # 1° 緯度 ≈ 111320 公尺
    lng_m = 111320.0 * math.cos(math.radians(center_lat))  # 1° 經度（依緯度換算）
    n = len(ring)
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        x1, y1 = ring[i][0] * lng_m, ring[i][1] * lat_m
        x2, y2 = ring[j][0] * lng_m, ring[j][1] * lat_m
        area += (x1 * y2 - x2 * y1)
    return abs(area) / 2.0


@app.route('/api/lookup-parcel', methods=['POST'])
def api_lookup_parcel():
    """
    段別 + 地號 → 土地資料（twland.ronny.tw，免費無需登入）
    輸入：{ "land_sect": "建國段", "land_no": "08350001" }
    輸出：{
      "lat": 22.xxx, "lng": 121.xxx,   ← 地塊中心座標
      "district": "台東市",             ← 鄉鎮市區
      "area_m2": 138.5,                ← 土地面積（平方公尺）
      "land_ping": 41.9,               ← 土地面積（坪）
    }
    """
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]

    body      = request.get_json() or {}
    land_sect = (body.get('land_sect') or '').strip()
    land_no   = (body.get('land_no') or '').strip()

    if not land_sect or not land_no:
        return jsonify({'error': '請填入段別和地號'}), 400

    # 地號正規化：去除非數字 → 補零到 8 碼 → 格式化成 XXXX-XXXX
    digits = ''.join(c for c in land_no if c.isdigit()).zfill(8)
    land_no_fmt = f"{digits[:4]}-{digits[4:]}"   # e.g. 0835-0001

    try:
        import urllib.request as _ur, urllib.parse as _up
        query = f"臺東縣,{land_sect},{land_no_fmt}"
        url   = 'https://twland.ronny.tw/index/search?' + _up.urlencode({'lands[]': query})
        req   = _ur.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with _ur.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode('utf-8'))

        features = data.get('features', [])
        not_found = data.get('notfound', [])

        if not features:
            msg = f'查無地號：臺東縣 {land_sect} {land_no_fmt}'
            if not_found:
                msg += f'（notfound: {not_found}）'
            return jsonify({'error': msg}), 404

        feat = features[0]
        props = feat.get('properties', {})
        geom  = feat.get('geometry', {})

        # 中心座標（twland 直接提供）
        lat = float(props.get('ycenter', 0))
        lng = float(props.get('xcenter', 0))

        # 鄉鎮市區（去掉「縣市」部分，只保留鄉鎮名）
        district = props.get('鄉鎮', '')

        # 計算面積：取 MultiPolygon 最大環的 Shoelace 面積
        area_m2 = 0.0
        coords_type = geom.get('type', '')
        rings = []
        if coords_type == 'MultiPolygon':
            for poly in geom.get('coordinates', []):
                if poly:
                    rings.append(poly[0])   # 外環
        elif coords_type == 'Polygon':
            c = geom.get('coordinates', [])
            if c:
                rings.append(c[0])
        for ring in rings:
            a = _polygon_area_sqm(ring, lat)
            if a > area_m2:
                area_m2 = a   # 取最大多邊形（面積最大的地塊）

        SQM_PER_PING = 3.30579
        land_ping = round(area_m2 / SQM_PER_PING, 1)

        return jsonify({
            'lat':       round(lat, 7),
            'lng':       round(lng, 7),
            'district':  district,
            'area_m2':   round(area_m2, 1),
            'land_ping': land_ping,
        })

    except Exception as e:
        return jsonify({'error': f'地號查詢失敗：{str(e)}'}), 500


def _haversine_m(lat1, lng1, lat2, lng2):
    """計算兩點 WGS84 座標的直線距離（公尺）"""
    import math
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@app.route('/api/geocode-address', methods=['POST'])
def api_geocode_address():
    """
    地址 → WGS84 座標（透過 Google Places Text Search API）。
    輸入：{ "address": "台東市中山路123號" }
    輸出：{ "lat": 22.xxx, "lng": 121.xxx }
    不需要段別，直接從地址解析，比 Easymap 更方便。
    """
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]

    body    = request.get_json() or {}
    address = (body.get('address') or '').strip()
    if not address:
        return jsonify({'error': '請填入地址'}), 400

    api_key = os.environ.get('GOOGLE_MAPS_API_KEY', '')
    if not api_key:
        return jsonify({'error': '未設定 GOOGLE_MAPS_API_KEY'}), 500

    try:
        import urllib.request as _ur
        req_data = json.dumps({
            'textQuery': address,
            'languageCode': 'zh-TW',
            'maxResultCount': 1,
        }).encode('utf-8')
        req = _ur.Request(
            'https://places.googleapis.com/v1/places:searchText',
            data=req_data,
            headers={
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': api_key,
                'X-Goog-FieldMask': 'places.location,places.formattedAddress',
            },
            method='POST'
        )
        with _ur.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        places = data.get('places', [])
        if places:
            loc = places[0].get('location', {})
            lat = loc.get('latitude')
            lng = loc.get('longitude')
            if lat is not None and lng is not None:
                return jsonify({'lat': float(lat), 'lng': float(lng),
                                'formatted': places[0].get('formattedAddress', '')})
        return jsonify({'error': f'查無結果：{address}'}), 404
    except Exception as e:
        return jsonify({'error': f'地址解析失敗：{str(e)}'}), 500


@app.route('/api/geocode-parcel', methods=['POST'])
def api_geocode_parcel():
    """
    地號 → WGS84 座標（透過 Easymap 內政部地籍圖資）。
    輸入：{ "district": "台東市", "land_sect": "建國段", "land_no": "08350001" }
    輸出：{ "lat": 22.xxx, "lng": 121.xxx }
    """
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]

    body     = request.get_json() or {}
    district  = (body.get('district') or '').strip()
    land_sect = (body.get('land_sect') or '').strip()
    land_no   = (body.get('land_no') or '').strip()

    if not district or not land_sect or not land_no:
        return jsonify({'error': '請填入鄉鎮、段別、地號'}), 400

    try:
        # 複用本機 geocode_price_data.py 中的 EasymapCrawler
        from geocode_price_data import EasymapCrawler
        crawler = EasymapCrawler()
        crawler.init()
        coords = crawler.get_coordinates('臺東縣', district, land_sect, land_no)
        if coords:
            return jsonify({'lat': coords['lat'], 'lng': coords['lng']})
        return jsonify({'error': f'查無座標：{district} {land_sect} {land_no}'}), 404
    except Exception as e:
        return jsonify({'error': f'Easymap 查詢失敗：{str(e)}'}), 500


@app.route('/api/valuation', methods=['POST'])
def api_valuation():
    """
    AI 市場估價。
    輸入：{
      "address": "地址或地號描述",
      "district": "鄉鎮（可選，不填則全縣）",
      "transaction_type": "交易標的（可選）",
      "building_type": "建物型態（可選）",
      "building_ping": 坪數（可選）,
      "land_ping": 土地坪數（可選）,
      "floor": 樓層（可選）,
      "total_floor": 總樓層（可選）,
      "age": 屋齡（可選）,
      "note": "備注說明（可選）"
    }
    輸出：{
      "suggested_min": 數字（萬）,
      "suggested_max": 數字（萬）,
      "median": 數字（萬）,
      "analysis": "AI 分析文字",
      "comparables": [ ...最相近的案例... ],
      "generated_at": "ISO 日期"
    }
    """
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]

    body = request.get_json() or {}
    address    = (body.get('address') or '').strip()
    district   = (body.get('district') or '').strip()
    tx_type    = (body.get('transaction_type') or '').strip()
    bld_type   = (body.get('building_type') or '').strip()
    bld_ping   = float(body.get('building_ping') or 0)
    land_ping  = float(body.get('land_ping') or 0)
    floor_val  = int(body.get('floor') or 0)
    total_floor= int(body.get('total_floor') or 0)
    age_val    = int(body.get('age') or 0)
    note_val   = (body.get('note') or '').strip()
    # 座標（由前端查地號或手動帶入）
    lat_val    = float(body.get('lat') or 0)
    lng_val    = float(body.get('lng') or 0)
    land_sect_val = (body.get('land_sect') or '').strip()
    land_no_val   = (body.get('land_no') or '').strip()
    # 土地分區資料（選填，使用者從 FOUNDI 等來源填入）
    use_zone_val  = (body.get('use_zone') or '').strip()
    coverage_val  = float(body.get('building_coverage') or 0)   # 建蔽率 %
    far_val       = float(body.get('floor_area_ratio') or 0)    # 容積率 %
    declared_price_val = float(body.get('declared_price') or 0) # 公告地價 元/㎡

    if not address:
        return jsonify({'error': '請輸入地址或地號'}), 400

    # ── 若有地號但無座標，嘗試自動透過 Easymap 補座標 ──────────────────
    if (not lat_val or not lng_val) and land_sect_val and land_no_val and district:
        try:
            from geocode_price_data import EasymapCrawler
            _crawler = EasymapCrawler()
            _crawler.init()
            _coords = _crawler.get_coordinates('臺東縣', district, land_sect_val, land_no_val)
            if _coords:
                lat_val = _coords['lat']
                lng_val = _coords['lng']
        except Exception:
            pass  # 查不到就退回路名比對，不阻斷流程

    # ── Step 1：Hard Filter — 候選池篩選 ────────────────────────────
    import re as _re
    import math as _math

    all_data = _load_price_data()
    from datetime import date
    today = date.today()
    three_years_ago = f"{today.year - 3}-{today.month:02d}-{today.day:02d}"

    candidates = []
    for r in all_data:
        # 只取近 3 年（擴大時間窗口，台東交易量少）
        if (r.get('date') or '') < three_years_ago:
            continue
        # 地區過濾
        if district and r.get('district') != district:
            continue
        # 交易標的過濾
        if tx_type and tx_type not in r.get('transaction_type', ''):
            continue
        # ★ Hard Filter 1：預設排除預售屋（時間點、定價邏輯不同）
        if '預售屋' in r.get('transaction_type', ''):
            continue
        # ★ Hard Filter 2：建物型態必須一致（有指定才強制）
        if bld_type and r.get('building_type') and bld_type not in r['building_type']:
            continue
        # ★ Hard Filter 3：坪數限 ±60%（避免 10 坪跟 100 坪互比）
        if bld_ping > 0 and r.get('building_ping', 0) > 0:
            ratio = r['building_ping'] / bld_ping
            if ratio < 0.4 or ratio > 2.5:
                continue
        candidates.append(r)

    # ── Step 2：相似度評分 ───────────────────────────────────────────

    def _extract_road(addr):
        """從地址擷取路名，例如「台東市中山路123號」→「中山路」"""
        m = _re.search(r'([^\s市縣鄉鎮區村里]+?(?:路|街|大道|道路)(?:[一二三四五六七八九十]段)?)', addr or '')
        return m.group(1) if m else ''

    def _addr_prefix(addr):
        """取地址路名+門牌號碼前半，用來判斷同棟/鄰棟"""
        m = _re.search(r'(\d+)號', addr or '')
        road = _extract_road(addr)
        return f"{road}{m.group(1)}" if m and road else ''

    subject_road   = _extract_road(address)
    subject_prefix = _addr_prefix(address)

    # 計算待估物件的相對樓層（無資料則 0）
    subject_floor_ratio = (floor_val / total_floor) if (floor_val > 0 and total_floor > 0) else 0

    def similarity_score(r):
        score = 0.0

        # ── 地理位置（最高優先，40 分）────────────────────────────────
        has_coords = lat_val and lng_val and r.get('lat') and r.get('lng')
        if has_coords:
            dist_m = _haversine_m(lat_val, lng_val, r['lat'], r['lng'])
            if dist_m <= 300:
                score += 40
            elif dist_m <= 600:
                score += 30
            elif dist_m <= 1000:
                score += 18
            elif dist_m <= 2000:
                score += 8
        else:
            comp_road = _extract_road(r.get('address', ''))
            if subject_road and comp_road:
                if comp_road == subject_road:
                    score += 35
                elif comp_road[:2] == subject_road[:2]:
                    score += 10

        # ── 同棟/鄰棟加分（20 分）────────────────────────────────────
        if subject_prefix:
            comp_prefix = _addr_prefix(r.get('address', ''))
            if comp_prefix and comp_prefix == subject_prefix:
                score += 20   # 同棟或同門牌：消除位置雜訊

        # ── 建物型態（已硬篩選，這裡視為確認加分 20 分）────────────────
        if bld_type and r.get('building_type') and bld_type in r['building_type']:
            score += 20

        # ── 坪數接近（建物，20 分）────────────────────────────────────
        if bld_ping > 0 and r.get('building_ping', 0) > 0:
            diff_ratio = abs(r['building_ping'] - bld_ping) / bld_ping
            score += max(0, 20 - diff_ratio * 40)

        # ── 土地坪數接近（15 分）─────────────────────────────────────
        if land_ping > 0 and r.get('land_ping', 0) > 0:
            diff_ratio = abs(r['land_ping'] - land_ping) / land_ping
            score += max(0, 15 - diff_ratio * 30)

        # ── 屋齡接近（10 分）─────────────────────────────────────────
        if age_val > 0 and r.get('age', 0) > 0:
            diff = abs(r['age'] - age_val)
            score += max(0, 10 - diff * 1)

        # ── 相對樓層接近（8 分）──────────────────────────────────────
        # 用 樓層/總樓層 比值差，比絕對層數更有意義
        r_floor = r.get('floor', 0)
        r_total = r.get('total_floor', 0)
        if subject_floor_ratio > 0 and r_floor > 0 and r_total > 0:
            comp_ratio = r_floor / r_total
            diff = abs(comp_ratio - subject_floor_ratio)
            score += max(0, 8 - diff * 16)   # 比值差 0.5 以上給 0 分

        # ── 指數時間衰減（10 分，半衰期 6 個月）─────────────────────────
        # 用指數衰減取代線性截止，越近越重要但不強制排除
        date_str = r.get('date', '')
        if date_str:
            try:
                days_ago = (today - date.fromisoformat(date_str)).days
                score += 10 * _math.exp(-days_ago / 180)
            except Exception:
                pass

        return score

    scored = [(r, similarity_score(r)) for r in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)
    top_scored = scored[:15]
    comparables = [r for r, _ in top_scored]

    if not comparables:
        # 放寬：移除型態硬篩，重試
        fallback = [r for r in all_data
                    if (r.get('date') or '') >= three_years_ago
                    and (not district or r.get('district') == district)
                    and '預售屋' not in r.get('transaction_type', '')]
        fallback.sort(key=lambda r: r.get('date', ''), reverse=True)
        comparables = fallback[:15]
        if not comparables:
            return jsonify({'error': f'找不到符合條件的參考案例（{district or "全縣"}，近 3 年）'}), 404

    # ── 信心分數（0–100）────────────────────────────────────────────────
    if top_scored:
        avg_sim = sum(s for _, s in top_scored[:10]) / min(len(top_scored), 10)
        # 分數上限約 100（各項滿分 40+20+20+20+15+10+8+10=143），正規化到 100
        raw_confidence = int(avg_sim / 1.43)
        data_density_bonus = min(20, len(top_scored))    # 案例越多信心越高
        confidence_score = min(100, raw_confidence + data_density_bonus)
    else:
        confidence_score = 0

    # ── Step 3：計算統計數值 ──────────────────────────────────────────
    prices = [r['total_price'] for r in comparables if r.get('total_price', 0) > 0]
    prices.sort()
    median_price = prices[len(prices) // 2] if prices else 0
    avg_price = round(sum(prices) / len(prices), 1) if prices else 0

    unit_prices = [r['unit_price'] for r in comparables if r.get('unit_price', 0) > 0]
    avg_unit = round(sum(unit_prices) / len(unit_prices), 1) if unit_prices else 0

    # ── Step 4：呼叫 Claude API ────────────────────────────────────────
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        return jsonify({'error': '未設定 ANTHROPIC_API_KEY'}), 500

    # 整理參考案例給 AI（加入距離資訊）
    comp_lines = []
    for i, r in enumerate(comparables[:10], 1):
        parts = [
            f"案例{i}：{r.get('address', '')}",
            f"成交日：{r.get('date', '')}",
            f"總價：{r.get('total_price', '')}萬",
        ]
        if r.get('unit_price', 0) > 0:
            parts.append(f"單價：{r['unit_price']}萬/坪")
        if r.get('building_ping', 0) > 0:
            parts.append(f"建物：{r['building_ping']}坪")
        if r.get('land_ping', 0) > 0:
            parts.append(f"地坪：{r['land_ping']}坪")
        if r.get('building_type'):
            parts.append(f"型態：{r['building_type']}")
        if r.get('age', 0) > 0:
            parts.append(f"屋齡：{r['age']}年")
        if r.get('floor', 0) > 0:
            parts.append(f"樓層：{r['floor']}/{r.get('total_floor', '?')}樓")
        # 附上距離（有座標才計算）
        if lat_val and lng_val and r.get('lat') and r.get('lng'):
            dist_m = _haversine_m(lat_val, lng_val, r['lat'], r['lng'])
            parts.append(f"距離：約{int(dist_m)}公尺")
        comp_lines.append('  ' + '，'.join(parts))

    # 待估物件條件
    subject_parts = [f"地址/地號：{address}"]
    if district:       subject_parts.append(f"鄉鎮：{district}")
    if tx_type:        subject_parts.append(f"交易標的：{tx_type}")
    if bld_type:       subject_parts.append(f"建物型態：{bld_type}")
    if bld_ping > 0:   subject_parts.append(f"建物面積：{bld_ping}坪")
    if land_ping > 0:  subject_parts.append(f"土地面積：{land_ping}坪")
    if floor_val > 0:  subject_parts.append(f"樓層：{floor_val}" + (f"/{total_floor}樓" if total_floor > 0 else "樓"))
    if age_val > 0:    subject_parts.append(f"屋齡：{age_val}年")
    if note_val:       subject_parts.append(f"備注：{note_val}")
    # 土地分區資料（有填才附上）
    if use_zone_val:          subject_parts.append(f"使用分區：{use_zone_val}")
    if coverage_val > 0:      subject_parts.append(f"建蔽率：{coverage_val}%")
    if far_val > 0:           subject_parts.append(f"容積率：{far_val}%")
    if declared_price_val > 0:subject_parts.append(f"公告地價：{declared_price_val:.0f}元/㎡")

    # 加入距離說明與信心等級供 AI 參考
    has_distance = lat_val and lng_val
    confidence_label = '高' if confidence_score >= 70 else ('中' if confidence_score >= 40 else '低')
    location_note = (
        f"（已提供 GPS 座標，比較案例已按直線距離篩選優先排序）"
        if has_distance else
        f"（未提供精確座標，按路名相似度篩選）"
    )

    # 若有分區資料加入特別提示
    zone_note = ""
    if use_zone_val or coverage_val > 0 or far_val > 0:
        zone_note = "\n5. 已提供土地分區資料，請在分析中說明分區對價值的影響（例如商業區通常比住宅區溢價）"

    prompt = f"""你是一位台東縣的專業不動產估價顧問，熟悉當地各區位價差。
請根據以下「參考成交案例」，對「待估物件」給出合理的市場開價建議。

【待估物件條件】
{chr(10).join(subject_parts)}

【近期參考成交案例（臺東縣，近3年實價登錄，已排除預售屋）{location_note}】
{chr(10).join(comp_lines)}

【統計摘要】
- 有效比較案例：{len(comparables)} 筆（資料信心：{confidence_label}，分數 {confidence_score}/100）
- 總價中位數：{median_price} 萬
- 總價平均：{avg_price} 萬
- 建物單價平均：{avg_unit} 萬/坪

【分析注意事項】
1. 距離較遠的案例（>1公里）請在分析中適當降低其參考權重
2. 較舊案例（>1年）市場可能已有變化，請酌情調整
3. 台東交易量少，若案例不足請說明估價信心較低
4. 請明確說明各案例與待估物件的差異（坪數、屋齡、位置），以及如何據此調整{zone_note}

請以 JSON 格式回覆，格式如下：
{{
  "suggested_min": 數字（萬，建議最低開價）,
  "suggested_max": 數字（萬，建議最高開價）,
  "key_factors": ["影響價格的關鍵因素（需具體，勿籠統）", "因素2", "因素3"],
  "analysis": "2~4段市場分析（繁體中文，說明選取哪些案例為主要參考依據、與待估物件的差異調整、最終如何得出開價範圍）",
  "strategy": "給業務員的議價建議（1~2句）"
}}

只回覆 JSON，不要加任何其他文字。"""

    try:
        import urllib.request
        req_data = json.dumps({
            'model': 'claude-haiku-4-5-20251001',
            'max_tokens': 1024,
            'messages': [{'role': 'user', 'content': prompt}]
        }).encode('utf-8')
        req = urllib.request.Request(
            'https://api.anthropic.com/v1/messages',
            data=req_data,
            headers={
                'x-api-key': api_key,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp_data = json.loads(resp.read().decode('utf-8'))
        ai_text = resp_data['content'][0]['text'].strip()
        # 清除 markdown code block（如果有的話）
        if ai_text.startswith('```'):
            ai_text = ai_text.split('\n', 1)[1].rsplit('```', 1)[0].strip()
        ai_result = json.loads(ai_text)
    except Exception as e:
        return jsonify({'error': f'AI 分析失敗：{e}'}), 500

    # ── Step 5：組合回傳 ──────────────────────────────────────────────
    return jsonify({
        'suggested_min':  ai_result.get('suggested_min', 0),
        'suggested_max':  ai_result.get('suggested_max', 0),
        'median':         median_price,
        'avg':            avg_price,
        'avg_unit':       avg_unit,
        'key_factors':    ai_result.get('key_factors', []),
        'analysis':       ai_result.get('analysis', ''),
        'strategy':       ai_result.get('strategy', ''),
        'comparables':    comparables[:10],
        'total_candidates': len(candidates),
        'confidence':     confidence_score,   # 0–100，評估比較案例品質
        'generated_at':   datetime.now().strftime('%Y-%m-%d %H:%M'),
        'subject': {
            'address': address, 'district': district,
            'transaction_type': tx_type, 'building_type': bld_type,
            'building_ping': bld_ping, 'land_ping': land_ping,
            'floor': floor_val, 'total_floor': total_floor,
            'age': age_val, 'note': note_val,
            'lat': lat_val or None, 'lng': lng_val or None,
            'use_zone': use_zone_val or None,
            'building_coverage': coverage_val or None,
            'floor_area_ratio': far_val or None,
            'declared_price': declared_price_val or None,
        }
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5010)), debug=bool(os.environ.get("FLASK_DEBUG")))
