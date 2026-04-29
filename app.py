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
from string import Template
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

# ─── 載入估價 prompt 模板 ───
_PROMPT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prompts", "valuation.txt")
with open(_PROMPT_FILE, encoding="utf-8") as _f:
    _VALUATION_PROMPT_TPL = Template(_f.read())
ADMIN_EMAILS = [e.strip() for e in (os.environ.get("ADMIN_EMAILS") or "").split(",") if e.strip()]
GCS_BUCKET = (os.environ.get("GCS_BUCKET") or "").strip()
PRICE_DATA_GCS_KEY = "price/price_data_v.json"   # GCS 上的路徑
LOCAL_DATA_PATH = os.path.join(os.path.dirname(__file__), "price_data_v.json")

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
GENERAL_FEEDBACK_FILE = os.path.join(_APP_DIR, "general_feedback.json")

TOKEN_SERIALIZER = URLSafeTimedSerializer(app.secret_key)
TOKEN_MAX_AGE = 300

# ── FOUNDI 段別快取（台東縣，24 小時有效，不分使用者）──────────────────
_foundi_sections: dict = {}   # { "利家段": {city_code, locality_code, section_code, ...} }
_foundi_sects_ts: float = 0.0
_foundi_sects_lock = threading.Lock()


def _foundi_get_sections(jwt: str) -> dict:
    """用 JWT 從 FOUNDI 取台東縣所有段別代碼，快取 24 小時。"""
    global _foundi_sections, _foundi_sects_ts
    with _foundi_sects_lock:
        if _foundi_sections and (time.time() - _foundi_sects_ts) < 86400:
            return _foundi_sections
        try:
            import urllib.request as _ur
            req_data = json.dumps({"city_code": "V", "locality_code": []}).encode('utf-8')
            req = _ur.Request(
                'https://agent.foundi.info/dataapi/transcript/cadasterSections/',
                data=req_data,
                headers={
                    'authorization': jwt,
                    'accept': 'application/json',
                    'content-type': 'application/json',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                },
                method='POST'
            )
            with _ur.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode('utf-8'))
            if isinstance(data, list) and data:
                _foundi_sections = {s['section_name']: s for s in data}
                _foundi_sects_ts = time.time()
        except Exception as e:
            import logging
            logging.warning(f'FOUNDI sections 快取失敗：{e}')
    return _foundi_sections

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


# ── LOG 工具函式 ──
def log_event(event_type, user_id="", detail=None):
    """記錄業務事件，輸出至 Cloud Logging（Cloud Run stdout 自動收集）。"""
    print(json.dumps({
        "time": datetime.now(timezone.utc).isoformat(),
        "event": event_type,   # 事件名稱，例如 "price_search"
        "user": user_id,
        "detail": detail or {}
    }, ensure_ascii=False), flush=True)


@app.route("/api/client-log", methods=["POST"])
def api_client_log():
    """接收前端 JS 錯誤，記錄至 Cloud Logging。"""
    data = request.get_json(silent=True) or {}
    log_event("client_error", detail=data)
    return jsonify({"ok": True})


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


@app.route("/api/foundi-jwt", methods=["POST"])
def api_foundi_jwt_set():
    """儲存 FOUNDI JWT 到 session，並回傳過期時間。"""
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]
    body = request.get_json() or {}
    jwt_val = (body.get("jwt") or "").strip()
    if not jwt_val:
        return jsonify({"error": "JWT 不能為空"}), 400
    # 解碼 JWT payload 取得過期時間（不驗簽名，只讀 payload）
    try:
        import base64 as _b64
        payload_b64 = jwt_val.split(".")[1]
        # 補齊 base64 padding
        payload_b64 += "=" * ((4 - len(payload_b64) % 4) % 4)
        payload = json.loads(_b64.b64decode(payload_b64).decode("utf-8"))
        exp_ts = payload.get("exp", 0)
    except Exception:
        return jsonify({"error": "無法解析 JWT，請確認格式正確"}), 400
    if exp_ts and exp_ts < time.time():
        return jsonify({"error": "此 JWT 已過期，請從 FOUNDI 重新取得"}), 400
    session["foundi_jwt"] = jwt_val
    session.modified = True
    exp_str = datetime.fromtimestamp(exp_ts).strftime("%H:%M") if exp_ts else "未知"
    return jsonify({"ok": True, "exp_ts": exp_ts, "exp_str": exp_str})


@app.route("/api/foundi-parcel", methods=["POST"])
def api_foundi_parcel():
    """
    段別 + 地號 → FOUNDI 土地分區資料（自動帶入估價表單）
    輸入：{ "land_sect": "利家段", "land_no": "54800006" }
    輸出：{
      "use_zone": "特定農業區（甲種建築用地）",
      "building_coverage": 60,  # 建蔽率 %（整數）
      "floor_area_ratio": 240,  # 容積率 %（整數）
      "declared_price": 570,    # 公告地價 元/㎡
      "current_value": 3900,    # 公告現值 元/㎡
      "land_area": 96.8,        # 土地坪數
      "land_area_m2": 320,
      "lat": 22.792408, "lng": 121.079193,
    }
    """
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]

    jwt_val = session.get("foundi_jwt", "").strip()
    if not jwt_val:
        return jsonify({"error": "FOUNDI_JWT_NOT_SET"}), 401

    # 檢查 JWT 是否過期
    try:
        import base64 as _b64
        payload_b64 = jwt_val.split(".")[1]
        payload_b64 += "=" * ((4 - len(payload_b64) % 4) % 4)
        payload = json.loads(_b64.b64decode(payload_b64).decode("utf-8"))
        if payload.get("exp", 0) < time.time():
            return jsonify({"error": "FOUNDI_JWT_EXPIRED"}), 401
    except Exception:
        return jsonify({"error": "FOUNDI_JWT_INVALID"}), 401

    body      = request.get_json() or {}
    land_sect = (body.get("land_sect") or "").strip()
    land_no   = (body.get("land_no")   or "").strip()

    if not land_sect or not land_no:
        return jsonify({"error": "請填入段別和地號"}), 400

    # 地號正規化：補零到 8 碼
    digits   = "".join(c for c in land_no if c.isdigit()).zfill(8)
    main_key = int(digits[:4])   # "0835" → 835
    sub_key  = int(digits[4:])   # "0001" → 1

    # 查段別代碼（帶入 JWT 以觸發快取刷新）
    sections = _foundi_get_sections(jwt_val)
    sec = sections.get(land_sect)
    if not sec:
        return jsonify({"error": f"找不到段別：{land_sect}"}), 404

    # 呼叫 FOUNDI land/mapLocation
    try:
        import urllib.request as _ur, urllib.parse as _up
        params = {
            "city_code":     sec["city_code"],
            "locality_code": sec["locality_code"],
            "section_code":  sec["section_code"],
            "main_key":      str(main_key),
            "sub_key":       str(sub_key),
        }
        url = "https://api.foundi.info/land/mapLocation/?" + _up.urlencode(params)
        req = _ur.Request(url, headers={
            "authorization": jwt_val,
            "accept": "application/json",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        })
        with _ur.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        return jsonify({"error": f"FOUNDI 查詢失敗：{e}"}), 500

    lands = data.get("lands", [])
    if not lands:
        return jsonify({"error": f"查無此地號：{land_sect} {digits[:4]}-{digits[4:]}"}), 404

    info = lands[0]["info"]

    # 組合使用分區字串：大分區（使用別）
    zone     = info.get("zone", [])
    sub_zone = info.get("sub_zone", [])
    zone_str = "、".join(zone)
    if sub_zone:
        zone_str += f"（{'、'.join(sub_zone)}）"

    # 建蔽率/容積率（FOUNDI 回傳 0~1 比例，轉成百分比整數）
    bcr = info.get("building_coverage_ratio", 0)  # 0.6 → 60
    far = info.get("floor_area_ratio", 0)          # 2.4 → 240

    # 座標中心點
    repr_coords = info.get("repr_point", {}).get("coordinates", [None, None])

    return jsonify({
        "use_zone":               zone_str,
        "building_coverage":      round(bcr * 100),
        "floor_area_ratio":       round(far * 100),
        "declared_price":         info.get("unit_value_with_square_meter", 0),         # 公告地價
        "current_value":          info.get("current_unit_value_with_square_meter", 0), # 公告現值
        "land_area":              info.get("land_area", 0),                 # 坪
        "land_area_m2":           info.get("land_area_in_square_meter", 0), # ㎡
        "building_coverage_area": info.get("building_coverage_area", 0),   # 建築面積坪
        "total_floor_area":       info.get("total_floor_area", 0),          # 可用建坪
        "lat":                    repr_coords[1],
        "lng":                    repr_coords[0],
    })


@app.route("/api/foundi-cadaster", methods=["POST"])
def api_foundi_cadaster():
    """
    cadaster_id 字串 → 段別名稱 + 地號 + 土地分區
    輸入：{ "cadaster_id": "V_04_0009_5480-0006" }
    輸出：{ land_sect, land_no, use_zone, building_coverage, floor_area_ratio,
            declared_price, current_value, land_area, lat, lng }
    """
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]

    body = request.get_json(silent=True) or {}
    cadaster_id = (body.get("cadaster_id") or "").strip()
    if not cadaster_id:
        return jsonify({"error": "缺少 cadaster_id"}), 400

    jwt_val = session.get("foundi_jwt", "").strip()
    if not jwt_val:
        return jsonify({"error": "FOUNDI_JWT_NOT_SET"}), 401

    # 解析 cadaster_id：V_04_0009_5480-0006
    try:
        parts    = cadaster_id.split("_")        # ['V','04','0009','5480-0006']
        city_code     = parts[0]
        locality_code = parts[1]
        section_code  = parts[2]
        main_sub = parts[3].split("-")            # ['5480','0006']
        main_key = main_sub[0]
        sub_key  = main_sub[1] if len(main_sub) > 1 else "0000"
        land_no  = main_key.zfill(4) + sub_key.zfill(4)
    except (IndexError, ValueError):
        return jsonify({"error": f"cadaster_id 格式不正確：{cadaster_id}"}), 400

    # 查段別名稱（反向查 sections 快取）
    sections = _foundi_get_sections(jwt_val)
    section_by_code = {v.get("section_code", ""): k for k, v in sections.items()}
    land_sect = section_by_code.get(section_code, "")

    # 呼叫 land/mapLocation 取土地分區
    try:
        import urllib.request as _ur
        import urllib.parse as _up
        params = {
            "city_code":     city_code,
            "locality_code": locality_code,
            "section_code":  section_code,
            "main_key":      main_key,
            "sub_key":       sub_key,
        }
        url = "https://api.foundi.info/land/mapLocation/?" + _up.urlencode(params)
        req = _ur.Request(url, headers={
            "authorization": jwt_val,
            "accept": "application/json",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        })
        with _ur.urlopen(req, timeout=10) as resp:
            land_data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        # 即使分區查詢失敗，至少回傳段別+地號
        return jsonify({"land_sect": land_sect, "land_no": land_no,
                        "warning": f"分區查詢失敗：{e}"})

    lands = land_data.get("lands", [])
    if not lands:
        return jsonify({"land_sect": land_sect, "land_no": land_no,
                        "warning": "查無土地分區資料"})

    info = lands[0]["info"]
    zone     = info.get("zone", [])
    sub_zone = info.get("sub_zone", [])
    zone_str = "、".join(zone)
    if sub_zone:
        zone_str += f"（{'、'.join(sub_zone)}）"

    bcr = info.get("building_coverage_ratio", 0)
    far = info.get("floor_area_ratio", 0)
    repr_coords = info.get("repr_point", {}).get("coordinates", [None, None])

    return jsonify({
        "land_sect":         land_sect,
        "land_no":           land_no,
        "use_zone":          zone_str,
        "building_coverage": round(bcr * 100),
        "floor_area_ratio":  round(far * 100),
        "declared_price":    info.get("unit_value_with_square_meter", 0),
        "current_value":     info.get("current_unit_value_with_square_meter", 0),
        "land_area":         info.get("land_area", 0),
        "land_area_m2":      info.get("land_area_in_square_meter", 0),  # ㎡
        "lat":               repr_coords[1],
        "lng":               repr_coords[0],
    })


@app.route("/api/foundi-building", methods=["POST"])
def api_foundi_building():
    """
    地址一鍵帶入：esDoorinfo 取建物資料 + cadaster_id → land/mapLocation 取土地分區
    輸入：{ "address": "台東縣台東市中山路123號" }
    輸出：建物欄位 + 地號欄位 + 土地分區欄位（全部）
    """
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]

    body = request.get_json(silent=True) or {}
    address = (body.get("address") or "").strip()
    if not address:
        return jsonify({"error": "缺少地址參數"}), 400

    jwt_val = session.get("foundi_jwt", "").strip()
    if not jwt_val:
        return jsonify({"error": "FOUNDI_JWT_NOT_SET"}), 401

    # 檢查 JWT 是否過期
    try:
        import base64 as _b64
        payload_b64 = jwt_val.split(".")[1]
        payload_b64 += "=" * ((4 - len(payload_b64) % 4) % 4)
        payload = json.loads(_b64.b64decode(payload_b64).decode("utf-8"))
        if payload.get("exp", 0) < time.time():
            return jsonify({"error": "FOUNDI JWT 已過期，請重新設定"}), 401
    except Exception:
        pass

    import urllib.request as _ur
    import urllib.parse as _up

    # ── Step 1：esDoorinfo → 建物基本資料 ──────────────────────────────
    try:
        url = ("https://agent.foundi.info/dataapi/address/esDoorinfo/?"
               + _up.urlencode({"address": address}))
        req = _ur.Request(url, headers={
            "authorization": jwt_val,
            "accept": "application/json, text/plain, */*",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "referer": "https://agent.foundi.info/",
        })
        with _ur.urlopen(req, timeout=10) as resp:
            http_status = resp.status
            raw = resp.read().decode("utf-8")
    except Exception as e:
        return jsonify({"error": f"FOUNDI 查詢失敗：{e}"}), 500

    if http_status == 204 or not raw.strip():
        return jsonify({"error": "此地址查無建物登記資料"}), 404

    try:
        data = json.loads(raw)
    except Exception:
        return jsonify({"error": f"無法解析回應（前200字）：{raw[:200]}"}), 500

    # 正規化成 dict
    if isinstance(data, list):
        item = data[0] if data else {}
    elif isinstance(data, dict):
        inner = data.get("results") or data.get("data") or data.get("building")
        item = (inner[0] if isinstance(inner, list) and inner
                else inner if isinstance(inner, dict)
                else data)
    else:
        item = {}

    # 實際資料在 info 子物件
    if "info" in item and isinstance(item["info"], dict):
        item = item["info"]

    # ── 建物坪數（m² → 坪）──
    area_ping = None
    for k in ("building_area_ping", "area_ping", "ping"):
        if item.get(k):
            area_ping = round(float(item[k]), 2); break
    if area_ping is None:
        for k in ("building_area", "area", "total_area"):
            if item.get(k):
                area_ping = round(float(item[k]) / 3.30579, 2); break

    # ── 建築完成日期 → 屋齡 ──
    completion_date = None
    age = None
    if item.get("completion_date"):
        completion_date = str(item["completion_date"])[:10]
        try:
            age = datetime.now().year - int(completion_date[:4])
        except Exception:
            pass

    # ── 樓層（floor 是 int 或 null；floors 是 list of {"area":x,"floor":y}）──
    raw_floor  = item.get("floor")
    floors_raw = item.get("floors", [])

    floor = int(raw_floor) if isinstance(raw_floor, (int, float)) else None

    if isinstance(floors_raw, list) and floors_raw:
        floor_nums = [int(f["floor"]) for f in floors_raw
                      if isinstance(f, dict) and f.get("floor") is not None]
        total_floors = max(floor_nums) if floor_nums else None
        if floor is None and floor_nums:
            floor = min(floor_nums)  # floor 未提供時，取最低所在樓層
    else:
        total_floors = None

    # ── cadaster_id → 地號 + 段別（供串接 land/mapLocation）──
    # 格式：V_04_0009_5480-0006（城市_鄉鎮_段碼_主號-次號）
    cadaster_id = item.get("cadaster_id") or ""   # 可能是 null，統一成空字串
    cadaster_base = item.get("cadaster_base") or ""  # 備用（建號）
    land_sect_name = None
    land_no        = None
    zone_result    = {}

    if cadaster_id:
        try:
            parts = cadaster_id.split("_")          # ['V','04','0009','5480-0006']
            city_code     = parts[0]
            locality_code = parts[1]
            section_code  = parts[2]
            main_sub      = parts[3].split("-")     # ['5480','0006']
            main_key      = main_sub[0]
            sub_key       = main_sub[1] if len(main_sub) > 1 else "0000"

            # 8碼地號 = 主號左補零到4碼 + 次號左補零到4碼
            land_no = main_key.zfill(4) + sub_key.zfill(4)

            # 查段別名稱（反向查快取）
            sections = _foundi_get_sections(jwt_val)
            section_by_code = {v.get("section_code", ""): k for k, v in sections.items()}
            land_sect_name = section_by_code.get(section_code, "")

            # ── Step 2：land/mapLocation → 土地分區 ──
            params = {
                "city_code":     city_code,
                "locality_code": locality_code,
                "section_code":  section_code,
                "main_key":      main_key,
                "sub_key":       sub_key,
            }
            land_url = "https://api.foundi.info/land/mapLocation/?" + _up.urlencode(params)
            land_req = _ur.Request(land_url, headers={
                "authorization": jwt_val,
                "accept": "application/json",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            })
            with _ur.urlopen(land_req, timeout=10) as land_resp:
                land_data = json.loads(land_resp.read().decode("utf-8"))

            lands = land_data.get("lands", [])
            if lands:
                info = lands[0]["info"]
                zone     = info.get("zone", [])
                sub_zone = info.get("sub_zone", [])
                zone_str = "、".join(zone)
                if sub_zone:
                    zone_str += f"（{'、'.join(sub_zone)}）"
                bcr = info.get("building_coverage_ratio", 0)
                far = info.get("floor_area_ratio", 0)
                repr_coords = info.get("repr_point", {}).get("coordinates", [None, None])
                zone_result = {
                    "use_zone":          zone_str,
                    "building_coverage": round(bcr * 100),
                    "floor_area_ratio":  round(far * 100),
                    "declared_price":    info.get("unit_value_with_square_meter", 0),
                    "current_value":     info.get("current_unit_value_with_square_meter", 0),
                    "land_area":         info.get("land_area", 0),
                    "land_area_m2":      info.get("land_area_in_square_meter", 0),  # ㎡
                    "lat":               repr_coords[1],
                    "lng":               repr_coords[0],
                }
        except Exception as e:
            import logging
            logging.warning(f"地號/分區串接失敗：{e}")

    # ── 組合回傳 ──
    result = {
        # 建物欄位
        "building_ping":   area_ping,
        "floor":           floor,
        "total_floors":    total_floors,
        "age":             age,
        "completion_date": completion_date,
        # 地號欄位
        "cadaster_id":     cadaster_id or None,
        "land_sect":       land_sect_name or None,
        "land_no":         land_no or None,
        **zone_result,
    }
    return jsonify({k: v for k, v in result.items() if v is not None})


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
    log_event("price_search", user_id=email, detail={"query": query, "district": district})
    min_price = body.get("min_price")   # 萬元
    max_price = body.get("max_price")
    min_ping = body.get("min_ping")     # 坪
    max_ping = body.get("max_ping")
    sort_by = (body.get("sort") or "date_desc").strip()
    limit = min(int(body.get("limit") or 100), 500)
    # 建物型態（多選，"土地" 為特殊值 → 篩 transaction_type='土地'）
    building_types = body.get("building_types") or []
    # 使用分區（都市 / 非都市）
    filter_urban = bool(body.get("filter_urban"))
    urban_zones = body.get("urban_zones") or []          # ["住","商","工","農","其他"]
    filter_non_urban = bool(body.get("filter_non_urban"))
    non_urban_zones = body.get("non_urban_zones") or []  # ["特定農業區", ...]
    non_urban_uses  = body.get("non_urban_uses")  or []  # ["農牧用地", ...]
    # 交易期間（YYYY-MM-DD 格式）
    date_from = (body.get("date_from") or "").strip()   # e.g. "2023-01-01"
    date_to   = (body.get("date_to")   or "").strip()   # e.g. "2024-12-31"
    # 屋齡範圍
    age_min = body.get("age_min")   # 年
    age_max = body.get("age_max")
    # 方圓範圍
    center_lat = body.get("center_lat")
    center_lng = body.get("center_lng")
    radius_km  = body.get("radius_km")

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

        # 交易期間
        rec_date = r.get("date") or ""
        if date_from and rec_date < date_from:
            continue
        if date_to   and rec_date > date_to:
            continue

        # 建物面積範圍（坪）
        ping = r.get("building_ping", 0)
        if min_ping is not None and ping < float(min_ping):
            continue
        if max_ping is not None and ping > float(max_ping):
            continue

        # 屋齡範圍
        age = r.get("age", 0) or 0
        if age_min is not None and age < int(age_min):
            continue
        if age_max is not None and age > int(age_max):
            continue

        # 建物型態多選
        if building_types:
            _BT_FULL = {
                "公寓": "公寓(5樓含以下無電梯)",
                "透天厝": "透天厝",
                "華廈": "華廈(10層含以下有電梯)",
                "住宅大樓": "住宅大樓(11層含以上有電梯)",
                "套房": "套房(1房1廳1衛)",
                "店面": "店面(店鋪)",
                "農舍": "農舍",
                "辦公商業大樓": "辦公商業大樓",
                "廠辦": "廠辦",
                "工廠": "工廠",
                "倉庫": "倉庫",
            }
            has_land  = "土地" in building_types
            bt_values = [_BT_FULL[b] for b in building_types if b in _BT_FULL]
            r_bt = r.get("building_type", "").strip()
            r_tx = r.get("transaction_type", "").strip()
            if not ((has_land and r_tx == "土地") or (bt_values and r_bt in bt_values)):
                continue

        # 使用分區：都市土地
        if filter_urban:
            r_uz = (r.get("urban_zone") or "").strip()
            if not r_uz:
                continue  # 非都市土地，排除
            if urban_zones:
                matched = False
                for z in urban_zones:
                    if z == "其他":
                        if not any(c in r_uz for c in ("住", "商", "工", "農")):
                            matched = True; break
                    elif z in r_uz:
                        matched = True; break
                if not matched:
                    continue

        # 使用分區：非都市土地
        if filter_non_urban:
            r_nuz = (r.get("non_urban_zone") or "").strip()
            r_nuu = (r.get("non_urban_use")  or "").strip()
            if not r_nuz and not r_nuu:
                continue  # 都市土地，排除
            if non_urban_zones and r_nuz not in non_urban_zones:
                continue
            if non_urban_uses and r_nuu not in non_urban_uses:
                continue

        # 方圓範圍（需有座標）
        if center_lat is not None and center_lng is not None and radius_km:
            r_lat = r.get("lat")
            r_lng = r.get("lng")
            if not r_lat or not r_lng:
                continue
            if _haversine_m(float(center_lat), float(center_lng), r_lat, r_lng) > float(radius_km) * 1000:
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

    # ── Step 2.5：地點統計 — 計算本地 vs 鄉鎮均價，提供 AI 地點加成依據 ──
    location_stats_text = ""
    if lat_val and lng_val:
        nearby_units = []
        for r in all_data:
            if (r.get('date') or '') < three_years_ago:
                continue
            if '預售屋' in r.get('transaction_type', ''):
                continue
            if tx_type and tx_type not in r.get('transaction_type', ''):
                continue
            if not (r.get('lat') and r.get('lng')):
                continue
            if r.get('unit_price', 0) <= 0:
                continue
            dist_m = _haversine_m(lat_val, lng_val, r['lat'], r['lng'])
            if dist_m <= 1000:
                nearby_units.append(r['unit_price'])

        district_units = [
            r['unit_price'] for r in all_data
            if (r.get('date') or '') >= three_years_ago
            and (not district or r.get('district') == district)
            and '預售屋' not in r.get('transaction_type', '')
            and (not tx_type or tx_type in r.get('transaction_type', ''))
            and r.get('unit_price', 0) > 0
        ]

        if nearby_units and district_units:
            local_avg = round(sum(nearby_units) / len(nearby_units), 1)
            dist_avg  = round(sum(district_units) / len(district_units), 1)
            ratio = local_avg / dist_avg if dist_avg > 0 else 1.0
            if ratio >= 1.3:
                tier = "明顯高於區域均價（精華路段／蛋黃區）"
            elif ratio >= 1.1:
                tier = "高於區域均價（較佳地段）"
            elif ratio >= 0.9:
                tier = "接近區域均價（一般地段）"
            elif ratio >= 0.75:
                tier = "低於區域均價（較偏遠地段）"
            else:
                tier = "明顯低於區域均價（偏遠／非精華區）"
            location_stats_text = (
                f"\n- 待估地點方圓1公里同類均價：{local_avg}萬/坪"
                f"（{len(nearby_units)}筆，近3年）"
                f"\n- {district or '全縣'}同類均價：{dist_avg}萬/坪"
                f"（{len(district_units)}筆）"
                f"\n- 地點相對位階：{tier}（本地/區域比 = {ratio:.2f}）"
            )
        elif district_units:
            dist_avg = round(sum(district_units) / len(district_units), 1)
            location_stats_text = (
                f"\n- {district or '全縣'}同類均價：{dist_avg}萬/坪"
                f"（{len(district_units)}筆）"
                f"\n- 待估地點方圓1公里成交案例不足，無法計算本地均價"
            )

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

    prompt = _VALUATION_PROMPT_TPL.substitute(
        subject_conditions="\n".join(subject_parts),
        location_note=location_note,
        comparables_list="\n".join(comp_lines),
        comp_count=len(comparables),
        confidence_label=confidence_label,
        confidence_score=confidence_score,
        median_price=median_price,
        avg_price=avg_price,
        avg_unit=avg_unit,
        zone_note=zone_note,
        location_stats=location_stats_text,
    )

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
        'data_warnings':  ai_result.get('data_warnings', []),
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
