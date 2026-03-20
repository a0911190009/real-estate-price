# -*- coding: utf-8 -*-
"""
房仲工具 — 實價登錄調查（real-estate-price）
查詢內政部實價登錄、附近成交價，與物件庫整合。
資料儲存在 GCS：price/price_data_v.json（臺東縣）
"""

import os
import json
import time
import threading
from datetime import timedelta, datetime
from flask import Flask, request, session, redirect, jsonify, send_from_directory
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5010)), debug=bool(os.environ.get("FLASK_DEBUG")))
