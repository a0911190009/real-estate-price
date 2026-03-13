# -*- coding: utf-8 -*-
"""
房仲工具 — 實價登錄調查（real-estate-price）
查詢內政部實價登錄、附近成交價，與物件庫整合。
目前為最小架構，後續可加入：批次匯入 CSV、依地址查詢、與物件庫連動。
"""

import os
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

PORTAL_URL = (os.environ.get("PORTAL_URL") or "").strip()
ADMIN_EMAILS = [e.strip() for e in (os.environ.get("ADMIN_EMAILS") or "").split(",") if e.strip()]

TOKEN_SERIALIZER = URLSafeTimedSerializer(app.secret_key)
TOKEN_MAX_AGE = 300


def _is_admin(email):
    return email in ADMIN_EMAILS


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


@app.route("/")
def index():
    """未登入時導向 Portal；已登入由前端處理。"""
    if session.get("user_email"):
        return send_from_directory("static", "index.html")
    return redirect(PORTAL_URL or "/")


# ── 假資料：台東地區近期成交紀錄 ──────────────────────────────────────
_FAKE_DATA = [
    {
        "id": "1",
        "address": "台東縣台東市中華路一段 120 號",
        "district": "台東市",
        "type": "住宅大樓",
        "total_price": 580,        # 萬元
        "unit_price": 14.2,        # 萬/坪
        "building_ping": 40.8,     # 坪
        "floor": "5/12",
        "age": 12,
        "date": "2025-11",
        "lat": 22.7583,
        "lng": 121.1444,
    },
    {
        "id": "2",
        "address": "台東縣台東市更生路 88 號",
        "district": "台東市",
        "type": "公寓",
        "total_price": 320,
        "unit_price": 10.5,
        "building_ping": 30.5,
        "floor": "3/5",
        "age": 28,
        "date": "2025-10",
        "lat": 22.7521,
        "lng": 121.1502,
    },
    {
        "id": "3",
        "address": "台東縣台東市四維路 210 號",
        "district": "台東市",
        "type": "透天厝",
        "total_price": 950,
        "unit_price": 16.8,
        "building_ping": 56.5,
        "floor": "1/3",
        "age": 20,
        "date": "2025-12",
        "lat": 22.7650,
        "lng": 121.1380,
    },
    {
        "id": "4",
        "address": "台東縣台東市正氣路 45 號",
        "district": "台東市",
        "type": "住宅大樓",
        "total_price": 460,
        "unit_price": 13.1,
        "building_ping": 35.1,
        "floor": "8/14",
        "age": 8,
        "date": "2025-09",
        "lat": 22.7555,
        "lng": 121.1460,
    },
    {
        "id": "5",
        "address": "台東縣卑南鄉知本路三段 300 號",
        "district": "卑南鄉",
        "type": "透天厝",
        "total_price": 780,
        "unit_price": 12.3,
        "building_ping": 63.4,
        "floor": "1/3",
        "age": 15,
        "date": "2025-11",
        "lat": 22.7012,
        "lng": 121.0845,
    },
    {
        "id": "6",
        "address": "台東縣台東市鐵花路 60 號",
        "district": "台東市",
        "type": "店面",
        "total_price": 1200,
        "unit_price": 30.2,
        "building_ping": 39.7,
        "floor": "1/6",
        "age": 35,
        "date": "2026-01",
        "lat": 22.7600,
        "lng": 121.1430,
    },
    {
        "id": "7",
        "address": "台東縣台東市長沙街 15 號",
        "district": "台東市",
        "type": "公寓",
        "total_price": 290,
        "unit_price": 9.8,
        "building_ping": 29.6,
        "floor": "2/4",
        "age": 40,
        "date": "2025-08",
        "lat": 22.7540,
        "lng": 121.1490,
    },
    {
        "id": "8",
        "address": "台東縣台東市大同路 180 號",
        "district": "台東市",
        "type": "住宅大樓",
        "total_price": 620,
        "unit_price": 15.5,
        "building_ping": 40.0,
        "floor": "10/16",
        "age": 5,
        "date": "2026-02",
        "lat": 22.7570,
        "lng": 121.1415,
    },
]


@app.route("/api/search", methods=["POST"])
def api_search():
    """
    查詢附近成交紀錄。
    目前回傳假資料，之後換成真實資料庫查詢。
    輸入：{ "query": "地址或區域", "lat": 22.75, "lng": 121.14, "radius_m": 2000 }
    """
    email, err = _require_user()
    if err:
        return jsonify(err[0]), err[1]

    data = request.get_json() or {}
    query = (data.get("query") or "").strip()
    radius_m = int(data.get("radius_m") or 2000)

    # 目前直接回傳全部假資料，之後可依 query/lat/lng 過濾
    results = _FAKE_DATA

    # 計算統計摘要
    if results:
        prices = [r["total_price"] for r in results]
        unit_prices = [r["unit_price"] for r in results]
        summary = {
            "count": len(results),
            "avg_total": round(sum(prices) / len(prices), 1),
            "max_total": max(prices),
            "min_total": min(prices),
            "avg_unit": round(sum(unit_prices) / len(unit_prices), 1),
            "max_unit": max(unit_prices),
            "min_unit": min(unit_prices),
        }
    else:
        summary = {"count": 0}

    return jsonify({
        "results": results,
        "summary": summary,
        "query": query,
        "radius_m": radius_m,
        "is_mock": True,   # 標記這是假資料，前端可顯示提示
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5010)), debug=bool(os.environ.get("FLASK_DEBUG")))
