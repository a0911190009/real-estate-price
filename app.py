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
    return jsonify({"portal_url": PORTAL_URL or "/"})


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5010)), debug=bool(os.environ.get("FLASK_DEBUG")))
