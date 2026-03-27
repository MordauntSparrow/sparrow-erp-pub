"""
Session JWT for API clients (mobile, MDT, integrations) using Authorization: Bearer.
Uses JWT_SECRET_KEY or SECRET_KEY; tokens are short-lived.
Requires PyJWT (pip install PyJWT), not python-jwt.
"""
import os
from datetime import datetime, timedelta, timezone

pyjwt = None
try:
    import jwt as _jwt
    if callable(getattr(_jwt, "encode", None)) and callable(getattr(_jwt, "decode", None)):
        pyjwt = _jwt
except ImportError:
    pass

# Default lifetime when not specified (18h allows a full shift)
DEFAULT_EXPIRY_HOURS = 18


def _get_secret():
    s = os.environ.get("JWT_SECRET_KEY") or os.environ.get("SECRET_KEY", "defaultsecretkey")
    _prod = (os.environ.get("FLASK_ENV") or "").strip().lower() == "production" or (
        os.environ.get("RAILWAY_ENVIRONMENT") or ""
    ).strip().lower() == "production"
    if _prod and (not s or s == "defaultsecretkey"):
        raise RuntimeError(
            "Set JWT_SECRET_KEY or a strong SECRET_KEY in production for API tokens."
        )
    return s


def _get_expiry_hours():
    try:
        return int(os.environ.get("SESSION_TOKEN_EXPIRY_HOURS", DEFAULT_EXPIRY_HOURS))
    except ValueError:
        return DEFAULT_EXPIRY_HOURS


def encode_session_token(user_id, username: str, role: str, expiry_hours: int = None) -> str:
    """
    Encode a short-lived JWT for API use. Payload: sub=user_id, username, role, exp, iat.
    user_id can be int or str (e.g. UUID). Returns the token string, or empty string if JWT not available.
    """
    if not pyjwt:
        return ""
    try:
        expiry = expiry_hours if expiry_hours is not None else _get_expiry_hours()
        now = datetime.now(timezone.utc)
        exp = now + timedelta(hours=expiry)
        payload = {
            "sub": user_id if isinstance(user_id, (int, str)) else str(user_id),
            "username": str(username),
            "role": str(role),
            "iat": int(now.timestamp()),
            "exp": int(exp.timestamp()),
        }
        out = pyjwt.encode(
            payload,
            _get_secret(),
            algorithm="HS256",
        )
        return out if isinstance(out, str) else out.decode("utf-8")
    except RuntimeError:
        raise
    except (AttributeError, TypeError, Exception):
        return ""


def decode_session_token(token: str):
    """
    Decode and validate a session JWT. Returns payload dict (with sub, username, role)
    or None if invalid/expired/missing JWT lib.
    """
    if not pyjwt or not token or not token.strip():
        return None
    try:
        secret = _get_secret()
    except RuntimeError:
        raise
    try:
        payload = pyjwt.decode(
            token.strip(),
            secret,
            algorithms=["HS256"],
        )
        sub = payload.get("sub")
        if sub is not None and payload.get("username") and payload.get("role"):
            # Keep sub as-is (int or str e.g. UUID)
            if isinstance(sub, float) and sub == int(sub):
                payload["sub"] = int(sub)
            return payload
    except Exception:
        pass
    return None
