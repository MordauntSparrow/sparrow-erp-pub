"""Fernet encrypt/decrypt for integration token JSON (SECRET_KEY-derived)."""
from __future__ import annotations

import base64
import hashlib
import json
from typing import Any, Mapping

from cryptography.fernet import Fernet, InvalidToken
from flask import current_app


def _fernet() -> Fernet:
    secret = current_app.config.get("SECRET_KEY") or "defaultsecretkey"
    key = hashlib.sha256(str(secret).encode("utf-8")).digest()
    fernet_key = base64.urlsafe_b64encode(key)
    return Fernet(fernet_key)


def encrypt_token_payload(data: Mapping[str, Any]) -> str:
    raw = json.dumps(dict(data), separators=(",", ":"), sort_keys=True)
    return _fernet().encrypt(raw.encode("utf-8")).decode("utf-8")


def decrypt_token_payload(blob: str | None) -> dict[str, Any]:
    if not blob:
        return {}
    try:
        raw = _fernet().decrypt(str(blob).encode("utf-8")).decode("utf-8")
        out = json.loads(raw)
        return out if isinstance(out, dict) else {}
    except (InvalidToken, json.JSONDecodeError, TypeError):
        return {}
