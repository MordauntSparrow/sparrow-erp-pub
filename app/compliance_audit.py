"""
Security / access events for rotating audit.log (accountability, DSPT logging expectations).
Avoid logging raw credentials; use pseudonymous hints for failed auth where helpful.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def pseudonymous_username_hint(username: Optional[str]) -> str:
    """Short stable hash for correlation without storing plaintext username on failure."""
    if not username or not str(username).strip():
        return ""
    normalized = str(username).strip().lower().encode("utf-8", errors="ignore")
    digest = hashlib.sha256(normalized).hexdigest()[:16]
    return f"sha256_16:{digest}"


def log_security_event(event_type: str, **details: Any) -> None:
    """Emit one JSON object as the log message line (works with JsonFormatter in create_app)."""
    try:
        logger = logging.getLogger("audit")
        row = {"event": event_type, "ts": _utc_iso()}
        for key, val in details.items():
            if val is not None and val != "":
                row[key] = val
        logger.info(json.dumps(row, default=str))
    except Exception:
        pass
