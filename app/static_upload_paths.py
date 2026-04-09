"""
Normalize paths stored in core manifest for files under app/static (uploads, logos, etc.).
"""
from __future__ import annotations

import os

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_ROOT = os.path.normpath(os.path.join(_APP_DIR, "static"))


def normalize_manifest_static_path(stored: str | None) -> str:
    """
    Return a path relative to the Flask static folder (e.g. uploads/foo.png), or ''.

    Accepts legacy values like 'static/uploads/foo.png' or Windows backslashes.
    Rejects path traversal.
    """
    if stored is None:
        return ""
    s = str(stored).strip()
    if not s:
        return ""
    s = s.replace("\\", "/").lstrip("/")
    while s.startswith("./"):
        s = s[2:]
    low = s.lower()
    if low.startswith("static/"):
        s = s[7:]
    parts = [p for p in s.split("/") if p and p != "."]
    if any(p == ".." for p in parts):
        return ""
    return "/".join(parts)


def static_upload_file_exists(stored_manifest_path: str | None) -> bool:
    """True if the manifest path resolves to an existing file under app/static/."""
    rel = normalize_manifest_static_path(stored_manifest_path)
    if not rel:
        return False
    full = os.path.normpath(os.path.join(STATIC_ROOT, rel))
    root = os.path.normpath(STATIC_ROOT)
    full_c = os.path.normcase(full)
    root_c = os.path.normcase(root)
    if full_c != root_c and not full_c.startswith(root_c + os.sep):
        return False
    return os.path.isfile(full)
