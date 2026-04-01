"""Resolve public site origin from environment (email links, etc.).

Typical absolute links (after ``resolve_public_base_url()``):

- Recruitment careers list: ``{base}{RECRUITMENT_VACANCIES_PATH}`` → ``…/vacancies``
- Employee portal entry: ``{base}{EMPLOYEE_PORTAL_PUBLIC_PATH}`` → ``…/employee-portal``
  (unauthenticated visitors are redirected to ``/employee-portal/login``).
"""
from __future__ import annotations

import os

# Path suffixes for outbound email / docs (single-origin with public base URL).
RECRUITMENT_VACANCIES_PATH = "/vacancies"
EMPLOYEE_PORTAL_PUBLIC_PATH = "/employee-portal"


def resolve_public_base_url(*, extra_env_keys: tuple[str, ...] = ()) -> str:
    """
    Checks ``extra_env_keys`` first, then ``SPARROW_PUBLIC_BASE_URL``,
    ``PUBLIC_BASE_URL``, then ``RAILWAY_PUBLIC_DOMAIN`` (Railway sets the hostname only;
    ``https://`` is prepended when no scheme is present).
    """
    for key in extra_env_keys + ("SPARROW_PUBLIC_BASE_URL", "PUBLIC_BASE_URL"):
        u = (os.environ.get(key) or "").strip().rstrip("/")
        if u:
            return u
    dom = (os.environ.get("RAILWAY_PUBLIC_DOMAIN") or "").strip()
    if not dom:
        return ""
    if "://" in dom:
        return dom.rstrip("/")
    return f"https://{dom.lstrip('/')}".rstrip("/")
