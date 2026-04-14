"""
Tenant industry / category profile (Core manifest ``organization_profile.industries``).

Admins set this under **Core settings → General** (multi-select). Slugs:
``medical``, ``security``, ``cleaning``, ``hospitality``.

**Templates**

.. code-block:: html+jinja

    {% if industry_visible('medical') %}
    {% if industry_visible('medical', 'security') %}  {# either #}

**Python**

.. code-block:: python

    from app.organization_profile import tenant_matches_industry, normalize_organization_industries

    ids = normalize_organization_industries(current_app.config.get("organization_industries"))
    if tenant_matches_industry(ids, "hospitality"):
        …

Legacy value ``medical_security`` (old combined checkbox) normalises to **both**
``medical`` and ``security`` so existing manifests keep the same effective access.

Empty or invalid saved values normalize to ``["medical"]``.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, List, Sequence, Set

# (slug, label, help) — slug is stable for code and demo-data keys; order = UI order
INDUSTRY_OPTIONS: tuple[tuple[str, str, str], ...] = (
    (
        "medical",
        "Medical",
        "Clinical and care settings — patient workflows, compliance, and medical-specific fields where modules support them.",
    ),
    (
        "security",
        "Security",
        "Static and mobile guarding, patrols, keys, incident reporting — security operations and workforce patterns.",
    ),
    (
        "cleaning",
        "Cleaning",
        "Contract cleaning, facilities hygiene, site-based rounds — field and service-delivery oriented options.",
    ),
    (
        "hospitality",
        "Hospitality",
        "Hotels, venues, food service, guest-facing operations — staffing and service patterns for that sector.",
    ),
)

VALID_INDUSTRY_SLUGS: frozenset[str] = frozenset(
    s for s, _, _ in INDUSTRY_OPTIONS)

# Older catalog / ad-hoc values → current slug (best-effort migration)
_LEGACY_INDUSTRY_SLUG_MAP: dict[str, str] = {
    "general": "medical",
    "healthcare": "medical",
    "ems": "medical",
    "social_care": "medical",
    "field_services": "cleaning",
    "construction": "cleaning",
    "professional_services": "hospitality",
}

DEFAULT_INDUSTRY_FALLBACK = "medical"

DEFAULT_ORGANIZATION_PROFILE: dict[str, Any] = {
    "industries": [DEFAULT_INDUSTRY_FALLBACK]}


def _normalised_raw_key(token: str) -> str:
    s = (
        str(token)
        .strip()
        .lower()
        .replace("&", " and ")
        .replace(" ", "_")
        .replace("-", "_")
    )
    while "__" in s:
        s = s.replace("__", "_")
    return s


def _is_legacy_medical_security_combo(token: str) -> bool:
    """Former combined option / free text → expand to medical + security."""
    return _normalised_raw_key(token) in ("medical_security", "medical_and_security")


def _canonical_industry_slug(token: str) -> str | None:
    s = _normalised_raw_key(token)
    if not s:
        return None
    if s in VALID_INDUSTRY_SLUGS:
        return s
    mapped = _LEGACY_INDUSTRY_SLUG_MAP.get(s)
    if mapped and mapped in VALID_INDUSTRY_SLUGS:
        return mapped
    return None


def normalize_organization_industries(raw: Any) -> List[str]:
    """
    Return a deduplicated list of valid industry slugs, preserving INDUSTRY_OPTIONS order.
    Unknown values are dropped. Legacy ``medical_security`` expands to medical + security.
    Empty result → ``["medical"]``.
    """
    if raw is None:
        return [DEFAULT_INDUSTRY_FALLBACK]
    if isinstance(raw, str):
        items: Sequence[Any] = [raw]
    elif isinstance(raw, (list, tuple, set)):
        items = raw
    else:
        return [DEFAULT_INDUSTRY_FALLBACK]

    order_index = {s: i for i, (s, _, _) in enumerate(INDUSTRY_OPTIONS)}
    seen: Set[str] = set()
    picked: List[str] = []
    for x in items:
        if _is_legacy_medical_security_combo(x):
            for sub in ("medical", "security"):
                if sub not in seen:
                    seen.add(sub)
                    picked.append(sub)
            continue
        can = _canonical_industry_slug(x)
        if can is None or can in seen:
            continue
        seen.add(can)
        picked.append(can)

    picked.sort(key=lambda slug: order_index.get(slug, 999))
    if not picked:
        return [DEFAULT_INDUSTRY_FALLBACK]
    return picked


def expand_tenant_industry_slugs(slugs: Iterable[str]) -> Set[str]:
    """Set of normalised tenant industry slugs (same as ``set(normalize(...))``)."""
    return set(normalize_organization_industries(list(slugs)))


def industries_from_manifest(manifest: Any) -> List[str]:
    """Read ``organization_profile.industries`` from a core manifest dict."""
    if not isinstance(manifest, dict):
        return [DEFAULT_INDUSTRY_FALLBACK]
    op = manifest.get("organization_profile")
    if not isinstance(op, dict):
        return [DEFAULT_INDUSTRY_FALLBACK]
    return normalize_organization_industries(op.get("industries"))


def migrate_core_manifest_organization_profile_defaults(manifest_path: str) -> bool:
    """
    If the core manifest has no usable ``organization_profile.industries``, persist
    ``[\"medical\"]``. Safe on every boot (idempotent). Helps upgraded deployments whose
    JSON predates this field so behaviour matches runtime normalization without relying
    on an admin save.
    """
    path = Path(manifest_path)
    if not path.is_file():
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError, TypeError):
        return False
    if not isinstance(data, dict):
        return False
    changed = False
    op = data.get("organization_profile")
    if not isinstance(op, dict):
        data["organization_profile"] = dict(DEFAULT_ORGANIZATION_PROFILE)
        changed = True
    else:
        raw = op.get("industries")
        if raw is None or not isinstance(raw, (list, tuple)) or len(raw) == 0:
            op["industries"] = [DEFAULT_INDUSTRY_FALLBACK]
            changed = True
    if not changed:
        return False
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
    except OSError:
        return False
    return True


def load_core_manifest_dict() -> dict[str, Any]:
    """Load ``app/config/manifest.json`` for install/upgrade scripts (no Flask app required)."""
    path = Path(__file__).resolve().parent / "config" / "manifest.json"
    if not path.is_file():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError, TypeError):
        return {}


def load_tenant_industries_for_install() -> List[str]:
    """Normalised industry slugs from Core manifest (same source as runtime ``organization_industries``)."""
    return industries_from_manifest(load_core_manifest_dict())


def tenant_matches_industry(
    tenant_industries: Iterable[str], *required: str
) -> bool:
    """
    True if the tenant matches at least one of ``required`` (OR). Slugs are canonicalised.
    Legacy ``medical_security`` in ``required`` means **both** ``medical`` and ``security``
    must be selected (for old templates); unknown required tokens are ignored; if none are
    valid, returns True (same as before).
    """
    if not required:
        return True
    tset = expand_tenant_industry_slugs(tenant_industries)
    any_valid = False
    for r in required:
        s = str(r)
        if _is_legacy_medical_security_combo(s):
            any_valid = True
            if "medical" in tset and "security" in tset:
                return True
            continue
        can = _canonical_industry_slug(s)
        if can:
            any_valid = True
            if can in tset:
                return True
    if not any_valid:
        return True
    return False
