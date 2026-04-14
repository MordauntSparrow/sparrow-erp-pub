"""
Core branding helpers: logo dimensions in ``site_settings`` (Core manifest).

Saved as integers (px). Templates use :func:`navbar_logo_inline_style` and
:func:`auth_logo_inline_style` so missing or legacy manifests still render safely.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, MutableMapping

DEFAULT_LOGO_NAVBAR_MAX_HEIGHT_PX = 48
DEFAULT_LOGO_NAVBAR_MAX_WIDTH_PX = 300
DEFAULT_LOGO_AUTH_MAX_HEIGHT_PX = 128
DEFAULT_LOGO_AUTH_MAX_WIDTH_PX = 380

_LOGO_SITE_DEFAULTS: dict[str, Any] = {
    "company_name": "Sparrow ERP",
    "branding": "name",
    "logo_path": "",
    "logo_navbar_max_height_px": DEFAULT_LOGO_NAVBAR_MAX_HEIGHT_PX,
    "logo_navbar_max_width_px": DEFAULT_LOGO_NAVBAR_MAX_WIDTH_PX,
    "logo_auth_max_height_px": DEFAULT_LOGO_AUTH_MAX_HEIGHT_PX,
    "logo_auth_max_width_px": DEFAULT_LOGO_AUTH_MAX_WIDTH_PX,
}


def clamp_logo_navbar_max_height_px(raw: Any) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_LOGO_NAVBAR_MAX_HEIGHT_PX
    return max(32, min(120, v))


def clamp_logo_navbar_max_width_px(raw: Any) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_LOGO_NAVBAR_MAX_WIDTH_PX
    return max(120, min(420, v))


def clamp_logo_auth_max_height_px(raw: Any) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_LOGO_AUTH_MAX_HEIGHT_PX
    return max(72, min(280, v))


def clamp_logo_auth_max_width_px(raw: Any) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_LOGO_AUTH_MAX_WIDTH_PX
    return max(160, min(520, v))


def merge_site_settings_defaults(raw: Any) -> dict[str, Any]:
    """Deep-merge branding defaults into ``site_settings`` (manifest fragment)."""
    if isinstance(raw, Mapping):
        return {**_LOGO_SITE_DEFAULTS, **dict(raw)}
    return dict(_LOGO_SITE_DEFAULTS)


def navbar_logo_inline_style(site: Any) -> str:
    """CSS for admin / app navbars (max height + width, object-fit)."""
    d = dict(site) if isinstance(site, Mapping) else {}
    h = clamp_logo_navbar_max_height_px(d.get("logo_navbar_max_height_px"))
    w = clamp_logo_navbar_max_width_px(d.get("logo_navbar_max_width_px"))
    return (
        f"max-height:{h}px;max-width:{w}px;width:auto;height:auto;"
        f"object-fit:contain;vertical-align:middle;"
    )


def auth_logo_inline_style(site: Any) -> str:
    """CSS for login / password-reset / portal auth headers."""
    d = dict(site) if isinstance(site, Mapping) else {}
    h = clamp_logo_auth_max_height_px(d.get("logo_auth_max_height_px"))
    w = clamp_logo_auth_max_width_px(d.get("logo_auth_max_width_px"))
    return (
        f"max-height:{h}px;max-width:{w}px;width:auto;height:auto;"
        f"object-fit:contain;vertical-align:middle;"
    )


def parse_logo_dimension_from_form(
    form_value: Any, clamp_fn, fallback: int
) -> int:
    """Parse POST field; invalid values use ``fallback``."""
    if form_value is None:
        return fallback
    s = str(form_value).strip()
    if not s:
        return fallback
    try:
        return clamp_fn(int(float(s)))
    except ValueError:
        return fallback


def apply_logo_dimensions_to_site_settings(
    target: MutableMapping[str, Any], form: Mapping[str, Any]
) -> None:
    """Write clamped logo size keys onto ``target`` (``site_settings`` dict)."""
    target["logo_navbar_max_height_px"] = parse_logo_dimension_from_form(
        form.get("logo_navbar_max_height_px"),
        clamp_logo_navbar_max_height_px,
        DEFAULT_LOGO_NAVBAR_MAX_HEIGHT_PX,
    )
    target["logo_navbar_max_width_px"] = parse_logo_dimension_from_form(
        form.get("logo_navbar_max_width_px"),
        clamp_logo_navbar_max_width_px,
        DEFAULT_LOGO_NAVBAR_MAX_WIDTH_PX,
    )
    target["logo_auth_max_height_px"] = parse_logo_dimension_from_form(
        form.get("logo_auth_max_height_px"),
        clamp_logo_auth_max_height_px,
        DEFAULT_LOGO_AUTH_MAX_HEIGHT_PX,
    )
    target["logo_auth_max_width_px"] = parse_logo_dimension_from_form(
        form.get("logo_auth_max_width_px"),
        clamp_logo_auth_max_width_px,
        DEFAULT_LOGO_AUTH_MAX_WIDTH_PX,
    )
