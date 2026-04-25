"""
Collect per-module cards for the core Integrations admin page.

Plugins declare usage in ``manifest.json`` / ``factory_manifest.json`` under
``accounting_integration_consumer`` (one object) or a list of objects for
multiple cards from the same module.

Each consumer entry may include:

- ``title`` (optional) — card heading; defaults to the plugin ``name``.
- ``summary`` (optional) — short description.
- ``provider_ids`` (optional list) — subset of accounting providers this feature
  targets; shown as human-readable names. Unknown ids are ignored.
- ``permission_ids`` (optional list) — permission strings; labels are resolved
  from the global permission catalog, then from the plugin's
  ``declared_permissions``.
- ``options`` (optional list of rows) — each row has ``label`` and exactly one
  value source:

  - ``value`` — static string (or any JSON-serialisable value, stringified).
  - ``integration_setting`` — key in core integration deployment settings
    (``default_provider``, ``auto_draft_invoice``, ``auto_draft_trigger``).
  - ``core_manifest_key`` — dotted path into the merged core manifest dict.
  - ``plugin_settings_key`` — dotted path under the plugin manifest's
    ``settings`` object (unwraps ``{"value": ...}`` leaves).

Only **enabled** plugins contribute cards. Empty or invalid declarations are
skipped.
"""
from __future__ import annotations

from typing import Any

from app.core_integrations import repository as _int_repo
from app.core_integrations.catalog import LIVE_PROVIDERS

_PROVIDER_TITLES: dict[str, str] = {p["id"]: p["title"] for p in LIVE_PROVIDERS}


def _deep_get(root: dict[str, Any] | None, path: str) -> Any:
    cur: Any = root or {}
    for part in (path or "").split("."):
        part = part.strip()
        if not part or not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _plugin_settings_lookup(manifest: dict[str, Any], path: str) -> Any:
    cur: Any = manifest.get("settings") or {}
    for part in (path or "").split("."):
        part = part.strip()
        if not part or not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    if isinstance(cur, dict) and "value" in cur:
        return cur.get("value")
    return cur


def _format_scalar(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, bool):
        return "Yes" if v else "No"
    s = str(v).strip()
    return s if s else "—"


def _format_integration_setting(key: str, settings: dict[str, Any]) -> str:
    k = (key or "").strip()
    if not k:
        return "—"
    v = settings.get(k)
    if k == "default_provider":
        if v is None or str(v).strip().lower() in ("", "none"):
            return "None"
    return _format_scalar(v)


def _declared_permission_labels(manifest: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    raw = manifest.get("declared_permissions")
    if not isinstance(raw, list):
        return out
    for item in raw:
        if not isinstance(item, dict):
            continue
        pid = (item.get("id") or "").strip()
        if not pid:
            continue
        out[pid] = (item.get("label") or pid).strip()
    return out


def _normalize_consumer_entries(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, dict):
        return [raw]
    return []


def _resolve_permission_rows(
    permission_ids: list[str],
    *,
    catalog_by_id: dict[str, str],
    local_labels: dict[str, str],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for pid in permission_ids:
        p = (pid or "").strip()
        if not p:
            continue
        label = catalog_by_id.get(p) or local_labels.get(p) or p
        rows.append({"id": p, "label": label})
    return rows


def _resolve_option_rows(
    options: Any,
    *,
    int_settings: dict[str, Any] | None,
    core_manifest: dict[str, Any] | None,
    manifest: dict[str, Any],
) -> list[dict[str, str]]:
    if not isinstance(options, list):
        return []
    out: list[dict[str, str]] = []
    for opt in options:
        if not isinstance(opt, dict):
            continue
        label = (opt.get("label") or "").strip()
        if not label:
            continue
        if "value" in opt:
            out.append({"label": label, "value": _format_scalar(opt.get("value"))})
            continue
        ik = (opt.get("integration_setting") or "").strip()
        if ik:
            out.append(
                {
                    "label": label,
                    "value": _format_integration_setting(ik, int_settings or {}),
                }
            )
            continue
        ck = (opt.get("core_manifest_key") or "").strip()
        if ck:
            out.append(
                {
                    "label": label,
                    "value": _format_scalar(_deep_get(core_manifest or {}, ck)),
                }
            )
            continue
        pk = (opt.get("plugin_settings_key") or "").strip()
        if pk:
            out.append(
                {
                    "label": label,
                    "value": _format_scalar(_plugin_settings_lookup(manifest, pk)),
                }
            )
            continue
    return out


def collect_accounting_integration_consumer_cards(
    plugins_map: dict[str, Any],
    *,
    permission_catalog: list[dict[str, Any]],
    int_settings: dict[str, Any] | None = None,
    core_manifest: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Build UI rows for ``core/integrations.html``.

    ``plugins_map`` is typically ``PluginManager(...).load_plugins()``.
    """
    catalog_by_id = {
        (r.get("id") or "").strip(): (r.get("label") or "").strip()
        for r in permission_catalog
        if isinstance(r, dict) and (r.get("id") or "").strip()
    }
    cards: list[dict[str, Any]] = []

    for _folder, manifest in (plugins_map or {}).items():
        if not isinstance(manifest, dict):
            continue
        if not manifest.get("enabled"):
            continue
        entries = _normalize_consumer_entries(
            manifest.get("accounting_integration_consumer")
        )
        if not entries:
            continue
        sys_name = (manifest.get("system_name") or "").strip() or _folder
        module_title = (manifest.get("name") or sys_name).strip()
        local_labels = _declared_permission_labels(manifest)

        for entry in entries:
            title = (entry.get("title") or "").strip() or module_title
            summary = (entry.get("summary") or "").strip()
            raw_pids = entry.get("provider_ids")
            provider_ids: list[str] = []
            if isinstance(raw_pids, list):
                for x in raw_pids:
                    s = str(x).strip().lower()
                    if s in _int_repo.PROVIDERS:
                        provider_ids.append(s)
            provider_labels = [_PROVIDER_TITLES.get(p, p) for p in provider_ids]

            raw_perm = entry.get("permission_ids")
            perm_ids: list[str] = []
            if isinstance(raw_perm, list):
                perm_ids = [str(x).strip() for x in raw_perm if str(x).strip()]

            permissions = _resolve_permission_rows(
                perm_ids,
                catalog_by_id=catalog_by_id,
                local_labels=local_labels,
            )
            options = _resolve_option_rows(
                entry.get("options"),
                int_settings=int_settings,
                core_manifest=core_manifest,
                manifest=manifest,
            )

            if not (summary or provider_ids or permissions or options):
                continue

            cards.append(
                {
                    "plugin": sys_name,
                    "module_title": title,
                    "summary": summary,
                    "provider_ids": provider_ids,
                    "provider_labels": provider_labels,
                    "permissions": permissions,
                    "options": options,
                }
            )

    cards.sort(
        key=lambda c: (
            (c.get("module_title") or "").lower(),
            (c.get("plugin") or "").lower(),
        )
    )
    return cards
