"""
Central permission IDs for Sparrow ERP (core + plugins).

Manifest conventions (factory_manifest.json / manifest.json):
- access_permission: str — required to see the plugin on the dashboard and use /plugin/<name>/…
  (falls back to legacy permission_required, then "<system_name>.access")
- permission_categories: optional list of {"id", "label", "sort", "parent?"}
  — parent ``id`` is another category id in the same plugin; used to group checkboxes in User Management.
- access_permission_category: optional category id for the module-access row (same plugin).
- declared_permissions: list of {"id": str, "label": str, "description": optional str, "category": optional str}
  — extra granular permissions (e.g. dispatcher vs call taker); optional ``category`` groups the row in UI.
- accounting_integration_consumer: optional object or list (see ``app/core_integrations/consumers.py``)
  — registers the plugin on the core Integrations page (permissions / options cards).

Environment:
- SPARROW_SUPPORT_SHADOW_USERNAME — login name for time-limited vendor support access
  (default: sparrowsupport). Customer-generated from Core settings; not a billable seat.

**support_break_glass** — legacy role string (e.g. old JWTs). New vendor shadow accounts use DB
role **superuser**; ``session[\"support_shadow\"]`` is set for auditing (login events) only.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from collections import defaultdict
from typing import Any

from flask_login import current_user

# Time-limited vendor support shadow user (see app.support_access)
SUPPORT_SHADOW_ROLE = "support_break_glass"

def normalize_stored_permissions(raw: Any) -> list[str]:
    """DB JSON / legacy text -> list of permission ids for forms and APIs."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if x is not None and str(x).strip()]
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            v = json.loads(s)
            if isinstance(v, list):
                return [str(x) for x in v if x is not None and str(x).strip()]
        except Exception:
            return []
    return []


def user_can_open_user_management() -> bool:
    """
    User Management (/users): superuser, admin, clinical lead, legacy vendor shadow role, or core.manage_users.
    """
    if not getattr(current_user, "is_authenticated", False):
        return False
    r = str(getattr(current_user, "role", "") or "").lower()
    if r in ("admin", "superuser", "clinical_lead", SUPPORT_SHADOW_ROLE):
        return True
    from app.objects import has_permission

    return has_permission("core.manage_users")


def user_can_open_org_admin_nav() -> bool:
    """Top nav / dashboard: Settings, System (plugins, updates) — admin, superuser, or legacy vendor shadow."""
    if not getattr(current_user, "is_authenticated", False):
        return False
    r = str(getattr(current_user, "role", "") or "").lower()
    return r in ("admin", "superuser", SUPPORT_SHADOW_ROLE)


def management_may_edit_user_row(
    editor_user_id: Any,
    target_user_id: Any,
    editor_role: str | None,
    target_role: str | None,
) -> bool:
    """User management table: you may always open Edit on your own row; otherwise role hierarchy applies."""
    if str(editor_user_id or "") == str(target_user_id or ""):
        return True
    return editor_may_edit_target_user(editor_role, target_role)


def management_may_delete_user_row(
    editor_user_id: Any,
    target_user_id: Any,
    editor_role: str | None,
    target_role: str | None,
) -> bool:
    """Never offer Delete on your own row; otherwise same as edit privilege for other accounts."""
    if str(editor_user_id or "") == str(target_user_id or ""):
        return False
    return editor_may_edit_target_user(editor_role, target_role)


def editor_may_edit_target_user(editor_role: str | None, target_role: str | None) -> bool:
    """
    Hierarchy: only superuser may change superuser, admin, or clinical_lead accounts.
    Admin / clinical_lead / delegated staff (core.manage_users) may manage staff, crew, and legacy user role.
    """
    e = (editor_role or "").lower()
    t = (target_role or "").lower()
    if e in ("superuser", SUPPORT_SHADOW_ROLE):
        return True
    if t == "superuser":
        return False
    if t in ("admin", "clinical_lead"):
        return False
    if e in ("admin", "clinical_lead"):
        return True
    if e == "staff":
        from app.objects import has_permission

        return has_permission("core.manage_users") and t in (
            "staff",
            "crew",
            "user",
            "",
        )
    return False


def editor_may_assign_elevated_core_role(new_role: str | None) -> bool:
    """Assigning admin or clinical_lead requires superuser or core.manage_users."""
    nr = (new_role or "").strip().lower()
    if nr not in ("admin", "clinical_lead"):
        return True
    er = str(getattr(current_user, "role", "") or "").lower()
    if er in ("superuser", SUPPORT_SHADOW_ROLE):
        return True
    from app.objects import has_permission

    return has_permission("core.manage_users")


def serialize_user_row_for_management_api(
    row: dict,
    editor_role: str | None,
    editor_user_id: Any = None,
) -> dict:
    """JSON-serializable user dict + may_edit / may_delete for the management UI."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    raw_perms = out.get("permissions")
    out["permissions"] = normalize_stored_permissions(raw_perms)
    tid = row.get("id")
    out["may_edit"] = management_may_edit_user_row(
        editor_user_id, tid, editor_role, row.get("role")
    )
    out["may_delete"] = management_may_delete_user_row(
        editor_user_id, tid, editor_role, row.get("role")
    )
    if "has_personal_pin" in out:
        out["has_personal_pin"] = bool(out.get("has_personal_pin"))
    return out


def plugin_access_permission_id(manifest: dict | None, system_name: str) -> str:
    m = manifest or {}
    explicit = (m.get("access_permission") or m.get("permission_required") or "").strip()
    if explicit:
        return explicit
    sn = (system_name or "").strip() or "plugin"
    return f"{sn}.access"


def _scoped_permission_category(plugin: str, raw: str | None) -> str | None:
    """Stable id: ``plugin::category_id`` (category ids are unique within a plugin manifest)."""
    if not raw:
        return None
    r = str(raw).strip()
    if not r:
        return None
    if "::" in r:
        return r
    pl = (plugin or "").strip() or "plugin"
    return f"{pl}::{r}"


def _parse_permission_category_definitions(
    manifest: dict, sys_name: str
) -> dict[str, dict[str, Any]]:
    """
    From manifest ``permission_categories``: {scoped_id: {label, sort, parent_scoped}}.
    ``parent`` in JSON references another category id **within the same plugin** (unscoped).
    """
    out: dict[str, dict[str, Any]] = {}
    raw_list = manifest.get("permission_categories")
    if not isinstance(raw_list, list):
        return out
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        rid = (item.get("id") or "").strip()
        if not rid:
            continue
        sid = _scoped_permission_category(sys_name, rid)
        if not sid:
            continue
        plab = (item.get("label") or rid).strip()
        try:
            sort = int(item.get("sort", 999))
        except (TypeError, ValueError):
            sort = 999
        parent_raw = (item.get("parent") or "").strip()
        parent_sid = (
            _scoped_permission_category(sys_name, parent_raw) if parent_raw else None
        )
        out[sid] = {"label": plab, "sort": sort, "parent": parent_sid}
    return out


def _category_path_labels(cat_defs: dict[str, dict[str, Any]], leaf_sid: str | None) -> list[str]:
    if not leaf_sid or leaf_sid not in cat_defs:
        return []
    labels: list[str] = []
    cur: str | None = leaf_sid
    seen: set[str] = set()
    while cur and cur not in seen:
        seen.add(cur)
        meta = cat_defs.get(cur)
        if not meta:
            break
        labels.append(str(meta.get("label") or ""))
        cur = meta.get("parent")
    labels.reverse()
    return [x for x in labels if x]


def _category_sort_tuple(
    cat_defs: dict[str, dict[str, Any]], leaf_sid: str | None
) -> tuple[int, ...]:
    keys: list[int] = []
    cur: str | None = leaf_sid
    seen: set[str] = set()
    while cur and cur not in seen:
        seen.add(cur)
        meta = cat_defs.get(cur)
        if not meta:
            break
        try:
            keys.insert(0, int(meta.get("sort", 999)))
        except (TypeError, ValueError):
            keys.insert(0, 999)
        cur = meta.get("parent")
    return tuple(keys)


def collect_permission_catalog(plugin_manager) -> list[dict[str, Any]]:
    """
    Build permission rows for user-management UI and role defaults.

    Each row: id, label, kind, plugin, optional ``category_path`` (list of heading labels),
    ``category_sort_key`` (tuple for ordering).

    Plugins may declare::

        "permission_categories": [
          {"id": "crew", "label": "Crew", "sort": 20},
          {"id": "crew_clinical", "label": "Clinical", "parent": "crew", "sort": 21}
        ],
        "access_permission_category": "crew_clinical",
        "declared_permissions": [
          {"id": "my_plugin.read", "label": "…", "category": "crew_clinical"}
        ]

    ``parent`` references another category ``id`` from the same plugin. When no categories are set,
    :func:`build_permission_catalog_ui_sections` falls back to grouping by plugin display name.
    """
    plugins = plugin_manager.load_plugins() or {}
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []

    def append_row(
        pid: str,
        label: str,
        kind: str,
        plugin: str,
        *,
        cat_defs: dict[str, dict[str, Any]],
        leaf_sid: str | None,
    ) -> None:
        pid = (pid or "").strip()
        if not pid or pid in seen:
            return
        seen.add(pid)
        path = _category_path_labels(cat_defs, leaf_sid)
        row: dict[str, Any] = {
            "id": pid,
            "label": label or pid,
            "kind": kind,
            "plugin": plugin,
            "category_path": path,
            "category_sort_key": _category_sort_tuple(cat_defs, leaf_sid) or (999,),
        }
        rows.append(row)

    for folder, manifest in plugins.items():
        if not isinstance(manifest, dict):
            continue
        sys_name = (manifest.get("system_name") or folder or "").strip() or folder
        name = (manifest.get("name") or sys_name).strip()
        cat_defs = _parse_permission_category_definitions(manifest, sys_name)
        acc = plugin_access_permission_id(manifest, sys_name)
        acc_cat_raw = (manifest.get("access_permission_category") or "").strip()
        acc_leaf = (
            _scoped_permission_category(sys_name, acc_cat_raw) if acc_cat_raw else None
        )
        append_row(
            acc,
            f"{name} — module access",
            "module_access",
            sys_name,
            cat_defs=cat_defs,
            leaf_sid=acc_leaf,
        )
        declared = manifest.get("declared_permissions")
        if isinstance(declared, list):
            for item in declared:
                if not isinstance(item, dict):
                    continue
                pid = (item.get("id") or "").strip()
                if not pid:
                    continue
                lbl = (item.get("label") or pid).strip()
                cat_raw = (item.get("category") or "").strip()
                leaf = (
                    _scoped_permission_category(sys_name, cat_raw) if cat_raw else None
                )
                append_row(pid, lbl, "feature", sys_name, cat_defs=cat_defs, leaf_sid=leaf)

    for idx, (pid, label) in enumerate(
        (
            ("core.manage_users", "Core — manage administrator accounts"),
            ("core.settings", "Core — site settings, SMTP, upgrades, plugin install"),
        )
    ):
        append_row(
            pid,
            label,
            "feature",
            "Sparrow_ERP_Core",
            cat_defs={},
            leaf_sid=None,
        )
        rows[-1]["category_path"] = ["Sparrow core"]
        rows[-1]["category_sort_key"] = (0, 5 + int(idx))

    rows.sort(
        key=lambda r: (
            tuple(r.get("category_sort_key") or (999,)),
            (r.get("plugin") or "").lower(),
            (r.get("label") or "").lower(),
            r.get("id") or "",
        )
    )
    return rows


def build_permission_catalog_ui_sections(
    catalog: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Group catalog rows for the user-management modals: each section has ``path`` (heading trail)
    and ``rows``. Falls back to one section per plugin when no ``category_path`` is set on any row.
    """
    if not catalog:
        return []
    has_paths = any((r.get("category_path") or []) for r in catalog)
    if not has_paths:
        by_pl: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in catalog:
            pl = (r.get("plugin") or "").strip() or "Other"
            by_pl[pl].append(r)
        out: list[dict[str, Any]] = []
        for pl in sorted(by_pl.keys(), key=str.lower):
            rs = sorted(
                by_pl[pl],
                key=lambda x: ((x.get("label") or "").lower(), x.get("id") or ""),
            )
            out.append({"path": [pl], "rows": rs})
        return out

    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for r in catalog:
        pt = tuple(r.get("category_path") or ())
        if not pt:
            pl = (r.get("plugin") or "Other").strip() or "Other"
            pt = (pl, "General")
        groups[pt].append(r)

    def sort_key(path: tuple[str, ...], rs: list[dict[str, Any]]) -> tuple:
        mink = min((tuple(x.get("category_sort_key") or (999,)) for x in rs), default=(999,))
        return (mink, [p.lower() for p in path])

    out2: list[dict[str, Any]] = []
    for pt in sorted(groups.keys(), key=lambda t: sort_key(t, groups[t])):
        rs = sorted(
            groups[pt],
            key=lambda x: ((x.get("label") or "").lower(), x.get("id") or ""),
        )
        out2.append({"path": list(pt), "rows": rs})
    return out2


def permission_ids_for_catalog(catalog: list[dict]) -> list[str]:
    return [r["id"] for r in catalog]


# Default permissions applied in the UI when an admin picks a role (can be edited before save)
def default_permission_ids_for_role(role: str, catalog: list[dict]) -> list[str]:
    r = (role or "").strip().lower()
    if r in ("admin", "superuser", SUPPORT_SHADOW_ROLE):
        return [x["id"] for x in catalog]
    if r == "staff":
        return [x["id"] for x in catalog if x.get("kind") == "module_access"]
    if r == "clinical_lead":
        out = [x["id"] for x in catalog if x.get("kind") == "module_access"]
        for x in catalog:
            if x.get("plugin") == "ventus_response_module":
                if x["id"] not in out:
                    out.append(x["id"])
        for pid in ("core.manage_users",):
            if pid not in out:
                out.append(pid)
        return out
    if r == "logistics":
        out = [x["id"] for x in catalog if x.get("kind") == "module_access"]
        for x in catalog:
            if x.get("plugin") in ("fleet_management", "inventory_control"):
                if x["id"] not in out:
                    out.append(x["id"])
        return out
    if r == "crew":
        return []
    return []


def user_can_access_plugin(user, manifest: dict | None, system_name: str) -> bool:
    """
    Dashboard + URL gate: admin/superuser always; else explicit permission on user.
    Supports optional manifest key alternate_access_permissions (list of permission ids),
    e.g. driver-only access to Fleet without fleet_management.access.
    """
    if not user or not getattr(user, "is_authenticated", False):
        return False
    role = str(getattr(user, "role", "") or "").lower()
    if role in ("admin", "superuser", SUPPORT_SHADOW_ROLE):
        return True
    m = manifest if isinstance(manifest, dict) else {}
    perm = plugin_access_permission_id(m, system_name)
    perms = getattr(user, "permissions", None) or []
    if perm in perms:
        return True
    alt = m.get("alternate_access_permissions")
    if isinstance(alt, list):
        for p in alt:
            if isinstance(p, str) and p.strip() and p.strip() in perms:
                return True
    return False
