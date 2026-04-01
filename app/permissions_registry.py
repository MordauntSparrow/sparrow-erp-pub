"""
Central permission IDs for Sparrow ERP (core + plugins).

Manifest conventions (factory_manifest.json / manifest.json):
- access_permission: str — required to see the plugin on the dashboard and use /plugin/<name>/…
  (falls back to legacy permission_required, then "<system_name>.access")
- declared_permissions: list of {"id": str, "label": str, "description": optional str}
  — extra granular permissions (e.g. dispatcher vs call taker inside a module).

Environment:
- SPARROW_SUPPORT_SHADOW_USERNAME — login name for time-limited vendor support access
  (default: sparrowsupport). Customer-generated from Core settings; not a billable seat.

**support_break_glass** — legacy role string (e.g. old JWTs). New vendor shadow accounts use DB
role **superuser**; ``session[\"support_shadow\"]`` is set for auditing (login events) only.
"""

from __future__ import annotations

import json
from datetime import date, datetime
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


def serialize_user_row_for_management_api(row: dict, editor_role: str | None) -> dict:
    """JSON-serializable user dict + may_edit / may_delete for the management UI."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    raw_perms = out.get("permissions")
    out["permissions"] = normalize_stored_permissions(raw_perms)
    tr = (row.get("role") or "").lower()
    out["may_edit"] = editor_may_edit_target_user(editor_role, row.get("role"))
    out["may_delete"] = out["may_edit"]
    return out


def plugin_access_permission_id(manifest: dict | None, system_name: str) -> str:
    m = manifest or {}
    explicit = (m.get("access_permission") or m.get("permission_required") or "").strip()
    if explicit:
        return explicit
    sn = (system_name or "").strip() or "plugin"
    return f"{sn}.access"


def collect_permission_catalog(plugin_manager) -> list[dict[str, Any]]:
    """
    Build {id, label, kind, plugin} for user-management UI and role defaults.
    kind: module_access | feature
    """
    plugins = plugin_manager.load_plugins() or {}
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []

    def add_row(pid: str, label: str, kind: str, plugin: str) -> None:
        pid = (pid or "").strip()
        if not pid or pid in seen:
            return
        seen.add(pid)
        rows.append(
            {
                "id": pid,
                "label": label or pid,
                "kind": kind,
                "plugin": plugin,
            }
        )

    for folder, manifest in plugins.items():
        if not isinstance(manifest, dict):
            continue
        sys_name = (manifest.get("system_name") or folder or "").strip() or folder
        name = (manifest.get("name") or sys_name).strip()
        acc = plugin_access_permission_id(manifest, sys_name)
        add_row(
            acc,
            f"{name} — module access",
            "module_access",
            sys_name,
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
                add_row(pid, lbl, "feature", sys_name)

    for pid, label in (
        ("core.manage_users", "Core — manage administrator accounts"),
        ("core.settings", "Core — site settings, SMTP, upgrades, plugin install"),
    ):
        add_row(pid, label, "feature", "Sparrow_ERP_Core")

    rows.sort(key=lambda r: (r["label"].lower(), r["id"]))
    return rows


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
