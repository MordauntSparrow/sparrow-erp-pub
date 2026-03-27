"""
Central permission IDs for Sparrow ERP (core + plugins).

Manifest conventions (factory_manifest.json / manifest.json):
- access_permission: str — required to see the plugin on the dashboard and use /plugin/<name>/…
  (falls back to legacy permission_required, then "<system_name>.access")
- declared_permissions: list of {"id": str, "label": str, "description": optional str}
  — extra granular permissions (e.g. dispatcher vs call taker inside a module).

Environment:
- SPARROW_SUPERUSER_USERNAMES — comma-separated login names allowed to have role superuser
  (default: jordan). Only a superuser can assign the superuser role, and only these names may hold it.
"""

from __future__ import annotations

import os
from typing import Any

# Usernames (lowercase) permitted to have role "superuser" when saved
def _superuser_name_allowlist() -> set[str]:
    raw = os.environ.get("SPARROW_SUPERUSER_USERNAMES", "jordan")
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def is_superuser_username_allowed(username: str | None) -> bool:
    if not username or not str(username).strip():
        return False
    return str(username).strip().lower() in _superuser_name_allowlist()


def editor_may_assign_superuser_role(editor) -> bool:
    """Only existing superusers may set the superuser role on forms."""
    if not editor or not getattr(editor, "is_authenticated", False):
        return False
    return str(getattr(editor, "role", "") or "").lower() == "superuser"


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

    rows.sort(key=lambda r: (r["label"].lower(), r["id"]))
    return rows


def permission_ids_for_catalog(catalog: list[dict]) -> list[str]:
    return [r["id"] for r in catalog]


# Default permissions applied in the UI when an admin picks a role (can be edited before save)
def default_permission_ids_for_role(role: str, catalog: list[dict]) -> list[str]:
    r = (role or "").strip().lower()
    if r in ("admin", "superuser"):
        return [x["id"] for x in catalog]
    if r == "staff":
        return [x["id"] for x in catalog if x.get("kind") == "module_access"]
    if r == "clinical_lead":
        out = [x["id"] for x in catalog if x.get("kind") == "module_access"]
        for x in catalog:
            if x.get("plugin") == "ventus_response_module":
                if x["id"] not in out:
                    out.append(x["id"])
        return out
    if r == "logistics":
        out = [x["id"] for x in catalog if x.get("kind") == "module_access"]
        for x in catalog:
            if x.get("plugin") in ("fleet_management", "asset_management", "inventory_control"):
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
    if role in ("admin", "superuser"):
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
