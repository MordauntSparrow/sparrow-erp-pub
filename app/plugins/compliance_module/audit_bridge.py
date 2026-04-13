"""
Attach structured fields to admin_staff_action_logs.detail_json for the current request.

Used by compliance_module routes so assurance exports and SIEM feeds show *what* changed,
not only POST /plugin/compliance_module/....
"""
from __future__ import annotations

from typing import Any, Dict, Optional


def attach_staff_audit_detail(payload: Dict[str, Any]) -> None:
    try:
        from flask import g, has_request_context
    except ImportError:
        return
    if not has_request_context():
        return
    merged = getattr(g, "_staff_audit_extra", None)
    if not isinstance(merged, dict):
        merged = {}
    for k, v in payload.items():
        if v is not None:
            merged[k] = v
    g._staff_audit_extra = merged


def note_policy_admin_action(
    action: str,
    *,
    policy_id: Optional[int] = None,
    policy_title: str = "",
    lifecycle_from: Optional[str] = None,
    lifecycle_to: Optional[str] = None,
    version: Optional[int] = None,
    admin_action: Optional[str] = None,
) -> None:
    attach_staff_audit_detail(
        {
            "compliance_policy_audit": {
                "channel": "admin",
                "action": action,
                "policy_id": policy_id,
                "policy_title": (policy_title or "")[:400],
                "lifecycle_from": lifecycle_from,
                "lifecycle_to": lifecycle_to,
                "version": version,
                "admin_action": admin_action,
            }
        }
    )


def note_document_type_admin_action(
    action: str,
    *,
    type_id: Optional[int] = None,
    label: str = "",
) -> None:
    attach_staff_audit_detail(
        {
            "compliance_document_type_audit": {
                "channel": "admin",
                "action": action,
                "document_type_id": type_id,
                "label": (label or "")[:200],
            }
        }
    )


def note_policy_contractor_acknowledge(
    *,
    policy_id: int,
    policy_title: str = "",
    contractor_id: int,
    version: int,
) -> None:
    attach_staff_audit_detail(
        {
            "compliance_policy_audit": {
                "channel": "contractor_portal",
                "action": "acknowledge",
                "policy_id": int(policy_id),
                "policy_title": (policy_title or "")[:400],
                "contractor_id": int(contractor_id),
                "version": int(version),
            }
        }
    )
