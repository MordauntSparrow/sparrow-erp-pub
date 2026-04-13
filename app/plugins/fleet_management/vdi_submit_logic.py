"""Shared VDI form POST handling (main app /VDIs and employee portal)."""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

from werkzeug.utils import secure_filename

from .objects import FleetService

_APP_PKG = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FLEET_UPLOAD_ROOT = os.path.join(_APP_PKG, "static", "uploads", "fleet")
ALLOWED_IMG = frozenset({"png", "jpg", "jpeg", "webp"})


def _ensure_upload_dir() -> None:
    os.makedirs(FLEET_UPLOAD_ROOT, exist_ok=True)
    os.makedirs(os.path.join(FLEET_UPLOAD_ROOT, "vehicles"), exist_ok=True)


def _safe_vdi_rel_path(vehicle_id: int, submission_id: int, fname: str) -> Optional[str]:
    vid = str(int(vehicle_id))
    sid = str(int(submission_id))
    clean = secure_filename(fname)
    if not clean or ".." in vid or ".." in sid:
        return None
    rel = f"fleet/vehicles/{vid}/vdi/{sid}/{clean}"
    if not re.match(r"^fleet/vehicles/\d+/vdi/\d+/[\w\.\-]+$", rel):
        return None
    return rel


def parse_vdi_form(
    request, schema: Dict[str, Any]
) -> Tuple[Dict[str, Any], Optional[int], Optional[str]]:
    """
    Returns (responses, mileage_val, error_message).
    """
    responses: dict = {}
    mileage_val = None
    for sec in schema.get("sections") or []:
        for field in sec.get("fields") or []:
            fid = field.get("id")
            if not fid:
                continue
            ftype = (field.get("type") or "text").lower()
            if ftype == "bool":
                raw_b = request.form.get(f"f_{fid}")
                if raw_b == "1":
                    responses[fid] = True
                elif raw_b == "0":
                    responses[fid] = False
                else:
                    responses[fid] = None
            elif ftype == "number":
                raw = (request.form.get(f"f_{fid}") or "").strip()
                if raw:
                    try:
                        responses[fid] = float(raw) if "." in raw else int(raw)
                    except ValueError:
                        responses[fid] = raw
                else:
                    responses[fid] = None
            elif ftype == "mileage":
                raw = (request.form.get(f"f_{fid}") or "").strip()
                if raw:
                    try:
                        responses[fid] = int(raw)
                        mileage_val = int(raw)
                    except ValueError:
                        responses[fid] = raw
                else:
                    responses[fid] = None
            else:
                responses[fid] = (request.form.get(f"f_{fid}") or "").strip() or None

    if mileage_val is None and responses.get("odometer") is not None:
        try:
            mileage_val = int(responses["odometer"])
        except (TypeError, ValueError):
            pass

    missing = []
    for sec in schema.get("sections") or []:
        for field in sec.get("fields") or []:
            if not field.get("required"):
                continue
            fid = field.get("id")
            ftype = (field.get("type") or "text").lower()
            if ftype == "photo":
                fobj = request.files.get(f"photo_{fid}")
                if not fobj or not fobj.filename:
                    missing.append(field.get("label") or fid)
            elif ftype == "bool":
                if responses.get(fid) is None:
                    missing.append(field.get("label") or fid)
            else:
                val = responses.get(fid)
                if val is None or val == "":
                    missing.append(field.get("label") or fid)
    if missing:
        return responses, mileage_val, "Required: " + ", ".join(missing)
    return responses, mileage_val, None


def save_vdi_photos_after_submit(
    request,
    schema: Dict[str, Any],
    submission_id: int,
    vehicle_id: int,
) -> List[str]:
    """Store under static/uploads/fleet/vehicles/{vehicle_id}/vdi/{submission_id}/."""
    _ensure_upload_dir()
    vid = str(int(vehicle_id))
    sid = str(int(submission_id))
    photo_paths: List[str] = []
    for sec in schema.get("sections") or []:
        for field in sec.get("fields") or []:
            if (field.get("type") or "").lower() != "photo":
                continue
            fid = field.get("id")
            fobj = request.files.get(f"photo_{fid}")
            if not fobj or not fobj.filename:
                continue
            ext = fobj.filename.rsplit(".", 1)[-1].lower() if "." in fobj.filename else ""
            if ext not in ALLOWED_IMG:
                continue
            fname = f"{fid}_{secure_filename(fobj.filename)}"
            rel = _safe_vdi_rel_path(int(vehicle_id), int(submission_id), fname)
            if not rel:
                continue
            dest_dir = os.path.join(
                FLEET_UPLOAD_ROOT, "vehicles", vid, "vdi", sid
            )
            os.makedirs(dest_dir, exist_ok=True)
            dest = os.path.join(dest_dir, secure_filename(fname))
            fobj.save(dest)
            photo_paths.append(rel)
    return photo_paths


def _safe_safety_rel_path(vehicle_id: int, check_id: int, fname: str) -> Optional[str]:
    vid = str(int(vehicle_id))
    cid = str(int(check_id))
    clean = secure_filename(fname)
    if not clean or ".." in vid or ".." in cid:
        return None
    rel = f"fleet/vehicles/{vid}/safety/{cid}/{clean}"
    if not re.match(r"^fleet/vehicles/\d+/safety/\d+/[\w\.\-]+$", rel):
        return None
    return rel


def save_fleet_safety_photos_after_submit(
    request,
    schema: Dict[str, Any],
    check_id: int,
    vehicle_id: int,
) -> List[str]:
    """Store under static/uploads/fleet/vehicles/{vehicle_id}/safety/{check_id}/."""
    _ensure_upload_dir()
    vid = str(int(vehicle_id))
    cid = str(int(check_id))
    photo_paths: List[str] = []
    for sec in schema.get("sections") or []:
        for field in sec.get("fields") or []:
            if (field.get("type") or "").lower() != "photo":
                continue
            fid = field.get("id")
            fobj = request.files.get(f"photo_{fid}")
            if not fobj or not fobj.filename:
                continue
            ext = fobj.filename.rsplit(".", 1)[-1].lower() if "." in fobj.filename else ""
            if ext not in ALLOWED_IMG:
                continue
            fname = f"{fid}_{secure_filename(fobj.filename)}"
            rel = _safe_safety_rel_path(int(vehicle_id), int(check_id), fname)
            if not rel:
                continue
            dest_dir = os.path.join(
                FLEET_UPLOAD_ROOT, "vehicles", vid, "safety", cid
            )
            os.makedirs(dest_dir, exist_ok=True)
            dest = os.path.join(dest_dir, secure_filename(fname))
            fobj.save(dest)
            photo_paths.append(rel)
    return photo_paths


def submit_vdi(
    svc: FleetService,
    request,
    *,
    vehicle_id: int,
    actor_type: str,
    actor_id: str,
) -> Tuple[Optional[str], Optional[int]]:
    """
    Process POST. Returns (error_message, submission_id).
    """
    schema = svc.get_vdi_schema()
    responses, mileage_val, err = parse_vdi_form(request, schema)
    if err:
        return err, None
    sid = svc.add_vdi_submission(
        vehicle_id=vehicle_id,
        actor_type=actor_type,
        actor_id=str(actor_id),
        mileage_reported=mileage_val,
        responses=responses,
        photo_paths=[],
    )
    paths = save_vdi_photos_after_submit(request, schema, sid, vehicle_id)
    if paths:
        svc.update_vdi_submission_photos(sid, paths)
    return None, sid
