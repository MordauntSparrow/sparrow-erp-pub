"""Event plan accommodation: venue list + hotel room allocations (optional section)."""
from __future__ import annotations

import json
from typing import Any

from flask import request

_VENUE_NAME_MAX = 255
_VENUE_ADDR_MAX = 512
_HOTEL_MAX = 255
_ROOM_MAX = 64
_STAFF_PER_ROOM_MAX = 30
_STAFF_LINE_MAX = 255


def _parse_json_array(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, (bytes, str)):
        s = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
        s = s.strip()
        if not s:
            return []
        try:
            v = json.loads(s)
        except (json.JSONDecodeError, TypeError):
            return []
        return v if isinstance(v, list) else []
    return []


def accommodation_venues_from_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for it in _parse_json_array(plan.get("accommodation_venues_json")):
        if not isinstance(it, dict):
            continue
        nm = str(it.get("name") or "").strip()[:_VENUE_NAME_MAX]
        ad = str(it.get("address") or "").strip()[:_VENUE_ADDR_MAX]
        if nm or ad:
            rows.append({"name_val": nm, "address_val": ad})
    return rows


def accommodation_rooms_from_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for it in _parse_json_array(plan.get("accommodation_rooms_json")):
        if not isinstance(it, dict):
            continue
        hotel = str(it.get("hotel_name") or "").strip()[:_HOTEL_MAX]
        room = str(it.get("room_no") or "").strip()[:_ROOM_MAX]
        staff = it.get("staff_names")
        names: list[str] = []
        if isinstance(staff, list):
            for x in staff[:_STAFF_PER_ROOM_MAX]:
                s = str(x or "").strip()[:_STAFF_LINE_MAX]
                if s:
                    names.append(s)
        staff_text = "\n".join(names)
        if hotel or room or staff_text:
            rows.append(
                {
                    "hotel_val": hotel,
                    "room_val": room,
                    "staff_text": staff_text,
                }
            )
    return rows


def pad_accommodation_venue_rows_for_edit(
    rows: list[dict[str, str]],
) -> list[dict[str, str] | None]:
    n = len(rows)
    size = max(2, min(40, n if n > 0 else 2))
    padded: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        padded[i] = r
    return padded


def pad_accommodation_room_rows_for_edit(
    rows: list[dict[str, str]],
) -> list[dict[str, str] | None]:
    n = len(rows)
    size = max(2, min(60, n if n > 0 else 2))
    padded: list[dict[str, str] | None] = [None] * size
    for i, r in enumerate(rows[:size]):
        padded[i] = r
    return padded


def accommodation_venues_json_from_form() -> str | None:
    names = request.form.getlist("acc_venue_name")
    addrs = request.form.getlist("acc_venue_address")
    out: list[dict[str, str]] = []
    for i in range(max(len(names), len(addrs))):
        nm = (names[i] if i < len(names) else "").strip()[:_VENUE_NAME_MAX]
        ad = (addrs[i] if i < len(addrs) else "").strip()[:_VENUE_ADDR_MAX]
        if nm or ad:
            out.append({"name": nm, "address": ad})
    if not out:
        return None
    return json.dumps(out, ensure_ascii=False)


def accommodation_rooms_json_from_form() -> str | None:
    hotels = request.form.getlist("acc_room_hotel")
    rooms = request.form.getlist("acc_room_no")
    staff_blocks = request.form.getlist("acc_room_staff")
    n = max(len(hotels), len(rooms), len(staff_blocks))
    out: list[dict[str, Any]] = []
    for i in range(n):
        h = (hotels[i] if i < len(hotels) else "").strip()[:_HOTEL_MAX]
        rno = (rooms[i] if i < len(rooms) else "").strip()[:_ROOM_MAX]
        sb = staff_blocks[i] if i < len(staff_blocks) else ""
        names = [
            ln.strip()[:_STAFF_LINE_MAX]
            for ln in str(sb or "").splitlines()
            if str(ln or "").strip()
        ][: _STAFF_PER_ROOM_MAX]
        if h or rno or names:
            out.append({"hotel_name": h, "room_no": rno, "staff_names": names})
    if not out:
        return None
    return json.dumps(out, ensure_ascii=False)


def accommodation_enabled_from_form() -> int:
    return 1 if (request.form.get("accommodation_enabled") == "1") else 0


def accommodation_pdf_visible(plan: dict[str, Any]) -> bool:
    if not int(plan.get("accommodation_enabled") or 0):
        return False
    return bool(accommodation_venues_from_plan(plan) or accommodation_rooms_from_plan(plan))


def accommodation_venue_pdf_rows(plan: dict[str, Any]) -> list[tuple[str, str]]:
    return [
        (r["name_val"], r["address_val"])
        for r in accommodation_venues_from_plan(plan)
        if (r.get("name_val") or "").strip() or (r.get("address_val") or "").strip()
    ]


def accommodation_room_pdf_rows(plan: dict[str, Any]) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for r in accommodation_rooms_from_plan(plan):
        hotel = (r.get("hotel_val") or "").strip()
        room = (r.get("room_val") or "").strip()
        st = (r.get("staff_text") or "").strip()
        if hotel or room or st:
            rows.append((hotel, room, st))
    return rows
