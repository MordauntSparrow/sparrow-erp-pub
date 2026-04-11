"""
Hospital suggestions for the medical event planner (PRD §5.1 step 5, CRM-HOSP-001).

1. Geocode UK postcode/outcode via postcodes.io (no key).
2. If ``GOOGLE_MAPS_API_KEY`` is set (same env as Ventus geocoding), query Google Places
   Nearby Search (type=hospital) and rank by distance.
3. Otherwise fall back to the static catalogue ranked by haversine.

Enable **Places API** (Nearby Search) on the GCP project for that key. If the call fails
or returns no rows, behaviour falls back to the catalogue without breaking the UI.
"""
from __future__ import annotations

import math
import os
import re
from typing import Any
from urllib.parse import quote

import requests

POSTCODES_IO_BASE = "https://api.postcodes.io"
PLACES_NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
REQUEST_TIMEOUT_S = 8
_MAX_SUGGESTIONS = 12
# Metres — NHS catchment-style radius from postcode centroid (Places max 50_000).
_PLACES_SEARCH_RADIUS_M = 40_000

_UK_POSTCODE_IN_TEXT = re.compile(
    r"\b([A-Z]{1,2}\d[A-Z0-9]?\s*\d[A-Z]{2})\b", re.IGNORECASE
)

# name, postcode (for display), lat/lng from postcodes.io bulk lookup
_HOSPITALS: list[dict[str, Any]] = [
    {
        "name": "Guy's Hospital — Emergency Department",
        "postcode": "SE1 9RT",
        "lat": 51.503331,
        "lng": -0.086771,
    },
    {
        "name": "St Thomas' Hospital — A&E",
        "postcode": "SE1 7EH",
        "lat": 51.49796,
        "lng": -0.11888,
    },
    {
        "name": "Royal London Hospital — Emergency Department",
        "postcode": "E1 1BB",
        "lat": 51.519019,
        "lng": -0.058106,
    },
    {
        "name": "Manchester Royal Infirmary — Emergency Department",
        "postcode": "M13 9WL",
        "lat": 53.462452,
        "lng": -2.227709,
    },
    {
        "name": "Royal Infirmary of Edinburgh — Emergency Department",
        "postcode": "EH16 4SA",
        "lat": 55.921754,
        "lng": -3.13594,
    },
    {
        "name": "University Hospital Wales — Emergency Unit",
        "postcode": "CF14 4XW",
        "lat": 51.507156,
        "lng": -3.189855,
    },
    {
        "name": "Queen Elizabeth Hospital Birmingham — Emergency Department",
        "postcode": "B15 2GW",
        "lat": 52.451833,
        "lng": -1.942563,
    },
    {
        "name": "Royal Sussex County Hospital — Emergency Department",
        "postcode": "BN2 5BE",
        "lat": 50.819462,
        "lng": -0.118149,
    },
    {
        "name": "Dorset County Hospital — Emergency Department",
        "postcode": "DT1 2JY",
        "lat": 50.712931,
        "lng": -2.446934,
    },
    {
        "name": "Royal Devon University Hospital — Emergency Department",
        "postcode": "EX2 5DW",
        "lat": 50.716692,
        "lng": -3.506694,
    },
    {
        "name": "Yeovil District Hospital — Emergency Department",
        "postcode": "BA21 4AT",
        "lat": 50.944835,
        "lng": -2.634713,
    },
]

_OUTCODE_RE = re.compile(r"^[A-Z]{1,2}\d[0-9A-Z]?$")


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return r * c


def _normalize_query(q: str) -> str:
    return " ".join((q or "").upper().strip().split())


def _geocode_uk_query(q: str) -> tuple[float, float] | None:
    """
    Resolve a UK postcode or outward code to a centroid using postcodes.io.
    Returns None if not found or request fails.
    """
    n = _normalize_query(q)
    if not n:
        return None
    compact = n.replace(" ", "")
    try:
        # Full postcode (compact form, e.g. SW1A1AA or DT78PG)
        r = requests.get(
            f"{POSTCODES_IO_BASE}/postcodes/{quote(compact, safe='')}",
            timeout=REQUEST_TIMEOUT_S,
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("status") == 200 and data.get("result"):
                res = data["result"]
                return float(res["latitude"]), float(res["longitude"])
        # Spaced form
        if " " in n:
            r2 = requests.get(
                f"{POSTCODES_IO_BASE}/postcodes/{quote(n, safe='')}",
                timeout=REQUEST_TIMEOUT_S,
            )
            if r2.status_code == 200:
                data = r2.json()
                if data.get("status") == 200 and data.get("result"):
                    res = data["result"]
                    return float(res["latitude"]), float(res["longitude"])
        # Outward code only (e.g. DT7, SE1)
        if _OUTCODE_RE.fullmatch(compact):
            r3 = requests.get(
                f"{POSTCODES_IO_BASE}/outcodes/{quote(compact, safe='')}",
                timeout=REQUEST_TIMEOUT_S,
            )
            if r3.status_code == 200:
                data = r3.json()
                if data.get("status") == 200 and data.get("result"):
                    res = data["result"]
                    return float(res["latitude"]), float(res["longitude"])
    except (OSError, ValueError, TypeError, requests.RequestException):
        return None
    return None


def _text_filter_catalog(q_norm: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    q_compact = q_norm.replace(" ", "")
    for r in _HOSPITALS:
        pc = (r["postcode"] or "").upper().replace(" ", "")
        nm = (r["name"] or "").upper()
        if q_norm in nm or q_compact and q_compact in pc or q_norm.replace(" ", "") in pc:
            out.append({"name": r["name"], "postcode": r["postcode"]})
        if len(out) >= _MAX_SUGGESTIONS:
            break
    return out


def _rows_sorted_by_name(limit: int) -> list[dict[str, str]]:
    rows = sorted(_HOSPITALS, key=lambda x: (x["name"] or "").lower())
    return [
        {"name": r["name"], "postcode": r["postcode"]}
        for r in rows[:limit]
    ]


def _rows_by_distance(lat: float, lng: float, limit: int) -> list[dict[str, Any]]:
    scored: list[tuple[float, dict[str, Any]]] = []
    for r in _HOSPITALS:
        d = _haversine_km(lat, lng, float(r["lat"]), float(r["lng"]))
        item = {"name": r["name"], "postcode": r["postcode"], "distance_km": round(d, 1)}
        scored.append((d, item))
    scored.sort(key=lambda x: x[0])
    return [x[1] for x in scored[:limit]]


def _postcode_from_vicinity(vicinity: str | None) -> str:
    if not vicinity:
        return ""
    m = _UK_POSTCODE_IN_TEXT.search(vicinity.upper())
    if not m:
        return ""
    return " ".join(m.group(1).upper().split())


def _places_nearby_hospital_rows(
    lat: float, lng: float, api_key: str
) -> list[dict[str, Any]] | None:
    """
    Google Places Nearby Search (legacy JSON).

    Returns:
        list of rows on OK / ZERO_RESULTS (possibly empty),
        None on transport errors or Places denial so callers can fall back.
    """
    try:
        r = requests.get(
            PLACES_NEARBY_URL,
            params={
                "location": f"{lat},{lng}",
                "radius": _PLACES_SEARCH_RADIUS_M,
                "type": "hospital",
                "key": api_key,
            },
            timeout=REQUEST_TIMEOUT_S,
        )
        data = r.json()
    except (OSError, ValueError, TypeError, requests.RequestException):
        return None

    status = data.get("status")
    if status == "ZERO_RESULTS":
        return []
    if status != "OK":
        return None

    results = data.get("results") or []
    seen: set[str] = set()
    rows: list[tuple[float, dict[str, Any]]] = []
    for pl in results:
        pid = pl.get("place_id") or ""
        if pid and pid in seen:
            continue
        if pid:
            seen.add(pid)
        geom = pl.get("geometry") or {}
        loc = geom.get("location") or {}
        try:
            plat = float(loc["lat"])
            plng = float(loc["lng"])
        except (KeyError, TypeError, ValueError):
            continue
        d = _haversine_km(lat, lng, plat, plng)
        name = (pl.get("name") or "").strip() or "Hospital"
        vicinity = pl.get("vicinity")
        vic_str = vicinity.strip() if isinstance(vicinity, str) else ""
        pc = _postcode_from_vicinity(vic_str)
        row: dict[str, Any] = {
            "name": name,
            "postcode": pc,
            "distance_km": round(d, 1),
        }
        if vic_str and not pc:
            row["vicinity"] = vic_str
        if pid:
            row["place_id"] = pid
        rows.append((d, row))

    rows.sort(key=lambda x: x[0])
    return [x[1] for x in rows]


def hospital_suggest_payload(query: str | None) -> dict[str, Any]:
    """
    Build JSON-serializable response for /api/event-plans/hospital-suggest.

    stub: True when results are not distance-ranked from a successful geocode
    (browse list, text match, or geocoder unavailable).
    """
    raw = (query or "").strip()
    if not raw:
        return {
            "suggestions": _rows_sorted_by_name(_MAX_SUGGESTIONS),
            "stub": True,
            "message": "Enter a UK postcode or outcode (e.g. DT7) for nearest hospitals.",
            "source": "catalogue_browse",
        }

    anchor = _geocode_uk_query(raw)
    if anchor:
        lat, lng = anchor
        key = (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()
        if key:
            places = _places_nearby_hospital_rows(lat, lng, key)
            if places:
                return {
                    "suggestions": places[:_MAX_SUGGESTIONS],
                    "stub": False,
                    "message": None,
                    "source": "google_places",
                }
        return {
            "suggestions": _rows_by_distance(lat, lng, _MAX_SUGGESTIONS),
            "stub": False,
            "message": None,
            "source": "catalogue",
        }

    n = _normalize_query(raw)
    text_hits = _text_filter_catalog(n)
    if text_hits:
        return {
            "suggestions": text_hits,
            "stub": True,
            "message": "Could not geocode that location; showing name or postcode matches.",
            "source": "catalogue_text",
        }

    return {
        "suggestions": [],
        "stub": True,
        "message": "Could not find that UK postcode. Try a full postcode or outcode (e.g. DT7).",
        "source": "none",
    }


def suggest_hospitals(query: str | None) -> list[dict[str, Any]]:
    """Return suggestion list only (used by tests and simple callers)."""
    return hospital_suggest_payload(query)["suggestions"]
