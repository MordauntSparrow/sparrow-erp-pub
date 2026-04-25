"""
Shared geospatial helpers for CRM (straight-line distance, rough drive hints, what3words).

Road distance and drive time are **indicative only** — not a substitute for sat-nav or
approved mileage logs. **Dispatch bases** (``crm_dispatch_bases``) are tenant-wide
vehicle departure points; **saved venues** (``crm_account_saved_venues``) are per–CRM-account
event sites the client reuses. Legacy **operating_base_*** on ``crm_accounts`` is still
used as a fallback when no dispatch base is selected.
"""
from __future__ import annotations

import math
from typing import Any


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two WGS84 points in kilometres."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return r * c


# Typical UK road route is longer than straight-line; keep conservative for quoting hints.
_DEFAULT_ROAD_FACTOR = 1.22
_DEFAULT_ASSUMED_SPEED_KMH = 52.0


def resolve_w3w_to_latlng(words: str) -> dict[str, Any]:
    """
    Resolve what3words to lat/lng using Ventus ResponseTriage (same API key as Ventus).
    Returns ``{"lat", "lng", "normalized"}`` or ``{"error": str}``.
    """
    try:
        from app.plugins.ventus_response_module.objects import ResponseTriage
    except ImportError:
        return {"error": "Ventus response module is not installed."}
    phrase = ResponseTriage.normalize_w3w_words((words or "").strip())
    if not ResponseTriage.w3w_phrase_has_three_words(phrase):
        return {"error": "what3words must be three words (e.g. filled.count.soap)"}
    result = ResponseTriage.get_lat_lng_from_w3w(phrase)
    if "lat" not in result:
        return {"error": str(result.get("error") or "Lookup failed")}
    return {
        "lat": float(result["lat"]),
        "lng": float(result["lng"]),
        "normalized": phrase,
    }


def routing_hint_base_to_venue(
    *,
    base_lat: float,
    base_lng: float,
    venue_lat: float,
    venue_lng: float,
    base_label: str | None = None,
    road_factor: float = _DEFAULT_ROAD_FACTOR,
    assumed_speed_kmh: float = _DEFAULT_ASSUMED_SPEED_KMH,
) -> dict[str, Any]:
    """Human-readable dict for JSON / notes (all distances in km unless noted)."""
    straight = haversine_km(base_lat, base_lng, venue_lat, venue_lng)
    road = max(0.0, straight * road_factor)
    drive_h = road / assumed_speed_kmh if assumed_speed_kmh > 0 else 0.0
    drive_min = max(1, int(round(drive_h * 60)))
    label = (base_label or "").strip() or "Operating base"
    return {
        "base_label": label,
        "straight_line_km": round(straight, 2),
        "approx_road_km": round(road, 2),
        "approx_drive_minutes": drive_min,
        "assumed_speed_kmh": assumed_speed_kmh,
        "road_factor": road_factor,
        "disclaimer": (
            "Straight-line (haversine) with an indicative road factor and average speed — "
            "not binding mileage; confirm with your own routing and mileage policy."
        ),
    }


def routing_note_line(hint: dict[str, Any]) -> str:
    """Single line for opportunity notes."""
    return (
        f"Routing hint ({hint.get('base_label', 'Base')} → venue): "
        f"~{hint['straight_line_km']} km straight, "
        f"~{hint['approx_road_km']} km indicative road, "
        f"~{hint['approx_drive_minutes']} min drive at ~{int(hint.get('assumed_speed_kmh', 0))} km/h average."
    )


def routing_hint_for_account_venue(
    conn,
    *,
    account_id: int | None,
    venue: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """
    If the CRM account has operating base coordinates and ``venue`` has lat/lng,
    return a routing hint dict; otherwise None. Fails soft if DB columns are missing.
    """
    if account_id is None or not isinstance(venue, dict):
        return None
    try:
        la = float(venue["lat"])
        ln = float(venue["lng"])
    except (KeyError, TypeError, ValueError):
        return None
    cur = conn.cursor(dictionary=True)
    row = None
    try:
        cur.execute(
            """
            SELECT operating_base_lat, operating_base_lng, operating_base_label,
                   operating_base_postcode
            FROM crm_accounts WHERE id=%s
            """,
            (int(account_id),),
        )
        row = cur.fetchone()
    except Exception:
        row = None
    finally:
        try:
            cur.close()
        except Exception:
            pass
    if not row:
        return None
    try:
        bla = row.get("operating_base_lat")
        bln = row.get("operating_base_lng")
        if bla is None or bln is None:
            return None
        bla_f = float(bla)
        bln_f = float(bln)
    except (TypeError, ValueError):
        return None
    hint = routing_hint_base_to_venue(
        base_lat=bla_f,
        base_lng=bln_f,
        venue_lat=la,
        venue_lng=ln,
        base_label=row.get("operating_base_label"),
    )
    bpc = row.get("operating_base_postcode")
    if bpc:
        hint["base_postcode"] = str(bpc).strip()
    hint["origin"] = "account"
    return hint


def routing_hint_dispatch_base_to_venue(
    conn,
    *,
    dispatch_base_id: int | None,
    venue: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Use a row from ``crm_dispatch_bases`` as the departure point for distance to venue."""
    if dispatch_base_id is None or not isinstance(venue, dict):
        return None
    try:
        la = float(venue["lat"])
        ln = float(venue["lng"])
    except (KeyError, TypeError, ValueError):
        return None
    cur = conn.cursor(dictionary=True)
    row = None
    try:
        cur.execute(
            """
            SELECT id, label, lat, lng, postcode
            FROM crm_dispatch_bases WHERE id=%s
            """,
            (int(dispatch_base_id),),
        )
        row = cur.fetchone()
    except Exception:
        row = None
    finally:
        try:
            cur.close()
        except Exception:
            pass
    if not row:
        return None
    try:
        bla = row.get("lat")
        bln = row.get("lng")
        if bla is None or bln is None:
            return None
        bla_f = float(bla)
        bln_f = float(bln)
    except (TypeError, ValueError):
        return None
    hint = routing_hint_base_to_venue(
        base_lat=bla_f,
        base_lng=bln_f,
        venue_lat=la,
        venue_lng=ln,
        base_label=row.get("label"),
    )
    bpc = row.get("postcode")
    if bpc:
        hint["base_postcode"] = str(bpc).strip()
    hint["origin"] = "dispatch"
    hint["dispatch_base_id"] = int(row["id"])
    return hint


def routing_hint_for_intake(
    conn,
    *,
    dispatch_base_id: int | None,
    account_id: int | None,
    venue: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Prefer configured dispatch base; else fall back to account operating base."""
    h = routing_hint_dispatch_base_to_venue(
        conn, dispatch_base_id=dispatch_base_id, venue=venue
    )
    if h:
        return h
    return routing_hint_for_account_venue(conn, account_id=account_id, venue=venue)
