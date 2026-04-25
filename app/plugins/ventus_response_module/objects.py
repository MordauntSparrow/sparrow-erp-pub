import os
import re
import json
import math
import logging
from datetime import datetime
from app.objects import get_db_connection
import requests
from geopy.distance import geodesic

logger = logging.getLogger(__name__)

GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")


def _w3w_api_key():
    """Read key at call time so a restart picks up env changes; supports common variable names."""
    return (os.environ.get("W3W_API_KEY") or os.environ.get("WHAT3WORDS_API_KEY") or "").strip()


class TriageValidationError(Exception):
    pass


class UnitSignOnError(Exception):
    pass


class UnitSignOffError(Exception):
    pass


class JobClaimError(Exception):
    pass


class ResponseTriage:
    """Lat/lng → easting/northing (EPSG:27700) transformer (lazy); used for CAD map and coordinate readouts."""
    _wgs84_to_bng_tf = None

    # --- Original Geocoding Utilities ---
    @staticmethod
    def wgs84_to_bng_easting_northing(lat, lng):
        """
        Latitude / longitude (decimal degrees, EPSG:4326) → easting / northing in metres (EPSG:27700).
        Returns (easting, northing) as rounded integers, or (None, None) if unavailable.
        """
        try:
            latf = float(lat)
            lngf = float(lng)
        except (TypeError, ValueError):
            return None, None
        if not (-90 <= latf <= 90) or not (-180 <= lngf <= 180):
            return None, None
        try:
            from pyproj import Transformer
        except ImportError:
            logger.debug("pyproj not installed; easting/northing omitted")
            return None, None
        try:
            if ResponseTriage._wgs84_to_bng_tf is None:
                ResponseTriage._wgs84_to_bng_tf = Transformer.from_crs(
                    "EPSG:4326", "EPSG:27700", always_xy=True
                )
            e, n = ResponseTriage._wgs84_to_bng_tf.transform(lngf, latf)
            if not (math.isfinite(e) and math.isfinite(n)):
                return None, None
            return int(round(e)), int(round(n))
        except Exception as exc:
            logger.debug("BNG transform failed: %s", exc)
            return None, None

    @staticmethod
    def get_lat_lng_from_google(address, city="Crawley, UK"):
        if not GOOGLE_MAPS_API_KEY:
            return {"error": "Google Maps API key not configured"}
        formatted_address = f"{address}, {city}"
        url = f"https://maps.googleapis.com/maps/api/geocode/json?address={formatted_address}&key={GOOGLE_MAPS_API_KEY}"
        response = requests.get(url)
        data = response.json()
        if data["status"] == "OK":
            location = data["results"][0]["geometry"]["location"]
            return {"lat": location["lat"], "lng": location["lng"]}
        return {"error": "Google Maps could not find address"}

    @staticmethod
    def get_lat_lng_from_osm(address):
        if not address:
            return {"error": "No address provided"}
        url = f"https://nominatim.openstreetmap.org/search?q={address}&format=json&limit=1"
        try:
            response = requests.get(url, timeout=5)
            if response.status_code != 200 or not response.text.strip():
                return {"error": f"OSM API Error {response.status_code}"}
            data = response.json()
            if not data:
                return {"error": "OSM could not find address"}
            return {"lat": float(data[0]["lat"]), "lng": float(data[0]["lon"])}
        except Exception as e:
            return {"error": f"OSM lookup failed: {e}"}

    @staticmethod
    def get_lat_lng_from_postcode(postcode):
        if not postcode:
            return {"error": "No postcode provided"}
        url = f"https://api.postcodes.io/postcodes/{postcode}"
        response = requests.get(url)
        data = response.json()
        if data.get("status") == 200 and data.get("result"):
            return {"lat": data["result"]["latitude"], "lng": data["result"]["longitude"]}
        return {"error": "Postcode not found"}

    @staticmethod
    def normalize_w3w_words(raw):
        """Accept pasted URLs, ///word.word.word, spaces, extra slashes; return dotted phrase for the W3W API."""
        if raw is None:
            return ""
        s = str(raw).strip()
        # Strip BOM / zero-width chars sometimes pasted from web
        s = s.strip("\ufeff\u200b\u200c\u200d")
        if not s:
            return ""
        # Pasted links: https://what3words.com/word.word.word or https://w3w.co/...
        s = re.sub(
            r"(?i)^https?://(?:www\.)?(?:what3words\.com|w3w\.co)/",
            "",
            s,
        )
        s = s.strip()
        # Leading slashes: ASCII, fullwidth (／), fraction slash (⁄), division slash (∕)
        s = re.sub(r"^[/\uFF0F\u2044\u2215]+", "", s)
        # Unicode full stops / middle dots → ASCII dot (same idea as w3w batch converter input cleanup)
        for u in ("\u3002", "\uff0e", "\u00b7", "\u2219"):
            s = s.replace(u, ".")
        s = re.sub(r"\s+", ".", s)
        s = s.strip(".")
        parts = [p for p in s.split(".") if p]
        return ".".join(parts[:3]) if len(parts) >= 3 else ".".join(parts)

    @staticmethod
    def w3w_phrase_has_three_words(phrase):
        if not phrase or not str(phrase).strip():
            return False
        parts = [p for p in str(phrase).strip().split(".") if p]
        return len(parts) == 3

    @staticmethod
    def _w3w_format_api_error(data):
        if not isinstance(data, dict):
            return str(data) if data else "What3Words API error"
        err = data.get("error")
        if isinstance(err, dict):
            return str(err.get("message") or err.get("code") or err).strip() or "What3Words API error"
        if err is not None:
            return str(err)
        return "What3Words API error"

    @staticmethod
    def get_lat_lng_from_w3w(what3words):
        """Resolve a 3-word address to lat/lng via https://developer.what3words.com/public-api/docs#convert-to-coordinates"""
        phrase = ResponseTriage.normalize_w3w_words(what3words)
        api_key = _w3w_api_key()
        if not phrase:
            return {"error": "No what3words phrase provided"}
        if not ResponseTriage.w3w_phrase_has_three_words(phrase):
            return {"error": "what3words must be exactly three words separated by dots (e.g. filled.count.soap)"}
        if not api_key:
            logger.warning(
                "what3words convert-to-coordinates skipped: set W3W_API_KEY or WHAT3WORDS_API_KEY (get a key at what3words.com)"
            )
            return {"error": "What3Words API key not configured (set W3W_API_KEY or WHAT3WORDS_API_KEY on the server)"}
        try:
            response = requests.get(
                "https://api.what3words.com/v3/convert-to-coordinates",
                params={"words": phrase, "key": api_key},
                timeout=12,
                headers={
                    "User-Agent": "SparrowERP-VentusResponse/1.0",
                    "X-Api-Key": api_key,
                },
            )
            data = response.json() if response.content else {}
        except Exception as exc:
            logger.warning("what3words request failed: %s", exc)
            return {"error": f"What3Words request failed: {exc}"}
        if response.status_code != 200:
            msg = ResponseTriage._w3w_format_api_error(data)
            logger.warning("what3words API HTTP %s: %s", response.status_code, msg)
            return {"error": msg}
        if isinstance(data, dict) and data.get("coordinates"):
            loc = data["coordinates"]
            try:
                return {"lat": float(loc["lat"]), "lng": float(loc["lng"])}
            except (TypeError, ValueError, KeyError) as exc:
                logger.warning("what3words unexpected coordinates shape: %s", exc)
                return {"error": "What3Words returned invalid coordinates"}
        logger.warning("what3words 200 response without coordinates: %s", data)
        return {"error": "What3Words location not found"}

    @staticmethod
    def get_w3w_grid_section_lines(center_lat, center_lng, half_diagonal_km=2.0):
        """
        Official v3 grid-section: line segments for the 3m grid inside a bounding box.

        Docs: https://docs.what3words.com/api/v3/ — «Grid section». Bounding box is
        south,west,north,east (WGS84); corner-to-corner span must not exceed 4 km.

        half_diagonal_km is the geodesic distance from the centre toward the SW and NE
        corners (bearings 225° and 45°). Default 2.0 km uses the full ~4 km SW–NE span.
        """
        api_key = _w3w_api_key()
        if not api_key:
            logger.warning(
                "what3words grid-section skipped: set W3W_API_KEY or WHAT3WORDS_API_KEY"
            )
            return {
                "error": "What3Words API key not configured (set W3W_API_KEY or WHAT3WORDS_API_KEY on the server)",
            }
        try:
            clat = float(center_lat)
            clng = float(center_lng)
        except (TypeError, ValueError):
            return {"error": "invalid center coordinates"}
        if not (-90 <= clat <= 90) or not (-180 <= clng <= 180):
            return {"error": "center coordinates out of range"}
        max_half = 2.0  # API limit: corner-to-corner ≤ 4 km → two opposite legs from centre.
        try:
            half = float(half_diagonal_km)
        except (TypeError, ValueError):
            half = max_half
        if half <= 0:
            half = max_half
        elif half > max_half:
            half = max_half
        try:
            dist = geodesic(kilometers=half)
            sw_pt = dist.destination((clat, clng), 225)
            ne_pt = dist.destination((clat, clng), 45)
            south, west = float(sw_pt.latitude), float(sw_pt.longitude)
            north, east = float(ne_pt.latitude), float(ne_pt.longitude)
        except Exception as exc:
            logger.warning("what3words grid-section bbox failed: %s", exc)
            return {"error": f"Could not build bounding box: {exc}"}
        bbox = f"{south},{west},{north},{east}"
        try:
            response = requests.get(
                "https://api.what3words.com/v3/grid-section",
                params={"bounding-box": bbox, "key": api_key, "format": "json"},
                timeout=15,
                headers={
                    "User-Agent": "SparrowERP-VentusResponse/1.0",
                    "X-Api-Key": api_key,
                },
            )
            data = response.json() if response.content else {}
        except Exception as exc:
            logger.warning("what3words grid-section request failed: %s", exc)
            return {"error": f"What3Words request failed: {exc}"}
        if response.status_code != 200:
            msg = ResponseTriage._w3w_format_api_error(data)
            logger.warning("what3words grid-section HTTP %s: %s", response.status_code, msg)
            return {"error": msg}
        if not isinstance(data, dict):
            return {"error": "What3Words returned an invalid response"}
        lines = data.get("lines")
        if not isinstance(lines, list):
            return {"error": "What3Words returned no grid lines"}
        return {"lines": lines}

    @staticmethod
    def get_w3w_words_from_coordinates(lat, lng, language="en"):
        """
        Reverse geocode: WGS84 lat/lng → 3 word address (v3 convert-to-3wa).

        https://developer.what3words.com/public-api/docs#convert-to-3-word-address
        """
        api_key = _w3w_api_key()
        if not api_key:
            logger.warning(
                "what3words convert-to-3wa skipped: set W3W_API_KEY or WHAT3WORDS_API_KEY"
            )
            return {
                "error": "What3Words API key not configured (set W3W_API_KEY or WHAT3WORDS_API_KEY on the server)",
            }
        try:
            latf = float(lat)
            lngf = float(lng)
        except (TypeError, ValueError):
            return {"error": "invalid coordinates"}
        if not (-90 <= latf <= 90) or not (-180 <= lngf <= 180):
            return {"error": "coordinates out of range"}
        lang = (language or "en").strip().lower()[:8] or "en"
        try:
            response = requests.get(
                "https://api.what3words.com/v3/convert-to-3wa",
                params={
                    "coordinates": f"{latf},{lngf}",
                    "key": api_key,
                    "language": lang,
                    "format": "json",
                },
                timeout=12,
                headers={
                    "User-Agent": "SparrowERP-VentusResponse/1.0",
                    "X-Api-Key": api_key,
                },
            )
            data = response.json() if response.content else {}
        except Exception as exc:
            logger.warning("what3words convert-to-3wa request failed: %s", exc)
            return {"error": f"What3Words request failed: {exc}"}
        if response.status_code != 200:
            msg = ResponseTriage._w3w_format_api_error(data)
            logger.warning("what3words convert-to-3wa HTTP %s: %s", response.status_code, msg)
            return {"error": msg}
        if not isinstance(data, dict):
            return {"error": "What3Words returned an invalid response"}
        words = str(data.get("words") or "").strip()
        if not words:
            return {"error": "What3Words returned no address"}
        return {
            "words": words,
            "nearest_place": data.get("nearestPlace"),
            "country": data.get("country"),
            "map_url": data.get("map"),
            "language": data.get("language"),
        }

    @staticmethod
    def is_within_range(coord1, coord2, max_distance=0.5):
        return geodesic((coord1["lat"], coord1["lng"]), (coord2["lat"], coord2["lng"])).km <= max_distance

    @staticmethod
    def get_best_lat_lng(address=None, postcode=None, what3words=None, manual_lat=None, manual_lng=None):
        """Resolve coordinates. Caller-supplied latitude/longitude (both valid) skip all external APIs."""
        try:
            if manual_lat is not None and manual_lng is not None:
                lat = float(manual_lat)
                lng = float(manual_lng)
                if -90 <= lat <= 90 and -180 <= lng <= 180:
                    return {"lat": lat, "lng": lng, "source": "manual_coordinates"}
        except (TypeError, ValueError):
            pass
        # what3words first when provided (even if postcode/address also present).
        w3w_fail = None
        w3w_phrase = ResponseTriage.normalize_w3w_words(what3words) if what3words else ""
        if w3w_phrase:
            if not ResponseTriage.w3w_phrase_has_three_words(w3w_phrase):
                return {
                    "error": "what3words must be exactly three words separated by dots (e.g. filled.count.soap)",
                }
            w3w_result = ResponseTriage.get_lat_lng_from_w3w(w3w_phrase)
            if "lat" in w3w_result:
                return w3w_result
            w3w_fail = w3w_result
        postcode_result = ResponseTriage.get_lat_lng_from_postcode(
            postcode) if postcode else None
        postcode_coords = postcode_result if postcode_result and "lat" in postcode_result else None
        google_result = ResponseTriage.get_lat_lng_from_google(
            address) if address else None
        google_coords = google_result if google_result and "lat" in google_result else None
        osm_result = ResponseTriage.get_lat_lng_from_osm(
            address) if address else None
        osm_coords = osm_result if osm_result and "lat" in osm_result else None
        if google_coords and postcode_coords:
            if ResponseTriage.is_within_range(google_coords, postcode_coords):
                return google_coords
            else:
                return postcode_coords
        elif osm_coords and postcode_coords:
            if ResponseTriage.is_within_range(osm_coords, postcode_coords):
                return osm_coords
            else:
                return postcode_coords
        elif postcode_coords:
            return postcode_coords
        elif google_coords:
            return google_coords
        elif osm_coords:
            return osm_coords
        if w3w_fail and isinstance(w3w_fail, dict) and w3w_fail.get("error"):
            return w3w_fail
        return {"error": "No location found"}

    # --- Original triage persistence ---
    @staticmethod
    def create(**triage_data):
        conn = get_db_connection()
        try:
            pre = triage_data.get("coordinates")
            if isinstance(pre, dict) and pre.get("lat") is not None and pre.get("lng") is not None:
                try:
                    lat = float(pre["lat"])
                    lng = float(pre["lng"])
                    if -90 <= lat <= 90 and -180 <= lng <= 180:
                        best_coordinates = {k: v for k, v in pre.items() if k != "error"}
                        best_coordinates["lat"] = lat
                        best_coordinates["lng"] = lng
                        triage_data["coordinates"] = best_coordinates
                    else:
                        best_coordinates = None
                except (TypeError, ValueError):
                    best_coordinates = None
                if best_coordinates is None:
                    best_coordinates = ResponseTriage.get_best_lat_lng(
                        address=triage_data.get("address"),
                        postcode=triage_data.get("postcode"),
                        what3words=triage_data.get("what3words"),
                    )
                    triage_data["coordinates"] = best_coordinates
            else:
                best_coordinates = ResponseTriage.get_best_lat_lng(
                    address=triage_data.get("address"),
                    postcode=triage_data.get("postcode"),
                    what3words=triage_data.get("what3words"),
                )
                triage_data["coordinates"] = best_coordinates
            query = """
                INSERT INTO response_triage 
                (created_by, vita_record_id, first_name, middle_name, last_name, 
                 patient_dob, phone_number, address, postcode, entry_requirements, reason_for_call, 
                 onset_datetime, patient_alone, exclusion_data, risk_flags, decision, coordinates)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSON), %s, %s, %s, 
                        CAST(%s AS JSON), CAST(%s AS JSON), %s, CAST(%s AS JSON))
            """
            patient_alone_raw = triage_data.get("patient_alone")
            if isinstance(patient_alone_raw, str):
                p = patient_alone_raw.strip().lower()
                if p in ("yes", "y", "true", "1"):
                    patient_alone_val = 1
                elif p in ("no", "n", "false", "0"):
                    patient_alone_val = 0
                else:
                    patient_alone_val = None
            elif patient_alone_raw in (0, 1, None):
                patient_alone_val = patient_alone_raw
            else:
                patient_alone_val = None
            with conn.cursor() as cursor:
                cursor.execute(query, (
                    triage_data.get("created_by"),
                    triage_data.get("vita_record_id"),
                    triage_data.get("first_name"),
                    triage_data.get("middle_name"),
                    triage_data.get("last_name"),
                    triage_data.get("patient_dob"),
                    triage_data.get("phone_number"),
                    triage_data.get("address"),
                    triage_data.get("postcode"),
                    json.dumps(triage_data.get("entry_requirements") or []),
                    triage_data.get("reason_for_call"),
                    triage_data.get("onset_datetime"),
                    patient_alone_val,
                    json.dumps(triage_data.get("exclusion_data") or {}),
                    json.dumps(triage_data.get("risk_flags") or []),
                    triage_data.get("decision"),
                    json.dumps(triage_data.get("coordinates") or {})
                ))
                conn.commit()
                new_id = cursor.lastrowid
            return new_id
        finally:
            conn.close()

    @staticmethod
    def post_triage_external_dispatch(triage_data):
        """No-op stub; optional outbound dispatch fires on CAD assignment (bridge module), not triage."""
        pass

    @staticmethod
    def get_by_id(record_id):
        conn = get_db_connection()
        try:
            query = "SELECT * FROM response_triage WHERE id = %s"
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(query, (record_id,))
                row = cursor.fetchone()
            if row and row.get('selected_conditions'):
                row['selected_conditions'] = json.loads(
                    row['selected_conditions'])
            return row
        finally:
            conn.close()

    @staticmethod
    def get_all():
        conn = get_db_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            query = """
                SELECT id, created_by, created_at, postcode, decision, reason_for_call, exclusion_data
                FROM response_triage
                ORDER BY created_at DESC
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            return rows
        finally:
            conn.close()

    # --- MDT API Methods (New Additions) ---
    @staticmethod
    def health_check():
        return {"status": "ok"}

    @staticmethod
    def sign_on(callsign, timestamp):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO mdts_signed_on (callSign, signOnTime, status)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE signOnTime = VALUES(signOnTime), status='on_standby'
                """, (callsign, timestamp, 'on_standby'))
                conn.commit()
            return {"message": "Signed on"}
        finally:
            conn.close()

    @staticmethod
    def sign_off(callsign):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM mdts_signed_on WHERE callSign = %s", (callsign,))
                conn.commit()
            return {"message": "Signed off"}
        finally:
            conn.close()

    @staticmethod
    def get_next_job(callsign):
        conn = get_db_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT cad FROM mdt_jobs
                    WHERE status = 'queued'
                    ORDER BY created_at ASC LIMIT 1
                """)
                job = cur.fetchone()
                return job  # None if no job
        finally:
            conn.close()

    @staticmethod
    def claim_job(cad, callsign):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS mdt_job_units (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        job_cad INT NOT NULL,
                        callsign VARCHAR(64) NOT NULL,
                        assigned_by VARCHAR(120),
                        assigned_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY uq_job_callsign (job_cad, callsign),
                        INDEX idx_job_cad (job_cad),
                        INDEX idx_callsign (callsign)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                cur.execute("""
                    UPDATE mdt_jobs SET status='claimed', claimedAt=NOW()
                    WHERE cad=%s AND status='queued'
                """, (cad,))
                if cur.rowcount != 1:
                    conn.rollback()
                    raise JobClaimError("Job already claimed or not found")
                cur.execute("""
                    INSERT INTO mdt_job_units (job_cad, callsign, assigned_by)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE assigned_by=VALUES(assigned_by), assigned_at=CURRENT_TIMESTAMP
                """, (cad, callsign, "mdt_claim"))
                cur.execute("""
                    UPDATE mdt_jobs j
                    JOIN (
                      SELECT job_cad, GROUP_CONCAT(callsign ORDER BY assigned_at SEPARATOR ',') AS claimed
                      FROM mdt_job_units
                      WHERE job_cad = %s
                      GROUP BY job_cad
                    ) x ON x.job_cad = j.cad
                    SET j.claimedBy = x.claimed
                    WHERE j.cad = %s
                """, (cad, cad))
                cur.execute("""
                    UPDATE mdts_signed_on SET assignedIncident=%s, status='assigned'
                    WHERE callSign=%s
                """, (cad, callsign))
                conn.commit()
            return {"message": "Job claimed"}
        finally:
            conn.close()

    @staticmethod
    def get_job_details(cad):
        conn = get_db_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT cad, status, triage_data
                    FROM mdt_jobs WHERE cad=%s
                """, (cad,))
                job = cur.fetchone()
                if not job:
                    return None
                if isinstance(job["triage_data"], str):
                    try:
                        job["triage_data"] = json.loads(job["triage_data"])
                    except Exception:
                        pass
                return job
        finally:
            conn.close()

    @staticmethod
    def update_status(cad, callsign, status, time):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE mdt_jobs SET status=%s, lastStatusTime=%s
                    WHERE cad=%s
                """, (status, time, cad))
                cur.execute("""
                    UPDATE mdts_signed_on SET status=%s
                    WHERE callSign=%s
                """, (status, callsign))
                conn.commit()
            return {"message": "Status updated"}
        finally:
            conn.close()

    @staticmethod
    def update_location(callsign, latitude, longitude, timestamp, status):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO mdt_locations (callSign, latitude, longitude, timestamp, status)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        latitude=VALUES(latitude),
                        longitude=VALUES(longitude),
                        timestamp=VALUES(timestamp),
                        status=VALUES(status)
                """, (callsign, latitude, longitude, timestamp, status))
                conn.commit()
            return {"message": "Location updated"}
        finally:
            conn.close()

    @staticmethod
    def get_all_locations():
        conn = get_db_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT callSign, latitude, longitude, timestamp, status
                    FROM mdt_locations
                """)
                return cur.fetchall()
        finally:
            conn.close()

    @staticmethod
    def get_unread_message_count(callsign):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM messages
                    WHERE recipient=%s AND `read`=0
                      AND LOWER(COALESCE(text,'')) NOT REGEXP '^[[:space:]]*cad[[:space:]]*#[0-9]+[[:space:]]+update:'
                """, (callsign,))
                row = cur.fetchone()
                return {"count": row[0] if row else 0}
        finally:
            conn.close()

    @staticmethod
    def get_messages(callsign):
        conn = get_db_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT id, `from`, text, timestamp, `read`
                    FROM messages
                    WHERE recipient=%s
                      AND LOWER(COALESCE(text,'')) NOT REGEXP '^[[:space:]]*cad[[:space:]]*#[0-9]+[[:space:]]+update:'
                    ORDER BY timestamp ASC
                """, (callsign,))
                return cur.fetchall()
        finally:
            conn.close()

    @staticmethod
    def post_message(callsign, text, sender="mdt"):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO messages (`from`, recipient, text, timestamp, `read`)
                    VALUES (%s, %s, %s, %s, 0)
                """, (sender, callsign, text, datetime.utcnow()))
                conn.commit()
            return {"message": "Message sent"}
        finally:
            conn.close()

    @staticmethod
    def mark_message_read(callsign, message_id):
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE messages SET `read`=1
                    WHERE id=%s AND recipient=%s
                """, (message_id, callsign))
                conn.commit()
            return {"message": "Message marked as read"}
        finally:
            conn.close()

    @staticmethod
    def get_standby_location(callsign):
        conn = get_db_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT name, lat, lng, isNew
                    FROM standby_locations
                    WHERE callSign=%s
                    ORDER BY updatedAt DESC LIMIT 1
                """, (callsign,))
                row = cur.fetchone()
                if not row:
                    return {"standbyLocation": None, "isNew": False}
                return {
                    "standbyLocation": {
                        "name": row["name"],
                        "lat": row["lat"],
                        "lng": row["lng"]
                    },
                    "isNew": bool(row["isNew"])
                }
        finally:
            conn.close()

    @staticmethod
    def get_history(callsign):
        conn = get_db_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("SHOW TABLES LIKE 'mdt_job_units'")
                if cur.fetchone() is None:
                    return []
                cur.execute("""
                    SELECT j.cad, j.completedAt, j.chief_complaint, j.outcome
                    FROM mdt_jobs j
                    JOIN mdt_job_units u ON u.job_cad = j.cad
                    WHERE u.callsign=%s AND j.status='cleared'
                    ORDER BY j.completedAt DESC
                """, (callsign,))
                return cur.fetchall()
        finally:
            conn.close()
