from datetime import datetime
import logging
from app.objects import get_db_connection


class EventService:
    """
    Service class for CRUD and search operations on events.
    """

    ALLOWED_FIELDS = {
        "event_name", "category", "entry_cost", "start_date", "end_date", "start_time", "end_time",
        "food_menu_path", "flyer_path", "music_type", "is_public",
        "band_details_public", "band_details_private",
        "dj_details_public", "dj_details_private",
        "other_music_details_public", "other_music_details_private",
        "event_details_public", "event_details_private"
    }

    @staticmethod
    def _coerce_event_data(data):
        """
        Sanitize and coerce input data for DB insertion/update.
        """
        clean = {}
        for field in EventService.ALLOWED_FIELDS:
            val = data.get(field)
            if field == "is_public":
                clean[field] = int(val) if val is not None else 1
            elif field == "entry_cost":
                try:
                    clean[field] = float(val) if val not in (
                        None, "") else None
                except Exception:
                    clean[field] = None
            else:
                clean[field] = val if val not in (None, "") else None
        return clean

    @staticmethod
    def get_all_events():
        """
        Return all events for admin.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT * FROM events
                ORDER BY start_date ASC, start_time ASC
            """)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_public_events():
        """
        Return public/upcoming events for website.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT * FROM events
                WHERE is_public = 1
                  AND (end_date >= CURDATE() OR start_date >= CURDATE())
                ORDER BY start_date ASC, start_time ASC
            """)
            return cur.fetchall() or []
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def get_event_by_id(event_id):
        """
        Return a single event by ID.
        """
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM events WHERE id = %s", (event_id,))
            return cur.fetchone()
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def create_event(data):
        """
        Insert a new event. Returns the inserted event or None on error.
        """
        clean = EventService._coerce_event_data(data)
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                INSERT INTO events (
                    event_name, category, entry_cost, start_date, end_date, start_time, end_time,
                    food_menu_path, flyer_path, music_type, is_public,
                    band_details_public, band_details_private,
                    dj_details_public, dj_details_private,
                    other_music_details_public, other_music_details_private,
                    event_details_public, event_details_private, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                clean["event_name"], clean["category"], clean["entry_cost"], clean[
                    "start_date"], clean["end_date"], clean["start_time"], clean["end_time"],
                clean["food_menu_path"], clean["flyer_path"], clean["music_type"], clean["is_public"],
                clean["band_details_public"], clean["band_details_private"],
                clean["dj_details_public"], clean["dj_details_private"],
                clean["other_music_details_public"], clean["other_music_details_private"],
                clean["event_details_public"], clean["event_details_private"], datetime.utcnow()
            ))
            conn.commit()
            event_id = cur.lastrowid
            return EventService.get_event_by_id(event_id)
        except Exception as e:
            logging.exception("Error inserting event: %s", e)
            return None
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def update_event(event_id, data):
        """
        Update an event. Returns the updated event or None on error.
        """
        clean = EventService._coerce_event_data(data)
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                UPDATE events
                SET event_name=%s, category=%s, entry_cost=%s, start_date=%s, end_date=%s, start_time=%s, end_time=%s,
                    food_menu_path=%s, flyer_path=%s, music_type=%s, is_public=%s,
                    band_details_public=%s, band_details_private=%s,
                    dj_details_public=%s, dj_details_private=%s,
                    other_music_details_public=%s, other_music_details_private=%s,
                    event_details_public=%s, event_details_private=%s
                WHERE id=%s
            """, (
                clean["event_name"], clean["category"], clean["entry_cost"], clean[
                    "start_date"], clean["end_date"], clean["start_time"], clean["end_time"],
                clean["food_menu_path"], clean["flyer_path"], clean["music_type"], clean["is_public"],
                clean["band_details_public"], clean["band_details_private"],
                clean["dj_details_public"], clean["dj_details_private"],
                clean["other_music_details_public"], clean["other_music_details_private"],
                clean["event_details_public"], clean["event_details_private"], event_id
            ))
            conn.commit()
            return EventService.get_event_by_id(event_id)
        except Exception as e:
            logging.exception("Error updating event: %s", e)
            return None
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def delete_event(event_id):
        """
        Delete an event.
        """
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM events WHERE id=%s", (event_id,))
            conn.commit()
        except Exception as e:
            logging.exception("Error deleting event: %s", e)
        finally:
            cur.close()
            conn.close()

    @staticmethod
    def search_events(
        keyword=None,
        start_date=None,
        end_date=None,
        music_type=None,
        is_public=None,
        page=1,
        page_size=20
    ):
        """
        Search and filter events. Returns paginated results.
        """
        where = []
        params = []
        if keyword:
            where.append("(" +
                         "event_name LIKE %s OR " +
                         "category LIKE %s OR " +
                         "event_details_public LIKE %s OR " +
                         "band_details_public LIKE %s OR " +
                         "dj_details_public LIKE %s OR " +
                         "other_music_details_public LIKE %s" +
                         ")")
            kw = f"%{keyword}%"
            params.extend([kw]*6)
        if start_date:
            where.append("start_date >= %s")
            params.append(start_date)
        if end_date:
            where.append("end_date <= %s")
            params.append(end_date)
        if music_type:
            where.append("music_type = %s")
            params.append(music_type)
        if is_public is not None:
            where.append("is_public = %s")
            params.append(int(is_public))
        where_sql = " AND ".join(where) if where else "1=1"
        offset = (page - 1) * page_size

        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"""
                SELECT * FROM events
                WHERE {where_sql}
                ORDER BY start_date ASC, start_time ASC
                LIMIT %s OFFSET %s
                """,
                (*params, page_size, offset)
            )
            events = cur.fetchall() or []
            # Get total count for pagination
            cur.execute(
                f"SELECT COUNT(*) as cnt FROM events WHERE {where_sql}", tuple(params))
            total = (cur.fetchone() or {}).get("cnt", 0)
            return {"items": events, "total": total, "page": page, "page_size": page_size}
        except Exception as e:
            logging.exception("Error searching events: %s", e)
            return {"items": [], "total": 0, "page": page, "page_size": page_size}
        finally:
            cur.close()
            conn.close()
