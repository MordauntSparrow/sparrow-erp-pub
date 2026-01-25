# keep import here to avoid Flask requirement at module import time in some contexts
from flask import render_template
import os
import json
import uuid
import mimetypes
import requests
from datetime import datetime, timedelta
from typing import Any
from app.objects import get_db_connection


# ------------------------
# Data folder helper
# ------------------------
def ensure_data_folder(module_dir: str) -> str:
    """
    Ensure that a 'data' folder exists inside the given module directory.
    Returns the absolute path to the data folder.
    """
    data_folder = os.path.join(module_dir, 'data')
    os.makedirs(data_folder, exist_ok=True)
    return data_folder


# ------------------------
# Small shared helpers
# ------------------------
def _ensure_url_has_scheme(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return "https://" + u


def _safe_json_load(path: str, default):
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if data is not None else default
    except Exception:
        return default


# -------------------- AnalyticsManager --------------------
class AnalyticsManager:
    """
    DB-backed AnalyticsManager (no analytics.json).

    Storage model (all aggregates; no raw per-visit rows):
      - website_page_views_daily(day, path, views)
      - website_page_views_hourly(day, hour, views)
      - website_page_views_country_daily(day, country, views)

    Geo lookup:
      - Uses ip-api.com (cached in geo_cache.json) to convert IP -> country
      - We do NOT store IPs in DB (only aggregated country counts)
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.geo_cache_file = os.path.join(data_dir, "geo_cache.json")

        os.makedirs(self.data_dir, exist_ok=True)

        if not os.path.exists(self.geo_cache_file):
            with open(self.geo_cache_file, 'w', encoding='utf-8') as f:
                json.dump({}, f, indent=2)

    # -----------------------
    # Recording (DB)
    # -----------------------
    def record_page_view(self, page, ip_address, user_agent, referrer=None, extra_fields=None):
        """
        Record a page view as aggregate increments.
        - daily per page path
        - hourly totals
        - daily per country

        Never raises (analytics must not break public requests).
        """
        path = self._normalize_page(page)
        if not path:
            return

        # Keep UTC to match your existing logic
        now = datetime.utcnow()
        day = now.date()
        hour = int(now.hour)

        country = self.get_country_from_ip(ip_address) or "Unknown"

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                # 1) Daily per-path
                cur.execute(
                    """
                    INSERT INTO website_page_views_daily (day, path, views)
                    VALUES (%s, %s, 1)
                    ON DUPLICATE KEY UPDATE views = views + 1
                    """,
                    (day, path)
                )

                # 2) Hourly totals (site-wide)
                cur.execute(
                    """
                    INSERT INTO website_page_views_hourly (day, hour, views)
                    VALUES (%s, %s, 1)
                    ON DUPLICATE KEY UPDATE views = views + 1
                    """,
                    (day, hour)
                )

                # 3) Daily per-country
                cur.execute(
                    """
                    INSERT INTO website_page_views_country_daily (day, country, views)
                    VALUES (%s, %s, 1)
                    ON DUPLICATE KEY UPDATE views = views + 1
                    """,
                    (day, country)
                )

                conn.commit()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as e:
            try:
                print(f"[WARN] Analytics DB write failed: {e}")
            except Exception:
                pass

    def _normalize_page(self, page: str) -> str:
        """
        Normalizes a page identifier into a stable path:
          - if full URL, keep only path
          - strip querystring and fragment
          - ensure leading slash
          - cap length to 512 (DB column)
        """
        if not page:
            return ""

        try:
            p = str(page).strip()
        except Exception:
            return ""

        # If it's a full URL, keep only the path portion
        try:
            from urllib.parse import urlparse
            parsed = urlparse(p)
            if parsed.scheme and parsed.netloc:
                p = parsed.path or "/"
        except Exception:
            pass

        # Strip querystring / fragment
        if "?" in p:
            p = p.split("?", 1)[0]
        if "#" in p:
            p = p.split("#", 1)[0]

        p = (p or "").strip()
        if not p:
            return ""

        if not p.startswith("/"):
            p = "/" + p

        if len(p) > 512:
            p = p[:512]

        return p

    # -----------------------
    # Time range helpers (UTC)
    # -----------------------
    def get_timerange(self, period):
        """
        Returns (start, end) in UTC for the current period.
        end is exclusive (ts < end).
        """
        now = datetime.utcnow()

        if period == "today":
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period == "weekly":
            start = (now - timedelta(days=6)).replace(hour=0,
                                                      minute=0, second=0, microsecond=0)
        elif period == "monthly":
            start = (now - timedelta(days=29)).replace(hour=0,
                                                       minute=0, second=0, microsecond=0)
        elif period == "year":
            start = (now - timedelta(days=364)).replace(hour=0,
                                                        minute=0, second=0, microsecond=0)
        else:
            return None, None

        end = now
        return start, end

    def get_previous_timerange(self, period):
        start, end = self.get_timerange(period)
        if start is None and end is None:
            return None, None

        delta = end - start
        prev_end = start
        prev_start = prev_end - delta
        return prev_start, prev_end

    def _daterange_bounds(self, time_range):
        """
        Convert (start_dt, end_dt) -> (start_date, end_date) inclusive.
        Your original end is exclusive, but for DATE aggregates we treat end_date as end.date().
        """
        if not time_range or time_range == (None, None):
            return None, None

        start, end = time_range
        start_date = start.date() if start else None
        end_date = end.date() if end else None
        return start_date, end_date

    # -----------------------
    # DB getters (no JSON)
    # -----------------------
    def get_page_views(self):
        """
        Legacy method in the old JSON version.
        Not supported in DB aggregate mode (no raw events).
        """
        return []

    def get_views_by_hour(self, time_range=None):
        """
        Returns {0..23: views} summed across the selected date range.
        """
        hourly = {i: 0 for i in range(24)}
        start_date, end_date = self._daterange_bounds(time_range)

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if start_date and end_date:
                cur.execute(
                    """
                    SELECT hour, SUM(views) AS total
                    FROM website_page_views_hourly
                    WHERE day >= %s AND day <= %s
                    GROUP BY hour
                    """,
                    (start_date, end_date)
                )
            elif start_date:
                cur.execute(
                    """
                    SELECT hour, SUM(views) AS total
                    FROM website_page_views_hourly
                    WHERE day >= %s
                    GROUP BY hour
                    """,
                    (start_date,)
                )
            else:
                cur.execute(
                    """
                    SELECT hour, SUM(views) AS total
                    FROM website_page_views_hourly
                    GROUP BY hour
                    """
                )

            for hr, total in (cur.fetchall() or []):
                try:
                    hr_i = int(hr)
                    if 0 <= hr_i <= 23:
                        hourly[hr_i] = int(total or 0)
                except Exception:
                    pass

            return hourly
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_views_by_weekday(self, time_range=None):
        """
        Returns {0..6: views} where 0=Monday.
        Derived from daily totals (sum across all paths per day, then bucket by weekday).
        """
        weekdays = {i: 0 for i in range(7)}
        start_date, end_date = self._daterange_bounds(time_range)

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if start_date and end_date:
                cur.execute(
                    """
                    SELECT day, SUM(views) AS total
                    FROM website_page_views_daily
                    WHERE day >= %s AND day <= %s
                    GROUP BY day
                    """,
                    (start_date, end_date)
                )
            elif start_date:
                cur.execute(
                    """
                    SELECT day, SUM(views) AS total
                    FROM website_page_views_daily
                    WHERE day >= %s
                    GROUP BY day
                    """,
                    (start_date,)
                )
            else:
                cur.execute(
                    """
                    SELECT day, SUM(views) AS total
                    FROM website_page_views_daily
                    GROUP BY day
                    """
                )

            rows = cur.fetchall() or []
            for day_val, total in rows:
                try:
                    # mysql-connector returns DATE as datetime.date
                    wd = day_val.weekday()
                    weekdays[wd] = weekdays.get(wd, 0) + int(total or 0)
                except Exception:
                    pass

            return weekdays
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_requests_by_country(self, time_range=None):
        """
        Returns {country: views} for the selected date range.
        """
        start_date, end_date = self._daterange_bounds(time_range)

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if start_date and end_date:
                cur.execute(
                    """
                    SELECT country, SUM(views) AS total
                    FROM website_page_views_country_daily
                    WHERE day >= %s AND day <= %s
                    GROUP BY country
                    ORDER BY total DESC
                    """,
                    (start_date, end_date)
                )
            elif start_date:
                cur.execute(
                    """
                    SELECT country, SUM(views) AS total
                    FROM website_page_views_country_daily
                    WHERE day >= %s
                    GROUP BY country
                    ORDER BY total DESC
                    """,
                    (start_date,)
                )
            else:
                cur.execute(
                    """
                    SELECT country, SUM(views) AS total
                    FROM website_page_views_country_daily
                    GROUP BY country
                    ORDER BY total DESC
                    """
                )

            counts = {}
            for country, total in (cur.fetchall() or []):
                c = (country or "Unknown")
                counts[c] = int(total or 0)
            return counts
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def get_popular_pages(self, period="alltime", time_range=None, limit=100):
        """
        Returns list of (path, views) sorted desc.
        """
        if time_range is None and period != "alltime":
            time_range = self.get_timerange(period)

        start_date, end_date = self._daterange_bounds(time_range)

        conn = get_db_connection()
        cur = conn.cursor()
        try:
            if period == "alltime" and not start_date and not end_date:
                cur.execute(
                    """
                    SELECT path, SUM(views) AS total
                    FROM website_page_views_daily
                    GROUP BY path
                    ORDER BY total DESC
                    LIMIT %s
                    """,
                    (int(limit),)
                )
            else:
                if start_date and end_date:
                    cur.execute(
                        """
                        SELECT path, SUM(views) AS total
                        FROM website_page_views_daily
                        WHERE day >= %s AND day <= %s
                        GROUP BY path
                        ORDER BY total DESC
                        LIMIT %s
                        """,
                        (start_date, end_date, int(limit))
                    )
                elif start_date:
                    cur.execute(
                        """
                        SELECT path, SUM(views) AS total
                        FROM website_page_views_daily
                        WHERE day >= %s
                        GROUP BY path
                        ORDER BY total DESC
                        LIMIT %s
                        """,
                        (start_date, int(limit))
                    )
                else:
                    cur.execute(
                        """
                        SELECT path, SUM(views) AS total
                        FROM website_page_views_daily
                        GROUP BY path
                        ORDER BY total DESC
                        LIMIT %s
                        """,
                        (int(limit),)
                    )

            rows = cur.fetchall() or []
            return [(r[0], int(r[1] or 0)) for r in rows]
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    # -----------------------
    # Geo lookup with caching (file)
    # -----------------------
    def _load_geo_cache(self):
        return _safe_json_load(self.geo_cache_file, {})

    def _save_geo_cache(self, cache):
        try:
            with open(self.geo_cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def get_country_from_ip(self, ip):
        """
        Uses ip-api.com to retrieve the country for the given IP.
        For local IPs, returns "United Kingdom" for testing.
        Adds a simple cache to avoid repeated HTTP calls.
        """
        if not ip:
            return "Unknown"

        if ip.startswith(("127.", "192.", "10.")):
            return "United Kingdom"

        cache = self._load_geo_cache()
        if ip in cache:
            return cache[ip]

        try:
            resp = requests.get(
                f"https://ip-api.com/json/{ip}?fields=country", timeout=2)
            if resp.status_code == 200:
                country = resp.json().get("country", "Unknown") or "Unknown"
            else:
                country = "Unknown"
        except Exception:
            country = "Unknown"

        cache[ip] = country
        self._save_geo_cache(cache)
        return country

# -------------------- ContactFormConfigManager --------------------


class ContactFormConfigManager:
    """
    Manages contact form configuration.
    Reads and writes settings to a JSON file (contact_form_config.json) in the website module's data folder.
    Each configuration maps a form identifier to its settings (e.g., recipient and subject).
    """

    def __init__(self, module_dir):
        self.data_folder = ensure_data_folder(module_dir)
        self.config_file = os.path.join(
            self.data_folder, 'contact_form_config.json')
        self.config = self.load_config()

    def load_config(self):
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data if isinstance(data, dict) else {}
            except Exception as e:
                print(f"Error loading contact form configuration: {e}")
        empty_config = {}
        self.save_config(empty_config)
        return empty_config

    def save_config(self, config):
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            self.config = config
        except Exception as e:
            print(f"Error saving contact form configuration: {e}")

    def update_configuration(self, form_id, recipient, subject):
        # Normalize keys to avoid duplicates like "Contact" vs "contact"
        fid = (form_id or "").strip().lower()
        if not fid:
            raise ValueError("form_id is required")
        self.config[fid] = {"recipient": recipient, "subject": subject}
        self.save_config(self.config)
        print(f"Updated configuration for form '{fid}'.")

    def get_configuration(self):
        return self.config


# -------------------- SpamProtection --------------------
class SpamProtection:
    """
    Centralized spam protection for contact forms.
    Checks the honeypot field (named "website") and, if Turnstile keys are configured in the core manifest,
    verifies the Turnstile token via Cloudflare Turnstile.
    """

    def __init__(self, config: dict):
        self.turnstile_site_key = (config.get(
            "turnstile", {}) or {}).get("site_key", "").strip()
        self.turnstile_secret_key = (config.get(
            "turnstile", {}) or {}).get("secret_key", "").strip()

    def is_spam(self, form, remote_ip: str = ""):
        if form.get("website", "").strip():
            return True, "Honeypot field filled"

        if self.turnstile_site_key and self.turnstile_secret_key:
            token = form.get("cf-turnstile-response", "").strip()
            if not token:
                # Not spam, but also not verified; you can choose to block if you want.
                return False, "Turnstile token missing"

            try:
                url = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
                payload = {
                    "secret": self.turnstile_secret_key,
                    "response": token,
                    "remoteip": remote_ip or "",
                }
                r = requests.post(url, data=payload, timeout=5)
                result = r.json() if r is not None else {}
                if not result.get("success", False):
                    return True, "Turnstile verification failed"
            except Exception as e:
                return True, f"Turnstile verification error: {e}"

        return False, ""


# -------------------- ContactFormSubmissionManager --------------------
class ContactFormSubmissionManager:
    """
    Manages contact form submissions by:
      1. Recording all submitted data into a JSON file.
      2. Processing the submission according to the contact form configuration.
         The submission is only processed if a configuration exists for the submitted form_id
         (comparison is case-insensitive). If the Sales module is installed (via the core manifest),
         the submission is forwarded; otherwise, it is emailed using the EmailManager from the core module.

    Email output:
      - Sends a modern HTML email with logo (if available) + a clean table of fields.
      - Still includes raw JSON at the bottom for debugging.
    """

    def __init__(self, data_dir: str):
        self.submissions_file = os.path.join(
            data_dir, "contact_submissions.json")
        if not os.path.exists(self.submissions_file):
            with open(self.submissions_file, 'w', encoding='utf-8') as f:
                json.dump([], f, indent=2)

    def record_submission(self, submission_data: dict):
        submissions = self._load_submissions()
        submissions.append(submission_data)
        self._save_submissions(submissions)
        print(f"Recorded contact form submission: {submission_data}")

    def _load_submissions(self):
        return _safe_json_load(self.submissions_file, [])

    def _save_submissions(self, submissions):
        with open(self.submissions_file, 'w', encoding='utf-8') as f:
            json.dump(submissions, f, indent=2, ensure_ascii=False)

    # --- Email rendering helpers ---

    def _guess_sender_label(self, submission_data: dict) -> str:
        for k in ("name", "full_name", "fullname", "customer_name"):
            v = (submission_data.get(k) or "").strip()
            if v:
                return v
        for k in ("email", "email_address"):
            v = (submission_data.get(k) or "").strip()
            if v:
                return v
        return "Website visitor"

    def _safe_items_for_table(self, submission_data: dict) -> list[tuple[str, str]]:
        items = []
        for k, v in (submission_data or {}).items():
            if v is None:
                continue

            key = str(k).strip()
            if not key:
                continue

            if isinstance(v, (dict, list)):
                val = json.dumps(v, ensure_ascii=False)
                if len(val) > 300:
                    val = val[:300] + "…"
            else:
                val = str(v)

            items.append((key, val))
        return items

    def _load_core_manifest_fallback(self) -> dict:
        """
        Best-effort read of app/config/manifest.json without relying on PluginManager.
        This keeps email/logo generation resilient even if import paths change.
        """
        try:
            # .../app/plugins/website_module/objects.py -> .../app/config/manifest.json
            app_dir = os.path.abspath(os.path.join(
                os.path.dirname(__file__), "..", ".."))
            manifest_path = os.path.join(app_dir, "config", "manifest.json")
            data = _safe_json_load(manifest_path, {})
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _build_logo_url(self, core_manifest: dict) -> str | None:
        """
        Build an absolute logo URL for email rendering.

        Assumptions:
          - core_manifest["logo_path"] matches base.html usage:
              url_for('static', filename=config.logo_path)
          - therefore logo is publicly reachable at:
              <website_url>/static/<logo_path>
        """
        website_url = _ensure_url_has_scheme(
            (core_manifest.get("website_url") or "").strip().rstrip("/"))
        logo_path = (core_manifest.get("logo_path") or "").strip().lstrip("/")

        if not website_url or not logo_path:
            return None

        return f"{website_url}/static/{logo_path}"

    def _render_submission_email_html(self, core_manifest: dict, submission_data: dict, subject: str) -> str:
        company = core_manifest.get("company_name") or "Sparrow ERP"
        website_url = _ensure_url_has_scheme(
            (core_manifest.get("website_url") or "").strip().rstrip("/"))
        logo_url = self._build_logo_url(core_manifest)

        sender_label = self._guess_sender_label(submission_data)
        submitted_at = (submission_data.get("timestamp")
                        or "").strip() or datetime.utcnow().isoformat()

        rows = []
        for k, v in self._safe_items_for_table(submission_data):
            rows.append(f"""
              <tr>
                <td style="padding:10px 12px; border-bottom:1px solid #eef2f7; color:#334155; font-weight:600; width:220px; vertical-align:top;">
                  {k}
                </td>
                <td style="padding:10px 12px; border-bottom:1px solid #eef2f7; color:#0f172a; vertical-align:top; white-space:pre-wrap;">
                  {v}
                </td>
              </tr>
            """)

        raw_json = json.dumps(submission_data, indent=2, ensure_ascii=False)

        logo_html = ""
        if logo_url:
            logo_html = f"""
              <img src="{logo_url}" alt="Logo"
                   style="height:40px; width:auto; border-radius:8px; background:#fff; padding:6px;" />
            """.strip()

        website_badge = ""
        if website_url:
            website_badge = f"""
              <div style="background:#f1f5f9; color:#0f172a; padding:8px 10px; border-radius:10px; font-size:13px;">
                <strong>Website:</strong>
                <a href="{website_url}" style="color:#0b5cff; text-decoration:none;">{website_url}</a>
              </div>
            """.strip()

        footer_site = f" • {website_url}" if website_url else ""

        return f"""
<!doctype html>
<html>
  <body style="margin:0; padding:0; background:#f6f8fb; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
    <div style="max-width:720px; margin:0 auto; padding:24px;">
      <div style="background:#ffffff; border:1px solid #e6eaf0; border-radius:14px; overflow:hidden;">
        <div style="padding:18px 20px; background:linear-gradient(135deg,#0b5cff,#6a5cff); color:#fff;">
          <div style="display:flex; align-items:center; gap:14px;">
            {logo_html}
            <div>
              <div style="font-size:14px; opacity:0.9;">New website submission</div>
              <div style="font-size:20px; font-weight:700; line-height:1.2;">{subject}</div>
            </div>
          </div>
        </div>

        <div style="padding:18px 20px;">
          <div style="display:flex; flex-wrap:wrap; gap:10px; margin-bottom:14px;">
            <div style="background:#f1f5f9; color:#0f172a; padding:8px 10px; border-radius:10px; font-size:13px;">
              <strong>From:</strong> {sender_label}
            </div>
            <div style="background:#f1f5f9; color:#0f172a; padding:8px 10px; border-radius:10px; font-size:13px;">
              <strong>Submitted:</strong> {submitted_at}
            </div>
            {website_badge}
          </div>

          <div style="border:1px solid #eef2f7; border-radius:12px; overflow:hidden;">
            <table cellpadding="0" cellspacing="0" style="width:100%; border-collapse:collapse;">
              <tbody>
                {''.join(rows)}
              </tbody>
            </table>
          </div>

          <div style="margin-top:16px; color:#64748b; font-size:12px; line-height:1.5;">
            This message was generated automatically from your website contact form.
          </div>

          <details style="margin-top:14px;">
            <summary style="cursor:pointer; color:#0b5cff; font-weight:600;">Show raw JSON (for debugging)</summary>
            <pre style="white-space:pre-wrap; background:#0b1220; color:#e2e8f0; padding:12px; border-radius:10px; overflow:auto; margin-top:10px;">{raw_json}</pre>
          </details>
        </div>

        <div style="padding:14px 20px; background:#f8fafc; border-top:1px solid #eef2f7; color:#64748b; font-size:12px;">
          {company}{footer_site}
        </div>
      </div>
    </div>
  </body>
</html>
        """.strip()

    def _is_sales_enabled(self) -> bool:
        """
        Best-effort check for sales plugin enabled.
        This avoids assuming core manifest contains per-plugin enabled flags.
        """
        try:
            app_dir = os.path.abspath(os.path.join(
                os.path.dirname(__file__), "..", ".."))
            sales_manifest = os.path.join(
                app_dir, "plugins", "sales", "manifest.json")
            man = _safe_json_load(sales_manifest, {})
            return bool(isinstance(man, dict) and man.get("enabled", False))
        except Exception:
            return False

    def process_submission(self, submission_data: dict) -> bool:
        self.record_submission(submission_data)

        module_dir = os.path.dirname(os.path.abspath(__file__))
        config_manager = ContactFormConfigManager(module_dir)
        current_config = config_manager.get_configuration() or {}

        form_id_submitted = (submission_data.get(
            "form_id") or "").strip().lower()
        if not form_id_submitted:
            print("No form_id provided. Submission not processed.")
            return False

        # Config keys are normalized on write, but keep this for backwards compatibility
        config_lower = {str(key).lower(): value for key,
                        value in current_config.items()}
        if form_id_submitted not in config_lower:
            print(
                f"Configuration for form '{form_id_submitted}' not defined. Submission not processed.")
            return False

        matched_config = config_lower[form_id_submitted] or {}
        email_recipient = (matched_config.get("recipient") or "").strip()
        email_subject = (matched_config.get("subject") or "").strip()

        if not email_recipient or not email_subject:
            print("Form configuration missing required fields. Submission not processed.")
            return False

        # Load core manifest (prefer PluginManager if available, fallback to direct read)
        core_manifest = {}
        try:
            from ...objects import PluginManager  # core module
            try:
                plugin_manager = PluginManager("plugins")
                core_manifest = plugin_manager.get_core_manifest() or {}
            except Exception:
                core_manifest = self._load_core_manifest_fallback()
        except Exception:
            core_manifest = self._load_core_manifest_fallback()

        # Optional: forward to Sales module if enabled (best-effort)
        if self._is_sales_enabled():
            print("Forwarding submission to Sales module:", submission_data)
            # TODO: Sales module integration
            return True

        # Email fallback (multipart: text + html)
        try:
            from ...objects import EmailManager  # core module
            email_manager = EmailManager()

            text_body = "New contact form submission:\n\n" + \
                json.dumps(submission_data, indent=2, ensure_ascii=False)
            html_body = self._render_submission_email_html(
                core_manifest, submission_data, email_subject)

            email_manager.send_email(email_subject, text_body, [
                                     email_recipient], html_body=html_body)
            print("Email sent to", email_recipient)
            return True

        except Exception as e:
            print("Error sending email:", e)
            return False


# ------------------------
# Page Store (JSON)
# ------------------------
class BuilderPageStore:
    """
    JSON file store for builder pages: data/pages/<page_id>.json
    """

    def __init__(self, data_dir: str):
        self.pages_dir = os.path.join(data_dir, 'pages')
        os.makedirs(self.pages_dir, exist_ok=True)

    def _path(self, page_id: str) -> str:
        return os.path.join(self.pages_dir, f"{page_id}.json")

    def load(self, page_id: str) -> dict | None:
        p = self._path(page_id)
        if not os.path.exists(p):
            return None
        with open(p, 'r', encoding='utf-8') as f:
            return json.load(f)

    def save(self, page_json: dict) -> None:
        if 'id' not in page_json:
            raise ValueError("page_json missing id")
        page_json.setdefault('updated_at', datetime.utcnow().isoformat())
        with open(self._path(page_json['id']), 'w', encoding='utf-8') as f:
            json.dump(page_json, f, indent=2, ensure_ascii=False)

    def create(self, title: str, route: str) -> dict:
        page_id = str(uuid.uuid4())[:8]
        page = {
            'id': page_id,
            'title': title,
            'route': route,
            'seo': {'title': title, 'description': ''},
            'vars': {},
            'blocks': [],
            'draft': True,
            'version': 1,
            'updated_at': datetime.utcnow().isoformat(),
        }
        self.save(page)
        return page


# ------------------------
# Blocks Registry (with layout primitives)
# ------------------------
class BlocksRegistry:
    """
    Registry: block metadata, defaults, schema, templates, and layout primitives.
    safe_registry() strips server-only fields.
    """

    def __init__(self):
        self._blocks = {
            'hero': {
                'label': 'Hero',
                'icon': 'bi-lightning',
                'defaults': {'headline': 'Headline', 'sub': 'Subheadline', 'cta_text': 'Get Started', 'bg': 'light'},
                'schema': [
                    {'name': 'headline', 'type': 'text',
                        'label': 'Headline', 'required': True},
                    {'name': 'sub', 'type': 'text', 'label': 'Subheadline'},
                    {'name': 'cta_text', 'type': 'text', 'label': 'CTA Text'},
                    {'name': 'bg', 'type': 'select', 'label': 'Background',
                        'options': ['light', 'dark', 'image']},
                ],
                'template': 'blocks/hero.html',
            },
            'text': {
                'label': 'Text',
                'icon': 'bi-fonts',
                'defaults': {'html': '<p>Write something…</p>'},
                'schema': [{'name': 'html', 'type': 'richtext', 'label': 'Content', 'required': True}],
                'template': 'blocks/text.html',
            },
            'image': {
                'label': 'Image',
                'icon': 'bi-image',
                'defaults': {'src': '', 'alt': '', 'rounded': True},
                'schema': [
                    {'name': 'src', 'type': 'text',
                        'label': 'Image URL', 'required': True},
                    {'name': 'alt', 'type': 'text', 'label': 'Alt text'},
                    {'name': 'rounded', 'type': 'switch',
                        'label': 'Rounded corners'},
                ],
                'template': 'blocks/image.html',
            },
            'section': {
                'label': 'Section',
                'icon': 'bi-layout-text-sidebar',
                'defaults': {'blocks': [], 'bg': 'light'},
                'schema': [
                    {'name': 'bg', 'type': 'select', 'label': 'Background',
                        'options': ['light', 'dark', 'image']},
                    {'name': 'blocks', 'type': 'blocks', 'label': 'Nested blocks'},
                ],
                'template': 'blocks/section.html',
            },
            'columns': {
                'label': 'Columns',
                'icon': 'bi-columns',
                'defaults': {'columns': [[], []]},
                'schema': [{'name': 'columns', 'type': 'columns', 'label': 'Column content', 'required': True}],
                'template': 'blocks/columns.html',
            },
            'button': {
                'label': 'Button',
                'icon': 'bi-hand-index',
                'defaults': {'text': 'Click Me', 'href': '#', 'style': 'primary'},
                'schema': [
                    {'name': 'text', 'type': 'text',
                        'label': 'Button text', 'required': True},
                    {'name': 'href', 'type': 'text',
                        'label': 'Link', 'required': True},
                    {'name': 'style', 'type': 'select', 'label': 'Style',
                     'options': ['primary', 'secondary', 'success', 'info', 'warning', 'danger', 'link']},
                ],
                'template': 'blocks/button.html',
            },
            'spacer': {
                'label': 'Spacer',
                'icon': 'bi-arrows-expand',
                'defaults': {'height': 20},
                'schema': [{'name': 'height', 'type': 'number', 'label': 'Height (px)', 'required': True}],
                'template': 'blocks/spacer.html',
            },
        }

    def exists(self, btype: str) -> bool:
        return btype in self._blocks

    def defaults(self, btype: str) -> dict:
        return self._blocks[btype]['defaults']

    def template(self, btype: str) -> str:
        return self._blocks[btype]['template']

    def schema(self, btype: str) -> list:
        return self._blocks[btype]['schema']

    def normalize(self, btype: str, props: dict | None) -> dict:
        base = dict(self._blocks[btype]['defaults'])
        props = dict(props or {})

        for k in list(props.keys()):
            if k not in base:
                props.pop(k, None)

        if btype == 'section':
            base['blocks'] = []
            for blk in props.get('blocks', []):
                if isinstance(blk, dict) and self.exists(blk.get('type')):
                    child_props = self.normalize(blk['type'], blk.get('props'))
                    base['blocks'].append(
                        {'type': blk['type'], 'props': child_props})
        elif btype == 'columns':
            base['columns'] = []
            for col in props.get('columns', []):
                cleaned_col = []
                for blk in col if isinstance(col, list) else []:
                    if isinstance(blk, dict) and self.exists(blk.get('type')):
                        child_props = self.normalize(
                            blk['type'], blk.get('props'))
                        cleaned_col.append(
                            {'type': blk['type'], 'props': child_props})
                base['columns'].append(cleaned_col or [])

        for k, v in props.items():
            if k not in ('blocks', 'columns'):
                base[k] = v
        return base

    def validate(self, btype: str, props: dict) -> tuple[bool, str | None]:
        for field in self._blocks[btype]['schema']:
            name = field['name']
            ftype = field.get('type')
            req = field.get('required', False)
            val = props.get(name)

            if ftype in ('blocks', 'columns'):
                if req and not isinstance(val, list):
                    return False, f"{field.get('label') or name} must be a list"
            else:
                if req and not str(val or '').strip():
                    return False, f"{field.get('label') or name} is required"
        return True, None

    def safe_registry(self) -> dict:
        safe = {}
        for k, v in self._blocks.items():
            safe[k] = {
                'label': v['label'],
                'icon': v['icon'],
                'defaults': v['defaults'],
                'schema': v['schema'],
            }
        return safe


# ------------------------
# Server Renderer (nested)
# ------------------------
class BuilderRenderer:
    """
    Server-side renderer supporting nested blocks (section, columns).
    Renders using Jinja partials under templates/blocks/.
    """

    def __init__(self):
        self._registry = BlocksRegistry()

    def render_block(self, block: dict) -> str:
        btype = block.get('type')
        props = dict(block.get('props') or {})
        if not self._registry.exists(btype):
            return ''

        tpl = self._registry.template(btype)

        if btype == 'section':
            children = props.get('blocks', [])
            html = [self.render_block(child) for child in children]
            props['blocks_html'] = '\n'.join(html)
        elif btype == 'columns':
            cols_html = []
            for col in props.get('columns', []):
                html = [self.render_block(child) for child in col]
                cols_html.append('\n'.join(html))
            props['columns_html'] = cols_html

        return render_template(tpl, **props)

    def render_page(self, page_json: dict) -> str:
        return '\n'.join(self.render_block(blk) for blk in page_json.get('blocks', []))
