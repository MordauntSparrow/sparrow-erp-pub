# keep import here to avoid Flask requirement at module import time in some contexts
from flask import render_template
import os
import json
import uuid
import html as html_module
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
            'seo': {
                'title': title,
                'description': '',
                'keywords': '',
                'image': '',
                'noindex': False,
                'nofollow': False,
                'omit_from_sitemap': False,
            },
            'vars': {},
            'blocks': [],
            'settings': {'merge_mode': 'augment'},
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
                'icon': 'bi-lightning-charge',
                'category': 'Hero & CTA',
                'description': 'Full-width headline, subtext and primary button',
                'defaults': {
                    'headline': 'Build something remarkable',
                    'sub': 'Professional layouts powered by Material Design — fast, responsive, yours.',
                    'cta_text': 'Get started',
                    'cta_href': '#',
                    'bg': 'gradient',
                    'align': 'center',
                    'image': '',
                },
                'schema': [
                    {'name': 'headline', 'type': 'text',
                        'label': 'Headline', 'required': True},
                    {'name': 'sub', 'type': 'text', 'label': 'Subheadline'},
                    {'name': 'cta_text', 'type': 'text', 'label': 'Button label'},
                    {'name': 'cta_href', 'type': 'text', 'label': 'Button link'},
                    {'name': 'bg', 'type': 'select', 'label': 'Background',
                        'options': ['light', 'dark', 'gradient', 'image']},
                    {'name': 'image', 'type': 'imageurl',
                        'label': 'Background image (when style is Image)', 'required': False},
                    {'name': 'align', 'type': 'select', 'label': 'Alignment',
                        'options': ['left', 'center', 'right']},
                ],
                'template': 'blocks/hero.html',
            },
            'cta_banner': {
                'label': 'CTA banner',
                'icon': 'bi-megaphone',
                'category': 'Hero & CTA',
                'description': 'Bold strip with call to action',
                'defaults': {
                    'title': 'Ready to grow?',
                    'subtitle': 'Join thousands of teams using our platform.',
                    'btn_text': 'Contact us',
                    'btn_href': '#',
                    'tone': 'primary',
                },
                'schema': [
                    {'name': 'title', 'type': 'text', 'label': 'Title', 'required': True},
                    {'name': 'subtitle', 'type': 'text', 'label': 'Subtitle'},
                    {'name': 'btn_text', 'type': 'text', 'label': 'Button text', 'required': True},
                    {'name': 'btn_href', 'type': 'text', 'label': 'Button link', 'required': True},
                    {'name': 'tone', 'type': 'select', 'label': 'Style',
                     'options': ['primary', 'dark', 'light', 'success']},
                ],
                'template': 'blocks/cta_banner.html',
            },
            'heading': {
                'label': 'Heading',
                'icon': 'bi-type-h1',
                'category': 'Content',
                'description': 'Semantic title (H1–H6)',
                'defaults': {'text': 'Section title', 'level': '2', 'align': 'left', 'sub': ''},
                'schema': [
                    {'name': 'text', 'type': 'text', 'label': 'Text', 'required': True},
                    {'name': 'level', 'type': 'select', 'label': 'Level',
                     'options': ['1', '2', '3', '4', '5', '6']},
                    {'name': 'align', 'type': 'select', 'label': 'Align',
                     'options': ['left', 'center', 'right']},
                    {'name': 'sub', 'type': 'text', 'label': 'Eyebrow / kicker (optional)'},
                ],
                'template': 'blocks/heading.html',
            },
            'text': {
                'label': 'Rich text',
                'icon': 'bi-text-paragraph',
                'category': 'Content',
                'description': 'Paragraphs, lists and basic HTML',
                'defaults': {'html': '<p class="lead text-muted mb-0">Tell your story with clear, compelling copy. Edit this block to match your brand voice.</p>'},
                'schema': [{'name': 'html', 'type': 'richtext', 'label': 'Content', 'required': True}],
                'template': 'blocks/text.html',
            },
            'card': {
                'label': 'Card',
                'icon': 'bi-card-heading',
                'category': 'Content',
                'description': 'Image, title, body and optional button',
                'defaults': {
                    'title': 'Card title',
                    'body': '<p class="text-muted mb-0">Supporting description for this feature or service.</p>',
                    'image': '',
                    'btn_text': '',
                    'btn_href': '#',
                    'shadow': 'soft',
                },
                'schema': [
                    {'name': 'title', 'type': 'text', 'label': 'Title', 'required': True},
                    {'name': 'body', 'type': 'richtext', 'label': 'Body'},
                    {'name': 'image', 'type': 'imageurl', 'label': 'Image URL (optional)', 'required': False},
                    {'name': 'btn_text', 'type': 'text', 'label': 'Button text (optional)'},
                    {'name': 'btn_href', 'type': 'text', 'label': 'Button link'},
                    {'name': 'shadow', 'type': 'select', 'label': 'Elevation',
                     'options': ['none', 'soft', 'strong']},
                ],
                'template': 'blocks/card.html',
            },
            'testimonial': {
                'label': 'Testimonial',
                'icon': 'bi-chat-quote',
                'category': 'Content',
                'description': 'Quote with avatar and attribution',
                'defaults': {
                    'quote': 'This product transformed how we work every day.',
                    'author': 'Alex Morgan',
                    'role': 'CEO, Example Co.',
                    'avatar': '',
                    'style': 'card',
                },
                'schema': [
                    {'name': 'quote', 'type': 'richtext', 'label': 'Quote', 'required': True},
                    {'name': 'author', 'type': 'text', 'label': 'Name', 'required': True},
                    {'name': 'role', 'type': 'text', 'label': 'Role / company'},
                    {'name': 'avatar', 'type': 'imageurl', 'label': 'Avatar image URL', 'required': False},
                    {'name': 'style', 'type': 'select', 'label': 'Layout',
                     'options': ['card', 'minimal', 'highlight']},
                ],
                'template': 'blocks/testimonial.html',
            },
            'stats_row': {
                'label': 'Stats row',
                'icon': 'bi-graph-up-arrow',
                'category': 'Content',
                'description': 'Three key metrics in a row',
                'defaults': {
                    'v1': '10k+', 'l1': 'Customers',
                    'v2': '99.9%', 'l2': 'Uptime',
                    'v3': '24/7', 'l3': 'Support',
                },
                'schema': [
                    {'name': 'v1', 'type': 'text', 'label': 'Value 1', 'required': True},
                    {'name': 'l1', 'type': 'text', 'label': 'Label 1', 'required': True},
                    {'name': 'v2', 'type': 'text', 'label': 'Value 2', 'required': True},
                    {'name': 'l2', 'type': 'text', 'label': 'Label 2', 'required': True},
                    {'name': 'v3', 'type': 'text', 'label': 'Value 3', 'required': True},
                    {'name': 'l3', 'type': 'text', 'label': 'Label 3', 'required': True},
                ],
                'template': 'blocks/stats_row.html',
            },
            'alert_block': {
                'label': 'Alert',
                'icon': 'bi-info-circle',
                'category': 'Content',
                'description': 'Notice, success or warning callout',
                'defaults': {'message': '<strong>Heads up!</strong> Use alerts for short, important messages.', 'variant': 'info', 'dismissible': False},
                'schema': [
                    {'name': 'message', 'type': 'richtext', 'label': 'Message', 'required': True},
                    {'name': 'variant', 'type': 'select', 'label': 'Style',
                     'options': ['primary', 'secondary', 'success', 'danger', 'warning', 'info', 'light', 'dark']},
                    {'name': 'dismissible', 'type': 'switch', 'label': 'Dismissible'},
                ],
                'template': 'blocks/alert_block.html',
            },
            'divider': {
                'label': 'Divider',
                'icon': 'bi-dash-lg',
                'category': 'Layout',
                'description': 'Visual separation between sections',
                'defaults': {'style': 'line', 'spacing': '3'},
                'schema': [
                    {'name': 'style', 'type': 'select', 'label': 'Style',
                     'options': ['line', 'fade', 'dots', 'spacer']},
                    {'name': 'spacing', 'type': 'select', 'label': 'Spacing',
                     'options': ['1', '2', '3', '4', '5']},
                ],
                'template': 'blocks/divider.html',
            },
            'image': {
                'label': 'Image',
                'icon': 'bi-image',
                'category': 'Media',
                'description': 'Responsive image with optional caption',
                'defaults': {
                    'src': '', 'alt': '', 'rounded': True, 'caption': '', 'shadow': True,
                    'priority_lcp': False,
                },
                'schema': [
                    {'name': 'src', 'type': 'imageurl',
                        'label': 'Image URL', 'required': True},
                    {'name': 'alt', 'type': 'text', 'label': 'Alt text', 'required': True},
                    {'name': 'caption', 'type': 'text', 'label': 'Caption (optional)'},
                    {'name': 'rounded', 'type': 'switch',
                        'label': 'Rounded corners'},
                    {'name': 'shadow', 'type': 'switch', 'label': 'Drop shadow'},
                    {'name': 'priority_lcp', 'type': 'switch',
                        'label': 'Load with high priority (above-the-fold / LCP)'},
                ],
                'template': 'blocks/image.html',
            },
            'video': {
                'label': 'Video',
                'icon': 'bi-play-btn',
                'category': 'Media',
                'description': 'YouTube or Vimeo embed',
                'defaults': {'url': 'https://www.youtube.com/watch?v=dQw4w9WgXcQ', 'ratio': '16x9', 'rounded': True},
                'schema': [
                    {'name': 'url', 'type': 'text', 'label': 'Video URL', 'required': True},
                    {'name': 'ratio', 'type': 'select', 'label': 'Aspect ratio',
                     'options': ['16x9', '4x3', '1x1']},
                    {'name': 'rounded', 'type': 'switch', 'label': 'Rounded frame'},
                ],
                'template': 'blocks/video.html',
            },
            'accordion_faq': {
                'label': 'FAQ accordion',
                'icon': 'bi-question-circle',
                'category': 'Content',
                'description': 'Expandable questions & answers (up to 5)',
                'defaults': {
                    'section_title': 'Frequently asked questions',
                    'section_sub': '',
                    'q1': 'How do I get started?',
                    'a1': '<p>You can edit this text in the builder. Add more Q&amp;A pairs below.</p>',
                    'q2': 'Is my data secure?',
                    'a2': '<p>Describe your security and compliance approach here.</p>',
                    'q3': '', 'a3': '',
                    'q4': '', 'a4': '',
                    'q5': '', 'a5': '',
                },
                'schema': [
                    {'name': 'section_title', 'type': 'text', 'label': 'Section title', 'required': True},
                    {'name': 'section_sub', 'type': 'text', 'label': 'Subtitle (optional)'},
                    {'name': 'q1', 'type': 'text', 'label': 'Question 1', 'required': True},
                    {'name': 'a1', 'type': 'richtext', 'label': 'Answer 1', 'required': True},
                    {'name': 'q2', 'type': 'text', 'label': 'Question 2'},
                    {'name': 'a2', 'type': 'richtext', 'label': 'Answer 2'},
                    {'name': 'q3', 'type': 'text', 'label': 'Question 3'},
                    {'name': 'a3', 'type': 'richtext', 'label': 'Answer 3'},
                    {'name': 'q4', 'type': 'text', 'label': 'Question 4'},
                    {'name': 'a4', 'type': 'richtext', 'label': 'Answer 4'},
                    {'name': 'q5', 'type': 'text', 'label': 'Question 5'},
                    {'name': 'a5', 'type': 'richtext', 'label': 'Answer 5'},
                ],
                'template': 'blocks/accordion_faq.html',
            },
            'feature_icons': {
                'label': 'Icon features',
                'icon': 'bi-stars',
                'category': 'Content',
                'description': 'Three selling points with Bootstrap icons',
                'defaults': {
                    'title': 'Everything you need',
                    'subtitle': 'Ship faster with a solid foundation.',
                    'i1': 'bi-lightning-charge', 't1': 'Fast', 'd1': 'Performance tuned for real users.',
                    'i2': 'bi-shield-check', 't2': 'Secure', 'd2': 'Security-minded defaults.',
                    'i3': 'bi-puzzle', 't3': 'Flexible', 'd3': 'Extend with modules and custom code.',
                },
                'schema': [
                    {'name': 'title', 'type': 'text', 'label': 'Heading', 'required': True},
                    {'name': 'subtitle', 'type': 'text', 'label': 'Subtitle'},
                    {'name': 'i1', 'type': 'text', 'label': 'Icon 1 (e.g. bi-lightning-charge)'},
                    {'name': 't1', 'type': 'text', 'label': 'Title 1', 'required': True},
                    {'name': 'd1', 'type': 'text', 'label': 'Description 1', 'required': True},
                    {'name': 'i2', 'type': 'text', 'label': 'Icon 2'},
                    {'name': 't2', 'type': 'text', 'label': 'Title 2', 'required': True},
                    {'name': 'd2', 'type': 'text', 'label': 'Description 2', 'required': True},
                    {'name': 'i3', 'type': 'text', 'label': 'Icon 3'},
                    {'name': 't3', 'type': 'text', 'label': 'Title 3', 'required': True},
                    {'name': 'd3', 'type': 'text', 'label': 'Description 3', 'required': True},
                ],
                'template': 'blocks/feature_icons.html',
            },
            'checklist': {
                'label': 'Checklist',
                'icon': 'bi-check2-square',
                'category': 'Content',
                'description': 'Bulleted list with check icons (one line per item)',
                'defaults': {
                    'title': "What's included",
                    'items': 'Free onboarding\nEmail support\nRegular updates\nUK-friendly hosting options',
                },
                'schema': [
                    {'name': 'title', 'type': 'text', 'label': 'Heading', 'required': True},
                    {'name': 'items', 'type': 'richtext', 'label': 'Items (one per line)', 'required': True},
                ],
                'template': 'blocks/checklist.html',
            },
            'logo_cloud': {
                'label': 'Logo cloud',
                'icon': 'bi-building',
                'category': 'Content',
                'description': '“Trusted by” row — image URLs, one per line',
                'defaults': {
                    'heading': 'Trusted by teams like yours',
                    'urls': '',
                    'grayscale': True,
                },
                'schema': [
                    {'name': 'heading', 'type': 'text', 'label': 'Heading'},
                    {'name': 'urls', 'type': 'richtext', 'label': 'Image URLs (one per line)', 'required': False},
                    {'name': 'grayscale', 'type': 'switch', 'label': 'Grayscale logos'},
                ],
                'template': 'blocks/logo_cloud.html',
            },
            'map_embed': {
                'label': 'Map',
                'icon': 'bi-geo-alt',
                'category': 'Media',
                'description': 'Google Maps embed URL (iframe src)',
                'defaults': {
                    'embed_src': '',
                    'height': 360,
                    'rounded': True,
                    'caption': '',
                },
                'schema': [
                    {'name': 'embed_src', 'type': 'text', 'label': 'Embed URL (iframe src)', 'required': False},
                    {'name': 'height', 'type': 'number', 'label': 'Height (px)', 'required': True},
                    {'name': 'rounded', 'type': 'switch', 'label': 'Rounded corners'},
                    {'name': 'caption', 'type': 'text', 'label': 'Caption (optional)'},
                ],
                'template': 'blocks/map_embed.html',
            },
            'newsletter': {
                'label': 'Newsletter signup',
                'icon': 'bi-envelope-heart',
                'category': 'Hero & CTA',
                'description': 'Email field + button (set form action to your handler)',
                'defaults': {
                    'heading': 'Stay in the loop',
                    'sub': 'Monthly product news — no spam.',
                    'placeholder': 'you@company.com',
                    'button': 'Subscribe',
                    'action': '#',
                    'method': 'post',
                },
                'schema': [
                    {'name': 'heading', 'type': 'text', 'label': 'Heading', 'required': True},
                    {'name': 'sub', 'type': 'text', 'label': 'Subtitle'},
                    {'name': 'placeholder', 'type': 'text', 'label': 'Input placeholder'},
                    {'name': 'button', 'type': 'text', 'label': 'Button label', 'required': True},
                    {'name': 'action', 'type': 'text', 'label': 'Form action URL', 'required': True},
                    {'name': 'method', 'type': 'select', 'label': 'Method',
                     'options': ['post', 'get']},
                ],
                'template': 'blocks/newsletter.html',
            },
            'social_row': {
                'label': 'Social links',
                'icon': 'bi-share',
                'category': 'Content',
                'description': 'Icon row for major networks (fill only what you use)',
                'defaults': {
                    'facebook': '', 'instagram': '', 'linkedin': '', 'x': '', 'youtube': '', 'github': '',
                },
                'schema': [
                    {'name': 'facebook', 'type': 'text', 'label': 'Facebook URL'},
                    {'name': 'instagram', 'type': 'text', 'label': 'Instagram URL'},
                    {'name': 'linkedin', 'type': 'text', 'label': 'LinkedIn URL'},
                    {'name': 'x', 'type': 'text', 'label': 'X (Twitter) URL'},
                    {'name': 'youtube', 'type': 'text', 'label': 'YouTube URL'},
                    {'name': 'github', 'type': 'text', 'label': 'GitHub URL'},
                ],
                'template': 'blocks/social_row.html',
            },
            'team_member': {
                'label': 'Team member',
                'icon': 'bi-person-badge',
                'category': 'Content',
                'description': 'Photo, name, role and short bio',
                'defaults': {
                    'name': 'Alex Morgan',
                    'role': 'Founder & CEO',
                    'bio': '<p>Short bio — background, focus, and what drives the team.</p>',
                    'photo': '',
                    'linkedin': '',
                },
                'schema': [
                    {'name': 'name', 'type': 'text', 'label': 'Name', 'required': True},
                    {'name': 'role', 'type': 'text', 'label': 'Role / title'},
                    {'name': 'bio', 'type': 'richtext', 'label': 'Bio'},
                    {'name': 'photo', 'type': 'imageurl', 'label': 'Photo URL', 'required': False},
                    {'name': 'linkedin', 'type': 'text', 'label': 'LinkedIn URL (optional)'},
                ],
                'template': 'blocks/team_member.html',
            },
            'contact_block': {
                'label': 'Contact info',
                'icon': 'bi-telephone',
                'category': 'Content',
                'description': 'Heading + rich text (address, hours, phones)',
                'defaults': {
                    'title': 'Contact us',
                    'body': '<p><strong>Phone:</strong> 01234 567890<br><strong>Email:</strong> hello@example.com<br><strong>Hours:</strong> Mon–Fri 9–5</p>',
                },
                'schema': [
                    {'name': 'title', 'type': 'text', 'label': 'Heading', 'required': True},
                    {'name': 'body', 'type': 'richtext', 'label': 'Content', 'required': True},
                ],
                'template': 'blocks/contact_block.html',
            },
            'embed_frame': {
                'label': 'Embed (iframe)',
                'icon': 'bi-code-square',
                'category': 'Media',
                'description': 'Trusted https embed only (calendars, Typeform, etc.)',
                'defaults': {
                    'src': '',
                    'title': 'Embedded content',
                    'height': 480,
                    'rounded': True,
                },
                'schema': [
                    {'name': 'src', 'type': 'text', 'label': 'Page URL (https only)', 'required': False},
                    {'name': 'title', 'type': 'text', 'label': 'Title (accessibility)'},
                    {'name': 'height', 'type': 'number', 'label': 'Height (px)', 'required': True},
                    {'name': 'rounded', 'type': 'switch', 'label': 'Rounded frame'},
                ],
                'template': 'blocks/embed_frame.html',
            },
            'pricing_table': {
                'label': 'Pricing table',
                'icon': 'bi-currency-pound',
                'category': 'Hero & CTA',
                'description': 'Three plan cards with price, bullets, and CTA',
                'defaults': {
                    'heading': 'Simple, transparent pricing',
                    'sub': 'Pick the plan that fits. Change any time.',
                    'p1_name': 'Starter', 'p1_price': '£9', 'p1_period': '/ month',
                    'p1_features': 'Up to 3 users\nEmail support\nCore features',
                    'p1_btn': 'Get started', 'p1_href': '#', 'p1_featured': False,
                    'p2_name': 'Pro', 'p2_price': '£29', 'p2_period': '/ month',
                    'p2_features': 'Up to 25 users\nPriority support\nAll features',
                    'p2_btn': 'Start trial', 'p2_href': '#', 'p2_featured': True,
                    'p3_name': 'Enterprise', 'p3_price': 'Let’s talk', 'p3_period': '',
                    'p3_features': 'Unlimited users\nDedicated success\nSLA & security review',
                    'p3_btn': 'Contact sales', 'p3_href': '#', 'p3_featured': False,
                },
                'schema': [
                    {'name': 'heading', 'type': 'text', 'label': 'Section heading', 'required': True},
                    {'name': 'sub', 'type': 'text', 'label': 'Subtitle'},
                    {'name': 'p1_name', 'type': 'text', 'label': 'Plan 1 name', 'required': True},
                    {'name': 'p1_price', 'type': 'text', 'label': 'Plan 1 price', 'required': True},
                    {'name': 'p1_period', 'type': 'text', 'label': 'Plan 1 period (e.g. / month)'},
                    {'name': 'p1_features', 'type': 'richtext', 'label': 'Plan 1 features (one per line)'},
                    {'name': 'p1_btn', 'type': 'text', 'label': 'Plan 1 button'},
                    {'name': 'p1_href', 'type': 'text', 'label': 'Plan 1 link'},
                    {'name': 'p1_featured', 'type': 'switch', 'label': 'Highlight plan 1'},
                    {'name': 'p2_name', 'type': 'text', 'label': 'Plan 2 name', 'required': True},
                    {'name': 'p2_price', 'type': 'text', 'label': 'Plan 2 price', 'required': True},
                    {'name': 'p2_period', 'type': 'text', 'label': 'Plan 2 period'},
                    {'name': 'p2_features', 'type': 'richtext', 'label': 'Plan 2 features (one per line)'},
                    {'name': 'p2_btn', 'type': 'text', 'label': 'Plan 2 button'},
                    {'name': 'p2_href', 'type': 'text', 'label': 'Plan 2 link'},
                    {'name': 'p2_featured', 'type': 'switch', 'label': 'Highlight plan 2'},
                    {'name': 'p3_name', 'type': 'text', 'label': 'Plan 3 name', 'required': True},
                    {'name': 'p3_price', 'type': 'text', 'label': 'Plan 3 price', 'required': True},
                    {'name': 'p3_period', 'type': 'text', 'label': 'Plan 3 period'},
                    {'name': 'p3_features', 'type': 'richtext', 'label': 'Plan 3 features (one per line)'},
                    {'name': 'p3_btn', 'type': 'text', 'label': 'Plan 3 button'},
                    {'name': 'p3_href', 'type': 'text', 'label': 'Plan 3 link'},
                    {'name': 'p3_featured', 'type': 'switch', 'label': 'Highlight plan 3'},
                ],
                'template': 'blocks/pricing_table.html',
            },
            'content_tabs': {
                'label': 'Tabs',
                'icon': 'bi-folder2-open',
                'category': 'Content',
                'description': 'Up to 3 tabs with rich HTML panels',
                'defaults': {
                    't1_label': 'Overview',
                    't1_body': '<p>First tab content — product summary, key benefits, or onboarding tips.</p>',
                    't2_label': 'Details',
                    't2_body': '<p>Second tab — specifications, FAQs, or deeper copy.</p>',
                    't3_label': 'Resources',
                    't3_body': '<p>Third tab — downloads, links, or next steps.</p>',
                },
                'schema': [
                    {'name': 't1_label', 'type': 'text', 'label': 'Tab 1 label', 'required': True},
                    {'name': 't1_body', 'type': 'richtext', 'label': 'Tab 1 content', 'required': True},
                    {'name': 't2_label', 'type': 'text', 'label': 'Tab 2 label'},
                    {'name': 't2_body', 'type': 'richtext', 'label': 'Tab 2 content'},
                    {'name': 't3_label', 'type': 'text', 'label': 'Tab 3 label'},
                    {'name': 't3_body', 'type': 'richtext', 'label': 'Tab 3 content'},
                ],
                'template': 'blocks/content_tabs.html',
            },
            'timeline': {
                'label': 'Timeline',
                'icon': 'bi-clock-history',
                'category': 'Content',
                'description': 'Vertical timeline (up to 4 milestones)',
                'defaults': {
                    'heading': 'Our journey',
                    'sub': 'From idea to launch.',
                    'e1_date': '2022', 'e1_title': 'Founded', 'e1_body': '<p>Started with a clear mission.</p>',
                    'e2_date': '2023', 'e2_title': 'First customers', 'e2_body': '<p>Shipped the MVP and learned fast.</p>',
                    'e3_date': '2024', 'e3_title': 'Scale', 'e3_body': '<p>Expanded the team and product surface.</p>',
                    'e4_date': '', 'e4_title': '', 'e4_body': '',
                },
                'schema': [
                    {'name': 'heading', 'type': 'text', 'label': 'Heading', 'required': True},
                    {'name': 'sub', 'type': 'text', 'label': 'Subtitle'},
                    {'name': 'e1_date', 'type': 'text', 'label': 'Event 1 date / label'},
                    {'name': 'e1_title', 'type': 'text', 'label': 'Event 1 title', 'required': True},
                    {'name': 'e1_body', 'type': 'richtext', 'label': 'Event 1 description'},
                    {'name': 'e2_date', 'type': 'text', 'label': 'Event 2 date / label'},
                    {'name': 'e2_title', 'type': 'text', 'label': 'Event 2 title'},
                    {'name': 'e2_body', 'type': 'richtext', 'label': 'Event 2 description'},
                    {'name': 'e3_date', 'type': 'text', 'label': 'Event 3 date / label'},
                    {'name': 'e3_title', 'type': 'text', 'label': 'Event 3 title'},
                    {'name': 'e3_body', 'type': 'richtext', 'label': 'Event 3 description'},
                    {'name': 'e4_date', 'type': 'text', 'label': 'Event 4 date / label'},
                    {'name': 'e4_title', 'type': 'text', 'label': 'Event 4 title'},
                    {'name': 'e4_body', 'type': 'richtext', 'label': 'Event 4 description'},
                ],
                'template': 'blocks/timeline.html',
            },
            'section': {
                'label': 'Section',
                'icon': 'bi-layout-sidebar-inset',
                'category': 'Layout',
                'description': 'Groups nested blocks with background',
                'defaults': {'blocks': [], 'bg': 'light', 'padding': '5'},
                'schema': [
                    {'name': 'bg', 'type': 'select', 'label': 'Background',
                        'options': ['light', 'dark', 'white', 'muted']},
                    {'name': 'padding', 'type': 'select', 'label': 'Vertical padding',
                     'options': ['3', '4', '5', '6']},
                    {'name': 'blocks', 'type': 'blocks', 'label': 'Nested blocks'},
                ],
                'template': 'blocks/section.html',
            },
            'columns': {
                'label': 'Columns',
                'icon': 'bi-columns-gap',
                'category': 'Layout',
                'description': 'Multi-column row for nested blocks',
                'defaults': {'columns': [[], []]},
                'schema': [{'name': 'columns', 'type': 'columns', 'label': 'Column content', 'required': True}],
                'template': 'blocks/columns.html',
            },
            'button': {
                'label': 'Button',
                'icon': 'bi-hand-index-thumb',
                'category': 'Actions',
                'description': 'Standalone link styled as a button',
                'defaults': {'text': 'Learn more', 'href': '#', 'style': 'primary', 'size': 'md', 'outline': False},
                'schema': [
                    {'name': 'text', 'type': 'text',
                        'label': 'Button text', 'required': True},
                    {'name': 'href', 'type': 'text',
                        'label': 'Link', 'required': True},
                    {'name': 'style', 'type': 'select', 'label': 'Colour',
                     'options': ['primary', 'secondary', 'success', 'info', 'warning', 'danger', 'dark', 'light', 'link']},
                    {'name': 'size', 'type': 'select', 'label': 'Size',
                     'options': ['sm', 'md', 'lg']},
                    {'name': 'outline', 'type': 'switch', 'label': 'Outline style'},
                ],
                'template': 'blocks/button.html',
            },
            'spacer': {
                'label': 'Spacer',
                'icon': 'bi-arrows-expand',
                'category': 'Layout',
                'description': 'Vertical whitespace',
                'defaults': {'height': 32},
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
            elif ftype == 'number':
                if req:
                    try:
                        float(val)
                    except (TypeError, ValueError):
                        return False, f"{field.get('label') or name} must be a number"
            elif ftype == 'imageurl':
                if req and not str(val or '').strip():
                    return False, f"{field.get('label') or name} is required"
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
                'category': v.get('category', 'Content'),
                'description': v.get('description', ''),
            }
        return safe

    def grouped_palette(self):
        """Ordered categories for the builder sidebar (Material-style palette)."""
        flat = self.safe_registry()
        order = [
            'Layout',
            'Hero & CTA',
            'Content',
            'Media',
            'Actions',
            'Other',
        ]
        buckets: dict[str, list[tuple[str, dict]]] = {c: [] for c in order}
        for k, v in flat.items():
            c = (v.get('category') or 'Content').strip()
            if c not in buckets:
                c = 'Other'
            buckets[c].append((k, v))
        return [(c, buckets[c]) for c in order if buckets[c]]


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

    def render_block(self, block: dict, path: list) -> str:
        btype = block.get('type')
        props = dict(block.get('props') or {})
        if not self._registry.exists(btype):
            return ''

        tpl = self._registry.template(btype)
        # Stable unique suffix for this block instance (accordions, a11y ids)
        self._block_seq = getattr(self, '_block_seq', 0) + 1
        props['_block_uid'] = self._block_seq
        props['canvas_mode'] = bool(getattr(self, '_canvas_mode', False))

        if btype == 'section':
            children = props.get('blocks', [])
            props['_wb_canvas_parent_path'] = json.dumps(
                path, separators=(',', ':'))
            parts = [self.render_block(child, path + [j]) for j, child in enumerate(children)]
            props['blocks_html'] = '\n'.join(parts)
        elif btype == 'columns':
            col_list = props.get('columns', [])
            props['_wb_col_parent_paths_json'] = [
                json.dumps(path + ['c', ci], separators=(',', ':'))
                for ci in range(len(col_list))
            ]
            cols_html = []
            for c, col in enumerate(col_list):
                cell_parts = [
                    self.render_block(child, path + ['c', c, b])
                    for b, child in enumerate(col)
                ]
                cols_html.append('\n'.join(cell_parts))
            props['columns_html'] = cols_html

        inner = render_template(tpl, **props)
        if getattr(self, '_canvas_mode', False):
            path_json = json.dumps(path, separators=(',', ':'))
            safe_path = html_module.escape(path_json)
            safe_type = html_module.escape(str(btype or ''))
            return (
                '<div class="wb-canvas-block" tabindex="-1" '
                f'data-wb-path="{safe_path}" data-wb-type="{safe_type}">'
                '<span class="wb-canvas-drag-handle" draggable="false" '
                'title="Drag to reorder">⠿</span>'
                f'{inner}</div>'
            )
        return inner

    def render_page(self, page_json: dict, canvas_mode: bool = False) -> str:
        """
        canvas_mode: wrap each block for builder preview (click-to-select via postMessage).
        Live/published pages must use canvas_mode=False (default).
        """
        self._canvas_mode = canvas_mode
        self._block_seq = 0
        blocks = page_json.get('blocks') or []
        return '\n'.join(self.render_block(blk, [i]) for i, blk in enumerate(blocks))
