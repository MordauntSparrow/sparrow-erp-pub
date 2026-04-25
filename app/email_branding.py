"""
Branded multipart email (HTML + plain) using Core manifest company name and logo.

Uses ``resolve_public_base_url`` so logos resolve in mail clients. Many clients block images;
plain text is always included.
"""
from __future__ import annotations

import html
import logging
import os
import re
from typing import Optional

from app.branding_utils import merge_site_settings_defaults
from app.public_base import resolve_public_base_url
from app.static_upload_paths import normalize_manifest_static_path

logger = logging.getLogger(__name__)


def _plugins_folder() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "plugins")


def get_transactional_email_branding() -> dict:
    """``company_name``, ``logo_abs_url`` (may be empty), ``accent`` hex."""
    company_name = "Sparrow ERP"
    logo_abs_url = ""
    accent = "#0d9488"
    try:
        from app.objects import PluginManager

        pm = PluginManager(_plugins_folder())
        core = pm.get_core_manifest() or {}
        site = merge_site_settings_defaults(core.get("site_settings"))
        company_name = (site.get("company_name") or company_name).strip() or company_name
        ts = core.get("theme_settings") or {}
        if isinstance(ts, dict):
            c = (ts.get("primary_color") or ts.get("primary") or "").strip()
            if re.match(r"^#[0-9A-Fa-f]{3,8}$", c):
                accent = c[:7]
        rel = normalize_manifest_static_path(site.get("logo_path"))
        if rel:
            base = resolve_public_base_url(
                extra_env_keys=(
                    "SPARROW_PUBLIC_BASE_URL",
                    "PUBLIC_BASE_URL",
                    "HR_PUBLIC_BASE_URL",
                    "TIME_BILLING_PUBLIC_BASE_URL",
                )
            )
            if base:
                logo_abs_url = f"{base.rstrip('/')}/static/{rel.lstrip('/')}"
    except Exception as e:
        logger.debug("email branding load skipped: %s", e)
    return {
        "company_name": company_name,
        "logo_abs_url": logo_abs_url,
        "accent": accent,
    }


def plain_text_to_email_html(text: str) -> str:
    """Escape and preserve paragraphs (blank line = new paragraph)."""
    s = (text or "").strip("\n")
    if not s:
        return "<p>&nbsp;</p>"
    chunks = [p.strip() for p in s.split("\n\n") if p.strip()]
    parts = []
    for ch in chunks:
        lines = [html.escape(line) for line in ch.split("\n")]
        parts.append("<p style=\"margin:0 0 14px 0;line-height:1.55;font-size:15px;color:#334155;\">" + "<br>".join(lines) + "</p>")
    return "".join(parts)


def build_branded_html_email(plain_body: str, *, preheader: Optional[str] = None) -> str:
    b = get_transactional_email_branding()
    name = html.escape(b["company_name"])
    accent = b["accent"]
    inner = plain_text_to_email_html(plain_body)
    pre = html.escape((preheader or "")[:140])
    logo_block = ""
    if b.get("logo_abs_url"):
        lu = html.escape(b["logo_abs_url"], quote=True)
        logo_block = (
            f'<div style="margin:0 auto 14px auto;text-align:center;">'
            f'<img src="{lu}" alt="" width="200" style="max-width:200px;height:auto;display:inline-block;border:0;outline:none;text-decoration:none;" />'
            f"</div>"
        )
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name}</title></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Segoe UI,Roboto,Helvetica,Arial,sans-serif;">
  <span style="display:none;font-size:1px;color:#f1f5f9;line-height:1px;max-height:0;max-width:0;opacity:0;overflow:hidden;">{pre}</span>
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f1f5f9;padding:24px 12px;">
    <tr><td align="center">
      <table role="presentation" width="600" cellspacing="0" cellpadding="0" style="max-width:600px;width:100%;background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(15,23,42,0.08);">
        <tr><td style="background:linear-gradient(135deg,{accent} 0%,#0f766e 100%);padding:22px 28px;text-align:center;">
          {logo_block}
          <div style="font-size:20px;font-weight:700;color:#ffffff;letter-spacing:-0.02em;">{name}</div>
          <div style="font-size:12px;color:rgba(255,255,255,0.88);margin-top:6px;">Notification</div>
        </td></tr>
        <tr><td style="padding:28px 28px 8px 28px;">{inner}</td></tr>
        <tr><td style="padding:16px 28px 28px 28px;border-top:1px solid #e2e8f0;">
          <p style="margin:0;font-size:12px;color:#94a3b8;line-height:1.45;">This message was sent by {name}. Please do not reply if this inbox is not monitored.</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


def send_branded_email(em, subject: str, plain_body: str, recipients: list, *, preheader: Optional[str] = None) -> None:
    """``em`` is EmailManager. Sends multipart alternative."""
    html_body = build_branded_html_email(plain_body, preheader=preheader or subject)
    em.send_email(
        subject=subject,
        body=plain_body,
        recipients=recipients,
        html_body=html_body,
    )
