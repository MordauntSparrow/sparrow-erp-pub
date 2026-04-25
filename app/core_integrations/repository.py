"""Persistence for core integration connections, settings, and audit events."""
from __future__ import annotations

from typing import Any, Optional

from app.objects import get_db_connection

PROVIDERS = frozenset(
    {"xero", "freeagent", "quickbooks", "sage", "freshbooks"}
)
DEFAULT_TRIGGERS = ("crm_quote_accepted", "manual_only")
ALLOWED_DEFAULT_PROVIDERS = frozenset(
    {"none", "xero", "freeagent", "quickbooks", "sage", "freshbooks"}
)


def schema_ready() -> bool:
    conn = get_db_connection()
    try:
        return (
            _table_exists(conn, "core_integration_connections")
            and _table_exists(conn, "core_integration_settings")
            and _table_exists(conn, "core_integration_oauth_clients")
            and _table_exists(conn, "core_integration_events")
        )
    finally:
        conn.close()


def _table_exists(conn, name: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES LIKE %s", (name,))
        return bool(cur.fetchone())
    finally:
        cur.close()


def load_connection(provider: str) -> Optional[dict[str, Any]]:
    if provider not in PROVIDERS:
        return None
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if not _table_exists(conn, "core_integration_connections"):
            return None
        cur.execute(
            """
            SELECT id, provider, status, encrypted_tokens, provider_org_id, provider_org_label,
                   connected_by_user_id, connected_at, updated_at, last_error, last_api_success_at,
                   refresh_token_expires_at
            FROM core_integration_connections WHERE provider = %s
            """,
            (provider,),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def upsert_connection_row(
    *,
    provider: str,
    status: str,
    encrypted_tokens: str | None,
    provider_org_id: str | None,
    provider_org_label: str | None,
    connected_by_user_id: str | None,
    last_error: str | None = None,
    last_api_success_at: Any = None,
    refresh_token_expires_at: Any = None,
) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO core_integration_connections
              (provider, status, encrypted_tokens, provider_org_id, provider_org_label,
               connected_by_user_id, connected_at, last_error, last_api_success_at, refresh_token_expires_at)
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              status = VALUES(status),
              encrypted_tokens = VALUES(encrypted_tokens),
              provider_org_id = VALUES(provider_org_id),
              provider_org_label = VALUES(provider_org_label),
              connected_by_user_id = VALUES(connected_by_user_id),
              last_error = VALUES(last_error),
              last_api_success_at = VALUES(last_api_success_at),
              refresh_token_expires_at = VALUES(refresh_token_expires_at),
              updated_at = CURRENT_TIMESTAMP
            """,
            (
                provider,
                status,
                encrypted_tokens,
                provider_org_id,
                provider_org_label,
                connected_by_user_id,
                last_error,
                last_api_success_at,
                refresh_token_expires_at,
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def clear_connection(provider: str) -> None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE core_integration_connections SET
              status = 'disconnected',
              encrypted_tokens = NULL,
              provider_org_id = NULL,
              provider_org_label = NULL,
              last_error = NULL,
              last_api_success_at = NULL,
              refresh_token_expires_at = NULL,
              updated_at = CURRENT_TIMESTAMP
            WHERE provider = %s
            """,
            (provider,),
        )
        if cur.rowcount == 0:
            cur.execute(
                """
                INSERT IGNORE INTO core_integration_connections
                  (provider, status, encrypted_tokens, provider_org_id, provider_org_label,
                   connected_by_user_id, connected_at, last_error, last_api_success_at, refresh_token_expires_at)
                VALUES (%s, 'disconnected', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """,
                (provider,),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def load_settings() -> dict[str, Any]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if not _table_exists(conn, "core_integration_settings"):
            return {
                "default_provider": "none",
                "auto_draft_invoice": False,
                "auto_draft_trigger": "crm_quote_accepted",
            }
        cur.execute("SELECT default_provider, auto_draft_invoice, auto_draft_trigger FROM core_integration_settings WHERE id = 1")
        row = cur.fetchone()
        if not row:
            return {
                "default_provider": "none",
                "auto_draft_invoice": False,
                "auto_draft_trigger": "crm_quote_accepted",
            }
        row["auto_draft_invoice"] = bool(row.get("auto_draft_invoice"))
        return row
    finally:
        cur.close()
        conn.close()


def save_settings(
    *,
    default_provider: str,
    auto_draft_invoice: bool,
    auto_draft_trigger: str,
) -> None:
    if default_provider not in ALLOWED_DEFAULT_PROVIDERS:
        default_provider = "none"
    if auto_draft_trigger not in DEFAULT_TRIGGERS:
        auto_draft_trigger = "crm_quote_accepted"
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE core_integration_settings SET
              default_provider = %s,
              auto_draft_invoice = %s,
              auto_draft_trigger = %s
            WHERE id = 1
            """,
            (default_provider, 1 if auto_draft_invoice else 0, auto_draft_trigger),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def load_oauth_client_display(provider: str) -> dict[str, Any]:
    """Stored OAuth app id + whether a secret exists (never returns secret)."""
    out: dict[str, Any] = {"client_id": "", "has_secret": False}
    p = (provider or "").strip().lower()
    if not p or p not in PROVIDERS:
        return out
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if not _table_exists(conn, "core_integration_oauth_clients"):
            return out
        cur.execute(
            "SELECT client_id, encrypted_client_secret FROM core_integration_oauth_clients WHERE provider = %s",
            (p,),
        )
        row = cur.fetchone()
        if not row:
            return out
        out["client_id"] = str(row.get("client_id") or "").strip()
        out["has_secret"] = bool((row.get("encrypted_client_secret") or "").strip())
        return out
    finally:
        cur.close()
        conn.close()


def load_oauth_client_credentials(provider: str) -> tuple[str, str]:
    """
    Decrypted client id + secret from DB (either may be empty).
    Requires Flask app context for Fernet (SECRET_KEY).
    """
    from app.core_integrations import crypto

    p = (provider or "").strip().lower()
    if not p or p not in PROVIDERS:
        return "", ""
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if not _table_exists(conn, "core_integration_oauth_clients"):
            return "", ""
        cur.execute(
            "SELECT client_id, encrypted_client_secret FROM core_integration_oauth_clients WHERE provider = %s",
            (p,),
        )
        row = cur.fetchone()
        if not row:
            return "", ""
        cid = str(row.get("client_id") or "").strip()
        blob = row.get("encrypted_client_secret")
        dec = crypto.decrypt_token_payload(blob) if blob else {}
        sec = str(dec.get("client_secret") or "").strip()
        return cid, sec
    finally:
        cur.close()
        conn.close()


def upsert_oauth_client(
    provider: str, *, client_id: str, client_secret_plain: str | None
) -> None:
    """Persist OAuth app credentials. Empty secret keeps the previous secret when one exists."""
    from app.core_integrations import crypto

    p = (provider or "").strip().lower()
    if p not in PROVIDERS:
        raise ValueError("unsupported provider")
    cid = (client_id or "").strip()
    secret_in = (client_secret_plain or "").strip()
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if not _table_exists(conn, "core_integration_oauth_clients"):
            raise RuntimeError("core_integration_oauth_clients table is missing; run database upgrades.")
        cur.execute(
            "SELECT encrypted_client_secret FROM core_integration_oauth_clients WHERE provider = %s",
            (p,),
        )
        row = cur.fetchone()
        old_blob = (row.get("encrypted_client_secret") if row else None) or ""
        if secret_in:
            enc = crypto.encrypt_token_payload({"client_secret": secret_in})
        elif old_blob.strip():
            enc = str(old_blob).strip()
        else:
            enc = None
        if not cid:
            raise ValueError("Client ID is required.")
        if not enc:
            raise ValueError("Client secret is required (or leave blank only when updating an existing secret).")
        cur.execute(
            """
            INSERT INTO core_integration_oauth_clients (provider, client_id, encrypted_client_secret)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
              client_id = VALUES(client_id),
              encrypted_client_secret = VALUES(encrypted_client_secret)
            """,
            (p, cid, enc),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def append_event(
    *,
    event_type: str,
    provider: str | None,
    message: str | None,
    user_id: str | None,
    ip: str | None = None,
    user_agent: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    import json as _json

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if not _table_exists(conn, "core_integration_events"):
            return
        meta_json = _json.dumps(meta or {}, separators=(",", ":"))
        cur.execute(
            """
            INSERT INTO core_integration_events
              (event_type, provider, message, user_id, ip, user_agent, meta)
            VALUES (%s, %s, %s, %s, %s, %s, CAST(%s AS JSON))
            """,
            (
                event_type[:64],
                (provider or "")[:32] or None,
                (message or "")[:512] or None,
                user_id,
                (ip or "")[:45] or None,
                (user_agent or "")[:512] or None,
                meta_json,
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def recent_events(limit: int = 10) -> list[dict[str, Any]]:
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)
    try:
        if not _table_exists(conn, "core_integration_events"):
            return []
        cur.execute(
            """
            SELECT id, event_type, provider, message, user_id, created_at
            FROM core_integration_events
            ORDER BY id DESC
            LIMIT %s
            """,
            (max(1, min(limit, 50)),),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()
        conn.close()
