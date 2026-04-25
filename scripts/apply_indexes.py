#!/usr/bin/env python3
"""Apply suggested indexes if missing.

Requires `pymysql` installed and DB credentials via env vars:
  DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME

This script is safe to run repeatedly; it checks INFORMATION_SCHEMA for existing indexes.
"""
import os
import sys
import pymysql


SUGGESTED_INDEXES = [
    # (table, index_name, column_list)
    ('mdt_jobs', 'idx_mdt_jobs_status_created_at', 'status, created_at'),
    ('mdt_jobs', 'idx_mdt_jobs_cad', 'cad'),
    ('mdts_signed_on', 'idx_mdts_signed_on_callsign', 'callSign'),
    ('mdts_signed_on', 'idx_mdts_signed_on_status', 'status'),
    ('mdt_locations', 'idx_mdt_locations_callSign', 'callSign'),
    ('messages', 'idx_messages_recipient', 'recipient'),
    ('response_triage', 'idx_response_triage_created_at', 'created_at'),
]


def get_conn():
    return pymysql.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=int(os.environ.get('DB_PORT', 3306)),
        user=os.environ.get('DB_USER', 'root'),
        password=os.environ.get('DB_PASS', ''),
        database=os.environ.get('DB_NAME', 'sparrow'),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )


def index_exists(conn, table, index_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND INDEX_NAME = %s
        """, (conn.db.decode() if isinstance(conn.db, bytes) else conn.db, table, index_name))
        row = cur.fetchone()
        return row and row.get('cnt', 0) > 0


def apply_index(conn, table, index_name, cols):
    with conn.cursor() as cur:
        sql = f"CREATE INDEX {index_name} ON {table} ({cols})"
        print('Executing:', sql)
        cur.execute(sql)


def main():
    try:
        conn = get_conn()
    except Exception as e:
        print('DB connection failed:', e)
        sys.exit(2)

    # pymysql Connection object does not expose db name as attribute in all versions
    # So fetch DB_NAME env var for checks
    db_name = os.environ.get('DB_NAME', 'sparrow')

    for table, idx, cols in SUGGESTED_INDEXES:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND INDEX_NAME = %s
                """, (db_name, table, idx))
                exists = cur.fetchone()['cnt'] > 0
            if exists:
                print(f'Index {idx} on {table} already exists')
            else:
                apply_index(conn, table, idx, cols)
        except Exception as e:
            print(f'Error checking/applying index {idx} on {table}:', e)

    conn.close()


if __name__ == '__main__':
    main()
