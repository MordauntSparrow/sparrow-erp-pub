"""Event plan equipment links (inventory) and diagram images (templates + uploads)."""
from __future__ import annotations

import os
import uuid
from typing import Any

from app.objects import get_db_connection

from .crm_static_paths import (
    crm_diagram_templates_relative_subpath,
    crm_diagram_templates_write_dir,
    crm_event_plan_diagrams_relative_subpath,
    crm_event_plan_diagrams_write_dir,
    crm_static_dir_for_app,
)

_MAP_EXT = (".png", ".jpg", ".jpeg", ".webp", ".gif")


def _image_ext(filename: str) -> str | None:
    lower = (filename or "").lower()
    for ext in _MAP_EXT:
        if lower.endswith(ext):
            return ext
    return None


def list_plan_equipment_safe(conn, plan_id: int) -> list[dict[str, Any]]:
    try:
        return list_plan_equipment_joined(conn, plan_id)
    except Exception:
        return []


def list_plan_equipment_joined(conn, plan_id: int) -> list[dict[str, Any]]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT a.id, a.public_asset_code, a.serial_number, i.name AS item_name,
                   l.sort_order
            FROM crm_event_plan_equipment_assets l
            INNER JOIN inventory_equipment_assets a ON a.id = l.equipment_asset_id
            INNER JOIN inventory_items i ON i.id = a.item_id
            WHERE l.plan_id=%s
            ORDER BY l.sort_order, a.id
            """,
            (int(plan_id),),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()


def add_equipment_asset_to_plan(conn, plan_id: int, equipment_asset_id: int) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM crm_event_plan_equipment_assets WHERE plan_id=%s AND equipment_asset_id=%s",
            (int(plan_id), int(equipment_asset_id)),
        )
        if cur.fetchone():
            return False
        cur.execute(
            "SELECT IFNULL(MAX(sort_order), -1) + 1 FROM crm_event_plan_equipment_assets WHERE plan_id=%s",
            (int(plan_id),),
        )
        n = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO crm_event_plan_equipment_assets (plan_id, equipment_asset_id, sort_order)
            VALUES (%s, %s, %s)
            """,
            (int(plan_id), int(equipment_asset_id), n),
        )
        conn.commit()
        return True
    finally:
        cur.close()


def remove_equipment_asset_from_plan(conn, plan_id: int, equipment_asset_id: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM crm_event_plan_equipment_assets WHERE plan_id=%s AND equipment_asset_id=%s",
            (int(plan_id), int(equipment_asset_id)),
        )
        conn.commit()
    finally:
        cur.close()


def resolve_equipment_asset_id(conn, raw: str) -> int | None:
    s = (raw or "").strip()
    if not s:
        return None
    if s.isdigit():
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT id FROM inventory_equipment_assets WHERE id=%s LIMIT 1",
                (int(s),),
            )
            row = cur.fetchone()
            return int(row[0]) if row else None
        finally:
            cur.close()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM inventory_equipment_assets WHERE public_asset_code=%s LIMIT 1",
            (s[:64],),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None
    finally:
        cur.close()


def list_diagram_rows_for_plan(conn, plan_id: int) -> list[dict[str, Any]]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT d.id, d.plan_id, d.template_id, d.plan_image_path, d.caption, d.sort_order,
                   t.image_path AS template_image_path, t.title AS template_title
            FROM crm_event_plan_diagrams d
            LEFT JOIN crm_event_plan_diagram_templates t ON t.id = d.template_id
            WHERE d.plan_id=%s
            ORDER BY d.sort_order, d.id
            """,
            (int(plan_id),),
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()


def list_diagram_templates(conn) -> list[dict[str, Any]]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, title, image_path, sort_order FROM crm_event_plan_diagram_templates ORDER BY sort_order, id"
        )
        return list(cur.fetchall() or [])
    finally:
        cur.close()


def add_diagram_from_template(conn, plan_id: int, template_id: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT IFNULL(MAX(sort_order), -1) + 1 FROM crm_event_plan_diagrams WHERE plan_id=%s",
            (int(plan_id),),
        )
        n = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO crm_event_plan_diagrams (plan_id, template_id, plan_image_path, caption, sort_order)
            VALUES (%s, %s, NULL, NULL, %s)
            """,
            (int(plan_id), int(template_id), n),
        )
        conn.commit()
    finally:
        cur.close()


def save_diagram_caption(conn, diagram_id: int, plan_id: int, caption: str | None) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE crm_event_plan_diagrams SET caption=%s WHERE id=%s AND plan_id=%s",
            ((caption or "").strip()[:512] or None, int(diagram_id), int(plan_id)),
        )
        conn.commit()
    finally:
        cur.close()


def delete_diagram(conn, diagram_id: int, plan_id: int, app) -> None:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT plan_image_path FROM crm_event_plan_diagrams WHERE id=%s AND plan_id=%s",
            (int(diagram_id), int(plan_id)),
        )
        row = cur.fetchone()
        if row and row.get("plan_image_path"):
            rel = row["plan_image_path"].replace("\\", "/")
            full = os.path.join(crm_static_dir_for_app(app), *rel.split("/"))
            try:
                if os.path.isfile(full):
                    os.remove(full)
            except OSError:
                pass
        cur.execute(
            "DELETE FROM crm_event_plan_diagrams WHERE id=%s AND plan_id=%s",
            (int(diagram_id), int(plan_id)),
        )
        conn.commit()
    finally:
        cur.close()


def store_uploaded_diagram(app, plan_id: int, file_storage, conn) -> None:
    ext = _image_ext(file_storage.filename or "")
    if not ext:
        raise ValueError("Use PNG, JPG, JPEG, WebP, or GIF")
    dest_dir = crm_event_plan_diagrams_write_dir(app)
    os.makedirs(dest_dir, exist_ok=True)
    fname = f"plan_{plan_id}_{uuid.uuid4().hex[:10]}{ext}"
    full = os.path.join(dest_dir, fname)
    file_storage.save(full)
    rel = f"{crm_event_plan_diagrams_relative_subpath()}/{fname}".replace("\\", "/")
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT IFNULL(MAX(sort_order), -1) + 1 FROM crm_event_plan_diagrams WHERE plan_id=%s",
            (int(plan_id),),
        )
        n = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO crm_event_plan_diagrams (plan_id, template_id, plan_image_path, caption, sort_order)
            VALUES (%s, NULL, %s, NULL, %s)
            """,
            (int(plan_id), rel, n),
        )
        conn.commit()
    finally:
        cur.close()


def store_template_diagram_image(app, file_storage, title: str) -> None:
    ext = _image_ext(file_storage.filename or "")
    if not ext:
        raise ValueError("Use PNG, JPG, JPEG, WebP, or GIF")
    dest_dir = crm_diagram_templates_write_dir(app)
    os.makedirs(dest_dir, exist_ok=True)
    fname = f"tpl_{uuid.uuid4().hex[:12]}{ext}"
    full = os.path.join(dest_dir, fname)
    file_storage.save(full)
    rel = f"{crm_diagram_templates_relative_subpath()}/{fname}".replace("\\", "/")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT IFNULL(MAX(sort_order), -1) + 1 FROM crm_event_plan_diagram_templates",
        )
        n = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO crm_event_plan_diagram_templates (title, image_path, sort_order)
            VALUES (%s, %s, %s)
            """,
            ((title or "Diagram").strip()[:255], rel, n),
        )
        conn.commit()
    finally:
        cur.close()


def delete_template(conn, template_id: int, app) -> None:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT image_path FROM crm_event_plan_diagram_templates WHERE id=%s",
            (int(template_id),),
        )
        row = cur.fetchone()
        if row and row.get("image_path"):
            rel = row["image_path"].replace("\\", "/")
            full = os.path.join(crm_static_dir_for_app(app), *rel.split("/"))
            try:
                if os.path.isfile(full):
                    os.remove(full)
            except OSError:
                pass
        cur.execute(
            "DELETE FROM crm_event_plan_diagram_templates WHERE id=%s", (int(template_id),)
        )
        conn.commit()
    finally:
        cur.close()


def diagram_pdf_src(row: dict[str, Any]) -> str | None:
    if row.get("template_id") and row.get("template_image_path"):
        return str(row["template_image_path"]).replace("\\", "/")
    if row.get("plan_image_path"):
        return str(row["plan_image_path"]).replace("\\", "/")
    return None
