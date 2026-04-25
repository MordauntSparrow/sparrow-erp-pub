"""Event plan editor user lookup: HR ``tb_contractors`` (+ staff details) merged with Sparrow ``users``."""
from __future__ import annotations

from typing import Any


def _table_exists(cur, name: str) -> bool:
    cur.execute("SHOW TABLES LIKE %s", (name,))
    return bool(cur.fetchone())


def _columns(cur, table: str) -> set[str]:
    cur.execute(f"SHOW COLUMNS FROM `{table}`")
    return {str(r.get("Field") or "").lower() for r in (cur.fetchall() or [])}


def search_event_plan_staff(conn, pat: str) -> list[dict[str, Any]]:
    """
    Typeahead rows: ``username`` (login / lookup key), ``display_name``, ``role_grade_hint``
    (clinical / job title from HR ``hr_staff_details.job_title`` or ``tb_contractors.job_title``),
    ``phone`` (from ``hr_staff_details.phone`` by ``contractor_id`` when set; overrides ``users``),
    ``source`` (``hr`` | ``account``), optional ``contractor_id``.
    """
    cur = conn.cursor(dictionary=True)
    merged: dict[str, dict[str, Any]] = {}
    try:
        # --- HR employees (Time Billing / HR person database) ---
        if _table_exists(cur, "tb_contractors"):
            ccols = _columns(cur, "tb_contractors")
            has_hr = _table_exists(cur, "hr_staff_details")
            hr_job = hr_phone = False
            if has_hr:
                hf = _columns(cur, "hr_staff_details")
                hr_job = "job_title" in hf
                hr_phone = "phone" in hf
            c_job = "job_title" in ccols
            has_username = "username" in ccols
            has_status = "status" in ccols

            sel = ["c.id AS contractor_id", "c.name", "c.email"]
            if has_username:
                sel.append("c.username")
            if c_job:
                sel.append("c.job_title AS contractor_job_title")
            if hr_phone:
                sel.append("h.phone AS hr_phone")
            if hr_job:
                sel.append("h.job_title AS hr_job_title")

            joins = "FROM tb_contractors c"
            if has_hr:
                joins += " LEFT JOIN hr_staff_details h ON h.contractor_id = c.id"

            wp: list[str] = []
            prm: list[Any] = []
            if has_username:
                wp.append("c.username LIKE %s")
                prm.append(pat)
            wp.append("LOWER(c.email) LIKE LOWER(%s)")
            prm.append(pat)
            wp.append("c.name LIKE %s")
            prm.append(pat)
            if "initials" in ccols:
                wp.append("c.initials LIKE %s")
                prm.append(pat)

            st_ex = ""
            if has_status:
                st_ex = (
                    " AND (c.status IS NULL OR TRIM(LOWER(c.status)) IN "
                    "('active','','pending','probation'))"
                )

            cur.execute(
                f"""
                SELECT {", ".join(sel)}
                {joins}
                WHERE ({' OR '.join(wp)}){st_ex}
                ORDER BY c.name ASC
                LIMIT 25
                """,
                tuple(prm),
            )
            for row in cur.fetchall() or []:
                uname = str(row.get("username") or "").strip()
                email = str(row.get("email") or "").strip()
                key = uname.lower() if uname else f"e:{email.lower()}"
                if not key or key == "e:":
                    continue
                hr_jt = str(row.get("hr_job_title") or "").strip() if hr_job else ""
                c_jt = (
                    str(row.get("contractor_job_title") or "").strip()
                    if c_job
                    else ""
                )
                grade = (hr_jt or c_jt).strip()
                nm = str(row.get("name") or "").strip()
                disp = nm or email or uname or f"ID {row.get('contractor_id')}"
                ph = str(row.get("hr_phone") or "").strip()[:32] if hr_phone else ""
                login_val = (uname or email)[:128]
                if not login_val:
                    continue
                try:
                    cid = int(row["contractor_id"])
                except (TypeError, ValueError, KeyError):
                    cid = None
                rec = {
                    "username": login_val,
                    "display_name": disp[:128],
                    "role_grade_hint": grade[:128],
                    "phone": ph,
                    "source": "hr",
                }
                if cid is not None:
                    rec["contractor_id"] = cid
                merged[key] = rec

        if not _table_exists(cur, "users"):
            return _finalize_event_plan_staff_rows(cur, merged)

        ucols = _columns(cur, "users")
        phone_col = next(
            (
                c
                for c in ("mobile_phone", "phone", "phone_number", "telephone", "mobile")
                if c in ucols
            ),
            None,
        )
        grade_col = next(
            (
                c
                for c in (
                    "job_title",
                    "title",
                    "position",
                    "grade",
                    "role_title",
                    "clinical_grade",
                )
                if c in ucols
            ),
            None,
        )
        sel_u = ["username"]
        if "contractor_id" in ucols:
            sel_u.append("contractor_id")
        for c in ("email", "first_name", "last_name"):
            if c in ucols:
                sel_u.append(c)
        if phone_col:
            sel_u.append(phone_col)
        if grade_col:
            sel_u.append(grade_col)

        name_concat = "TRIM(CONCAT(COALESCE(first_name, ''), ' ', COALESCE(last_name, '')))"
        wu = ["username LIKE %s"]
        pu: list[Any] = [pat]
        if "email" in ucols:
            wu.append("LOWER(email) LIKE LOWER(%s)")
            pu.append(pat)
        if "first_name" in ucols and "last_name" in ucols:
            wu.append(f"{name_concat} LIKE %s")
            pu.append(pat)

        cur.execute(
            f"""
            SELECT {", ".join(sel_u)}
              FROM users
             WHERE ({' OR '.join(wu)})
             ORDER BY username ASC
             LIMIT 25
            """,
            tuple(pu),
        )
        for row in cur.fetchall() or []:
            u = str(row.get("username") or "").strip()
            if not u:
                continue
            key = u.lower()
            fn = ""
            if "first_name" in row or "last_name" in row:
                fn = (
                    str(row.get("first_name") or "").strip()
                    + " "
                    + str(row.get("last_name") or "").strip()
                ).strip()
            disp = fn or str(row.get("email") or "").strip() or u
            hint = str(row.get(grade_col) or "").strip() if grade_col else ""
            ph = str(row.get(phone_col) or "").strip()[:32] if phone_col else ""
            cid_raw = row.get("contractor_id")
            cid: int | None = None
            if cid_raw is not None and str(cid_raw).strip().isdigit():
                try:
                    cid = int(cid_raw)
                except (TypeError, ValueError):
                    cid = None

            hit = merged.get(key)
            if hit and hit.get("source") == "hr":
                if fn:
                    hit["display_name"] = disp[:128]
                if hint and not (hit.get("role_grade_hint") or "").strip():
                    hit["role_grade_hint"] = hint[:128]
                if ph and not (hit.get("phone") or "").strip():
                    hit["phone"] = ph
                continue

            merged_by_cid = None
            if cid is not None:
                for m in merged.values():
                    if m.get("contractor_id") == cid:
                        merged_by_cid = m
                        break
            if merged_by_cid is not None:
                merged_by_cid["username"] = u[:128]
                if disp:
                    merged_by_cid["display_name"] = disp[:128]
                if hint and not (merged_by_cid.get("role_grade_hint") or "").strip():
                    merged_by_cid["role_grade_hint"] = hint[:128]
                if ph and not (merged_by_cid.get("phone") or "").strip():
                    merged_by_cid["phone"] = ph
                continue

            if key in merged:
                continue

            merged[key] = {
                "username": u,
                "display_name": disp[:128],
                "role_grade_hint": hint[:128],
                "phone": ph,
                "source": "account",
            }
            if cid is not None:
                merged[key]["contractor_id"] = cid

        return _finalize_event_plan_staff_rows(cur, merged)

    finally:
        cur.close()


def _finalize_event_plan_staff_rows(
    cur, merged: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    _apply_hr_staff_phones(cur, merged)
    out = _sorted_results(merged)
    for r in out:
        r.pop("contractor_id", None)
    return out


def _apply_hr_staff_phones(cur, merged: dict[str, dict[str, Any]]) -> None:
    """
    Set ``phone`` from HR ``hr_staff_details.phone`` for every merged row with a
    ``contractor_id`` when HR has a non-empty value. Overwrites ``users`` phone so
    the event plan editor consistently shows the HR module number.
    """
    if not merged or not _table_exists(cur, "hr_staff_details"):
        return
    if "phone" not in _columns(cur, "hr_staff_details"):
        return
    cids: list[int] = []
    seen: set[int] = set()
    for r in merged.values():
        raw = r.get("contractor_id")
        if raw is None or str(raw).strip() == "":
            continue
        try:
            cid = int(raw)
        except (TypeError, ValueError):
            continue
        if cid not in seen:
            seen.add(cid)
            cids.append(cid)
    if not cids:
        return
    placeholders = ", ".join(["%s"] * len(cids))
    cur.execute(
        f"""
        SELECT contractor_id, phone
          FROM hr_staff_details
         WHERE contractor_id IN ({placeholders})
           AND phone IS NOT NULL AND TRIM(phone) <> ''
        """,
        tuple(cids),
    )
    hr_phone: dict[int, str] = {}
    for row in cur.fetchall() or []:
        try:
            cid = int(row["contractor_id"])
        except (TypeError, ValueError, KeyError):
            continue
        p = str(row.get("phone") or "").strip()
        if p:
            hr_phone[cid] = p[:32]
    if not hr_phone:
        return
    for r in merged.values():
        raw = r.get("contractor_id")
        if raw is None:
            continue
        try:
            cid = int(raw)
        except (TypeError, ValueError):
            continue
        p = hr_phone.get(cid)
        if p:
            r["phone"] = p


def _sorted_results(merged: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = list(merged.values())

    def sort_key(r: dict[str, Any]) -> tuple[int, str]:
        src = 0 if r.get("source") == "hr" else 1
        return (src, (r.get("display_name") or r.get("username") or "").lower())

    rows.sort(key=sort_key)
    return rows[:30]


def written_by_me_display_for_staff(conn, user_id: Any, username_fallback: str) -> str:
    """
    Full name for the event plan **Written by → Use me** control: prefer
    ``tb_contractors.name`` (via ``users.contractor_id`` or matching contractor
    ``username``), else ``users`` first/last name, else Sparrow username.
    """
    un_fb = (username_fallback or "").strip()
    cur = conn.cursor(dictionary=True)
    try:
        if not user_id or not _table_exists(cur, "users"):
            return (un_fb[:255] if un_fb else "")
        ucols = _columns(cur, "users")
        parts = ["id", "username"]
        if "first_name" in ucols:
            parts.append("first_name")
        if "last_name" in ucols:
            parts.append("last_name")
        if "contractor_id" in ucols:
            parts.append("contractor_id")
        cur.execute(
            f"SELECT {', '.join(parts)} FROM users WHERE id = %s LIMIT 1",
            (user_id,),
        )
        u = cur.fetchone() or {}
        un = str(u.get("username") or un_fb or "").strip()
        fn = ""
        if "first_name" in ucols or "last_name" in ucols:
            fn = (
                str(u.get("first_name") or "").strip()
                + " "
                + str(u.get("last_name") or "").strip()
            ).strip()

        name_from_contractor = ""
        if _table_exists(cur, "tb_contractors"):
            ccols = _columns(cur, "tb_contractors")
            cid_raw = u.get("contractor_id") if "contractor_id" in ucols else None
            if cid_raw is not None and str(cid_raw).strip().isdigit():
                try:
                    cur.execute(
                        "SELECT name FROM tb_contractors WHERE id = %s LIMIT 1",
                        (int(cid_raw),),
                    )
                    r = cur.fetchone()
                    if r:
                        name_from_contractor = str(r.get("name") or "").strip()
                except (TypeError, ValueError):
                    name_from_contractor = ""
            if not name_from_contractor and un and "username" in ccols:
                cur.execute(
                    """
                    SELECT name FROM tb_contractors
                    WHERE LOWER(TRIM(username)) = LOWER(TRIM(%s))
                    LIMIT 1
                    """,
                    (un,),
                )
                r = cur.fetchone()
                if r:
                    name_from_contractor = str(r.get("name") or "").strip()

        if name_from_contractor:
            return name_from_contractor[:255]
        if fn:
            return fn[:255]
        pick = un or un_fb
        return pick[:255] if pick else ""
    finally:
        cur.close()
