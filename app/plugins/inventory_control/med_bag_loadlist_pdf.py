"""
PDF kit loadlist for med bag instances (parent + nested modules), with lot/expiry columns.
"""

from __future__ import annotations

import html
from io import BytesIO
from typing import Any, Dict, List

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def _p(text: Any, max_len: int = 500) -> str:
    """Plain text for ReportLab Table cells (no markup)."""
    if text is None:
        return ""
    s = str(text).replace("\r", " ").replace("\n", " ").strip()
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return s


def _para_xml(text: Any) -> str:
    """Escape for ReportLab Paragraph markup."""
    return html.escape("" if text is None else str(text), quote=True)


def render_med_bag_loadlist_pdf(data: Dict[str, Any]) -> bytes:
    """
    Build a PDF from `MedBagService.build_hierarchical_loadlist` payload.
    """
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        rightMargin=1.4 * cm,
        leftMargin=1.4 * cm,
        topMargin=1.4 * cm,
        bottomMargin=1.4 * cm,
        title="Med bag kit loadlist",
    )
    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    body = styles["Normal"]
    body.fontSize = 9
    h_sec = styles["Heading4"]
    h_sec.fontSize = 11
    h_sec.spaceBefore = 8
    h_sec.spaceAfter = 4

    story: List[Any] = []
    story.append(Paragraph("Med bag kit loadlist", title_style))
    root = data.get("root") or {}
    sub_parts = [f"Root bag #{root.get('id', '')}"]
    if root.get("public_asset_number"):
        sub_parts.append(str(root["public_asset_number"]))
    if root.get("template_code"):
        sub_parts.append(str(root["template_code"]))
    story.append(Paragraph(_para_xml(" — ".join(sub_parts)), body))
    if root.get("template_name"):
        story.append(Paragraph(_para_xml(str(root["template_name"])), body))
    story.append(Paragraph(_para_xml(f"Generated: {data.get('generated_at_label', '')}"), body))
    story.append(Spacer(1, 0.35 * cm))

    sections: List[Dict[str, Any]] = data.get("sections") or []
    tw = doc.width
    col_w = [tw * 0.11, tw * 0.34, tw * 0.09, tw * 0.09, tw * 0.19, tw * 0.14]

    for sec in sections:
        depth = int(sec.get("depth") or 0)
        prefix = ("↳ " * depth) if depth else ""
        head = prefix + (sec.get("title") or f"Bag #{sec.get('instance_id')}")
        tmpl = sec.get("template_name") or ""
        story.append(
            Paragraph(
                f"<b>{_para_xml(head)}</b>"
                + (f" <font size=8 color=grey>({_para_xml(tmpl)})</font>" if tmpl else ""),
                h_sec,
            )
        )
        lines = sec.get("lines") or []
        if not lines:
            story.append(Paragraph("<i>No consumable lines on this bag.</i>", body))
            story.append(Spacer(1, 0.15 * cm))
            continue

        hdr = ["SKU", "Item", "Expected", "On bag", "Lot", "Expiry"]
        tbl_data: List[List[str]] = [hdr]
        for ln in lines:
            tbl_data.append(
                [
                    _p(ln.get("sku"), 32),
                    _p(ln.get("item_name"), 80),
                    _p(ln.get("quantity_expected")),
                    _p(ln.get("quantity_on_bag")),
                    _p(ln.get("lot_number"), 40),
                    _p(ln.get("expiry_date"), 16),
                ]
            )
        t = Table(tbl_data, colWidths=col_w, repeatRows=1)
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e9ecef")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("BOX", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#dee2e6")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fa")]),
                ]
            )
        )
        story.append(t)
        story.append(Spacer(1, 0.25 * cm))

    doc.build(story)
    return buf.getvalue()
