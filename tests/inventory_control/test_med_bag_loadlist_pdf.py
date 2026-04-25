"""Med bag kit loadlist PDF (no DB)."""

from app.plugins.inventory_control.med_bag_loadlist_pdf import render_med_bag_loadlist_pdf
from app.plugins.inventory_control.med_bag_service import enrich_sections_expiry_hints


def test_render_med_bag_loadlist_pdf_minimal():
    data = {
        "root_instance_id": 1,
        "root": {
            "id": 1,
            "public_asset_number": "MB-001",
            "template_code": "RESP",
            "template_name": "Response bag",
        },
        "generated_at_label": "2099-01-01 12:00",
        "sections": [
            {
                "instance_id": 1,
                "depth": 0,
                "title": "Bag #1 — MB-001 — RESP",
                "template_name": "Response bag",
                "template_code": "RESP",
                "lines": [
                    {
                        "sku": "PAR500",
                        "item_name": "Paracetamol 500mg",
                        "quantity_expected": 10,
                        "quantity_on_bag": 10,
                        "lot_number": "LOT-A",
                        "expiry_date": "2027-06-30",
                    }
                ],
            },
            {
                "instance_id": 2,
                "depth": 1,
                "title": "Bag #2 — AIR-1 — AIRWAY",
                "template_name": "Airway pod",
                "template_code": "AIRWAY",
                "lines": [],
            },
        ],
    }
    pdf = render_med_bag_loadlist_pdf(data)
    assert pdf[:4] == b"%PDF"
    assert len(pdf) > 500


def test_enrich_sections_expiry_hints():
    sections = [
        {
            "lines": [
                {"expiry_date": "2000-01-01"},
                {"expiry_date": "2099-12-31"},
                {"expiry_date": None},
            ]
        }
    ]
    enrich_sections_expiry_hints(sections)
    ln0 = sections[0]["lines"][0]
    assert ln0["expiry_tier"] == "expired"
    assert "Expired" in ln0["expiry_hint"]


def test_enrich_sections_expiry_hints_training_bag():
    sections = [{"usage_context": "training", "lines": [{"expiry_date": "2000-01-01"}]}]
    enrich_sections_expiry_hints(sections)
    ln = sections[0]["lines"][0]
    assert ln["expiry_tier"] == "training_suppressed"
    assert "Training" in ln["expiry_hint"]
