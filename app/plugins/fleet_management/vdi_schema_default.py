"""Default Vehicle Daily Inspection (VDI) form schema (JSON). Admin can replace via fleet_vdi_schema table."""

from __future__ import annotations

import json

DEFAULT_VDI_SCHEMA: dict = {
    "version": 1,
    "title": "Vehicle daily inspection",
    "additional_forms": [],
    "sections": [
        {
            "id": "mileage",
            "title": "Mileage",
            "fields": [
                {
                    "id": "odometer",
                    "type": "mileage",
                    "label": "Current odometer reading",
                    "required": True,
                    "min": 0,
                    "max": 2000000,
                }
            ],
        },
        {
            "id": "walkaround",
            "title": "Walk-around checks",
            "fields": [
                {
                    "id": "tyres_visual",
                    "type": "bool",
                    "label": "Tyres visually OK (no obvious damage)",
                    "required": True,
                },
                {
                    "id": "tyre_depth_mm",
                    "type": "number",
                    "label": "Minimum tread depth (mm)",
                    "required": False,
                    "min": 0,
                    "max": 20,
                    "step": 0.1,
                },
                {
                    "id": "engine_fluids",
                    "type": "bool",
                    "label": "Engine bay / fluids OK",
                    "required": True,
                },
                {
                    "id": "lights",
                    "type": "bool",
                    "label": "Lights / indicators OK",
                    "required": True,
                },
                {
                    "id": "notes",
                    "type": "text",
                    "label": "Additional notes",
                    "required": False,
                },
            ],
        },
        {
            "id": "photos",
            "title": "Photos",
            "fields": [
                {
                    "id": "photo_front",
                    "type": "photo",
                    "label": "Front of vehicle",
                    "required": False,
                },
                {
                    "id": "photo_rear",
                    "type": "photo",
                    "label": "Rear of vehicle",
                    "required": False,
                },
            ],
        },
    ],
}


def default_vdi_schema_json() -> str:
    return json.dumps(DEFAULT_VDI_SCHEMA, indent=2)
