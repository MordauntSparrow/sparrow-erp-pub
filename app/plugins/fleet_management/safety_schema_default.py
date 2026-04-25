"""Default Safety Check form schema (per vehicle type). Admin edits via form builder."""

from __future__ import annotations

import json

DEFAULT_SAFETY_SCHEMA: dict = {
    "version": 1,
    "title": "Workshop safety check",
    "sections": [
        {
            "id": "mileage",
            "title": "Odometer",
            "fields": [
                {
                    "id": "odometer",
                    "type": "mileage",
                    "label": "Odometer at check (or use the field at top of page)",
                    "required": False,
                    "min": 0,
                    "max": 2000000,
                }
            ],
        },
        {
            "id": "fluids",
            "title": "Fluids & levels",
            "fields": [
                {
                    "id": "engine_oil",
                    "type": "bool",
                    "label": "Engine oil level OK",
                    "required": True,
                },
                {
                    "id": "coolant",
                    "type": "bool",
                    "label": "Coolant level OK",
                    "required": True,
                },
                {
                    "id": "screenwash",
                    "type": "bool",
                    "label": "Screen wash topped up",
                    "required": False,
                },
                {
                    "id": "fluid_notes",
                    "type": "text",
                    "label": "Fluid / leak notes",
                    "required": False,
                },
            ],
        },
        {
            "id": "safety",
            "title": "Safety items",
            "fields": [
                {
                    "id": "tyres",
                    "type": "bool",
                    "label": "Tyres / pressures visually OK",
                    "required": True,
                },
                {
                    "id": "lights",
                    "type": "bool",
                    "label": "Lights & warnings OK",
                    "required": True,
                },
                {
                    "id": "findings",
                    "type": "text",
                    "label": "Findings & follow-up",
                    "required": False,
                },
            ],
        },
    ],
}


def default_safety_schema_json() -> str:
    return json.dumps(DEFAULT_SAFETY_SCHEMA, indent=2)
