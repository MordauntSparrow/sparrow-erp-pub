"""What's new: version comparison and payload merge."""
from __future__ import annotations

import json
import os
import tempfile

from app.user_whats_new import (
    _payload_has_visible_content,
    _normalize_whats_new_sections,
    _release_should_show,
    _version_tuple,
    load_whats_new_payload,
)


def test_version_tuple_basic():
    assert _version_tuple("1.2.1") > _version_tuple("1.2.0")
    assert _version_tuple("2.0.0") > _version_tuple("1.9.9")
    assert _version_tuple("") == (0,)


def test_release_should_show():
    assert _release_should_show(release="1.1.0", last_seen=None) is True
    assert _release_should_show(release="1.1.0", last_seen="") is True
    assert _release_should_show(release="1.1.0", last_seen="1.1.0") is False
    assert _release_should_show(release="1.1.1", last_seen="1.1.0") is True


def test_normalize_sections_and_payload_visibility():
    assert _normalize_whats_new_sections(None) == []
    assert _normalize_whats_new_sections([{"heading": "H", "paragraphs": ["a", "b"]}]) == [
        {"heading": "H", "paragraphs": ["a", "b"]}
    ]
    assert _payload_has_visible_content({"version": "1.0.0", "title": "", "summary": ""}) is False
    assert (
        _payload_has_visible_content(
            {
                "version": "1.0.0",
                "sections": [{"heading": "Only heading", "paragraphs": []}],
            }
        )
        is True
    )
    assert (
        _payload_has_visible_content(
            {
                "version": "1.0.0",
                "sections": [{"heading": "", "paragraphs": ["body only"]}],
            }
        )
        is True
    )


def test_load_whats_new_payload_merges_manifest():
    with tempfile.TemporaryDirectory() as d:
        cfg = os.path.join(d, "config")
        os.makedirs(cfg, exist_ok=True)
        path = os.path.join(cfg, "whats_new.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "version": "1.0.0",
                    "title": "From file",
                    "summary": "S",
                    "bullets": ["a"],
                },
                f,
            )
        merged = load_whats_new_payload(
            app_root=d,
            core_manifest={
                "whats_new": {
                    "title": "From manifest",
                    "summary": "Over",
                }
            },
        )
        assert merged["version"] == "1.0.0"
        assert merged["title"] == "From manifest"
        assert merged["summary"] == "Over"
        assert merged["bullets"] == ["a"]
