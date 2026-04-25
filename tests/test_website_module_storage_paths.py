"""Resolved website_module site_data paths (volume vs local)."""
import json
import os
from pathlib import Path


def test_resolved_site_data_local(tmp_path, monkeypatch):
    monkeypatch.delenv("RAILWAY_VOLUME_MOUNT_PATH", raising=False)
    monkeypatch.delenv("SPARROW_DATA_ROOT", raising=False)
    monkeypatch.delenv("PERSISTENT_DATA_ROOT", raising=False)
    monkeypatch.delenv("RAILWAY_DATA_ROOT", raising=False)
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    from app.storage_paths import (
        migrate_website_module_site_data_once,
        resolved_pages_json_path,
        resolved_website_module_data_dir,
        resolved_website_module_site_data_dir,
        resolved_website_module_user_manifest_path,
    )

    mod = tmp_path / "website_module"
    mod.mkdir()
    site = resolved_website_module_site_data_dir(str(mod))
    assert site == os.path.join(str(mod), "site_data")
    assert resolved_pages_json_path(str(mod)) == os.path.join(site, "pages.json")
    assert resolved_website_module_data_dir(str(mod)) == os.path.join(site, "data")
    assert resolved_website_module_user_manifest_path(str(mod)) == os.path.join(site, "manifest.json")

    legacy_pages = mod / "pages.json"
    legacy_pages.write_text("[{}]", encoding="utf-8")
    migrate_website_module_site_data_once(str(mod))
    target = mod / "site_data" / "pages.json"
    assert target.is_file()


def test_resolved_site_data_volume(monkeypatch, tmp_path):
    vol = os.path.join(str(tmp_path), "railway_volume")
    os.makedirs(vol, exist_ok=True)
    monkeypatch.setattr(
        "app.storage_paths.get_persistent_data_root", lambda: vol
    )
    from app.storage_paths import resolved_pages_json_path, resolved_website_module_site_data_dir

    mod = "/app/app/plugins/website_module"
    site = resolved_website_module_site_data_dir(mod)
    expected = os.path.join(vol, "plugins", "website_module", "site_data")
    assert os.path.normpath(site) == os.path.normpath(expected)
    assert os.path.normpath(resolved_pages_json_path(mod)) == os.path.normpath(
        os.path.join(expected, "pages.json")
    )


def test_push_website_volume_full_replace_public_and_static(tmp_path, monkeypatch):
    """Push replaces public_templates + static trees; orphans removed (PRD FR-3)."""
    vol = str(tmp_path / "vol")
    os.makedirs(vol, exist_ok=True)
    monkeypatch.setattr("app.storage_paths.get_persistent_data_root", lambda: vol)

    pkg = tmp_path / "app_pkg"
    wm = pkg / "plugins" / "website_module"
    (wm / "templates" / "public_bundled").mkdir(parents=True)
    (wm / "templates" / "public_bundled" / "index.html").write_text("<h1>new</h1>", encoding="utf-8")
    (wm / "static_bundled").mkdir(parents=True)
    (wm / "static_bundled" / "hero.webp").write_bytes(b"NEWBYTES")

    sd = wm / "site_data"
    sd.mkdir(parents=True)
    (sd / "pages.json").write_text("[]", encoding="utf-8")
    (sd / "manifest.json").write_text("{}", encoding="utf-8")
    (sd / "local_service_pages.json").write_text("{}", encoding="utf-8")

    pub_dst = Path(vol) / "plugins" / "website_module" / "public_templates"
    static_dst = Path(vol) / "plugins" / "website_module" / "static"
    pub_dst.mkdir(parents=True)
    (pub_dst / "index.html").write_text("<h1>old</h1>", encoding="utf-8")
    (pub_dst / "orphan.html").write_text("stale", encoding="utf-8")
    static_dst.mkdir(parents=True)
    (static_dst / "hero.webp").write_bytes(b"OLDBYTES")
    (static_dst / "only_on_volume.bin").write_bytes(b"x")
    (static_dst / "uploads" / "builder").mkdir(parents=True)
    (static_dst / "uploads" / "builder" / "asset.txt").write_text("tenant-media", encoding="utf-8")
    (static_dst / "favicon.ico").write_bytes(b"ICO")

    from app.storage_paths import push_website_volume_from_package

    r = push_website_volume_from_package(str(pkg), prefer_bundled_public=True)
    assert r.get("public_ok") is True
    assert r.get("static_ok") is True
    assert (pub_dst / "index.html").read_text(encoding="utf-8") == "<h1>new</h1>"
    assert not (pub_dst / "orphan.html").exists()
    assert (static_dst / "hero.webp").read_bytes() == b"NEWBYTES"
    assert not (static_dst / "only_on_volume.bin").exists()
    assert (static_dst / "uploads" / "builder" / "asset.txt").read_text(encoding="utf-8") == "tenant-media"
    assert (static_dst / "favicon.ico").read_bytes() == b"ICO"


def test_push_website_volume_static_skipped_without_sources(tmp_path, monkeypatch):
    """When static_bundled and real static are absent, static_ok is False (FR-4)."""
    vol = str(tmp_path / "vol2")
    os.makedirs(vol, exist_ok=True)
    monkeypatch.setattr("app.storage_paths.get_persistent_data_root", lambda: vol)

    pkg = tmp_path / "app_pkg2"
    wm = pkg / "plugins" / "website_module"
    (wm / "templates" / "public_bundled").mkdir(parents=True)
    (wm / "templates" / "public_bundled" / "index.html").write_text("ok", encoding="utf-8")
    (wm / "site_data").mkdir(parents=True)
    (wm / "site_data" / "pages.json").write_text("[]", encoding="utf-8")

    from app.storage_paths import push_website_volume_from_package

    r = push_website_volume_from_package(str(pkg), prefer_bundled_public=True)
    assert r.get("public_ok") is True
    assert r.get("static_ok") is False


def test_push_skips_existing_site_data_json(tmp_path, monkeypatch):
    """Push must not overwrite tenant pages.json / manifest already on the volume."""
    vol = str(tmp_path / "vol_sd")
    os.makedirs(vol, exist_ok=True)
    monkeypatch.setattr("app.storage_paths.get_persistent_data_root", lambda: vol)

    pkg = tmp_path / "app_pkg_sd"
    wm = pkg / "plugins" / "website_module"
    (wm / "templates" / "public_bundled").mkdir(parents=True)
    (wm / "templates" / "public_bundled" / "index.html").write_text("x", encoding="utf-8")
    (wm / "static_bundled").mkdir(parents=True)
    (wm / "static_bundled" / "a.css").write_text("/*pkg*/", encoding="utf-8")
    sd = wm / "site_data"
    sd.mkdir(parents=True)
    (sd / "pages.json").write_text('["from_package"]', encoding="utf-8")
    (sd / "manifest.json").write_text('{"version": "9.9.9", "settings": {}}', encoding="utf-8")
    (sd / "local_service_pages.json").write_text('{"pkg": true}', encoding="utf-8")

    site_dst = Path(vol) / "plugins" / "website_module" / "site_data"
    site_dst.mkdir(parents=True)
    (site_dst / "pages.json").write_text('["TENANT_PAGES"]', encoding="utf-8")
    (site_dst / "manifest.json").write_text('{"settings": {"favicon_path": "favicon.ico"}}', encoding="utf-8")
    (site_dst / "local_service_pages.json").write_text('{"tenant": true}', encoding="utf-8")

    from app.storage_paths import push_website_volume_from_package

    r = push_website_volume_from_package(str(pkg), prefer_bundled_public=True)
    assert r.get("ok") is True
    assert (site_dst / "pages.json").read_text(encoding="utf-8") == '["TENANT_PAGES"]'
    assert '"favicon_path": "favicon.ico"' in (site_dst / "manifest.json").read_text(encoding="utf-8")
    assert '"tenant": true' in (site_dst / "local_service_pages.json").read_text(encoding="utf-8")
    assert r.get("json_ok") == 0


def test_push_seeds_manifest_without_version(tmp_path, monkeypatch):
    """First-time manifest seed strips bundled version from the volume copy."""
    vol = str(tmp_path / "vol_mf")
    os.makedirs(vol, exist_ok=True)
    monkeypatch.setattr("app.storage_paths.get_persistent_data_root", lambda: vol)

    pkg = tmp_path / "app_pkg_mf"
    wm = pkg / "plugins" / "website_module"
    (wm / "templates" / "public_bundled").mkdir(parents=True)
    (wm / "templates" / "public_bundled" / "index.html").write_text("x", encoding="utf-8")
    (wm / "static_bundled").mkdir(parents=True)
    (wm / "static_bundled" / "a.css").write_text("/*x*/", encoding="utf-8")
    sd = wm / "site_data"
    sd.mkdir(parents=True)
    (sd / "pages.json").write_text("[]", encoding="utf-8")
    (sd / "manifest.json").write_text(
        json.dumps({"version": "1.2.3", "settings": {"x": 1}}),
        encoding="utf-8",
    )
    (sd / "local_service_pages.json").write_text("{}", encoding="utf-8")

    from app.storage_paths import push_website_volume_from_package

    r = push_website_volume_from_package(str(pkg), prefer_bundled_public=True)
    assert r.get("ok") is True
    site_dst = Path(vol) / "plugins" / "website_module" / "site_data" / "manifest.json"
    data = json.loads(site_dst.read_text(encoding="utf-8"))
    assert "version" not in data
    assert data.get("settings", {}).get("x") == 1


def test_pull_website_module_from_volume_merges_into_bundled(tmp_path, monkeypatch):
    """Volume → package: public_bundled / static_bundled / site_data receive live volume files."""
    vol = str(tmp_path / "vol_pull")
    web = Path(vol) / "plugins" / "website_module"
    (web / "public_templates").mkdir(parents=True)
    (web / "public_templates" / "index.html").write_text("<h1>FROM_VOLUME</h1>", encoding="utf-8")
    (web / "static").mkdir(parents=True)
    (web / "static" / "theme.css").write_text("/*vol*/", encoding="utf-8")
    (web / "site_data").mkdir(parents=True)
    (web / "site_data" / "pages.json").write_text("[1,2]", encoding="utf-8")
    (web / "site_data" / "data").mkdir(parents=True)
    (web / "site_data" / "data" / "x.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr("app.storage_paths.get_persistent_data_root", lambda: vol)

    pkg = tmp_path / "app_pull"
    wm = pkg / "plugins" / "website_module"
    (wm / "templates" / "public_bundled").mkdir(parents=True)
    (wm / "templates" / "public_bundled" / "index.html").write_text("old", encoding="utf-8")
    (wm / "static_bundled").mkdir(parents=True)
    (wm / "static_bundled" / "theme.css").write_text("oldcss", encoding="utf-8")
    (wm / "site_data").mkdir(parents=True)
    (wm / "site_data" / "pages.json").write_text("[]", encoding="utf-8")

    from app.storage_paths import pull_website_module_from_volume

    r = pull_website_module_from_volume(str(pkg))
    assert r.get("ok") is True
    assert r.get("public_ok") is True
    assert r.get("static_ok") is True
    assert r.get("site_data_ok") is True
    assert (wm / "templates" / "public_bundled" / "index.html").read_text(encoding="utf-8") == "<h1>FROM_VOLUME</h1>"
    assert (wm / "static_bundled" / "theme.css").read_text(encoding="utf-8") == "/*vol*/"
    assert (wm / "site_data" / "pages.json").read_text(encoding="utf-8") == "[1,2]"
    assert (wm / "site_data" / "data" / "x.json").is_file()


def test_website_volume_data_root_looks_customized(tmp_path):
    from app.storage_paths import website_volume_data_root_looks_customized

    dr = str(tmp_path / "cust")
    assert website_volume_data_root_looks_customized(dr) is False

    site = Path(dr) / "plugins" / "website_module" / "site_data"
    site.mkdir(parents=True)
    (site / "pages.json").write_text('[{"title":"X","route":"/about"}]', encoding="utf-8")
    assert website_volume_data_root_looks_customized(dr) is True
