"""
Persistent file storage for container deployments (e.g. Railway).

When a persistent root env var is set, upload/data directories under the app
image are replaced with symlinks into the volume so redeploys keep logos, HR
files, training assets, Cura uploads, inventory data, website static uploads,
CRM medical event plan PDFs (``app/static/uploads/crm_event_plans/``; reads also check repo-root ``static/``), etc.

Website **pages / builder JSON / user manifest** live under
``{data_root}/plugins/website_module/site_data/`` (see ``resolved_*`` helpers below).

**Public HTML** (Page Manager Jinja pages: ``index.html``, ``*.html``, ``base.html``) is stored under
``plugins/website_module/templates/public`` in the image; on POSIX + persistent root that directory
is replaced with a symlink to ``{data_root}/plugins/website_module/public_templates/`` so custom
templates survive redeploys (same idea as ``website_module/static``).

Local dev (no env): no-op.

**Primary env (Railway):** ``RAILWAY_VOLUME_MOUNT_PATH`` — set to your volume mount path,
typically ``/volume``. **Also accepted:** ``SPARROW_DATA_ROOT``, ``PERSISTENT_DATA_ROOT``,
``RAILWAY_DATA_ROOT``. On Railway, if none are set but ``/volume`` exists, it is used
automatically (standard Railway mount).

**Emergency / recovery:** Use **Website admin → Volume & templates** to push
``templates/public_bundled`` (and plugin ``pages.json`` / ``manifest.json``) onto the volume, or
download a ZIP of the live volume copy. Or set ``SPARROW_WEBSITE_VOLUME_SYNC_FROM_IMAGE=1`` for
one deploy (then remove) so startup runs the same push before symlinks bind.

**Bundled public HTML:** Git tracks only ``templates/public/``. The Docker image build copies it to
``templates/public_bundled/`` (ignored in Git) so the admin “push to volume” action still has real
files after ``templates/public`` is symlinked to the volume. Edit public templates only under
``templates/public``.

**Core ERP manifest** (company name, theme, branding paths in manifest, etc.): on POSIX with a
persistent root, ``app/config/manifest.json`` is replaced with a symlink to
``{data_root}/config/core_manifest.json`` so Core settings survive image redeploys (alongside
``config/.env.smtp`` for email). First boot seeds the volume from the image file if present, else
defaults. An existing file on the volume is never overwritten by the image.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
import zipfile
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.branding_utils import merge_site_settings_defaults

# Railway’s usual volume mount path when you add a volume to a service.
_RAILWAY_DEFAULT_VOLUME_MOUNT = "/volume"


def persistent_core_manifest_volume_path(data_root: str) -> str:
    """Absolute path to durable core manifest JSON on the volume."""
    return os.path.join(os.path.abspath(data_root), "config", "core_manifest.json")


def default_core_manifest_dict() -> Dict[str, Any]:
    """Default Core module manifest (single source for seed / first boot)."""
    return {
        "name": "Core Module",
        "system_name": "Sparrow_ERP_Core",
        "version": "1.0.0",
        "theme_settings": {
            "theme": "default",
            "custom_css_path": "",
            "dashboard_background_mode": "slideshow",
            "dashboard_background_color": "#1e293b",
            "dashboard_background_image_path": "",
        },
        "site_settings": merge_site_settings_defaults({}),
        "ai_settings": {
            "chat_model": "",
        },
        "organization_profile": {
            "industries": ["medical"],
        },
    }


def write_default_core_manifest_file(path: str) -> None:
    """Write ``default_core_manifest_dict()`` to ``path`` (parent dirs created)."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(default_core_manifest_dict(), f, indent=4)


def _symlink_core_manifest_into_volume(app_pkg_dir: str, data_root: str) -> None:
    """
    Point ``app/config/manifest.json`` at ``{data_root}/config/core_manifest.json``.

    Does not overwrite an existing volume file. Seeds the volume from the image's manifest only
    when the volume file is missing.
    """
    app_pkg_dir = os.path.abspath(app_pkg_dir)
    link_path = os.path.join(app_pkg_dir, "config", "manifest.json")
    abs_target = os.path.abspath(persistent_core_manifest_volume_path(data_root))
    vol_dir = os.path.dirname(abs_target)
    os.makedirs(vol_dir, exist_ok=True)
    os.makedirs(os.path.dirname(link_path), exist_ok=True)

    if os.path.isdir(link_path) and not os.path.islink(link_path):
        print(
            "[sparrow] config/manifest.json is a directory; skip core manifest volume bind.",
            file=sys.stderr,
        )
        return

    if os.path.islink(link_path):
        try:
            if os.path.realpath(link_path) == os.path.realpath(abs_target):
                return
        except OSError:
            pass
        os.unlink(link_path)

    if not os.path.isfile(abs_target):
        if os.path.isfile(link_path) and not os.path.islink(link_path):
            try:
                shutil.copy2(link_path, abs_target)
            except OSError as e:
                print(
                    f"[sparrow] core manifest seed copy failed: {e}",
                    file=sys.stderr,
                )
                write_default_core_manifest_file(abs_target)
        else:
            write_default_core_manifest_file(abs_target)

    if os.path.isfile(link_path) and not os.path.islink(link_path):
        os.remove(link_path)
    elif os.path.lexists(link_path):
        try:
            os.unlink(link_path)
        except OSError:
            pass

    try:
        os.symlink(abs_target, link_path)
        print(
            f"[sparrow] Core settings manifest on volume: {abs_target}",
            file=sys.stderr,
        )
    except OSError as e:
        print(f"[sparrow] core manifest symlink failed: {e}", file=sys.stderr)


def get_persistent_smtp_env_path() -> Optional[str]:
    """
    Path for SMTP-related variables on the persistent volume (Railway, etc.).

    File: ``{data_root}/config/.env.smtp`` — loaded after ``app/config/.env`` so saved
    Core email settings survive container redeploys. Only ``SMTP_*`` keys should appear
    here (written by ``update_env_var`` when a volume root is configured).
    """
    root = get_persistent_data_root()
    if not root:
        return None
    return os.path.join(os.path.abspath(root), "config", ".env.smtp")


def load_volume_smtp_into_os_environ(*, skip_keys: Optional[Iterable[str]] = None) -> None:
    """
    Apply ``SMTP_*`` from ``.env.smtp`` on the volume.

    Keys listed in ``skip_keys`` should be non-empty platform overrides (e.g. Railway Variables
    set before ``load_dotenv``). Those values win. Keys that exist in the environment but are
    empty must **not** be skipped—otherwise placeholder dashboard vars block loading from disk.
    All other ``SMTP_*`` entries from the file override values from the image ``app/config/.env``.
    """
    path = get_persistent_smtp_env_path()
    if not path or not os.path.isfile(path):
        return
    skip = frozenset(skip_keys) if skip_keys else frozenset()
    try:
        from dotenv import dotenv_values
    except ImportError:
        return
    try:
        for k, v in (dotenv_values(path) or {}).items():
            if not k or v is None:
                continue
            ks = str(k).strip()
            if not ks.startswith("SMTP_"):
                continue
            if ks in skip:
                continue
            os.environ[ks] = str(v).strip()
    except OSError:
        pass


def get_persistent_data_root() -> Optional[str]:
    for key in (
        "RAILWAY_VOLUME_MOUNT_PATH",
        "SPARROW_DATA_ROOT",
        "PERSISTENT_DATA_ROOT",
        "RAILWAY_DATA_ROOT",
    ):
        root = (os.environ.get(key) or "").strip()
        if root:
            return root
    # Same convention as core + HR docs: Railway volume at /volume, no extra env needed.
    if (os.environ.get("RAILWAY_ENVIRONMENT") or "").strip() and os.path.isdir(
        _RAILWAY_DEFAULT_VOLUME_MOUNT
    ):
        return _RAILWAY_DEFAULT_VOLUME_MOUNT
    return None


def _is_posix_like() -> bool:
    return os.name != "nt"


def _merge_tree_into(src_dir: str, dst_dir: str) -> None:
    """Copy files/dirs from src into dst without removing dst contents."""
    os.makedirs(dst_dir, exist_ok=True)
    for name in os.listdir(src_dir):
        s = os.path.join(src_dir, name)
        d = os.path.join(dst_dir, name)
        if os.path.isdir(s):
            shutil.copytree(s, d, dirs_exist_ok=True)
        else:
            if not os.path.exists(d):
                shutil.copy2(s, d)


def _merge_tree_overwrite(src_dir: str, dst_dir: str) -> None:
    """
    Copy all regular files under src_dir into dst_dir with the same relative paths,
    overwriting existing files. Directories are created as needed. Skips symlinked src_dir.
    """
    src_dir = os.path.abspath(src_dir)
    dst_dir = os.path.abspath(dst_dir)
    if not os.path.isdir(src_dir) or os.path.islink(src_dir):
        return
    for root, _dirs, files in os.walk(src_dir):
        rel = os.path.relpath(root, src_dir)
        dest_root = dst_dir if rel in (".", "") else os.path.join(dst_dir, rel)
        os.makedirs(dest_root, exist_ok=True)
        for name in files:
            sfile = os.path.join(root, name)
            if os.path.isfile(sfile) and not os.path.islink(sfile):
                shutil.copy2(sfile, os.path.join(dest_root, name))


def website_volume_sync_from_image_requested() -> bool:
    """True when ops set ``SPARROW_WEBSITE_VOLUME_SYNC_FROM_IMAGE`` for a one-shot volume repair."""
    v = (os.environ.get("SPARROW_WEBSITE_VOLUME_SYNC_FROM_IMAGE") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _website_module_dir(app_pkg_dir: str) -> str:
    return os.path.join(os.path.abspath(app_pkg_dir), "plugins", "website_module")


def _resolve_website_public_push_source(module_dir: str, prefer_bundled: bool) -> Optional[str]:
    """
    Directory of HTML to push onto the volume: real dir only (not a symlink).

    Prefer ``templates/public_bundled`` when ``prefer_bundled`` so pushes work after
    ``templates/public`` is already symlinked to the volume.
    """
    wm = os.path.abspath(module_dir)
    bundled = os.path.join(wm, "templates", "public_bundled")
    pub = os.path.join(wm, "templates", "public")
    candidates = (bundled, pub) if prefer_bundled else (pub, bundled)
    for c in candidates:
        if not os.path.isdir(c) or os.path.islink(c):
            continue
        try:
            names = os.listdir(c)
        except OSError:
            continue
        for n in names:
            p = os.path.join(c, n)
            if os.path.isfile(p) and not os.path.islink(p):
                return c
    return None


def push_website_volume_from_package(
    app_pkg_dir: str, *, prefer_bundled_public: bool
) -> Dict[str, Any]:
    """
    Overwrite volume ``public_templates`` and ``site_data/{pages.json,manifest.json,local_service_pages.json}`` from the
    plugin package on disk (image / bind mount).

    Returns ``{"ok": bool, "message": str, "log": [str, ...]}``.
    """
    log: List[str] = []
    root = get_persistent_data_root()
    if not root:
        return {
            "ok": False,
            "message": "No persistent volume is configured (set RAILWAY_VOLUME_MOUNT_PATH or similar).",
            "log": [],
        }
    app_pkg_dir = os.path.abspath(app_pkg_dir)
    dr = os.path.abspath(root)
    wm = _website_module_dir(app_pkg_dir)
    public_dst = os.path.join(dr, "plugins", "website_module", "public_templates")
    site_dst = os.path.join(dr, "plugins", "website_module", "site_data")

    src = _resolve_website_public_push_source(wm, prefer_bundled_public)
    public_ok = False
    if src:
        try:
            os.makedirs(public_dst, exist_ok=True)
            _merge_tree_overwrite(src, public_dst)
            public_ok = True
            log.append(f"Public HTML copied from {src} → volume public_templates.")
        except OSError as e:
            log.append(f"Public HTML copy failed: {e}")
    else:
        log.append(
            "Public HTML skipped (no templates/public_bundled or non-symlink templates/public with files)."
        )

    json_ok = 0
    os.makedirs(site_dst, exist_ok=True)
    for fname in ("pages.json", "manifest.json", "local_service_pages.json"):
        dest = os.path.join(site_dst, fname)
        srcf = os.path.join(wm, "site_data", fname)
        if not os.path.isfile(srcf) or os.path.islink(srcf):
            srcf = os.path.join(wm, fname)
        if os.path.isfile(srcf) and not os.path.islink(srcf):
            try:
                shutil.copy2(srcf, dest)
                json_ok += 1
                log.append(f"Copied {fname} to site_data on volume.")
            except OSError as e:
                log.append(f"Copy {fname} failed: {e}")

    ok = public_ok or json_ok > 0
    if not ok:
        msg = "Nothing was copied. Add templates/public_bundled or plugin pages.json/manifest.json."
    else:
        msg = " ".join(log)
    return {
        "ok": ok,
        "message": msg,
        "log": log,
        "public_ok": public_ok,
        "json_ok": json_ok,
    }


def build_website_volume_backup_zip(app_pkg_dir: str) -> Tuple[Optional[bytes], str]:
    """
    ZIP ``public_templates`` and ``site_data`` from the volume for download / Git backup.

    Returns ``(zip_bytes, error_message)``; ``error_message`` empty on success.
    """
    root = get_persistent_data_root()
    if not root:
        return None, "No persistent volume is configured."
    dr = os.path.abspath(root)
    pairs = [
        (
            os.path.join(dr, "plugins", "website_module", "public_templates"),
            "public_templates",
        ),
        (
            os.path.join(dr, "plugins", "website_module", "site_data"),
            "site_data",
        ),
    ]
    buf = io.BytesIO()
    added = 0
    try:
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for base, arc_prefix in pairs:
                if not os.path.isdir(base):
                    continue
                for folder, _subdirs, files in os.walk(base):
                    for fn in files:
                        path = os.path.join(folder, fn)
                        if not os.path.isfile(path):
                            continue
                        rel = os.path.relpath(path, base).replace("\\", "/")
                        zf.write(path, f"{arc_prefix}/{rel}")
                        added += 1
    except OSError as e:
        return None, str(e)
    if added == 0:
        return None, "No files found on the volume to include in the backup."
    return buf.getvalue(), ""


def sync_website_module_volume_from_image(app_pkg_dir: str, data_root: str) -> None:
    """
    Startup hook: if ``SPARROW_WEBSITE_VOLUME_SYNC_FROM_IMAGE`` is set, push package files to volume.

    Uses ``prefer_bundled_public=False`` first so a fresh container’s real ``templates/public``
    wins; ``public_bundled`` is the fallback order inside ``push_website_volume_from_package``.
    """
    if not website_volume_sync_from_image_requested():
        return
    _ = data_root  # volume root is resolved inside push via get_persistent_data_root()
    result = push_website_volume_from_package(
        app_pkg_dir, prefer_bundled_public=False
    )
    for line in result.get("log") or []:
        print(f"[sparrow] SPARROW_WEBSITE_VOLUME_SYNC_FROM_IMAGE: {line}", file=sys.stderr)
    if not result.get("ok"):
        print(
            f"[sparrow] SPARROW_WEBSITE_VOLUME_SYNC_FROM_IMAGE: {result.get('message', 'failed')}",
            file=sys.stderr,
        )
    else:
        print(
            "[sparrow] Remove SPARROW_WEBSITE_VOLUME_SYNC_FROM_IMAGE from the environment after this deploy.",
            file=sys.stderr,
        )


def _replace_with_symlink(link_path: str, target_path: str) -> None:
    """
    Ensure link_path is a symlink to target_path (directory).
    If link_path is a non-empty directory, merge into target then replace with symlink.
    """
    os.makedirs(os.path.dirname(link_path) or ".", exist_ok=True)
    os.makedirs(target_path, exist_ok=True)

    if os.path.islink(link_path):
        try:
            if os.path.realpath(link_path) == os.path.realpath(target_path):
                return
        except OSError:
            pass
        os.unlink(link_path)
    elif os.path.isdir(link_path):
        if os.listdir(link_path):
            _merge_tree_into(link_path, target_path)
        shutil.rmtree(link_path)
    elif os.path.lexists(link_path):
        os.remove(link_path)

    # Absolute target: reliable inside Docker/Railway regardless of cwd.
    os.symlink(os.path.abspath(target_path), link_path, target_is_directory=True)


def _pairs_for_app(app_pkg_dir: str, data_root: str) -> List[Tuple[str, str]]:
    """(path_inside_image, path_on_volume) for directory symlinks."""
    app_pkg_dir = os.path.abspath(app_pkg_dir)
    plugins = os.path.join(app_pkg_dir, "plugins")
    dr = os.path.abspath(data_root)
    pairs: List[Tuple[str, str]] = [
        (
            os.path.join(app_pkg_dir, "static", "uploads"),
            os.path.join(dr, "app_static", "uploads"),
        ),
        (
            os.path.join(plugins, "inventory_control", "data"),
            os.path.join(dr, "plugins", "inventory_control", "data"),
        ),
        (
            os.path.join(plugins, "event_manager_module", "static"),
            os.path.join(dr, "plugins", "event_manager_module", "static"),
        ),
        (
            os.path.join(plugins, "website_module", "static"),
            os.path.join(dr, "plugins", "website_module", "static"),
        ),
        (
            os.path.join(plugins, "website_module", "templates", "public"),
            os.path.join(dr, "plugins", "website_module", "public_templates"),
        ),
        (
            os.path.join(plugins, "medical_records_module", "data"),
            os.path.join(dr, "plugins", "medical_records_module", "data"),
        ),
    ]
    return pairs


def bind_persistent_directories(app_pkg_dir: str) -> None:
    """
    Symlink known upload/data dirs (and website public Jinja templates) into the volume.

    app_pkg_dir: absolute path to the Python package named ``app``
                 (same as Flask ``app.root_path`` for the admin app).
    """
    data_root = get_persistent_data_root()
    if not data_root:
        _warn_if_production_without_persistent_root()
        return
    if not _is_posix_like():
        print(
            "[sparrow] Persistent data root is set but symlinks are skipped on Windows; "
            "use WSL or Docker for volume parity.",
            file=sys.stderr,
        )
        return

    try:
        sync_website_module_volume_from_image(app_pkg_dir, data_root)
        _symlink_core_manifest_into_volume(app_pkg_dir, data_root)
        for link_path, target_path in _pairs_for_app(app_pkg_dir, data_root):
            if not os.path.isdir(os.path.dirname(link_path)):
                # e.g. static/ missing — unusual; skip safely
                continue
            _replace_with_symlink(link_path, target_path)
        print(f"[sparrow] Persistent storage bound: {os.path.abspath(data_root)}", file=sys.stderr)
    except OSError as e:
        print(f"[sparrow] Persistent storage bind failed: {e}", file=sys.stderr)


def _warn_if_production_without_persistent_root() -> None:
    """Log once per process if production-like deploy has no volume root (uploads are ephemeral)."""
    if getattr(_warn_if_production_without_persistent_root, "_done", False):
        return
    rail = (os.environ.get("RAILWAY_ENVIRONMENT") or "").strip().lower()
    flask_env = (os.environ.get("FLASK_ENV") or "").strip().lower()
    if rail not in ("production", "preview") and flask_env != "production":
        return
    print(
        "[sparrow] WARNING: No persistent storage root resolved. Set "
        "RAILWAY_VOLUME_MOUNT_PATH=/volume (or SPARROW_DATA_ROOT to the same path) "
        "to match your Railway volume mount, or ensure the volume is mounted at "
        f"{_RAILWAY_DEFAULT_VOLUME_MOUNT!r} so Sparrow can bind automatically. "
        "Otherwise logos, uploads, core manifest (company/theme), and website public HTML "
        "(Page Manager) live in the image and are lost on redeploy.",
        file=sys.stderr,
    )
    setattr(_warn_if_production_without_persistent_root, "_done", True)


def list_expected_volume_subdirs() -> Iterable[str]:
    """Document layout under the volume root (for ops / README)."""
    return (
        "config/",
        "app_static/uploads/",
        "plugins/inventory_control/data/",
        "plugins/event_manager_module/static/",
        "plugins/website_module/static/",
        "plugins/website_module/public_templates/",
        "plugins/website_module/site_data/",
        "plugins/medical_records_module/data/",
    )


# ---------------------------------------------------------------------------
# Website module durable JSON / builder data (Railway volume)
# ---------------------------------------------------------------------------


def resolved_website_module_site_data_dir(module_dir: str) -> str:
    """
    Directory for tenant-owned website files: pages.json, builder data/, user manifest overlay.

    With a persistent root: ``{data_root}/plugins/website_module/site_data``.
    Local dev (no root): ``{module_dir}/site_data`` (next to the plugin package).
    """
    module_dir = os.path.abspath(module_dir)
    root = get_persistent_data_root()
    if root:
        p = os.path.join(os.path.abspath(root), "plugins", "website_module", "site_data")
        os.makedirs(p, exist_ok=True)
        return p
    p = os.path.join(module_dir, "site_data")
    os.makedirs(p, exist_ok=True)
    return p


def resolved_pages_json_path(module_dir: str) -> str:
    """Canonical ``pages.json`` path (Page Manager + public routes + builder SEO defaults)."""
    return os.path.join(resolved_website_module_site_data_dir(module_dir), "pages.json")


def resolved_local_service_pages_json_path(module_dir: str) -> str:
    """
    Optional programmatic SEO landings (``local_service_pages.json``).

    Prefers ``site_data/local_service_pages.json``; falls back to legacy plugin root file.
    """
    module_dir = os.path.abspath(module_dir)
    site_p = os.path.join(
        resolved_website_module_site_data_dir(module_dir), "local_service_pages.json"
    )
    legacy = os.path.join(module_dir, "local_service_pages.json")
    if os.path.isfile(site_p):
        return site_p
    if os.path.isfile(legacy):
        return legacy
    return site_p


def resolved_website_module_data_dir(module_dir: str) -> str:
    """
    Builder drafts (``pages/*.json``), analytics ``geo_cache.json``, contact config, etc.
    """
    p = os.path.join(resolved_website_module_site_data_dir(module_dir), "data")
    os.makedirs(p, exist_ok=True)
    return p


def resolved_website_module_user_manifest_path(module_dir: str) -> str:
    """User-edited website settings overlay (merged over bundled plugin ``manifest.json``)."""
    return os.path.join(resolved_website_module_site_data_dir(module_dir), "manifest.json")


def migrate_website_module_site_data_once(module_dir: str) -> None:
    """
    One-time copy from image paths to volume-backed ``site_data`` when the target is missing.

    Does **not** overwrite existing files on the volume (safe across redeploys).
    """
    module_dir = os.path.abspath(module_dir)
    site = resolved_website_module_site_data_dir(module_dir)
    legacy_pages = os.path.join(module_dir, "pages.json")
    target_pages = os.path.join(site, "pages.json")
    if not os.path.isfile(target_pages) and os.path.isfile(legacy_pages):
        try:
            shutil.copy2(legacy_pages, target_pages)
        except OSError as e:
            print(f"[sparrow] website_module pages.json migration copy failed: {e}", file=sys.stderr)

    legacy_lsp = os.path.join(module_dir, "local_service_pages.json")
    target_lsp = os.path.join(site, "local_service_pages.json")
    if not os.path.isfile(target_lsp) and os.path.isfile(legacy_lsp):
        try:
            shutil.copy2(legacy_lsp, target_lsp)
        except OSError as e:
            print(
                f"[sparrow] website_module local_service_pages.json migration copy failed: {e}",
                file=sys.stderr,
            )

    legacy_data = os.path.join(module_dir, "data")
    target_data = os.path.join(site, "data")
    if os.path.isdir(legacy_data) and os.path.realpath(legacy_data) != os.path.realpath(target_data):
        try:
            os.makedirs(target_data, exist_ok=True)
            if not os.listdir(target_data):
                _merge_tree_into(legacy_data, target_data)
        except OSError as e:
            print(f"[sparrow] website_module data/ migration failed: {e}", file=sys.stderr)

    legacy_manifest = os.path.join(module_dir, "manifest.json")
    target_manifest = os.path.join(site, "manifest.json")
    if not os.path.isfile(target_manifest) and os.path.isfile(legacy_manifest):
        try:
            shutil.copy2(legacy_manifest, target_manifest)
        except OSError as e:
            print(f"[sparrow] website_module manifest migration copy failed: {e}", file=sys.stderr)
