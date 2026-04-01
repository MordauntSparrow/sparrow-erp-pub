"""
Persistent file storage for container deployments (e.g. Railway).

When a persistent root env var is set, upload/data directories under the app
image are replaced with symlinks into the volume so redeploys keep logos, HR
files, training assets, Cura uploads, inventory data, website static uploads, etc.

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
"""

from __future__ import annotations

import os
import shutil
import sys
from typing import Iterable, List, Optional, Tuple

# Railway’s usual volume mount path when you add a volume to a service.
_RAILWAY_DEFAULT_VOLUME_MOUNT = "/volume"


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
        "Otherwise logos, uploads, and website public HTML (Page Manager) live in the image and are lost on redeploy.",
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
