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

**Emergency / recovery:** Use **Website admin → Volume & deploy** to push
``templates/public_bundled`` and ``static_bundled`` onto the volume (existing ``site_data`` JSON is
retained; static merge keeps ``uploads/``, manifest-linked assets, and any volume-only files not in the package). Or download a ZIP of the live volume copy. Or set
``SPARROW_WEBSITE_VOLUME_SYNC_FROM_IMAGE=1`` for one deploy (then remove) so startup runs the same push before
symlinks bind.

**Reverse (rehydrate module from volume):** Admin **Pull volume → module** copies live
``public_templates``, ``static``, and ``site_data`` from the volume into the package tree—preferring
``templates/public_bundled`` / ``static_bundled`` when present (Docker rebuild / dev copy), else real
``templates/public`` and ``static`` when they are not already symlinks to the volume. One-shot boot:
``SPARROW_WEBSITE_PULL_FROM_VOLUME=1`` (remove after). Optional auto when the volume looks customised and
``SPARROW_WEBSITE_DEMO=1`` or ``WEBSITE_DEMO=1`` (or ``SPARROW_WEBSITE_AUTO_REHYDRATE=1``) is set—see
``maybe_pull_website_module_from_volume_on_boot``.

**Bundled public HTML:** Git tracks only ``templates/public/``. The Docker image build copies it to
``templates/public_bundled/`` (ignored in Git) so the admin “push to volume” action still has real
files after ``templates/public`` is symlinked to the volume. Edit public templates only under
``templates/public``.

**Bundled website static:** The image also copies ``plugins/website_module/static`` to
``static_bundled/`` so pushes can refresh volume ``plugins/website_module/static`` (used for media
URLs) after that directory is symlinked to the volume.

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
import tempfile
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


def _replace_directory_tree(src_dir: str, dst_dir: str) -> None:
    """
    Make ``dst_dir`` an exact copy of ``src_dir``: remove destination if present, then copytree.

    ``src_dir`` must be a real directory (not a symlink). Symlinks inside ``src_dir`` are followed
    so files are materialized on the destination (avoids Docker ``cp -a`` symlink artifacts blocking
    updates). Used for volume push of ``public_templates`` and website ``static``.
    """
    src_dir = os.path.abspath(src_dir)
    dst_dir = os.path.abspath(dst_dir)
    if not os.path.isdir(src_dir) or os.path.islink(src_dir):
        raise OSError(f"push source is not a real directory: {src_dir}")
    if os.path.lexists(dst_dir):
        if os.path.islink(dst_dir):
            os.unlink(dst_dir)
        elif os.path.isdir(dst_dir):
            shutil.rmtree(dst_dir)
        else:
            os.remove(dst_dir)
    parent = os.path.dirname(dst_dir)
    if parent:
        os.makedirs(parent, exist_ok=True)
    shutil.copytree(src_dir, dst_dir, symlinks=False)


def _strip_version_from_seeded_site_manifest(path: str) -> None:
    """First-time seed on volume: never persist bundled plugin ``version`` into tenant site_data."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "version" not in data:
            return
        data = dict(data)
        del data["version"]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
    except (OSError, json.JSONDecodeError, TypeError):
        pass


def _website_static_files_to_preserve_from_manifest(site_data_dir: str) -> List[str]:
    """Relative paths under website ``static/`` referenced by saved settings (excludes uploads/)."""
    rels: List[str] = []
    p = os.path.join(os.path.abspath(site_data_dir), "manifest.json")
    if not os.path.isfile(p):
        return rels
    try:
        with open(p, encoding="utf-8") as f:
            m = json.load(f)
        if not isinstance(m, dict):
            return rels
        settings = m.get("settings")
        if not isinstance(settings, dict):
            return rels
        for key in ("favicon_path", "default_og_image"):
            v = settings.get(key)
            if not v or not isinstance(v, str):
                continue
            rel = v.replace("\\", "/").strip().lstrip("/")
            if not rel or ".." in rel or rel.startswith("uploads/"):
                continue
            rels.append(rel)
    except (OSError, json.JSONDecodeError, TypeError):
        pass
    return rels


def _relpaths_of_regular_files_under(root_dir: str) -> set[str]:
    """POSIX-style ``relpath`` keys for every regular file under ``root_dir``."""
    root_dir = os.path.abspath(root_dir)
    out: set[str] = set()
    if not os.path.isdir(root_dir) or os.path.islink(root_dir):
        return out
    for walk_root, _, files in os.walk(root_dir):
        for fn in files:
            p = os.path.join(walk_root, fn)
            if os.path.isfile(p) and not os.path.islink(p):
                rel = os.path.relpath(p, root_dir).replace("\\", "/")
                out.add(rel)
    return out


def _stage_volume_only_static_files(static_dst: str, static_src: str, extra_root: str) -> None:
    """
    Stage regular files present on the volume static tree but absent from the push ``static_src``
    (excluding ``uploads/``, which is staged wholesale). Restored after replace so tenant-only
    assets are not deleted on update.
    """
    static_dst = os.path.abspath(static_dst)
    static_src = os.path.abspath(static_src)
    if not os.path.isdir(static_dst) or not os.path.isdir(static_src) or os.path.islink(static_src):
        return
    packaged = _relpaths_of_regular_files_under(static_src)
    for walk_root, _, files in os.walk(static_dst):
        for fn in files:
            abs_p = os.path.join(walk_root, fn)
            rel = os.path.relpath(abs_p, static_dst).replace("\\", "/")
            if rel.startswith("uploads/"):
                continue
            if rel in packaged:
                continue
            if os.path.isfile(abs_p) and not os.path.islink(abs_p):
                out = os.path.join(extra_root, rel)
                os.makedirs(os.path.dirname(out), exist_ok=True)
                shutil.copy2(abs_p, out)


def _stage_website_static_user_preservation(
    static_dst: str, site_data_dir: str, static_src: str
) -> Optional[str]:
    """
    Copy tenant-owned static into a temp dir before a full static tree replace:

    - whole ``uploads/`` tree
    - manifest-linked root files (favicon / default OG)
    - any other regular files on the volume not present in the new ``static_src`` package tree
    """
    static_dst = os.path.abspath(static_dst)
    if not os.path.isdir(static_dst):
        return None
    tmp = tempfile.mkdtemp(prefix="sparrow_wm_static_")
    try:
        uploads = os.path.join(static_dst, "uploads")
        if os.path.isdir(uploads):
            shutil.copytree(uploads, os.path.join(tmp, "__uploads__"), symlinks=False)
        files_dir = os.path.join(tmp, "__files__")
        seen: set[str] = set()
        for rel in _website_static_files_to_preserve_from_manifest(site_data_dir):
            if rel in seen:
                continue
            seen.add(rel)
            abs_src = os.path.join(static_dst, rel)
            if os.path.isfile(abs_src) and not os.path.islink(abs_src):
                dst_f = os.path.join(files_dir, rel)
                os.makedirs(os.path.dirname(dst_f), exist_ok=True)
                shutil.copy2(abs_src, dst_f)
        fav = os.path.join(static_dst, "favicon.ico")
        if os.path.isfile(fav) and not os.path.islink(fav) and "favicon.ico" not in seen:
            dst_f = os.path.join(files_dir, "favicon.ico")
            os.makedirs(os.path.dirname(dst_f), exist_ok=True)
            shutil.copy2(fav, dst_f)
        extra_root = os.path.join(tmp, "__extra__")
        os.makedirs(extra_root, exist_ok=True)
        _stage_volume_only_static_files(static_dst, static_src, extra_root)
        return tmp
    except OSError:
        shutil.rmtree(tmp, ignore_errors=True)
        return None


def _restore_website_static_user_preservation(stage_dir: Optional[str], static_dst: str) -> None:
    """Merge staged uploads, manifest files, and volume-only extras back after ``static`` replace."""
    if not stage_dir or not os.path.isdir(stage_dir):
        return
    static_dst = os.path.abspath(static_dst)
    try:
        os.makedirs(static_dst, exist_ok=True)
        uploads_bak = os.path.join(stage_dir, "__uploads__")
        if os.path.isdir(uploads_bak):
            dst_u = os.path.join(static_dst, "uploads")
            shutil.copytree(uploads_bak, dst_u, dirs_exist_ok=True)
        files_root = os.path.join(stage_dir, "__files__")
        if os.path.isdir(files_root):
            for root, _, names in os.walk(files_root):
                for name in names:
                    src_f = os.path.join(root, name)
                    rel = os.path.relpath(src_f, files_root)
                    dst_f = os.path.join(static_dst, rel)
                    parent = os.path.dirname(dst_f)
                    if parent:
                        os.makedirs(parent, exist_ok=True)
                    if os.path.isfile(src_f):
                        shutil.copy2(src_f, dst_f)
        extra_root = os.path.join(stage_dir, "__extra__")
        if os.path.isdir(extra_root):
            for root, _, names in os.walk(extra_root):
                for name in names:
                    src_f = os.path.join(root, name)
                    rel = os.path.relpath(src_f, extra_root)
                    dst_f = os.path.join(static_dst, rel)
                    parent = os.path.dirname(dst_f)
                    if parent:
                        os.makedirs(parent, exist_ok=True)
                    if os.path.isfile(src_f):
                        shutil.copy2(src_f, dst_f)
    finally:
        shutil.rmtree(stage_dir, ignore_errors=True)


def _replace_website_static_tree_preserving_user_content(
    static_src: str, static_dst: str, site_data_dir: str
) -> None:
    """Full replace of website static from image, keeping uploads, manifest assets, and volume-only files."""
    stage = _stage_website_static_user_preservation(static_dst, site_data_dir, static_src)
    try:
        _replace_directory_tree(static_src, static_dst)
    finally:
        _restore_website_static_user_preservation(stage, static_dst)


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


def _resolve_website_static_push_source(module_dir: str, prefer_bundled: bool) -> Optional[str]:
    """
    Directory of website static assets to push onto the volume: real dir only (not a symlink).

    Prefer ``static_bundled`` when ``prefer_bundled`` (same pattern as ``public_bundled``).
    """
    wm = os.path.abspath(module_dir)
    bundled = os.path.join(wm, "static_bundled")
    stat = os.path.join(wm, "static")
    candidates = (bundled, stat) if prefer_bundled else (stat, bundled)
    for c in candidates:
        if not os.path.isdir(c) or os.path.islink(c):
            continue
        try:
            if not os.listdir(c):
                continue
        except OSError:
            continue
        return c
    return None


def push_website_volume_from_package(
    app_pkg_dir: str, *, prefer_bundled_public: bool
) -> Dict[str, Any]:
    """
    Full replace on the volume:

    - ``public_templates`` from ``templates/public_bundled`` (or real ``templates/public`` when not
      symlinked), not merge-in-place — removes orphans from removed/renamed package paths.
    - ``plugins/website_module/static`` from ``static_bundled`` (or real ``static``): full replace,
      then merges back ``uploads/``, manifest-linked root files, and **any other regular files that
      existed on the volume but are not in the new package static tree** (tenant-only assets).
    - ``site_data/{pages.json,manifest.json,local_service_pages.json}``: copied from the package only
      when the destination file is missing (existing tenant JSON is never overwritten). New
      ``manifest.json`` seeds have top-level ``version`` stripped so updates can still bump it from
      the bundled manifest.

    ``prefer_bundled_public`` applies to **both** public and static source resolution when True.

    Returns ``ok``, ``message``, ``log``, ``public_ok``, ``static_ok``, ``json_ok`` (int count).
    """
    log: List[str] = []
    root = get_persistent_data_root()
    if not root:
        return {
            "ok": False,
            "message": "No persistent volume is configured (set RAILWAY_VOLUME_MOUNT_PATH or similar).",
            "log": [],
            "public_ok": False,
            "static_ok": False,
            "json_ok": 0,
        }
    app_pkg_dir = os.path.abspath(app_pkg_dir)
    dr = os.path.abspath(root)
    wm = _website_module_dir(app_pkg_dir)
    public_dst = os.path.join(dr, "plugins", "website_module", "public_templates")
    static_dst = os.path.join(dr, "plugins", "website_module", "static")
    site_dst = os.path.join(dr, "plugins", "website_module", "site_data")
    os.makedirs(os.path.join(dr, "plugins", "website_module"), exist_ok=True)

    prefer = prefer_bundled_public

    public_src = _resolve_website_public_push_source(wm, prefer)
    public_ok = False
    if public_src:
        try:
            _replace_directory_tree(public_src, public_dst)
            public_ok = True
            log.append(f"Public HTML replaced from {public_src} → volume public_templates.")
        except OSError as e:
            log.append(f"Public HTML replace failed: {e}")
    else:
        log.append(
            "Public HTML skipped (no templates/public_bundled or non-symlink templates/public with files)."
        )

    static_src = _resolve_website_static_push_source(wm, prefer)
    static_ok = False
    if static_src:
        try:
            _replace_website_static_tree_preserving_user_content(static_src, static_dst, site_dst)
            static_ok = True
            log.append(
                f"Website static replaced from {static_src} → volume plugins/website_module/static "
                "(uploads, manifest-linked files, and volume-only files not in the package retained)."
            )
        except OSError as e:
            log.append(f"Website static replace failed: {e}")
    else:
        log.append(
            "Website static skipped (no plugins/website_module/static_bundled or non-symlink static with files)."
        )

    json_ok = 0
    os.makedirs(site_dst, exist_ok=True)
    for fname in ("pages.json", "manifest.json", "local_service_pages.json"):
        dest = os.path.join(site_dst, fname)
        if os.path.isfile(dest) and not os.path.islink(dest):
            log.append(f"Skipped {fname} (already on volume; tenant data retained).")
            continue
        srcf = os.path.join(wm, "site_data", fname)
        if not os.path.isfile(srcf) or os.path.islink(srcf):
            srcf = os.path.join(wm, fname)
        if os.path.isfile(srcf) and not os.path.islink(srcf):
            try:
                shutil.copy2(srcf, dest)
                json_ok += 1
                log.append(f"Seeded {fname} to site_data on volume.")
                if fname == "manifest.json":
                    _strip_version_from_seeded_site_manifest(dest)
            except OSError as e:
                log.append(f"Copy {fname} failed: {e}")

    ok = bool(public_ok or static_ok or json_ok > 0)
    if not ok:
        msg = (
            "Nothing was copied. Add templates/public_bundled and static_bundled to the Docker image, "
            "or ensure plugin pages.json/manifest.json exist."
        )
    else:
        msg = " ".join(log)
    return {
        "ok": ok,
        "message": msg,
        "log": log,
        "public_ok": public_ok,
        "static_ok": static_ok,
        "json_ok": json_ok,
    }


def website_volume_pull_from_volume_requested() -> bool:
    """One-shot boot / ops: ``SPARROW_WEBSITE_PULL_FROM_VOLUME=1`` forces volume → module rehydrate."""
    v = (os.environ.get("SPARROW_WEBSITE_PULL_FROM_VOLUME") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _website_volume_demo_or_auto_rehydrate_env() -> Tuple[bool, bool]:
    """
    Returns ``(demo_like, explicit_auto_rehydrate)``.

    * ``demo_like`` — ``SPARROW_WEBSITE_DEMO`` or ``WEBSITE_DEMO`` truthy.
    * ``explicit_auto_rehydrate`` — ``SPARROW_WEBSITE_AUTO_REHYDRATE`` truthy (still requires customised volume).
    """
    demo = (os.environ.get("SPARROW_WEBSITE_DEMO") or "").strip().lower() in ("1", "true", "yes", "on")
    demo = demo or (os.environ.get("WEBSITE_DEMO") or "").strip().lower() in ("1", "true", "yes", "on")
    auto = (os.environ.get("SPARROW_WEBSITE_AUTO_REHYDRATE") or "").strip().lower() in ("1", "true", "yes", "on")
    return demo, auto


def website_volume_data_root_looks_customized(data_root: str) -> bool:
    """
    Heuristic: volume has more than a trivial single-route stub, builder/data, large index, or social URLs.

    Used to avoid auto rehydrate on empty demo volumes.
    """
    dr = os.path.abspath(data_root)
    site = os.path.join(dr, "plugins", "website_module", "site_data")
    pages_p = os.path.join(site, "pages.json")
    if os.path.isfile(pages_p):
        try:
            with open(pages_p, encoding="utf-8") as f:
                pl = json.load(f)
            if isinstance(pl, list):
                if len(pl) >= 2:
                    return True
                if len(pl) == 1 and isinstance(pl[0], dict):
                    r = str(pl[0].get("route") or "").strip()
                    if r and r != "/":
                        return True
                    t = str(pl[0].get("title") or "").strip().lower()
                    if t and t not in ("home", "welcome"):
                        return True
        except (OSError, json.JSONDecodeError, TypeError):
            pass

    mf = os.path.join(site, "manifest.json")
    if os.path.isfile(mf):
        try:
            with open(mf, encoding="utf-8") as f:
                m = json.load(f)
            settings = m.get("settings") if isinstance(m, dict) else None
            if isinstance(settings, dict):
                for key in (
                    "facebook_url",
                    "instagram_url",
                    "linkedin_url",
                    "twitter_url",
                    "youtube_url",
                    "tiktok_url",
                ):
                    u = (settings.get(key) or "").strip()
                    if len(u) > 12 and ("http" in u or "www." in u):
                        return True
        except (OSError, json.JSONDecodeError, TypeError):
            pass

    data_dir = os.path.join(site, "data")
    if os.path.isdir(data_dir):
        try:
            for _root, dirs, files in os.walk(data_dir):
                if files:
                    return True
                if dirs:
                    return True
        except OSError:
            pass

    idx = os.path.join(dr, "plugins", "website_module", "public_templates", "index.html")
    try:
        if os.path.isfile(idx) and os.path.getsize(idx) > 12000:
            return True
    except OSError:
        pass

    return False


def _website_volume_paths(data_root: str) -> Tuple[str, str, str]:
    dr = os.path.abspath(data_root)
    pub = os.path.join(dr, "plugins", "website_module", "public_templates")
    stat = os.path.join(dr, "plugins", "website_module", "static")
    site = os.path.join(dr, "plugins", "website_module", "site_data")
    return pub, stat, site


def _website_pull_dest_public(module_dir: str, vol_public: str) -> Tuple[Optional[str], str]:
    """Writable package dir for public HTML merge (not the live volume path)."""
    wm = os.path.abspath(module_dir)
    bundled = os.path.join(wm, "templates", "public_bundled")
    pub = os.path.join(wm, "templates", "public")
    vol_public = os.path.abspath(vol_public)

    if os.path.isdir(bundled) and not os.path.islink(bundled):
        return bundled, "templates/public_bundled"

    try:
        if os.path.isdir(pub) and os.path.realpath(pub) == vol_public:
            return None, "templates/public already points at volume"
    except OSError:
        pass

    if os.path.isdir(pub) and not os.path.islink(pub):
        return pub, "templates/public"

    return None, "no writable public target (symlink only or missing)"


def _website_pull_dest_static(module_dir: str, vol_static: str) -> Tuple[Optional[str], str]:
    wm = os.path.abspath(module_dir)
    bundled = os.path.join(wm, "static_bundled")
    stat = os.path.join(wm, "static")
    vol_static = os.path.abspath(vol_static)

    if os.path.isdir(bundled) and not os.path.islink(bundled):
        return bundled, "static_bundled"

    try:
        if os.path.isdir(stat) and os.path.realpath(stat) == vol_static:
            return None, "static already points at volume"
    except OSError:
        pass

    if os.path.isdir(stat) and not os.path.islink(stat):
        return stat, "plugins/website_module/static"

    return None, "no writable static target (symlink only or missing)"


def _copy_site_data_tree_merge(src_dir: str, dst_dir: str) -> None:
    """Copy ``site_data`` files from volume into package ``site_data`` (overwrite same names)."""
    if not os.path.isdir(src_dir):
        return
    dst_dir = os.path.abspath(dst_dir)
    os.makedirs(dst_dir, exist_ok=True)
    for name in os.listdir(src_dir):
        s = os.path.join(src_dir, name)
        d = os.path.join(dst_dir, name)
        if os.path.isdir(s) and not os.path.islink(s):
            shutil.copytree(s, d, dirs_exist_ok=True)
        elif os.path.isfile(s) and not os.path.islink(s):
            shutil.copy2(s, d)


def pull_website_module_from_volume(app_pkg_dir: str) -> Dict[str, Any]:
    """
    Copy live website assets **from the volume** into the **package** tree (reverse of push).

    - ``public_templates`` → ``templates/public_bundled`` if that real dir exists, else non-symlink
      ``templates/public``.
    - Volume ``static`` → ``static_bundled`` if present, else non-symlink ``static``.
    - ``site_data`` → ``plugins/website_module/site_data`` in the package (never the volume path).

    Intended after a module/image replace so Git-tracked or bundled snapshot dirs match what is live
    on the volume (social site recovery, local dev sync).
    """
    log: List[str] = []
    root = get_persistent_data_root()
    if not root:
        return {
            "ok": False,
            "message": "No persistent volume is configured.",
            "log": [],
            "public_ok": False,
            "static_ok": False,
            "site_data_ok": False,
        }
    app_pkg_dir = os.path.abspath(app_pkg_dir)
    wm = _website_module_dir(app_pkg_dir)
    vol_public, vol_static, vol_site = _website_volume_paths(root)
    pkg_site = os.path.join(wm, "site_data")

    public_ok = False
    static_ok = False
    site_data_ok = False

    if os.path.isdir(vol_public):
        dest, note = _website_pull_dest_public(wm, vol_public)
        if dest:
            try:
                _merge_tree_overwrite(vol_public, dest)
                public_ok = True
                log.append(f"Public HTML merged from volume → {note}.")
            except OSError as e:
                log.append(f"Public pull failed: {e}")
        else:
            log.append(f"Public pull skipped ({note}).")
    else:
        log.append("Volume public_templates missing; public pull skipped.")

    if os.path.isdir(vol_static):
        dest_s, note_s = _website_pull_dest_static(wm, vol_static)
        if dest_s:
            try:
                _merge_tree_overwrite(vol_static, dest_s)
                static_ok = True
                log.append(f"Website static merged from volume → {note_s}.")
            except OSError as e:
                log.append(f"Static pull failed: {e}")
        else:
            log.append(f"Static pull skipped ({note_s}).")
    else:
        log.append("Volume website static missing; static pull skipped.")

    if os.path.isdir(vol_site):
        try:
            if os.path.normcase(os.path.abspath(vol_site)) != os.path.normcase(os.path.abspath(pkg_site)):
                _copy_site_data_tree_merge(vol_site, pkg_site)
                site_data_ok = True
                log.append("site_data merged from volume → plugins/website_module/site_data.")
            else:
                log.append("site_data pull skipped (package site_data is the volume path).")
        except OSError as e:
            log.append(f"site_data pull failed: {e}")
    else:
        log.append("Volume site_data missing; site_data pull skipped.")

    ok = bool(public_ok or static_ok or site_data_ok)
    msg = " ".join(log) if log else "Nothing pulled."
    return {
        "ok": ok,
        "message": msg,
        "log": log,
        "public_ok": public_ok,
        "static_ok": static_ok,
        "site_data_ok": site_data_ok,
    }


def maybe_pull_website_module_from_volume_on_boot(app_pkg_dir: str) -> None:
    """
    Optionally rehydrate the package from the volume during ``bind_persistent_directories``.

    Runs when:

    * ``SPARROW_WEBSITE_PULL_FROM_VOLUME=1`` (one-shot; remove after deploy), or
    * Volume passes :func:`website_volume_data_root_looks_customized` **and**
      (``SPARROW_WEBSITE_DEMO`` / ``WEBSITE_DEMO`` **or** ``SPARROW_WEBSITE_AUTO_REHYDRATE``) is set.

    Logs to stderr; never raises.
    """
    dr = get_persistent_data_root()
    if not dr:
        return
    force = website_volume_pull_from_volume_requested()
    demo, explicit_auto = _website_volume_demo_or_auto_rehydrate_env()
    customized = website_volume_data_root_looks_customized(dr)
    if not force and not ((demo or explicit_auto) and customized):
        return
    try:
        result = pull_website_module_from_volume(app_pkg_dir)
    except OSError as e:
        print(f"[sparrow] website volume rehydrate failed: {e}", file=sys.stderr)
        return
    label = "SPARROW_WEBSITE_PULL_FROM_VOLUME" if force else "website auto-rehydrate"
    for line in result.get("log") or []:
        print(f"[sparrow] {label}: {line}", file=sys.stderr)
    if not result.get("ok"):
        print(
            f"[sparrow] {label}: {result.get('message', 'failed')}",
            file=sys.stderr,
        )
    if force:
        print(
            "[sparrow] Remove SPARROW_WEBSITE_PULL_FROM_VOLUME from the environment after this deploy.",
            file=sys.stderr,
        )


def build_website_volume_backup_zip(app_pkg_dir: str) -> Tuple[Optional[bytes], str]:
    """
    ZIP ``public_templates``, ``plugins/website_module/static``, and ``site_data`` from the volume
    for download / Git backup.

    Returns ``(zip_bytes, error_message)``; ``error_message`` empty on success.
    """
    _ = app_pkg_dir
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
            os.path.join(dr, "plugins", "website_module", "static"),
            "plugins/website_module/static",
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

    Uses ``prefer_bundled_public=True`` so Docker image snapshots (``public_bundled``,
    ``static_bundled``) win — same policy as the admin **Push package → volume** action.
    """
    if not website_volume_sync_from_image_requested():
        return
    _ = data_root  # volume root is resolved inside push via get_persistent_data_root()
    result = push_website_volume_from_package(
        app_pkg_dir, prefer_bundled_public=True
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

    try:
        sync_website_module_volume_from_image(app_pkg_dir, data_root)
        maybe_pull_website_module_from_volume_on_boot(app_pkg_dir)
    except OSError as e:
        print(f"[sparrow] website volume sync/rehydrate failed: {e}", file=sys.stderr)

    if not _is_posix_like():
        print(
            "[sparrow] Persistent data root is set but symlinks are skipped on Windows; "
            "use WSL or Docker for volume parity. (Volume sync / rehydrate still ran if applicable.)",
            file=sys.stderr,
        )
        return

    try:
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
            _strip_version_from_seeded_site_manifest(target_manifest)
        except OSError as e:
            print(f"[sparrow] website_module manifest migration copy failed: {e}", file=sys.stderr)
