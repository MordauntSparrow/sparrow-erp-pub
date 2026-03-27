"""
Persistent file storage for container deployments (e.g. Railway).

When SPARROW_DATA_ROOT or RAILWAY_VOLUME_MOUNT_PATH is set, upload/data
directories under the app image are replaced with symlinks into the volume
so redeploys keep HR, training, compliance, inventory, etc. files.

Local dev (no env): no-op.

Override: set SPARROW_DATA_ROOT explicitly; otherwise Railway's volume mount
path is used when present.
"""

from __future__ import annotations

import os
import shutil
import sys
from typing import Iterable, List, Optional, Tuple


def get_persistent_data_root() -> Optional[str]:
    root = (os.environ.get("SPARROW_DATA_ROOT") or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH") or "").strip()
    return root or None


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
    ]
    return pairs


def bind_persistent_directories(app_pkg_dir: str) -> None:
    """
    Symlink known upload/data dirs into the persistent volume.

    app_pkg_dir: absolute path to the Python package named ``app``
                 (same as Flask ``app.root_path`` for the admin app).
    """
    data_root = get_persistent_data_root()
    if not data_root:
        return
    if not _is_posix_like():
        print(
            "[sparrow] SPARROW_DATA_ROOT / RAILWAY_VOLUME_MOUNT_PATH is set but symlinks are "
            "skipped on Windows; use WSL or Docker for volume parity.",
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


def list_expected_volume_subdirs() -> Iterable[str]:
    """Document layout under the volume root (for ops / README)."""
    return (
        "app_static/uploads/",
        "plugins/inventory_control/data/",
        "plugins/event_manager_module/static/",
        "plugins/website_module/static/",
    )
