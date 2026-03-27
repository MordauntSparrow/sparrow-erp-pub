import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Directories to skip when scanning (avoid venv, caches, etc.)
SCAN_EXCLUDE_DIRS = {"__pycache__", "venv", ".venv", "env", ".env", "node_modules", ".git", "backups", "updates"}


def scan_imports(project_root):
    """
    Scan all Python files under project_root for import statements.
    Includes app/, plugins/, core/, tests/, scripts/, and root-level .py so requirements.txt doesn't miss deps.
    Skips SCAN_EXCLUDE_DIRS to avoid pulling in venv/cache packages.
    """
    imports = set()
    root = Path(project_root).resolve()
    print(f"Scanning Python files under {root} (app, plugins, core, tests, scripts, etc.)...")

    for py_file in root.rglob("*.py"):
        # Skip paths that contain any excluded directory
        try:
            parts = py_file.relative_to(root).parts
        except ValueError:
            continue
        if any(p in SCAN_EXCLUDE_DIRS for p in parts):
            continue

        print(f"Scanning file: {py_file}")
        try:
            with open(py_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("import ") or line.startswith("from "):
                        parts = line.split()
                        if len(parts) < 2:
                            continue
                        # Get the root module (e.g., "requests" from "import requests" or "from requests import ...")
                        root_import = parts[1].split(".")[0]
                        imports.add(root_import)
        except Exception as e:
            print(f"Error reading file {py_file}: {e}")
    return imports


# Packages that must not be installed by the dependency handler (broken/wrong names or use different install name).
BLOCKLIST_PACKAGES = {"mysql", "mysqlclient", "dotenv"}


def install_imports(imports):
    """Attempt to pip install all detected imports, ignoring stdlib and local modules."""
    if hasattr(sys, "stdlib_module_names"):
        stdlib_modules = sys.stdlib_module_names
    else:
        stdlib_modules = {
            "os", "sys", "json", "shutil", "zipfile", "subprocess",
            "importlib", "threading", "pathlib", "datetime", "re",
            "uuid", "time", "email", "functools", "traceback", "typing", "urllib"
        }

    for imp in imports:
        # Skip stdlib
        if imp in stdlib_modules:
            print(f"Skipping standard library module: {imp}")
            continue

        # Skip local project modules
        if imp.startswith("app") or imp in {"plugins"}:
            print(f"Skipping local project module: {imp}")
            continue

        # Skip blocklisted packages (wrong pip name or should not be auto-installed)
        if imp in BLOCKLIST_PACKAGES:
            print(f"Skipping blocklisted package: {imp}")
            continue

        try:
            print(f"Attempting to install: {imp}")
            subprocess.check_call([
                sys.executable, "-m", "pip", "install",
                "--quiet", "--disable-pip-version-check", imp
            ])
        except subprocess.CalledProcessError as e:
            print(f"Failed to install: {imp}. Error: {e}")


def generate_requirements(file_path="requirements.txt"):
    """Generate a requirements.txt file dynamically by scanning imports."""
    # Project root = parent of app/ (so we scan app/, app/plugins/, app/core/, tests/, scripts/, run.py, etc.)
    project_root = Path(__file__).resolve().parent.parent
    imports = scan_imports(project_root)
    # Install all detected (non-stdlib) imports.
    install_imports(imports)
    print(f"Generating {file_path} from installed packages...")
    try:
        with open(file_path, "w") as f:
            subprocess.run([
                sys.executable, "-m", "pip", "freeze", "--disable-pip-version-check"
            ], stdout=f, check=True)
        print(f"requirements.txt updated at {file_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to generate requirements.txt: {e}")


def restart_application():
    """Restart the application."""
    print("Restarting application to apply changes...")
    os.execv(sys.executable, [sys.executable] + sys.argv)


if __name__ == "__main__":
    requirements_path = "requirements.txt"
    print("Scanning Python files for imports and updating requirements.txt...")
    generate_requirements(requirements_path)
    print("Launching the application...")
