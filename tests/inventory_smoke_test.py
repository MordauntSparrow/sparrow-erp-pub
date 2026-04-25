#!/usr/bin/env python3
"""Smoke tests for Inventory Control plugin.

Usage:
  Set BASE_URL (default http://localhost:5000) and run:
    python tests/inventory_smoke_test.py

Checks that inventory_control plugin endpoints respond (no 5xx).
"""
import os
import sys
import requests


def check(path, base, method="GET"):
    url = base.rstrip("/") + path
    try:
        r = requests.request(method, url, timeout=6, allow_redirects=False)
        print(f"{method} {path} -> {r.status_code}")
        return r.status_code
    except Exception as e:
        print(f"{method} {path} -> FAILED: {e}")
        return None


def main():
    base = os.environ.get("BASE_URL", "http://localhost:5000")
    print(f"Running inventory smoke tests against: {base}")

    paths = [
        "/plugin/inventory_control/",
        "/plugin/inventory_control/items",
        "/plugin/inventory_control/api/health",
        "/plugin/inventory_control/api/items",
        "/plugin/inventory_control/api/locations",
        "/plugin/inventory_control/api/dashboard",
    ]

    results = {}
    for p in paths:
        results[p] = check(p, base)

    failures = [
        p for p, s in results.items()
        if s is None or (isinstance(s, int) and s >= 500)
    ]
    if failures:
        print("\nInventory smoke test: FAILURES detected")
        for f in failures:
            print(f"  - {f}: {results[f]}")
        sys.exit(2)
    print("\nInventory smoke test: OK (no server errors)")


if __name__ == "__main__":
    main()
