#!/usr/bin/env python3
"""Simple smoke test script for core endpoints.

Usage:
  Set `BASE_URL` environment variable (default http://localhost:5000) and run:
    python tests/smoke_test.py

It will check:
  - Root admin plugin landing
  - Socket.IO client asset
  - Jobs API
  - CAD dashboard page

This is a lightweight check intended for CI smoke runs (no auth performed).
"""
import os
import sys
import requests


def check(path, base):
    url = base.rstrip('/') + path
    try:
        r = requests.get(url, timeout=6)
        print(f"GET {path} -> {r.status_code}")
        return r.status_code
    except Exception as e:
        print(f"GET {path} -> FAILED: {e}")
        return None


def main():
    base = os.environ.get('BASE_URL', 'http://localhost:5000')
    print(f"Running smoke tests against: {base}")

    paths = [
        '/plugin/ventus_response_module/',
        '/socket.io/socket.io.js',
        '/plugin/ventus_response_module/jobs',
        '/plugin/ventus_response_module/cad'
    ]

    results = {}
    for p in paths:
        results[p] = check(p, base)

    failures = [p for p, s in results.items() if s is None or (
        isinstance(s, int) and s >= 500)]
    if failures:
        print('\nSmoke test: FAILURES detected')
        for f in failures:
            print(f" - {f}: {results[f]}")
        sys.exit(2)
    print('\nSmoke test: OK (no server errors detected)')


if __name__ == '__main__':
    main()
