#!/usr/bin/env bash
# Run from anywhere; changes to repo root.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
python scripts/verify_cura_mi_schema.py
echo ""
echo "Sparrow Cura/MI DB preflight passed."
echo "If you changed schema: restart the app process so workers pick up code + DB."
