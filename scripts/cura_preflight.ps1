# Sparrow: verify Cura / MI database tables before deploy or tester handoff.
# Run from repo root or anywhere:  .\scripts\cura_preflight.ps1
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot
python scripts/verify_cura_mi_schema.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host ""
Write-Host "Sparrow Cura/MI DB preflight passed."
Write-Host "If you changed schema: restart the app process so workers pick up code + DB."
