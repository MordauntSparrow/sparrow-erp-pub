"""SMTP env on persistent volume: load precedence and update_env_var target path."""

import os

import pytest


def test_load_volume_smtp_respects_skip_keys(monkeypatch, tmp_path):
    from app.storage_paths import load_volume_smtp_into_os_environ

    f = tmp_path / ".env.smtp"
    f.write_text("SMTP_USERNAME=vol_user\nSMTP_HOST=vol.host\n", encoding="utf-8")
    monkeypatch.setattr(
        "app.storage_paths.get_persistent_smtp_env_path", lambda: str(f)
    )
    monkeypatch.delenv("SMTP_HOST", raising=False)
    monkeypatch.setenv("SMTP_USERNAME", "railway_user")

    skip = {
        k
        for k in os.environ
        if k.startswith("SMTP_") and str(os.environ.get(k, "") or "").strip()
    }
    load_volume_smtp_into_os_environ(skip_keys=skip)

    assert os.environ.get("SMTP_USERNAME") == "railway_user"
    assert os.environ.get("SMTP_HOST") == "vol.host"


def test_update_env_var_smtp_writes_volume_file(monkeypatch, tmp_path):
    root = tmp_path / "vol"
    (root / "config").mkdir(parents=True)
    monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(root))
    monkeypatch.delenv("SMTP_HOST", raising=False)

    from app.create_app import update_env_var

    update_env_var("SMTP_HOST", "smtp.example.com")

    smtp_path = root / "config" / ".env.smtp"
    assert smtp_path.is_file()
    text = smtp_path.read_text(encoding="utf-8")
    assert "SMTP_HOST" in text and "smtp.example.com" in text
    assert os.environ.get("SMTP_HOST") == "smtp.example.com"


def test_load_volume_smtp_empty_platform_var_does_not_block_file(monkeypatch, tmp_path):
    """Railway-style empty SMTP_* placeholders must not prevent loading from volume."""
    from app.storage_paths import load_volume_smtp_into_os_environ

    f = tmp_path / ".env.smtp"
    f.write_text("SMTP_USERNAME=vol_user\nSMTP_HOST=vol.host\n", encoding="utf-8")
    monkeypatch.setattr(
        "app.storage_paths.get_persistent_smtp_env_path", lambda: str(f)
    )
    monkeypatch.setenv("SMTP_USERNAME", "")
    monkeypatch.delenv("SMTP_HOST", raising=False)

    skip = {
        k
        for k in os.environ
        if k.startswith("SMTP_") and str(os.environ.get(k, "") or "").strip()
    }
    load_volume_smtp_into_os_environ(skip_keys=skip)

    assert os.environ.get("SMTP_USERNAME") == "vol_user"
    assert os.environ.get("SMTP_HOST") == "vol.host"
