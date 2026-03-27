"""
Central LLM configuration for Sparrow ERP (OpenAI-compatible client).

All modules use the ``openai`` Python SDK against whatever provider you configure
via ``OPENAI_BASE_URL`` and ``OPENAI_API_KEY``. You do **not** need a paid
OpenAI account if you use **OpenRouter** (many models, including free tiers) or
**Google Gemini** (free tier via AI Studio) through its OpenAI-compatible
endpoint.

Provider quick start (no code changes in plugins)
-------------------------------------------------
**OpenRouter** — https://openrouter.ai/ — create an API key, then::

    OPENAI_API_KEY=<openrouter key>
    OPENAI_BASE_URL=https://openrouter.ai/api/v1
    OPENAI_MODEL=google/gemini-2.0-flash-001:free   # example; pick any model id

Optional headers (recommended by OpenRouter)::

    OPENROUTER_HTTP_REFERER=https://your-company.example
    OPENROUTER_SITE_TITLE=Sparrow ERP

**Gemini (Google AI Studio)** — https://aistudio.google.com/apikey ::

    OPENAI_API_KEY=<Gemini API key>   # or GEMINI_API_KEY (alias below)
    OPENAI_BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai/
    OPENAI_MODEL=gemini-2.0-flash

Tool-calling behaviour can vary by provider/model; if a model fails with tools,
try another on OpenRouter or check provider docs.

Environment variables
-----------------------
OPENAI_API_KEY
    API key for the configured base URL (OpenAI, OpenRouter, Gemini compat, etc.)

SPARROW_OPENAI_API_KEY, GEMINI_API_KEY, GOOGLE_API_KEY
    Optional aliases if ``OPENAI_API_KEY`` is empty (same secret, clearer naming).

OPENAI_BASE_URL (optional)
    OpenAI-compatible API root. Omit for https://api.openai.com/v1 .

OPENAI_MODEL (optional)
    Model id when **Core settings → AI chat model** is left blank. Default
    ``gpt-4o-mini``.

Core manifest (``app/config/manifest.json``) — ``ai_settings.chat_model``
    Set from **Core settings → General** (chat model dropdown / custom id). When non-empty,
    this **overrides** ``OPENAI_MODEL``. Preset ids live in ``CHAT_MODEL_DROPDOWN_CHOICES``.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Optional


DEFAULT_OPENAI_MODEL = "gpt-4o-mini"

# Presets (optional): set OPENAI_BASE_URL_PRESET=openrouter|gemini to fill base URL if OPENAI_BASE_URL is empty.
_PRESETS = {
    "openrouter": "https://openrouter.ai/api/v1",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/openai/",
}

# Core manifest path (same layout as PluginManager: app/config/manifest.json)
_CORE_MANIFEST_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "config",
    "manifest.json",
)

_MODEL_ID_RE = re.compile(r"^[a-zA-Z0-9._/:+\-]{1,200}$")

# Curated ids for Core Settings dropdown (manifest override). Empty string = use OPENAI_MODEL from .env.
# "Other…" is handled in the template via __custom__ + text field; any sanitized id may still be saved.
CHAT_MODEL_DROPDOWN_CHOICES: tuple[tuple[str, str], ...] = (
    ("gpt-4o-mini", "OpenAI — gpt-4o-mini"),
    ("gpt-4o", "OpenAI — gpt-4o"),
    ("gpt-4.1-mini", "OpenAI — gpt-4.1-mini"),
    ("gpt-4.1", "OpenAI — gpt-4.1"),
    ("o4-mini", "OpenAI — o4-mini"),
    # OpenRouter (common / free-tier style ids)
    ("google/gemini-2.0-flash-001:free", "OpenRouter — Gemini 2.0 Flash (free)"),
    ("google/gemini-2.5-flash-preview-05-20:free", "OpenRouter — Gemini 2.5 Flash preview (free)"),
    ("meta-llama/llama-3.3-70b-instruct:free", "OpenRouter — Llama 3.3 70B Instruct (free)"),
    ("deepseek/deepseek-chat:free", "OpenRouter — DeepSeek Chat (free)"),
    ("mistralai/mistral-7b-instruct:free", "OpenRouter — Mistral 7B Instruct (free)"),
    ("openai/gpt-4o-mini", "OpenRouter — openai/gpt-4o-mini"),
    ("anthropic/claude-3.5-sonnet", "OpenRouter — Claude 3.5 Sonnet"),
    # Gemini OpenAI-compatible endpoint (AI Studio)
    ("gemini-2.0-flash", "Gemini API — gemini-2.0-flash"),
    ("gemini-2.5-flash-preview-05-20", "Gemini API — gemini-2.5-flash-preview"),
    ("gemini-2.5-pro-preview-05-06", "Gemini API — gemini-2.5-pro-preview"),
)


def chat_model_dropdown_value_set() -> set[str]:
    """Non-empty values from CHAT_MODEL_DROPDOWN_CHOICES (for template / validation hints)."""
    return {v for v, _ in CHAT_MODEL_DROPDOWN_CHOICES if v}


def sanitize_chat_model_id(raw: str) -> Optional[str]:
    """
    Return a safe model id or None if invalid/empty.
    Allows typical OpenAI / OpenRouter / Gemini ids.
    """
    s = (raw or "").strip()
    if not s:
        return None
    if len(s) > 200 or _MODEL_ID_RE.match(s) is None:
        return None
    return s


def _chat_model_from_manifest() -> Optional[str]:
    if not os.path.isfile(_CORE_MANIFEST_PATH):
        return None
    try:
        with open(_CORE_MANIFEST_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        ai = (data or {}).get("ai_settings") or {}
        m = sanitize_chat_model_id(str(ai.get("chat_model") or ""))
        return m
    except (OSError, json.JSONDecodeError, TypeError):
        return None


def get_openai_api_key() -> Optional[str]:
    raw = (
        os.environ.get("OPENAI_API_KEY")
        or os.environ.get("SPARROW_OPENAI_API_KEY")
        or os.environ.get("GEMINI_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
        or ""
    ).strip()
    return raw or None


def get_openai_base_url() -> Optional[str]:
    explicit = (os.environ.get("OPENAI_BASE_URL") or "").strip()
    if explicit:
        return explicit
    preset = (os.environ.get("OPENAI_BASE_URL_PRESET") or "").strip().lower()
    if preset in _PRESETS:
        return _PRESETS[preset]
    return None


def get_openai_model(default: str = DEFAULT_OPENAI_MODEL) -> str:
    """
    Resolved chat model: non-empty **Core settings** value wins, else
    ``OPENAI_MODEL`` env, else ``default``.
    """
    m = _chat_model_from_manifest()
    if m:
        return m
    env_m = (os.environ.get("OPENAI_MODEL") or "").strip()
    return env_m or default


def _openrouter_headers() -> Optional[Dict[str, str]]:
    base = (get_openai_base_url() or "").lower()
    if "openrouter.ai" not in base:
        return None
    h: Dict[str, str] = {}
    ref = (
        os.environ.get("OPENROUTER_HTTP_REFERER")
        or os.environ.get("OPENROUTER_SITE_URL")
        or ""
    ).strip()
    title = (
        os.environ.get("OPENROUTER_SITE_TITLE")
        or os.environ.get("OPENROUTER_APP_NAME")
        or "Sparrow ERP"
    ).strip()
    if ref:
        h["HTTP-Referer"] = ref
    if title:
        h["X-Title"] = title
    return h or None


def is_openai_configured() -> bool:
    """True if an API key is present (AI UIs may enable features)."""
    return bool(get_openai_api_key())


def build_openai_client() -> Any:
    """
    Construct ``openai.OpenAI`` with central settings.
    Raises ImportError if the ``openai`` package is missing.
    Raises ValueError if no API key is configured.
    """
    import openai

    key = get_openai_api_key()
    if not key:
        raise ValueError(
            "No LLM API key configured. Set OPENAI_API_KEY (or GEMINI_API_KEY / "
            "OpenRouter key) and usually OPENAI_BASE_URL — see app/ai_config.py."
        )
    base = get_openai_base_url()
    headers = _openrouter_headers()
    kwargs: Dict[str, Any] = {"api_key": key, "base_url": base or None}
    if headers:
        kwargs["default_headers"] = headers
    return openai.OpenAI(**kwargs)
