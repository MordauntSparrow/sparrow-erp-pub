"""
Portal AI: dashboard summary and assistant chat for contractors.
Uses central OpenAI config (``app.ai_config`` / OPENAI_API_KEY). Tools give the
assistant access to the contractor's summary (policies, training, todos, etc.).
"""
import json
import logging
from typing import Any, Dict, List, Optional

from app.ai_config import (
    build_openai_client,
    get_openai_model,
    is_openai_configured,
)

logger = logging.getLogger(__name__)


def is_ai_available() -> bool:
    return is_openai_configured()


def get_ai_dashboard_summary(contractor_id: int, context: Dict[str, Any]) -> Optional[str]:
    """
    Generate a short, friendly one-line summary for the dashboard (e.g. "You're all set today." or "Today: 2 policies to sign, 1 training to complete.").
    Returns None if AI not configured or on error.
    """
    if not is_openai_configured() or not context:
        return None
    try:
        client = build_openai_client()
        parts = []
        if context.get("pending_policies"):
            parts.append(f"{context['pending_policies']} policy/policies to sign")
        if context.get("pending_training"):
            parts.append(f"{context['pending_training']} training item(s) to complete")
        if context.get("pending_hr_requests"):
            parts.append(f"{context['pending_hr_requests']} HR document request(s)")
        if context.get("pending_todo_count"):
            parts.append(f"{context['pending_todo_count']} to-do(s)")
        if context.get("unread_messages"):
            parts.append(f"{context['unread_messages']} unread message(s)")
        context_str = ", ".join(parts) if parts else "nothing pending"
        todo_preview = (context.get("todo_titles") or [])[:5]
        prompt = f"""The staff member's portal summary is: {context_str}.
Pending to-do titles (first few): {todo_preview or 'none'}.
Write exactly one short, friendly sentence (under 100 chars) for their dashboard. If nothing pending, say they're all set or have a good day. No bullet points, no quotes."""
        response = client.chat.completions.create(
            model=get_openai_model(),
            messages=[{"role": "user", "content": prompt}],
        )
        choice = response.choices[0] if response.choices else None
        if choice and choice.message and choice.message.content:
            return (choice.message.content or "").strip()[:200]
    except ImportError:
        logger.debug("openai not installed")
    except Exception as e:
        logger.debug("Portal AI summary failed: %s", e)
    return None


# Assistant chat: tool so the AI can answer "what do I need to do?"
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_my_summary",
            "description": "Get what this staff member needs to do: policies to sign, training to complete, HR requests, pending todos, unread messages. Use this when they ask what they need to do, what's pending, or for a summary.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]

SYSTEM_PROMPT = """You are the Employee Portal assistant. You help staff with:
- What they need to do: policies to sign, training to complete, HR document requests, to-dos, and unread messages.
- Where to find things in the portal (Time & Billing, HR, Compliance, Training, Scheduling, etc.).
- General navigation and next steps.

Always act only for the logged-in staff member. Be concise and friendly. When you use get_my_summary, summarize the result in plain language and suggest the relevant links (Compliance, Training, HR, Portal dashboard). Don't make up data—use the tool to get their real summary."""


def _execute_tool(contractor_id: int, name: str, args: Dict[str, Any]) -> str:
    if name != "get_my_summary":
        return "Unknown tool."
    try:
        from .services import get_dashboard_summary_context
        ctx = get_dashboard_summary_context(contractor_id)
        return json.dumps(ctx)
    except Exception as e:
        logger.exception("get_my_summary failed: %s", e)
        return json.dumps({"error": str(e)})


def assistant_chat(contractor_id: int, messages: List[Dict[str, str]]) -> Optional[str]:
    """
    Send messages to the portal assistant and return the reply.
    messages: list of {"role": "user"|"assistant", "content": "..."}
    Returns None if AI not configured or on error.
    """
    if not is_openai_configured():
        return None
    try:
        client = build_openai_client()
        all_messages: List[Dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            *messages,
        ]
        max_iterations = 5
        while max_iterations > 0:
            max_iterations -= 1
            response = client.chat.completions.create(
                model=get_openai_model(),
                messages=all_messages,
                tools=TOOLS,
                tool_choice="auto",
            )
            choice = response.choices[0] if response.choices else None
            if not choice:
                return "Sorry, I couldn't generate a reply."
            msg = choice.message
            if getattr(msg, "tool_calls", None):
                for tc in msg.tool_calls:
                    name = getattr(tc.function, "name", None) or getattr(tc, "name", "")
                    args_str = getattr(tc.function, "arguments", None) or getattr(tc, "arguments", "{}")
                    try:
                        args = json.loads(args_str) if isinstance(args_str, str) else (args_str or {})
                    except json.JSONDecodeError:
                        args = {}
                    result = _execute_tool(contractor_id, name, args)
                    all_messages.append(msg)
                    all_messages.append({
                        "role": "tool",
                        "tool_call_id": getattr(tc, "id", ""),
                        "content": result,
                    })
                continue
            return (getattr(msg, "content", None) or "").strip()
        return "I hit a limit. Please try again with a shorter message."
    except ImportError:
        logger.warning("openai package not installed")
        return None
    except Exception as e:
        logger.exception("Portal assistant chat failed: %s", e)
        return None
