"""
Scheduling AI assistant: contractors can chat about availability, shifts, and coverage.
Uses central OpenAI config (``app.ai_config``) with tool-calling; tools are scoped
to the current contractor.
"""
import json
import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional

from app.ai_config import build_openai_client, get_openai_model, is_openai_configured

from .services import ScheduleService

logger = logging.getLogger(__name__)


def _parse_time(s: Optional[str]):
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.strptime(s.strip()[:5], "%H:%M").time()
    except (ValueError, TypeError):
        return None


def _time_str(t: Any) -> str:
    if t is None:
        return ""
    if hasattr(t, "strftime"):
        return t.strftime("%H:%M")
    if hasattr(t, "total_seconds"):
        sec = int(t.total_seconds()) % (24 * 3600)
        return f"{sec // 3600:02d}:{(sec % 3600) // 60:02d}"
    return str(t)[:5]


# Tool definitions for OpenAI (function calling)
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_my_availability",
            "description": "Get the current user's availability windows (when they are generally available to be scheduled).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_availability",
            "description": "Add an availability window. day_of_week: 0=Monday, 6=Sunday. Times in HH:MM. effective_from and optional effective_to in YYYY-MM-DD.",
            "parameters": {
                "type": "object",
                "properties": {
                    "day_of_week": {"type": "integer", "description": "0=Monday, 6=Sunday"},
                    "start_time": {"type": "string", "description": "e.g. 09:00"},
                    "end_time": {"type": "string", "description": "e.g. 17:00"},
                    "effective_from": {"type": "string", "description": "Start date YYYY-MM-DD"},
                    "effective_to": {"type": "string", "description": "End date YYYY-MM-DD or omit for ongoing"},
                },
                "required": ["day_of_week", "start_time", "end_time", "effective_from"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_my_shifts",
            "description": "Get the current user's shifts. Pass date for a single day, or date_from and date_to for a range (YYYY-MM-DD).",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "Single day YYYY-MM-DD"},
                    "date_from": {"type": "string", "description": "Range start YYYY-MM-DD"},
                    "date_to": {"type": "string", "description": "Range end YYYY-MM-DD"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_shift_details",
            "description": "Get details of a specific shift by ID. Use this when the user refers to 'my shift on Friday' or a shift id.",
            "parameters": {
                "type": "object",
                "properties": {"shift_id": {"type": "integer", "description": "Shift ID"}},
                "required": ["shift_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "suggest_cover_for_shift",
            "description": "Find who could cover a specific shift (staff who are free at that time). Pass the shift_id. Use when the user asks who can take their shift or who could cover.",
            "parameters": {
                "type": "object",
                "properties": {"shift_id": {"type": "integer", "description": "The shift to find cover for"}},
                "required": ["shift_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "request_time_off",
            "description": "Submit a time off request (e.g. annual leave). Dates in YYYY-MM-DD.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": ["start_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "report_sickness",
            "description": "Report sickness absence. Dates in YYYY-MM-DD.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": ["start_date"],
            },
        },
    },
]


def execute_tool(contractor_id: int, tool_name: str, arguments: Dict[str, Any]) -> str:
    """Execute a tool for the given contractor and return a short summary for the model."""
    try:
        if tool_name == "get_my_availability":
            rows = ScheduleService.list_availability(contractor_id)
            if not rows:
                return "You have no availability windows set yet. You can add them with add_availability."
            day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            lines = []
            for r in rows:
                day = day_names[r["day_of_week"]] if 0 <= r.get("day_of_week", -1) <= 6 else str(r.get("day_of_week"))
                lines.append(f"- {day} {_time_str(r.get('start_time'))}–{_time_str(r.get('end_time'))} (from {r.get('effective_from')} to {r.get('effective_to') or 'ongoing'})")
            return "Current availability:\n" + "\n".join(lines)

        if tool_name == "add_availability":
            day_of_week = arguments.get("day_of_week")
            start_time = _parse_time(arguments.get("start_time"))
            end_time = _parse_time(arguments.get("end_time"))
            effective_from_s = arguments.get("effective_from")
            effective_to_s = arguments.get("effective_to")
            if day_of_week is None or start_time is None or end_time is None or not effective_from_s:
                return "Error: need day_of_week (0–6), start_time, end_time (HH:MM), and effective_from (YYYY-MM-DD)."
            try:
                effective_from = date.fromisoformat(effective_from_s)
                effective_to = date.fromisoformat(effective_to_s) if effective_to_s else None
            except ValueError:
                return "Error: invalid date format. Use YYYY-MM-DD."
            if not (0 <= day_of_week <= 6):
                return "Error: day_of_week must be 0 (Monday) to 6 (Sunday)."
            ScheduleService.add_availability(
                contractor_id, day_of_week, start_time, end_time, effective_from, effective_to
            )
            day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            return f"Done. Added availability: {day_names[day_of_week]} {start_time.strftime('%H:%M')}–{end_time.strftime('%H:%M')} from {effective_from_s}."

        if tool_name == "get_my_shifts":
            work_date = None
            date_from = None
            date_to = None
            if arguments.get("date"):
                try:
                    work_date = date.fromisoformat(arguments["date"])
                except ValueError:
                    return "Error: invalid date. Use YYYY-MM-DD."
            if arguments.get("date_from"):
                try:
                    date_from = date.fromisoformat(arguments["date_from"])
                except ValueError:
                    return "Error: invalid date_from."
            if arguments.get("date_to"):
                try:
                    date_to = date.fromisoformat(arguments["date_to"])
                except ValueError:
                    return "Error: invalid date_to."
            if not work_date and not date_from:
                # default: next 7 days
                today = date.today()
                date_from = today
                date_to = today + timedelta(days=6)
            elif work_date:
                date_from = work_date
                date_to = work_date
            shifts = ScheduleService.list_shifts(
                contractor_id=contractor_id, date_from=date_from, date_to=date_to
            )
            if not shifts:
                return "No shifts in that period."
            lines = []
            for s in shifts:
                wd = s.get("work_date")
                wd_str = wd.isoformat() if hasattr(wd, "isoformat") else str(wd)
                lines.append(f"- Shift {s.get('id')}: {wd_str} {_time_str(s.get('scheduled_start'))}–{_time_str(s.get('scheduled_end'))} at {s.get('client_name', '—')} ({s.get('status', '—')})")
            return "Your shifts:\n" + "\n".join(lines)

        if tool_name == "get_shift_details":
            shift_id = arguments.get("shift_id")
            if shift_id is None:
                return "Error: shift_id required."
            shift = ScheduleService.get_shift(int(shift_id))
            if not shift:
                return "Shift not found."
            if shift["contractor_id"] != contractor_id:
                return "That shift is not yours."
            wd = shift.get("work_date")
            wd_str = wd.isoformat() if hasattr(wd, "isoformat") else str(wd)
            return f"Shift {shift['id']}: {wd_str} {_time_str(shift.get('scheduled_start'))}–{_time_str(shift.get('scheduled_end'))} at {shift.get('client_name')} ({shift.get('job_type_name')}). Status: {shift.get('status')}."

        if tool_name == "suggest_cover_for_shift":
            shift_id = arguments.get("shift_id")
            if shift_id is None:
                return "Error: shift_id required."
            shift = ScheduleService.get_shift(int(shift_id))
            if not shift:
                return "Shift not found."
            if shift["contractor_id"] != contractor_id:
                return "That shift is not yours."
            wd = shift.get("work_date")
            st = shift.get("scheduled_start")
            en = shift.get("scheduled_end")
            if not wd or not st or not en:
                return "Shift has no date or times."
            if hasattr(st, "strftime"):
                start_time = st
            else:
                start_time = _parse_time(_time_str(st))
            if hasattr(en, "strftime"):
                end_time = en
            else:
                end_time = _parse_time(_time_str(en))
            if start_time is None or end_time is None:
                return "Could not read shift times."
            available = ScheduleService.suggest_available_contractors(
                work_date=wd, start_time=start_time, end_time=end_time
            )
            # Exclude current user from "who can take my shift"
            names = [c.get("name") or c.get("email") or f"ID {c.get('id')}" for c in available if c.get("id") != contractor_id]
            if not names:
                return "No other staff are free for that slot. You could ask your manager to find cover."
            return "Staff who could cover this shift: " + ", ".join(names) + ". Suggest they contact the scheduler to arrange the swap."

        if tool_name == "request_time_off":
            start_s = arguments.get("start_date")
            end_s = arguments.get("end_date") or start_s
            reason = (arguments.get("reason") or "").strip() or None
            if not start_s:
                return "Error: start_date required (YYYY-MM-DD)."
            try:
                start_date = date.fromisoformat(start_s)
                end_date = date.fromisoformat(end_s) if end_s else start_date
                if end_date < start_date:
                    end_date = start_date
            except ValueError:
                return "Error: invalid date. Use YYYY-MM-DD."
            tid = ScheduleService.create_time_off(
                contractor_id, start_date, end_date, reason=reason, type="annual"
            )
            return f"Time off request submitted for {start_date} to {end_date}. Request ID: {tid}. Your manager will review it."

        if tool_name == "report_sickness":
            start_s = arguments.get("start_date")
            end_s = arguments.get("end_date") or start_s
            reason = (arguments.get("reason") or "").strip() or "Sickness"
            if not start_s:
                return "Error: start_date required (YYYY-MM-DD)."
            try:
                start_date = date.fromisoformat(start_s)
                end_date = date.fromisoformat(end_s) if end_s else start_date
                if end_date < start_date:
                    end_date = start_date
            except ValueError:
                return "Error: invalid date. Use YYYY-MM-DD."
            tid = ScheduleService.create_time_off(
                contractor_id, start_date, end_date, reason=reason, type="sickness"
            )
            return f"Sickness reported for {start_date} to {end_date}. Get well soon. Your manager has been notified."

        return f"Unknown tool: {tool_name}"
    except Exception as e:
        logger.exception("AI tool %s failed", tool_name)
        return f"Something went wrong: {str(e)}"


SYSTEM_PROMPT = """You are a helpful scheduling assistant for staff. You help with:
- Viewing and updating their availability (when they're free to be scheduled).
- Viewing their shifts and finding out who could cover a shift if they need to give it away.
- Submitting time off requests or reporting sickness.

Always act in the user's name only (you have access only to their data). Be concise and friendly. When you use a tool, summarize the result in plain language. If the user says something like "I'm free Tuesday afternoons" or "I can do Mondays 9 to 5", use add_availability with the right day and times (effective_from can be today's date). If they ask "who can take my shift" or "who could cover Friday?", use get_my_shifts or get_shift_details to find the shift, then suggest_cover_for_shift with that shift_id. Don't make up shift IDs—look them up first if needed."""


def chat(contractor_id: int, messages: List[Dict[str, str]]) -> Optional[str]:
    """
    Send messages to the AI and return the assistant's reply.
    messages: list of {"role": "user"|"assistant", "content": "..."}
    Returns None if AI is not configured or on error.
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
            if msg.tool_calls:
                for tc in msg.tool_calls:
                    name = tc.function.name if hasattr(tc.function, "name") else getattr(tc, "name", "")
                    args_str = tc.function.arguments if hasattr(tc.function, "arguments") else getattr(tc, "arguments", "{}")
                    try:
                        args = json.loads(args_str)
                    except json.JSONDecodeError:
                        args = {}
                    result = execute_tool(contractor_id, name, args)
                    all_messages.append(msg)
                    all_messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result,
                    })
                continue
            return (msg.content or "").strip()
        return "I hit a limit. Please try again with a shorter message."
    except ImportError:
        logger.warning("openai package not installed")
        return None
    except Exception as e:
        logger.exception("Scheduling AI chat failed: %s", e)
        return None


def is_ai_available() -> bool:
    return is_openai_configured()
