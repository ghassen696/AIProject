from typing import List, Dict, Any
import re
from datetime import datetime, timezone,timedelta

from typing import List, Dict, Any
import re
from datetime import datetime, timezone,timedelta

def parse_timestamp(ts):
    """
    Parse a timestamp into a Python datetime object.
    Handles:
      - int/float: Unix timestamps (in seconds or milliseconds)
      - str (ISO 8601 or numeric): e.g., "2025-08-31T12:30:45", "1756789012345"
    Returns:
      datetime object (UTC) or None if parsing fails.
    """

    # Case 1: Numeric (int/float) → Unix timestamp
    if isinstance(ts, (int, float)):
        try:
            # Heuristic: values > 1e12 are probably milliseconds
            return datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None

    # Case 2: String
    if isinstance(ts, str):
        ts = ts.strip()

        # Try ISO 8601 format first (e.g., "2025-08-31T12:30:45")
        try:
            dt = datetime.fromisoformat(ts)
            # If naive datetime, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

        # Try numeric string (Unix timestamp)
        if ts.isdigit():
            try:
                ts_int = int(ts)
                return datetime.utcfromtimestamp(ts_int / 1000 if ts_int > 1e12 else ts_int)
            except (OSError, OverflowError, ValueError):
                return None

    # Case 3: Invalid type
    return None
def normalize_date(date_input):
    """
    Normalize different date input types into a datetime.date object.
    Accepts datetime, date, or string (YYYY-MM-DD).
    """
    if isinstance(date_input, datetime):
        return date_input.date()
    if hasattr(date_input, "year") and hasattr(date_input, "month"):  # datetime.date
        return date_input
    if isinstance(date_input, str):
        return datetime.strptime(date_input.split(" ")[0], "%Y-%m-%d").date()
    raise ValueError(f"Unsupported date type: {type(date_input)}")


def truncate_text(text: str, max_len: int = 200) -> str:
    if not text:
        return ""
    return text if len(text) <= max_len else text[:100] + " ... " + text[-100:]

KEY_MAP = {
    "ctrl_l": "Ctrl", "alt_l": "Alt", "tab": "Tab", "shift": "Shift",
    "enter": "Enter", "backspace": "Backspace", "space": "Space",
    "cmd": "Cmd", "up": "↑", "down": "↓", "left": "←", "right": "→"
}
def parse_keys(text: str):
    keys = re.findall(r"<Key\.([a-z0-9_]+)>", text)
    return [KEY_MAP.get(k, f"Key_{k}") for k in keys]

def fmt_ts(ts):
    try:
        if isinstance(ts, (int, float)):
            return datetime.utcfromtimestamp(ts / 1000).isoformat()
        return str(ts)
    except Exception:
        return str(ts)

def fmt_dur(seconds):
    if not seconds:
        return "0s"
    if seconds < 60:
        return f"{int(seconds)}s"
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins}m {secs}s"

from itertools import groupby

def collapse_repeats(keys):
    collapsed = []
    for key, group in groupby(keys):
        count = len(list(group))
        if count > 3:  # only collapse if repeated a lot
            collapsed.append(f"{key} (x{count})")
        else:
            collapsed.extend([key] * count)
    return collapsed





from typing import List, Dict, Any
import re
from datetime import datetime, timezone,timedelta

def parse_timestamp(ts):


    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None

    if isinstance(ts, str):
        ts = ts.strip()

        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

        if ts.isdigit():
            try:
                ts_int = int(ts)
                return datetime.utcfromtimestamp(ts_int / 1000 if ts_int > 1e12 else ts_int)
            except (OSError, OverflowError, ValueError):
                return None

    return None
def normalize_date(date_input):

    if isinstance(date_input, datetime):
        return date_input.date()
    if hasattr(date_input, "year") and hasattr(date_input, "month"):  # datetime.date
        return date_input
    if isinstance(date_input, str):
        return datetime.strptime(date_input.split(" ")[0], "%Y-%m-%d").date()
    raise ValueError(f"Unsupported date type: {type(date_input)}")


def truncate_text(text: str, max_len: int = 200) -> str:
    if not text:
        return ""
    return text if len(text) <= max_len else text[:100] + " ... " + text[-100:]

KEY_MAP = {
    "ctrl_l": "Ctrl",
    "alt_l": "Alt",
    "tab": "Tab",
    "shift": "Shift",
    "enter": "Enter",
    "backspace": "Backspace",
    "up": "↑", "down": "↓", "left": "←", "right": "→"
}

def fmt_ts(ts):
    try:
        if isinstance(ts, (int, float)):
            return datetime.utcfromtimestamp(ts / 1000).isoformat()
        return str(ts)
    except Exception:
        return str(ts)

def fmt_dur(seconds):
    if not seconds:
        return "0s"
    if seconds < 60:
        return f"{int(seconds)}s"
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins}m {secs}s"

from itertools import groupby

def collapse_repeats(keys):
    collapsed = []
    for key, group in groupby(keys):
        count = len(list(group))
        if count > 3:  # only collapse if repeated a lot
            collapsed.append(f"{key} (x{count})")
        else:
            collapsed.extend([key] * count)
    return collapsed


def parse_keys(text: str):
    keys = re.findall(r"<Key\.([a-z0-9_]+)>", text)
    mapped = [KEY_MAP.get(k, k) for k in keys]
    return mapped


    


def normalize_date(date_input):
    """
    Normalize different date input types into a datetime.date object.
    Accepts datetime, date, or string (YYYY-MM-DD).
    """
    if isinstance(date_input, datetime):
        return date_input.date()
    if hasattr(date_input, "year") and hasattr(date_input, "month"):  # datetime.date
        return date_input
    if isinstance(date_input, str):
        return datetime.strptime(date_input.split(" ")[0], "%Y-%m-%d").date()
    raise ValueError(f"Unsupported date type: {type(date_input)}")


def truncate_text(text: str, max_len: int = 200) -> str:
    if not text:
        return ""
    return text if len(text) <= max_len else text[:100] + " ... " + text[-100:]

KEY_MAP = {
    "ctrl_l": "Ctrl",
    "alt_l": "Alt",
    "tab": "Tab",
    "shift": "Shift",
    "enter": "Enter",
    "backspace": "Backspace",
    "up": "↑", "down": "↓", "left": "←", "right": "→"
}

def fmt_ts(ts):
    try:
        if isinstance(ts, (int, float)):
            return datetime.utcfromtimestamp(ts / 1000).isoformat()
        return str(ts)
    except Exception:
        return str(ts)

def fmt_dur(seconds):
    if not seconds:
        return "0s"
    if seconds < 60:
        return f"{int(seconds)}s"
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins}m {secs}s"

from itertools import groupby

def collapse_repeats(keys):
    collapsed = []
    for key, group in groupby(keys):
        count = len(list(group))
        if count > 3:  # only collapse if repeated a lot
            collapsed.append(f"{key} (x{count})")
        else:
            collapsed.extend([key] * count)
    return collapsed


def parse_keys(text: str):
    keys = re.findall(r"<Key\.([a-z0-9_]+)>", text)
    mapped = [KEY_MAP.get(k, k) for k in keys]
    return mapped


from typing import List, Dict, Any
import re
from datetime import datetime, timezone,timedelta

def parse_timestamp(ts):


    if isinstance(ts, (int, float)):
        try:
            return datetime.fromtimestamp(ts / 1000 if ts > 1e12 else ts, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None

    if isinstance(ts, str):
        ts = ts.strip()

        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

        if ts.isdigit():
            try:
                ts_int = int(ts)
                return datetime.utcfromtimestamp(ts_int / 1000 if ts_int > 1e12 else ts_int)
            except (OSError, OverflowError, ValueError):
                return None

    return None
def normalize_date(date_input):

    if isinstance(date_input, datetime):
        return date_input.date()
    if hasattr(date_input, "year") and hasattr(date_input, "month"):  # datetime.date
        return date_input
    if isinstance(date_input, str):
        return datetime.strptime(date_input.split(" ")[0], "%Y-%m-%d").date()
    raise ValueError(f"Unsupported date type: {type(date_input)}")


def truncate_text(text: str, max_len: int = 200) -> str:
    if not text:
        return ""
    return text if len(text) <= max_len else text[:100] + " ... " + text[-100:]

KEY_MAP = {
    "ctrl_l": "Ctrl",
    "alt_l": "Alt",
    "tab": "Tab",
    "shift": "Shift",
    "enter": "Enter",
    "backspace": "Backspace",
    "up": "↑", "down": "↓", "left": "←", "right": "→"
}

def fmt_ts(ts):
    try:
        if isinstance(ts, (int, float)):
            return datetime.utcfromtimestamp(ts / 1000).isoformat()
        return str(ts)
    except Exception:
        return str(ts)

def fmt_dur(seconds):
    if not seconds:
        return "0s"
    if seconds < 60:
        return f"{int(seconds)}s"
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins}m {secs}s"

from itertools import groupby

def collapse_repeats(keys):
    collapsed = []
    for key, group in groupby(keys):
        count = len(list(group))
        if count > 3:  # only collapse if repeated a lot
            collapsed.append(f"{key} (x{count})")
        else:
            collapsed.extend([key] * count)
    return collapsed


def parse_keys(text: str):
    keys = re.findall(r"<Key\.([a-z0-9_]+)>", text)
    mapped = [KEY_MAP.get(k, k) for k in keys]
    return mapped

def assess_chunk_quality(chunk: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Assess the quality of a chunk based on event types and content.
    Args:
        chunk: List of event dictionaries from adaptive_chunk_logs.
    Returns:
        Dict with quality metrics and whether the chunk has sufficient data.
    """
    if not chunk:
        return {
            "quality_score": 0.0,
            "sufficient_data": False,
            "event_count": 0,
            "char_count": 0,
            "unique_apps": 0,
            "meaningful_events": 0
        }

    event_count = len(chunk)
    meaningful_events = sum(1 for e in chunk if (e.get("event") in ["keystrokes", "clipboard_paste"] and
                                                e.get("description") != "No meaningful keystrokes") or
                                               (e.get("event") == "window_switch" and
                                                e.get("application") in ["Code", "WindowsTerminal", "Notepad"]))
    char_count = sum(len(e.get("description", "")) for e in chunk)
    unique_apps = len(set(e.get("application", "unknown") for e in chunk if e.get("application") != "unknown"))
    has_content = any(e.get("description", "").startswith(("typed:", "Pasted content:")) for e in chunk)

    quality_score = sum([
        0.4 if meaningful_events > 0 else 0.0,  # Prioritize productive events
        min(event_count / 10, 1.0) * 0.2,      # Event density
        min(char_count / 500, 1.0) * 0.2,      # Content richness
        min(unique_apps / 3, 1.0) * 0.15,      # Application diversity
        0.05 if has_content else 0.0           # Presence of typing/paste
    ])

    sufficient_data = quality_score > 0.35 and meaningful_events > 0

    if not sufficient_data:
        print(f"Discarded chunk: events={event_count}, meaningful={meaningful_events}, score={quality_score:.2f}")

    return {
        "quality_score": quality_score,
        "sufficient_data": sufficient_data,
        "event_count": event_count,
        "char_count": char_count,
        "unique_apps": unique_apps,
        "meaningful_events": meaningful_events
    }
    
def adaptive_chunk_logs(
    logs: List[Dict],
    max_logs: int = 200,
    max_tokens: int = 1000,  # Token-based limit
    slice_minutes: int = 30,
    min_chunk_size: int = 10  # Reduced for smaller chunks
) -> List[List[Dict[str, Any]]]:
    chunks: List[List[Dict[str, Any]]] = []
    current_chunk: List[Dict[str, Any]] = []
    current_size = 0
    current_window = None
    start_time = None
    last_event_type = None

    def finalize():
        nonlocal current_chunk, current_size
        if current_chunk and assess_chunk_quality(current_chunk)["sufficient_data"]:
            chunks.append(current_chunk)
        current_chunk = []
        current_size = 0
        current_window = None
        start_time = None

    # Sort logs by timestamp
    sorted_logs = sorted(
        logs, key=lambda x: parse_timestamp(x.get("timestamp")) or datetime.min
    )

    for log in sorted_logs:
        ts_dt = parse_timestamp(log.get("timestamp"))
        if ts_dt is None:
            print(f"⚠️ Skipping log with invalid timestamp: {log}")
            continue

        win = str(log.get("window", "unknown"))
        app = str(log.get("application", "unknown")).replace(".exe", "")
        desc_struct = normalize_event(log)
        if desc_struct is None:
            continue
        desc_str = desc_struct["description"]
        event_type = log.get("event", "unknown")

        event_dict = {
            "timestamp": ts_dt,
            "description": desc_str,
            "raw_event": desc_struct,
            "window": win,
            "application": app,
            "event": event_type,
            "session_id": desc_struct["session_id"],
            "seq_num": desc_struct["seq_num"]
        }

        if start_time is None:
            start_time = ts_dt

        # Dynamic time slice based on event density
        event_count = len(current_chunk)
        slice_minutes_adj = 15 if event_count > 50 else 30

        # Split on significant events
        if event_type in ["pause", "idle_start", "idle_end"] and current_chunk:
            finalize()

        # Time-based split
        if (ts_dt - start_time).total_seconds() > slice_minutes_adj * 60:
            finalize()
            start_time = ts_dt
            current_window = None

        # Window-based split
        if win != current_window and len(current_chunk) >= min_chunk_size:
            finalize()
            current_window = win
        elif current_window is None:
            current_window = win

        current_chunk.append(event_dict)
        current_size += len(desc_str) // 4  # Approximate tokens

        # Token-based split
        if len(current_chunk) >= max_logs or current_size >= max_tokens:
            finalize()

    finalize()
    return chunks
def normalize_event(log: Dict[str, Any]) -> Dict[str, Any]:
    ts = parse_timestamp(log.get("timestamp"))
    if ts is None:
        print(f"⚠️ Invalid timestamp in log: {log}")
        return None  # Skip invalid events

    event_type = log.get("event", "unknown")
    window = str(log.get("window", "unknown"))  # Ensure string
    app = str(log.get("application", "unknown")).replace(".exe", "")
    text = str(log.get("text", ""))
    control = str(log.get("control", ""))  # Capture control
    session_id = str(log.get("session_id", "unknown"))  # Add context
    seq_num = int(log.get("seq_num", 0))  # Add context

    normalized = {
        "event_type": event_type,
        "application": app,
        "window": window,
        "start_time": ts,
        "end_time": ts,
        "duration_sec": 0.0,
        "description": "",
        "control": control,
        "session_id": session_id,
        "seq_num": seq_num
    }

    if event_type == "keystrokes":
        keys = parse_keys(text)
        clean_text = re.sub(r"<Key\..*?>", "", text).strip()
        backspaces = keys.count("Backspace")
        desc_parts = []
        if clean_text:
            desc_parts.append(f"typed: '{truncate_text(clean_text)}'")
        if backspaces:
            desc_parts.append(f"deleted {backspaces} chars")
        if keys and not clean_text:
            desc_parts.append(f"pressed {'+'.join(collapse_repeats(keys))}")
        normalized["description"] = "; ".join(desc_parts) or "No meaningful keystrokes"
        normalized["duration_sec"] = max((len(clean_text) + len(keys)) * 0.15, 0.5)  # Scale by content
        normalized["end_time"] = ts + timedelta(seconds=normalized["duration_sec"])

    elif event_type == "clipboard_paste":
        preview = truncate_text(text, 100)
        normalized["description"] = f"Pasted content: {preview}" if preview else "Empty paste"
        normalized["duration_sec"] = 2.0 + len(text) / 1000  # Scale by length
        normalized["end_time"] = ts + timedelta(seconds=normalized["duration_sec"])

    elif event_type in ["window_switch", "resume", "consent_given"]:
        normalized["description"] = f"{event_type.replace('_', ' ').title()} in {app} ({window})"
        normalized["duration_sec"] = 0.5  # Small active duration
        normalized["end_time"] = ts + timedelta(seconds=0.5)

    elif event_type == "idle_start":
        normalized["description"] = f"Went idle in {app} ({window})"
        normalized["duration_sec"] = 0.0

    elif event_type == "idle_end":
        dur = max(float(log.get("idle_duration_sec", 0)), 0.0)  # Validate
        normalized["description"] = f"Returned from idle (away {fmt_dur(dur)})"
        normalized["duration_sec"] = dur
        normalized["end_time"] = ts + timedelta(seconds=dur)

    elif event_type == "pause":
        reason = str(log.get("reason", "unspecified"))
        dur = max(float(log.get("duration_minutes", 0)) * 60, 0.0)
        normalized["description"] = f"Paused work ({reason}, {fmt_dur(dur)})"
        normalized["duration_sec"] = dur
        normalized["end_time"] = ts + timedelta(seconds=dur)

    else:
        normalized["description"] = f"Unknown event {event_type} in {app} ({window})"
        normalized["duration_sec"] = 0.5

    return normalized
