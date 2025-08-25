from typing import List, Dict, Any
from datetime import datetime

def truncate_text(text: str, max_len: int = 200) -> str:
    if not text:
        return ""
    return text if len(text) <= max_len else text[:100] + " ... " + text[-100:]

def normalize_event(log: Dict[str, Any]) -> str:
    ts = log.get("timestamp")
    event = log.get("event")
    window = log.get("window", "")
    text = log.get("text", "")

    if event == "keystrokes":
        return f"[{ts}] Typing in {window}: {truncate_text(text, 120)}"
    elif event == "clipboard_paste":
        return f"[{ts}] Pasted text in {window}: {truncate_text(text, 200)}"
    elif event == "window_switch":
        return f"[{ts}] Switched window to {window}"
    elif event == "idle_start":
        return f"[{ts}] Idle started in {window}"
    elif event == "idle_end":
        dur = log.get("idle_duration_sec", 0)
        return f"[{ts}] Idle ended (duration {int(dur)}s)"
    elif event == "pause":
        reason = log.get("reason", "unspecified")
        dur = log.get("duration_minutes", 0)
        return f"[{ts}] Paused work (reason: {reason}, {dur} min)"
    elif event == "resume":
        return f"[{ts}] Resumed work"
    else:
        return f"[{ts}] {event} in {window}"

def chunk_logs(logs: List[Dict], chunk_size: int = 200) -> List[List[str]]:
    """
    Simple fixed-size chunking (baseline).
    """
    normalized = [normalize_event(l) for l in logs]
    chunks = [normalized[i:i+chunk_size] for i in range(0, len(normalized), chunk_size)]
    return chunks

def smart_chunk_logs(
    logs: List[Dict], 
    max_logs: int = 200, 
    max_chars: int = 2000, 
    slice_minutes: int = 30
) -> List[List[str]]:
    """
    Smarter chunking:
    - Split by time (slice_minutes)
    - Merge frequent window switches
    - Limit size (logs + characters)
    - Summarize long/repetitive text
    """
    chunks = []
    current_chunk = []
    current_size = 0
    current_window = None
    start_time = None

    def finalize():
        nonlocal current_chunk, current_size
        if current_chunk:
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0

    for log in logs:
        ts = log["timestamp"]
        window = log.get("window", "unknown")
        line = normalize_event(log)

        # init time window
        if start_time is None:
            start_time = ts

        # enforce max time slice
        ts_dt = datetime.fromisoformat(ts)
        if (ts_dt - start_time).total_seconds() > slice_minutes * 60:
            finalize()
            start_time = ts_dt
            current_window = None
        else:
            if start_time is None:
                start_time = ts_dt

        # new window → finalize if last one was too long
        if window != current_window:
            # only split if last chunk is large enough
            if len(current_chunk) > 20:
                finalize()
            current_window = window

        # add line
        current_chunk.append(line)
        current_size += len(line)

        # enforce size
        if len(current_chunk) >= max_logs or current_size >= max_chars:
            finalize()

    finalize()
    return chunks
