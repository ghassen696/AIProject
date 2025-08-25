# LLM_backend/log_processor.py
from datetime import datetime
from typing import List, Dict

def _ts(x):
    if isinstance(x, str):
        return datetime.fromisoformat(x.replace('Z','+00:00')) if 'Z' in x else datetime.fromisoformat(x)
    return x

def preprocess_logs(records: List[Dict]) -> List[Dict]:
    cleaned = []
    for r in records:
        text = (r.get("text") or "").replace("<Key.backspace>", "")
        if text.strip() or r.get("event") != "keystrokes":
            cleaned.append({
                "timestamp": r.get("timestamp"),
                "employee_id": r.get("employee_id"),
                "session_id": r.get("session_id"),
                "event": r.get("event"),
                "window": r.get("window"),
                "application": r.get("application"),
                "text": text
            })
    cleaned.sort(key=lambda x: _ts(x["timestamp"]))
    return cleaned

def chunk_logs(cleaned: List[Dict], chunk_size: int = 80) -> List[Dict]:
    chunks = []
    for i in range(0, len(cleaned), chunk_size):
        block = cleaned[i:i+chunk_size]
        start = _ts(block[0]["timestamp"])
        end = _ts(block[-1]["timestamp"])
        duration = (end - start).total_seconds()
        chunks.append({
            "start_ts": block[0]["timestamp"],
            "end_ts": block[-1]["timestamp"],
            "duration_sec": max(duration, 0),
            "employee_id": block[0].get("employee_id"),
            "session_id": block[0].get("session_id"),
            "windows": list({x.get("window") for x in block if x.get("window")}),
            "applications": list({x.get("application") for x in block if x.get("application")}),
            "events": block,
            "source_chunk_size": len(block)
        })
    return chunks
