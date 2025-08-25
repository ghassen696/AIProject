# LLM_backend/summarizer.py
from .ollama_client import ask_json
from .log_processor import preprocess_logs, chunk_logs

SUMMARY_PROMPT = """You are a helpful work-activity summarizer.
Given a chunk of raw user activity logs (keystrokes, windows, clipboard, idle/pause),
produce ONE concise task summary for the *whole chunk*, describing the main task.

Return JSON like:
{
  "task_summary": "…",
  "notes": "optional, brief"
}
Chunk:
"""

def summarize_chunk(chunk: dict, model: str = "llama3.2"):
    prompt = SUMMARY_PROMPT + str(chunk["events"])
    out = ask_json(model, prompt)
    if isinstance(out, dict) and "task_summary" in out:
        return out
    # fallback
    return {"task_summary": str(out)[:500]}

def summarize_logs(records, model: str = "llama3.2", chunk_size: int = 80):
    cleaned = preprocess_logs(records)
    chunks = chunk_logs(cleaned, chunk_size=chunk_size)
    summaries = []
    for c in chunks:
        s = summarize_chunk(c, model=model)
        summaries.append({
            "employee_id": c["employee_id"],
            "session_id": c["session_id"],
            "start_ts": c["start_ts"],
            "end_ts": c["end_ts"],
            "duration_sec": c["duration_sec"],
            "windows": c["windows"],
            "applications": c["applications"],
            "source_chunk_size": c["source_chunk_size"],
            "task_summary": s.get("task_summary","").strip(),
            "notes": s.get("notes","")
        })
    return summaries
