import json
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import numpy as np 
import ollama
from elasticsearch import helpers
from .elastic_utils import fetch_employee_kpis,fetch_chunks_for_employee,fetch_chunk_summaries,save_classifications, fetch_logs_for_employee,save_work_patterns,save_daily_report,save_daily_chunks,save_summaries
from .preprocessing import parse_timestamp, adaptive_chunk_logs,normalize_date


MODEL_CONFIGS = {
    "classification": {"model": "llama3.2:latest", "temperature": 0.1},
    "summarization": {"model": "llama3.2:latest", "temperature": 0.3},
    "daily_report": {"model": "llama3.2:latest", "temperature": 0.2},
}

PRODUCTIVITY_WEIGHTS: Dict[str, float] = {
    "Coding": 1.0,
    "Writing": 0.8,
    "Research": 0.7,
    "Communication": 0.6,
    "Admin": 0.5,
    "Tools": 0.4,
    "Other": 0.3,
}


def ollama_chat_retry(model: str, messages: List[Dict[str, str]], retries: int = 3, delay: float = 2.0, **options) -> Dict[str, Any]:

    for i in range(retries):
        try:
            return ollama.chat(model=model, messages=messages, options=options or None)
        except Exception as e:
            if i == retries - 1:
                raise
            print(f"⚠️ Ollama call failed (attempt {i+1}/{retries}): {e} — retrying in {delay}s...")
            time.sleep(delay)

def robust_ollama_call(prompt: str, model: str = "llama3.2:latest", temperature: float = 0.2, max_retries: int = 3) -> str:

    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": prompt},
    ]
    resp = ollama_chat_retry(model=model, messages=messages, retries=max_retries, temperature=temperature)
    return resp["message"]["content"]

def is_ollama_running() -> bool:
    try:
        _ = ollama_chat_retry(
            model="llama3.2:latest",
            messages=[{"role": "system", "content": "Ping"}, {"role": "user", "content": "Ping"}],
            retries=1,
            num_predict=1, 
        )
        return True
    except Exception as e:
        print(f"⚠️ Ollama not running: {e}")
        return False

def start_ollama() -> None:
    try:
        subprocess.Popen(["ollama", "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(5)
        print("✅ Ollama server started")
    except Exception as e:
        print(f"❌ Failed to start Ollama: {e}")

def extract_json_block(raw_text: str) -> str:
    """
    Extract JSON if it's inside ```json ... ```; otherwise return stripped input.
    """
    match = re.search(r"```json(.*?)```", raw_text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return raw_text.strip()

def safe_parse_json(raw: str) -> Dict[str, Any]:
    raw = extract_json_block(raw)
    try:
        return json.loads(raw)
    except Exception:
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except Exception:
                return {}
    return {}

def ensure_schema(classification: Dict[str, Any]) -> Dict[str, Any]:
    default = {
        "primary_activity": {
            "category": "Other",
            "subcategory": "Unknown",
            "confidence": 0.0,
            "reasoning": "",
        },
        "secondary_activities": [],
        "productivity_indicators": {
            "focus_level": 0.0,
            "task_switching_frequency": "unknown",
            "deep_work_detected": False,
        },
        "key_applications": [],

    }
    merged = {**default, **(classification or {})}
    for k in default:
        if isinstance(default[k], dict):
            merged[k] = {**default[k], **merged.get(k, {})}
    return merged

def assess_chunk_quality(chunk: List[str]) -> Dict[str, Any]:

    content = "\n".join(chunk)

    has_typing = bool(re.search(r"\btyped:\b", content, re.IGNORECASE))
    has_apps = bool(re.search(r"^(\[.*?\])?\s*In\s+\w+", content, re.IGNORECASE | re.MULTILINE))
    has_windows = bool(re.search(r"\([^)]+\)", content))
    event_count = len(chunk)
    char_count = len(content)
    unique_apps = len(set(re.findall(r"In\s+([A-Za-z0-9_.-]+)", content)))

    quality_score = sum([
        0.3 if has_typing else 0.0,
        0.2 if has_apps else 0.0,
        0.2 if has_windows else 0.0,
        min(event_count / 10, 1.0) * 0.15,
        min(char_count / 500, 1.0) * 0.10,
        min(unique_apps / 3, 1.0) * 0.05,
    ])

    return {
        "quality_score": quality_score,
        "sufficient_data": quality_score > 0.4,
        "event_count": event_count,
        "char_count": char_count,
        "unique_apps": unique_apps,
        "has_meaningful_content": has_typing or unique_apps > 1,
    }

def recursive_chunk_summary(chunk_data: Dict[str, Any], max_len: int = 3500, depth: int = 0) -> str:

    events: List[str] = chunk_data.get("events", []) or []
    combined = "\n".join(events)

    # Metadata
    meta_parts: List[str] = []
    if "window" in chunk_data:
        meta_parts.append(f"Window: {chunk_data['window']}")
    if "start_time" in chunk_data and "end_time" in chunk_data:
        meta_parts.append(f"Time: {chunk_data['start_time']} → {chunk_data['end_time']}")
    meta = "\n".join(meta_parts)

    # Base case
    if len(combined) <= max_len:
        prompt = f"""Analyze the following employee activity logs and create a concise summary.

{meta}

Focus on:
- Main tasks/activities performed
- Applications used and their purpose
- 


 patterns and focus level
- Any notable behaviors or patterns

Activity Logs:
{combined}

Provide a clear, professional summary in 2-3 sentences."""
        config = MODEL_CONFIGS["summarization"]
        return robust_ollama_call(prompt, **config)

    # Recursive split
    mid = max(1, len(events) // 2)
    part1_data = {**chunk_data, "events": events[:mid]}
    part2_data = {**chunk_data, "events": events[mid:]}

    part1_summary = recursive_chunk_summary(part1_data, max_len, depth + 1)
    part2_summary = recursive_chunk_summary(part2_data, max_len, depth + 1)

    # Combine sub-summaries and re-run summary
    combined_data = {**chunk_data, "events": [part1_summary, part2_summary]}
    return recursive_chunk_summary(combined_data, max_len, depth + 1)

def summarize_chunk_ollama(chunk: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not chunk:
        return {"summary": "No events in this chunk.", "start_time": None, "end_time": None}

    # Ensure each event has description
    for event in chunk:
        if "description" not in event:
            event["description"] = ""

    # Extract timestamps
    start_time = None
    end_time = None
    if isinstance(chunk[0], dict) and "timestamp" in chunk[0]:
        ts = chunk[0]["timestamp"]
        start_time = ts.isoformat() if isinstance(ts, datetime) else str(ts)
    if isinstance(chunk[-1], dict) and "timestamp" in chunk[-1]:
        ts = chunk[-1]["timestamp"]
        end_time = ts.isoformat() if isinstance(ts, datetime) else str(ts)
    if not start_time or not end_time:
        print(f"⚠️ Missing timestamps in chunk: start={start_time}, end={end_time}, chunk[0]={chunk[0]}")

    windows = set(e.get("window", "Unknown") for e in chunk)
    apps = set(e.get("application", "Unknown") for e in chunk)
    combined_events = "\n".join(e["description"] for e in chunk)

    prompt = f"""
Analyze the following employee activity logs and generate a concise summary.

Chunk metadata:
- Start time: {start_time or 'Unknown'}
- End time: {end_time or 'Unknown'}
- Windows involved: {', '.join(windows)}
- Applications used: {', '.join(apps)}

Activity logs:
{combined_events}

Focus on:
1. Main tasks and activities
2. Applications used and their purpose
3. Patterns and focus
4. Any notable behaviors or patterns

Provide a professional summary in 2-3 sentences.
"""
    config = MODEL_CONFIGS["summarization"]
    summary = robust_ollama_call(prompt, **config)
    return {"summary": summary, "start_time": start_time, "end_time": end_time}

def recursive_summarize(texts: List[str], model: str = "llama3.2:latest", max_len: int = 6000, depth: int = 0) -> str:

    combined = "\n".join(texts)
    if len(combined) <= max_len:
        prompt = f"""You are generating a daily report for an employee.
Below are summaries of their work sessions. Write a concise daily summary:

{combined}
"""
        messages = [
            {"role": "system", "content": "You are a helpful summarizer creating daily executive reports."},
            {"role": "user", "content": prompt},
        ]
        resp = ollama_chat_retry(model=model, messages=messages, temperature=MODEL_CONFIGS["daily_report"]["temperature"])
        return resp["message"]["content"]

    mid = max(1, len(texts) // 2)
    part1 = recursive_summarize(texts[:mid], model, max_len, depth + 1)
    part2 = recursive_summarize(texts[mid:], model, max_len, depth + 1)
    return recursive_summarize([part1, part2], model, max_len, depth + 1)

# --------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------

def classify_chunk_ollama(chunk_content: List[Dict[str, Any]], context: Optional[str] = None) -> Dict[str, Any]:
    event_texts = [event["description"] for event in chunk_content if "description" in event]
    if not event_texts:
        return ensure_schema({
            "primary_activity": {
                "category": "Other",
                "subcategory": "Insufficient Data",
                "confidence": 0.1,
                "reasoning": "No textual events available in chunk"
            }
        })

    combined_text = "\n".join(event_texts)
    context_prompt = f"\nContext from surrounding activity:\n{context}\n" if context else ""

    prompt = f"""
You are analyzing employee activity logs to understand work patterns.
{context_prompt}

Classify the following activity logs based ONLY on the textual descriptions of events.
Detect:
1. Primary work activity (what they spent most time on)
2. Secondary activities if present
3. 


 indicators (focus, task switching, deep work)
4. Key applications used

Return ONLY valid JSON in this exact format:
{{
    "primary_activity": {{
        "category": "Coding|Writing|Communication|Research|Admin|Tools|Break|Other",
        "subcategory": "detailed subcategory based on actual activity",
        "confidence": 0.85,
        "reasoning": "explanation based on events"
    }},
    "secondary_activities": [
        {{
            "category": "Communication",
            "subcategory": "e.g., Slack messaging",
            "confidence": 0.7,
            "time_percentage": 0.2
        }}
    ],
    "productivity_indicators": {{
        "focus_level": 0.8,
        "task_switching_frequency": "low|medium|high",
        "deep_work_detected": true
    }},
    "key_applications": ["vscode", "chrome"],

}}

Activity Logs:
{combined_text}

Notes:
- Detect activities dynamically from the text, do NOT just choose the given categories.
- Use categories only as guidance, infer what the employee actually did.
- Focus on patterns and context, not individual keystrokes.
"""

    config = MODEL_CONFIGS["classification"]
    raw_response = robust_ollama_call(prompt, **config)
    parsed = safe_parse_json(raw_response)
    return ensure_schema(parsed)


def classify_with_temporal_context(chunks: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:

    classifications: List[Dict[str, Any]] = []

    for i, chunk in enumerate(chunks):
        context = ""

        # Include last 3 events from previous chunk as context
        if i > 0 and chunks[i - 1]:
            prev_events = chunks[i - 1][-3:]
            prev_texts = "\n".join(event.get("description", "") for event in prev_events)
            context += f"Previous activity:\n{prev_texts}\n"

        # Include first 3 events from next chunk as context
        if i < len(chunks) - 1 and chunks[i + 1]:
            next_events = chunks[i + 1][:3]
            next_texts = "\n".join(event.get("description", "") for event in next_events)
            context += f"Next activity:\n{next_texts}\n"

        # Classify dynamically based on chunk text + temporal context
        classification = classify_chunk_ollama(chunk, context=context)
        classifications.append(classification)

    return classifications

def calculate_score(classifications: List[Dict[str, Any]],
                    chunks: Optional[List[List[Dict[str, Any]]]] = None,
                    kpi_doc: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    """
    Calculate overall productivity score, deep work time, and average productivity.
    If kpi_doc is provided, derive deep work and focus from KPI instead of classifications.
    """
    if not classifications and not kpi_doc:
        return {"overall_score": 0.0, "deep_work_time": 0.0, "average_productivity": 0.0}

    total_score = 0.0
    deep_work_time = 0.0
    total_entries = len(classifications) if classifications else 1

    if kpi_doc:
        # Use KPI-derived metrics
        total_active_sec = kpi_doc.get("active_sec", 0)
        total_idle_sec = kpi_doc.get("idle_sec", 0)
        total_pause_sec = kpi_doc.get("pause_sec", 0)
        sessions = kpi_doc.get("sessions", [])

        active_total = total_active_sec + total_idle_sec + total_pause_sec
        # Deep work: sum session durations >= 15 minutes
        deep_work_time = sum(
            s.get("active_sec", 0) / 60 for s in sessions if s.get("active_sec", 0) >= 15 * 60
        )

        # Approximate overall productivity as focus ratio
        focus_level = total_active_sec / active_total if active_total > 0 else 0.0
        overall_score = min(focus_level, 1.0)

        return {
            "overall_score": round(overall_score, 2),
            "deep_work_time": round(deep_work_time, 1),
            "average_productivity": round(overall_score, 2),
        }

    # Fallback: use classification-based scoring
    for idx, cls in enumerate(classifications):
        primary = cls.get("primary_activity", {})
        category = primary.get("category", "Other")
        confidence = float(primary.get("confidence", 0.0))
        activity_weight = PRODUCTIVITY_WEIGHTS.get(category, 0.3)
        chunk_score = activity_weight * confidence
        total_score += chunk_score

        if cls.get("productivity_indicators", {}).get("deep_work_detected", False):
            if chunks and idx < len(chunks) and len(chunks[idx]) >= 2:
                start_ts = chunks[idx][0].get("timestamp")
                end_ts = chunks[idx][-1].get("timestamp")
                if isinstance(start_ts, datetime) and isinstance(end_ts, datetime):
                    deep_work_time += (end_ts - start_ts).total_seconds() / 60
                else:
                    deep_work_time += 1
            else:
                deep_work_time += 1

    overall_score = total_score / total_entries if total_entries else 0.0
    return {
        "overall_score": min(overall_score, 1.0),
        "deep_work_time": round(deep_work_time, 1),
        "average_productivity": total_score / total_entries if total_entries else 0.0,
    }


def analyze_work_patterns(classifications: List[Dict[str, Any]],
                          chunks: Optional[List[List[Dict[str, Any]]]],
                          es,
                          employee_id: str,
                          date_str: str) -> Dict[str, Any]:
    if not classifications:
        return {}

    # Fetch KPI summary
    kpi_doc = None
    try:
        date_str_norm = normalize_date(date_str).strftime("%Y-%m-%d")
        kpi_doc = es.get(index="employee_kpi_summary3", id=f"{employee_id}-{date_str_norm}")["_source"]
        total_active_sec = kpi_doc.get("active_sec", 0)
        total_idle_sec = kpi_doc.get("idle_sec", 0)
        total_pause_sec = kpi_doc.get("pause_sec", 0)
    except Exception as e:
        print(f"⚠️ Failed to fetch KPI summary for {employee_id}-{date_str}: {e}")
        total_active_sec = total_idle_sec = total_pause_sec = 0

    total_sec = total_active_sec + total_idle_sec + total_pause_sec
    total_chunks = len(classifications)
    category_counts: Dict[str, int] = {}
    high_focus_chunks = 0

    for cls in classifications:
        primary = cls.get("primary_activity", {})
        category = primary.get("category", "Other")
        category_counts[category] = category_counts.get(category, 0) + 1

        indicators = cls.get("productivity_indicators", {})
        if indicators.get("focus_level", 0) > 0.7:
            high_focus_chunks += 1

    work_distribution = {
        cat: (count / total_chunks) * 100.0 for cat, count in category_counts.items()
    }

    avg_focus = sum(
        float(cls.get("productivity_indicators", {}).get("focus_level", 0.0))
        for cls in classifications
    ) / max(total_chunks, 1)

    deep_work_time = 0.0
    overall_score = 0.0
    if kpi_doc:
        sessions = kpi_doc.get("sessions", [])
        deep_work_time = sum(
            s.get("active_sec", 0) for s in sessions if s.get("active_sec", 0) >= 15 * 60
        ) / 60.0
        overall_score = (total_active_sec / total_sec) if total_sec > 0 else 0.0

    return {
        "total_chunks_analyzed": total_chunks,
        "work_distribution": work_distribution,
        "time_allocation": {
            "total_active_minutes": round(total_active_sec / 60, 1),
            "total_idle_minutes": round(total_idle_sec / 60, 1),
            "total_pause_minutes": round(total_pause_sec / 60, 1),
        },
        "productivity_metrics": {
            "average_focus_level": round(avg_focus, 2),
            "deep_work_time_minutes": round(deep_work_time, 1),
            "high_focus_percentage": round((high_focus_chunks / total_chunks) * 100.0, 1),
            "overall_productivity_score": round(overall_score, 2),
        },
    }

def enhanced_daily_rollup(es, employee_id: str, date_str: str, work_patterns: Dict[str, Any]) -> str:
    date_norm = normalize_date(date_str).strftime("%Y-%m-%d")

    summaries = fetch_chunk_summaries(es, employee_id, date_norm)
    if not summaries:
        print(f"⚠️ No summaries found for {employee_id} on {date_norm}")
        return "No activity data available for this date."

    patterns = work_patterns.get("work_distribution", {})
    time_alloc = work_patterns.get("time_allocation", {})
    total_active = max(time_alloc.get("total_active_minutes", 1), 1)  # avoid div by zero
    productivity = work_patterns.get("productivity_metrics", {})
    deep_work_pct = (productivity.get("deep_work_time_minutes", 0) / total_active) * 100


    pattern_context = f"""
Work Pattern Analysis for {date_str}:
- Total active time: {time_alloc.get('total_active_minutes', 0):.0f} minutes
- Productivity score: {productivity.get('overall_productivity_score', 0):.2f}/1.0
- Deep work time: {deep_work_pct:.1f}%
- Focus level: {productivity.get('average_focus_level', 0):.2f}/1.0

Activity Distribution:
{chr(10).join(f"- {cat.title()}: {pct:.1f}%" for cat, pct in sorted(patterns.items(), key=lambda x: x[1], reverse=True))}
"""

    combined_summaries = "\n\n".join(f"Session {i+1}: {summary}" for i, summary in enumerate(summaries))

    prompt = f"""Create a comprehensive daily work report for employee {employee_id}.

{pattern_context}

Session-by-Session Activity:
{combined_summaries}

Generate a professional daily report that includes:

1. **Executive Summary** (2-3 sentences of key accomplishments)
2. **Main Activities** (primary tasks and time allocation)
3. **Productivity Insights** (focus patterns, work quality indicators)
4. **Notable Observations** (any patterns, concerns, or highlights)

Keep the report factual, professional, and focused on work outcomes. Aim for 150-200 words total."""
    config = MODEL_CONFIGS["daily_report"]
    final_summary = robust_ollama_call(prompt, **config)

    report_doc = {
        "employee_id": employee_id,
        "daily_summary": final_summary,
        "work_patterns": work_patterns,
        "summary_count": len(summaries),
        "chunk_summaries": summaries,   # keep per-session summaries
        "totals": {
            "active": work_patterns.get("time_allocation", {}).get("total_active_minutes", 0),
            "idle": work_patterns.get("time_allocation", {}).get("total_idle_minutes", 0),
        }
    }
    save_daily_report(es, employee_id, date_norm, report_doc)


    return final_summary


def summarize_employee_day(es, employee_id: str, date_str: str, max_workers: int = 4) -> Optional[Dict[str, Any]]:
    start_time = time.time()

    if not is_ollama_running():
        start_ollama()
        if not is_ollama_running():
            print("⚠️ Failed to start Ollama. Skipping summarization.")
            return None

    logs = fetch_logs_for_employee(es, "employee_activity", employee_id, date_str)
    if not logs:
        print(f"⚠️ No logs found for {employee_id} on {date_str}")
        return None
    chunks = adaptive_chunk_logs(logs)
    print(f"📊 Created {len(chunks)} high-quality chunks from {len(logs)} logs")
    save_daily_chunks(es, employee_id, date_str, chunks)

    print("🔍 Classifying activities...")
    try:
        classifications = classify_with_temporal_context(chunks)
        print(f"✅ Classified {len(classifications)} chunks")
    except Exception as e:
        print(f"❌ Classification failed: {e}")
        return None

    print("📝 Summarizing chunks...")
    summaries: List[Optional[Dict[str, Any]]] = [None] * len(chunks)
    errors: List[str] = []

    def process_chunk_summary(i: int, chunk: List[Dict[str, Any]]) -> Tuple[int, Dict[str, Any]]:
        try:
            result = summarize_chunk_ollama(chunk)
            return i, result
        except Exception as ex:
            err_msg = f"⚠️ Failed to summarize chunk {i}: {ex}"
            print(err_msg)
            print("Chunk content:", chunk)
            return i, {"summary": f"ERROR: could not summarize", "start_time": None, "end_time": None}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_chunk_summary, i, chunk) for i, chunk in enumerate(chunks)]
        for future in as_completed(futures):
            i, result = future.result()
            summaries[i] = result
            if result["summary"].startswith("ERROR:"):
                errors.append(f"Chunk {i}: summarization error")

    valid_summaries = [s["summary"] for s in summaries if isinstance(s, dict) and not s["summary"].startswith("ERROR:")]
    summary_docs = [
        {"summary": s["summary"], "start_time": s["start_time"], "end_time": s["end_time"]}
        for s in summaries if isinstance(s, dict)
    ]
    if errors:
        print(f"⚠️ {len(errors)} chunks had errors during summarization")

    print("📈 Analyzing work patterns...")
    work_patterns = analyze_work_patterns(classifications, chunks, es, employee_id, date_str)

    try:
        if summary_docs:
            save_summaries(es, employee_id, date_str, summary_docs, chunks)
        save_classifications(es, employee_id, date_str, classifications, chunks)
        save_work_patterns(es, employee_id, date_str, work_patterns)
        print("💾 Saved classifications and work patterns")
    except Exception as e:
        print(f"❌ Failed to save intermediate results: {e}")

    print("📋 Creating daily report...")
    try:
        daily_summary = enhanced_daily_rollup(es, employee_id, date_str, work_patterns)
        print("✅ Daily report generated")
    except Exception as e:
        print(f"❌ Failed to generate daily report: {e}")
        daily_summary = "Failed to generate daily summary due to processing error."

    processing_time = time.time() - start_time
    result = {
        "employee_id": employee_id,
        "date": date_str,
        "processing_stats": {
            "total_chunks": len(chunks),
            "successful_summaries": len(valid_summaries),
            "classification_count": len(classifications),
            "errors": len(errors),
            "processing_time_seconds": round(processing_time, 2),
        },
        "summaries": valid_summaries,
        "classifications": classifications,
        "work_patterns": work_patterns,
        "daily_summary": daily_summary,
    }
    print(f"🎉 Completed processing for {employee_id} on {date_str} in {processing_time:.1f}s")
    return result

def batch_process_employees(es, employee_ids: List[str], date_str: str, max_workers: int = 2) -> Dict[str, Any]:
    print(f"🏭 Starting batch processing for {len(employee_ids)} employees on {date_str}")

    results: Dict[str, Any] = {}
    errors: Dict[str, str] = {}

    def process_employee(emp_id: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[str]]:
        try:
            result = summarize_employee_day(es, emp_id, date_str, max_workers=2)
            return emp_id, result, None
        except Exception as e:
            return emp_id, None, str(e)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_employee, emp_id) for emp_id in employee_ids]
        for future in as_completed(futures):
            emp_id, result, error = future.result()
            if error or result is None:
                errors[emp_id] = error or "Unknown error"
                print(f"❌ Failed to process {emp_id}: {errors[emp_id]}")
            else:
                results[emp_id] = result
                print(f"✅ Processed {emp_id} successfully")

    batch_stats = {
        "total_employees": len(employee_ids),
        "successful_processing": len(results),
        "failed_processing": len(errors),
        "success_rate": (len(results) / len(employee_ids) * 100.0) if employee_ids else 0.0,
        "errors": errors,
    }

    print(f"📊 Batch processing complete: {len(results)}/{len(employee_ids)} employees processed")

    return {
        "results": results,
        "batch_stats": batch_stats,
        "processed_date": date_str,
    }

def generate_insights_report(es, employee_id: str,date_range_days:int, model_type: str = "daily_report" ) -> str:

    end_date = datetime.now()
    start_date = end_date - timedelta(days=date_range_days)

    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"employee_id.keyword": employee_id}},
                    {"range": {"date": {
                        "gte": start_date.strftime("%Y-%m-%d"),
                        "lte": end_date.strftime("%Y-%m-%d"),
                    }}},
                ]
            }
        },
        "sort": [{"date": {"order": "asc"}}],
        "size": 100,
    }

    try:
        resp = es.search(index="employee_daily_reports", body=body)
        reports = [hit["_source"] for hit in resp["hits"]["hits"]]

        if not reports:
            return f"No reports found for {employee_id} in the last {date_range_days} days"

        # Extract summaries and productivity scores
        daily_summaries = [report.get("daily_summary", "") for report in reports]
        productivity_scores = [
            report.get("work_patterns", {}).get("productivity_metrics", {}).get("overall_productivity_score", 0.0)
            for report in reports
        ]

        # Compute average productivity
        avg_productivity = float(np.mean(productivity_scores)) if productivity_scores else 0.0

        # Robust trend detection using linear slope
        trend_direction = "stable"
        if len(productivity_scores) > 1:
            x = np.arange(len(productivity_scores))
            slope = np.polyfit(x, productivity_scores, 1)[0]  # slope of best fit line
            if slope > 0.01:
                trend_direction = "improving"
            elif slope < -0.01:
                trend_direction = "declining"

         # Build LLM prompt
        prompt = f"""Generate a {date_range_days}-day insights report for employee {employee_id}.

Average Productivity Score: {avg_productivity:.2f}/1.0
Productivity Trend: {trend_direction}
Reports Available: {len(reports)} days

Daily Summaries:
{chr(10).join(f"Day {i+1}: {summary}" for i, summary in enumerate(daily_summaries))}

Create an executive insights report covering:
1. **Performance Overview** (key trends and patterns)
2. **Strengths & Achievements** (what they're doing well)
3. **Areas for Improvement** (productivity opportunities)
4. **Recommendations** (actionable suggestions)

Keep it professional and constructive."""
    
        # Adjust length for weekly vs monthly
        if date_range_days > 14:
            prompt += " Make it 300-500 words."

        config = MODEL_CONFIGS.get(model_type, MODEL_CONFIGS["daily_report"])
        insights = robust_ollama_call(prompt, **config)

        # Save report to Elasticsearch
        doc = {
            "employee_id": employee_id,
            "report_type": f"{date_range_days}_day_insights",
            "date_range": {
                "start": start_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d"),
            },
            "insights_report": insights,
            "trend_analysis": {
                "average_productivity": avg_productivity,
                "trend_direction": trend_direction,
                "days_analyzed": len(reports),
            },
            "generated_at": datetime.utcnow().isoformat(),
        }
        es.index(
            index="employee_insights_reports",
            id=f"{employee_id}-{date_range_days}day-{end_date.strftime('%Y%m%d')}",
            body=doc
        )

        return insights

    except Exception as e:
        print(f"❌ Failed to generate insights report: {e}")
        return f"Error generating insights report: {str(e)}"

