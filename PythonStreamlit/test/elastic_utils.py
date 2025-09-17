from elasticsearch import Elasticsearch , helpers
from datetime import datetime, timedelta,timezone
import uuid
from typing import List, Dict, Any, Optional, Tuple
from .preprocessing import normalize_date
from .config import ES_PROFILE_INDEX, ES_TASK_MATCH_INDEX

def connect_elasticsearch():
    return Elasticsearch("http://localhost:9200")  # adjust if needed



def fetch_employee_kpis(es,employee_id: str, date: str):
    """
    Fetch daily KPIs for an employee from Elasticsearch.
    """
    resp = es.search(
        index="employee_kpi_summary3",
        body={
            "query": {
                "bool": {
                    "must": [
                        {"term": {"employee_id": employee_id}},
                        {"term": {"date": date}}
                    ]
                }
            }
        }
    )
    if resp["hits"]["hits"]:
        return resp["hits"]["hits"][0]["_source"]
    return None


def fetch_all_employees(es, index, start, end):
    """
    Get distinct employee_ids with logs in the given time range.
    Scroll-safe version (works even with >10k logs).
    """
    query = {
        "size": 1000,
        "_source": ["employee_id"],
        "query": {
            "range": {
                "timestamp": {"gte": start, "lte": end}
            }
        }
    }

    resp = es.search(index=index, body=query, scroll="2m")
    scroll_id = resp.get("_scroll_id")
    hits = resp["hits"]["hits"]

    employees = set()
    while hits:
        for h in hits:
            employees.add(h["_source"]["employee_id"])

        resp = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = resp.get("_scroll_id")
        hits = resp["hits"]["hits"]

    return list(employees)


def fetch_logs_for_employee(es, index, employee_id, date_str, size=10000):
    date_str = normalize_date(date_str)
    start_dt = datetime.combine(date_str, datetime.min.time(), tzinfo=timezone.utc)
    end_dt   = datetime.combine(date_str, datetime.max.time(), tzinfo=timezone.utc)

    start = int(start_dt.timestamp() * 1000)
    end   = int(end_dt.timestamp() * 1000)

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"employee_id.keyword": employee_id}},
                    {"range": {"timestamp": {"gte": start, "lte": end}}}
                ]
            }
        },
        "sort": [{"timestamp": {"order": "asc"}}]
    }

    resp = es.search(index=index, body=query, size=size, scroll="2m")
    scroll_id = resp.get("_scroll_id")
    logs = resp["hits"]["hits"]
    all_logs = [h["_source"] for h in logs]

    while True:
        resp = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = resp.get("_scroll_id")
        hits = resp["hits"]["hits"]
        if not hits:
            break
        all_logs.extend([h["_source"] for h in hits])

    return all_logs

def sanitize_for_es(obj):
    """
    Recursively convert datetimes to ISO strings so Elasticsearch can index them.
    Works on dicts, lists, and scalars.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, list):
        return [sanitize_for_es(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: sanitize_for_es(v) for k, v in obj.items()}
    else:
        return obj

def save_daily_chunks(es, employee_id: str, date_str: str, chunks: list):
    """
    Save chunked logs into Elasticsearch with metadata.
    Fully recursive datetime sanitization to prevent indexing errors.
    Each chunk contains:
      - employee_id
      - date
      - chunk_id
      - chunk_index
      - chunk_size
      - start_time, end_time
      - content (list of normalized logs)
    """
    date_str = normalize_date(date_str)

    def recursive_sanitize(obj):
        """Convert all datetime objects in nested structures to ISO strings."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, list):
            return [recursive_sanitize(i) for i in obj]
        elif isinstance(obj, dict):
            return {k: recursive_sanitize(v) for k, v in obj.items() if v is not None}
        else:
            return obj

    actions = []

    for i, chunk in enumerate(chunks):
        if not chunk:
            continue

        # Extract timestamps for chunk metadata
        timestamps = [e.get("timestamp") for e in chunk if e.get("timestamp")]
        start_ts = min(timestamps) if timestamps else None
        end_ts = max(timestamps) if timestamps else None

        doc = {
            "employee_id": employee_id,
            "date": date_str,
            "chunk_id": f"{employee_id}-{date_str}-chunk{i+1}",
            "chunk_index": i + 1,
            "chunk_size": len(chunk),
            "start_time": recursive_sanitize(start_ts) if start_ts else None,
            "end_time": recursive_sanitize(end_ts) if end_ts else None,
            "content": [recursive_sanitize(e) for e in chunk],
        }

        actions.append({
            "_index": "employee_daily_chunks",
            "_id": str(uuid.uuid4()),
            "_source": doc
        })

    if actions:
        try:
            helpers.bulk(es, actions, refresh=True)
            print(f"💾 Saved {len(actions)} chunks for {employee_id} on {date_str}")
        except Exception as e:
            print(f"⚠️ Failed to save chunks for {employee_id} on {date_str}: {e}")
            for doc in actions:
                try:
                    es.index(index="employee_daily_chunks", id=doc["_id"], document=doc["_source"], refresh=True)
                except Exception as single_err:
                    print(f"❌ Failed single doc {doc['_id']}: {single_err}")

def get_chunk_meta(chunks, i):
    """
    Return chunk metadata (dict) if available, otherwise {}.
    Supports:
      - full dict chunks from Elasticsearch
      - ES hits with _source
      - raw text chunks (list of strings)
    """
    if i < len(chunks):
        chunk = chunks[i]

        # Case 1: ES hit with _source
        if isinstance(chunk, dict) and "_source" in chunk:
            return chunk["_source"]

        # Case 2: dict already in right shape
        if isinstance(chunk, dict):
            return chunk

    return {}



def save_classifications(es, employee_id: str, date_str: str, classifications: List[Dict[str, Any]], chunks: List[List[Dict[str, Any]]]) -> None:
    date_str = normalize_date(date_str).strftime("%Y-%m-%d")

    actions = []
    for i, classification in enumerate(classifications):
        doc = {
            "employee_id": employee_id,
            "date": date_str,
            "classification_id": f"{employee_id}-{date_str}-class{i+1}",
            "chunk_index": i + 1,
            "classification": sanitize_for_es(classification),
        }
        actions.append({
            "_index": "employee_daily_classifications",
            "_id": str(uuid.uuid4()),
            "_source": sanitize_for_es(doc),
        })

    if actions:
        try:
            helpers.bulk(es, actions, refresh=True)
            print(f"🏷️ Saved {len(actions)} classifications for {employee_id} on {date_str}")
        except Exception as e:
            print(f"⚠️ Failed to save classifications for {employee_id} on {date_str}: {e}")
            for action in actions:
                try:
                    es.index(index="employee_daily_classifications", id=action["_id"], document=action["_source"], refresh=True)
                except Exception as single_err:
                    print(f"❌ Failed single doc {action['_id']}: {single_err}")
def save_daily_report(es, employee_id: str, date_str: str, report: dict):
    """
    Save the final daily report (executive summary + totals + patterns + classifications).
    """
    date_str = normalize_date(date_str)

    doc = {
        "employee_id": employee_id,
        "date": date_str,
        "report_id": f"{employee_id}-{date_str}-report",
        **sanitize_for_es(report)  # sanitize nested datetimes
    }

    es.index(index="employee_daily_reports", id=str(uuid.uuid4()), document=doc, refresh=True)
    print(f"🗂️ Saved daily report for {employee_id} on {date_str}")

def save_work_patterns(es, employee_id: str, date_str: str, patterns: Dict[str, Any]) -> None:
    date_str = normalize_date(date_str)

    doc = {
        "employee_id": employee_id,
        "date": date_str,
        "patterns": sanitize_for_es(patterns),
        "analysis_timestamp": datetime.utcnow().isoformat(),
    }

    es.index(
        index="employee_work_patterns",
        id=f"{employee_id}-{date_str}-patterns",
        document=doc,
        refresh=True
    )
    print(f"📈 Saved work patterns for {employee_id} on {date_str}")

def save_summaries(es, employee_id: str, date_str: str, summaries: List[Dict[str, Any]], chunks: List[List[Dict[str, Any]]]) -> None:
    date_str = normalize_date(date_str).strftime("%Y-%m-%d")

    actions = []
    for i, summary_doc in enumerate(summaries):
        doc = {
            "employee_id": employee_id,
            "date": date_str,
            "summary_id": f"{employee_id}-{date_str}-summary{i+1}",
            "chunk_index": i + 1,
            "start_time": summary_doc["start_time"],
            "end_time": summary_doc["end_time"],
            "summary": sanitize_for_es(summary_doc["summary"]),
        }
        actions.append({
            "_index": "employee_daily_summaries",
            "_id": str(uuid.uuid4()),
            "_source": sanitize_for_es(doc),
        })

    if actions:
        try:
            helpers.bulk(es, actions, refresh=True)
            print(f"📝 Saved {len(actions)} summaries for {employee_id} on {date_str}")
        except Exception as e:
            print(f"⚠️ Failed to save summaries for {employee_id} on {date_str}: {e}")
            for action in actions:
                try:
                    es.index(index="employee_daily_summaries", id=action["_id"], document=action["_source"], refresh=True)
                except Exception as single_err:
                    print(f"❌ Failed single doc {action['_id']}: {single_err}")
                    
def fetch_chunk_summaries(es, employee_id: str, date_str: str) -> List[Dict[str, Any]]:
    """
    Fetch summaries for a given employee and date.
    Returns list of dicts with chunk_index, summary, start_time, end_time.
    """
    date_str = normalize_date(date_str).strftime("%Y-%m-%d")

    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"employee_id.keyword": employee_id}},
                    {"term": {"date": date_str}},
                ]
            }
        },
        "sort": [{"chunk_index": {"order": "asc"}}],
        "size": 1000,
    }

    resp = es.search(index="employee_daily_summaries", body=body)
    return [
        {
            "chunk_index": hit["_source"].get("chunk_index"),
            "summary": hit["_source"].get("summary"),
            "start_time": hit["_source"].get("start_time"),
            "end_time": hit["_source"].get("end_time"),
        }
        for hit in resp["hits"]["hits"]
    ]

def fetch_chunks_for_employee(es, employee_id: str, date_str: str) -> List[Dict[str, Any]]:
    """
    Fetch pre-built chunks (if you store them) from Elasticsearch.
    """
    date_str = normalize_date(date_str)
  # normalize

    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"employee_id": employee_id}},
                    {"term": {"date": date_str}},
                ]
            }
        },
        "sort": [{"chunk_index": {"order": "asc"}}],
        "size": 1000,
    }
    resp = es.search(index="employee_daily_chunks", body=body)
    return [hit["_source"] for hit in resp["hits"]["hits"]]

#added code for the kpi integration
def fetch_kpi_summary(es, employee_id: str, date_str: str) -> Dict[str, Any]:
    """
    Return the KPI summary document for (employee_id, date).
    If none found return {}.
    """
    date_str = normalize_date(date_str).strftime("%Y-%m-%d")
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"employee_id.keyword": employee_id}},
                    {"term": {"date": date_str}}
                ]
            }
        },
        "size": 1
    }
    try:
        resp = es.search(index="employee_kpi_summary3", body=body)
        hits = resp.get("hits", {}).get("hits", [])
        if hits:
            return hits[0]["_source"]
    except Exception as e:
        print(f"⚠️ fetch_kpi_summary error: {e}")
    return {}

def create_employee_profiles_index(es, dims: int = 768, index_name: str = ES_PROFILE_INDEX):
    mapping = {
        "mappings": {
            "properties": {
                "employee_id": {"type": "keyword"},
                "name": {"type": "text"},
                "skills": {"type": "keyword"},
                "strengths": {"type": "keyword"},
                "productivity_metrics": {
                    "properties": {
                        "avg_active_pct": {"type": "float"},
                        "avg_idle_pct": {"type": "float"},
                        "avg_pause_pct": {"type": "float"},
                        "avg_keystrokes_per_hour": {"type": "float"},
                        "avg_window_switch_count": {"type": "integer"}
                    }
                },
                "daily_reports": {
                    "type": "nested",
                    "properties": {
                        "date": {"type": "date"},
                        "summary": {"type": "text"},
                        "embedded_summary": {"type": "dense_vector", "dims": dims}
                    }
                },
                "profile_embedding": {"type": "dense_vector", "dims": dims},
                "current_load": {"type": "integer"},
                "last_update": {"type": "date"}
            }
        }
    }
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)
        print(f"Created index {index_name} with dims={dims}")
    else:
        print(f"Index {index_name} already exists")
def create_task_match_index(es, index_name: str = ES_TASK_MATCH_INDEX):
    mapping = {
        "mappings": {
            "properties": {
                "task_id": {"type": "keyword"},
                "task_text": {"type": "text"},
                "employee_id": {"type": "keyword"},
                "semantic_score": {"type": "float"},
                "final_score": {"type": "float"},
                "timestamp": {"type": "date"},
                "accepted": {"type": "boolean"},
                "feedback": {"type": "text"},
                "time_spent_minutes": {"type": "float"}
            }
        }
    }
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)

def save_task_match(es, task_id: str, task_text: str, candidate: dict, index_name: str = ES_TASK_MATCH_INDEX):
    doc = {
        "task_id": task_id,
        "task_text": task_text,
        "employee_id": candidate["employee_id"],
        "semantic_score": candidate["semantic_score"],
        "final_score": candidate["final_score"],
        "timestamp": datetime.utcnow().isoformat(),
        "accepted": None,
        "feedback": None
    }
    es.index(index=index_name, id=f"{task_id}-{candidate['employee_id']}", body=doc)
