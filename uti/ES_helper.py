from elasticsearch import Elasticsearch
from datetime import datetime, timezone
import json

# Convert a date string to epoch milliseconds
def to_epoch_ms(dt):
    """Convert datetime or string (dd/mm/yyyy) to epoch milliseconds."""
    if isinstance(dt, str):
        dt = datetime.strptime(dt, "%d/%m/%Y")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

es = Elasticsearch("http://localhost:9200")

# Fetch logs in a given date range using scroll API
def fetch_logs_in_range(start_dt, end_dt, index="employee-activity-logs"):

    start_ts = to_epoch_ms(start_dt)
    end_ts = to_epoch_ms(end_dt)

    query = {
        "query": {
            "range": {
                "timestamp": {
                    "gte": start_ts,
                    "lte": end_ts,
                    "format": "epoch_millis"
                }
            }
        },
        "sort": [{"timestamp": "asc"}],
        "size": 1000  # batch size
    }

    results = []
    resp = es.search(index=index, body=query, scroll='2m')
    scroll_id = resp['_scroll_id']
    hits = resp['hits']['hits']

    while hits:
        results.extend(hits)
        resp = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = resp['_scroll_id']
        hits = resp['hits']['hits']

    return [doc["_source"] for doc in results]
"""
def save_summary_to_elastic(summary, summary_type, timestamp):
    index_name = f"employee-summary-{summary_type}"
    ts = int(timestamp.timestamp() * 1000)

    for s in summary:
        doc = {
            "employee_id": s["employee_id"],
            "summary_type": summary_type,
            "timestamp": ts,
            "task_categories": s.get("task_categories", []),
            "summary": s.get("summary", ""),
        }
        es.index(index=index_name, body=doc)"""
def save_summary_to_elastic(summary, summary_type, timestamp):
    index_name = f"employee-summary-{summary_type}"
    ts = int(timestamp.timestamp() * 1000)

    # Just insert one merged summary, no loop
    doc = {
        "summary_type": summary_type,
        "timestamp": ts,
        "task_categories": summary.get("categories", []),
        "summary": summary.get("summary", ""),
        # Optional: "employee_id": "global" or some identifier if needed
    }
    # Optional: give it a fixed ID so it's unique per day
    es.index(index=index_name, body=doc)




