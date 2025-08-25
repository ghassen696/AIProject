from elasticsearch import Elasticsearch , helpers
from datetime import datetime
import uuid
from datetime import datetime

def connect_elasticsearch():
    return Elasticsearch("http://localhost:9200")  # adjust if needed


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


def fetch_logs_for_employee(es, index, employee_id, start, end):
    """
    Fetch ALL logs for a given employee in the given time range.
    Scroll-safe version (handles >10k logs).
    """
    query = {
        "size": 1000,
        "query": {
            "bool": {
                "must": [
                    {"term": {"employee_id": employee_id}},
                    {"range": {"timestamp": {"gte": start, "lte": end}}}
                ]
            }
        },
        "sort": [{"timestamp": {"order": "asc"}}]
    }

    resp = es.search(index=index, body=query, scroll="2m")
    scroll_id = resp.get("_scroll_id")
    hits = resp["hits"]["hits"]

    logs = []
    while hits:
        for h in hits:
            logs.append(h["_source"])

        resp = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = resp.get("_scroll_id")
        hits = resp["hits"]["hits"]

    return logs

def save_daily_chunks(es, employee_id: str, date_str: str, chunks: list):
    """
    Save chunked logs into Elasticsearch with metadata.
    Each chunk gets:
      - employee_id
      - date
      - chunk_id
      - chunk_size
      - start_time, end_time (from logs)
      - content (list of normalized logs)
    """
    actions = []
    for i, chunk in enumerate(chunks):
        if not chunk:
            continue

        # Extract timestamps (our normalize_event used [timestamp] prefix)
        def parse_ts(line: str) -> int:
            from datetime import datetime
            try:
                ts_str = line.split("]")[0][1:]
                dt = datetime.fromisoformat(ts_str)
                return int(dt.timestamp() * 1000)
            except:
                return None

        timestamps = [parse_ts(line) for line in chunk if parse_ts(line)]
        start_ts = min(timestamps) if timestamps else None
        end_ts = max(timestamps) if timestamps else None

        doc = {
            "employee_id": employee_id,
            "date": date_str,
            "chunk_id": f"{employee_id}-{date_str}-chunk{i+1}",
            "chunk_index": i+1,
            "chunk_size": len(chunk),
            "start_time": start_ts,
            "end_time": end_ts,
            "content": chunk,
        }

        actions.append({
            "_index": "employee_daily_chunks",
            "_id": str(uuid.uuid4()),
            "_source": doc
        })

    if actions:
        helpers.bulk(es, actions)
        print(f"💾 Saved {len(actions)} chunks for {employee_id} on {date_str}")