# jobs/daily_pipeline.py
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from LLM_backend.summarizer import summarize_logs
from config import ELASTICSEARCH_URL

es = Elasticsearch(ELASTICSEARCH_URL, verify_certs=False)

def fetch_logs(start, end, employee_id=None, batch_size=5000, index="employee_activity"):
    must = [{"range": {"timestamp": {"gte": start.isoformat(), "lte": end.isoformat()}}}]
    if employee_id:
        must.append({"term": {"employee_id": employee_id}})
    query = {"bool": {"must": must}}

    res = es.search(index=index, query=query, size=batch_size, scroll="2m")
    scroll_id = res.get("_scroll_id")
    records = [h["_source"] for h in res["hits"]["hits"]]

    while True:
        res = es.scroll(scroll_id=scroll_id, scroll="2m")
        hits = res["hits"]["hits"]
        if not hits: break
        records.extend([h["_source"] for h in hits])
    return records

def store_daily_tasks(summaries):
    for s in summaries:
        es.index(index="employee_tasks_daily", document=s)

def run_daily(start=None, end=None, model="llama3.2", employee_id=None):
    end = end or datetime.utcnow()
    start = start or (end - timedelta(days=1))
    records = fetch_logs(start, end, employee_id=employee_id)
    if not records:
        print("No logs for window."); return
    summaries = summarize_logs(records, model=model, chunk_size=80)
    store_daily_tasks(summaries)
    print(f"Stored {len(summaries)} daily tasks.")

if __name__ == "__main__":
    run_daily()
