# jobs/workload_match.py
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from LLM_backend.classifier import classify_against_workload
from config import ELASTICSEARCH_URL

es = Elasticsearch(ELASTICSEARCH_URL, verify_certs=False)

def latest_workload():
    res = es.search(index="workloads", sort=[{"timestamp":{"order":"desc"}}], size=1)
    if not res["hits"]["hits"]:
        return None
    return res["hits"]["hits"][0]["_source"]["workload_text"]

def last_week_daily_tasks():
    now = datetime.utcnow()
    res = es.search(
        index="employee_tasks_daily",
        query={"range": {"start_ts": {"gte": (now - timedelta(days=7)).isoformat(), "lte": now.isoformat()}}},
        size=10000
    )
    return [h["_source"] for h in res["hits"]["hits"]]

def run_match(model="llama3.2"):
    wl = latest_workload()
    if not wl:
        print("No workload uploaded."); return
    daily = last_week_daily_tasks()
    if not daily:
        print("No daily tasks to match."); return

    classified = classify_against_workload(daily, wl, model=model)
    # Persist back by merging fields
    for src, match in zip(daily, classified[:len(daily)]):
        doc = {
            **src,
            "matched_category": match.get("matched_category"),
            "matched_task": match.get("matched_task"),
            "confidence": match.get("confidence"),
            "is_expected": match.get("is_expected", False),
        }
        es.index(index="employee_tasks_matched", document=doc)
    print(f"Matched {min(len(daily), len(classified))} tasks.")

if __name__ == "__main__":
    run_match()
