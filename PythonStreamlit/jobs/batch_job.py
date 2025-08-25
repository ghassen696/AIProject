import os
from datetime import datetime, timedelta
import pandas as pd
from elasticsearch import Elasticsearch
from ollama import chat
from config import ELASTICSEARCH_URL

# -----------------------------
# Connect to Elasticsearch
# -----------------------------
es = Elasticsearch(ELASTICSEARCH_URL, verify_certs=False)

# -----------------------------
# Fetch Logs from Elasticsearch
# -----------------------------
def fetch_logs(index="employee_activity", start_date=None, end_date=None, batch_size=5000):
    must = [{"match_all": {}}]
    if start_date and end_date:
        must.append({
            "range": {
                "timestamp": {
                    "gte": start_date.isoformat(),
                    "lte": end_date.isoformat()
                }
            }
        })

    query = {"bool": {"must": must}}
    res = es.search(index=index, query=query, size=batch_size, scroll="2m")
    scroll_id = res["_scroll_id"]
    records = [hit["_source"] for hit in res["hits"]["hits"]]

    while True:
        res = es.scroll(scroll_id=scroll_id, scroll="2m")
        hits = res["hits"]["hits"]
        if not hits:
            break
        records.extend([hit["_source"] for hit in hits])

    return pd.DataFrame(records)


# -----------------------------
# Summarize Logs with Llama
# -----------------------------
def summarize_logs(text_chunk):
    response = chat(model="llama3.2", messages=[
        {"role": "system", "content": "You are a log summarizer."},
        {"role": "user", "content": f"Summarize these employee logs:\n\n{text_chunk}"}
    ])
    return response["message"]["content"]


# -----------------------------
# Process and Save Summaries
# -----------------------------
def process_and_store(df, index_out="employee_tasks"):
    if df.empty:
        print("No logs to process.")
        return

    # Group by employee
    for emp_id, group in df.groupby("employee_id"):
        # Concatenate all text and window events
        combined_texts = []
        for _, row in group.iterrows():
            if row.get("event") in ["keystrokes", "window_switch", "clipboard"]:
                txt = f"[{row.get('timestamp')}] {row.get('event')} | {row.get('window')} | {row.get('text', '')}"
                combined_texts.append(txt)

        text_chunk = "\n".join(combined_texts[:200])  # cap at 200 logs for now (LLM limit)
        summary = summarize_logs(text_chunk)

        # Store back into Elasticsearch
        doc = {
            "employee_id": emp_id,
            "date": datetime.utcnow().isoformat(),
            "summary": summary,
            "raw_logs_count": len(group),
        }
        es.index(index=index_out, body=doc)
        print(f"✔ Processed employee {emp_id} with {len(group)} logs.")


# -----------------------------
# Main Entry Point
# -----------------------------
if __name__ == "__main__":
    # Default: yesterday’s logs
    start_date = datetime.utcnow() - timedelta(days=1)
    end_date = datetime.utcnow()

    print(f"🔄 Fetching logs from {start_date} to {end_date}...")
    df = fetch_logs(start_date=start_date, end_date=end_date)

    print(f"📦 Retrieved {len(df)} logs.")
    process_and_store(df)
    print("✅ Batch job finished.")
