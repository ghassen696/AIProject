import ollama
from elasticsearch import helpers
import uuid
from datetime import datetime





def fetch_chunks_for_employee(es, employee_id, date_str):
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"employee_id": employee_id}},
                    {"term": {"date": date_str}}
                ]
            }
        },
        "sort": [{"chunk_index": "asc"}],
        "size": 1000
    }
    resp = es.search(index="employee_daily_chunks", body=body)
    return [hit["_source"] for hit in resp["hits"]["hits"]]

def recursive_chunk_summary(texts, model="llama3.2:1b", max_len=3500, depth=0):
    """
    Recursively summarize a list of log lines until the combined text fits max_len.
    """
    combined = "\n".join(texts)

    # Base case: text fits in model context
    if len(combined) <= max_len:
        prompt = f"""You are analyzing employee activity logs.
    Summarize the following logs into a clear description of what the employee did:

    {combined}
    """
        response = ollama.chat(model=model, messages=[
            {"role": "system", "content": "You are a helpful work activity summarizer."},
            {"role": "user", "content": prompt}
        ])
        return response["message"]["content"]

    # Recursive case: split and summarize smaller parts
    mid = len(texts) // 2
    part1 = recursive_chunk_summary(texts[:mid], model, max_len, depth+1)
    part2 = recursive_chunk_summary(texts[mid:], model, max_len, depth+1)

    # Summarize the two partial summaries together
    return recursive_chunk_summary([part1, part2], model, max_len, depth+1)

def summarize_chunk_ollama(chunk_content: list) -> str:
    return recursive_chunk_summary(chunk_content)


def save_summaries(es, employee_id, date_str, summaries):
    actions = []
    for i, summary in enumerate(summaries):
        doc = {
            "employee_id": employee_id,
            "date": date_str,
            "summary_id": f"{employee_id}-{date_str}-summary{i+1}",
            "chunk_index": i+1,
            "summary": summary,
        }
        actions.append({
            "_index": "employee_daily_summaries",
            "_id": str(uuid.uuid4()),
            "_source": doc
        })
    if actions:
        helpers.bulk(es, actions , refresh=True)
        print(f"📝 Saved {len(actions)} summaries for {employee_id} on {date_str}")

def recursive_summarize(texts, model="llama3.2:1b", max_len=6000, depth=0):
    """
    Recursively summarize a list of texts until the combined summary fits max_len.
    """
    combined = "\n".join(texts)

    # Base case: text fits in model context
    if len(combined) <= max_len:
        prompt = f"""You are generating a daily report for an employee.
Below are summaries of their work sessions. Write a **concise daily summary**:

{combined}
"""
        response = ollama.chat(model=model, messages=[
            {"role": "system", "content": "You are a helpful summarizer creating daily executive reports."},
            {"role": "user", "content": prompt}
        ])
        return response["message"]["content"]

    # Recursive case: split and summarize smaller batches
    batch_size = max(1, len(texts) // 2)  # split into 2 halves
    mid = len(texts) // 2
    part1 = recursive_summarize(texts[:mid], model, max_len, depth+1)
    part2 = recursive_summarize(texts[mid:], model, max_len, depth+1)

    return recursive_summarize([part1, part2], model, max_len, depth+1)

def rollup_daily_summary(es, employee_id, date_str):
    # Fetch all chunk summaries
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"employee_id": employee_id}},
                    {"term": {"date": date_str}}
                ]
            }
        },
        "sort": [{"chunk_index": "asc"}],
        "size": 1000
    }
    resp = es.search(index="employee_daily_summaries", body=body)
    summaries = [hit["_source"]["summary"] for hit in resp["hits"]["hits"]]

    if not summaries:
        print(f"⚠️ No chunk summaries for {employee_id} on {date_str}")
        return None

    # Recursive summarization instead of raw truncation
    final_summary = recursive_summarize(summaries)

    # Save to ES
    doc = {
        "employee_id": employee_id,
        "date": date_str,
        "daily_summary": final_summary,
    }
    es.index(index="employee_daily_reports", id=f"{employee_id}-{date_str}", body=doc)
    print(f"📊 Saved daily report for {employee_id} on {date_str}")
    return final_summary



def summarize_employee_day(es, employee_id, date_str):
    # 1. Summarize chunks
    chunks = fetch_chunks_for_employee(es, employee_id, date_str)
    summaries = []
    for chunk in chunks:
        try:
            summary = summarize_chunk_ollama(chunk["content"])
            summaries.append(summary)
        except Exception as e:
            print(f"⚠️ Failed to summarize chunk {chunk['chunk_id']}: {e}")

    save_summaries(es, employee_id, date_str, summaries)

    # 2. Roll up into daily summary
    rollup_daily_summary(es, employee_id, date_str)
