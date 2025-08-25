import sys
import os
# Allow imports from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
import json
from uti.ES_helper import fetch_logs_in_range, save_summary_to_elastic
from LLM.llama_classifier import classify_tasks
# Chunking utility
def chunk_logs(logs, chunk_size):
    for i in range(0, len(logs), chunk_size):
        yield logs[i:i + chunk_size]

def merge_results(results):
    all_categories = set()
    all_summaries = []

    for result in results:
        if isinstance(result, dict):
            cats = result.get("categories", [])
            summ = result.get("summary", "")
            all_categories.update(cats)
            all_summaries.append(summ)

    final_summary = " ".join(all_summaries)
    return {
        "categories": list(all_categories),
        "summary": final_summary
    }

def run_daily_summary():
    today = datetime.now()
    start_dt = today - timedelta(days=2)
    end_dt = today
    start = start_dt.strftime("%d/%m/%Y")
    end = end_dt.strftime("%d/%m/%Y")

    print(f"[INFO] Fetching logs from {start} to {end}")
    logs = fetch_logs_in_range(start, end)
    if not logs:
        print("[INFO] No logs found for the specified date range.")
        return

    print(f"[INFO] {len(logs)} logs fetched. Classifying in chunks...")

    # Split logs into chunks
    chunk_size = 3  # You can tune this depending on model limits
    chunked_results = []

    for idx, chunk in enumerate(chunk_logs(logs, chunk_size)):
        print(f"[INFO] Classifying chunk {idx + 1}...")
        results = classify_tasks(chunk)
        if not results:
            print(f"[WARNING] Chunk {idx + 1} returned no output.")
            continue
        for r in results:
            if isinstance(r, dict):
                chunked_results.append(r)


    # Merge results
    summary = merge_results(chunked_results)

    # Save to Elasticsearch
    print(f"[INFO] Saving merged summary...")
    if not summary:
        print("[WARNING] No valid summaries were returned by LLaMA.")
        return
    print("[DEBUG] Summary categories:", summary["categories"])
    print("[DEBUG] Summary text:", summary["summary"])

    #save_summary_to_elastic(summary, summary_type='daily', timestamp=start_dt)
    filename = f"daily_summary_{start_dt.strftime('%Y-%m-%d')}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"[SUCCESS] Summary saved to file: {filename}")
    print(f"[SUCCESS] Summary saved for {start}.")

if __name__ == "__main__":
    run_daily_summary()
