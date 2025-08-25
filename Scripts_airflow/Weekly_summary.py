from uti.ES_helper import fetch_logs_in_range, save_summary_to_elastic
from LLM.llama_classifier import classify_tasks
from datetime import datetime, timedelta

def run_weekly_summary():
    today = datetime.now()
    start = today - timedelta(days=7)
    end = today

    logs = fetch_logs_in_range(start, end)
    if not logs:
        print("No logs found for the past week.")
        return

    summary = classify_tasks(logs)
    save_summary_to_elastic(summary, summary_type='weekly', timestamp=end)

if __name__ == "__main__":
    run_weekly_summary()
