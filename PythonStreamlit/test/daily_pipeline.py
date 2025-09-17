"""# daily_pipeline.py

from datetime import datetime, timedelta
from elastic_utils import (
    connect_elasticsearch,
    fetch_all_employees,
    fetch_logs_for_employee,
    save_daily_chunks,
)
from preprocessing import smart_chunk_logs
from summarization import summarize_employee_day


INDEX = "employee_activity"


def process_employee_day(es, employee_id: str, start_ms: int, end_ms: int, date_str: str):
"""
"""
    Fetch logs for one employee on a given day,
    preprocess into chunks, save them, and summarize.
"""
"""
    logs = fetch_logs_for_employee(es, INDEX, employee_id, start_ms, end_ms)
    if not logs:
        print(f"⚠️ No logs found for {employee_id} on {date_str}")
        return

    print(f"✅ {employee_id}: {len(logs)} logs")

    # 1. Preprocess & smart chunking
    chunks = smart_chunk_logs(
        logs,
        max_logs=200,        # max log lines per chunk
        max_chars=2000,      # max characters per chunk
        slice_minutes=30     # cut by 30-min time windows
    )

    for i, chunk in enumerate(chunks):
        chunk_text = "\n".join(chunk)
        print("="*60)
        print(f"Employee {employee_id} | {date_str} | Chunk {i+1}/{len(chunks)}")
        print(chunk_text[:800])  # Preview only first 800 chars
        print("="*60)

    # 2. Save normalized chunks into ES
    save_daily_chunks(es, employee_id, date_str, chunks)

    # 3. Generate summaries & rollups
    summarize_employee_day(es, employee_id, date_str)


def run_daily_pipeline():
    """"""
    Run the daily batch job for all employees.
    Default: process yesterday's logs.
    """"""
    es = connect_elasticsearch()

    # Example: fixed test date
    target_date = datetime(2025, 8, 28)

    # Auto-yesterday (uncomment in production)
    # target_date = datetime.utcnow().date() - timedelta(days=1)

    start_of_day = int(datetime.combine(target_date, datetime.min.time()).timestamp() * 1000)
    end_of_day = int(datetime.combine(target_date, datetime.max.time()).timestamp() * 1000)
    date_str = target_date.strftime("%Y-%m-%d")

    employees = fetch_all_employees(es, INDEX, start_of_day, end_of_day)
    print(f"🔎 Found {len(employees)} employees with logs on {date_str}")

    for emp in employees:
        process_employee_day(es, emp, start_of_day, end_of_day, date_str)


if __name__ == "__main__":
    run_daily_pipeline()
"""
# daily_pipeline.py
from datetime import datetime, timedelta
from .elastic_utils import connect_elasticsearch, fetch_all_employees
from .summarization import batch_process_employees

INDEX = "employee_activity"

def run_daily_pipeline():
    es = connect_elasticsearch()
    
    # Process yesterday’s data (or adjust as needed)
    #target_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    target_date = datetime(2025, 9, 15)
    start_of_day = int(datetime.combine(target_date, datetime.min.time()).timestamp() * 1000)
    end_of_day = int(datetime.combine(target_date, datetime.max.time()).timestamp() * 1000)
    date_str = target_date.strftime("%Y-%m-%d")

    employee_ids = fetch_all_employees(es, INDEX, start_of_day, end_of_day) 
    if not employee_ids:
        print("⚠️ No employees found in ES")
        return
    
    print(f"📅 Running daily pipeline for {target_date}, {len(employee_ids)} employees")
    batch_result = batch_process_employees(es, employee_ids, target_date, max_workers=3)
    
    print("✅ Daily pipeline finished")
    return batch_result

if __name__ == "__main__":
    run_daily_pipeline()
