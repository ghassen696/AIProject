from datetime import datetime
from elasticsearch import Elasticsearch
from .employee_profile_builder import (
    append_daily_report_to_profile,
    match_task_to_employees,
    store_admin_feedback,
)

# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")

# ------------------------------
# 1. Get all employees with daily reports
# ------------------------------
def get_all_employee_ids(es, report_index="employee_daily_reports"):
    q = {
        "size": 0,
        "aggs": {
            "unique_employees": {
                "terms": {"field": "employee_id.keyword", "size": 10000}
            }
        }
    }
    res = es.search(index=report_index, body=q)
    return [b["key"] for b in res["aggregations"]["unique_employees"]["buckets"]]

employee_ids = get_all_employee_ids(es)
print(f"📌 Found {len(employee_ids)} employees in daily reports")

# Pick a date that exists in your daily reports
date_str = "2025-09-15"

# ------------------------------
# 2. Update / create profiles for ALL employees
# ------------------------------
for emp_id in employee_ids:
    append_daily_report_to_profile(es, emp_id, date_str)

# ------------------------------
# 3. Test task matching (compare against ALL employees)
# ------------------------------
tasks = [
    "work on a project with python",
    "Prepare project timeline for Q4",

]

for task in tasks:
    print("=" * 80)
    print(f"🔎 Matching task: {task}")
    candidates = match_task_to_employees(es, task, top_k=len(employee_ids), explain_top_n=3)
    
    if not candidates:
        print("⚠️ No suitable candidates found for this task.")
        continue

    for rank, c in enumerate(candidates, start=1):
        print(f"{rank}. {c['employee_id']} | Score: {c['final_score']:.3f}")
        if "llm_explanation" in c:
            print("   →", c["llm_explanation"])


# ------------------------------
# 4. (Optional) Simulate feedback
if tasks and employee_ids and candidates:
    chosen_task = tasks[0]
    top_candidate = candidates[0]
    store_admin_feedback(
        es,
        top_candidate["employee_id"],
        task_id="task_001",
        task_text=chosen_task,
        prediction_score=top_candidate["final_score"],
        feedback="accepted"
    )
else:
    print("⚠️ No candidates found, skipping feedback simulation.")
