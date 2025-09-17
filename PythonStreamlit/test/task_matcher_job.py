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
"""from datetime import datetime
from elasticsearch import Elasticsearch
from .employee_profile_builder import append_daily_report_to_profile

es = Elasticsearch("http://localhost:9200")

def get_all_employee_ids_with_report_for_date(es, date_str, report_index="employee_daily_reports"):
    q = {
        "query": {"term": {"date": date_str}},
        "size": 10000,
        "_source": ["employee_id"]
    }
    res = es.search(index=report_index, body=q)
    return list({hit["_source"]["employee_id"] for hit in res["hits"]["hits"]})

today_str = datetime.utcnow().date().isoformat()
employee_ids = get_all_employee_ids_with_report_for_date(es, today_str)

print(f"📌 Found {len(employee_ids)} employees with reports for {today_str}")

for emp_id in employee_ids:
    append_daily_report_to_profile(es, emp_id, today_str)
"""