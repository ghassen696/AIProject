# reporting_pipeline.py
import argparse
from datetime import datetime, timedelta
from elastic_utils import connect_elasticsearch, fetch_all_employees
from summarization import generate_insights_report

INDEX = "employee_activity"

def run_reporting_pipeline(report_type: str):
    days = 7 if report_type == "weekly" else 30
    model_type = f"{report_type}_report"

    es = connect_elasticsearch()
    start_date = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)
    end_date = int(datetime.utcnow().timestamp() * 1000)

    employee_ids = fetch_all_employees(es, INDEX, start_date, end_date)
    if not employee_ids:
        print(f"⚠️ No employees found in last {days} days")
        return

    print(f"📅 Running {report_type} pipeline for {len(employee_ids)} employees")
    for emp in employee_ids:
        report = generate_insights_report(es, emp, date_range_days=days, model_type=model_type)
        print(f"\n📊 {report_type.capitalize()} insights for {emp}:\n{report}\n")

    print(f"✅ {report_type.capitalize()} pipeline completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-type", choices=["weekly", "monthly"], default="weekly")
    args = parser.parse_args()

    run_reporting_pipeline(args.report_type)
