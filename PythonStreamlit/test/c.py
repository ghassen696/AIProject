# LLM/task_matcher.py

from typing import List, Dict
from elasticsearch import Elasticsearch
from datetime import datetime
import pandas as pd
from summarization import MODEL_CONFIGS, robust_ollama_call

# -----------------------------
# 1️⃣ Helper: Read tasks
# -----------------------------
def read_tasks_from_excel(file_path: str) -> List[str]:
    df = pd.read_excel(file_path)
    # Expect column "task_description"
    return df["task_description"].dropna().tolist()

def read_tasks_from_list(task_list: List[str]) -> List[str]:
    return [t.strip() for t in task_list if t.strip()]

# -----------------------------
# 2️⃣ Fetch employee insights
# -----------------------------
def get_employee_insights(es, employee_ids: List[str], period_days: int = 7) -> Dict[str, Dict]:
    insights = {}
    for emp_id in employee_ids:
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"employee_id.keyword": emp_id}},
                        {"range": {"date_range.start": {"gte": f"now-{period_days}d/d"}}}
                    ]
                }
            },
            "sort": [{"generated_at": {"order": "desc"}}],
            "size": 1
        }
        res = es.search(index="employee_insights_reports", body=body)
        hits = res.get("hits", {}).get("hits", [])
        if hits:
            insights[emp_id] = hits[0]["_source"]
    return insights

# -----------------------------
# 3️⃣ Generate suitability via LLM
# -----------------------------
def evaluate_task_suitability(task: str, employee_insights: Dict[str, Dict]) -> List[Dict]:
    results = []
    for emp_id, report in employee_insights.items():
        summary = report.get("insights_report", "")
        prompt = f"""
You are an expert task allocator. Based on the following employee insights, evaluate how suitable this employee is for the task below.

Task: {task}
Employee Insights: {summary}

Score the employee from 0.0 (not suitable) to 1.0 (highly suitable) and provide a short explanation.
Respond in JSON format: {{"score": <float>, "explanation": "<text>"}}
"""
        config = MODEL_CONFIGS.get("daily_report", {})
        try:
            llm_response = robust_ollama_call(prompt, **config)
            # Attempt to parse JSON from LLM
            candidate_data = eval(llm_response) if llm_response.strip().startswith("{") else {"score": 0.0, "explanation": llm_response}
        except Exception as e:
            candidate_data = {"score": 0.0, "explanation": f"LLM error: {e}"}

        results.append({
            "employee_id": emp_id,
            "score": float(candidate_data.get("score", 0.0)),
            "explanation": candidate_data.get("explanation", "")
        })
    # Sort by descending score
    return sorted(results, key=lambda x: x["score"], reverse=True)

# -----------------------------
# 4️⃣ Save recommendations to ES
# -----------------------------
def save_task_recommendations(es, task: str, candidates: List[Dict]):
    doc = {
        "task": task,
        "date_generated": datetime.utcnow().isoformat(),
        "candidates": candidates
    }
    doc_id = f"{task[:30]}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    try:
        es.index(index="task_assignment_suggestions", id=doc_id, body=doc)
        print(f"📥 Indexed recommendation for task '{task}' into task_assignment_suggestions")
    except Exception as e:
        print(f"❌ Failed to index task recommendation: {e}")

# -----------------------------
# 5️⃣ Main entry function
# -----------------------------
def match_tasks_to_employees(es, tasks: List[str], employee_ids: List[str], period_days: int = 7):
    employee_insights = get_employee_insights(es, employee_ids, period_days)
    for task in tasks:
        candidates = evaluate_task_suitability(task, employee_insights)
        save_task_recommendations(es, task, candidates)
        print(f"✅ Task matched: '{task}' | Top candidate: {candidates[0]['employee_id']} (score {candidates[0]['score']:.2f})")
