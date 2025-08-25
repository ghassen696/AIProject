# LLM_backend/classifier.py
from .ollama_client import ask_json

CLASSIFY_PROMPT = """You are a productivity classifier.
You will receive:
1) A workload document with tasks the manager expects.
2) A list of employee summarized tasks (with durations).

For each employee task, map it to the closest workload task/category.
Return a JSON array where each item is:
{
  "matched_category": "category-name-or-task",
  "matched_task": "closest workload line/desc",
  "confidence": 0-1,
  "is_expected": true/false
}

Use concise categories; if no good match, mark is_expected=false with low confidence.
Workload:
<<<
{workload}
>>>
Employee tasks:
<<<
{employee_tasks}
>>>
"""

def classify_against_workload(employee_tasks, workload_text, model: str = "llama3.2"):
    prompt = CLASSIFY_PROMPT.format(workload=workload_text, employee_tasks=employee_tasks)
    out = ask_json(model, prompt)
    # normalize
    if isinstance(out, dict):
        out = [out]
    return out if isinstance(out, list) else []
