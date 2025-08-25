from collections import defaultdict
from ollama import chat
from LLM.prompts import build_prompt
import json
def classify_tasks(logs):
    grouped_by_employee = defaultdict(list)
    summaries = []

    for log in logs:
        emp_id = log.get("employee_id")
        grouped_by_employee[emp_id].append(log)

    for emp_id, emp_logs in grouped_by_employee.items():
        prompt = build_prompt(emp_logs)
        try:
            response = chat(model='tinyllama', messages=[
                {"role": "user", "content": prompt}
            ])
            content = response['message']['content'].strip()

            # Try to isolate the first valid JSON object
            start = content.find('{')
            end = content.rfind('}') + 1
            if start == -1 or end == -1:
                raise ValueError("No JSON object found in response.")

            json_str = content[start:end]
            parsed = json.loads(json_str)

            summaries.append({
                "employee_id": emp_id,
                "task_categories": parsed.get("categories", []),
                "summary": parsed.get("summary", ""),
            })

        except Exception as e:
            print(f"[ERROR] LLaMA output for {emp_id}:\n{response.get('message', {}).get('content', '')}")
            print(f"[Exception] {e}")
            continue

    return summaries
