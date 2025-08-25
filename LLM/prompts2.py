from uti.Classification_cat import classification_categories

def build_prompt(employee_logs):
    joined_text = "\n".join([
        f"[{log['timestamp']}] ({log['application']}) {log.get('window', '')} - {log.get('text', '')}"
        for log in employee_logs
    ])

    category_list = "\n".join([f"- {cat}" for cat in classification_categories])

    return f"""
You are a productivity analysis assistant.

Here is a chronological list of application activity logs for an employee during a work period:
---
{joined_text}
---

From these logs:
1. Determine what kind of technical tasks they were doing.
2. Assign one or more categories from the following predefined list:

{category_list}

Return exactly one JSON object with these keys and formats:

{{
  "categories": [category1, category2, ...]
  "summary": "A concise paragraph summarizing the employee's technical work, no newlines"
}}

IMPORTANT: ONLY return a single JSON object. No explanations, no text, no code, no comments, nothing but JSON.

Example output:

{{
  "categories": [Data Engineering, Spark Streaming],
  "summary": "The employee worked primarily on developing and debugging Spark streaming jobs for real-time data processing."
}}


"""
