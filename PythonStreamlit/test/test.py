"""import os
import ollama

os.environ["OLLAMA_HOST"] = "http://127.0.0.1:11434"

response = ollama.chat(
    model="llama3.2:1b",
    messages=[{"role": "system", "content": "You are a helpful summarizer."},
              {"role": "user", "content": "Hello"}]
)
print(response["message"]["content"])
"""
from elasticsearch import Elasticsearch

from datetime import datetime, timedelta
import random

# Connect
es = Elasticsearch("http://localhost:9200")

# 1️⃣ employee_activity
for i in range(10):
    doc = {
        "employee_id": f"EMP{i+1}",
        "session_id": f"SES{i+1}",
        "timestamp": (datetime.now() - timedelta(days=random.randint(0,7))).isoformat(),
        "event": random.choice(["keystrokes","window_switch","pause"]),
        "duration_sec": random.randint(5, 300),
        "duration_minutes": random.randint(1,10),
        "text": "sample text" if random.random() > 0.5 else None,
        "application": random.choice(["VSCode","Chrome","Slack","Notepad"]),
        "window": f"Window {random.randint(1,5)}",
        "control": "Edit"
    }
    es.index(index="employee_activity", document=doc)

# 2️⃣ employee_tasks_daily
for i in range(5):
    doc = {
        "employee_id": f"EMP{i+1}",
        "start_ts": (datetime.now() - timedelta(days=i)).isoformat(),
        "end_ts": (datetime.now() - timedelta(days=i) + timedelta(hours=8)).isoformat(),
        "duration_sec": 8*3600,
        "task_summary": "Worked on coding tasks",
        "applications": ["VSCode","Chrome"],
        "windows": ["Window 1","Window 2"],
        "source_chunk_size": random.randint(1,10)
    }
    es.index(index="employee_tasks_daily", document=doc)

# 3️⃣ employee_tasks_matched
for i in range(5):
    doc = {
        "employee_id": f"EMP{i+1}",
        "start_ts": (datetime.now() - timedelta(days=i)).isoformat(),
        "end_ts": (datetime.now() - timedelta(days=i) + timedelta(hours=8)).isoformat(),
        "duration_sec": 8*3600,
        "task_summary": "Worked on coding tasks",
        "matched_category": "Development",
        "confidence": round(random.uniform(0.7,0.99),2),
        "is_expected": random.choice([True, False])
    }
    es.index(index="employee_tasks_matched", document=doc)

# 4️⃣ workloads (for upload test)
doc = {
    "manager_id": "manager1",
    "team": "default",
    "timestamp": datetime.utcnow().isoformat(),
    "workload_text": "Finish feature X, review PRs, write tests"
}
es.index(index="workloads", document=doc)
