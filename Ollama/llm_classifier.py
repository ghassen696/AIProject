from elasticsearch import Elasticsearch
from datetime import datetime, timedelta, timezone
import ollama
from collections import defaultdict
import json

es = Elasticsearch("http://localhost:9200")

# Dictionary: category -> list of items
classification_categories = {
    "Enterprise Networking": [
        "Enterprise Network",
        "Switches",
        "Routers",
        "Security",
        "WLAN",
        "Network Management Control & Analysis",
        "Enterprise Network Solution"
    ],
    "Optical Networking": [
        "Enterprise Optical Transmission & Access",
        "Optical Transmission",
        "Optical Access",
        "Enterprise Optical Networking Solution",
        "Optical Network Management Control & Analysis"
    ],
    "Data Storage & Servers": [
        "Data Storage",
        "Storage Solution",
        "Flash Storage",
        "Scale out Storage",
        "Server - Intelligent Computing",
        "Intelligent Servers",
        "Kunpeng Computing",
        "Ascend Computing",
        "Intelligent Accelerator Components",
        "Server Solutions",
        "Management Software"
    ],
    "Digital Power & Facilities": [
        "Digital Power",
        "Site Power Facility",
        "Data Center Facility",
        "FusionSolar",
        "FusionCharge",
        "mPower" 
    ],
    "Sensing & IoT": [
        "Industry Sensing",
        "Industry Sensing Solutions",
        "Sensing Unit",
        "Sensing Edge"
    ],
    "Enterprise Wireless & Trunking": [
        "Enterprise Wireless",
        "eLTE Trunking",
        "eLTE Integrated Access",
        "GSM-R",
        "Enterprise Microwave"
    ],
    "Core Network & 5G": [
        "Enterprise Core Network",
        "Industry 5G Network"
    ],
    "Cloud & Software Management": [
        "Huawei Qiankun CloudService",
        "iMasterCloud",
        "NeoSight Unified Management System",
        "eSight Unified Management System",
        "NCE",
        "U2000"
    ],
    "Enterprise Data Center & Cloud": [
        "Enterprise Data Center",
        "Cloud Computing",
        "Data Center Solution"
    ],
    "Intelligent Collaboration": [
        "Intelligent Collaboration",
        "Video Conferencing Platform",
        "IdeaHub Series",
        "Telepresence Endpoints"
    ],
    "Automotive Solutions": [
        "Intelligent Automotive Solution"
    ],
    "Enterprise Services": [
        "Enterprise Services and Software",
        "Training and Certification Service",
        "Enterprise Software Bussiness",
        "Industry O&M and Operation Support Service",
        "IT Rollout and Integration Service",
        "Network Rollout and Integration Service",
        "Service Experience Management & Operation",
        "Enterprise Service Common"
    ],
    "Other / Meta": ["browsing"]
}

def to_epoch_ms(date_str):
    dt = datetime.strptime(date_str, "%d/%m/%Y")
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def fetch_logs_for_day(employee_id, date):
    #start = int(datetime.strptime(date, "%Y-%m-%d").timestamp() * 1000)
    #end = int((datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).timestamp() * 1000)
    start = to_epoch_ms("10/06/2025")
    end = to_epoch_ms("17/06/2025")
    query = {
        "bool": {
            "must": [
                {"term": {"employee_id": employee_id}},
                {"range": {"timestamp": {"gte": start, "lt": end}}}
            ]
        }
    }
    results = []
    res = es.search(index="employee-activity-logs", size=10000, query=query , scroll='2m')
    scroll_id = res['_scroll_id']
    hits = resp['hits']['hits']

    while hits:
        results.extend(hits)
        resp = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = resp['_scroll_id']
        hits = resp['hits']['hits']

    return [doc["_source"] for doc in results]

def group_sessions(logs):
    sessions = []
    current = []
    last_ts = 0
    for log in sorted(logs, key=lambda x: x["timestamp"]):
        if current and (log["timestamp"] - last_ts > 60000 or log["is_idle"] == 1):
            sessions.append(current)
            current = []
        current.append(log)
        last_ts = log["timestamp"]
    if current:
        sessions.append(current)
    return sessions

def classify_with_llm(text):
    prompt = f"""
Classify the following employee activity text into one of the predefined categories:
{json.dumps(classification_categories, indent=2)}
Text: \"{text}\"
Return only the category name.
"""
    response = ollama.chat(model="llama3", messages=[{"role": "user", "content": prompt}])
    return response["message"]["content"].strip()

def score_efficiency(session):
    length = sum(len(log.get("text", "")) for log in session)
    time = (session[-1]["timestamp"] - session[0]["timestamp"]) / 60000  # in minutes
    return round(length / (time + 1), 2)  # avoid division by 0

def summarize_session(session):
    text = " ".join(log.get("text", "") for log in session if log.get("text"))
    app = session[0].get("application", "")
    start = datetime.fromtimestamp(session[0]["timestamp"] / 1000).isoformat()
    end = datetime.fromtimestamp(session[-1]["timestamp"] / 1000).isoformat()
    category = classify_with_llm(text[:300])
    score = score_efficiency(session)

    return {
        "start_time": start,
        "end_time": end,
        "application": app,
        "classification": category,
        "text_snippet": text[:300],
        "efficiency_score": score
    }

def run_daily_summary(employee_id, date):
    logs = fetch_logs_for_day(employee_id, date)
    sessions = group_sessions(logs)
    classified = [summarize_session(s) for s in sessions]
    daily_score = round(sum(x["efficiency_score"] for x in classified), 2)

    es.index(index="classified-daily-summary", document={
        "employee_id": employee_id,
        "date": date,
        "classified_sessions": classified,
        "daily_efficiency_score": daily_score
    })

# Example usage
run_daily_summary("G50047910-5JjP5", "2025-06-17")
