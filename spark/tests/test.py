from elasticsearch import Elasticsearch
from datetime import datetime, timezone

# ---------------- CONFIG ----------------
ES_HOST = "http://localhost:9200"
DAILY_INDEX = "employee_kpi_daily_v1"
RAW_INDEX = "employee_activity"   # change if different
EMPLOYEE_ID = "G50047910-5JjP5"  # change for test

# Target date
TARGET_DATE = datetime(2025, 9, 15, tzinfo=timezone.utc)
start_ts = int(datetime(2025, 9, 15, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
end_ts   = int(datetime(2025, 9, 15, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)

# ---------------- CONNECT ----------------
es = Elasticsearch(ES_HOST)

# ----------- Fetch Daily KPI Doc -----------
daily_doc = es.search(
    index=DAILY_INDEX,
    query={
        "bool": {
            "must": [
                {"term": {"employee_id.keyword": EMPLOYEE_ID}},
                {"range": {"last_event_time": {"gte": start_ts, "lte": end_ts}}}
            ]
        }
    }
)

if not daily_doc["hits"]["hits"]:
    print(f"❌ No KPI doc found for {EMPLOYEE_ID} {TARGET_DATE.date()}")
    exit()

daily_kpi = daily_doc["hits"]["hits"][0]["_source"]
print("✅ Found daily KPI doc:", daily_kpi)

# ----------- Fetch Raw Logs for Same Day -----------
resp = es.search(
    index=RAW_INDEX,
    size=5000,  # increase if needed
    query={
        "bool": {
            "must": [
                {"term": {"employee_id.keyword": EMPLOYEE_ID}},
                {"range": {"timestamp": {"gte": start_ts, "lte": end_ts}}}
            ]
        }
    },
    sort=[{"timestamp": "asc"}]
)

events = [hit["_source"] for hit in resp["hits"]["hits"]]
print(f"📊 Retrieved {len(events)} raw events for {EMPLOYEE_ID} on {TARGET_DATE.date()}")

# ----------- Recompute KPIs from Raw Logs -----------
keystrokes = sum(1 for e in events if e["event"] == "keystrokes")
idle_time = sum(float(e.get("idle_duration_sec", 0)) for e in events if e["event"] == "idle_end")
pauses = sum(1 for e in events if e["event"] == "pause")
pause_minutes = sum(float(e.get("duration_minutes", 0)) for e in events if e["event"] == "pause")
last_event = events[-1] if events else {}
last_event_type = last_event.get("event")
last_event_time = last_event.get("timestamp")

manual_kpi = {
    "keystrokes_today": keystrokes,
    "total_idle_today": idle_time,
    "pauses_today": pauses,
    "total_pause_minutes_today": pause_minutes,
    "last_event": last_event_type,
    "last_event_time": last_event_time,
}

print("\n🔄 Manually recomputed KPIs:", manual_kpi)

# ----------- Comparison Function -----------
def compare(expected, actual, label):
    status = "✅" if expected == actual else "❌"
    print(f"{label:<30} expected={expected} actual={actual}  {status}")

print("\n📋 KPI Comparison Report:")
compare(daily_kpi.get("keystrokes_today"), manual_kpi["keystrokes_today"], "Keystrokes")
compare(daily_kpi.get("total_idle_today"), manual_kpi["total_idle_today"], "Total Idle (sec)")
compare(daily_kpi.get("pauses_today"), manual_kpi["pauses_today"], "Pauses")
compare(daily_kpi.get("total_pause_minutes_today"), manual_kpi["total_pause_minutes_today"], "Pause Minutes")
print(f"Last Event Type: expected={daily_kpi.get('employee_status')}  actual={manual_kpi['last_event']}")
print(f"Last Event Time: expected={daily_kpi.get('last_event_time')}  actual={manual_kpi['last_event_time']}")
