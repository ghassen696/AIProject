import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch
from config import ELASTICSEARCH_URL

es = Elasticsearch(ELASTICSEARCH_URL)

# ------------------ Pause Duration Limits ------------------
PAUSE_DURATION_LIMITS = {
    "lunch": 60 * 60,        # 1 hour
    "bathroom": 15 * 60,     # 15 minutes
    "phone_call": 10 * 60,   # 10 minutes
    "meeting": 2 * 60 * 60,  # 2 hours
    "personal_break": 15 * 60
}

# ------------------ Helper Function ------------------
def replace_idle_with_pause(idle_event, pause_type):
    pause_limit = PAUSE_DURATION_LIMITS[pause_type]
    start_ts = idle_event["start_ts"]
    end_ts = idle_event["end_ts"]
    duration_sec = idle_event["idle_duration_sec"]

    if duration_sec <= pause_limit:
        return [{
            "event": "pause",
            "reason": pause_type,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "duration_sec": duration_sec
        }]

    else:
        pause_end_ts = start_ts + timedelta(seconds=pause_limit)
        return [
            {
                "event": "pause",
                "reason": pause_type,
                "start_ts": start_ts,
                "end_ts": pause_end_ts,
                "duration_sec": pause_limit
            },
            {
                "event": "idle",
                "start_ts": pause_end_ts,
                "end_ts": end_ts,
                "duration_sec": duration_sec - pause_limit
            }
        ]

# ------------------ Main Activity Page ------------------
def show_my_activity_page():
    st.header("👤 My Activity & Data Correction")

    current_employee_id = st.session_state.get("employee_id", "G50047910-5JjP5")
    if not current_employee_id:
        st.error("Employee ID not found in session.")
        return

    # --- Load today’s activity ---
    today_utc = datetime.now(timezone.utc).date()
    start_of_day = datetime.combine(today_utc, datetime.min.time(), tzinfo=timezone.utc)
    end_of_day = datetime.combine(today_utc, datetime.max.time(), tzinfo=timezone.utc)
    start_ms, end_ms = int(start_of_day.timestamp() * 1000), int(end_of_day.timestamp() * 1000)

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"employee_id.keyword": current_employee_id}},
                    {"range": {"timestamp": {"gte": start_ms, "lte": end_ms}}}
                ]
            }
        },
        "size": 10000,
        "sort": [{"timestamp": {"order": "asc"}}]
    }

    res = es.search(index="employee_activity", body=query)
    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        st.warning("No activity data found for today.")
        return

    df = pd.DataFrame([h["_source"] for h in hits])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    st.subheader("📋 Today's Activity Logs")
    st.dataframe(df[["timestamp", "event", "application", "control", "reason", "duration_minutes", "idle_duration_sec"]])

    # ------------------ Idle → Pause Correction ------------------
    st.subheader("✏️ Correct Idle as Pause")

    idle_events = df[df["event"] == "idle_end"]  # choose only completed idle intervals
    if idle_events.empty:
        st.info("No idle intervals detected today.")
        return

    selected_idle = st.selectbox(
        "Select Idle Interval",
        idle_events.index,
        format_func=lambda idx: f"{df.loc[idx, 'timestamp']} | duration={df.loc[idx, 'idle_duration_sec']} sec"
    )

    pause_type = st.selectbox("Replace with Pause Type", list(PAUSE_DURATION_LIMITS.keys()))
    reason = st.text_area("Reason / Justification")

    if st.button("Submit Correction"):
        idle_row = df.loc[selected_idle]

        idle_event = {
            "start_ts": idle_row["timestamp"] - timedelta(seconds=idle_row["idle_duration_sec"]),
            "end_ts": idle_row["timestamp"],
            "idle_duration_sec": idle_row["idle_duration_sec"]
        }

        corrected = replace_idle_with_pause(idle_event, pause_type)

        # Save request in Elasticsearch for admin validation
        doc = {
            "employee_id": current_employee_id,
            "original_event": "idle",
            "correction": corrected,
            "reason": reason,
            "status": "pending",
            "requested_at": datetime.utcnow().isoformat()
        }
        es.index(index="employee_modification_requests", document=doc)
        st.success("✅ Correction request submitted successfully.")
