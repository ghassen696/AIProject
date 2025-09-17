# streamlit_activity_corrections.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch
from config import ELASTICSEARCH_URL
import math
import uuid
import json

# ---- Configuration ----
es = Elasticsearch(ELASTICSEARCH_URL)
EMPLOYEE_INDEX = "employee_activity"
REQUEST_INDEX = "employee_modification_requests"

# default pause durations (in seconds)

PAUSE_DURATION_LIMITS = {
    "lunch": 60 * 60,        # 1 hour
    "bathroom": 15 * 60,     # 15 minutes
    "phone_call": 10 * 60,   # 10 minutes
    "meeting": 2 * 60 * 60,  # 2 hours
    "personal_break": 15 * 60
}
# ---------------- Helper functions ----------------
def ms_to_dt(ms):
    return pd.to_datetime(ms, unit="ms", utc=True)

def dt_to_iso(dt):
    # Expect dt either datetime aware or naive (assume UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()
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
def detect_idle_intervals(df):
    """
    Scan dataframe (sorted by timestamp asc) and return list of detected idle intervals.
    Each interval dict contains:
      - start_ts_ms, end_ts_ms
      - duration_seconds
      - start_event_id, end_event_id (may be None)
      - session_id
      - human-friendly start/end datetimes (UTC)
    """
    intervals = []
    # Ensure sorted
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.sort_values("timestamp").reset_index(drop=True)
    # We'll look for events named 'idle_start' and 'resume' OR 'pause' entries
    idle_rows = df[df["event"] == "idle_start"]
    # For every idle_start, find next 'resume' in same session (or next event)
    for idx, idle_row in idle_rows.iterrows():
        start_ts = idle_row["timestamp"]
        start_event_id = idle_row.get("_id") if "_id" in idle_row else None
        session = idle_row.get("session_id", None)
        # find resume after this index
        later = df[(df["timestamp"] > start_ts)]
        if session is not None:
            later = later[later["session_id"] == session]
        resume_row = later[later["event"] == "resume"]
        if not resume_row.empty:
            end_ts = resume_row.iloc[0]["timestamp"]
            end_event_id = resume_row.iloc[0].get("_id") if "_id" in resume_row.iloc[0] else None
        else:
            # fallback: use next event in same session if any, else last event of day
            next_event = later.iloc[0] if not later.empty else None
            if next_event is not None:
                end_ts = next_event["timestamp"]
                end_event_id = next_event.get("_id", None)
            else:
                # no following event: set end to start + 5 minutes as minimal fallback (employee can correct)
                end_ts = start_ts + (5 * 60 * 1000)
                end_event_id = None

        duration_seconds = int((pd.to_datetime(end_ts, unit="ms") - pd.to_datetime(start_ts, unit="ms")).total_seconds())
        intervals.append({
           "start_ts_ms": int(start_ts.timestamp() * 1000),
            "end_ts_ms": int(end_ts.timestamp() * 1000),
            "duration_seconds": max(duration_seconds, 0),
            "start_event_id": start_event_id,
            "end_event_id": end_event_id,
            "session_id": session,
            "start_dt": ms_to_dt(start_ts),
            "end_dt": ms_to_dt(end_ts),
        })

    # Merge overlapping / adjacent idle intervals optionally (simple approach: if gap < 1s, merge)
    merged = []
    for itv in sorted(intervals, key=lambda x: x["start_ts_ms"]):
        if not merged:
            merged.append(itv)
            continue
        prev = merged[-1]
        if itv["start_ts_ms"] <= prev["end_ts_ms"] + 1000:  # adjacent/overlap within 1s
            # merge
            prev["end_ts_ms"] = max(prev["end_ts_ms"], itv["end_ts_ms"])
            prev["end_event_id"] = itv["end_event_id"] or prev["end_event_id"]
            prev["duration_seconds"] = int((pd.to_datetime(prev["end_ts_ms"], unit="ms") - pd.to_datetime(prev["start_ts_ms"], unit="ms")).total_seconds())
            prev["end_dt"] = ms_to_dt(prev["end_ts_ms"])
        else:
            merged.append(itv)
    return merged

def fetch_today_logs(employee_id, date_utc=None):
    if date_utc is None:
        date_utc = datetime.now(timezone.utc).date()
    start_of_day = datetime.combine(date_utc, datetime.min.time(), tzinfo=timezone.utc)
    end_of_day = datetime.combine(date_utc, datetime.max.time(), tzinfo=timezone.utc)
    start_ms = int(start_of_day.timestamp() * 1000)
    end_ms = int(end_of_day.timestamp() * 1000)
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"employee_id.keyword": employee_id}},
                    {"range": {"timestamp": {"gte": start_ms, "lte": end_ms}}}
                ]
            }
        },
        "size": 10000,
        "sort": [{"timestamp": {"order": "asc"}}]
    }
    res = es.search(index=EMPLOYEE_INDEX, body=query)
    hits = res.get("hits", {}).get("hits", [])
    # attach _id into each _source row for later reference
    rows = []
    for h in hits:
        src = h["_source"].copy()
        # preserve metadata
        src["_id"] = h.get("_id")
        rows.append(src)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df

def index_modification_request(doc):
    # create unique id
    req_id = str(uuid.uuid4())
    es.index(index=REQUEST_INDEX, id=req_id, document=doc)
    return req_id

# ---------------- Streamlit UI ----------------
def show_my_activity_page2():
    st.header("👤 My Activity & Data Correction")

    # auto-refresh (simple)
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = 0
    if st.session_state.last_refresh + 30 < datetime.now().timestamp():
        st.session_state.last_refresh = datetime.now().timestamp()
        st.session_state.reload_table = True

    # current employee id from session or default (as before)
    current_employee_id = st.session_state.get("employee_id", "G50047910-5JjP5")
    if not current_employee_id:
        st.error("Employee ID not found in session.")
        return

    # date selector (employee may want to correct a past day)
    st.subheader("Select Day")
    selected_date = st.date_input("Which day?", value=datetime.now(timezone.utc).date())
    if st.button("Load logs for selected day") or st.session_state.get("reload_table", False):
        st.session_state.reload_table = False
        st.session_state.df = fetch_today_logs(current_employee_id, date_utc=selected_date)

    if "df" not in st.session_state or st.session_state.df.empty:
        st.warning("No activity data found for that day (or not loaded). Click 'Load logs for selected day'.")
        return

    df = st.session_state.df.copy()
    # show a paginated view
    st.subheader("📋 Activity Logs")
    page_size = 20
    if "page_number" not in st.session_state:
        st.session_state.page_number = 1
    total_pages = math.ceil(len(df) / page_size)
    c1, c2, c3 = st.columns([1,2,1])
    with c1:
        if st.button("Previous") and st.session_state.page_number > 1:
            st.session_state.page_number -= 1
    with c3:
        if st.button("Next") and st.session_state.page_number < total_pages:
            st.session_state.page_number += 1
    start_idx = (st.session_state.page_number - 1) * page_size
    end_idx = start_idx + page_size
    display_df = df.iloc[start_idx:end_idx].copy()
    # show only important columns
    display_df_disp = display_df[["timestamp", "event", "application", "control", "text", "_id"]]
    st.dataframe(display_df_disp, use_container_width=True)
    st.caption(f"Page {st.session_state.page_number} of {total_pages}")

    # summary
    st.subheader("🔥 Event Type Summary")
    event_counts = df["event"].value_counts()
    st.bar_chart(event_counts)

    # ---------------- Detect idle intervals ----------------
    st.subheader("🕵️ Detected Idle Intervals")
    # prepare df with timestamp in ms
    df_for_detection = df.copy()
    
    df_for_detection["timestamp_ms"] = (df_for_detection["timestamp"].astype("int64") // 1_000_000).astype(int)
    # adapt detect_idle_intervals to pass ms timestamps (we'll convert back)
    # to keep simple, create a temporary df same format as original code expected
    temp_df = df_for_detection.rename(columns={"timestamp_ms": "timestamp"})
    # ensure _id present as column in temp_df
    detected = detect_idle_intervals(temp_df)
    if not detected:
        st.info("No idle intervals were detected automatically.")
    else:
        # build DataFrame for display
        det_rows = []
        for i, itv in enumerate(detected):
            det_rows.append({
                "index": i,
                "start": itv["start_dt"].strftime("%Y-%m-%d %H:%M:%S %Z"),
                "end": itv["end_dt"].strftime("%Y-%m-%d %H:%M:%S %Z"),
                "duration_min": round(itv["duration_seconds"] / 60, 2),
                "session_id": itv.get("session_id"),
                "start_event_id": itv.get("start_event_id"),
                "end_event_id": itv.get("end_event_id")
            })
        det_df = pd.DataFrame(det_rows)
        st.table(det_df[["index", "start", "end", "duration_min", "session_id"]])

    # ----------------- Correction UI -----------------
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
  

    # ----------------- Quick actions -----------------
    st.subheader("🔁 Quick actions / Info")
    st.write("If your session had connection issues (e.g., 'heartbeat' gaps), create an exclusion request for the day so it won't affect productivity scoring.")
    if st.button("Create quick exclude request for selected day (connection issues)"):
        start_of_day = datetime.combine(selected_date, datetime.min.time(), tzinfo=timezone.utc)
        end_of_day = datetime.combine(selected_date, datetime.max.time(), tzinfo=timezone.utc)
        doc = {
            "employee_id": current_employee_id,
            "request_type": "exclude_day",
            "date": selected_date.isoformat(),
            "start_time_ms": int(start_of_day.timestamp() * 1000),
            "end_time_ms": int(end_of_day.timestamp() * 1000),
            "start_iso": dt_to_iso(start_of_day),
            "end_iso": dt_to_iso(end_of_day),
            "affected_event_ids": [],
            "reason": "Employee requested entire day excluded (connection issues)",
            "status": "pending",
            "requested_at": datetime.utcnow().isoformat(),
        }
        req_id = index_modification_request(doc)
        st.success(f"Exclusion request created (id: {req_id}).")

    # ----------------- Show user's pending requests for that day -----------------
    st.subheader("📨 Your modification requests for the selected day")
    req_q = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"employee_id.keyword": current_employee_id}},
                    {"term": {"date": selected_date.isoformat()}}
                ]
            }
        },
        "size": 100,
        "sort": [{"requested_at": {"order": "desc"}}]
    }
    reqs_res = es.search(index="employee_modification_requests", body=req_q)
    reqs = [r["_source"] | {"_id": r["_id"]} for r in reqs_res.get("hits", {}).get("hits", [])]
    if reqs:
        st.table(pd.DataFrame(reqs)[["requested_at", "request_type", "status", "reason", "employee_notes", "_id"]])
    else:
        st.info("No modification requests found for that day.")

# ----------------- Admin helper (example) -----------------
# The following is not part of the UI, but is a sample function admins can call/server-run
# to apply a pending request. Keep it separate (only admins should use).
def admin_apply_request(request_id, admin_user="admin"):
    """
    Example admin function (script) to apply a pending request.
    - Fetch request doc from REQUEST_INDEX
    - If replace_idle_with_pause:
        * Create a canonical 'pause' event document in EMPLOYEE_INDEX with start/end/pause_type
        * Mark affected events with 'corrected': True and 'correction_request_id': request_id
    - If remove_idle:
        * Mark affected events with 'marked_as_noise': True and 'correction_request_id'
    - If exclude_day:
        * Mark request as approved (scoring system should check requests index when computing productivity)
    IMPORTANT: This function demonstrates one safe approach: do not delete raw events; instead add a canonical event and mark originals.
    """
    req = es.get(index=REQUEST_INDEX, id=request_id)["_source"]
    if req["status"] != "pending":
        return {"ok": False, "reason": "request not pending"}
    employee_id = req["employee_id"]
    start_ms = req["start_time_ms"]
    end_ms = req["end_time_ms"]
    affected_ids = req.get("affected_event_ids", [])

    if req["request_type"] == "replace_idle_with_pause":
        # create pause event
        pause_doc = {
            "timestamp": start_ms,
            "event": "pause",
            "pause_type": req.get("employee_notes", {}).get("pause_type"),
            "start_time_ms": start_ms,
            "end_time_ms": end_ms,
            "duration_seconds": int((end_ms - start_ms) / 1000),
            "employee_id": employee_id,
            "correction_request_id": request_id,
            "created_by": admin_user,
            "created_at": datetime.utcnow().isoformat()
        }
        es.index(index=EMPLOYEE_INDEX, document=pause_doc)
        # mark affected events as corrected
        for eid in affected_ids:
            try:
                es.update(index=EMPLOYEE_INDEX, id=eid, doc={"doc": {"corrected": True, "correction_request_id": request_id}})
            except Exception as e:
                # continue but record
                print(f"Failed to mark event {eid}: {e}")

        # update request status
        es.update(index=REQUEST_INDEX, id=request_id, doc={"doc": {"status": "approved", "admin_actioned_by": admin_user, "admin_actioned_at": datetime.utcnow().isoformat()}})
        return {"ok": True, "action": "pause_created"}
    elif req["request_type"] == "remove_idle":
        for eid in affected_ids:
            try:
                es.update(index=EMPLOYEE_INDEX, id=eid, doc={"doc": {"marked_as_noise": True, "correction_request_id": request_id}})
            except Exception as e:
                print(f"Failed to mark event {eid}: {e}")
        es.update(index=REQUEST_INDEX, id=request_id, doc={"doc": {"status": "approved", "admin_actioned_by": admin_user, "admin_actioned_at": datetime.utcnow().isoformat()}})
        return {"ok": True, "action": "marked_noise"}
    elif req["request_type"] == "exclude_day":
        es.update(index=REQUEST_INDEX, id=request_id, doc={"doc": {"status": "approved", "admin_actioned_by": admin_user, "admin_actioned_at": datetime.utcnow().isoformat()}})
        # Productivity scoring pipeline should check this request index when building daily score and ignore the day if approved.
        return {"ok": True, "action": "day_excluded"}
    else:
        return {"ok": False, "reason": "unknown request_type"}

# ----------------- Run page -----------------
if __name__ == "__main__":
    show_my_activity_page2()
