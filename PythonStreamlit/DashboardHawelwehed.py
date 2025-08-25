"""import streamlit as st
import streamlit.components.v1 as components
import urllib.parse
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
def dashboard_page():
    st.set_page_config(page_title="Huawei Dashboard", layout="wide")

    st.title("📊 Huawei Employee Activity Monitoring Dashboard")
    user_role = st.session_state.get("role", "user")
    username = st.session_state.get("username", "")


    # Tabs for different sections
    tab1, tab2 = st.tabs([ "📡 Real-Time Activity Dashboard","📈 Team Performance Dashboard"])
       
    with tab1:
        st.markdown('<style>div.block-container{padding-top:1rem;}</style>',unsafe_allow_html=True)
        es = Elasticsearch("http://localhost:9200")

        def get_unique_terms(index_name, field, size=1000):
            body = {
                "size": 0,
                "aggs": {
                    "unique_values": {
                        "terms": {
                            "field": field,
                            "size": size
                        }
                    }
                }
            }
            try:
                res = es.search(index=index_name, body=body)
                buckets = res.get("aggregations", {}).get("unique_values", {}).get("buckets", [])
                return [bucket["key"] for bucket in buckets]
            except Exception as e:
                st.error(f"Error fetching unique terms for field '{field}': {e}")
                return []

        index_name = "keylogger_events"
        employee_ids = get_unique_terms(index_name, "employee_id")
        st.session_state["employee_ids"] = get_unique_terms(index_name, "employee_id")

        st.sidebar.header("Filters")

        preset_times = {
            "Last 15 minutes": timedelta(minutes=15),
            "Last 30 minutes": timedelta(minutes=30),
            "Last 1 hour": timedelta(hours=1),
            "Last 24 hours": timedelta(days=1),
            "Last 7 days": timedelta(days=7),
            "Custom range": None
        }

        selected_preset = st.sidebar.selectbox("Select Time Range", list(preset_times.keys()), index=3)

        if selected_preset == "Custom range":
            start_date = st.sidebar.date_input("Start Date", datetime.now().date())
            start_time = st.sidebar.time_input("Start Time", datetime.now().time())
            end_date = st.sidebar.date_input("End Date", datetime.now().date())
            end_time = st.sidebar.time_input("End Time", datetime.now().time())
            start_dt = datetime.combine(start_date, start_time)
            end_dt = datetime.combine(end_date, end_time)
        else:
            end_dt = datetime.now()
            start_dt = end_dt - preset_times[selected_preset]

        # Employee filter
        if user_role == "admin":
            selected_employees = st.sidebar.multiselect(
                "Select Employee ID(s)",
                options=employee_ids,
                default=employee_ids
            )
        else:
            if username in employee_ids:
                selected_employees = [username]
            else:
                st.sidebar.warning("Your data is not available yet.")
                selected_employees = []

        def build_filter(field, values):
            if not values:
                return ""
            escaped_values = [v.replace('"', '\\"') for v in values]
            filters = ",".join([f"(match_phrase:{{{field}:\"{v}\"}})" for v in escaped_values])
            return filters

        filters_list = []

        if selected_employees and len(selected_employees) < len(employee_ids):
            filters_list.append(build_filter("employee_id", selected_employees))

        filters_combined = ",".join(filters_list)
        filters_kibana = f"filters:!({filters_combined})" if filters_combined else "filters:!()"

        # --- Convert start and end datetime to Kibana-compatible ISO8601 string
        start_str = start_dt.isoformat()
        end_str = end_dt.isoformat()

        base_url = "http://localhost:5601/app/dashboards#/view/1df79be0-5ce8-11f0-b604-0ba41f78bced"
        _g = f"(refreshInterval:(pause:!t,value:0),time:(from:'{start_str}',to:'{end_str}'),{filters_kibana})"

        kibana_url = f"{base_url}?embed=true&_g={urllib.parse.quote(_g)}"

        components.iframe(kibana_url, height=1200, scrolling=True)
    # ----------------------
    with tab2:
        st.header("📊 Team Performance Overview")

        st.markdown("Visualize the task completion percentage and workload distribution.")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("✅ Task Completion Rate")
            # TODO: Replace with your real chart
            st.progress(0.75)

        with col2:
            st.subheader("👤 Task Distribution by Team Member")
            # TODO: Use e.g., st.bar_chart() or matplotlib/seaborn
            st.bar_chart({"Team Members": [30, 45, 25]}, width=400)

        st.markdown("---")"""
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from config import ELASTICSEARCH_URL
import io, subprocess, sys

es = Elasticsearch(ELASTICSEARCH_URL, verify_certs=False)

def get_employee_data(index="employee_activity", start_date=None, end_date=None, batch_size=5000):
    must = [{"match_all": {}}]
    if start_date and end_date:
        must.append({"range": {"timestamp": {"gte": start_date.isoformat(), "lte": end_date.isoformat()}}})
    query = {"bool": {"must": must}}

    res = es.search(index=index, query=query, size=batch_size, scroll="2m")
    scroll_id = res.get("_scroll_id")
    records = [hit["_source"] for hit in res["hits"]["hits"]]
    while True:
        res = es.scroll(scroll_id=scroll_id, scroll="2m")
        hits = res["hits"]["hits"]
        if not hits: break
        records.extend([hit["_source"] for hit in hits])

    df = pd.DataFrame(records)
    if not df.empty:
        if "duration_sec" in df.columns and "idle_duration_sec" not in df.columns:
            df.rename(columns={"duration_sec": "idle_duration_sec"}, inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
    return df

def _fetch_unique_terms(index_name, field, size=1000):
    body = {"size":0,"aggs":{"unique_values":{"terms":{"field":field,"size":size}}}}
    try:
        res = es.search(index=index_name, body=body)
        buckets = res.get("aggregations", {}).get("unique_values", {}).get("buckets", [])
        return [b["key"] for b in buckets]
    except Exception as e:
        st.error(f"Error fetching unique terms for field '{field}': {e}")
        return []

def _read_index(index, query, size=2000):
    try:
        res = es.search(index=index, query=query, size=size)
        return [h["_source"] for h in res["hits"]["hits"]]
    except Exception as e:
        st.error(f"Error reading index {index}: {e}")
        return []

def dashboard_page():
    st.set_page_config(page_title="Employee Activity Dashboard", layout="wide")
    st.title("📊 Employee Activity Monitoring (Batch + Workload Matching)")

    with st.sidebar.expander("Filters",expanded=True):
        st.header("🔎 Filters")
        date_range = st.date_input("Date Range", [datetime.now() - timedelta(days=7), datetime.now()])
        user_role = st.session_state.get("role", "admin")  # default admin for demo
        username = st.session_state.get("username", "manager1")

        st.markdown("---")
        st.subheader("📂 Workload Upload")
        up = st.file_uploader("Upload a workload (txt/csv/md)", type=["txt","csv","md"])
        if up is not None:
            try:
                if up.type.startswith("text/") or up.name.endswith((".txt",".md",".csv")):
                    content = up.read().decode("utf-8", errors="ignore")
                    es.index(index="workloads", document={
                        "manager_id": username,
                        "team": "default",
                        "timestamp": datetime.utcnow(),
                        "workload_text": content
                    })
                    st.success("Workload uploaded and saved.")
                else:
                    st.warning("Only txt/csv/md supported here (keep it simple).")
            except Exception as e:
                st.error(f"Upload failed: {e}")

        st.markdown("---")
        st.subheader("🛠 Batch Jobs (optional)")
        colA, colB = st.columns(2)
        if colA.button("Run Daily Summarization"):
            try:
                out = subprocess.run([sys.executable, "jobs/daily_pipeline.py"], capture_output=True, text=True)
                st.code(out.stdout or out.stderr)
            except Exception as e:
                st.error(f"Failed: {e}")
        if colB.button("Run Workload Match"):
            try:
                out = subprocess.run([sys.executable, "jobs/workload_match.py"], capture_output=True, text=True)
                st.code(out.stdout or out.stderr)
            except Exception as e:
                st.error(f"Failed: {e}")

    # Load raw activity for KPIs (as you had)
    df = get_employee_data(start_date=date_range[0], end_date=date_range[1])
    if df.empty:
        st.warning("No data found for selected date range.")
        return

    if "employee_ids" not in st.session_state:
        st.session_state["employee_ids"] = _fetch_unique_terms("employee_activity", "employee_id")

    employee_ids = st.session_state["employee_ids"]
    if user_role == "admin":
        selected_employees = st.multiselect("Select Employee(s)", options=employee_ids, default=employee_ids[:3])
    else:
        selected_employees = [username] if username in employee_ids else []

    if not selected_employees:
        st.warning("No data to display."); return

    df = df[df["employee_id"].isin(selected_employees)]
    employee_id = st.selectbox("Employee", df["employee_id"].dropna().unique())
    df = df[df["employee_id"] == employee_id]

    sessions = df["session_id"].dropna().unique()
    session_id = st.selectbox("Session", sessions) if len(sessions) > 0 else None
    if session_id:
        df = df[df["session_id"] == session_id]

    # --- KPIs (unchanged from yours) ---
    total_idle = df["idle_duration_sec"].fillna(0).sum()
    total_pause = df["duration_minutes"].fillna(0).sum() * 60
    total_keystrokes = df[df["event"] == "keystrokes"].shape[0]
    total_chars = df[df["event"] == "keystrokes"]["text"].dropna().astype(str).str.len().sum()
    apps_used = df["application"].dropna().nunique()
    window_switches = df[df["event"] == "window_switch"].shape[0]
    corrections = df[df["event"] == "keystrokes"]["text"].dropna().astype(str).str.count("<Key.backspace>").sum()
    typing_correction_rate = (corrections / max(total_keystrokes, 1)) * 100
    session_duration = (df["timestamp"].max() - df["timestamp"].min()).total_seconds()
    active_time = max(session_duration - total_idle - total_pause, 0)
    focus_ratio = (active_time / max(session_duration, 1)) * 100
    keystrokes_per_min = total_keystrokes / (active_time / 60) if active_time > 0 else 0
    typing_speed = total_chars / (active_time / 60) if active_time > 0 else 0
    task_switch_rate = window_switches / (active_time / 60) if active_time > 0 else 0
    top_window = df["window"].dropna().mode()[0] if "window" in df and not df["window"].dropna().empty else "-"

    efficiency_score = min(100, (focus_ratio * 0.4) + (keystrokes_per_min * 0.3) + (typing_speed * 0.3))

    st.subheader("📌 Key Metrics")
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("⌨️ Keystrokes", total_keystrokes)
    c2.metric("🕑 Idle Time (min)", f"{total_idle/60:.1f}")
    c3.metric("⏸ Pause Time (min)", f"{total_pause/60:.1f}")
    c4.metric("💻 Apps Used", apps_used)
    c5.metric("⚡ Efficiency Score", f"{efficiency_score:.1f}%")

    c6,c7,c8,c9,c10 = st.columns(5)
    c6.metric("⏱ Active Time (h)", f"{active_time/3600:.2f}")
    c7.metric("🎯 Focus Ratio", f"{focus_ratio:.1f}%")
    c8.metric("⌨️ Keystrokes/min", f"{keystrokes_per_min:.1f}")
    c9.metric("✍️ Typing Speed (chars/min)", f"{typing_speed:.1f}")
    c10.metric("🔄 Task Switch Rate/min", f"{task_switch_rate:.2f}")

    c11,c12 = st.columns(2)
    c11.metric("✏️ Corrections", int(corrections))
    c12.metric("🪟 Top Active Window", top_window)

    # --- Apps ---
    st.subheader("💻 Application Usage")
    app_counts = df["application"].value_counts().reset_index()
    app_counts.columns = ["application", "count"]
    if not app_counts.empty:
        st.plotly_chart(px.bar(app_counts, x="application", y="count", title="Top Applications Used"), use_container_width=True)

    # --- Timeline ---
    st.subheader("📈 Event Timeline")
    fig_timeline = px.scatter(
        df, x="timestamp", y="event", color="event",
        hover_data=["application", "window", "control", "text"],
        title="Activity Timeline (Events Over Time)"
    )
    st.plotly_chart(fig_timeline, use_container_width=True)

    # --- Time Distribution ---
    st.subheader("⏳ Session Breakdown")
    time_breakdown = pd.DataFrame({"State":["Active","Idle","Pause"], "Seconds":[active_time, total_idle, total_pause]})
    st.plotly_chart(px.pie(time_breakdown, names="State", values="Seconds", title="Time Distribution"), use_container_width=True)

    # --- New: Task Analytics ---
    st.markdown("---")
    st.header("🧠 Task Analytics (LLM)")
    view_mode = st.radio("View Mode", ["Daily Tasks", "Matched (Workload)"], horizontal=True)

    start_iso = date_range[0].isoformat()
    end_iso = date_range[1].isoformat()

    if view_mode == "Daily Tasks":
        docs = _read_index(
            "employee_tasks_daily",
            {"bool":{"must":[
                {"term":{"employee_id": employee_id}},
                {"range":{"start_ts":{"gte": start_iso, "lte": end_iso}}}
            ]}},
            size=5000
        )
        tdf = pd.DataFrame(docs)
        if tdf.empty:
            st.info("No daily tasks yet. Run the Daily Summarization job.")
        else:
            st.subheader("Summarized Task Chunks")
            st.dataframe(tdf[["start_ts","end_ts","duration_sec","task_summary","applications","windows","source_chunk_size"]], use_container_width=True)
            st.plotly_chart(
                px.bar(tdf, x="task_summary", y="duration_sec", title="Time by Summarized Task").update_xaxes(categoryorder="total descending"),
                use_container_width=True
            )

    else:  # Matched
        docs = _read_index(
            "employee_tasks_matched",
            {"bool":{"must":[
                {"term":{"employee_id": employee_id}},
                {"range":{"start_ts":{"gte": start_iso, "lte": end_iso}}}
            ]}},
            size=5000
        )
        mdf = pd.DataFrame(docs)
        if mdf.empty:
            st.info("No matched tasks yet. Upload a workload, then run Workload Match.")
        else:
            st.subheader("Matched Tasks vs Workload")
            st.dataframe(mdf[["start_ts","end_ts","duration_sec","task_summary","matched_category","confidence","is_expected"]], use_container_width=True)

            # Efficiency KPIs
            total_task_time = mdf["duration_sec"].fillna(0).sum()
            expected_time = mdf.loc[mdf["is_expected"]==True, "duration_sec"].fillna(0).sum()
            unexpected_time = total_task_time - expected_time
            efficiency = (expected_time / total_task_time * 100) if total_task_time>0 else 0

            k1,k2,k3 = st.columns(3)
            k1.metric("Total Task Time (h)", f"{total_task_time/3600:.2f}")
            k2.metric("On-Workload Time (h)", f"{expected_time/3600:.2f}")
            k3.metric("Workload Efficiency", f"{efficiency:.1f}%")

            # Charts
            by_cat = mdf.groupby("matched_category")["duration_sec"].sum().reset_index().sort_values("duration_sec", ascending=False)
            st.plotly_chart(px.bar(by_cat, x="matched_category", y="duration_sec", title="Time by Matched Category"), use_container_width=True)

    # Latest raw events
    st.subheader("🔴 Latest Events")
    st.dataframe(df.sort_values("timestamp", ascending=False).head(15), use_container_width=True)

    # Mock data (you can load from ES instead)
    daily_metrics = pd.DataFrame([
        {
            "employee_id": "G50047910-5JjP5",
            "date": "2025-08-21",
            "active_minutes": 420,
            "idle_minutes": 45,
            "pause_minutes": 30,
            "num_window_switches": 58,
            "num_keystrokes": 1340,
            "num_pastes": 12,
            "focus_apps": {"VSCode": 210, "Chrome": 180, "Notepad": 30}
        },
        {
            "employee_id": "G50047910-5JjP5",
            "date": "2025-08-22",
            "active_minutes": 390,
            "idle_minutes": 60,
            "pause_minutes": 40,
            "num_window_switches": 72,
            "num_keystrokes": 1210,
            "num_pastes": 9,
            "focus_apps": {"VSCode": 160, "Chrome": 200, "Slack": 30}
        }
    ])

    # Expand focus_apps into columns
    focus_expanded = daily_metrics["focus_apps"].apply(pd.Series).fillna(0)
    df = pd.concat([daily_metrics.drop(columns=["focus_apps"]), focus_expanded], axis=1)

    st.title("📊 Employee Activity Dashboard")

    employee = st.selectbox("Select Employee", df["employee_id"].unique())
    emp_data = df[df["employee_id"] == employee]

    # Activity time breakdown
    st.subheader("⏱️ Active vs Idle vs Pause")
    fig1 = px.bar(emp_data, x="date", y=["active_minutes", "idle_minutes", "pause_minutes"],
                barmode="stack", title="Daily Time Breakdown (minutes)")
    st.plotly_chart(fig1, use_container_width=True)

    # Keystrokes & pastes trend
    st.subheader("⌨️ Keystrokes & Clipboard Usage")
    fig2 = px.line(emp_data, x="date", y=["num_keystrokes", "num_pastes"],
                markers=True, title="Daily Keystrokes & Pasting Activity")
    st.plotly_chart(fig2, use_container_width=True)

    # Window switching frequency
    st.subheader("🔄 Context Switching")
    fig3 = px.bar(emp_data, x="date", y="num_window_switches",
                title="Window Switches per Day")
    st.plotly_chart(fig3, use_container_width=True)

    # Focus apps
    st.subheader("💻 Application Focus Time")
    focus_cols = [col for col in df.columns if col not in
                ["employee_id", "date", "active_minutes", "idle_minutes",
                "pause_minutes", "num_window_switches", "num_keystrokes", "num_pastes"]]
    focus_long = emp_data.melt(id_vars=["date"], value_vars=focus_cols,
                            var_name="Application", value_name="Minutes")
    fig4 = px.bar(focus_long, x="date", y="Minutes", color="Application",
                barmode="stack", title="Focus Time per Application")
    st.plotly_chart(fig4, use_container_width=True)

    # Daily summaries (static example)
    st.subheader("📝 Daily Summary")
    daily_reports = {
        "2025-08-21": "The employee spent the day alternating between coding in VSCode and browsing resources in Chrome. Idle time was minimal.",
        "2025-08-22": "The employee focused on Chrome for research, coding in VSCode, and brief Slack usage. Idle time increased slightly."
    }
    for date, summary in daily_reports.items():
        st.markdown(f"**{date}**: {summary}")


if __name__ == "__main__":
    dashboard_page()
