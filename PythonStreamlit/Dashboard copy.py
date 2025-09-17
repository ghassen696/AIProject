import streamlit as st
import pandas as pd
from dashboard_utils import (
    ElasticsearchConnector, 
    DataProcessor, 
    ChartGenerator, 
    create_sidebar_filters
)
import plotly.express as px
import plotly.graph_objects as go

import asyncio
from test.Daily_Work_Alignment import match_tasks_to_activities, read_tasks_from_list

def main():
    # Header
    st.markdown('<h1 class="main-header">📊 Employee Productivity Dashboard</h1>', unsafe_allow_html=True)
    
    # Initialize Elasticsearch connector
    try:
        es_connector = ElasticsearchConnector()
    except Exception as e:
        st.error(f"❌ Failed to connect to Elasticsearch: {str(e)}")
        st.stop()
    
    # Sidebar filters
    # Sidebar filters
    start_date, end_date, all_selected_employees = create_sidebar_filters(es_connector)

    # Role-based filtering
    if st.session_state.role == "admin":
        selected_employees = all_selected_employees  # admin sees all selected employees
    else:
        selected_employees = [st.session_state.employee_id]  # regular user sees only their own data

    
    # Display selected filters
    if selected_employees:
        st.info(f"📅 **Period:** {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} | "
                f"👥 **Employees:** {len(selected_employees)} selected")
    else:
        st.warning("Please select at least one employee to view data")
        st.stop()
    
    # Create tabs for different sections
    tab1, tab2, tab3,tab4  = st.tabs([
        "⏱️ Realtime Dashboard",
        "📈 Overview", 
        "📋 Daily Reports", 
        "🔍Work Allignement"
        

    ])
    with tab1:
        show_realtime_dashboard_tab(es_connector, selected_employees)

    with tab2:
        show_overview_tab(es_connector, selected_employees, start_date, end_date)
        show_kpi_summary_tab(es_connector, selected_employees, start_date, end_date)

    
    with tab3:
        show_daily_reports_tab(es_connector, selected_employees, start_date, end_date)
        show_work_distribution_tab(es_connector, selected_employees, start_date, end_date)

    with tab4:
        st.header("📝 Work Alignment")

        # Employee & date selection
        employee_id = st.selectbox("Select Employee", selected_employees)
        start_date_input = st.date_input("Start Date", start_date)
        end_date_input = st.date_input("End Date", end_date)

        # Task input method
        task_input_method = st.radio("Input Tasks", ["Manual Entry", "Upload Excel"])
        tasks = []

        if task_input_method == "Manual Entry":
            task_text = st.text_area(
                "Enter tasks (one per line, format: description; ddl: m/d/Y; priority: high/medium/low; notes: ...)"
            )
            if task_text:
                tasks = read_tasks_from_list(task_text.split("\n"))
        else:
            task_file = st.file_uploader("Upload Excel file with 'task_description' column", type=["xlsx"])
            if task_file:
                df_tasks = pd.read_excel(task_file)
                tasks = df_tasks["task_description"].dropna().tolist()

        # Run alignment button
        if tasks and st.button("🔎 Match Tasks to Activities"):
            with st.spinner("Matching tasks to activities..."):
                result = run_task_alignment(
                    es_connector.es,
                    employee_id,
                    start_date_input,
                    end_date_input,
                    tasks
                )

            # Display results
            st.subheader("✅ Task Matching Results")
            for match in result["matches"]:
                with st.expander(f"{match['task_description']}"):
                    st.write(f"**Time Spent:** {match['time_spent_minutes']:.1f} min")
                    st.write(f"**Match Score:** {match['match_score']:.2f}")
                    st.write(f"**Reasoning:** {match['reasoning']}")

            st.metric("Overall Productivity Score", f"{result['productivity_score']:.2f}")
            st.metric("Completion Rate", f"{result['completion_rate']:.2f}")
            st.metric("Time Efficiency", f"{result['time_efficiency']:.2f}")

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
import plotly.graph_objects as go

import streamlit as st
import pandas as pd
import time
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
import plotly.graph_objects as go
from collections import Counter

def show_realtime_dashboard_tab(es_connector, selected_employees):
    st.header("⚡ Realtime Employee Dashboard")

    # Auto-refresh every 30 seconds
    st_autorefresh(interval=30_000, key="realtime_refresh")

    # Time range: start of today to now
    now_ms = int(time.time() * 1000)
    start_of_today_ms = int(datetime.combine(datetime.today(), datetime.min.time()).timestamp() * 1000)

    # Elasticsearch query
    res = es_connector.es.search(
        index="employee_kpi_daily_v1",
        body={
            "query": {
                "range": {
                    "date": {
                        "gte": start_of_today_ms,
                        "lte": now_ms
                    }
                }
            },
            "size": 50,
            "sort": [{"last_event_time": {"order": "desc"}}]
        }
    )

    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        st.warning("No data available for today.")
        return

    # Convert to DataFrame
    df = pd.DataFrame([hit["_source"] for hit in hits])
    df["date"] = pd.to_datetime(df["date"], unit="ms")
    df["last_event_time"] = pd.to_datetime(df["last_event_time"], unit="ms")

    # Filter selected employees
    if selected_employees:
        df = df[df["employee_id"].isin(selected_employees)]
    if df.empty:
        st.warning("No data for selected employees.")
        return

    # -----------------------------
    # KPI Cards
    # -----------------------------
    st.subheader("📌 Key Metrics")
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Keystrokes", f"{df['keystrokes_today'].sum():,}")
    with col2:
        st.metric("Total Pauses", f"{df['pauses_today'].sum():,}")
    with col3:
        st.metric("Total Pause Minutes", f"{df['total_pause_minutes_today'].sum():.1f}")
    with col4:
        st.metric("Total Idle Minutes", f"{(df['total_idle_today'].sum()/60):.1f}")
    with col5:
        active_count = (df['employee_status'] == 'active').sum()
        st.metric("Active Employees", f"{active_count}/{len(df)}")

    # -----------------------------
    # Employee Status Distribution
    # -----------------------------
    st.subheader("👥 Employee Status")
    status_counts = df["employee_status"].value_counts()
    fig_status = px.pie(
        names=status_counts.index,
        values=status_counts.values,
        hole=0.4,
        title="Employee Status Distribution"
    )
    st.plotly_chart(fig_status, use_container_width=True)

    # -----------------------------
    # Keystrokes vs Idle Time
    # -----------------------------
    st.subheader("⌨️ Keystrokes vs Idle Time")
    fig_keystrokes = go.Figure()
    fig_keystrokes.add_trace(go.Bar(
        x=df['employee_id'],
        y=df['keystrokes_today'],
        name='Keystrokes',
        marker_color='indigo'
    ))
    fig_keystrokes.add_trace(go.Bar(
        x=df['employee_id'],
        y=df['total_idle_today']/60,  # Convert seconds to minutes
        name='Idle Minutes',
        marker_color='orange'
    ))
    fig_keystrokes.update_layout(barmode='group', title="Keystrokes vs Idle Time per Employee")
    st.plotly_chart(fig_keystrokes, use_container_width=True)

    # -----------------------------
    # Pauses Today
    # -----------------------------
    st.subheader("⏸️ Pauses Today (minutes)")
    fig_pauses = px.bar(
        df,
        x="employee_id",
        y="total_pause_minutes_today",
        color="employee_id",
        title="Total Pause Minutes per Employee"
    )
    st.plotly_chart(fig_pauses, use_container_width=True)

    # -----------------------------
    # Event Heatmap
    # -----------------------------
    st.subheader("🔥 Event Frequency Heatmap")
    all_events = []
    for _, row in df.iterrows():
        events_count = Counter(row.get("events_today", []))
        for event, count in events_count.items():
            all_events.append({
                "employee_id": row["employee_id"],
                "event": event,
                "count": count
            })
    if all_events:
        events_df = pd.DataFrame(all_events)
        fig_heatmap = px.density_heatmap(
            events_df,
            x="employee_id",
            y="event",
            z="count",
            color_continuous_scale="Viridis",
            title="Event Counts per Employee"
        )
        st.plotly_chart(fig_heatmap, use_container_width=True)

    # -----------------------------
    # Expandable Employee Details
    # -----------------------------
    st.subheader("📝 Employee Details")
    for _, row in df.iterrows():
        with st.expander(f"{row['employee_id']}"):
            st.write(f"**Status:** {row['employee_status']}")
            st.write(f"**Keystrokes Today:** {row['keystrokes_today']}")
            st.write(f"**Pauses Today:** {row['pauses_today']} ({row['total_pause_minutes_today']:.2f} min)")
            st.write(f"**Idle Today:** {row['total_idle_today']/60:.2f} min")
            st.write(f"**Last Idle Duration:** {row.get('last_idle_duration', 0)/60:.2f} min")
            st.write(f"**Last Event Time:** {row['last_event_time']}")
            st.write(f"**Total Events Today:** {len(row.get('events_today', []))}")
            st.write("**Event Breakdown:**")
            st.json(Counter(row.get("events_today", [])))



def run_task_alignment(es, employee_id: str, start_date, end_date, tasks: list) -> dict:
    """
    Run task-to-activity matching and return the result.
    """
    # Ensure dates are in string format
    start_date_str = start_date.strftime("%Y-%m-%d") if hasattr(start_date, "strftime") else str(start_date)
    end_date_str = end_date.strftime("%Y-%m-%d") if hasattr(end_date, "strftime") else str(end_date)

    # Run the async matching
    result = asyncio.run(match_tasks_to_activities(es, employee_id, start_date_str, end_date_str, tasks))
    return result
def show_kpi_summary_tab(es_connector, selected_employees, start_date, end_date):
    # Fetch KPI data
    kpi_data = es_connector.query_kpi_summary(
        employee_ids=selected_employees,
        start_date=start_date,
        end_date=end_date
    )
    if not kpi_data:
        st.warning("No KPI summary data available for the selection.")
        return

    kpi_df = pd.DataFrame(kpi_data)


    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Keystrokes", f"{kpi_df['total_keystrokes'].sum():,}")
    with col2:
        st.metric("Avg Pause Duration (min)", f"{kpi_df['avg_pause_duration_min'].mean():.1f}")
    with col3:
        st.metric("Unique Apps", f"{int(kpi_df['unique_apps_count'].mean())}")
    with col4:
        st.metric("Event Types", f"{int(kpi_df['distinct_event_types'].mean())}")
    with col5:
        st.metric("Window Switches", f"{int(kpi_df['window_switch_count'].sum())}")

    # 🔹 DONUT CHART (Active/Idle/Pause %)
    st.subheader("⏱️ Activity Distribution")
    if len(kpi_df["employee_id"].unique()) == 1:
        row = kpi_df.iloc[0]
        labels = ["Active %", "Idle %", "Pause %"]
        if "active_pct" in row and "idle_pct" in row and "pause_pct" in row:
            values = [row["active_pct"], row["idle_pct"], row["pause_pct"]]
        else:
            total_sec = row.get("total_sec", 1)  # avoid division by zero
            values = [
                row.get("active_sec", 0) / total_sec * 100,
                row.get("idle_sec", 0) / total_sec * 100,
                row.get("pause_sec", 0) / total_sec * 100
            ]
        fig = px.pie(names=labels, values=values, hole=0.4,
                     title=f"Activity Breakdown - {row['employee_id']}")
        st.plotly_chart(fig, use_container_width=True)
    else:
        weights = kpi_df["total_sec"]
        active = (kpi_df["active_pct"] * weights).sum() / weights.sum()
        idle = (kpi_df["idle_pct"] * weights).sum() / weights.sum()
        pause = (kpi_df["pause_pct"] * weights).sum() / weights.sum()
        labels = ["Active %", "Idle %", "Pause %"]
        values = [active, idle, pause]
        fig = px.pie(names=labels, values=values, hole=0.4,
                     title="Global Team Activity Breakdown")
        st.plotly_chart(fig, use_container_width=True)

    # 🔹 TYPING PER HOUR
    st.subheader("⌨️ Typing Per Hour")
    typing_data = []
    for _, row in kpi_df.iterrows():
        for item in row.get("typing_per_hour", []):
            typing_data.append({
                "employee_id": row["employee_id"],
                "hour": item["hour"],
                "chars_per_hour": item["chars_per_hour"]
            })
    if typing_data:
        typing_df = pd.DataFrame(typing_data)
        fig = px.bar(typing_df, x="hour", y="chars_per_hour",
                     color="employee_id", barmode="group",
                     title="Typing Activity per Hour")
        st.plotly_chart(fig, use_container_width=True)

    # 🔹 KEYSTROKES PER ACTIVE EVENT
    st.subheader("📈 Keystrokes per Active Event (Efficiency)")
    keystroke_eff = []
    for _, row in kpi_df.iterrows():
        for item in row.get("keystrokes_per_active_hour", []):
            keystroke_eff.append({
                "employee_id": row["employee_id"],
                "hour": item["hour"],
                "keystrokes_per_active_event": item["keystrokes_per_active_event"]
            })
    if keystroke_eff:
        eff_df = pd.DataFrame(keystroke_eff)
        fig = px.line(eff_df, x="hour", y="keystrokes_per_active_event",
                      color="employee_id", markers=True,
                      title="Keystrokes per Active Event Over Time")
        st.plotly_chart(fig, use_container_width=True)

    # 🔹 APP USAGE PER HOUR
    st.subheader("🖥️ App Usage Per Hour")
    app_usage = []
    for _, row in kpi_df.iterrows():
        for item in row.get("app_usage_per_hour", []):
            app_usage.append({
                "employee_id": row["employee_id"],
                "application": item["application"],
                "hour": item["hour"],
                "events_per_hour": item["events_per_hour"]
            })
    if app_usage:
        app_df = pd.DataFrame(app_usage)
        fig = px.bar(app_df, x="hour", y="events_per_hour",
                     color="application", facet_col="employee_id",
                     barmode="stack", title="Application Events per Hour")
        st.plotly_chart(fig, use_container_width=True)

    # 🔹 TOP WINDOWS
    st.subheader("🪟 Top Windows by Switch Count")
    windows = []
    for _, row in kpi_df.iterrows():
        for item in row.get("top_windows", []):
            windows.append({
                "employee_id": row["employee_id"],
                "window": item.get("window", "Unknown"),
                "switch_count": item["switch_count"]
            })
    if windows:
        win_df = pd.DataFrame(windows)
        fig = px.bar(win_df, x="switch_count", y="window",
                     color="employee_id", orientation="h",
                     title="Most Frequent Windows")
        st.plotly_chart(fig, use_container_width=True)

    # 🔹 SESSION BREAKDOWN
    st.subheader("📅 Session Breakdown")
    session_data = []
    for _, row in kpi_df.iterrows():
        for s in row.get("sessions", []):
            session_data.append({
            "employee_id": row["employee_id"],
            "session_id": s.get("session_id", "unknown"),
            "session_duration_sec": s.get("session_duration_sec", 0),
            "active_sec": s.get("active_sec", 0),
            "idle_sec": s.get("idle_sec", 0),
            "pause_min": s.get("pause_min", 0)
            })

    if session_data:
        sess_df = pd.DataFrame(session_data)
        sess_df["pause_sec"] = sess_df["pause_min"] * 60

        # Stacked bar chart
        melted = pd.melt(sess_df,
                         id_vars=["employee_id", "session_id"],
                         value_vars=["active_sec", "idle_sec", "pause_sec"],
                         var_name="state", value_name="seconds")
        state_labels = {"active_sec": "Active", "idle_sec": "Idle", "pause_sec": "Pause"}
        melted["state"] = melted["state"].map(state_labels)

        fig = px.bar(melted, x="session_id", y="seconds",
                     color="state", barmode="stack",
                     facet_col="employee_id",
                     title="Session Time Breakdown")
        st.plotly_chart(fig, use_container_width=True)

        # Session table
        st.dataframe(sess_df[["employee_id", "session_id",
                              "session_duration_sec", "active_sec", "idle_sec", "pause_min"]])



def show_overview_tab(es_connector, selected_employees, start_date, end_date):
    """Display overview tab with KPIs and trends"""
    st.header("📌 Key Metrics")
    
    # Fetch daily reports data
  
def show_daily_reports_tab(es_connector, selected_employees, start_date, end_date):
    """Display daily reports tab"""
    st.header("📋 Daily Reports")
    
    daily_data = es_connector.query_daily_reports(selected_employees, start_date, end_date)
    df = DataProcessor.process_daily_reports(daily_data)
    
    if df.empty:
        st.warning("No daily reports found for the selected filters")
        return
    
    # Employee selector for detailed view
    if len(selected_employees) > 1:
        selected_employee = st.selectbox("Select employee for detailed view:", selected_employees)
        filtered_df = df[df['employee_id'] == selected_employee]
    else:
        filtered_df = df
    
    # Display daily summaries
    st.subheader("📝 Daily Summaries")
    for _, row in filtered_df.iterrows():
        with st.expander(f"{row['employee_id']} - {row['date']}"):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.write("**Summary:**")
                st.write(row['daily_summary'] or "No summary available")
            
            with col2:
                st.metric("Productivity Score", f"{row['productivity_score']:.2f}")
                st.metric("Active Time", f"{row['total_active_minutes']:.0f} min")
                st.metric("Focus Level", f"{row['focus_level']:.2f}")
    
    # Display data table
    st.subheader("📊 Data Table")
    display_df = filtered_df[['employee_id', 'date', 'productivity_score', 'focus_level', 
                             'total_active_minutes', 'deep_work_percentage']].round(2)
    st.dataframe(display_df, use_container_width=True)

def show_work_distribution_tab(es_connector, selected_employees, start_date, end_date):
    """Display work distribution tab"""
    st.header("🥧 Work Distribution")
    
    daily_data = es_connector.query_daily_reports(selected_employees, start_date, end_date)
    df = DataProcessor.process_daily_reports(daily_data)
    
    if df.empty:
        st.warning("No work distribution data found for the selected filters")
        return
    
    # Aggregate work distribution
    work_dist = DataProcessor.aggregate_work_distribution(df)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Average Work Distribution")
        ChartGenerator.create_work_distribution_pie(work_dist)
    
    with col2:
        st.subheader("Distribution by Percentage")
        if work_dist:
            dist_df = pd.DataFrame(list(work_dist.items()), columns=['Activity', 'Percentage'])
            dist_df = dist_df.sort_values('Percentage', ascending=False)
            st.dataframe(dist_df.round(2))
        
        st.subheader("📈 Work Pattern Insights")
        if work_dist:
            top_activity = max(work_dist, key=work_dist.get)
            st.success(f"🏆 **Primary Activity:** {top_activity} ({work_dist[top_activity]:.1f}%)")
            
            if len(work_dist) > 1:
                sorted_activities = sorted(work_dist.items(), key=lambda x: x[1], reverse=True)
                st.info(f"📊 **Activity Breakdown:** " + 
                       ", ".join([f"{act}: {pct:.1f}%" for act, pct in sorted_activities[:3]]))



if __name__ == "__main__":
    main()