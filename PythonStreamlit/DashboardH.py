import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from config import ELASTICSEARCH_URL
import io, subprocess, sys
import numpy as np
es = Elasticsearch(ELASTICSEARCH_URL, verify_certs=False)
# Color scheme constants
COLORS = {
    'productive': '#2E8B57', 
    'idle': '#DC143C',       
    'pause': '#FF8C00',       
    'apps': '#4169E1',       
    'neutral': '#696969',      
    'success': '#32CD32',     
    'warning': '#FFD700'     
}

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

def get_daily_metrics(employee_ids, start_date, end_date):
    query = {
        "bool": {
            "must": [
                {"terms": {"employee_id": employee_ids}},
                {"range": {"date": {"gte": start_date.isoformat(), "lte": end_date.isoformat()}}}
            ]
        }
    }
    res = es.search(index="employee_daily_summaries", query=query, size=5000)
    records = [h["_source"] for h in res["hits"]["hits"]]
    return pd.DataFrame(records)

def get_daily_reports(employee_id, start_date, end_date):
    query = {
        "bool": {
            "must": [
                {"term": {"employee_id": employee_id}},
                {"range": {"date": {"gte": start_date.isoformat(), "lte": end_date.isoformat()}}}
            ]
        }
    }
    res = es.search(index="employee_daily_reports", query=query, size=100)
    return {h["_source"]["date"]: h["_source"]["daily_summary"] for h in res["hits"]["hits"]}

def calculate_productivity_score(df):
    """Calculate a comprehensive productivity score based on multiple factors"""
    if df.empty:
        return 0
    
    # Base metrics
    total_idle = df["idle_duration_sec"].fillna(0).sum()
    total_pause = df["duration_minutes"].fillna(0).sum() * 60
    total_keystrokes = df[df["event"] == "keystrokes"].shape[0]
    window_switches = df[df["event"] == "window_switch"].shape[0]
    
    # Time calculations
    session_duration = (df["timestamp"].max() - df["timestamp"].min()).total_seconds()
    active_time = max(session_duration - total_idle - total_pause, 0)
    
    # Scoring factors (0-100 scale)
    focus_score = max(0, 100 - (total_idle / max(session_duration, 1)) * 100)
    activity_score = min(100, (total_keystrokes / max(active_time / 60, 1)) * 10)
    consistency_score = max(0, 100 - (window_switches / max(active_time / 60, 1)) * 5)
    
    # Weighted average
    productivity_score = (focus_score * 0.4 + activity_score * 0.4 + consistency_score * 0.2)
    return min(100, max(0, productivity_score))

def detect_anomalies(df, employee_id):
    """Detect anomalies in employee activity patterns"""
    if df.empty:
        return []
    
    anomalies = []
    
    # Calculate daily statistics
    df['date'] = df['timestamp'].dt.date
    daily_stats = df.groupby('date').agg({
        'idle_duration_sec': 'sum',
        'duration_minutes': 'sum',
        'event': 'count'
    }).reset_index()
    
    if len(daily_stats) < 2:
        return anomalies
    
    # Detect unusual idle time (more than 2 standard deviations from mean)
    idle_mean = daily_stats['idle_duration_sec'].mean()
    idle_std = daily_stats['idle_duration_sec'].std()
    if idle_std > 0:
        high_idle_days = daily_stats[daily_stats['idle_duration_sec'] > idle_mean + 2*idle_std]
        for _, row in high_idle_days.iterrows():
            anomalies.append({
                'type': 'High Idle Time',
                'date': row['date'],
                'value': f"{row['idle_duration_sec']/3600:.1f}h",
                'severity': 'warning'
            })
    
    # Detect unusually low activity days
    activity_mean = daily_stats['event'].mean()
    activity_std = daily_stats['event'].std()
    if activity_std > 0:
        low_activity_days = daily_stats[daily_stats['event'] < activity_mean - 2*activity_std]
        for _, row in low_activity_days.iterrows():
            anomalies.append({
                'type': 'Low Activity',
                'date': row['date'],
                'value': f"{row['event']} events",
                'severity': 'warning'
            })
    
    return anomalies

def create_trend_chart(df, metric, title, color):
    """Create a trend chart for a specific metric over time"""
    if df.empty:
        return None
    
    df_copy = df.copy()
    df_copy['date'] = df_copy['timestamp'].dt.date
    
    if metric == 'keystrokes':
        daily_data = df_copy[df_copy['event'] == 'keystrokes'].groupby('date').size().reset_index()
        daily_data.columns = ['date', 'count']
        y_col = 'count'
        y_title = 'Keystrokes'
    elif metric == 'idle_time':
        daily_data = df_copy.groupby('date')['idle_duration_sec'].sum().reset_index()
        daily_data['hours'] = daily_data['idle_duration_sec'] / 3600
        y_col = 'hours'
        y_title = 'Idle Time (hours)'
    elif metric == 'app_switches':
        daily_data = df_copy[df_copy['event'] == 'window_switch'].groupby('date').size().reset_index()
        daily_data.columns = ['date', 'count']
        y_col = 'count'
        y_title = 'Window Switches'
    else:
        return None
    
    if daily_data.empty:
        return None
    
    fig = px.line(daily_data, x='date', y=y_col, 
                   title=title, color_discrete_sequence=[color])
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title=y_title,
        showlegend=False,
        height=300
    )
    return fig

def dashboard_page():
    st.set_page_config(page_title="Employee Activity Dashboard", layout="wide")
    
    # Custom CSS for better styling
    st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .anomaly-warning {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        padding: 0.5rem;
        border-radius: 0.25rem;
        margin: 0.5rem 0;
    }
    .anomaly-danger {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        padding: 0.5rem;
        border-radius: 0.25rem;
        margin: 0.5rem 0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.title("📊 Employee Activity Monitoring Dashboard")
    st.markdown("---")

    # Sidebar with enhanced filters
    with st.sidebar:
        st.header("🔎 Dashboard Filters")
        
        # Date range filter
        st.subheader("📅 Date Range")
        date_range = st.date_input(
            "Select Date Range", 
            [datetime.now() - timedelta(days=7), datetime.now()],
            help="Choose the date range for analysis"
        )
        
        # Employee filter
        st.subheader("👤 Employee Selection")
        if "employee_ids" not in st.session_state:
            st.session_state["employee_ids"] = _fetch_unique_terms("employee_activity", "employee_id")
        
        employee_ids = st.session_state["employee_ids"]
        selected_employees = st.multiselect(
            "Select Employees", 
            options=employee_ids, 
            default=employee_ids[:3] if len(employee_ids) >= 3 else employee_ids,
            help="Select one or more employees to analyze"
        )
        
        # Application filter
        st.subheader("💻 Application Filter")
        if selected_employees:
            df_temp = get_employee_data(start_date=date_range[0], end_date=date_range[1])
            df_temp = df_temp[df_temp["employee_id"].isin(selected_employees)]
            available_apps = df_temp["application"].dropna().unique()
            selected_apps = st.multiselect(
                "Filter by Applications",
                options=available_apps,
                default=available_apps[:5] if len(available_apps) >= 5 else available_apps,
                help="Select specific applications to focus on"
            )
        else:
            selected_apps = []
        
        st.markdown("---")
        
        # Workload upload section
        st.subheader("📂 Workload Management")
        up = st.file_uploader("Upload Workload", type=["txt","csv","md"])
        if up is not None:
            try:
                if up.type.startswith("text/") or up.name.endswith((".txt",".md",".csv")):
                    content = up.read().decode("utf-8", errors="ignore")
                    es.index(index="workloads", document={
                        "manager_id": "manager1",
                        "team": "default",
                        "timestamp": datetime.utcnow(),
                        "workload_text": content
                    })
                    st.success("✅ Workload uploaded successfully!")
                else:
                    st.warning("⚠️ Only txt/csv/md files supported")
            except Exception as e:
                st.error(f"❌ Upload failed: {e}")
        
        # Batch jobs section
        st.subheader("🛠️ Batch Operations")
        col1, col2 = st.columns(2)
        if col1.button("📊 Daily Pipeline", help="Run daily summarization"):
            try:
                out = subprocess.run([sys.executable, "jobs/daily_pipeline.py"], 
                                   capture_output=True, text=True)
                if out.returncode == 0:
                    st.success("✅ Pipeline completed")
                else:
                    st.error(f"❌ Pipeline failed: {out.stderr}")
            except Exception as e:
                st.error(f"❌ Failed: {e}")
                
        if col2.button("🔍 Workload Match", help="Run workload matching"):
            try:
                out = subprocess.run([sys.executable, "jobs/workload_match.py"], 
                                   capture_output=True, text=True)
                if out.returncode == 0:
                    st.success("✅ Workload matching completed")
                else:
                    st.error(f"❌ Workload matching failed: {out.stderr}")
            except Exception as e:
                st.error(f"❌ Failed: {e}")

    # Main dashboard content
    if not selected_employees:
        st.warning("⚠️ Please select at least one employee from the sidebar to view the dashboard.")
        return

    # Load and filter data
    df = get_employee_data(start_date=date_range[0], end_date=date_range[1])
    if df.empty:
        st.warning("⚠️ No data found for the selected date range.")
        return

    # Apply filters
    df = df[df["employee_id"].isin(selected_employees)]
    if selected_apps:
        df = df[df["application"].isin(selected_apps)]

    # Employee selector for detailed view
    if len(selected_employees) > 1:
        selected_employee = st.selectbox(
            "👤 Select Employee for Detailed View", 
            options=selected_employees,
            help="Choose an employee to see detailed metrics and insights"
        )
        df_employee = df[df["employee_id"] == selected_employee]
    else:
        selected_employee = selected_employees[0]
        df_employee = df

    # Calculate metrics
    total_idle = df_employee["idle_duration_sec"].fillna(0).sum()
    total_pause = df_employee["duration_minutes"].fillna(0).sum() * 60
    total_keystrokes = df_employee[df_employee["event"] == "keystrokes"].shape[0]
    total_chars = df_employee[df_employee["event"] == "keystrokes"]["text"].dropna().astype(str).str.len().sum()
    apps_used = df_employee["application"].dropna().nunique()
    window_switches = df_employee[df_employee["event"] == "window_switch"].shape[0]
    corrections = df_employee[df_employee["event"] == "keystrokes"]["text"].dropna().astype(str).str.count("<Key.backspace>").sum()
    
    session_duration = (df_employee["timestamp"].max() - df_employee["timestamp"].min()).total_seconds()
    active_time = max(session_duration - total_idle - total_pause, 0)
    focus_ratio = (active_time / max(session_duration, 1)) * 100
    keystrokes_per_min = total_keystrokes / (active_time / 60) if active_time > 0 else 0
    typing_speed = total_chars / (active_time / 60) if active_time > 0 else 0
    task_switch_rate = window_switches / (active_time / 60) if active_time > 0 else 0
    typing_correction_rate = (corrections / max(total_keystrokes, 1)) * 100
    
    # Calculate productivity score
    productivity_score = calculate_productivity_score(df_employee)
    
    # Detect anomalies
    anomalies = detect_anomalies(df_employee, selected_employee)

    # Header with summary
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.subheader(f"📊 Activity Summary for {selected_employee}")
        st.caption(f"Period: {date_range[0]} to {date_range[1]}")
    
    with col2:
        st.metric("📈 Productivity Score", f"{productivity_score:.1f}%")
    
    with col3:
        if anomalies:
            st.metric("⚠️ Anomalies Detected", len(anomalies))
        else:
            st.metric("✅ No Anomalies", "Clean")

    # Key Performance Indicators
    st.subheader("🎯 Key Performance Indicators")
    
    # Row 1: Time-based metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "⏱️ Active Time", 
            f"{active_time/3600:.2f}h",
            f"{focus_ratio:.1f}% focus",
            delta_color="normal"
        )
    
    with col2:
        st.metric(
            "🕑 Idle Time", 
            f"{total_idle/3600:.2f}h",
            f"{(total_idle/session_duration*100):.1f}% of session",
            delta_color="inverse"
        )
    
    with col3:
        st.metric(
            "⏸️ Pause Time", 
            f"{total_pause/3600:.2f}h",
            f"{(total_pause/session_duration*100):.1f}% of session",
            delta_color="inverse"
        )
    
    with col4:
        st.metric(
            "📅 Session Duration", 
            f"{session_duration/3600:.2f}h",
            f"Total time tracked",
            delta_color="normal"
        )

    # Row 2: Productivity metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "⌨️ Keystrokes", 
            f"{total_keystrokes:,}",
            f"{keystrokes_per_min:.1f}/min",
            delta_color="normal"
        )
    
    with col2:
        st.metric(
            "✍️ Typing Speed", 
            f"{typing_speed:.1f} chars/min",
            f"{total_chars:,} total chars",
            delta_color="normal"
        )
    
    with col3:
        st.metric(
            "🔄 Task Switches", 
            f"{window_switches}",
            f"{task_switch_rate:.2f}/min",
            delta_color="inverse"
        )
    
    with col4:
        st.metric(
            "✏️ Corrections", 
            f"{corrections}",
            f"{typing_correction_rate:.1f}% rate",
            delta_color="inverse"
        )

    # Anomalies section
    if anomalies:
        st.subheader("⚠️ Activity Anomalies Detected")
        for anomaly in anomalies:
            severity_class = "anomaly-danger" if anomaly['severity'] == 'danger' else "anomaly-warning"
            st.markdown(f"""
            <div class="{severity_class}">
                <strong>{anomaly['type']}</strong> on {anomaly['date']}: {anomaly['value']}
            </div>
            """, unsafe_allow_html=True)

    # Visualizations section
    st.subheader("📊 Activity Visualizations")
    
    # Create tabs for different chart types
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Trends", "⏰ Time Distribution", "💻 App Usage", "📊 Activity Breakdown"])
    
    with tab1:
        st.subheader("📈 Activity Trends Over Time")
        
        # Create trend charts
        col1, col2 = st.columns(2)
        
        with col1:
            keystrokes_fig = create_trend_chart(df_employee, 'keystrokes', 'Daily Keystrokes Trend', COLORS['productive'])
            if keystrokes_fig:
                st.plotly_chart(keystrokes_fig, use_container_width=True)
            else:
                st.info("No keystroke data available for trend analysis")
        
        with col2:
            idle_fig = create_trend_chart(df_employee, 'idle_time', 'Daily Idle Time Trend', COLORS['idle'])
            if idle_fig:
                st.plotly_chart(idle_fig, use_container_width=True)
            else:
                st.info("No idle time data available for trend analysis")
        
        # App switching trend
        app_switch_fig = create_trend_chart(df_employee, 'app_switches', 'Daily Window Switching Trend', COLORS['apps'])
        if app_switch_fig:
            st.plotly_chart(app_switch_fig, use_container_width=True)
    
    with tab2:
        st.subheader("⏰ Time Distribution Analysis")
        
        # Time breakdown pie chart
        time_breakdown = pd.DataFrame({
            "State": ["Active", "Idle", "Pause"],
            "Hours": [active_time/3600, total_idle/3600, total_pause/3600],
            "Color": [COLORS['productive'], COLORS['idle'], COLORS['pause']]
        })
        
        fig_pie = px.pie(
            time_breakdown, 
            values="Hours", 
            names="State",
            title="Session Time Distribution",
            color_discrete_sequence=[COLORS['productive'], COLORS['idle'], COLORS['pause']]
        )
        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)
        
        # Hourly activity heatmap
        if not df_employee.empty:
            df_employee['hour'] = df_employee['timestamp'].dt.hour
            hourly_activity = df_employee.groupby('hour').size().reset_index()
            hourly_activity.columns = ['Hour', 'Activity Count']
            
            fig_heatmap = px.bar(
                hourly_activity, 
                x='Hour', 
                y='Activity Count',
                title="Hourly Activity Pattern",
                color_discrete_sequence=[COLORS['apps']]
            )
            fig_heatmap.update_layout(height=300)
            st.plotly_chart(fig_heatmap, use_container_width=True)
    
    with tab3:
        st.subheader("💻 Application Usage Analysis")
        
        # Top applications used
        app_counts = df_employee["application"].value_counts().head(10).reset_index()
        app_counts.columns = ["Application", "Usage Count"]
        
        if not app_counts.empty:
            fig_apps = px.bar(
                app_counts, 
                x="Application", 
                y="Usage Count",
                title="Top 10 Applications Used",
                color_discrete_sequence=[COLORS['apps']]
            )
            fig_apps.update_layout(height=400)
            fig_apps.update_xaxes(tickangle=45)
            st.plotly_chart(fig_apps, use_container_width=True)
            
            # Application usage by time
            if 'duration_minutes' in df_employee.columns:
                app_time = df_employee.groupby('application')['duration_minutes'].sum().reset_index()
                app_time = app_time.sort_values('duration_minutes', ascending=False).head(10)
                
                fig_app_time = px.bar(
                    app_time,
                    x='application',
                    y='duration_minutes',
                    title="Application Usage by Time (Minutes)",
                    color_discrete_sequence=[COLORS['productive']]
                )
                fig_app_time.update_layout(height=400)
                fig_app_time.update_xaxes(tickangle=45)
                st.plotly_chart(fig_app_time, use_container_width=True)
        else:
            st.info("No application usage data available")
    
    with tab4:
        st.subheader("📊 Detailed Activity Breakdown")
        
        # Event timeline
        if not df_employee.empty:
            fig_timeline = px.scatter(
                df_employee, 
                x="timestamp", 
                y="event", 
                color="event",
                hover_data=["application", "window", "text"],
                title="Activity Timeline - Events Over Time",
                color_discrete_sequence=[COLORS['productive'], COLORS['apps'], COLORS['idle'], COLORS['pause']]
            )
            fig_timeline.update_layout(height=400)
            st.plotly_chart(fig_timeline, use_container_width=True)
        
        # Daily activity breakdown
        daily_metrics = get_daily_metrics([selected_employee], date_range[0], date_range[1])
        if not daily_metrics.empty and "summary" in daily_metrics.columns:
            st.subheader("📄 Daily Activity Summaries")
            for _, row in daily_metrics.sort_values(["date","chunk_index"]).iterrows():
                with st.expander(f"📅 {row['date']} - Chunk {row['chunk_index']}"):
                    st.write(row['summary'])
        else:
            st.info("⚠️ No daily summaries available for this employee in the selected range")


    # Raw data section (collapsible)
    with st.expander("🔍 View Raw Activity Data"):
        st.subheader("📋 Latest Activity Events")
        st.dataframe(
            df_employee.sort_values("timestamp", ascending=False).head(20),
            use_container_width=True
        )

if __name__ == "__main__":
    dashboard_page()
