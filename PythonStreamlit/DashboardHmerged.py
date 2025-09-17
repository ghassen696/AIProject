import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
from elasticsearch import Elasticsearch
from typing import List, Tuple, Optional

# If you have this in a config file, import it; otherwise set your URL here
# from config import ELASTICSEARCH_URL
ELASTICSEARCH_URL = "http://localhost:9200"


# -----------------------------
# Utilities
# -----------------------------
def _to_date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _to_epoch_millis(d: date, end_of_day: bool = False) -> int:
    # employee_activity.timestamp is epoch_millis
    dt = datetime(d.year, d.month, d.day, 23, 59, 59, 999000) if end_of_day else datetime(d.year, d.month, d.day)
    return int(dt.timestamp() * 1000)


def _safe_mean(series: pd.Series) -> Optional[float]:
    try:
        return float(series.mean()) if not series.empty else np.nan
    except Exception:
        return np.nan


def _safe_sum(series: pd.Series) -> float:
    try:
        return float(series.sum()) if not series.empty else 0.0
    except Exception:
        return 0.0


# -----------------------------
# ES Queries (aligned to mappings)
# -----------------------------
def get_available_employees(es: Elasticsearch) -> List[str]:
    try:
        # Use employee_daily_reports as canonical employee list; fallback to activity index.
        query = {"size": 0, "aggs": {"emp": {"terms": {"field": "employee_id.keyword", "size": 10000}}}}
        res = es.search(index="employee_daily_reports", body=query)
        buckets = res.get("aggregations", {}).get("emp", {}).get("buckets", [])
        emps = [b["key"] for b in buckets]
        if emps:
            return emps
        # fallback
        res2 = es.search(index="employee_activity", body=query)
        buckets2 = res2.get("aggregations", {}).get("emp", {}).get("buckets", [])
        return [b["key"] for b in buckets2]
    except Exception:
        return []


def get_daily_reports_data(es: Elasticsearch, start_d: date, end_d: date, employees: List[str]) -> pd.DataFrame:
    try:
        must = [{"range": {"date": {"gte": _to_date_str(start_d), "lte": _to_date_str(end_d)}}}]
        if employees:
            must.append({"terms": {"employee_id.keyword": employees}})
        body = {"query": {"bool": {"must": must}}, "size": 10000}
        res = es.search(index="employee_daily_reports", body=body)

        rows = []
        for hit in res.get("hits", {}).get("hits", []):
            s = hit.get("_source", {})
            totals = s.get("totals", {}) or {}
            wp = s.get("work_patterns", {}) or {}
            pm = (wp.get("productivity_metrics") or {}) if wp else {}
            ta = (wp.get("time_allocation") or {}) if wp else {}
            wd = (wp.get("work_distribution") or {}) if wp else {}

            rows.append(
                {
                    "employee_id": s.get("employee_id"),
                    "date": pd.to_datetime(s.get("date")),
                    "total_active_minutes": float(totals.get("active", 0.0)),
                    "total_idle_minutes": float(totals.get("idle", 0.0)),
                    "overall_productivity_score": float(pm.get("overall_productivity_score", np.nan)),
                    "average_focus_level": float(pm.get("average_focus_level", np.nan)),
                    "deep_work_percentage": float(pm.get("deep_work_percentage", np.nan)),
                    "deep_work_time_minutes": float(pm.get("deep_work_time_minutes", np.nan)),
                    "high_focus_percentage": float(pm.get("high_focus_percentage", np.nan)),
                    "active_percentage": float(ta.get("active_percentage", np.nan)),
                    "coding_percentage": float(wd.get("Coding", np.nan)),
                }
            )
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def get_work_patterns_data(es: Elasticsearch, start_d: date, end_d: date, employees: List[str]) -> pd.DataFrame:
    try:
        must = [{"range": {"date": {"gte": _to_date_str(start_d), "lte": _to_date_str(end_d)}}}]
        if employees:
            must.append({"terms": {"employee_id.keyword": employees}})
        body = {"query": {"bool": {"must": must}}, "size": 10000}
        res = es.search(index="employee_work_patterns", body=body)

        rows = []
        for hit in res.get("hits", {}).get("hits", []):
            s = hit.get("_source", {})
            patterns = s.get("patterns", {}) or {}
            pm = patterns.get("productivity_metrics", {}) or {}
            ta = patterns.get("time_allocation", {}) or {}
            wd = patterns.get("work_distribution", {}) or {}

            rows.append(
                {
                    "employee_id": s.get("employee_id"),
                    "date": pd.to_datetime(s.get("date")),
                    "analysis_timestamp": pd.to_datetime(s.get("analysis_timestamp")) if s.get("analysis_timestamp") else pd.NaT,
                    "overall_productivity_score": float(pm.get("overall_productivity_score", np.nan)),
                    "average_focus_level": float(pm.get("average_focus_level", np.nan)),
                    "deep_work_percentage": float(pm.get("deep_work_percentage", np.nan)),
                    "deep_work_time_minutes": float(pm.get("deep_work_time_minutes", np.nan)),
                    "high_focus_percentage": float(pm.get("high_focus_percentage", np.nan)),
                    "total_active_minutes": float(ta.get("total_active_minutes", np.nan)),
                    "total_idle_minutes": float(ta.get("total_idle_minutes", np.nan)),
                    "active_percentage": float(ta.get("active_percentage", np.nan)),
                    "coding_percentage": float(wd.get("Coding", np.nan)),
                    "total_chunks_analyzed": patterns.get("total_chunks_analyzed", np.nan),
                }
            )
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def get_classifications_data(es: Elasticsearch, start_d: date, end_d: date, employees: List[str]) -> pd.DataFrame:
    try:
        must = [{"range": {"date": {"gte": _to_date_str(start_d), "lte": _to_date_str(end_d)}}}]
        if employees:
            must.append({"terms": {"employee_id.keyword": employees}})
        body = {"query": {"bool": {"must": must}}, "size": 10000}
        res = es.search(index="employee_daily_classifications", body=body)

        rows = []
        for hit in res.get("hits", {}).get("hits", []):
            s = hit.get("_source", {})
            c = s.get("classification", {}) or {}

            pa = c.get("primary_activity", {}) or {}
            pi = c.get("productivity_indicators", {}) or {}
            ta = c.get("time_analysis", {}) or {}

            rows.append(
                {
                    "employee_id": s.get("employee_id"),
                    "date": pd.to_datetime(s.get("date")),
                    "chunk_index": s.get("chunk_index"),
                    "classification_id": s.get("classification_id"),
                    "key_applications": c.get("key_applications"),
                    "primary_activity_category": pa.get("category"),
                    "primary_activity_subcategory": pa.get("subcategory"),
                    "primary_activity_confidence": float(pa.get("confidence", np.nan)),
                    "focus_level": float(pi.get("focus_level", np.nan)),
                    "deep_work_detected": bool(pi.get("deep_work_detected", False)),
                    "task_switching_frequency": pi.get("task_switching_frequency"),
                    "active_time_minutes": float(ta.get("active_time_minutes", 0)),
                    "idle_time_minutes": float(ta.get("idle_time_minutes", 0)),
                    "estimated_productivity": float(ta.get("estimated_productivity", np.nan)),
                }
            )
        df = pd.DataFrame(rows)
        # normalize task_switching labels (Low/Medium/High) capitalization if needed
        if "task_switching_frequency" in df.columns and not df.empty:
            df["task_switching_frequency"] = df["task_switching_frequency"].astype(str).str.title()
        return df
    except Exception:
        return pd.DataFrame()


def get_daily_summaries_data(es: Elasticsearch, start_d: date, end_d: date, employees: List[str]) -> pd.DataFrame:
    try:
        must = [{"range": {"date": {"gte": _to_date_str(start_d), "lte": _to_date_str(end_d)}}}]
        if employees:
            must.append({"terms": {"employee_id.keyword": employees}})
        body = {"query": {"bool": {"must": must}}, "size": 10000}
        res = es.search(index="employee_daily_summaries", body=body)
        rows = []
        for hit in res.get("hits", {}).get("hits", []):
            s = hit.get("_source", {})
            rows.append(
                {
                    "employee_id": s.get("employee_id"),
                    "date": pd.to_datetime(s.get("date")),
                    "chunk_index": s.get("chunk_index"),
                    "summary_id": s.get("summary_id"),
                    "summary": s.get("summary"),
                }
            )
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def get_activity_logs(es: Elasticsearch, start_d: date, end_d: date, employees: List[str]) -> pd.DataFrame:
    """
    employee_activity:
      - timestamp: epoch_millis
      - application: keyword
      - event, control, window, text (semi-structured)
      - idle_duration_sec, duration_minutes
    """
    try:
        must = [
            {
                "range": {
                    "timestamp": {
                        "gte": _to_epoch_millis(start_d),
                        "lte": _to_epoch_millis(end_d, end_of_day=True),
                    }
                }
            }
        ]
        if employees:
            must.append({"terms": {"employee_id": employees}})  # employee_id is keyword in mapping
        body = {"query": {"bool": {"must": must}}, "size": 10000}
        res = es.search(index="employee_activity", body=body)
        rows = []
        for hit in res.get("hits", {}).get("hits", []):
            s = hit.get("_source", {})
            rows.append(
                {
                    "employee_id": s.get("employee_id"),
                    "timestamp": pd.to_datetime(s.get("timestamp"), unit="ms", errors="coerce"),
                    "application": s.get("application"),
                    "event": s.get("event"),
                    "window": s.get("window"),
                    "control": s.get("control"),
                    "text": s.get("text"),
                    "duration_minutes": float(s.get("duration_minutes", np.nan)) if s.get("duration_minutes") is not None else np.nan,
                    "idle_duration_sec": float(s.get("idle_duration_sec", np.nan)) if s.get("idle_duration_sec") is not None else np.nan,
                    "session_id": s.get("session_id"),
                    "seq_num": s.get("seq_num"),
                }
            )
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


# -----------------------------
# Data merge
# -----------------------------
def combine_data_sources(dr: pd.DataFrame, wp: pd.DataFrame, cl: pd.DataFrame) -> pd.DataFrame:
    # Prefer daily_reports as base; else fall back to work_patterns; else empty
    base = dr.copy() if not dr.empty else (wp.copy() if not wp.empty else pd.DataFrame())
    if base.empty:
        return pd.DataFrame()

    # Ensure date type aligns for merging
    if "date" in base.columns:
        base["date"] = pd.to_datetime(base["date"]).dt.normalize()
    if not cl.empty and "date" in cl.columns:
        cl["date"] = pd.to_datetime(cl["date"]).dt.normalize()

    # Aggregate classifications per (employee_id, date)
    if not cl.empty:
        agg = (
            cl.groupby(["employee_id", "date"])
            .agg(
                focus_level=("focus_level", "mean"),
                estimated_productivity=("estimated_productivity", "mean"),
                active_time_minutes=("active_time_minutes", "sum"),
                idle_time_minutes=("idle_time_minutes", "sum"),
                primary_activity_category=("primary_activity_category", lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan),
                task_switching_frequency=("task_switching_frequency", lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan),
            )
            .reset_index()
        )
        base = base.merge(agg, on=["employee_id", "date"], how="left")

    # If work_patterns has metrics that daily_reports lacks for the same dates/employees, left-join them
    if not wp.empty:
        wp_sel = wp[
            [
                c
                for c in [
                    "employee_id",
                    "date",
                    "overall_productivity_score",
                    "average_focus_level",
                    "deep_work_percentage",
                    "deep_work_time_minutes",
                    "high_focus_percentage",
                    "active_percentage",
                    "total_active_minutes",
                    "total_idle_minutes",
                    "coding_percentage",
                ]
                if c in wp.columns
            ]
        ].drop_duplicates(["employee_id", "date"], keep="last")
        base = base.merge(wp_sel, on=["employee_id", "date"], how="left", suffixes=("", "_wp"))

        # Prefer explicit values from daily_reports; fill nulls from work_patterns
        for col in ["overall_productivity_score", "average_focus_level", "deep_work_percentage", "deep_work_time_minutes", "active_percentage", "total_active_minutes", "total_idle_minutes", "coding_percentage"]:
            if col in base.columns and f"{col}_wp" in base.columns:
                base[col] = base[col].fillna(base[f"{col}_wp"])
                base.drop(columns=[f"{col}_wp"], inplace=True, errors="ignore")

    return base


# -----------------------------
# Dashboard
# -----------------------------
def dashboard_page():
    # Styling
    st.markdown(
        """
        <style>
        .main-header {
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            padding: 2rem;
            border-radius: 15px;
            color: white;
            text-align: center;
            margin-bottom: 2rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Connect ES
    try:
        es = Elasticsearch(ELASTICSEARCH_URL)
        es_connected = es.ping()
    except Exception:
        es = None
        es_connected = False

    st.markdown(
        """
        <div class="main-header">
            <h1>🏢 Employee Analytics Dashboard</h1>
            <p>Productivity • Focus • Activity • Applications</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not es_connected:
        st.error("⚠️ Elasticsearch connection failed. Please check ELASTICSEARCH_URL and cluster status.")
        return
    else:
        st.success("✅ Connected to Elasticsearch")

    # Sidebar filters
    st.sidebar.header("🔧 Dashboard Controls")
    date_mode = st.sidebar.radio("📅 Date Selection", ["Single Day", "Date Range"], index=1)

    if date_mode == "Single Day":
        selected = st.sidebar.date_input(
            "Select date",
            value=datetime.now().date() - timedelta(days=1),
            max_value=datetime.now().date(),
        )
        start_d = end_d = selected
    else:
        c1, c2 = st.sidebar.columns(2)
        with c1:
            start_d = st.sidebar.date_input(
                "Start date",
                value=datetime.now().date() - timedelta(days=7),
                max_value=datetime.now().date(),
            )
        with c2:
            end_d = st.sidebar.date_input(
                "End date",
                value=datetime.now().date(),
                max_value=datetime.now().date(),
            )
        if isinstance(start_d, list):  # streamlit quirk guard
            start_d = start_d[0]
        if isinstance(end_d, list):
            end_d = end_d[0]

    employees = get_available_employees(es)
    employee_opts = ["All Employees"] + employees
    selected_emps = st.sidebar.multiselect("👥 Employees", employee_opts, default=["All Employees"])
    if "All Employees" in selected_emps or not selected_emps:
        final_emps = employees
    else:
        final_emps = selected_emps

    if st.sidebar.button("🔄 Refresh"):
        st.rerun()

    # Load data
    with st.spinner("Loading data..."):
        dr = get_daily_reports_data(es, start_d, end_d, final_emps)
        wp = get_work_patterns_data(es, start_d, end_d, final_emps)
        cl = get_classifications_data(es, start_d, end_d, final_emps)
        sm = get_daily_summaries_data(es, start_d, end_d, final_emps)
        al = get_activity_logs(es, start_d, end_d, final_emps)

    if dr.empty and wp.empty and cl.empty and al.empty and sm.empty:
        st.warning("No data for the selected filters.")
        return

    combined = combine_data_sources(dr, wp, cl)

    # -----------------------------
    # KPIs
    # -----------------------------
    st.markdown("## 📈 Key Performance Indicators")

    c1, c2, c3, c4, c5 = st.columns(5)

    # Avg Productivity: prefer overall_productivity_score, else estimated_productivity
    with c1:
        if "overall_productivity_score" in combined.columns and combined["overall_productivity_score"].notna().any():
            avg_prod = _safe_mean(combined["overall_productivity_score"])
        elif "estimated_productivity" in combined.columns and combined["estimated_productivity"].notna().any():
            avg_prod = _safe_mean(combined["estimated_productivity"])
        else:
            avg_prod = np.nan
        st.metric("Avg Productivity", f"{avg_prod:.1%}" if pd.notna(avg_prod) else "N/A")

    # Total Active Hours
    with c2:
        total_active_minutes = 0.0
        if "total_active_minutes" in combined.columns:
            total_active_minutes += _safe_sum(combined["total_active_minutes"])
        # Add classified active_time_minutes if present (complements day totals)
        if "active_time_minutes" in combined.columns:
            total_active_minutes = max(total_active_minutes, _safe_sum(combined["active_time_minutes"]))  # avoid double counting
        st.metric("Total Active Hours", f"{total_active_minutes/60:.0f}h")

    # Avg Focus Level (prefer average_focus_level, else focus_level)
    with c3:
        if "average_focus_level" in combined.columns and combined["average_focus_level"].notna().any():
            avg_focus = _safe_mean(combined["average_focus_level"])
        elif "focus_level" in combined.columns and combined["focus_level"].notna().any():
            avg_focus = _safe_mean(combined["focus_level"])
        else:
            avg_focus = np.nan
        st.metric("Avg Focus Level", f"{avg_focus:.1%}" if pd.notna(avg_focus) else "N/A")

    # Deep Work %
    with c4:
        if "deep_work_percentage" in combined.columns and combined["deep_work_percentage"].notna().any():
            deep_pct = _safe_mean(combined["deep_work_percentage"])
            st.metric("Deep Work %", f"{deep_pct:.1%}")
        else:
            st.metric("Deep Work %", "N/A")

    with c5:
        st.metric("Active Employees", combined["employee_id"].nunique() if "employee_id" in combined.columns else 0)

    st.markdown("---")

    # -----------------------------
    # Row 1 Charts
    # -----------------------------
    r1c1, r1c2 = st.columns(2)

    with r1c1:
        st.markdown("### 📊 Productivity Trend")
        if "date" in combined.columns:
            if "overall_productivity_score" in combined.columns and combined["overall_productivity_score"].notna().any():
                trend = combined.groupby("date")["overall_productivity_score"].mean().reset_index()
                ycol = "overall_productivity_score"
            elif "estimated_productivity" in combined.columns and combined["estimated_productivity"].notna().any():
                trend = combined.groupby("date")["estimated_productivity"].mean().reset_index()
                ycol = "estimated_productivity"
            else:
                trend = pd.DataFrame()
                ycol = None

            if not trend.empty and ycol:
                fig = px.line(trend, x="date", y=ycol, title="Daily Productivity Trend")
                fig.update_layout(yaxis_tickformat=".1%")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No productivity time series available.")
        else:
            st.info("No date field to build trend.")

    with r1c2:
        st.markdown("### ⏱️ Active vs Idle (Avg per Employee)")
        if {"employee_id"}.issubset(combined.columns) and (
            "total_active_minutes" in combined.columns or "total_idle_minutes" in combined.columns
        ):
            tdf = combined.groupby("employee_id").agg(
                total_active_minutes=("total_active_minutes", "mean") if "total_active_minutes" in combined.columns else ("employee_id", "size"),
                total_idle_minutes=("total_idle_minutes", "mean") if "total_idle_minutes" in combined.columns else ("employee_id", "size"),
            ).reset_index()

            # Replace bogus counts from the fallback aggregation with 0
            if "total_active_minutes" in combined.columns is False:
                tdf["total_active_minutes"] = 0
            if "total_idle_minutes" in combined.columns is False:
                tdf["total_idle_minutes"] = 0

            fig = go.Figure(
                data=[
                    go.Bar(name="Active", x=tdf["employee_id"], y=tdf["total_active_minutes"]),
                    go.Bar(name="Idle", x=tdf["employee_id"], y=tdf["total_idle_minutes"]),
                ]
            )
            fig.update_layout(barmode="stack", title="Average Daily Time Distribution by Employee", xaxis_title="Employee", yaxis_title="Minutes")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No active/idle data available.")

    # -----------------------------
    # Row 2 Charts (Apps & Activities)
    # -----------------------------
    r2c1, r2c2 = st.columns(2)

    with r2c1:
        st.markdown("### 🔑 Top Applications Used (Classifications)")
        if not cl.empty and "key_applications" in cl.columns:
            apps = cl["key_applications"].dropna().explode()
            if not apps.empty:
                app_counts = apps.value_counts().nlargest(12).reset_index()
                app_counts.columns = ["Application", "Count"]
                fig = px.bar(app_counts, x="Application", y="Count", title="Most Used Applications")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No key_applications found.")
        else:
            st.info("No classification app data.")

    with r2c2:
        st.markdown("### 🎯 Primary Activity Breakdown")
        if not cl.empty and "primary_activity_category" in cl.columns and cl["primary_activity_category"].notna().any():
            counts = cl["primary_activity_category"].value_counts()
            fig = px.pie(values=counts.values, names=counts.index, title="Primary Activity Distribution")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No primary activity data.")

    # -----------------------------
    # Row 3 Charts (Deep Work & Switching)
    # -----------------------------
    r3c1, r3c2 = st.columns(2)

    with r3c1:
        st.markdown("### 🧠 Deep Work (Minutes per Employee)")
        # Prefer deep_work_time_minutes from combined (reports/work_patterns)
        if "deep_work_time_minutes" in combined.columns and combined["deep_work_time_minutes"].notna().any():
            ddf = combined.groupby("employee_id")["deep_work_time_minutes"].mean().reset_index()
            fig = px.bar(ddf, x="employee_id", y="deep_work_time_minutes", title="Avg Deep Work Minutes by Employee")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No deep work minutes available.")

    with r3c2:
        st.markdown("### 🔄 Task Switching Frequency")
        if not cl.empty and "task_switching_frequency" in cl.columns and cl["task_switching_frequency"].notna().any():
            switch_counts = cl["task_switching_frequency"].value_counts()
            fig = px.bar(x=switch_counts.index, y=switch_counts.values, title="Task Switching Frequency Distribution", labels={"x": "Frequency", "y": "Count"})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No task switching data.")

    # -----------------------------
    # Activity Logs (Raw)
    # -----------------------------
    st.markdown("## 📝 Raw Activity (employee_activity)")
    if not al.empty:
        c4a, c4b = st.columns(2)

        with c4a:
            st.markdown("### 🖥️ Application Usage (Events)")
            if "application" in al.columns and al["application"].notna().any():
                app_counts = al["application"].value_counts().nlargest(12).reset_index()
                app_counts.columns = ["Application", "Events"]
                fig = px.bar(app_counts, x="Application", y="Events", title="Top Applications by Event Count")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No application field in activity logs.")

        with c4b:
            st.markdown("### 💤 Idle Duration (Seconds)")
            if "idle_duration_sec" in al.columns and al["idle_duration_sec"].notna().any():
                fig = px.histogram(al, x="idle_duration_sec", nbins=30, title="Idle Duration Distribution (s)")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No idle_duration_sec in activity logs.")

        # Timeline of events (proxy for engagement)
        st.markdown("### ⏱️ Events Timeline")
        if {"timestamp", "employee_id"}.issubset(al.columns):
            # Count events per hour per employee
            tmp = al.copy()
            tmp["hour"] = tmp["timestamp"].dt.floor("H")
            hourly = tmp.groupby(["employee_id", "hour"]).size().reset_index(name="events")
            fig = px.line(hourly, x="hour", y="events", color="employee_id", title="Events per Hour (by Employee)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No timestamp/employee_id to build timeline.")

        st.dataframe(al.sort_values("timestamp", ascending=False).head(200), use_container_width=True)
    else:
        st.info("No activity logs for the selected period.")

    # -----------------------------
    # Employee Summary Table
    # -----------------------------
    st.markdown("## 📋 Employee Performance Summary")
    if not combined.empty:
        cols = []
        for c in [
            "overall_productivity_score",
            "estimated_productivity",
            "average_focus_level",
            "focus_level",
            "deep_work_percentage",
            "total_active_minutes",
            "total_idle_minutes",
            "coding_percentage",
        ]:
            if c in combined.columns:
                cols.append(c)

        if cols:
            summary = combined.groupby("employee_id")[cols].mean().reset_index()

            # Pretty columns
            rename_map = {
                "overall_productivity_score": "Productivity Score",
                "estimated_productivity": "Estimated Productivity",
                "average_focus_level": "Focus Level",
                "focus_level": "Focus Level (CL)",
                "deep_work_percentage": "Deep Work %",
                "total_active_minutes": "Avg Active (min)",
                "total_idle_minutes": "Avg Idle (min)",
                "coding_percentage": "Coding %",
            }
            summary = summary.rename(columns=rename_map)

            # Percent-format known % metrics
            for pct_col in ["Productivity Score", "Estimated Productivity", "Focus Level", "Focus Level (CL)", "Deep Work %", "Coding %"]:
                if pct_col in summary.columns:
                    summary[pct_col] = summary[pct_col].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "N/A")

            st.dataframe(summary, use_container_width=True)
        else:
            st.info("No aggregatable performance columns available.")
    else:
        st.info("No combined daily data to summarize.")

    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# Run locally for quick testing
if __name__ == "__main__":
    dashboard_page()
