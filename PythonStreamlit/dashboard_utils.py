import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
# dashboard_utils.py
import subprocess
import sys
import json

class ElasticsearchConnector:
    """Handle Elasticsearch connections and queries"""

    def __init__(self, host="localhost", port=9200):
        self.es = Elasticsearch([f"http://{host}:{port}"])

    def query_kpi_summary(self, employee_ids=None, start_date=None, end_date=None):
        """Fetch enriched KPI summaries from employee_kpi_summary2 index"""
        query = {"query": {"bool": {"must": []}}}

        if employee_ids:
            query["query"]["bool"]["must"].append({"terms": {"employee_id.keyword": employee_ids}})

        if start_date and end_date:
            query["query"]["bool"]["must"].append({
                "range": {
                    "date": {
                        "gte": start_date.strftime("%Y-%m-%d"),
                        "lte": end_date.strftime("%Y-%m-%d")
                    }
                }
            })

        try:
            result = self.es.search(index="employee_kpi_summary3", body=query, size=1000)
            return [hit["_source"] for hit in result["hits"]["hits"]]
        except Exception as e:
            st.error(f"Error querying KPI summary: {str(e)}")
            return []


    def query_employee_activity(self, employee_ids=None, start_date=None, end_date=None):
        """Query raw employee activity logs"""
        query = {"query": {"bool": {"must": []}}}

        if employee_ids:
            query["query"]["bool"]["must"].append({"terms": {"employee_id": employee_ids}})

        if start_date and end_date:
            query["query"]["bool"]["must"].append({
                "range": {
                    "timestamp": {
                        "gte": int(start_date.timestamp() * 1000),
                        "lte": int(end_date.timestamp() * 1000)
                    }
                }
            })

        try:
            result = self.es.search(index="employee_activity", body=query, size=1000)
            return [hit["_source"] for hit in result["hits"]["hits"]]
        except Exception as e:
            st.error(f"Error querying employee activity: {str(e)}")
            return []

    def query_daily_reports(self, employee_ids=None, start_date=None, end_date=None):
        """Query aggregated daily reports"""
        query = {"query": {"bool": {"must": []}}}

        if employee_ids:
            query["query"]["bool"]["must"].append({"terms": {"employee_id.keyword": employee_ids}})

        if start_date and end_date:
            query["query"]["bool"]["must"].append({
                "range": {
                    "date": {
                        "gte": start_date.strftime("%Y-%m-%d"),
                        "lte": end_date.strftime("%Y-%m-%d")
                    }
                }
            })

        try:
            result = self.es.search(index="employee_daily_reports", body=query, size=1000)
            return [hit["_source"] for hit in result["hits"]["hits"]]
        except Exception as e:
            st.error(f"Error querying daily reports: {str(e)}")
            return []

    def get_all_employee_ids(self):
        """Get all unique employee IDs from daily reports"""
        query = {
            "aggs": {
                "unique_employees": {
                    "terms": {"field": "employee_id.keyword", "size": 1000}
                }
            },
            "size": 0
        }
        try:
            result = self.es.search(index="employee_daily_reports", body=query)
            buckets = result["aggregations"]["unique_employees"]["buckets"]
            return [bucket["key"] for bucket in buckets]
        except Exception as e:
            st.error(f"Error getting employee IDs: {str(e)}")
            return []

    def query_insights_reports(self, employee_ids=None, insight_type="7_day_insights"):
        """
        Fetch insights reports (currently from employee_daily_reports).
        Supports: 7_day_insights (last 7 days).
        """
        if insight_type == "7_day_insights":
            start_date = datetime.now() - timedelta(days=7)
            end_date = datetime.now()
            return self.query_daily_reports(employee_ids, start_date, end_date)

        # Future expansion
        st.warning(f"Insight type '{insight_type}' not implemented. Returning empty list.")
        return []


class DataProcessor:
    """Process and format data for visualizations"""

    @staticmethod
    def process_daily_reports(daily_data):
        """Convert daily reports to DataFrame"""
        if not daily_data:
            return pd.DataFrame()

        processed = []
        for report in daily_data:
            row = {
                'employee_id': report.get('employee_id'),
                'date': report.get('date'),
                'daily_summary': report.get('daily_summary', ''),
                'total_active_minutes': report.get('work_patterns', {}).get('time_allocation', {}).get(
                    'total_active_minutes', report.get('totals', {}).get('active', 0) * 60),
                'total_idle_minutes': report.get('work_patterns', {}).get('time_allocation', {}).get(
                    'total_idle_minutes', report.get('totals', {}).get('idle', 0) * 60),
                'active_percentage': report.get('work_patterns', {}).get('time_allocation', {}).get(
                    'active_percentage', 0),
                'productivity_score': report.get('work_patterns', {}).get('productivity_metrics', {}).get(
                    'overall_productivity_score', 0),
                'focus_level': report.get('work_patterns', {}).get('productivity_metrics', {}).get(
                    'average_focus_level', 0),
                'deep_work_percentage': report.get('work_patterns', {}).get('productivity_metrics', {}).get(
                    'deep_work_percentage', 0),
                'work_distribution': report.get('work_patterns', {}).get('work_distribution', {})
            }
            processed.append(row)

        df = pd.DataFrame(processed)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        return df

    @staticmethod
    def process_kpi_summary(raw_reports: list) -> pd.DataFrame:
        """
        Process raw KPI summaries from Elasticsearch into a clean DataFrame for the dashboard.
        Computes average pause duration safely and handles missing fields.
        """
        import pandas as pd

        processed = []

        for report in raw_reports:
            pause_count = report.get('pause_count', 0)
            pause_total_min = report.get('pause_total_min', 0.0)
            # Avoid division by zero
            avg_pause_duration = pause_total_min / pause_count if pause_count > 0 else 0.0

            processed.append({
                'employee_id': report.get('employee_id', 'Unknown'),
                'date': pd.to_datetime(report.get('date', None)),
                'total_sec': report.get('total_sec', 0),
                'active_sec': report.get('active_sec', 0),
                'idle_sec': report.get('idle_sec', 0),
                'pause_sec': report.get('pause_sec', 0),
                'active_pct': report.get('active_pct', 0.0),
                'idle_pct': report.get('idle_pct', 0.0),
                'pause_pct': report.get('pause_pct', 0.0),
                'pause_count': pause_count,
                'pause_total_min': pause_total_min,
                'avg_pause_duration_min': avg_pause_duration,
                'total_keystrokes': report.get('total_keystrokes', 0),
            })

        df = pd.DataFrame(processed)

        # Optional: sort by date and employee
        if 'date' in df.columns:
            df.sort_values(['employee_id', 'date'], inplace=True)

        return df



    @staticmethod
    def calculate_kpis(df):
        """Calculate aggregate KPIs"""
        if df.empty:
            return {
                'total_active_hours': 0,
                'total_idle_hours': 0,
                'avg_productivity': 0,
                'avg_focus_level': 0,
                'avg_deep_work': 0
            }

        return {
            'total_active_hours': df['total_active_minutes'].sum() / 60,
            'total_idle_hours': df['total_idle_minutes'].sum() / 60,
            'avg_productivity': df['productivity_score'].mean(),
            'avg_focus_level': df['focus_level'].mean(),
            'avg_deep_work': df['deep_work_percentage'].mean()
        }

    @staticmethod
    def aggregate_work_distribution(df):
        """Aggregate work distribution across all records"""
        if df.empty:
            return {}

        all_distributions = {}
        for _, row in df.iterrows():
            work_dist = row['work_distribution']
            if isinstance(work_dist, dict):
                for activity, percentage in work_dist.items():
                    if activity not in all_distributions:
                        all_distributions[activity] = []
                    all_distributions[activity].append(percentage)

        avg_distribution = {
            activity: sum(percentages) / len(percentages)
            for activity, percentages in all_distributions.items()
        }

        return avg_distribution


class ChartGenerator:
    """Generate various charts and visualizations"""

    @staticmethod
    def create_kpi_cards(kpis):
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric("Total Active Hours", f"{kpis['total_active_hours']:.1f}")
        with col2:
            st.metric("Total Idle Hours", f"{kpis['total_idle_hours']:.1f}")
        with col3:
            st.metric("Avg Productivity", f"{kpis['avg_productivity']:.2f}")
        with col4:
            st.metric("Avg Focus Level", f"{kpis['avg_focus_level']:.2f}")
        with col5:
            st.metric("Avg Deep Work %", f"{kpis['avg_deep_work']:.1f}%")

    @staticmethod
    def create_productivity_trend(df):
        if df.empty:
            st.info("No data available for productivity trend")
            return

        df_sorted = df.sort_values('date')

        fig = px.line(
            df_sorted,
            x='date',
            y='productivity_score',
            color='employee_id',
            title='Productivity Score Trend Over Time'
        )
        fig.update_layout(xaxis_title="Date", yaxis_title="Productivity Score", height=400)
        st.plotly_chart(fig, use_container_width=True)

    @staticmethod
    def create_work_distribution_pie(work_dist):
        if not work_dist:
            st.info("No work distribution data available")
            return

        fig = px.pie(
            values=list(work_dist.values()),
            names=list(work_dist.keys()),
            title="Average Work Distribution"
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    @staticmethod
    def create_active_vs_idle_bar(df):
        if df.empty:
            st.info("No data available for active vs idle comparison")
            return

        df_melted = pd.melt(
            df[['employee_id', 'date', 'total_active_minutes', 'total_idle_minutes']],
            id_vars=['employee_id', 'date'],
            value_vars=['total_active_minutes', 'total_idle_minutes'],
            var_name='time_type',
            value_name='minutes'
        )

        df_melted['time_type'] = df_melted['time_type'].map({
            'total_active_minutes': 'Active',
            'total_idle_minutes': 'Idle'
        })

        fig = px.bar(
            df_melted,
            x='date',
            y='minutes',
            color='time_type',
            facet_col='employee_id',
            title="Active vs Idle Time by Employee"
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)


from datetime import datetime, timedelta

def create_sidebar_filters(es_connector):
    """Sidebar filters for selecting dates and employees"""
    st.sidebar.header("📊 Dashboard Filters")

    # Date filter
    st.sidebar.subheader("Date Selection")
    date_option = st.sidebar.radio("Select date option:", ["Single Date", "Date Range"])

    if date_option == "Single Date":
        selected_date = st.sidebar.date_input("Select Date", datetime.now().date())
        start_date = end_date = datetime.combine(selected_date, datetime.min.time())
    else:
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(datetime.now().date() - timedelta(days=7), datetime.now().date()),
            max_value=datetime.now().date()
        )
        if len(date_range) == 2:
            start_date = datetime.combine(date_range[0], datetime.min.time())
            end_date = datetime.combine(date_range[1], datetime.max.time())
        else:
            start_date = end_date = datetime.combine(date_range[0], datetime.min.time())

    # Employee filter
    st.sidebar.subheader("Employee Selection")
    all_employees = es_connector.get_all_employee_ids()

    if st.session_state.role == "admin":
        # Admin can select all or specific employees
        if all_employees:
            employee_option = st.sidebar.radio("Select employees:", ["All Employees", "Specific Employees"])
            if employee_option == "All Employees":
                selected_employees = all_employees
            else:
                selected_employees = st.sidebar.multiselect("Choose employees:", all_employees)
        else:
            st.sidebar.warning("No employees found in database")
            selected_employees = []
    else:
        # Normal users can only see themselves
        selected_employees = [st.session_state.employee_id]
        st.sidebar.text(f"Employee: {selected_employees[0]}")  # just for info

    return start_date, end_date, selected_employees
