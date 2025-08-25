"""import streamlit as st
import streamlit.components.v1 as components
import urllib.parse
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

# --- Page config ---
st.set_page_config(page_title="Huawei Activity Monitoring", layout="wide")
st.title("📊 Huawei Employee Activity Monitoring Dashboard")
st.markdown("This dashboard shows real-time activity logs monitored and analyzed by the system.")
st.markdown('<style>div.block-container{padding-top:1rem;}</style>',unsafe_allow_html=True)

# --- Elasticsearch client ---
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

# --- Fetch dynamic filter values ---
index_name = "keylogger_events"
employee_ids = get_unique_terms(index_name, "employee_id")

# --- Sidebar Filters ---
st.sidebar.header("Filters")

# --- Time range: preset or custom ---
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
selected_employees = st.sidebar.multiselect("Select Employee ID(s)", options=employee_ids, default=employee_ids)

# --- Clear filters button ---
if st.sidebar.button("Clear All Filters"):
    # Reset widgets by rerunning and clearing selections from session state
    for key in st.session_state.keys():
        del st.session_state[key]
    st.rerun()

# --- Helper to build Kibana filters ---
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

# --- Convert start and end datetime to Kibana-compatible ISO8601 string ---
start_str = start_dt.isoformat()
end_str = end_dt.isoformat()

# --- Build Kibana URL ---
base_url = "http://localhost:5601/app/dashboards#/view/1df79be0-5ce8-11f0-b604-0ba41f78bced"
_g = f"(refreshInterval:(pause:!t,value:0),time:(from:'{start_str}',to:'{end_str}'),{filters_kibana})"

kibana_url = f"{base_url}?embed=true&_g={urllib.parse.quote(_g)}"

# --- Main content ---
components.iframe(kibana_url, height=1200, scrolling=True)

st.markdown("---")
st.caption("Filters dynamically update the embedded Kibana dashboard.")
"""
