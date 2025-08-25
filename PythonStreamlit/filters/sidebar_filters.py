import streamlit as st


from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

def get_unique_terms(index_name, field, size=1000):
    body = {
        "size": 0,
        "aggs": {
            "unique_values": {
                "terms": {
                    "field": f"{field}.keyword",
                    "size": size
                }
            }
        }
    }
    try:
        res = es.search(index=index_name, body=body)
        return [bucket["key"] for bucket in res["aggregations"]["unique_values"]["buckets"]]
    except Exception as e:
        return []

def get_event_types(index_name="keylogger_events"):
    return get_unique_terms(index_name, "event")

def get_applications(index_name="keylogger_events"):
    return get_unique_terms(index_name, "application")

def get_employee_ids(index_name="keylogger_events"):
    return get_unique_terms(index_name, "employee_id")


def show_sidebar_filters():
    st.sidebar.header("Filters")

    time_ranges = ["Last 15 minutes", "Last 30 minutes", "Last 1 hour", "Last 24 hours", "Last 7 days"]
    time_range = st.sidebar.selectbox("Time Range", time_ranges, index=3)

    events = get_event_types()
    applications = get_applications()
    employee_ids = get_employee_ids()

    # To clear filters, keep track of a session state flag
    if 'clear_filters' not in st.session_state:
        st.session_state.clear_filters = False

    if st.sidebar.button("Clear All Filters"):
        st.session_state.clear_filters = True
        st.experimental_rerun()

    if st.session_state.clear_filters:
        selected_events = []
        selected_apps = []
        selected_employees = []
        st.session_state.clear_filters = False  # reset flag after clearing
    else:
        selected_events = st.sidebar.multiselect("Event Type(s)", events, default=events)
        selected_apps = st.sidebar.multiselect("Application(s)", applications, default=applications)
        selected_employees = st.sidebar.multiselect("Employee ID(s)", employee_ids, default=employee_ids)

    window_search = st.sidebar.text_input("Search Window Title (keyword)", "")

    return time_range, selected_events, selected_apps, selected_employees, window_search
