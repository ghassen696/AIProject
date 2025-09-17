import streamlit as st
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch
from config import ELASTICSEARCH_URL

es = Elasticsearch(ELASTICSEARCH_URL)

def show_admin_modifications_page():
    st.header("🛠️ Employee Modification Requests (Admin)")

    # Fetch all pending requests
    query = {
        "query": {
            "term": {"status.keyword": "pending"}
        },
        "size": 5000,
        "sort": [{"requested_at": {"order": "desc"}}]
    }

    res = es.search(index="employee_modification_requests", body=query)
    hits = res.get("hits", {}).get("hits", [])

    if not hits:
        st.info("No pending modification requests.")
        return

    # Convert to DataFrame
    df = pd.DataFrame([h["_source"] for h in hits])
    df["requested_at"] = pd.to_datetime(df["requested_at"])

    # Optional filters
    st.sidebar.subheader("Filters")
    employee_filter = st.sidebar.text_input("Filter by Employee ID")
    request_type_filter = st.sidebar.multiselect(
        "Request Type", options=df["request_type"].unique(), default=df["request_type"].unique()
    )

    filtered_df = df[df["request_type"].isin(request_type_filter)]
    if employee_filter:
        filtered_df = filtered_df[filtered_df["employee_id"].str.contains(employee_filter)]

    st.subheader(f"Pending Requests ({len(filtered_df)})")

    for idx, row in filtered_df.iterrows():
        with st.expander(f"Employee: {row['employee_id']} | Type: {row['request_type']}"):
            st.markdown(f"**Requested at:** {row['requested_at']}")
            st.markdown(f"**Time Interval:** {row['start_time']} → {row['end_time']}")
            st.markdown(f"**Reason:** {row['reason']}")

            # Fetch original activity for that interval
            start_ms = int(pd.to_datetime(row["start_time"]).timestamp() * 1000)
            end_ms = int(pd.to_datetime(row["end_time"]).timestamp() * 1000)
            activity_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"employee_id.keyword": row["employee_id"]}},
                            {"range": {"timestamp": {"gte": start_ms, "lte": end_ms}}}
                        ]
                    }
                },
                "size": 1000,
                "sort": [{"timestamp": {"order": "asc"}}]
            }
            activity_res = es.search(index="employee_activity", body=activity_query)
            activity_hits = activity_res.get("hits", {}).get("hits", [])
            if activity_hits:
                activity_df = pd.DataFrame([h["_source"] for h in activity_hits])
                activity_df["timestamp"] = pd.to_datetime(activity_df["timestamp"], unit="ms")
                st.dataframe(activity_df[["timestamp", "event", "application", "control"]], use_container_width=True)
            else:
                st.info("No activity found for this interval.")

            # Approve or Decline buttons
            col1, col2 = st.columns(2)
            if col1.button("✅ Approve", key=f"approve_{idx}"):
                es.update(
                    index="employee_modification_requests",
                    id=row.get("_id"),
                    body={"doc": {"status": "approved", "reviewed_at": datetime.utcnow().isoformat()}}
                )
                st.success("Request approved!")
                st.experimental_rerun()
            if col2.button("❌ Decline", key=f"decline_{idx}"):
                es.update(
                    index="employee_modification_requests",
                    id=row.get("_id"),
                    body={"doc": {"status": "declined", "reviewed_at": datetime.utcnow().isoformat()}}
                )
                st.warning("Request declined!")
                st.experimental_rerun()
