import sys
import os
import uuid
from datetime import datetime
from io import BytesIO

import streamlit as st
import pandas as pd
from elasticsearch import Elasticsearch

from test.employee_profile_builder import match_task_to_employees, store_admin_feedback

# -----------------------------
# Configuration
# -----------------------------
ELASTICSEARCH_URL = "http://localhost:9200"
es = Elasticsearch(ELASTICSEARCH_URL)

if not es.ping():
    st.error(f"Elasticsearch is not reachable at {ELASTICSEARCH_URL}")
    st.stop()

# -----------------------------
# Provide CSV template
# -----------------------------
def provide_template():
    df = pd.DataFrame(columns=["Task Description", "Deadline", "Priority", "Notes"])
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    st.download_button(
        label="📥 Download Task Template (CSV)",
        data=buffer,
        file_name="task_template.csv",
        mime="text/csv"
    )

# -----------------------------
# Load tasks from uploaded file
# -----------------------------
def load_tasks_from_file(uploaded_file):
    try:
        if uploaded_file.type == "text/plain":
            return [{"description": t.strip()} for t in uploaded_file.read().decode("utf-8").splitlines() if t.strip()]
        elif uploaded_file.type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            from docx import Document
            doc = Document(uploaded_file)
            return [{"description": p.text.strip()} for p in doc.paragraphs if p.text.strip()]
        elif uploaded_file.type in ["text/csv", "application/vnd.ms-excel"]:
            df = pd.read_csv(uploaded_file)
            if "Task Description" in df.columns:
                return [{"description": str(t).strip()} for t in df["Task Description"].dropna()]
            else:
                return [{"description": str(t).strip()} for t in df.iloc[:, 0].dropna()]
        elif uploaded_file.type in ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]:
            df = pd.read_excel(uploaded_file)
            if "Task Description" in df.columns:
                return [{"description": str(t).strip()} for t in df["Task Description"].dropna()]
            else:
                return [{"description": str(t).strip()} for t in df.iloc[:, 0].dropna()]
    except Exception as e:
        st.error(f"Error reading uploaded file: {e}")
        return []
    return []

# -----------------------------
# Streamlit Task Assistant
# -----------------------------
def Task_Assistant():
    st.title("📝 Huawei Task Assistant")
    st.markdown("Predict the most suitable employee(s) for each task using AI insights.")

    provide_template()

    # Task input
    example_tasks = """Prepare Monthly Sales Report
Deploy Cloud Infrastructure
Update Customer Database"""
    task_input = st.text_area(
        "✏️ Enter Task Description Here (one per line)",
        value=example_tasks,
        height=200
    )

    uploaded_file = st.file_uploader("📄 Or Upload Task List (CSV/DOCX/TXT/XLSX)", type=["txt", "docx", "csv", "xlsx"])

    # Prepare tasks list
    tasks = []
    if uploaded_file:
        tasks = load_tasks_from_file(uploaded_file)
    elif task_input.strip():
        tasks = [{"description": t.strip()} for t in task_input.splitlines() if t.strip()]

    if not tasks:
        st.warning("Please enter a task description or upload a file.")
        return

    # Match tasks and display candidates
    all_candidates = []
    for task_idx, task in enumerate(tasks):
        st.subheader(f"🔹 Task {task_idx+1}: {task['description']}")
        try:
            candidates = match_task_to_employees(es, task["description"], top_k=10, explain_top_n=3)
        except Exception as e:
            st.error(f"Failed to match employees for task: {e}")
            continue

        all_candidates.append(candidates)

        if not candidates:
            st.info("No suitable candidate found.")
            continue

        for rank, c in enumerate(candidates, start=1):
            st.write(f"{rank}. {c['employee_id']} — Score: {c['final_score']:.2f}")
            st.write("→", c.get("llm_explanation", "No explanation available"))

            # Feedback radio button
            key = f"feedback-{task_idx}-{c['employee_id']}"
            selected_feedback = st.radio(
                f"Feedback for {c['employee_id']} (Task {task_idx+1})",
                ("accepted", "rejected"),
                index=0,
                key=key
            )
            st.session_state[key] = selected_feedback

    # Submit all feedback
    if st.button("💾 Submit All Feedbacks"):
        batch_id = uuid.uuid4().hex[:8]
        for task_idx, task in enumerate(tasks):
            candidates = all_candidates[task_idx]
            for c in candidates:
                key = f"feedback-{task_idx}-{c['employee_id']}"
                feedback = st.session_state.get(key)
                if feedback:
                    try:
                        store_admin_feedback(
                            es,
                            c["employee_id"],
                            task_id=f"{batch_id}_{task_idx}_{uuid.uuid4().hex[:6]}",
                            task_text=task["description"],
                            prediction_score=c["final_score"],
                            feedback=feedback
                        )
                    except Exception as e:
                        st.error(f"Failed to store feedback for {c['employee_id']}: {e}")
        st.success("✅ All feedback submitted successfully!")

# -----------------------------
# Run Streamlit app
# -----------------------------
if __name__ == "__main__":
    Task_Assistant()
