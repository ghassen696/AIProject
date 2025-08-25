import streamlit as st


def Task_assitant():
    
    st.set_page_config(page_title="Huawei Task Assistant", layout="wide")

    st.title("📝 Predict the Most Suitable Assignee")
    st.markdown("This tool helps predict task assignments.")

    st.markdown("Enter a task description or upload a task document to get assignment suggestions from the AI.")

    st.markdown("---")
    st.markdown('<style>div.block-container{padding-top:2rem;}</style>',unsafe_allow_html=True)

    task_input = st.text_area("✏️ Enter Task Description Here")

    uploaded_file = st.file_uploader("📄 Or Upload a Task List (TXT/DOCX/CSV)", type=["txt", "docx", "csv"])

    if st.button("🔍 Predict Suitable Assignee(s)"):
        with st.spinner("Analyzing tasks with AI..."):
                # TODO: Call your LLM function here with task_input or uploaded_file
                # Example dummy response
                st.success("Prediction Complete ✅")
                st.write("🔹 Task: *Prepare Sales Report* — Recommended: **John Doe**")
                st.write("🔹 Task: *Deploy Cloud Infrastructure* — Recommended: **Alice Smith**")
        
    # ----------------------
"""
import streamlit as st
from datetime import date
from LLM_backend.classifier import summarize_all_logs, classify_tasks
from auth import get_employee_logs
import pandas as pd


# -------------------- Workload Document --------------------
def load_workload_document(uploaded_file):
    if uploaded_file.type == "text/plain":
        return uploaded_file.read().decode("utf-8")
    elif uploaded_file.type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        from docx import Document
        doc = Document(uploaded_file)
        return "\n".join([p.text for p in doc.paragraphs])
    elif uploaded_file.type == "text/csv":
        df = pd.read_csv(uploaded_file)
        return "\n".join(df.iloc[:,0].astype(str).tolist())
    return ""
def Task_assitant():

    # -------------------- Streamlit Layout --------------------
    st.set_page_config(page_title="Huawei Task Assistant", layout="wide")
    st.title("📝 Huawei Task Assistant")
    st.markdown("Analyze employee activity logs and classify tasks in real-time.")

    employee_id = st.text_input("Employee ID")
    log_date = st.date_input("Select Date", value=date.today())
    uploaded_file = st.file_uploader("Upload Workload Document (TXT/DOCX/CSV)", type=["txt","docx","csv"])

    if st.button("🚀 Analyze Logs"):
        if not employee_id or not uploaded_file:
            st.warning("Please provide both Employee ID and Workload Document.")
        else:
            workload_doc = load_workload_document(uploaded_file)
            logs = get_employee_logs(employee_id, log_date.strftime("%Y-%m-%d"))
            
            if not logs:
                st.info("No logs found for this employee on the selected date.")
            else:
                st.info("Processing logs in real-time... ⏳")
                summary_placeholder = st.empty()
                progress_bar = st.progress(0)
                total_chunks = len(logs)//50 + 1
                chunk_count = 0
                final_summaries = []

                # ------------------ Summarize Logs in Stream ------------------
                for update in summarize_all_logs(logs):
                    if update["type"] == "chunk_update":
                        final_summaries = update["summaries"]
                        summary_placeholder.json(final_summaries)
                        chunk_count += 1
                        progress_bar.progress(min(chunk_count/total_chunks, 1.0))
                    elif update["type"] == "daily_summary":
                        st.subheader("📝 Daily Summary")
                        st.json(update["daily_summary"])
                        st.success("✅ Analysis Complete!")

                # ------------------ Classify Tasks ------------------
                if final_summaries:
                    st.subheader("🔹 Classified Tasks")
                    classified_tasks = classify_tasks(final_summaries, workload_doc)
                    st.json(classified_tasks)
"""