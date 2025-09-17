import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
from rag_pipeline.retriever import retrieve_and_answer

st.set_page_config(page_title="Huawei Cloud AI Assistant", layout="wide")

st.title("🤖 Huawei Cloud AI Assistant")
st.markdown("""
Ask me anything about Huawei Cloud services, deployments, logs, and best practices.
I will fetch real-time answers from your knowledge base!
""")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

user_query = st.chat_input("Ask your question...")

if user_query:
    st.session_state.chat_history.append({"role": "user", "content": user_query})
    with st.spinner("🔍 Thinking..."):
        try:
            llm_response = retrieve_and_answer(user_query)
        except Exception as e:
            llm_response = f"⚠️ Error: {e}"
    st.session_state.chat_history.append({"role": "assistant", "content": llm_response})

# Display chat history
for message in st.session_state.chat_history:
    st.chat_message(message["role"]).markdown(message["content"])
