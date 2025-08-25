import streamlit as st
import time
import base64
from auth import get_user_doc_by_username, update_user, check_password_strength, hash_password, is_token_valid
from elasticsearch import Elasticsearch
from config import ELASTICSEARCH_URL

es = Elasticsearch(ELASTICSEARCH_URL)
def set_password_page(token):
    st.title("🔐 Set Your Password")

    try:
        res = es.search(index="users", body={"query": {"term": {"token.keyword": token}}})
        hits = res.get("hits", {}).get("hits", [])
        if not hits:
            st.error("Invalid or expired token.")
            return

        user_doc = hits[0]
        user_id = user_doc["_id"]
        user_data = user_doc["_source"]

        email = st.text_input("📧 Confirm your email", placeholder="user@example.com")
        password = st.text_input("🔐 New password", type="password")
        confirm_password = st.text_input("🔐 Confirm password", type="password")

        if st.button("✅ Set Password"):
            if not email or not password or not confirm_password:
                st.warning("All fields are required.")
                return

            if email.lower() != user_data.get("email", "").lower():
                st.error("Email does not match the invitation.")
                return

            if password != confirm_password:
                st.error("Passwords do not match.")
                return

            errors = check_password_strength(password)
            if errors:
                for err in errors:
                    st.warning(err)
                return

            es.update(index="users", id=user_id, body={
                "doc": {
                    "password_hash": hash_password(password),
                    "status": "active",
                    "token": None
                }
            })

            st.success("✅ Password set! Redirecting to login...")
            time.sleep(2)
            st.switch_page("main.py")  # Optional
    except Exception as e:
        st.error("Something went wrong.")
