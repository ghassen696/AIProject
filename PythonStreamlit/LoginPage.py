import streamlit as st
from auth import get_user, check_password

def login_page():
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    if st.session_state.logged_in:
        return True  

    #st.set_page_config(page_title="Login", layout="centered")
    st.title("🔐 Huawei Cloud Team Login")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        user = get_user(username)
        if user and check_password(password, user["password_hash"]):
            st.session_state.logged_in = True
            st.session_state.username = username
            st.session_state.role = user.get("role", "cloud_user")
            st.session_state.employee_id = user.get("employee_id")  # Employee ID for filtering data
            st.success("✅ Login successful!")
            st.rerun()
        else:
            st.error("❌ Invalid credentials")

    return False
