import streamlit as st
from DashboardH import dashboard_page
#from Cloud_AI_Assistant import chat_assistant_page
from Task_Assitant import Task_assitant
from LoginPage import login_page
from admin_panel import admin_panel
from password_page import set_password_page
#from DashboardHawelwehed.py import dashboard_page
import traceback

st.set_page_config(
    page_title="Huawei Task Assistant",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="assets/ha1.svg"
)

# --- Query param check for password reset ---
query_params = st.query_params
token = query_params.get("token")

if token:
    set_password_page(token)
    st.stop()  # Stop after showing set password page

# --- Login check ---
if not st.session_state.get("logged_in"):
    if not login_page():
        st.stop()

# --- Sidebar Branding + login status and logout button ---
with st.sidebar:
    st.image("assets/ha2.svg", width=50)
    st.markdown("### Huawei")
    st.markdown("Activity Monitoring", help="Activity Monitoring")

    if st.session_state.get("logged_in"):
        st.markdown(
            f"✅ Logged in as **{st.session_state.username}** ({st.session_state.role})"
        )
        if st.button("Logout"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    st.markdown("---")

# --- Navigation dictionary ---
pages = {
    "📊 Dashboard": {
        "func": dashboard_page,
        "desc": "View overall activity metrics and charts",
        "roles": ["admin", "user"]
    },
    "📋 Task Assigner": {
        "func": Task_assitant,
        "desc": "Assign and track tasks",
        "roles": ["admin"]
    },
    "🤖 Chat Assistant": {
        #"func": chat_assistant_page,
        "desc": "Interact with AI assistant for help and insights",
        "roles": ["admin", "user"]
    },
    "🔧 Admin Panel": {
        "func": admin_panel,
        "desc": "Manage users, roles, and permissions",
        "roles": ["admin"]
    }
}

# --- Role-based filtering ---
user_role = st.session_state.get("role")
available_pages = {
    name: data
    for name, data in pages.items()
    if user_role in data["roles"]
}

# --- Default page selection ---
if "selected_page" not in st.session_state:
    st.session_state.selected_page = (
        list(available_pages.keys())[0] if available_pages else None
    )

# --- Sidebar navigation ---
with st.sidebar:
    for page_name in available_pages.keys():
        if st.button(page_name, key=page_name):
            st.session_state.selected_page = page_name
    st.markdown("---")

# --- Show selected page ---
try:
    if st.session_state.selected_page in available_pages:
        available_pages[st.session_state.selected_page]["func"]()
    else:
        st.error("❌ You don't have access to this page.")
except Exception:
    st.error("An error occurred while loading the page.")
    st.text(traceback.format_exc())

# --- Footer ---
st.markdown("---")
st.caption("© Huawei Cloud Team | 2025")
