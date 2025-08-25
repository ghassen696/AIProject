import streamlit as st
from auth import get_all_users, update_user, delete_user, check_password_strength
from user_management import invite_user, send_invite_email
import re 

# Enhanced CSS styling for better visual appeal
def inject_css():
    st.markdown(
        """
        <style>
        /* Main container styling */
        .main-header {
            background: linear-gradient(90deg, #1a1a1a 0%, #2d2d2d 100%);
            padding: 1.5rem 1rem;
            border-radius: 12px;
            margin-bottom: 1.5rem;
            color: #ffffff;
            text-align: center;
            border: 1px solid #333333;
        }
        
        .main-header h1 {
            margin: 0;
            font-size: 2rem;
            font-weight: 700;
            color: #4a90e2;
        }
        
        .main-header p {
            margin: 0.5rem 0 0 0;
            font-size: 1rem;
            opacity: 0.9;
            color: #b0b0b0;
        }
        
        /* Card styling for user entries */
        .user-card {
            background: #1a1a1a;
            border: 1px solid #333333;
            border-radius: 8px;
            padding: 1rem;
            margin: 0.75rem 0;
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
            transition: all 0.3s ease;
        }
        
        .user-card:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.4);
            transform: translateY(-2px);
            border-color: #4a90e2;
        }
        
        /* User info styling */
        .user-info {
            display: flex;
            align-items: center;
            gap: 1rem;
        }
        
        .user-avatar {
            width: 45px;
            height: 45px;
            border-radius: 50%;
            background: linear-gradient(135deg, #4a90e2 0%, #357abd 100%);
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
            font-size: 1.1rem;
        }
        
        .user-details h3 {
            margin: 0 0 0.5rem 0;
            color: #ffffff;
            font-size: 1.2rem;
        }
        
        .user-details p {
            margin: 0.25rem 0;
            color: #b0b0b0;
            font-size: 0.85rem;
        }
        
        .status-badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }
        
        .status-pending {
            background-color: #2d2d2d;
            color: #ffd700;
            border: 1px solid #4a4a4a;
        }
        
        .status-active {
            background-color: #1a2e1a;
            color: #4ade80;
            border: 1px solid #2d4a2d;
        }
        
        .status-inactive {
            background-color: #2e1a1a;
            color: #f87171;
            border: 1px solid #4a2d2d;
        }
        
        /* Button styling */
        .action-button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 0.75rem 1.5rem;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            width: 100%;
            margin: 0.25rem 0;
        }
        
        .action-button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
        }
        
        .edit-button {
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        }
        
        .delete-button {
            background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%);
            color: #721c24;
        }
        
        .danger-button {
            background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
            color: white;
        }
        
        .success-button {
            background: linear-gradient(135deg, #00b894 0%, #00cec9 100%);
            color: white;
        }
        
        /* Form styling */
        .form-container {
            background: #1a1a1a;
            border: 10px solid #333333;
            border-radius: 8px;
            padding: 1rem;
            margin: 1.5rem 0;
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        }
        
        .form-title {
            color: #4a90e2;
            font-size: 1.4rem;
            margin-bottom: 0.2rem;
            text-align: center;
            font-weight: 600;
        }
        
        /* Pagination styling */
        .pagination-container {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 1rem;
            margin: 2rem 0;
        }
        
        .page-info {
            background: #2d2d2d;
            padding: 0.5rem 1rem;
            border-radius: 20px;
            color: #b0b0b0;
            font-weight: 600;
            border: 1px solid #333333;
        }
        
        /* Stats cards */
        .stats-container {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 0.75rem;
            margin: 1.5rem 0;
        }
        
        .stat-card {
            background: #1a1a1a;
            border: 1px solid #333333;
            border-radius: 8px;
            padding: 1rem;
            text-align: center;
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
            transition: all 0.3s ease;
        }
        
        .stat-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.4);
            border-color: #4a90e2;
        }
        
        .stat-number {
            font-size: 1.5rem;
            font-weight: bold;
            color: #4a90e2;
            margin-bottom: 0.25rem;
        }
        
        .stat-label {
            color: #b0b0b0;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 500;
        }
        
        /* Responsive design */
        @media (max-width: 768px) {
            .user-info {
                flex-direction: column;
                text-align: center;
            }
            
            .stats-container {
                grid-template-columns: 1fr;
            }
        }
        
        /* Custom Streamlit overrides */
        .stButton > button {
            border: none;
            border-radius: 8px;
            font-weight: 600;
            transition: all 0.3s ease;
        }
        
        .stExpander {
            border: 1px solid #333333;
            border-radius: 8px;
            margin: 1rem 0;
        }
        
        .stExpander > div > div > div {
            background-color: #2d2d2d;
            border-radius: 8px;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

def render_user_stats(users):
    """Render statistics cards for user overview"""
    total_users = len(users)
    active_users = len([u for u in users if u.get('status', '').lower() == 'active'])
    pending_users = len([u for u in users if u.get('status', '').lower() == 'pending'])
    admin_users = len([u for u in users if u.get('role') == 'admin'])
    
    st.markdown("""
    <div class="stats-container">
        <div class="stat-card">
            <div class="stat-number">{}</div>
            <div class="stat-label">Total Users</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">{}</div>
            <div class="stat-label">Active Users</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">{}</div>
            <div class="stat-label">Pending Users</div>
        </div>
        <div class="stat-card">
            <div class="stat-number">{}</div>
            <div class="stat-label">Admin Users</div>
        </div>
    </div>
    """.format(total_users, active_users, pending_users, admin_users), unsafe_allow_html=True)

def render_user_card(user, start_idx):
    """Render individual user card with enhanced styling"""
    status = user.get('status', '').strip().lower()
    status_class = f"status-{status}" if status in ['pending', 'active', 'inactive'] else "status-inactive"
    status_display = status.capitalize() if status else "Inactive"
    
    # Get first letter of username for avatar
    avatar_letter = user['username'][0].upper() if user['username'] else "U"
    
    # Determine role color
    role_color = "#e74c3c" if user['role'] == 'admin' else "#3498db"
    
    st.markdown(f"""
    <div class="user-card">
        <div class="user-info">
            <div class="user-avatar" style="background: {role_color}">
                {avatar_letter}
            </div>
            <div class="user-details">
                <h3>{user['username']}</h3>
                <p><strong>Role:</strong> <span style="color: {role_color}; font-weight: 600;">{user['role'].title()}</span></p>
                <p><strong>Email:</strong> {user.get('email', 'N/A')}</p>
                <p><strong>Employee ID:</strong> {user.get('employee_id', 'N/A')}</p>
                <span class="status-badge {status_class}">{status_display}</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Action buttons in columns
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("✏️ Edit", key=f"edit_{user.get('_id', user['username'])}_{start_idx}", 
                     help=f"Edit user {user['username']}"):
            st.session_state.editing_user = user['username']
    
    with col2:
        if user['username'] == 'admin':
            st.button("🗑️ Delete", key=f"del_disabled_{user['_id']}_{start_idx}", 
                     disabled=True, help="Admin user cannot be deleted")
        else:
            confirm_key = f"confirm_delete_{user['username']}_{start_idx}"
            if st.session_state.get(confirm_key, False):
                if st.button("⚠️ Confirm", key=f"confirm_btn_{user['_id']}_{start_idx}", 
                           help="Click to confirm deletion"):
                    delete_user(user['username'])
                    st.success(f"✅ User {user['username']} deleted successfully!")
                    st.session_state[confirm_key] = False
                    st.rerun()
                if st.button("❌ Cancel", key=f"cancel_btn_{user['_id']}_{start_idx}"):
                    st.session_state[confirm_key] = False
                    st.rerun()
            else:
                if st.button("🗑️ Delete", key=f"delete_{user['_id']}_{start_idx}", 
                           help=f"Delete user {user['username']}"):
                    st.session_state[confirm_key] = True
                    st.rerun()
    
    with col3:
        if user.get('status', '').lower() == 'pending':
            if st.button("✅ Activate", key=f"activate_{user['_id']}_{start_idx}", 
                        help=f"Activate user {user['username']}"):
                # Add activation logic here
                st.success(f"✅ User {user['username']} activated!")
                st.rerun()
        else:
            st.button("ℹ️ Active", key=f"active_{user['_id']}_{start_idx}", 
                     disabled=True, help="User is already active")
    
    # Render edit form if this user is being edited
    if st.session_state.get("editing_user") == user['username']:
        render_edit_form(user, start_idx)

def render_edit_form(user, start_idx):
    """Render enhanced edit form for user modification"""
    with st.expander(f"✏️ Edit User: {user['username']}", expanded=True):
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔐 Password Settings")
            new_password = st.text_input(
                "New Password",
                type="password",
                key=f"pw_{user['_id']}_{start_idx}",
                help="Leave blank to keep current password"
            )
            
            if new_password:
                password_strength = check_password_strength(new_password)
                if password_strength:
                    st.error("❌ Password issues:")
                    for error in password_strength:
                        st.error(f"• {error}")
                else:
                    st.success("✅ Password meets security requirements")
        
        with col2:
            st.subheader("👤 User Settings")
            new_role = st.selectbox(
                "Role",
                options=["admin", "user"],
                index=["admin", "user"].index(user['role']),
                key=f"role_{user['_id']}_{start_idx}",
                help="Select user role"
            )
            
            if user['username'] == 'admin':
                st.info("🔒 Admin user role cannot be changed")
                new_role = 'admin'
        
        # Employee ID selection
        st.subheader("🆔 Employee Assignment")
        if user['username'] == 'admin':
            st.info("🔒 Admin user employee ID cannot be changed")
            new_employee_id = user.get('employee_id', 'N/A')
        else:
            if "employee_ids" not in st.session_state:
                try:
                    from PythonStreamlit.DashboardH2 import get_unique_terms
                    st.session_state["employee_ids"] = get_unique_terms("employee_activity", "employee_id")
                except:
                    st.session_state["employee_ids"] = []

            employee_ids = st.session_state.get("employee_ids", [])
            if employee_ids:
                new_employee_id = st.selectbox(
                    "Employee ID",
                    options=[""] + employee_ids,
                    index=employee_ids.index(user.get('employee_id', '')) + 1 if user.get('employee_id', '') in employee_ids else 0,
                    key=f"empid_{user['_id']}_{start_idx}",
                    help="Select employee ID to associate with this user"
                )
            else:
                st.warning("⚠️ No employee IDs available in the system")
                new_employee_id = user.get('employee_id', '')
        
        st.markdown("---")
        
        # Action buttons
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("✅ Save Changes", key=f"save_{user['_id']}_{start_idx}", 
                        help="Save all changes"):
                if new_password and password_strength:
                    st.error("Please fix password issues before saving")
                    return
                
                success = update_user(
                    user['username'],
                    new_password if new_password else None,
                    new_role,
                    new_employee_id
                )
                
                if success:
                    st.success(f"✅ User {user['username']} updated successfully!")
                    del st.session_state.editing_user
                    st.rerun()
                else:
                    st.error("❌ Failed to update user. Please try again.")
        
        with col2:
            if st.button("❌ Cancel", key=f"cancel_{user['_id']}_{start_idx}", 
                        help="Cancel editing"):
                del st.session_state.editing_user
                st.rerun()
        
        with col3:
            if st.button("🔄 Reset", key=f"reset_{user['_id']}_{start_idx}", 
                        help="Reset form to original values"):
                st.rerun()

def render_invite_form():
    st.subheader("➕ Invite New User")
    st.markdown("Invite users to create an account via email")
    """Render enhanced invite new user form"""

    
    with st.form("invite_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            invite_email = st.text_input(
                "📧 Email Address",
                placeholder="user@company.com",
                help="Enter the email address for the new user"
            )
            
            role = st.selectbox(
                "👤 Role",
                options=["user", "admin"],
                index=0,
                help="Select the role for the new user"
            )
        
        with col2:
            employee_ids = st.session_state.get("employee_ids", [])
            if employee_ids:
                employee_id = st.selectbox(
                    "🆔 Employee ID",
                    options=[""] + employee_ids,
                    help="Select employee ID to associate with this user"
                )
            else:
                st.info("ℹ️ No employee IDs available in the system")
                employee_id = None
            
            # Auto-generate username from email
            if invite_email:
                username = invite_email.split("@")[0]
                st.info(f"👤 Username will be: **{username}**")
        
        st.markdown("---")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            submit_invite = st.form_submit_button(
                "📤 Send Invitation",
                help="Send invitation email to the new user",
                use_container_width=True
            )
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Handle form submission
    if submit_invite:
        if not invite_email:
            st.error("❌ Please provide an email address")
        elif not employee_id:
            st.error("❌ Please select an employee ID")
        else:
            username = invite_email.split("@")[0]
            token = invite_user(invite_email, employee_id, role, username)
            
            if token:
                send_invite_email(invite_email, token)
                st.success(f"✅ Invitation sent successfully to {invite_email}")
                st.balloons()
                st.rerun()
            else:
                st.error("❌ Failed to send invitation. Please try again.")

def admin_panel():
    """Main admin panel function with enhanced layout"""
    inject_css()
    
    # Enhanced header
    st.markdown("""
    <div class="main-header">
        <h1>🔧 Admin Panel</h1>
    </div>
    """, unsafe_allow_html=True)
    
    # Get users data
    users = get_all_users()
    
    # Display statistics
    render_user_stats(users)
    
    # Main content tabs
    tab1, tab2 = st.tabs(["👥 User Management", "➕ Invite Users"])
    
    with tab1:
        st.subheader("👥 User Management")
        st.markdown("Manage existing users, roles, and permissions")
        
        # Pagination controls
        users_per_page = 5
        total_users = len(users)
        total_pages = (total_users + users_per_page - 1) // users_per_page

        if "current_page" not in st.session_state:
            st.session_state.current_page = 0

        start_idx = st.session_state.current_page * users_per_page
        end_idx = min(start_idx + users_per_page, total_users)
        current_users = users[start_idx:end_idx]

        # Display current users
        for user in current_users:
            render_user_card(user, start_idx)
        
        # Enhanced pagination
        if total_pages > 1:
            st.markdown("---")
            col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
            
            with col1:
                if st.button("⬅️ Previous", key="prev_page", 
                           disabled=(st.session_state.current_page == 0)):
                    st.session_state.current_page -= 1
                    st.rerun()
            
            with col2:
                if st.button("➡️ Next", key="next_page", 
                           disabled=(st.session_state.current_page >= total_pages - 1)):
                    st.session_state.current_page += 1
                    st.rerun()
            
            with col3:
                st.markdown(f"""
                <div class="page-info">
                    📄 Page {st.session_state.current_page + 1} of {total_pages}
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                if st.button("🏠 First", key="first_page", 
                           disabled=(st.session_state.current_page == 0)):
                    st.session_state.current_page = 0
                    st.rerun()
            
            with col5:
                if st.button("🏁 Last", key="last_page", 
                           disabled=(st.session_state.current_page >= total_pages - 1)):
                    st.session_state.current_page = total_pages - 1
                    st.rerun()
    
    with tab2:
        render_invite_form()
    
    
if __name__ == "__main__":
    admin_panel()