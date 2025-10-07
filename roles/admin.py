"""
Admin Role Interface - System Administration & Management
=========================================================

Complete administrative control panel with:
- User Management
- Master Trade Mapping Management
- Database Backup/Restore
- System Settings
- Audit Trail Viewing
"""

import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import logging
import sqlite3
import os
import shutil
import json
from pathlib import Path
from typing import Dict, List, Optional
import hashlib
from typing import Tuple

try:
    from database.setup import DatabaseManager
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False

try:
    from core.data_processor import load_master_trade_mapping
    TRADE_MAPPING_AVAILABLE = True
except ImportError:
    TRADE_MAPPING_AVAILABLE = False

logger = logging.getLogger(__name__)


class AdminInterface:
    """Administrative interface for system management"""
    
    def __init__(self, db_path: str = "building_inspection.db", user_info: dict = None):
        self.user_info = user_info or {}
        self.db_path = db_path
        self.db = DatabaseManager(db_path) if DATABASE_AVAILABLE else None
        
        # Session state initialization
        if 'admin_active_tab' not in st.session_state:
            st.session_state.admin_active_tab = 'users'
        if 'admin_selected_user' not in st.session_state:
            st.session_state.admin_selected_user = None
        if 'admin_edit_mode' not in st.session_state:
            st.session_state.admin_edit_mode = False
    
    def show(self):
        """Main admin dashboard"""
        
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    color: white; padding: 1.5rem; border-radius: 10px; margin-bottom: 1rem;">
            <h2 style="margin: 0;">System Administration</h2>
            <p style="margin: 0.5rem 0 0 0;">Administrator: {self.user_info.get('name', 'Admin')}</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Navigation tabs
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "👥 User Management",
            "🗺️ Trade Mapping",
            "💾 Database",
            "🗑️ Cleanup",
            "⚙️ Settings",
            "📋 Audit Trail"
        ])
        
        with tab1:
            self._show_user_management()
        
        with tab2:
            self._show_trade_mapping_management()
        
        with tab3:
            self._show_database_management()
        
        with tab4:
            self._show_database_cleanup()

        with tab5:
            self._show_system_settings()

        with tab6:
            self._show_audit_trail()
    
    # ========================================================================
    # USER MANAGEMENT
    # ========================================================================
    
    def _show_user_management(self):
        """User management interface"""
        
        st.markdown("### User Management")
        
        # Initialize show_inactive state
        if 'show_inactive_users' not in st.session_state:
            st.session_state.show_inactive_users = False
        
        # Action buttons
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("➕ Add New User", use_container_width=True, type="primary"):
                st.session_state.admin_edit_mode = 'create'
                st.session_state.admin_selected_user = None
                st.rerun()
        
        with col2:
            if st.button("🔄 Refresh", use_container_width=True):
                st.rerun()
        
        with col3:
            show_inactive = st.checkbox(
                "Show Inactive Users", 
                value=st.session_state.show_inactive_users,
                key="show_inactive_checkbox"
            )
            if show_inactive != st.session_state.show_inactive_users:
                st.session_state.show_inactive_users = show_inactive
                st.rerun()
        
        with col4:
            if st.button("📊 User Statistics", use_container_width=True):
                self._show_user_statistics()
        
        st.markdown("---")
        
        # Show form if in edit mode
        if st.session_state.admin_edit_mode:
            self._show_user_form()
            st.markdown("---")
        
        # Load and display users
        try:
            users_df = self._load_users(st.session_state.show_inactive_users)
            
            if users_df.empty:
                st.info("No users found")
                return
            
            st.markdown("#### Active Users" if not st.session_state.show_inactive_users else "#### All Users")
            
            # Filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                roles = ['All'] + sorted(users_df['role'].unique().tolist())
                role_filter = st.selectbox("Filter by Role:", roles)
            
            with col2:
                search_term = st.text_input("Search:", placeholder="Username, name, or email")
            
            with col3:
                sort_by = st.selectbox("Sort by:", ["Username", "Role", "Created Date", "Last Login"])
            
            # Apply filters
            filtered_users = users_df.copy()
            
            if role_filter != 'All':
                filtered_users = filtered_users[filtered_users['role'] == role_filter]
            
            if search_term:
                mask = (
                    filtered_users['username'].str.contains(search_term, case=False, na=False) |
                    filtered_users['name'].str.contains(search_term, case=False, na=False) |
                    filtered_users['email'].str.contains(search_term, case=False, na=False)
                )
                filtered_users = filtered_users[mask]
            
            st.caption(f"Showing {len(filtered_users)} of {len(users_df)} users")
            
            # Display user cards
            for idx, (_, user) in enumerate(filtered_users.iterrows()):
                self._render_user_card(user, idx)
                
        except Exception as e:
            st.error(f"Error loading users: {e}")
            logger.error(f"User management error: {e}")
    
    def _load_users(self, show_inactive: bool = False) -> pd.DataFrame:
        """Load users from database"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = """
                SELECT 
                    u.id,
                    u.username,
                    u.email,
                    u.role,
                    u.is_active,
                    u.created_at,
                    u.last_login,
                    COALESCE(u.first_name || ' ' || u.last_name, u.username) as name,
                    COALESCE(p.company, '') as company,
                    COALESCE(p.job_title, '') as job_title,
                    COALESCE(p.phone, '') as phone
                FROM users u
                LEFT JOIN user_profiles p ON u.id = p.user_id
            """
            
            if not show_inactive:
                query += " WHERE u.is_active = 1"
            
            query += " ORDER BY u.created_at DESC"
            
            users_df = pd.read_sql_query(query, conn)
            conn.close()
            
            return users_df
            
        except Exception as e:
            logger.error(f"Error loading users: {e}")
            return pd.DataFrame()
    
    def _render_user_card(self, user, idx):
        """Render individual user card"""
        
        user_id = user['id']
        is_active = user['is_active'] == 1
        
        # Role color coding
        role_colors = {
            'admin': '#667eea',
            'inspector': '#4CAF50',
            'builder': '#FF9800',
            'developer': '#1e3c72',
            'owner': '#2196F3'
        }
        
        role_color = role_colors.get(user['role'], '#607D8B')
        
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
            
            with col1:
                status_icon = "✅" if is_active else "❌"
                st.markdown(f"{status_icon} **{user['username']}** - {user['name']}")
                st.caption(f"📧 {user['email']}")
            
            with col2:
                st.markdown(f"""
                <div style="background-color: {role_color}; color: white; padding: 0.3rem 0.6rem; 
                            border-radius: 5px; text-align: center; font-size: 0.85em;">
                    {user['role'].upper()}
                </div>
                """, unsafe_allow_html=True)
                
                if user['company']:
                    st.caption(f"🏢 {user['company']}")
            
            with col3:
                if pd.notna(user.get('last_login')):
                    try:
                        last_login = pd.to_datetime(user['last_login'])
                        st.caption(f"🕐 Last login: {last_login.strftime('%Y-%m-%d')}")
                    except:
                        st.caption("🕐 Last login: Never")
                else:
                    st.caption("🕐 Last login: Never")
            
            with col4:
                if st.button("Edit", key=f"edit_user_{user_id}_{idx}", use_container_width=True):
                    st.session_state.admin_edit_mode = 'edit'
                    st.session_state.admin_selected_user = user_id
                    st.rerun()
        
        st.divider()
    
    def _show_user_form(self):
        """Show user create/edit form"""
        
        mode = st.session_state.admin_edit_mode
        
        # Get user_id early for use in keys
        if mode == 'edit':
            user_id = st.session_state.admin_selected_user
        else:
            user_id = 'new'
        
        st.markdown("---")
        
        if mode == 'create':
            st.markdown("### ➕ Create New User")
            user_data = {}
        else:
            st.markdown("### ✏️ Edit User")
            
            # Load existing user data
            if st.session_state.admin_selected_user:
                user_data = self._load_user_details(st.session_state.admin_selected_user)
                
                if not user_data:
                    st.error(f"Could not load user with ID: {st.session_state.admin_selected_user}")
                    
                    if st.button("Back to User List"):
                        st.session_state.admin_edit_mode = False
                        st.session_state.admin_selected_user = None
                        st.rerun()
                    return
                
                st.info(f"Editing user: {user_data.get('username', 'Unknown')}")
            else:
                st.error("No user selected for editing")
                
                if st.button("Back to User List"):
                    st.session_state.admin_edit_mode = False
                    st.rerun()
                return
        
        with st.form("user_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Account Information**")
                
                username = st.text_input(
                    "Username*",
                    value=user_data.get('username', ''),
                    disabled=(mode == 'edit'),
                    help="Username cannot be changed after creation"
                )
                
                email = st.text_input(
                    "Email*",
                    value=user_data.get('email', '')
                )
                
                role = st.selectbox(
                    "Role*",
                    options=['inspector', 'developer', 'builder', 'owner','admin'],  # Removed 'owner'
                    index=['inspector', 'developer', 'builder', 'owner', 'admin'].index(
                        user_data.get('role', 'inspector')
                    ) if user_data.get('role') else 0
                )
                
                is_active = st.checkbox(
                    "Active User",
                    value=user_data.get('is_active', True)
                )
                
                # PASSWORD SECTION - SIMPLE AND CLEAR
                st.markdown("---")
                
                if mode == 'create':
                    st.markdown("**Password** (Required)")
                    password = st.text_input("Password*", type="password", key=f"pwd_{user_id}")
                    confirm_password = st.text_input("Confirm Password*", type="password", key=f"pwd_confirm_{user_id}")
                else:
                    st.markdown("**Change Password** (Optional)")
                    st.caption("Leave blank to keep current password")
                    password = st.text_input(
                        "New Password", 
                        type="password", 
                        key=f"new_pwd_{user_id}",
                        placeholder="Enter new password or leave blank"
                    )
                    confirm_password = st.text_input(
                        "Confirm New Password", 
                        type="password", 
                        key=f"confirm_pwd_{user_id}",
                        placeholder="Confirm new password"
                    )
            
            with col2:
                st.markdown("**Profile Information**")
                
                first_name = st.text_input(
                    "First Name",
                    value=user_data.get('first_name', '')
                )
                
                last_name = st.text_input(
                    "Last Name",
                    value=user_data.get('last_name', '')
                )
                
                company = st.text_input(
                    "Company",
                    value=user_data.get('company', '')
                )
                
                job_title = st.text_input(
                    "Job Title",
                    value=user_data.get('job_title', '')
                )
                
                phone = st.text_input(
                    "Phone",
                    value=user_data.get('phone', '')
                )
            
            st.markdown("---")
            
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                submit = st.form_submit_button(
                    "Create User" if mode == 'create' else "Update User",
                    type="primary",
                    use_container_width=True
                )
            
            with col3:
                cancel = st.form_submit_button("Cancel", use_container_width=True)
            
            if submit:
                # Validation
                errors = []
                
                if not username:
                    errors.append("Username is required")
                if not email:
                    errors.append("Email is required")
                
                # Password validation for create mode
                if mode == 'create':
                    if not password:
                        errors.append("Password is required for new users")
                    elif len(password) < 6:
                        errors.append("Password must be at least 6 characters")
                    elif password != confirm_password:
                        errors.append("Passwords do not match")
                
                # Password validation for edit mode (only if they entered something)
                if mode == 'edit' and password:
                    if len(password) < 6:
                        errors.append("New password must be at least 6 characters")
                    elif password != confirm_password:
                        errors.append("Passwords do not match")
                
                # If editing and no password entered, set to None (keep existing)
                if mode == 'edit' and not password:
                    password = None
                
                if errors:
                    for error in errors:
                        st.error(error)
                else:
                    # Save user
                    try:
                        success = self._save_user(
                            mode=mode,
                            user_id=st.session_state.admin_selected_user if mode == 'edit' else None,
                            username=username,
                            email=email,
                            password=password,
                            role=role,
                            is_active=is_active,
                            first_name=first_name,
                            last_name=last_name,
                            company=company,
                            job_title=job_title,
                            phone=phone
                        )
                        
                        if success:
                            st.success(f"User {'created' if mode == 'create' else 'updated'} successfully!")
                            st.session_state.admin_edit_mode = False
                            st.session_state.admin_selected_user = None
                            import time
                            time.sleep(1)
                            st.rerun()
                    except Exception as e:
                        st.error(f"Unexpected error: {str(e)}")
                        logger.error(f"Form submission error: {e}")
            
            if cancel:
                st.session_state.admin_edit_mode = False
                st.session_state.admin_selected_user = None
                st.rerun()
    
    def _load_user_details(self, user_id: int) -> dict:
        """Load detailed user information"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get user data from users table (first_name, last_name are here)
            cursor.execute("""
                SELECT u.username, u.email, u.role, u.is_active,
                    u.first_name, u.last_name,
                    COALESCE(p.company, '') as company, 
                    COALESCE(p.job_title, '') as job_title, 
                    COALESCE(p.phone, '') as phone
                FROM users u
                LEFT JOIN user_profiles p ON u.id = p.user_id
                WHERE u.id = ?
            """, (user_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'username': result[0],
                    'email': result[1],
                    'role': result[2],
                    'is_active': result[3] == 1,
                    'first_name': result[4] or '',
                    'last_name': result[5] or '',
                    'company': result[6] or '',
                    'job_title': result[7] or '',
                    'phone': result[8] or ''
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error loading user details: {e}")
            st.error(f"Error loading user details: {e}")
            return {}
    
    def _save_user(self, mode: str, user_id: Optional[int], username: str, email: str,
               password: Optional[str], role: str, is_active: bool, first_name: str,
               last_name: str, company: str, job_title: str, phone: str) -> bool:
        """Save user to database"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if mode == 'create':
                # Check if username already exists
                cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
                if cursor.fetchone():
                    st.error(f"Username '{username}' already exists")
                    conn.close()
                    return False
                
                # Generate salt and hash password
                import secrets
                salt = secrets.token_hex(16)
                password_hash = hashlib.sha256((password + salt).encode()).hexdigest()
                
                # Provide default values for first_name and last_name if empty
                fn = first_name if first_name else username
                ln = last_name if last_name else ""
                
                # Insert user with first_name and last_name
                cursor.execute("""
                    INSERT INTO users (username, email, password_hash, salt, role, is_active, 
                                    first_name, last_name, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (username, email, password_hash, salt, role, 1 if is_active else 0, 
                    fn, ln, datetime.now().isoformat()))
                
                new_user_id = cursor.lastrowid
                
                # Check if user_profiles table exists and insert additional profile data
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='user_profiles'
                """)
                
                if cursor.fetchone():
                    # user_profiles only has: user_id, company, job_title, phone
                    cursor.execute("""
                        INSERT INTO user_profiles (user_id, company, job_title, phone)
                        VALUES (?, ?, ?, ?)
                    """, (new_user_id, company, job_title, phone))
                else:
                    logger.warning("user_profiles table does not exist")
                
            else:  # edit mode
                # Check if user exists
                cursor.execute("SELECT salt FROM users WHERE id = ?", (user_id,))
                result = cursor.fetchone()
                
                if not result:
                    st.error(f"User ID {user_id} not found")
                    conn.close()
                    return False
                
                # Provide default values for first_name and last_name if empty
                fn = first_name if first_name else username
                ln = last_name if last_name else ""
                
                # Update user
                if password:
                    # Generate new salt for password change
                    import secrets
                    salt = secrets.token_hex(16)
                    password_hash = hashlib.sha256((password + salt).encode()).hexdigest()
                    
                    cursor.execute("""
                        UPDATE users 
                        SET email = ?, password_hash = ?, salt = ?, role = ?, is_active = ?,
                            first_name = ?, last_name = ?
                        WHERE id = ?
                    """, (email, password_hash, salt, role, 1 if is_active else 0, 
                        fn, ln, user_id))
                else:
                    cursor.execute("""
                        UPDATE users 
                        SET email = ?, role = ?, is_active = ?, first_name = ?, last_name = ?
                        WHERE id = ?
                    """, (email, role, 1 if is_active else 0, fn, ln, user_id))
                
                # Update user_profiles if it exists
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='user_profiles'
                """)
                
                if cursor.fetchone():
                    cursor.execute("SELECT user_id FROM user_profiles WHERE user_id = ?", (user_id,))
                    
                    if cursor.fetchone():
                        # user_profiles only has: company, job_title, phone (no names)
                        cursor.execute("""
                            UPDATE user_profiles 
                            SET company = ?, job_title = ?, phone = ?
                            WHERE user_id = ?
                        """, (company, job_title, phone, user_id))
                    else:
                        cursor.execute("""
                            INSERT INTO user_profiles (user_id, company, job_title, phone)
                            VALUES (?, ?, ?, ?)
                        """, (user_id, company, job_title, phone))
            
            conn.commit()
            conn.close()
            
            return True
            
        except sqlite3.IntegrityError as e:
            logger.error(f"Database integrity error: {e}")
            st.error(f"Database error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error saving user: {e}")
            st.error(f"Error details: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _show_user_statistics(self):
        """Show user statistics"""
        
        with st.expander("User Statistics", expanded=True):
            try:
                conn = sqlite3.connect(self.db_path)
                
                # Role distribution
                role_df = pd.read_sql_query("""
                    SELECT role, COUNT(*) as count
                    FROM users
                    WHERE is_active = 1
                    GROUP BY role
                    ORDER BY count DESC
                """, conn)
                
                # Activity stats
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
                active_users = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = 0")
                inactive_users = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE last_login >= date('now', '-7 days')
                """)
                recent_logins = cursor.fetchone()[0]
                
                conn.close()
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Active Users", active_users)
                with col2:
                    st.metric("Inactive Users", inactive_users)
                with col3:
                    st.metric("Logins (7 days)", recent_logins)
                
                st.markdown("**Users by Role:**")
                st.bar_chart(role_df.set_index('role')['count'])
                
            except Exception as e:
                st.error(f"Error loading statistics: {e}")
    
    # ========================================================================
    # TRADE MAPPING MANAGEMENT
    # ========================================================================
    
    def _show_trade_mapping_management(self):
        """Master trade mapping management"""
        
        st.markdown("### Master Trade Mapping Management")
        
        st.info("Manage the system-wide master trade mapping that all inspectors use by default")
        
        # Current master mapping status
        try:
            if TRADE_MAPPING_AVAILABLE:
                master_mapping = load_master_trade_mapping()
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Mappings", len(master_mapping))
                with col2:
                    st.metric("Unique Trades", master_mapping['Trade'].nunique())
                with col3:
                    st.metric("Unique Rooms", master_mapping['Room'].nunique())
                with col4:
                    st.metric("Unique Components", master_mapping['Component'].nunique())
                
                st.markdown("---")
                
                # Management actions
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Upload New Master Mapping**")
                    
                    uploaded_file = st.file_uploader(
                        "Upload new master mapping",
                        type=["csv"],
                        help="CSV file with Room, Component, Trade columns"
                    )
                    
                    if uploaded_file:
                        try:
                            new_mapping = pd.read_csv(uploaded_file)
                            
                            # Validate
                            required_cols = ['Room', 'Component', 'Trade']
                            missing = [col for col in required_cols if col not in new_mapping.columns]
                            
                            if missing:
                                st.error(f"Missing columns: {', '.join(missing)}")
                            else:
                                st.success(f"Valid mapping: {len(new_mapping)} entries")
                                
                                col_a, col_b = st.columns(2)
                                
                                with col_a:
                                    if st.button("Set as Master Mapping", type="primary", use_container_width=True):
                                        if self._save_master_mapping(new_mapping):
                                            st.success("Master mapping updated & synced to database!")
                                            st.rerun()
                                        else:
                                            st.error("Failed to update mapping")
                                
                                with col_b:
                                    if st.button("Validate First", use_container_width=True):
                                        self._validate_trade_mapping(new_mapping)
                        except Exception as e:
                            st.error(f"Error reading file: {e}")

                with col2:
                    st.markdown("**Download & Export**")
                    
                    csv_data = master_mapping.to_csv(index=False)
                    
                    st.download_button(
                        "Download Current Master",
                        data=csv_data,
                        file_name=f"master_trade_mapping_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                    
                    # Template download
                    template = """Room,Component,Trade
                Apartment Entry Door,Door Handle,Doors
                Apartment Entry Door,Door Locks and Keys,Doors
                Bathroom,Tiles,Flooring - Tiles
                Kitchen Area,Cabinets,Carpentry & Joinery
                Bedroom,Windows,Windows
                Living Room,Air Conditioning,HVAC"""
                    
                    st.download_button(
                        "Download Template",
                        data=template,
                        file_name="trade_mapping_template.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                    
                    st.caption("Auto-syncs to database when updated")
                
                st.markdown("---")
                
                # Display current mapping
                st.markdown("**Current Master Mapping:**")
                
                # Search and filter
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    search = st.text_input("Search:", placeholder="Room, component, or trade")
                with col2:
                    trade_filter = st.selectbox(
                        "Filter by Trade:",
                        ['All'] + sorted(master_mapping['Trade'].unique().tolist())
                    )
                with col3:
                    room_filter = st.selectbox(
                        "Filter by Room:",
                        ['All'] + sorted(master_mapping['Room'].unique().tolist())
                    )
                
                # Apply filters
                display_mapping = master_mapping.copy()
                
                if search:
                    mask = (
                        display_mapping['Room'].str.contains(search, case=False, na=False) |
                        display_mapping['Component'].str.contains(search, case=False, na=False) |
                        display_mapping['Trade'].str.contains(search, case=False, na=False)
                    )
                    display_mapping = display_mapping[mask]
                
                if trade_filter != 'All':
                    display_mapping = display_mapping[display_mapping['Trade'] == trade_filter]
                
                if room_filter != 'All':
                    display_mapping = display_mapping[display_mapping['Room'] == room_filter]
                
                st.caption(f"Showing {len(display_mapping)} of {len(master_mapping)} mappings")
                
                display_mapping.index = range(1, len(display_mapping) + 1)
                st.dataframe(display_mapping, use_container_width=True, height=400)
                
            else:
                st.error("Trade mapping module not available")
                
        except Exception as e:
            st.error(f"Error loading master mapping: {e}")
    
    def _save_master_mapping(self, mapping_df: pd.DataFrame) -> bool:
        """Save master trade mapping"""
        
        try:
            # Save to file
            mapping_df.to_csv("MasterTradeMapping.csv", index=False)
            
            # Save to database if available
            if self.db:
                self.db.save_trade_mapping(mapping_df)
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving master mapping: {e}")
            return False
    
    def _validate_trade_mapping(self, mapping_df: pd.DataFrame):
        """Validate trade mapping"""
        
        with st.expander("Validation Results", expanded=True):
            # Check for duplicates
            duplicates = mapping_df.duplicated(subset=['Room', 'Component']).sum()
            
            if duplicates > 0:
                st.warning(f"Found {duplicates} duplicate Room-Component combinations")
            else:
                st.success("No duplicate mappings found")
            
            # Check for empty values
            empty_rooms = mapping_df['Room'].isna().sum()
            empty_components = mapping_df['Component'].isna().sum()
            empty_trades = mapping_df['Trade'].isna().sum()
            
            if empty_rooms + empty_components + empty_trades > 0:
                st.warning(f"Found empty values: Rooms={empty_rooms}, Components={empty_components}, Trades={empty_trades}")
            else:
                st.success("No empty values found")
            
            # Trade distribution
            st.markdown("**Trade Distribution:**")
            trade_counts = mapping_df['Trade'].value_counts()
            st.bar_chart(trade_counts)
    
    # ========================================================================
    # DATABASE MANAGEMENT
    # ========================================================================
    
    def _show_database_management(self):
        """Database backup, restore, and maintenance"""
        
        st.markdown("### Database Management")
        
        # Database info
        db_path = Path(self.db_path)
        
        if db_path.exists():
            db_size = db_path.stat().st_size / (1024 * 1024)  # MB
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Database Size", f"{db_size:.2f} MB")
            with col2:
                st.metric("Database Path", str(db_path))
            with col3:
                st.metric("Status", "Connected")
        
        st.markdown("---")
        
        # Backup section
        st.markdown("#### Backup Operations")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Create Backup**")
            
            backup_name = st.text_input(
                "Backup Name (optional):",
                placeholder="Leave empty for auto-generated name"
            )
            
            include_files = st.checkbox("Include uploaded files", value=True)
            
            if st.button("Create Backup", type="primary", use_container_width=True):
                backup_path = self._create_backup(backup_name, include_files)
                if backup_path:
                    st.success(f"Backup created: {backup_path}")
                    
                    # Offer download
                    try:
                        with open(backup_path, 'rb') as f:
                            st.download_button(
                                "Download Backup",
                                data=f.read(),
                                file_name=os.path.basename(backup_path),
                                mime="application/octet-stream",
                                use_container_width=True
                            )
                    except Exception as e:
                        st.error(f"Error preparing download: {e}")
                else:
                    st.error("Backup failed")
        
        with col2:
            st.markdown("**Restore from Backup**")
            
            backup_file = st.file_uploader(
                "Select backup file to restore",
                type=["db", "sqlite", "backup"]
            )
            
            if backup_file:
                st.warning("⚠️ Restoring will overwrite current database!")
                
                confirm_restore = st.checkbox("I understand and want to proceed")
                
                if confirm_restore:
                    if st.button("Restore Database", type="primary", use_container_width=True):
                        if self._restore_backup(backup_file):
                            st.success("Database restored successfully!")
                            st.info("Please restart the application")
                        else:
                            st.error("Restore failed")
        
        st.markdown("---")
        
        # Maintenance section
        st.markdown("#### Database Maintenance")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Optimize Database", use_container_width=True):
                if self._optimize_database():
                    st.success("Database optimized")
                else:
                    st.error("Optimization failed")
        
        with col2:
            if st.button("Check Integrity", use_container_width=True):
                self._check_database_integrity()
        
        with col3:
            if st.button("View Statistics", use_container_width=True):
                self._show_database_statistics()
        
        st.markdown("---")
        
        # Existing backups
        st.markdown("#### Existing Backups")
        self._show_existing_backups()
    
    """
    Add Database Cleanup Section to Admin Interface
    ================================================
    This code integrates directly into the existing admin.py file.
    """

    # Add this method to the AdminInterface class (around line 800, after _show_database_management)

    def _show_database_cleanup(self):
        """Database cleanup section for removing seed data"""
        
        st.markdown("### 🗑️ Database Cleanup")
        st.markdown("Remove test/seed data from the database")
        
        with st.expander("View Current Buildings", expanded=False):
            try:
                import pandas as pd
                conn = sqlite3.connect(self.db_path)
                
                buildings_df = pd.read_sql_query("""
                    SELECT 
                        b.name as Building,
                        b.address as Address,
                        COUNT(DISTINCT i.id) as Inspections,
                        b.total_defects as Defects,
                        b.created_at as Created
                    FROM inspector_buildings b
                    LEFT JOIN inspector_inspections i ON b.id = i.building_id
                    GROUP BY b.id
                    ORDER BY b.created_at DESC
                """, conn)
                
                conn.close()
                
                if not buildings_df.empty:
                    st.dataframe(buildings_df, use_container_width=True)
                else:
                    st.info("No buildings in database")
                    
            except Exception as e:
                st.error(f"Error loading buildings: {e}")
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Remove Seed Data")
            st.markdown("Removes test buildings:")
            st.markdown("- Harbour Views Apartments")
            st.markdown("- City Central Complex")
            st.markdown("- Test Building - Schema Verification")
            
            if st.button("🗑️ Remove Seed Data", type="secondary", use_container_width=True):
                with st.spinner("Removing seed data..."):
                    success, message = self._remove_seed_data()
                    if success:
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)
        
        with col2:
            st.markdown("#### Clear All Data")
            st.warning("⚠️ This will delete ALL buildings and inspections")
            
            confirm_clear = st.checkbox("I understand this will delete everything")
            
            if st.button("🔥 Clear All Data", type="primary", disabled=not confirm_clear, use_container_width=True):
                with st.spinner("Clearing all data..."):
                    success, message = self._clear_all_data()
                    if success:
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)


    def _remove_seed_data(self) -> Tuple[bool, str]:
        """Remove seed/test data from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get seed building IDs
            cursor.execute("""
                SELECT id, name FROM inspector_buildings
                WHERE name IN (
                    'Harbour Views Apartments',
                    'City Central Complex',
                    'Test Building - Schema Verification'
                )
            """)
            
            seed_buildings = cursor.fetchall()
            
            if not seed_buildings:
                conn.close()
                return True, "✓ No seed data found - database is already clean"
            
            # Start transaction
            cursor.execute("BEGIN TRANSACTION")
            
            removed_count = 0
            
            # Delete in correct order (respecting foreign keys)
            for building_id, name in seed_buildings:
                
                # Get inspection IDs for this building
                cursor.execute("""
                    SELECT id FROM inspector_inspections
                    WHERE building_id = ?
                """, (building_id,))
                
                inspection_ids = [row[0] for row in cursor.fetchall()]
                
                # Delete work orders
                for inspection_id in inspection_ids:
                    cursor.execute("""
                        DELETE FROM inspector_work_orders
                        WHERE inspection_id = ?
                    """, (inspection_id,))
                
                # Delete inspection items
                for inspection_id in inspection_ids:
                    cursor.execute("""
                        DELETE FROM inspector_inspection_items
                        WHERE inspection_id = ?
                    """, (inspection_id,))
                
                # Delete unit inspections (if table exists)
                try:
                    for inspection_id in inspection_ids:
                        cursor.execute("""
                            DELETE FROM inspector_unit_inspections
                            WHERE inspection_id = ?
                        """, (inspection_id,))
                except:
                    pass  # Table might not exist
                
                # Delete metrics summary (if table exists)
                try:
                    for inspection_id in inspection_ids:
                        cursor.execute("""
                            DELETE FROM inspector_metrics_summary
                            WHERE inspection_id = ?
                        """, (inspection_id,))
                except:
                    pass  # Table might not exist
                
                # Delete inspections
                cursor.execute("""
                    DELETE FROM inspector_inspections
                    WHERE building_id = ?
                """, (building_id,))
                
                # Delete project progress (if table exists)
                try:
                    cursor.execute("""
                        DELETE FROM inspector_project_progress
                        WHERE building_id = ?
                    """, (building_id,))
                except:
                    pass  # Table might not exist
                
                # Delete building
                cursor.execute("""
                    DELETE FROM inspector_buildings
                    WHERE id = ?
                """, (building_id,))
                
                removed_count += 1
            
            # Clean up CSV processing log for test files
            try:
                cursor.execute("""
                    DELETE FROM inspector_csv_processing_log
                    WHERE building_name IN (
                        'Harbour Views Apartments',
                        'City Central Complex',
                        'Test Building - Schema Verification'
                    )
                """)
            except:
                pass  # Table might not exist
            
            # Commit changes
            conn.commit()
            conn.close()
            
            return True, f"✅ Removed {removed_count} seed buildings successfully"
            
        except Exception as e:
            try:
                conn.rollback()
                conn.close()
            except:
                pass
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"Error removing seed data: {e}\n{error_details}")
            return False, f"❌ Error removing seed data: {str(e)}"


    def _clear_all_data(self) -> Tuple[bool, str]:
        """Clear all inspection data from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("BEGIN TRANSACTION")
            
            # Delete in order respecting foreign keys
            tables_to_clear = [
                'inspector_work_orders',
                'inspector_metrics_summary',
                'inspector_inspection_items',
                'inspector_unit_inspections',
                'inspector_project_progress',
                'inspector_inspections',
                'inspector_buildings',
                'inspector_csv_processing_log'
            ]
            
            cleared_count = 0
            for table in tables_to_clear:
                try:
                    cursor.execute(f"DELETE FROM {table}")
                    cleared_count += 1
                except Exception as e:
                    logger.warning(f"Could not clear {table}: {e}")
            
            conn.commit()
            conn.close()
            
            return True, f"✅ Cleared {cleared_count} tables successfully"
            
        except Exception as e:
            try:
                conn.rollback()
                conn.close()
            except:
                pass
            logger.error(f"Error clearing data: {e}")
            return False, f"❌ Error clearing data: {str(e)}"
    
    def _create_backup(self, backup_name: Optional[str], include_files: bool) -> Optional[str]:
        """Create database backup"""
        
        try:
            # Create backups directory
            backup_dir = Path("backups")
            backup_dir.mkdir(exist_ok=True)
            
            # Generate backup filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if backup_name:
                clean_name = "".join(c for c in backup_name if c.isalnum() or c in (' ', '-', '_')).strip()
                filename = f"backup_{clean_name}_{timestamp}.db"
            else:
                filename = f"backup_{timestamp}.db"
            
            backup_path = backup_dir / filename
            
            # Copy database
            shutil.copy2(self.db_path, backup_path)
            
            # If including files, create zip
            if include_files and Path("uploads").exists():
                import zipfile
                
                zip_path = backup_dir / filename.replace('.db', '.zip')
                
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    # Add database
                    zipf.write(backup_path, arcname=filename)
                    
                    # Add uploads
                    for root, dirs, files in os.walk("uploads"):
                        for file in files:
                            file_path = os.path.join(root, file)
                            arcname = os.path.relpath(file_path, ".")
                            zipf.write(file_path, arcname=arcname)
                
                # Remove standalone db file
                backup_path.unlink()
                return str(zip_path)
            
            return str(backup_path)
            
        except Exception as e:
            logger.error(f"Backup error: {e}")
            st.error(f"Backup failed: {e}")
            return None
    
    def _restore_backup(self, backup_file) -> bool:
        """Restore database from backup"""
        
        try:
            # Create temporary backup of current database
            current_backup = f"{self.db_path}.pre_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(self.db_path, current_backup)
            
            # Restore from uploaded file
            with open(self.db_path, 'wb') as f:
                f.write(backup_file.getbuffer())
            
            st.info(f"Previous database saved as: {current_backup}")
            
            return True
            
        except Exception as e:
            logger.error(f"Restore error: {e}")
            st.error(f"Restore failed: {e}")
            return False
    
    def _optimize_database(self) -> bool:
        """Optimize database (VACUUM)"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("VACUUM")
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Optimization error: {e}")
            return False
    
    def _check_database_integrity(self):
        """Check database integrity"""
        
        with st.expander("Integrity Check Results", expanded=True):
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                cursor.execute("PRAGMA integrity_check")
                result = cursor.fetchone()
                
                if result[0] == 'ok':
                    st.success("Database integrity: OK")
                else:
                    st.error(f"Database integrity issues: {result[0]}")
                
                # Check for orphaned records
                st.markdown("**Orphaned Records Check:**")
                
                # Check work orders without inspections
                cursor.execute("""
                    SELECT COUNT(*) FROM inspector_work_orders 
                    WHERE inspection_id NOT IN (SELECT id FROM inspector_inspections)
                """)
                orphaned_work_orders = cursor.fetchone()[0]
                
                if orphaned_work_orders > 0:
                    st.warning(f"Found {orphaned_work_orders} orphaned work orders")
                else:
                    st.success("No orphaned work orders")
                
                conn.close()
                
            except Exception as e:
                st.error(f"Integrity check failed: {e}")
    
    def _show_database_statistics(self):
        """Show detailed database statistics"""
        
        with st.expander("Database Statistics", expanded=True):
            try:
                if self.db:
                    stats = self.db.get_database_stats()
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**Core Tables:**")
                        st.metric("Users", stats.get('users_count', 0))
                        st.metric("Buildings", stats.get('inspector_buildings_count', 0))
                        st.metric("Inspections", stats.get('inspector_inspections_count', 0))
                    
                    with col2:
                        st.markdown("**Work Management:**")
                        st.metric("Work Orders", stats.get('inspector_work_orders_count', 0))
                        st.metric("Progress Records", stats.get('inspector_project_progress_count', 0))
                        st.metric("Processing Logs", stats.get('inspector_csv_processing_log_count', 0))
                    
                    # Table sizes
                    st.markdown("**Table Sizes:**")
                    
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    
                    cursor.execute("""
                        SELECT name FROM sqlite_master 
                        WHERE type='table' 
                        ORDER BY name
                    """)
                    
                    tables = cursor.fetchall()
                    
                    table_sizes = []
                    for (table_name,) in tables:
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        count = cursor.fetchone()[0]
                        table_sizes.append({'Table': table_name, 'Rows': count})
                    
                    conn.close()
                    
                    sizes_df = pd.DataFrame(table_sizes)
                    st.dataframe(sizes_df, use_container_width=True)
                    
            except Exception as e:
                st.error(f"Error loading statistics: {e}")
    
    def _show_existing_backups(self):
        """Show list of existing backups"""
        
        backup_dir = Path("backups")
        
        if not backup_dir.exists():
            st.info("No backups found")
            return
        
        backups = list(backup_dir.glob("backup_*.db")) + list(backup_dir.glob("backup_*.zip"))
        
        if not backups:
            st.info("No backups found")
            return
        
        # Sort by modification time (newest first)
        backups.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        st.caption(f"Found {len(backups)} backup(s)")
        
        for backup in backups[:10]:  # Show last 10
            stat = backup.stat()
            size_mb = stat.st_size / (1024 * 1024)
            modified = datetime.fromtimestamp(stat.st_mtime)
            
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
            
            with col1:
                st.text(backup.name)
            
            with col2:
                st.caption(f"{size_mb:.2f} MB")
            
            with col3:
                st.caption(modified.strftime("%Y-%m-%d %H:%M"))
            
            with col4:
                try:
                    with open(backup, 'rb') as f:
                        st.download_button(
                            "Download",
                            data=f.read(),
                            file_name=backup.name,
                            mime="application/octet-stream",
                            key=f"download_backup_{backup.name}"
                        )
                except Exception as e:
                    st.error(f"Error: {e}")
    
    # ========================================================================
    # SYSTEM SETTINGS
    # ========================================================================
    
    def _show_system_settings(self):
        """System settings and configuration"""
        
        st.markdown("### System Settings")
        
        # Load current settings
        settings = self._load_system_settings()
        
        with st.form("system_settings_form"):
            st.markdown("#### General Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                system_name = st.text_input(
                    "System Name",
                    value=settings.get('system_name', 'Building Inspection System')
                )
                
                default_urgency_threshold = st.number_input(
                    "Default Urgency Threshold (days)",
                    min_value=1,
                    max_value=90,
                    value=settings.get('default_urgency_threshold', 7)
                )
                
                max_file_size_mb = st.number_input(
                    "Max Upload File Size (MB)",
                    min_value=1,
                    max_value=100,
                    value=settings.get('max_file_size_mb', 10)
                )
            
            with col2:
                auto_work_order_creation = st.checkbox(
                    "Auto-create work orders from defects",
                    value=settings.get('auto_work_order_creation', True)
                )
                
                require_photo_evidence = st.checkbox(
                    "Require photo evidence for completion",
                    value=settings.get('require_photo_evidence', False)
                )
                
                # Email notifications disabled - removed checkbox
            
            st.markdown("---")
            
            # EMAIL CONFIGURATION SECTION - COMMENTED OUT
            # st.markdown("#### Email Configuration")
            # st.info("Email notifications feature coming soon")
            
            st.markdown("#### Workflow Settings")
            
            col1, col2 = st.columns(2)
            
            with col1:
                require_builder_notes = st.checkbox(
                    "Require builder notes on completion",
                    value=settings.get('require_builder_notes', True)
                )
                
                auto_approve_threshold = st.number_input(
                    "Auto-approve after days (0 = disabled)",
                    min_value=0,
                    max_value=90,
                    value=settings.get('auto_approve_threshold', 0)
                )
            
            with col2:
                enable_quality_scoring = st.checkbox(
                    "Enable quality scoring",
                    value=settings.get('enable_quality_scoring', True)
                )
                
                min_quality_score = st.number_input(
                    "Minimum quality score",
                    min_value=0,
                    max_value=100,
                    value=settings.get('min_quality_score', 70)
                )
            
            st.markdown("---")
            
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                submit = st.form_submit_button("Save Settings", type="primary", use_container_width=True)
            
            with col3:
                reset = st.form_submit_button("Reset to Defaults", use_container_width=True)
            
            if submit:
                new_settings = {
                    'system_name': system_name,
                    'default_urgency_threshold': default_urgency_threshold,
                    'max_file_size_mb': max_file_size_mb,
                    'auto_work_order_creation': auto_work_order_creation,
                    'require_photo_evidence': require_photo_evidence,
                    'enable_email_notifications': False,  # Always false for now
                    'smtp_host': settings.get('smtp_host', ''),
                    'smtp_port': settings.get('smtp_port', 587),
                    'smtp_username': settings.get('smtp_username', ''),
                    'smtp_password': settings.get('smtp_password', ''),
                    'from_email': settings.get('from_email', ''),
                    'require_builder_notes': require_builder_notes,
                    'auto_approve_threshold': auto_approve_threshold,
                    'enable_quality_scoring': enable_quality_scoring,
                    'min_quality_score': min_quality_score
                }
                
                if self._save_system_settings(new_settings):
                    st.success("Settings saved successfully!")
                    st.rerun()
                else:
                    st.error("Failed to save settings")
            
            if reset:
                if self._save_system_settings(self._get_default_settings()):
                    st.success("Settings reset to defaults!")
                    st.rerun()
    
    def _load_system_settings(self) -> dict:
        """Load system settings from file"""
        
        settings_file = Path("system_settings.json")
        
        if settings_file.exists():
            try:
                with open(settings_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading settings: {e}")
        
        return self._get_default_settings()
    
    def _save_system_settings(self, settings: dict) -> bool:
        """Save system settings to file"""
        
        try:
            settings_file = Path("system_settings.json")
            
            with open(settings_file, 'w') as f:
                json.dump(settings, f, indent=2)
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving settings: {e}")
            return False
    
    def _get_default_settings(self) -> dict:
        """Get default system settings"""
        
        return {
            'system_name': 'Building Inspection System',
            'default_urgency_threshold': 7,
            'max_file_size_mb': 10,
            'auto_work_order_creation': True,
            'require_photo_evidence': False,
            'enable_email_notifications': False,
            'smtp_host': '',
            'smtp_port': 587,
            'smtp_username': '',
            'smtp_password': '',
            'from_email': '',
            'require_builder_notes': True,
            'auto_approve_threshold': 0,
            'enable_quality_scoring': True,
            'min_quality_score': 70
        }
    
    # ========================================================================
    # AUDIT TRAIL
    # ========================================================================
    
    def _show_audit_trail(self):
        """Show system audit trail"""
        
        st.markdown("### Audit Trail")
        
        st.info("Track all system activities and user actions")
        
        # Filters
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            action_type = st.selectbox(
                "Action Type:",
                ['All', 'Login', 'User Created', 'User Updated', 'Setting Changed', 
                 'Backup Created', 'Database Restored', 'Work Order', 'Inspection']
            )
        
        with col2:
            user_filter = st.selectbox(
                "User:",
                ['All'] + self._get_active_usernames()
            )
        
        with col3:
            date_range = st.selectbox(
                "Date Range:",
                ['Today', 'Last 7 Days', 'Last 30 Days', 'Last 90 Days', 'All Time']
            )
        
        with col4:
            if st.button("Refresh", use_container_width=True):
                st.rerun()
        
        st.markdown("---")
        
        # Load audit logs
        audit_logs = self._load_audit_logs(action_type, user_filter, date_range)
        
        if audit_logs.empty:
            st.info("No audit logs found for the selected filters")
            return
        
        st.caption(f"Showing {len(audit_logs)} log entries")
        
        # Display logs
        for idx, (_, log) in enumerate(audit_logs.iterrows()):
            self._render_audit_log_entry(log, idx)
        
        # Export option
        st.markdown("---")
        
        if st.button("Export Audit Trail (CSV)", use_container_width=True):
            csv_data = audit_logs.to_csv(index=False)
            st.download_button(
                "Download Audit Trail",
                data=csv_data,
                file_name=f"audit_trail_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    
    def _load_audit_logs(self, action_type: str, user_filter: str, date_range: str) -> pd.DataFrame:
        """Load audit logs from database"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Base query - combining multiple sources
            logs = []
            
            # User logins
            query = """
                SELECT 
                    u.username,
                    'Login' as action_type,
                    'User logged in' as description,
                    u.last_login as timestamp
                FROM users u
                WHERE u.last_login IS NOT NULL
            """
            
            login_logs = pd.read_sql_query(query, conn)
            logs.append(login_logs)
            
            # CSV processing logs
            query = """
                SELECT 
                    inspector_name as username,
                    'Inspection' as action_type,
                    'Processed CSV: ' || original_filename as description,
                    created_at as timestamp
                FROM inspector_csv_processing_log
            """
            
            inspection_logs = pd.read_sql_query(query, conn)
            logs.append(inspection_logs)
            
            # Work order updates
            query = """
                SELECT 
                    'system' as username,
                    'Work Order' as action_type,
                    'Work order status: ' || status as description,
                    updated_at as timestamp
                FROM inspector_work_orders
                WHERE updated_at IS NOT NULL
            """
            
            work_order_logs = pd.read_sql_query(query, conn)
            logs.append(work_order_logs)
            
            conn.close()
            
            # Combine all logs
            if logs:
                combined_logs = pd.concat(logs, ignore_index=True)
                
                # Apply filters
                if action_type != 'All':
                    combined_logs = combined_logs[combined_logs['action_type'] == action_type]
                
                if user_filter != 'All':
                    combined_logs = combined_logs[combined_logs['username'] == user_filter]
                
                # Date range filter
                if date_range != 'All Time':
                    combined_logs['timestamp'] = pd.to_datetime(combined_logs['timestamp'])
                    now = datetime.now()
                    
                    if date_range == 'Today':
                        cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
                    elif date_range == 'Last 7 Days':
                        cutoff = now - timedelta(days=7)
                    elif date_range == 'Last 30 Days':
                        cutoff = now - timedelta(days=30)
                    else:  # Last 90 Days
                        cutoff = now - timedelta(days=90)
                    
                    combined_logs = combined_logs[combined_logs['timestamp'] >= cutoff]
                
                # Sort by timestamp descending
                combined_logs = combined_logs.sort_values('timestamp', ascending=False)
                
                return combined_logs
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error loading audit logs: {e}")
            return pd.DataFrame()
    
    def _render_audit_log_entry(self, log, idx):
        """Render single audit log entry"""
        
        # Action type colors
        action_colors = {
            'Login': '#4CAF50',
            'Inspection': '#2196F3',
            'Work Order': '#FF9800',
            'User Created': '#9C27B0',
            'User Updated': '#607D8B',
            'Setting Changed': '#F44336',
            'Backup Created': '#00BCD4',
            'Database Restored': '#FF5722'
        }
        
        color = action_colors.get(log['action_type'], '#757575')
        
        col1, col2, col3, col4 = st.columns([1, 2, 3, 2])
        
        with col1:
            st.markdown(f"""
            <div style="background-color: {color}; color: white; padding: 0.3rem 0.6rem; 
                        border-radius: 5px; text-align: center; font-size: 0.75em;">
                {log['action_type']}
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.caption(f"User: {log['username']}")
        
        with col3:
            st.caption(log['description'])
        
        with col4:
            try:
                timestamp = pd.to_datetime(log['timestamp'])
                st.caption(timestamp.strftime('%Y-%m-%d %H:%M:%S'))
            except:
                st.caption(str(log['timestamp']))
        
        st.divider()
    
    def _get_active_usernames(self) -> List[str]:
        """Get list of active usernames"""
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT username FROM users 
                WHERE is_active = 1 
                ORDER BY username
            """)
            
            usernames = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            return usernames
            
        except Exception as e:
            logger.error(f"Error loading usernames: {e}")
            return []


def render_admin_interface(user_info=None, auth_manager=None):
    """Entry point for admin interface"""
    
    if 'admin_interface' not in st.session_state:
        st.session_state.admin_interface = AdminInterface(user_info=user_info)
    
    st.session_state.admin_interface.user_info = user_info
    st.session_state.admin_interface.show()


if __name__ == "__main__":
    print("Admin Interface Module")
    print("=" * 50)
    print("Features:")
    print("- User Management (Create, Edit, Deactivate)")
    print("- Master Trade Mapping Management")
    print("- Database Backup/Restore")
    print("- System Settings Configuration")
    print("- Audit Trail Viewing")
    print("=" * 50)