"""
Building Inspection System V3 - Main Application Entry Point
With Inspection Reports Page for Developer Role
Updated with Automatic Schema Migration for Streamlit Cloud
"""

import streamlit as st
import sqlite3
from pathlib import Path
import sys
import os
import logging

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

# Import components
from database.setup import setup_database, DatabaseManager
from roles.inspector import render_inspector_interface
# from roles.owner import render_owner_interface
from roles.builder import render_builder_interface
from roles.admin import render_admin_interface

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_system_settings() -> dict:
    """Load system settings from file"""
    settings_file = Path("system_settings.json")
    
    if settings_file.exists():
        try:
            import json
            with open(settings_file, 'r') as f:
                return json.load(f)
        except:
            pass
    
    # Return defaults if file doesn't exist or can't be read
    return {
        'system_name': 'Building Inspection System V3',
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

# ✅ 1. Pure, cache-safe database resource (NO Streamlit UI calls)
@st.cache_resource(show_spinner=False)
def get_db_manager():
    """
    Create or open the database and ensure schema exists.
    Absolutely NO st.* calls inside (cache-safe).
    """
    db_manager = DatabaseManager("building_inspection.db")

    # Ensure database exists; setup_database logs via logger (not Streamlit UI)
    if not os.path.exists(db_manager.db_path):
        logger.info("Database doesn't exist - creating fresh.")
        setup_database(
            db_path=db_manager.db_path,
            force_recreate=False,
            seed_test_data=False
        )
    else:
        # Open a connection so downstream code can use it immediately
        db_manager.connect()
        logger.info(f"Connected to DB: {db_manager.db_path}")

    return db_manager


# ✅ 2. Wrapper with Streamlit UI feedback (safe outside of cache)
def init_database_with_ui():
    """
    Wrapper that adds UI messages safely outside of the cached function.
    This is what you call from main().
    """
    db_manager = get_db_manager()

    # Run schema checks/migrations (no st.* inside _apply_schema_migrations)
    migration_success, update_count = _apply_schema_migrations(db_manager)

    # Show results safely (outside cached function)
    if update_count > 0:
        st.toast(f"✅ Applied {update_count} database updates", icon="🔧")

    if not migration_success:
        st.warning("⚠️ Schema migration failed - attempting database recreation.")
        logger.warning("Migration failed — recreating database.")
        db_manager.initialize_database(force_recreate=True)

    # Save instance for reuse (safe now)
    st.session_state["db_manager"] = db_manager
    return db_manager
  
def _apply_schema_migrations(db_manager: DatabaseManager) -> bool:
    """
    Apply schema migrations to existing database
    
    This function adds any missing columns/tables without losing existing data.
    Safe to run multiple times - only applies changes if needed.
    
    Returns:
        True if migrations successful or not needed, False if failed
    """
    try:
        conn = db_manager.connect()
        cursor = conn.cursor()
        
        logger.info("Checking for schema updates...")
        
        # Track what was updated
        updates_applied = []
        
        # ===================================================================
        # MIGRATION 1: Add owner_signoff_timestamp to inspector_inspection_items
        # ===================================================================
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM pragma_table_info('inspector_inspection_items') 
                WHERE name='owner_signoff_timestamp'
            """)
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                    ALTER TABLE inspector_inspection_items 
                    ADD COLUMN owner_signoff_timestamp TIMESTAMP
                """)
                updates_applied.append("Added owner_signoff_timestamp column")
                logger.info("✅ Added owner_signoff_timestamp column")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                logger.warning(f"Could not add owner_signoff_timestamp: {e}")
        
        # ===================================================================
        # MIGRATION 2: Add inspection_date to inspector_inspection_items
        # ===================================================================
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM pragma_table_info('inspector_inspection_items') 
                WHERE name='inspection_date'
            """)
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                    ALTER TABLE inspector_inspection_items 
                    ADD COLUMN inspection_date DATE
                """)
                updates_applied.append("Added inspection_date column")
                logger.info("✅ Added inspection_date column")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                logger.warning(f"Could not add inspection_date: {e}")
        
        # ===================================================================
        # MIGRATION 3: Create inspector_unit_inspections table
        # ===================================================================
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='inspector_unit_inspections'
        """)
        
        if not cursor.fetchone():
            cursor.execute("""
                CREATE TABLE inspector_unit_inspections (
                    id TEXT PRIMARY KEY,
                    inspection_id TEXT NOT NULL,
                    building_id TEXT NOT NULL,
                    unit TEXT NOT NULL,
                    unit_type TEXT,
                    inspection_date DATE,
                    inspector_name TEXT,
                    items_count INTEGER DEFAULT 0,
                    defects_count INTEGER DEFAULT 0,
                    owner_signoff_timestamp TIMESTAMP,
                    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'in_progress', 'completed')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (inspection_id) REFERENCES inspector_inspections (id),
                    FOREIGN KEY (building_id) REFERENCES inspector_buildings (id)
                )
            """)
            
            # Create indexes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_inspector_unit_inspections_inspection 
                ON inspector_unit_inspections(inspection_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_inspector_unit_inspections_unit 
                ON inspector_unit_inspections(unit)
            """)
            
            updates_applied.append("Created inspector_unit_inspections table")
            logger.info("✅ Created inspector_unit_inspections table")
        
        # ===================================================================
        # MIGRATION 4: Add file_checksum to inspector_csv_processing_log
        # ===================================================================
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM pragma_table_info('inspector_csv_processing_log') 
                WHERE name='file_checksum'
            """)
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                    ALTER TABLE inspector_csv_processing_log 
                    ADD COLUMN file_checksum TEXT
                """)
                updates_applied.append("Added file_checksum column")
                logger.info("✅ Added file_checksum column")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                logger.warning(f"Could not add file_checksum: {e}")
        
        # Commit all changes
        conn.commit()
        
        # Show results
        if updates_applied:
            logger.info(f"Applied {len(updates_applied)} schema updates:")
            for update in updates_applied:
                logger.info(f"  - {update}")
            # Return info about updates (don't use st.toast in cached function)
            return True, len(updates_applied)
        else:
            logger.info("✓ Database schema is up-to-date")
            return True, 0
        
        return True, 0
        
    except Exception as e:
        logger.error(f"Schema migration failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False, 0


def simple_authenticate(username: str, password: str) -> dict:
    """Authenticate against database with hardcoded fallback"""
    
    # Try database first
    try:
        db = get_database_connection()
        if db is not None:
            cursor = db.cursor()
            cursor.execute("""
                SELECT id, username, password_hash, salt, role, first_name, last_name, is_active
                FROM users
                WHERE username = ?
            """, (username,))
            
            result = cursor.fetchone()
            db.close()
            
            if result:
                user_id, db_username, password_hash, salt, role, first_name, last_name, is_active = result
                
                # Check if user is active
                if is_active != 1:
                    return {"success": False}
                
                # Verify password with salt
                import hashlib
                input_hash = hashlib.sha256((password + salt).encode()).hexdigest()
                
                if input_hash == password_hash:
                    return {
                        "success": True,
                        "role": role,
                        "user_id": user_id,
                        "username": db_username,
                        "name": f"{first_name} {last_name}".strip()
                    }
    except Exception as e:
        print(f"Database authentication error: {e}")
    
    # Fallback to hardcoded users
    hardcoded_users = {
        "admin": {"password": "admin123", "role": "admin", "user_id": 1, "name": "System Administrator"},
        "inspector": {"password": "test123", "role": "inspector", "user_id": 2, "name": "John Smith"},
        "inspector1": {"password": "test123", "role": "inspector", "user_id": 2, "name": "John Smith"},
        "inspector2": {"password": "test123", "role": "inspector", "user_id": 3, "name": "Sarah Johnson"},
        "developer": {"password": "test123", "role": "developer", "user_id": 4, "name": "Mike Chen"},
        "builder": {"password": "test123", "role": "builder", "user_id": 5, "name": "David Wilson"},
    }
    
    if username in hardcoded_users and hardcoded_users[username]["password"] == password:
        return {
            "success": True,
            "role": hardcoded_users[username]["role"],
            "user_id": hardcoded_users[username]["user_id"],
            "username": username,
            "name": hardcoded_users[username]["name"]
        }
    
    return {"success": False}


def get_database_connection():
    """Get database connection and handle errors gracefully"""
    try:
        db = sqlite3.connect("building_inspection.db")
        return db
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None


def get_user_info_from_db(user_id: int) -> dict:
    """Get user information from database"""
    try:
        db = get_database_connection()
        if db is None:
            return {}
        
        cursor = db.cursor()
        cursor.execute("""
            SELECT u.username, u.first_name, u.last_name, u.email, u.role,
                   p.company, p.job_title, p.phone
            FROM users u
            LEFT JOIN user_profiles p ON u.id = p.user_id
            WHERE u.id = ?
        """, (user_id,))
        
        result = cursor.fetchone()
        db.close()
        
        if result:
            return {
                "user_id": user_id,
                "username": result[0],
                "name": f"{result[1]} {result[2]}".strip(),
                "email": result[3],
                "role": result[4],
                "company": result[5] or "Not specified",
                "job_title": result[6] or "Not specified",
                "phone": result[7] or "Not specified"
            }
        else:
            return {
                "user_id": user_id,
                "username": st.session_state.get('username', 'Unknown'),
                "name": st.session_state.get('username', 'Unknown').title(),
                "role": st.session_state.get('user_role', 'unknown')
            }
    except Exception as e:
        st.error(f"Error fetching user info: {e}")
        return {
            "user_id": user_id,
            "username": st.session_state.get('username', 'Unknown'),
            "name": st.session_state.get('username', 'Unknown').title(),
            "role": st.session_state.get('user_role', 'unknown')
        }


def ensure_demo_users_exist():
    """Ensure demo users exist in database"""
    try:
        db = get_database_connection()
        if db is None:
            return
        
        cursor = db.cursor()
        import hashlib
        import secrets
        
        demo_users = [
            ("admin", "admin123", "admin", "System", "Administrator"),
            ("inspector", "test123", "inspector", "John", "Smith"),
            ("developer", "test123", "developer", "Mike", "Chen"),
            ("builder", "test123", "builder", "David", "Wilson"),
        ]
        
        for username, password, role, first_name, last_name in demo_users:
            # Check if user exists
            cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
            if cursor.fetchone():
                continue  # User already exists
            
            # Create user with salt and hash
            salt = secrets.token_hex(16)
            password_hash = hashlib.sha256((password + salt).encode()).hexdigest()
            
            cursor.execute("""
                INSERT INTO users (username, email, password_hash, salt, role, is_active, 
                                   first_name, last_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (username, f"{username}@test.com", password_hash, salt, role, 1, 
                  first_name, last_name))
        
        db.commit()
        db.close()
        
    except Exception as e:
        print(f"Error creating demo users: {e}")


class SimpleAuthManager:
    """Simple authentication manager for the demo"""
    
    def __init__(self):
        self.db_path = "building_inspection.db"
    
    def get_database_connection(self):
        """Get database connection"""
        return get_database_connection()
    
    def validate_user(self, user_id: int) -> bool:
        """Validate if user exists and is active"""
        try:
            db = self.get_database_connection()
            if db is None:
                return False
            
            cursor = db.cursor()
            cursor.execute("SELECT is_active FROM users WHERE id = ?", (user_id,))
            result = cursor.fetchone()
            db.close()
            
            return result and result[0] == 1
        except:
            return True


def render_inspection_reports(user_info: dict, auth_manager):
    """Render inspection reports page for developers (read-only view)"""
    try:
        from roles.inspector import InspectorInterface
        
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); 
                    color: white; padding: 1.5rem; border-radius: 10px; margin-bottom: 1rem;">
            <h2 style="margin: 0;">Inspection Reports (Read-Only)</h2>
            <p style="margin: 0.5rem 0 0 0;">View detailed inspection reports across all buildings</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Initialize inspector interface for data access
        if "inspection_viewer" not in st.session_state:
            st.session_state.inspection_viewer = InspectorInterface(user_info=user_info)

        inspector = st.session_state.inspection_viewer
        if getattr(inspector, "db_manager", None) is None and "db_manager" in st.session_state:
            inspector.db_manager = st.session_state["db_manager"]
        
        # Get inspection history
        try:
            import pandas as pd
            conn = inspector.db_manager.connect()
            
            inspections_df = pd.read_sql_query("""
                SELECT 
                    i.id,
                    b.name as building_name,
                    i.inspection_date,
                    i.inspector_name,
                    i.total_units,
                    i.total_defects,
                    i.ready_pct,
                    i.urgent_defects,
                    i.original_filename,
                    i.created_at
                FROM inspector_inspections i
                JOIN inspector_buildings b ON i.building_id = b.id
                WHERE i.original_filename IS NOT NULL
                ORDER BY i.created_at DESC
            """, conn)
            
            if inspections_df.empty:
                st.info("No inspection reports available yet")
                return
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Inspections", len(inspections_df))
            with col2:
                st.metric("Buildings Inspected", inspections_df['building_name'].nunique())
            with col3:
                st.metric("Total Defects Found", inspections_df['total_defects'].sum())
            with col4:
                avg_ready = inspections_df['ready_pct'].mean()
                st.metric("Avg Ready Rate", f"{avg_ready:.1f}%")
            
            st.markdown("---")
            
            # Filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                buildings = ['All'] + sorted(inspections_df['building_name'].unique().tolist())
                building_filter = st.selectbox("Building:", buildings)
            
            with col2:
                inspectors = ['All'] + sorted(inspections_df['inspector_name'].unique().tolist())
                inspector_filter = st.selectbox("Inspector:", inspectors)
            
            with col3:
                date_range = st.selectbox("Date Range:", 
                    ["All Time", "Last 7 Days", "Last 30 Days", "Last 90 Days"])
            
            # Apply filters
            filtered = inspections_df.copy()
            if building_filter != 'All':
                filtered = filtered[filtered['building_name'] == building_filter]
            if inspector_filter != 'All':
                filtered = filtered[filtered['inspector_name'] == inspector_filter]
            
            if date_range != "All Time":
                from datetime import datetime, timedelta
                now = datetime.now()
                if date_range == "Last 7 Days":
                    cutoff = now - timedelta(days=7)
                elif date_range == "Last 30 Days":
                    cutoff = now - timedelta(days=30)
                else:  # Last 90 Days
                    cutoff = now - timedelta(days=90)
                
                filtered['created_at_dt'] = pd.to_datetime(filtered['created_at'])
                filtered = filtered[filtered['created_at_dt'] >= cutoff]
            
            st.caption(f"Showing {len(filtered)} of {len(inspections_df)} inspections")
            
            # Display inspections list
            st.markdown("### Inspection Reports")
            
            for idx, (_, inspection) in enumerate(filtered.iterrows()):
                with st.expander(
                    f"📋 {inspection['building_name']} - {inspection['inspection_date']} "
                    f"({inspection['total_defects']} defects, {inspection['ready_pct']:.1f}% ready)",
                    expanded=False
                ):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.markdown(f"**Building:** {inspection['building_name']}")
                        st.markdown(f"**Inspection Date:** {inspection['inspection_date']}")
                        st.markdown(f"**Inspector:** {inspection['inspector_name']}")
                        st.markdown(f"**Original File:** {inspection['original_filename']}")
                    
                    with col2:
                        st.metric("Total Units", inspection['total_units'])
                        st.metric("Total Defects", inspection['total_defects'])
                        st.metric("Urgent Defects", inspection['urgent_defects'])
                        st.metric("Ready Rate", f"{inspection['ready_pct']:.1f}%")
                    
                    st.markdown("---")
                    
                    # View full report button
                    if st.button(f"📊 View Full Report", key=f"view_report_{inspection['id']}_{idx}"):
                        st.session_state.selected_inspection_id = inspection['id']
                        st.session_state.view_full_report = True
                        st.rerun()
            
            # Show full report if selected
            if st.session_state.get('view_full_report', False) and st.session_state.get('selected_inspection_id'):
                st.markdown("---")
                st.markdown("## Full Inspection Report")
                
                try:
                    processed_data, metrics = inspector.processor.load_inspection_from_database(
                        st.session_state.selected_inspection_id
                    )
                    
                    # Display using inspector's display method (read-only)
                    inspector._display_inspection_summary(metrics, processed_data)
                    
                    # Close button
                    if st.button("Close Full Report"):
                        st.session_state.view_full_report = False
                        st.session_state.selected_inspection_id = None
                        st.rerun()
                        
                except Exception as e:
                    st.error(f"Error loading full report: {e}")
                    if st.button("Back to List"):
                        st.session_state.view_full_report = False
                        st.session_state.selected_inspection_id = None
                        st.rerun()
            
        except Exception as e:
            st.error(f"Error loading inspections: {e}")
            import traceback
            with st.expander("Error Details"):
                st.code(traceback.format_exc())
    
    except ImportError as e:
        st.error("Inspector module not available for viewing reports")
        st.info("Please ensure the inspector interface is properly configured")


def main():
    """Main application entry point"""
    
    # Load system settings FIRST - before anything else
    settings = load_system_settings()
    system_name = settings.get('system_name', 'Building Inspection System V3')
    
    st.set_page_config(
        page_title=system_name,
        page_icon="🏗️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize database with auto-migration
    db_manager = init_database_with_ui()

    
    # Ensure demo users exist in database
    ensure_demo_users_exist()
    
    # Initialize auth manager
    auth_manager = SimpleAuthManager()
    
    # Sidebar navigation
    with st.sidebar:
        st.title(f"🏗️ {system_name}")
        
        # Authentication
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
        
        if not st.session_state.authenticated:
            st.subheader("🔐 Login")
            
            with st.form("login_form"):
                username = st.text_input("Username", placeholder="Enter username")
                password = st.text_input("Password", type="password", placeholder="Enter password")
                login_button = st.form_submit_button("Login", type="primary")
                
                if login_button:
                    auth_result = simple_authenticate(username, password)
                    
                    if auth_result["success"]:
                        st.session_state.authenticated = True
                        st.session_state.user_role = auth_result["role"]
                        st.session_state.user_id = auth_result["user_id"]
                        st.session_state.username = auth_result["username"]
                        st.session_state.user_name = auth_result["name"]
                        st.success("Login successful!")
                        st.rerun()
                    else:
                        st.error("Invalid credentials")
            
            # Demo credentials info
            with st.expander("🔑 Demo Credentials"):
                st.write("**Available Users:**")
                st.code("""
Inspector: inspector / test123
Developer: developer / test123
Builder: builder / test123
Admin: admin / admin123
                """)
        
        else:
            # User info
            st.success(f"✅ Logged in as: **{st.session_state.user_role.title()}**")
            st.write(f"👤 User: {st.session_state.username}")
            
            st.divider()
            
            # Navigation menu based on role
            if st.session_state.user_role == "admin":
                page = "Admin Panel"
                st.info("💼 System Administration")
                
            elif st.session_state.user_role == "inspector":
                page = "Inspector Dashboard"
                st.info("💡 All inspection tools are in the main dashboard")
                
            elif st.session_state.user_role == "owner":
                page = st.selectbox("🧭 Navigate to:", [
                    "Owner Dashboard"
                ])
                
            elif st.session_state.user_role == "developer":
                page = st.selectbox("🧭 Navigate to:", [
                    "Developer Dashboard",
                    "Inspection Reports"
                ])
                
            elif st.session_state.user_role == "builder":
                page = "Builder Dashboard"
                st.info("💡 All work orders are in the main dashboard")
                
            else:
                page = "Dashboard"

            st.divider()
            
            if st.button("🚪 Logout", use_container_width=True):
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()
    
    # Main content area
    if st.session_state.get('authenticated', False):
        try:
            user_info = get_user_info_from_db(st.session_state.user_id)
            
            # Simplified routing
            if st.session_state.user_role == "admin":
                render_admin_interface(user_info=user_info, auth_manager=auth_manager)
                
            elif st.session_state.user_role == "inspector":
                render_inspector_interface(user_info=user_info, auth_manager=auth_manager)
                
            elif st.session_state.user_role == "builder":
                render_builder_interface(user_info=user_info, auth_manager=auth_manager)
                
            elif "Owner Dashboard" in page:
                try:
                    from roles.owner import render_owner_interface
                    render_owner_interface(user_info=user_info, auth_manager=auth_manager)
                except ImportError:
                    st.title("🏢 Owner Dashboard")
                    st.info("🚧 Owner portal coming in Phase 2 - DLP Management")
                
            elif "Developer Dashboard" in page:
                try:
                    from roles.developer import render_developer_interface
                    render_developer_interface(user_info=user_info, auth_manager=auth_manager)
                except ImportError:
                    st.title("💼 Developer Dashboard")
                    st.info("🚧 Developer role interface coming soon!")
                    
            elif "Inspection Reports" in page:
                render_inspection_reports(user_info=user_info, auth_manager=auth_manager)
                
            else:
                st.title(f"🏗️ {system_name}")
                st.info(f"Welcome, {st.session_state.user_role.title()}! Use the sidebar to navigate.")
            
        except Exception as e:
            st.error(f"⚠️ Application Error: {str(e)}")
            st.info("Please check your database setup and try again.")
            
            with st.expander("Error Details (for debugging)"):
                st.code(str(e))
            
            if st.button("🔄 Restart Application"):
                st.rerun()
    
    else:
        # Not authenticated - show welcome page
        st.title(f"🏗️ {system_name}")
        
        st.markdown(f"""
        ## Welcome to ECM Building Pre-Settlement Inspection System

        A comprehensive defect management platform designed for property development.

        ### 🎯 Key Features

        - 📋 **Inspector Tools**: iAuditor CSV processing, automated reporting, quality analytics
        - 🔧 **Builder Portal**: Work order management, progress tracking, photo evidence
        - 💼 **Developer Dashboard**: Defect approval workflow, portfolio analytics, settlement readiness
        - 👨‍💼 **Admin Control**: User management, trade mapping, system configuration

        ### 🔒 Access Control

        **Please login using the sidebar to access your role-specific dashboard.**

        Designed for Essential Community Management's pre-settlement inspection workflow.
        """)
        
        st.markdown("### 🎮 Demo Credentials")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **👥 Available Users:**
            - **Inspector**: `inspector` / `test123` 
            - **Admin**: `admin` / `admin123`
            """)
        
        with col2:
            st.markdown("""
            **🚀 Additional Roles:**
            - **Developer**: `developer` / `test123`
            - **Builder**: `builder` / `test123`
            """)
        
        st.info("💡 **Tip**: Use the sidebar login form to access the system with any of the credentials above.")


if __name__ == "__main__":
    main()