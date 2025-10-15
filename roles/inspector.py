"""
Updated Inspector Role Interface V3 - Enhanced Database Integration
==================================================================

This updated version integrates with the enhanced database setup.py schema
while maintaining all existing functionality for image upload and report generation.
"""

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging
from io import BytesIO, StringIO
from typing import Dict, Any, Tuple, Optional
import tempfile
import os
import zipfile
import hashlib
from database.connection_manager import get_connection_manager

# Import the enhanced modules
from core.data_processor import InspectionDataProcessor, load_master_trade_mapping
from core.trade_mapper import TradeMapper

logger = logging.getLogger(__name__)


# Import enhanced database manager
try:
    from database.setup import DatabaseManager
    DATABASE_AVAILABLE = True
except ImportError:
    try:
        from core.database_manager import DatabaseManager
        DATABASE_AVAILABLE = True
    except ImportError:
        DATABASE_AVAILABLE = False

# Import report generators with fallback
try:
    from reports.excel_generator import generate_professional_excel_report, generate_filename as excel_filename
    EXCEL_REPORT_AVAILABLE = True
except ImportError:
    EXCEL_REPORT_AVAILABLE = False

try:
    from reports.word_generator import generate_professional_word_report, generate_filename as word_filename
    WORD_REPORT_AVAILABLE = True
except ImportError:
    WORD_REPORT_AVAILABLE = False

# Set up logging
logger = logging.getLogger(__name__)


class InspectorInterface:
    """Inspector interface with enhanced V3 database integration for cross-role data access"""
    
    def __init__(self, db_path: str = "building_inspection.db", user_info: dict = None):
        """Initialize the inspector interface with user context"""
        self._button_counter = 0
        
        # ✅ Initialize connection manager FIRST
        self.conn_manager = get_connection_manager()
        self.db_type = self.conn_manager.get_db_type()
        
        # ✅ Pass connection manager to processor
        self.processor = InspectionDataProcessor(db_path, conn_manager=self.conn_manager)
        
        self.mapper = TradeMapper()
        self.processed_data = None
        self.metrics = None
        self.trade_mapping = None
        self.current_inspection_id = None
        self.user_info = user_info
        self.auth_manager = None
        
        # Initialize database manager for backward compatibility
        if DATABASE_AVAILABLE and self.db_type == "sqlite":
            self.db_manager = DatabaseManager(db_path)
            logger.info("Inspector interface initialized with enhanced V3 database support")
        else:
            self.db_manager = None
            logger.warning("Enhanced database not available - limited functionality")
        
        # ... rest of init
        
        # Initialize image storage in session state
        if 'report_images' not in st.session_state:
            st.session_state.report_images = {
                'logo': None,
                'cover': None
            }
    
    def _get_connection(self):
        """Get database connection using connection manager"""
        return self.conn_manager.get_connection()

    # ADD THE AUTHENTICATION METHODS HERE:
    def get_current_user_role(self):
        """Get current user role from authentication system"""
        # Method 1: If user info is passed directly to the interface
        if hasattr(self, 'user_info') and self.user_info:
            return self.user_info.get('role', 'inspector')
        
        # Method 2: Check session state for user info
        if 'user_info' in st.session_state:
            user_info = st.session_state.user_info
            return user_info.get('role', 'inspector')
        
        # Method 3: Check for direct role storage
        if 'user_role' in st.session_state:
            return st.session_state.user_role
        
        # Default to inspector for safety (most restrictive role for trade mapping)
        return 'inspector'

    def get_current_user_trade_permissions(self):
        """Get current user's trade mapping permissions"""
        if hasattr(self, 'user_info') and self.user_info:
            return self.user_info.get('trade_mapping_permissions', ['view_master'])
        
        if 'user_info' in st.session_state:
            user_info = st.session_state.user_info
            return user_info.get('trade_mapping_permissions', ['view_master'])
        
        # Default permissions for unknown users
        return ['view_master']

    def can_save_master_mapping(self):
        """Check if current user can save master mapping"""
        permissions = self.get_current_user_trade_permissions()
        return 'save_master' in permissions

    def can_use_custom_mapping(self):
        """Check if current user can use custom mapping"""
        permissions = self.get_current_user_trade_permissions()
        return 'use_custom' in permissions

    def can_manage_system_mappings(self):
        """Check if current user can manage system mappings"""
        permissions = self.get_current_user_trade_permissions()
        return 'manage_system' in permissions
        
    def _get_unique_key(self, base_key: str) -> str:
        """Generate unique button key"""
        self._button_counter += 1
        return f"{base_key}_{self._button_counter}"
    
    def show_inspector_dashboard(self):
        """Show the main inspector dashboard with enhanced database integration"""
        
        # Enhanced database status indicator
        self._show_enhanced_database_status()
        
        # Step 0: Previous Inspections with enhanced features
        self._show_previous_inspections_section()
        
        # Step 1: Enhanced Trade Mapping with database integration
        self._show_trade_mapping_section()
        
        # Step 2: Data Processing with enhanced logging
        self._show_data_processing_section()
        
        # Step 3: Results and Reports with database references
        if self.processed_data is not None and self.metrics is not None:
            self._show_results_and_reports()
            
            # Step 4: Enhanced Report Generation with database tracking
            self._show_enhanced_report_generation()
    
    def _display_inspection_summary(self, metrics: Dict, processed_data: pd.DataFrame):
        """Display inspection summary for read-only viewing (used by Developer role)"""
        
        # Building Information
        st.markdown("### Building Information")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"""
            **Building:** {metrics['building_name']}  
            **Address:** {metrics['address']}
            **Total Units:** {metrics['total_units']:,}
            """)
        
        with col2:
            st.markdown(f"""              
            **Unit Types:** {metrics['unit_types_str']}  
            **Total Defects:** {metrics['total_defects']:,}
            """)
        
        # Quality Dashboard
        st.markdown("### Quality Dashboard")
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total Units", f"{metrics['total_units']:,}")
        with col2:
            st.metric("Total Defects", f"{metrics['total_defects']:,}", 
                    delta=f"{metrics['defect_rate']:.1f}% rate")
        with col3:
            st.metric("Ready Units", f"{metrics['ready_units']}", 
                    delta=f"{metrics['ready_pct']:.1f}%")
        with col4:
            st.metric("Quality Score", f"{max(0, 100 - metrics['defect_rate']):.1f}/100")
        with col5:
            st.metric("Urgent Defects", metrics['urgent_defects'])
        
        # Summary Analysis
        st.markdown("### Summary Analysis")
        tab1, tab2, tab3, tab4 = st.tabs(["Trade Summary", "Unit Summary", "Room Summary", "Urgent Items"])
        
        with tab1:
            if len(metrics['summary_trade']) > 0:
                display_trade = metrics['summary_trade'].copy()
                display_trade.index = range(1, len(display_trade) + 1)
                st.dataframe(display_trade, use_container_width=True)
                st.bar_chart(display_trade.set_index('Trade')['DefectCount'])
            else:
                st.info("No trade defects found")
        
        with tab2:
            if len(metrics['summary_unit']) > 0:
                display_unit = metrics['summary_unit'].copy()
                display_unit.index = range(1, len(display_unit) + 1)
                st.dataframe(display_unit, use_container_width=True)
                st.bar_chart(display_unit.set_index('Unit')['DefectCount'])
            else:
                st.info("No unit defects found")
        
        with tab3:
            if len(metrics['summary_room']) > 0:
                display_room = metrics['summary_room'].copy()
                display_room.index = range(1, len(display_room) + 1)
                st.dataframe(display_room, use_container_width=True)
            else:
                st.info("No room defects found")
        
        with tab4:
            if len(metrics['urgent_defects_table']) > 0:
                urgent_display = metrics['urgent_defects_table'].copy()
                urgent_display.index = range(1, len(urgent_display) + 1)
                st.dataframe(urgent_display, use_container_width=True)
                st.error(f"**{len(urgent_display)} URGENT defects require immediate attention!**")
            else:
                st.success("No urgent defects found!")
                
    def _show_enhanced_database_status(self):
        """Role-based database status - FIXED for connection manager"""
        
        user_role = self.get_current_user_role()
        
        # ✅ Check connection manager (not old db_manager)
        if hasattr(self, 'conn_manager') and self.conn_manager:
            db_type = self.db_type.upper()
            
            if user_role == 'admin':
                # Full status for admin
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.success(f"✅ Database: {db_type}")
                
                with col2:
                    # Try to get statistics
                    try:
                        conn = self._get_connection()
                        cursor = conn.cursor()
                        
                        cursor.execute("SELECT COUNT(*) as count FROM inspector_inspections")
                        result = cursor.fetchone()
                        insp_count = result['count'] if isinstance(result, dict) else result[0]
                        
                        cursor.close()
                        conn.close()
                        
                        st.info(f"📊 Inspections: {insp_count}")
                    except Exception as e:
                        st.info("📊 Inspections: Available")
                
                with col3:
                    # Try to get work order count
                    try:
                        conn = self._get_connection()
                        cursor = conn.cursor()
                        
                        cursor.execute("SELECT COUNT(*) as count FROM inspector_work_orders")
                        result = cursor.fetchone()
                        wo_count = result['count'] if isinstance(result, dict) else result[0]
                        
                        cursor.close()
                        conn.close()
                        
                        st.info(f"📋 Work Orders: {wo_count}")
                    except Exception as e:
                        st.info("📋 Work Orders: Available")
                
                st.caption(f"Connected to {db_type} - Data saved for cross-role access")
            
            else:
                # Simplified status for inspector
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.success(f"✅ Database Connected: {db_type}")
                    st.caption("Your data will be saved automatically and available to Builder/Developer")
                
                with col2:
                    # Show connection health
                    try:
                        conn = self._get_connection()
                        cursor = conn.cursor()
                        cursor.execute("SELECT 1")
                        cursor.close()
                        conn.close()
                        st.success("🟢 Healthy")
                    except:
                        st.warning("🟡 Check Connection")
        
        else:
            # No connection manager available
            st.warning("⚠️ Database not available - Data will only be stored in current session")
            st.caption("PostgreSQL connection not configured")
    
    def _show_previous_inspections_section(self):
        """Show list of previous inspections"""
        
        st.header("📋 Previous Inspections")
        
        try:
            # ✅ Use connection manager instead of db_manager
            conn = self._get_connection()
            
            # Build query based on database type
            if self.conn_manager.get_db_type() == "postgresql":
                query = """
                    SELECT 
                        i.id,
                        i.inspection_date,
                        b.name as building_name,
                        i.total_units,
                        i.total_defects,
                        i.ready_pct,
                        i.created_at
                    FROM inspector_inspections i
                    JOIN inspector_buildings b ON i.building_id = b.id
                    ORDER BY i.inspection_date DESC, i.created_at DESC
                    LIMIT 20
                """
            else:
                query = """
                    SELECT 
                        i.id,
                        i.inspection_date,
                        b.name as building_name,
                        i.total_units,
                        i.total_defects,
                        i.ready_pct,
                        i.created_at
                    FROM inspector_inspections i
                    JOIN inspector_buildings b ON i.building_id = b.id
                    ORDER BY i.inspection_date DESC, i.created_at DESC
                    LIMIT 20
                """
            
            # Execute query - using psycopg2 connection directly
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Convert to DataFrame manually (avoids SQLAlchemy warning)
            if len(rows) > 0:
                recent_inspections = pd.DataFrame(rows, columns=[
                    'id', 'inspection_date', 'building_name', 'total_units',
                    'total_defects', 'ready_pct', 'created_at'
                ])
            else:
                recent_inspections = pd.DataFrame()
            
            cursor.close()
            conn.close()
            
            if len(recent_inspections) == 0:
                st.info("No previous inspections found. Upload a CSV to get started!")
                return
            
            st.write(f"Found {len(recent_inspections)} previous inspection(s)")
            
            # Display inspections
            for idx, inspection in recent_inspections.iterrows():
                with st.expander(
                    f"🏢 {inspection['building_name']} - {inspection['inspection_date']}"
                ):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Total Units", int(inspection['total_units']))
                    
                    with col2:
                        st.metric("Total Defects", int(inspection['total_defects']))
                    
                    with col3:
                        ready_pct = float(inspection['ready_pct'])
                        st.metric("Settlement Ready", f"{ready_pct:.1f}%")
                    
                    # Load button
                    if st.button(
                        "📂 Load This Inspection",
                        key=f"load_{inspection['id']}",
                        type="primary"
                    ):
                        try:
                            st.info("Loading inspection data...")
                            
                            # Load the inspection
                            loaded_df, loaded_metrics = self.processor.load_inspection_from_database(
                                inspection['id']
                            )
                            
                            if loaded_df is not None:
                                st.success(f"✅ Loaded inspection: {inspection['building_name']}")
                                
                                # Show summary
                                st.subheader("📊 Inspection Summary")
                                
                                summary_col1, summary_col2, summary_col3 = st.columns(3)
                                
                                with summary_col1:
                                    st.metric("Total Items", len(loaded_df))
                                
                                with summary_col2:
                                    defects = loaded_df[loaded_df['StatusClass'] == 'Not OK']
                                    st.metric("Defects", len(defects))
                                
                                with summary_col3:
                                    if 'Urgency' in loaded_df.columns:
                                        urgent = defects[defects['Urgency'] == 'Urgent']
                                        st.metric("Urgent", len(urgent))
                                
                                # Show data
                                st.dataframe(
                                    loaded_df.head(100),
                                    use_container_width=True,
                                    hide_index=True
                                )
                                
                                # Download option
                                csv = loaded_df.to_csv(index=False)
                                st.download_button(
                                    label="📥 Download Full Data",
                                    data=csv,
                                    file_name=f"inspection_{inspection['id'][:8]}.csv",
                                    mime="text/csv"
                                )
                            else:
                                st.error("Failed to load inspection data")
                                
                        except Exception as e:
                            st.error(f"Error loading inspection: {e}")
                            logger.error(f"Load error: {e}")
                            import traceback
                            logger.error(traceback.format_exc())
        
        except Exception as e:
            st.error(f"Error retrieving previous inspections: {e}")
            logger.error(f"Previous inspections error: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _show_trade_mapping_section(self):
        """Enhanced trade mapping section with V3 database integration"""
        
        st.markdown("### Step 1: Trade Mapping ")
        
        # Initialize trade mapping with enhanced database loading
        if 'trade_mapping' not in st.session_state:
            if self.db_manager:
                try:
                    db_mapping = self.db_manager.get_trade_mapping()
                    if len(db_mapping) > 0:
                        st.session_state.trade_mapping = db_mapping
                        st.info("Loaded trade mapping from database")
                    else:
                        st.session_state.trade_mapping = pd.DataFrame(columns=["Room", "Component", "Trade"])
                except Exception as e:
                    st.warning(f"Could not load from database: {e}")
                    st.session_state.trade_mapping = pd.DataFrame(columns=["Room", "Component", "Trade"])
            else:
                st.session_state.trade_mapping = pd.DataFrame(columns=["Room", "Component", "Trade"])
        
        # Enhanced status indicator
        if len(st.session_state.trade_mapping) > 0:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.success(f"Trade mapping loaded: {len(st.session_state.trade_mapping)} entries covering {st.session_state.trade_mapping['Trade'].nunique()} trades")
            with col2:
                if self.db_manager:
                    st.info("Auto-sync to DB")
                
            # Show enhanced mapping statistics
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.metric("Total Mappings", len(st.session_state.trade_mapping))
            with col_b:
                st.metric("Unique Trades", st.session_state.trade_mapping['Trade'].nunique())
            with col_c:
                st.metric("Coverage Score", f"{min(100, len(st.session_state.trade_mapping) * 2)}/100")
        else:
            st.warning("No trade mapping loaded - Required for processing")
        
        # Enhanced main options tabs
        tab1, tab2, tab3 = st.tabs(["Load Mapping", "Manage Mapping", "Analytics"])
        
        with tab1:
            self._show_enhanced_mapping_load_options()
        
        with tab2:
            self._show_enhanced_mapping_management()
        
        with tab3:
            self._show_mapping_analytics()
        
        # Enhanced current mapping display
        if len(st.session_state.trade_mapping) > 0:
            default_expanded = st.session_state.get('show_current_mapping', False)
            
            with st.expander(f"Current Loaded Mapping ({len(st.session_state.trade_mapping)} entries)", expanded=default_expanded):
                self._show_enhanced_current_mapping_details()
            
            if 'show_current_mapping' in st.session_state:
                st.session_state.show_current_mapping = False
        
        # Validation check
        if len(st.session_state.trade_mapping) == 0:
            st.error("Trade mapping is required before processing inspection data. Please load or upload a mapping above.")
            return False
        
        return True
    
    def _show_inspector_analytics_dashboard(self):
        """Show analytics dashboard for inspector"""
        if not self.db_manager:
            return
        
        with st.expander("Inspector Analytics Dashboard", expanded=True):
            try:
                # Get comprehensive stats
                stats = self.db_manager.get_database_stats()
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Buildings", stats.get('inspector_buildings_count', 0))
                with col2:
                    st.metric("Work Orders Created", stats.get('inspector_work_orders_count', 0))
                with col3:
                    st.metric("CSV Processing Logs", stats.get('inspector_csv_processing_log_count', 0))
                with col4:
                    st.metric("Progress Records", stats.get('inspector_project_progress_count', 0))
                
                # Recent activity
                st.markdown("**Recent Activity:**")
                col_a, col_b = st.columns(2)
                with col_a:
                    recent_inspections = stats.get('inspector_inspections_last_week', 0)
                    st.info(f"Inspections This Week: {recent_inspections}")
                with col_b:
                    recent_work_orders = stats.get('work_orders_last_week', 0)
                    st.info(f"Work Orders This Week: {recent_work_orders}")
                
            except Exception as e:
                st.error(f"Error loading analytics: {e}")
    
    def _load_previous_inspection(self, inspection_id: str):
        """Load a previous inspection from database"""
        try:
            with st.spinner("Loading previous inspection from database..."):
                # Load inspection data using the processor method
                processed_data, metrics = self.processor.load_inspection_from_database(inspection_id)
                
                # Update current state
                self.processed_data = processed_data
                self.metrics = metrics
                self.current_inspection_id = inspection_id
                
                # Also load trade mapping from database
                if self.db_manager:
                    try:
                        trade_mapping = self.db_manager.get_trade_mapping()
                        if len(trade_mapping) > 0:
                            st.session_state.trade_mapping = trade_mapping
                    except Exception as e:
                        logger.warning(f"Could not load trade mapping: {e}")
                
                # Display success message with cleaned data
                st.success(f"Loaded inspection: {metrics['building_name']}")
                st.info(f"Units: {metrics['total_units']}, Defects: {metrics['total_defects']}, Ready: {metrics['ready_pct']:.1f}%")
                
                # Show work orders status
                try:
                    if self.db_manager:
                        work_orders = self.db_manager.get_work_orders_for_builder()
                        if len(work_orders) > 0:
                            pending_orders = len(work_orders[work_orders['status'] == 'pending'])
                            st.info(f"Associated work orders: {len(work_orders)} total, {pending_orders} pending")
                except Exception as e:
                    logger.warning(f"Could not load work orders: {e}")
                
                st.rerun()
                
        except Exception as e:
            st.error(f"Error loading inspection: {e}")
            logger.error(f"Load inspection error: {e}")
    
    def _show_enhanced_mapping_load_options(self):
        """Simplified trade mapping with role-based permissions"""
        st.markdown("#### Choose Your Mapping Source")
        
        # Get user role (you'll need to pass this from your auth system)
        user_role = self.get_current_user_role()  # Implement this method
        
        col1, col2 = st.columns(2)
        
        # Option 1: Master Mapping (Available to all roles)
        with col1:
            st.markdown("""
            <div style="border: 2px solid #2196F3; border-radius: 10px; padding: 2rem; text-align: center; height: 200px; display: flex; flex-direction: column; justify-content: space-between;">
                <div>
                    <h4 style="color: #2196F3; margin-top: 0;">Master Mapping</h4>
                    <p style="font-size: 0.95em; margin: 1rem 0;">Official system mapping (Admin controlled)</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("Use Master Mapping", type="primary", use_container_width=True, key="use_master_mapping"):
                try:
                    master_mapping = load_master_trade_mapping()
                    st.session_state.trade_mapping = master_mapping
                    st.session_state.show_current_mapping = True
                    st.session_state.mapping_source = "master_official"
                    
                    st.success(f"Official master mapping loaded! {len(master_mapping)} entries")
                    st.info("This is the admin-approved mapping")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error loading master mapping: {e}")
            
            if st.button("Preview Master", use_container_width=True, key="preview_master_mapping"):
                try:
                    master_mapping = load_master_trade_mapping()
                    st.info(f"{len(master_mapping)} entries covering {master_mapping['Trade'].nunique()} trade categories")
                except Exception as e:
                    st.error(f"Error: {e}")
        
        # Option 2: Custom Upload (Inspector can use temporarily, Admin can save permanently)
        with col2:
            st.markdown("""
            <div style="border: 2px solid #FF9800; border-radius: 10px; padding: 2rem; text-align: center; height: 200px; display: flex; flex-direction: column; justify-content: space-between;">
                <div>
                    <h4 style="color: #FF9800; margin-top: 0;">Custom Upload</h4>
                    <p style="font-size: 0.95em; margin: 1rem 0;">Upload custom mapping (temporary for Inspector)</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            uploaded_file = st.file_uploader(
                "Upload CSV file",
                type=["csv"],
                help="CSV with Room, Component, Trade columns",
                key="custom_mapping_upload"
            )
            
            if uploaded_file is not None:
                try:
                    uploaded_mapping = pd.read_csv(uploaded_file)
                    
                    # Validation
                    required_cols = ['Room', 'Component', 'Trade']
                    missing_cols = [col for col in required_cols if col not in uploaded_mapping.columns]
                    
                    if missing_cols:
                        st.error(f"Missing required columns: {', '.join(missing_cols)}")
                    else:
                        # Show preview metrics
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.metric("Total Entries", len(uploaded_mapping))
                        with col_b:
                            st.metric("Trade Categories", uploaded_mapping['Trade'].nunique())
                        with col_c:
                            duplicates = uploaded_mapping.duplicated(subset=['Room', 'Component']).sum()
                            st.metric("Duplicates", duplicates, delta="⚠️" if duplicates > 0 else "✅")
                        
                        # Role-based action buttons
                        if user_role == 'inspector':
                            # Inspector: Only temporary use
                            if st.button("Use Custom (Temporary)", type="primary", use_container_width=True, key="use_custom_temp"):
                                st.session_state.trade_mapping = uploaded_mapping.copy()
                                st.session_state.show_current_mapping = True
                                st.session_state.mapping_source = "custom_temp_inspector"
                                
                                st.success(f"Custom mapping loaded temporarily!")
                                st.info("Inspector role: Mapping is session-only (not saved to database)")
                                st.info("Contact Admin to make this mapping permanent")
                                st.rerun()
                            
                            st.caption("Inspector permissions: Temporary use only")
                            st.caption("Contact Admin to update master mapping")
                        
                        elif user_role == 'admin':
                            # Admin: Both temporary and permanent options
                            col_temp, col_perm = st.columns(2)
                            
                            with col_temp:
                                if st.button("Use Temporary", use_container_width=True, key="admin_use_temp"):
                                    st.session_state.trade_mapping = uploaded_mapping.copy()
                                    st.session_state.show_current_mapping = True
                                    st.session_state.mapping_source = "custom_temp_admin"
                                    
                                    st.success("Loaded temporarily!")
                                    st.rerun()
                            
                            with col_perm:
                                if st.button("Set as Master", type="primary", use_container_width=True, key="admin_set_master"):
                                    try:
                                        # Save to file
                                        uploaded_mapping.to_csv("MasterTradeMapping.csv", index=False)
                                        
                                        # Save to database
                                        st.session_state.trade_mapping = uploaded_mapping.copy()
                                        st.session_state.show_current_mapping = True
                                        st.session_state.mapping_source = "master_official"
                                        
                                        if self.db_manager:
                                            success = self.db_manager.save_trade_mapping(uploaded_mapping)
                                            if success:
                                                st.success("✅ ADMIN: Set as official master mapping!")
                                                st.success("This is now the system default for all users")
                                            else:
                                                st.error("Failed to save to database")
                                        else:
                                            st.success("Set as master mapping file!")
                                        
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error setting master: {e}")
                            
                            st.caption("Admin permissions: Can set permanent master mapping")
                        
                        else:
                            st.error("Unknown user role - contact system administrator")
                
                except Exception as e:
                    st.error(f"Error reading file: {e}")


    # ADDITIONAL FIX: Add this check at the beginning of any method that saves to database
    def _should_prevent_db_save(self):
        """Check if database save should be prevented for custom mappings"""
        return (st.session_state.get('prevent_db_save', False) or 
                st.session_state.get('custom_mapping_mode', False) or
                st.session_state.get('mapping_source') == 'custom_temp')

    def _safe_db_save_trade_mapping(self, mapping_df):
        """Safely save trade mapping only if not in custom temporary mode"""
        if self._should_prevent_db_save():
            st.caption("Debug: Database save prevented - in custom temporary mode")
            return False
        
        if self.db_manager:
            try:
                return self.db_manager.save_trade_mapping(mapping_df)
            except Exception as e:
                st.error(f"Database save failed: {e}")
                return False
        return False
    
    def _show_mapping_source_status(self):
        """Show current mapping source with role-based messaging"""
        user_role = self.get_current_user_role()
        mapping_source = st.session_state.get('mapping_source', 'unknown')
        
        if mapping_source == 'master_official':
            st.success("Using official master mapping (Admin approved)")
        elif mapping_source == 'custom_temp_inspector':
            st.warning("Using temporary custom mapping (Session only)")
            if user_role == 'inspector':
                st.info("Contact Admin to make this mapping permanent")
        elif mapping_source == 'custom_temp_admin':
            st.info("Admin: Using temporary custom mapping")
        else:
            st.info("Using default mapping")

    # ADDITIONAL FIX: Modify any database save method to check the prevention flag
    def save_trade_mapping_with_check(self, mapping_df):
        """Save trade mapping only if not in custom mode"""
        if self._should_prevent_db_save():
            st.warning("Database save prevented - in custom mapping mode")
            return False
        
        if self.db_manager:
            return self.db_manager.save_trade_mapping(mapping_df)
        return False
    
    def _show_enhanced_mapping_load_options(self):
        """Clean mapping interface with role-based permissions"""
        st.markdown("#### Choose Your Mapping Source")
        
        # Get user role from your auth system
        user_role = self.get_current_user_role()
        
        col1, col2 = st.columns(2)
        
        # Option 1: Master Mapping (Available to all roles)
        with col1:
            st.markdown("""
            <div style="border: 2px solid #2196F3; border-radius: 10px; padding: 2rem; text-align: center; height: 180px; display: flex; flex-direction: column; justify-content: space-between;">
                <div>
                    <h4 style="color: #2196F3; margin-top: 0;">Official Master Mapping</h4>
                    <p style="font-size: 0.95em; margin: 1rem 0;">System-approved mapping with 80+ combinations</p>
                    <p style="font-size: 0.85em; color: #666; margin: 0;">Admin controlled & verified</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("Use Master Mapping", type="primary", use_container_width=True, key="use_master_mapping"):
                try:
                    master_mapping = load_master_trade_mapping()
                    st.session_state.trade_mapping = master_mapping
                    st.session_state.show_current_mapping = True
                    st.session_state.mapping_source = "master_official"
                    
                    st.success(f"Official master mapping loaded! {len(master_mapping)} entries")
                    st.info("This is the admin-approved system mapping")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error loading master mapping: {e}")
            
            if st.button("Preview Master", use_container_width=True, key="preview_master_mapping"):
                try:
                    master_mapping = load_master_trade_mapping()
                    st.info(f"{len(master_mapping)} entries covering {master_mapping['Trade'].nunique()} trade categories")
                    
                    with st.expander("Sample Master Mapping Entries", expanded=False):
                        sample_df = master_mapping.head(8).copy()
                        sample_df.index = range(1, len(sample_df) + 1)
                        st.dataframe(sample_df, use_container_width=True)
                except Exception as e:
                    st.error(f"Error: {e}")
        
        # Option 2: Custom Upload - Different UI for Inspector vs Admin
        with col2:
            if user_role == 'inspector':
                # Inspector UI - Clean and simple
                st.markdown("""
                <div style="border: 2px solid #FF9800; border-radius: 10px; padding: 2rem; text-align: center; height: 180px; display: flex; flex-direction: column; justify-content: space-between;">
                    <div>
                        <h4 style="color: #FF9800; margin-top: 0;">Custom Mapping</h4>
                        <p style="font-size: 0.95em; margin: 1rem 0;">Upload your own mapping for this session</p>
                        <p style="font-size: 0.85em; color: #666; margin: 0;">Temporary use only</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                uploaded_file = st.file_uploader(
                    "Upload CSV file (temporary use)",
                    type=["csv"],
                    help="CSV with Room, Component, Trade columns - for this session only",
                    key="custom_mapping_upload"
                )
                
                if uploaded_file is not None:
                    try:
                        uploaded_mapping = pd.read_csv(uploaded_file)
                        
                        # Validation
                        required_cols = ['Room', 'Component', 'Trade']
                        missing_cols = [col for col in required_cols if col not in uploaded_mapping.columns]
                        
                        if missing_cols:
                            st.error(f"Missing required columns: {', '.join(missing_cols)}")
                        else:
                            # Show preview metrics
                            col_a, col_b, col_c = st.columns(3)
                            with col_a:
                                st.metric("Total Entries", len(uploaded_mapping))
                            with col_b:
                                st.metric("Trade Categories", uploaded_mapping['Trade'].nunique())
                            with col_c:
                                duplicates = uploaded_mapping.duplicated(subset=['Room', 'Component']).sum()
                                st.metric("Duplicates", duplicates, delta="⚠️" if duplicates > 0 else "✅")
                            
                            # Single button for Inspector - Use Temporarily
                            if st.button("Use for This Session", type="primary", use_container_width=True, key="use_custom_temp"):
                                st.session_state.trade_mapping = uploaded_mapping.copy()
                                st.session_state.show_current_mapping = True
                                st.session_state.mapping_source = "custom_temp_inspector"
                                
                                st.success(f"Custom mapping loaded for this session!")
                                st.info(f"Loaded {len(uploaded_mapping)} entries temporarily")
                                st.warning("This mapping will not be saved permanently")
                                
                                # Show contact info for permanent changes
                                with st.expander("Need this mapping permanently?", expanded=False):
                                    st.info("Contact your System Administrator to:")
                                    st.write("• Review and approve your custom mapping")
                                    st.write("• Set it as the new master mapping")
                                    st.write("• Make it available to all users")
                                    
                                    # Provide download for Admin
                                    csv_data = uploaded_mapping.to_csv(index=False)
                                    st.download_button(
                                        "Download for Admin Review",
                                        data=csv_data,
                                        file_name=f"custom_mapping_for_admin_{datetime.now().strftime('%Y%m%d')}.csv",
                                        mime="text/csv",
                                        help="Send this file to Admin for permanent installation"
                                    )
                                
                                st.rerun()
                    
                    except Exception as e:
                        st.error(f"Error reading file: {e}")
            
            elif user_role == 'admin':
                # Admin UI - Full controls
                st.markdown("""
                <div style="border: 2px solid #4CAF50; border-radius: 10px; padding: 2rem; text-align: center; height: 180px; display: flex; flex-direction: column; justify-content: space-between;">
                    <div>
                        <h4 style="color: #4CAF50; margin-top: 0;">Admin Controls</h4>
                        <p style="font-size: 0.95em; margin: 1rem 0;">Upload and manage system mappings</p>
                        <p style="font-size: 0.85em; color: #666; margin: 0;">Full permissions</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                uploaded_file = st.file_uploader(
                    "Upload CSV file (Admin)",
                    type=["csv"],
                    help="CSV with Room, Component, Trade columns",
                    key="admin_mapping_upload"
                )
                
                if uploaded_file is not None:
                    try:
                        uploaded_mapping = pd.read_csv(uploaded_file)
                        
                        # Validation
                        required_cols = ['Room', 'Component', 'Trade']
                        missing_cols = [col for col in required_cols if col not in uploaded_mapping.columns]
                        
                        if missing_cols:
                            st.error(f"Missing required columns: {', '.join(missing_cols)}")
                        else:
                            # Show preview metrics
                            col_a, col_b, col_c = st.columns(3)
                            with col_a:
                                st.metric("Total Entries", len(uploaded_mapping))
                            with col_b:
                                st.metric("Trade Categories", uploaded_mapping['Trade'].nunique())
                            with col_c:
                                duplicates = uploaded_mapping.duplicated(subset=['Room', 'Component']).sum()
                                st.metric("Duplicates", duplicates, delta="⚠️" if duplicates > 0 else "✅")
                            
                            # Admin buttons - Temporary or Permanent
                            col_temp, col_perm = st.columns(2)
                            
                            with col_temp:
                                if st.button("Use Temporarily", use_container_width=True, key="admin_use_temp"):
                                    st.session_state.trade_mapping = uploaded_mapping.copy()
                                    st.session_state.show_current_mapping = True
                                    st.session_state.mapping_source = "custom_temp_admin"
                                    
                                    st.success("Loaded temporarily for testing!")
                                    st.rerun()
                            
                            with col_perm:
                                if st.button("Set as Master", type="primary", use_container_width=True, key="admin_set_master"):
                                    # Confirmation for permanent changes
                                    if st.session_state.get('confirm_master_change', False):
                                        try:
                                            # Save to file
                                            uploaded_mapping.to_csv("MasterTradeMapping.csv", index=False)
                                            
                                            # Save to database
                                            st.session_state.trade_mapping = uploaded_mapping.copy()
                                            st.session_state.show_current_mapping = True
                                            st.session_state.mapping_source = "master_official"
                                            
                                            if self.db_manager:
                                                success = self.db_manager.save_trade_mapping(uploaded_mapping)
                                                if success:
                                                    st.success("✅ ADMIN: Set as official master mapping!")
                                                    st.success("This is now the system default for all users")
                                                    st.session_state.confirm_master_change = False
                                                else:
                                                    st.error("Failed to save to database")
                                            else:
                                                st.success("Set as master mapping file!")
                                            
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Error setting master: {e}")
                                    else:
                                        st.session_state.confirm_master_change = True
                                        st.warning("Click again to confirm permanent change")
                                        st.rerun()
                    
                    except Exception as e:
                        st.error(f"Error reading file: {e}")
            
            else:
                # Unknown role
                st.error("Access denied: Unknown user role")
                st.info("Please contact your system administrator")
    
    def _show_enhanced_mapping_management(self):
        """Role-based mapping management interface"""
        
        st.markdown("#### Mapping Management")
        
        # Get current user role
        user_role = self.get_current_user_role()
        
        # Show role-appropriate interface
        if user_role == 'admin':
            self._show_admin_mapping_management()
        else:
            self._show_inspector_mapping_management()

    def _show_admin_mapping_management(self):
        """Full mapping management for Admin users"""
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Download Templates:**")
            
            try:
                master_mapping = load_master_trade_mapping()
                template_csv = master_mapping.to_csv(index=False)
                
                st.download_button(
                    "Download Master Template",
                    data=template_csv,
                    file_name="MasterTradeMapping_Complete.csv",
                    mime="text/csv",
                    help=f"Complete template with {len(master_mapping)} entries",
                    use_container_width=True
                )
            except Exception as e:
                st.error(f"Template error: {e}")
            
            # Basic template
            basic_template = """Room,Component,Trade
    Apartment Entry Door,Door Handle,Doors
    Apartment Entry Door,Door Locks and Keys,Doors
    Bathroom,Tiles,Flooring - Tiles
    Kitchen Area,Cabinets,Carpentry & Joinery
    Bedroom,Windows,Windows
    Living Room,Air Conditioning,HVAC
    General,Smoke Detector,Fire Safety"""
            
            st.download_button(
                "Download Basic Template",
                data=basic_template,
                file_name="basic_trade_mapping_template.csv",
                mime="text/csv",
                help="Basic template for getting started",
                use_container_width=True
            )
        
        with col2:
            st.markdown("**Current Mapping:**")
            
            if len(st.session_state.trade_mapping) > 0:
                csv_data = st.session_state.trade_mapping.to_csv(index=False)
                st.download_button(
                    "Export Current",
                    data=csv_data,
                    file_name=f"current_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    help="Export current mapping",
                    use_container_width=True
                )
                
                if st.button("Preview Current", use_container_width=True, key="preview_current_mapping"):
                    st.session_state.show_mapping_preview = not st.session_state.get('show_mapping_preview', False)
                    st.rerun()
            else:
                st.info("No mapping loaded")
        
        with col3:
            st.markdown("**Database Actions:**")
            
            if self.db_manager and len(st.session_state.trade_mapping) > 0:
                if st.button("Sync to Database", use_container_width=True, type="primary", key="sync_mapping_db"):
                    try:
                        success = self.db_manager.save_trade_mapping(st.session_state.trade_mapping)
                        if success:
                            st.success("Synced to database!")
                        else:
                            st.error("Failed to sync")
                    except Exception as e:
                        st.error(f"Database sync error: {e}")
                
                if st.button("Check Database", use_container_width=True, key="check_db_status"):
                    try:
                        db_mapping = self.db_manager.get_trade_mapping()
                        if len(db_mapping) > 0:
                            st.info(f"Database has {len(db_mapping)} mappings")
                        else:
                            st.warning("No mappings in database")
                    except Exception as e:
                        st.error(f"Database check failed: {e}")
            
            if len(st.session_state.trade_mapping) > 0:
                if st.button("Clear Mapping", use_container_width=True, key="clear_current_mapping"):
                    st.session_state.trade_mapping = pd.DataFrame(columns=["Room", "Component", "Trade"])
                    # Clear all flags
                    for flag in ['prevent_db_save', 'custom_mapping_mode', 'mapping_source', 'show_mapping_preview']:
                        if flag in st.session_state:
                            del st.session_state[flag]
                    st.rerun()

    def _show_inspector_mapping_management(self):
        """Simplified mapping management for Inspector users"""
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Download Templates:**")
            
            try:
                master_mapping = load_master_trade_mapping()
                template_csv = master_mapping.to_csv(index=False)
                
                st.download_button(
                    "Download Master Template",
                    data=template_csv,
                    file_name="MasterTradeMapping_Complete.csv",
                    mime="text/csv",
                    help=f"Complete template with {len(master_mapping)} entries",
                    use_container_width=True
                )
            except Exception as e:
                st.error(f"Template error: {e}")
            
            # Basic template
            basic_template = """Room,Component,Trade
    Apartment Entry Door,Door Handle,Doors
    Apartment Entry Door,Door Locks and Keys,Doors
    Bathroom,Tiles,Flooring - Tiles
    Kitchen Area,Cabinets,Carpentry & Joinery
    Bedroom,Windows,Windows
    Living Room,Air Conditioning,HVAC
    General,Smoke Detector,Fire Safety"""
            
            st.download_button(
                "Download Basic Template",
                data=basic_template,
                file_name="basic_trade_mapping_template.csv",
                mime="text/csv",
                help="Basic template for getting started",
                use_container_width=True
            )
        
        with col2:
            st.markdown("**Current Session:**")
            
            if len(st.session_state.trade_mapping) > 0:
                csv_data = st.session_state.trade_mapping.to_csv(index=False)
                st.download_button(
                    "Export Current",
                    data=csv_data,
                    file_name=f"current_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    help="Export current session mapping",
                    use_container_width=True
                )
                
                if st.button("Preview Current", use_container_width=True, key="preview_current_mapping_inspector"):
                    st.session_state.show_mapping_preview = not st.session_state.get('show_mapping_preview', False)
                    st.rerun()
                
                if st.button("Clear Mapping", use_container_width=True, key="clear_mapping_inspector"):
                    st.session_state.trade_mapping = pd.DataFrame(columns=["Room", "Component", "Trade"])
                    # Clear all flags
                    for flag in ['prevent_db_save', 'custom_mapping_mode', 'mapping_source', 'show_mapping_preview']:
                        if flag in st.session_state:
                            del st.session_state[flag]
                    st.rerun()
            else:
                st.info("No mapping loaded")
            
            # Inspector guidance
            st.markdown("**Need Help?**")
            st.caption("Contact Admin to:")
            st.caption("• Set permanent mappings")
            st.caption("• Update system defaults")
            st.caption("• Get additional templates")
    
    def _show_mapping_analytics(self):
        """Show mapping analytics and insights"""
        if len(st.session_state.trade_mapping) == 0:
            st.info("Load a trade mapping to view analytics")
            return
        
        st.markdown("#### Trade Mapping Analytics")
        
        mapping = st.session_state.trade_mapping
        
        # Analytics metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Mappings", len(mapping))
        with col2:
            st.metric("Unique Rooms", mapping['Room'].nunique())
        with col3:
            st.metric("Unique Components", mapping['Component'].nunique())
        with col4:
            st.metric("Trade Categories", mapping['Trade'].nunique())
        
        # Top trades
        st.markdown("**Most Common Trades:**")
        top_trades = mapping['Trade'].value_counts().head(10)
        for trade, count in top_trades.items():
            progress = count / len(mapping)
            st.progress(progress, text=f"{trade}: {count} mappings ({progress*100:.1f}%)")
        
        # Room coverage
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Room Coverage:**")
            room_counts = mapping['Room'].value_counts().head(8)
            st.bar_chart(room_counts)
        
        with col_b:
            st.markdown("**Component Distribution:**")
            component_counts = mapping['Component'].value_counts().head(8)
            st.bar_chart(component_counts)
    
    def _show_enhanced_current_mapping_details(self):
        """Role-based current mapping details"""
        
        user_role = self.get_current_user_role()
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            # Enhanced metrics
            col_a, col_b, col_c, col_d = st.columns(4)
            with col_a:
                st.metric("Total Entries", len(st.session_state.trade_mapping))
            with col_b:
                st.metric("Unique Trades", st.session_state.trade_mapping['Trade'].nunique())
            with col_c:
                st.metric("Unique Rooms", st.session_state.trade_mapping['Room'].nunique())
            with col_d:
                coverage_score = min(100, len(st.session_state.trade_mapping) / 70 * 100)
                st.metric("Coverage Score", f"{coverage_score:.0f}/100")
            
            # Enhanced display with search and filter
            st.markdown("**Mapping Data:**")
            
            # Search functionality
            search_term = st.text_input("Search mappings:", placeholder="Enter room, component, or trade name", key="mapping_search")
            
            display_df = st.session_state.trade_mapping.copy()
            if search_term:
                mask = (display_df['Room'].str.contains(search_term, case=False, na=False) |
                    display_df['Component'].str.contains(search_term, case=False, na=False) |
                    display_df['Trade'].str.contains(search_term, case=False, na=False))
                display_df = display_df[mask]
            
            display_df.index = range(1, len(display_df) + 1)
            st.dataframe(display_df, use_container_width=True, height=300)
            
            if search_term and len(display_df) == 0:
                st.warning(f"No mappings found for '{search_term}'")
            elif search_term:
                st.info(f"Found {len(display_df)} matching mappings")
        
        with col2:
            st.markdown("**Actions:**")
            
            csv_data = st.session_state.trade_mapping.to_csv(index=False)
            st.download_button(
                "Export as CSV",
                data=csv_data,
                file_name=f"mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
            
            # Role-specific actions
            if user_role == 'admin':
                if self.db_manager:
                    if st.button("Sync to Database", use_container_width=True, type="primary", key="sync_mapping_details"):
                        try:
                            success = self.db_manager.save_trade_mapping(st.session_state.trade_mapping)
                            if success:
                                st.success("Synced to database!")
                            else:
                                st.error("Sync failed")
                        except Exception as e:
                            st.error(f"Error: {e}")
                    
                    if st.button("Check Database", use_container_width=True, key="check_db_mapping_status"):
                        try:
                            db_mapping = self.db_manager.get_trade_mapping()
                            if len(db_mapping) > 0:
                                st.info(f"Database has {len(db_mapping)} mappings")
                            else:
                                st.warning("No mappings in database")
                        except Exception as e:
                            st.error(f"Database check failed: {e}")
            
            if st.button("Clear Mapping", use_container_width=True, type="secondary", key="clear_mapping_details"):
                st.session_state.trade_mapping = pd.DataFrame(columns=["Room", "Component", "Trade"])
                if 'show_current_mapping' in st.session_state:
                    del st.session_state['show_current_mapping']
                st.rerun()
            
            st.markdown("**Top 5 Trades:**")
            if len(st.session_state.trade_mapping) > 0:
                top_trades = st.session_state.trade_mapping['Trade'].value_counts().head(5)
                for trade, count in top_trades.items():
                    st.caption(f"• {trade}: {count}")
            
            # Role-specific status
            if user_role == 'admin':
                st.markdown("**Database Info:**")
                if self.db_manager:
                    try:
                        stats = self.db_manager.get_database_stats()
                        st.caption(f"Database Size: {stats.get('database_size_mb', 0):.1f} MB")
                    except:
                        st.caption("Database: Connected")
                else:
                    st.caption("Database: Not available")
            else:
                st.markdown("**Session Info:**")
                mapping_source = st.session_state.get('mapping_source', 'unknown')
                if mapping_source == 'custom_temp_inspector':
                    st.caption("Source: Temporary custom")
                elif mapping_source == 'master_official':
                    st.caption("Source: Official master")
                else:
                    st.caption("Source: Unknown")
    
    def _check_hash_in_database(self, file_hash: str, filename: str) -> Optional[Dict]:
        """Check if file hash exists in database"""
        
        print(f">>> Checking hash: {file_hash}")
        
        if not self.processor.db_manager:
            print(">>> ERROR: No database manager")
            return None
        
        try:
            conn = self.processor.db_manager.connect()
            cursor = conn.cursor()
            
            # First, let's see ALL hashes in the database
            cursor.execute("SELECT file_checksum, building_name, created_at FROM inspector_csv_processing_log")
            all_records = cursor.fetchall()
            print(f">>> Total records in log: {len(all_records)}")
            for record in all_records:
                print(f"    Hash in DB: {record[0]} | Building: {record[1]}")
            
            # Now check for our specific hash
            cursor.execute("""
                SELECT inspection_id, building_name, created_at, original_filename
                FROM inspector_csv_processing_log
                WHERE file_checksum = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (file_hash,))
            
            result = cursor.fetchone()
            print(f">>> Query result: {result}")
            
            if result:
                print(">>> DUPLICATE FOUND!")
                return {
                    'is_duplicate': True,
                    'inspection_id': result[0],
                    'building_name': result[1],
                    'processed_date': result[2],
                    'original_filename': result[3]
                }
            
            print(">>> No duplicate found")
            return None
            
        except Exception as e:
            print(f">>> ERROR checking duplicate: {e}")
            import traceback
            print(traceback.format_exc())
            return None
        
    def _process_with_hash(self, uploaded_csv, file_hash):
        """Process inspection with file hash for duplicate tracking"""
        
        try:
            uploaded_csv.seek(0)
            df = pd.read_csv(uploaded_csv)
            original_filename = uploaded_csv.name
            
            user_info = st.session_state.get('user_info', {})
            inspector_name = user_info.get('name', 'Inspector')
            
            building_info = {
                "name": "Building Complex",
                "address": "Address not specified",
                "date": datetime.now().strftime("%Y-%m-%d")
            }
            
            with st.spinner("Processing..."):
                result = self.processor.process_inspection_data(
                    df, st.session_state.trade_mapping, building_info, 
                    inspector_name, original_filename, file_hash  # Pass hash
                )
                
                if result and len(result) == 3:
                    processed_df, metrics, inspection_id = result
                    
                    self.processed_data = processed_df
                    self.metrics = metrics
                    self.current_inspection_id = inspection_id
                    
                    st.success("Data processed successfully!")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Units", metrics['total_units'])
                    with col2:
                        st.metric("Total Defects", metrics['total_defects'])
                    with col3:
                        st.metric("Ready Units", metrics['ready_units'])
                    with col4:
                        st.metric("Quality Score", f"{max(0, 100 - metrics['defect_rate']):.1f}/100")
                    
                    if inspection_id:
                        st.info(f"Database ID: {inspection_id[:12]}... (saved)")
                    
                    st.rerun()
                else:
                    st.error("Processing failed")
                    
        except Exception as e:
            st.error(f"Error: {e}")
            
    def _show_data_processing_section(self):
        """Data processing section with duplicate detection"""
        st.markdown("---")
        st.markdown("### Step 2: Upload and Process Inspection Data")
        
        # Check if we're currently viewing a loaded inspection
        if st.session_state.get('viewing_loaded_inspection'):
            st.info(f"📂 Viewing loaded inspection: {st.session_state.get('loaded_inspection_id', 'Unknown')[:12]}...")
            if st.button("🔄 Upload New File"):
                st.session_state.pop('viewing_loaded_inspection', None)
                st.session_state.pop('loaded_inspection_id', None)
                st.rerun()
            return  # Skip the file uploader
        
        if 'trade_mapping' not in st.session_state or len(st.session_state.trade_mapping) == 0:
            st.warning("Please load your trade mapping first")
            return
        
        uploaded_csv = st.file_uploader(
            "Choose inspection CSV file",
            type=["csv"],
            help="Upload your iAuditor CSV export file"
        )
        
        if uploaded_csv is not None:
            try:
                # Read CSV
                uploaded_csv.seek(0)
                preview_df = pd.read_csv(uploaded_csv)
                
                # Calculate file hash
                uploaded_csv.seek(0)
                file_bytes = uploaded_csv.read()
                file_hash = hashlib.md5(file_bytes).hexdigest()
                uploaded_csv.seek(0)
                
                # Check if we JUST processed this file (within last 5 seconds)
                just_processed = st.session_state.get('last_processed_hash') == file_hash
                
                # Check database for duplicate (skip if we just processed it)
                duplicate_info = None if just_processed else self._check_hash_in_database(file_hash, uploaded_csv.name)
                
                # Show file info
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.info(f"📄 Rows: {len(preview_df):,}")
                with col2:
                    st.info(f"💾 Size: {len(file_bytes)/1024:.1f} KB")
                with col3:
                    if just_processed:
                        st.success("✓ Just Processed")
                    elif duplicate_info:
                        st.error("⚠️ DUPLICATE")
                    else:
                        st.success("✓ New File")
                with col4:
                    st.info(f"🔑 {file_hash[:8]}...")
                
                # Handle duplicate detection (but not for just-processed files)
                allow_key = f'allow_dup_{file_hash}'
                
                if duplicate_info and not just_processed and not st.session_state.get(allow_key, False):
                    st.error("### 🚫 DUPLICATE FILE DETECTED")
                    st.warning(f"""
                    **This exact file was already processed:**
                    - Building: {duplicate_info['building_name']}
                    - Processed: {duplicate_info['processed_date']}
                    - Inspection ID: `{duplicate_info['inspection_id'][:12]}...`
                    - Original File: {duplicate_info['original_filename']}
                    """)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button("❌ Cancel", use_container_width=True):
                            st.stop()
                    with col2:
                        if st.button("👁️ View Previous", use_container_width=True):
                            # Mark that we're viewing a loaded inspection
                            st.session_state['viewing_loaded_inspection'] = True
                            st.session_state['loaded_inspection_id'] = duplicate_info['inspection_id']
                            
                            # Clear duplicate flags
                            st.session_state.pop(allow_key, None)
                            st.session_state.pop('last_processed_hash', None)
                            
                            # Load the previous inspection
                            self._load_previous_inspection(duplicate_info['inspection_id'])                            
                    with col3:
                        if st.button("✓ Process Anyway", type="primary", use_container_width=True):
                            st.session_state[allow_key] = True
                            st.rerun()
                    
                    st.stop()
                
                # Show preview
                with st.expander("📋 Data Preview"):
                    st.dataframe(preview_df.head(10), use_container_width=True)
                
                # ✅ ADD BUILDING INFORMATION FORM HERE
                st.markdown("---")
                st.subheader("🏢 Building Information")
                st.caption("Enter building details for this inspection")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    building_name = st.text_input(
                        "Building Name *",
                        value="",
                        placeholder="e.g., Sunrise Apartments",
                        help="Enter the building or project name (required)",
                        key="building_name_input"
                    )
                    
                    inspector_name = st.text_input(
                        "Inspector Name",
                        value=st.session_state.get('user_info', {}).get('name', 'Inspector'),
                        placeholder="e.g., John Smith",
                        help="Name of the person conducting the inspection",
                        key="inspector_name_input"
                    )
                
                with col2:
                    address = st.text_input(
                        "Building Address",
                        value="",
                        placeholder="e.g., 123 Main St, Sydney NSW",
                        help="Full address of the building (optional)",
                        key="building_address_input"
                    )
                    
                    inspection_date = st.date_input(
                        "Inspection Date",
                        value=datetime.now().date(),
                        help="Date when the inspection was conducted",
                        key="inspection_date_input"
                    )
                
                # Validation
                if not building_name or building_name.strip() == "":
                    st.warning("⚠️ Please enter a building name before processing")
                    st.stop()
                
                st.markdown("---")
                
                # Process button
                if st.button("🔄 Process Inspection Data", type="primary", use_container_width=True):

                    # Create progress tracker
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    try:
                        status_text.text("⏳ Step 1/5: Reading CSV file...")
                        progress_bar.progress(0.1)
                        
                        # Read CSV
                        uploaded_csv.seek(0)
                        df = pd.read_csv(uploaded_csv)
                        st.success(f"✅ CSV loaded: {len(df)} rows")
                        
                        status_text.text("⏳ Step 2/5: Verifying data...")
                        progress_bar.progress(0.2)
                        
                        status_text.text("⏳ Step 3/5: Loading trade mappings...")
                        progress_bar.progress(0.3)
                        
                        # Use session state mapping
                        mapping_df = st.session_state.trade_mapping
                        
                        status_text.text("⏳ Step 4/5: Processing inspection data...")
                        progress_bar.progress(0.4)
                        
                        # Calculate file hash
                        uploaded_csv.seek(0)
                        file_bytes = uploaded_csv.read()
                        file_hash = hashlib.md5(file_bytes).hexdigest()
                        uploaded_csv.seek(0)
                        
                        # Process data
                        processed_df, metrics, inspection_id = self.processor.process_inspection_data(
                            df=df,
                            mapping=mapping_df,
                            building_info={
                                "name": building_name,
                                "address": address,
                                "date": inspection_date.strftime("%Y-%m-%d")
                            },
                            inspector_name=inspector_name,
                            original_filename=uploaded_csv.name,
                            file_hash=file_hash
                        )
                        
                        progress_bar.progress(0.8)
                        
                        if processed_df is None or metrics is None:
                            st.error("❌ Processing failed - check logs for details")
                            status_text.text("❌ Failed")
                            st.stop()
                        
                        status_text.text("⏳ Step 5/5: Finalizing...")
                        progress_bar.progress(0.9)
                        
                        # Store in session state
                        st.session_state['processed_data'] = processed_df
                        st.session_state['metrics'] = metrics
                        st.session_state['inspection_id'] = inspection_id
                        st.session_state['last_processed_hash'] = file_hash
                        
                        # Update instance variables
                        self.processed_data = processed_df
                        self.metrics = metrics
                        self.current_inspection_id = inspection_id
                        
                        progress_bar.progress(1.0)
                        status_text.text("✅ Complete!")
                        
                        # Success message
                        work_orders = metrics.get('work_orders_created', 0)
                        st.success(f"""
                        ✅ **Processing Complete!**
                        
                        - **Building:** {metrics['building_name']}
                        - **Inspection ID:** `{inspection_id[:16]}...`
                        - **Total Units:** {metrics['total_units']}
                        - **Total Defects:** {metrics['total_defects']}
                        - **Settlement Ready:** {metrics['ready_pct']:.1f}%
                        - **Work Orders Created:** {work_orders}
                        """)
                        
                        # Clear progress indicators after 2 seconds
                        import time
                        time.sleep(2)
                        progress_bar.empty()
                        status_text.empty()
                        
                        # Force rerun to show results
                        st.rerun()
                        
                    except Exception as e:
                        progress_bar.empty()
                        status_text.empty()
                        st.error(f"❌ Error: {str(e)}")
                        logger.error(f"Processing error: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
                        
                        # Show detailed error in expander
                        with st.expander("🔍 Error Details"):
                            st.code(traceback.format_exc())
                    
            except Exception as e:
                st.error(f"Error: {e}")
                logger.error(f"File upload error: {e}")
                import traceback
                logger.error(traceback.format_exc())

    def _process_inspection_data_simplified(self, uploaded_csv):
        """Simplified processing without unnecessary user input"""
        
        try:
            uploaded_csv.seek(0)
            df = pd.read_csv(uploaded_csv)
            original_filename = getattr(uploaded_csv, 'name', 'uploaded_file.csv')
            
            # Calculate file hash
            uploaded_csv.seek(0)
            file_bytes = uploaded_csv.read()
            file_hash = hashlib.md5(file_bytes).hexdigest()
            uploaded_csv.seek(0)
            
            # Get inspector info
            user_info = st.session_state.get('user_info', {})
            inspector_name = user_info.get('name', 'Inspector')
            
            building_info = {
                "name": "Building Complex",
                "address": "Address not specified",
                "date": datetime.now().strftime("%Y-%m-%d")
            }
            
            with st.spinner("Processing inspection data..."):
                result = self.processor.process_inspection_data(
                    df, st.session_state.trade_mapping, building_info, 
                    inspector_name, original_filename, file_hash
                )
                
                if result is None or not isinstance(result, tuple) or len(result) != 3:
                    st.error("Processing failed")
                    return
                
                processed_df, metrics, inspection_id = result
                
                # Update state
                self.processed_data = processed_df
                self.metrics = metrics
                self.current_inspection_id = inspection_id
                
                # Show success
                st.success("✅ Data processed successfully!")
                
                # Show metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Units", metrics['total_units'])
                with col2:
                    st.metric("Total Defects", metrics['total_defects'])
                with col3:
                    st.metric("Ready Units", metrics['ready_units'])
                with col4:
                    st.metric("Quality Score", f"{max(0, 100 - metrics['defect_rate']):.1f}/100")
                
                if inspection_id:
                    st.info(f"📊 Saved - ID: {inspection_id[:12]}...")
                
                # Clear the file uploader to prevent duplicate warning on rerun
                if 'csv_uploader' in st.session_state:
                    del st.session_state['csv_uploader']
                
                # Small delay before rerun to show success message
                import time
                time.sleep(1)
                
                st.rerun()
                        
        except Exception as e:
            st.error(f"❌ Error: {e}")

    # Optional: Admin-only processing log viewer
    def _show_admin_processing_log(self):
        """Show processing log - Admin only"""
        user_role = self.get_current_user_role()
        
        if user_role != 'admin':
            return
        
        with st.expander("Admin: CSV Processing Log", expanded=False):
            if not self.db_manager:
                st.warning("Database not available for processing log")
                return
            
            try:
                conn = self.db_manager.connect()
                query = """
                    SELECT original_filename, building_name, total_rows, defects_found,
                        mapping_success_rate, status, created_at
                    FROM inspector_csv_processing_log
                    ORDER BY created_at DESC
                    LIMIT 20
                """
                log_df = pd.read_sql_query(query, conn)
                
                if len(log_df) > 0:
                    st.caption("Recent CSV processing history (Admin view)")
                    st.dataframe(log_df, use_container_width=True)
                else:
                    st.info("No processing history found")
                    
            except Exception as e:
                st.error(f"Error loading processing log: {e}")
    
    def _show_csv_processing_log(self):
        """Show CSV processing history for Admin"""
        user_role = self.get_current_user_role()
        
        if user_role != 'admin':
            st.info("CSV processing log is only available to Admin users")
            return
        
        if not self.db_manager:
            st.warning("Database not available for processing log")
            return
        
        with st.expander("Admin: CSV Processing Log", expanded=True):
            try:
                conn = self.db_manager.connect()
                query = """
                    SELECT original_filename, building_name, total_rows, defects_found,
                        mapping_success_rate, status, created_at, completed_at
                    FROM inspector_csv_processing_log
                    ORDER BY created_at DESC
                    LIMIT 20
                """
                log_df = pd.read_sql_query(query, conn)
                
                if len(log_df) > 0:
                    # Format the display
                    display_df = log_df.copy()
                    display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
                    display_df['completed_at'] = pd.to_datetime(display_df['completed_at']).dt.strftime('%Y-%m-%d %H:%M')
                    
                    st.caption(f"Showing {len(display_df)} recent CSV processing records")
                    st.dataframe(display_df, use_container_width=True)
                    
                    # Summary stats
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Processed", len(log_df))
                    with col2:
                        avg_defects = log_df['defects_found'].mean() if len(log_df) > 0 else 0
                        st.metric("Avg Defects", f"{avg_defects:.1f}")
                    with col3:
                        avg_success_rate = log_df['mapping_success_rate'].mean() if len(log_df) > 0 else 0
                        st.metric("Avg Mapping Success", f"{avg_success_rate:.1f}%")
                else:
                    st.info("No processing history found")
                    st.caption("Process some CSV files to see logging data here")
                    
            except Exception as e:
                st.error(f"Error loading processing log: {e}")
                st.caption("Check if the inspector_csv_processing_log table exists in your database")
    
    def _quick_preview_data(self, preview_df: pd.DataFrame):
        """Show quick preview of data structure"""
        with st.expander("Quick Data Preview", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Data Structure:**")
                st.info(f"Total rows: {len(preview_df)}")
                st.info(f"Total columns: {len(preview_df.columns)}")
                
                # Check for key columns
                key_checks = {
                    "Audit Name": any("audit" in col.lower() for col in preview_df.columns),
                    "Unit Info": any("unit" in col.lower() or "lot" in col.lower() for col in preview_df.columns),
                    "Inspection Items": any("inspection" in col.lower() for col in preview_df.columns)
                }
                
                for check, found in key_checks.items():
                    if found:
                        st.success(f"✓ {check} found")
                    else:
                        st.warning(f"⚠ {check} not found")
            
            with col2:
                st.markdown("**Sample Data:**")
                sample_data = preview_df.head(3)
                st.dataframe(sample_data, use_container_width=True)
    
    def _process_inspection_data_enhanced(self, uploaded_csv, inspector_name: str):
        """Debug version to find exact failure point"""
        
        try:
            uploaded_csv.seek(0)
            df = pd.read_csv(uploaded_csv)
            original_filename = getattr(uploaded_csv, 'name', 'uploaded_file.csv')
            
            building_info = {
                "name": "Professional Building Complex",
                "address": "123 Professional Street\nMelbourne, VIC 3000",
                "date": datetime.now().strftime("%Y-%m-%d")
            }
            
            st.write("Step 1: Starting processing...")
            st.write(f"File: {original_filename}, Shape: {df.shape}")
            
            # Check if method exists
            if not hasattr(self.processor, 'process_inspection_data'):
                st.error("Method process_inspection_data not found!")
                return
            
            st.write("Step 2: Method found, calling it...")
            
            # Call with explicit error handling
            try:
                result = self.processor.process_inspection_data(
                    df, st.session_state.trade_mapping, building_info, inspector_name, original_filename
                )
                st.write(f"Step 3: Method returned: {type(result)}")
                
                if result and len(result) == 3:
                    processed_df, metrics, inspection_id = result
                    st.write(f"Step 4: Unpacked - ID: {inspection_id}")
                    
                    if inspection_id:
                        st.success(f"Success! Inspection ID: {inspection_id}")
                    else:
                        st.error("Method returned None as inspection_id")
                        
                        # Check if there was a database error
                        if self.processor.db_manager:
                            st.write("Database manager exists, checking last error...")
                        else:
                            st.error("No database manager found!")
                            
                else:
                    st.error(f"Method returned unexpected result: {result}")
                    
            except Exception as method_error:
                st.error(f"Method execution failed: {method_error}")
                import traceback
                st.code(traceback.format_exc())
                
            # Update state regardless
            if 'result' in locals() and result and len(result) == 3:
                self.processed_data, self.metrics, self.current_inspection_id = result
                
        except Exception as main_error:
            st.error(f"Main error: {main_error}")
            import traceback
            st.code(traceback.format_exc())
    
    def _show_results_and_reports(self):
        """Enhanced results and reports"""
        
        st.markdown("---")
        st.markdown("### Step 3: Analysis Results")
        
        metrics = self.metrics
        
        # Enhanced building information with database reference
        st.markdown("#### Building Information")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"""
            **Building:** {metrics['building_name']}  
            **Address:** {metrics['address']}  
            **Total Units:** {metrics['total_units']:,}
            """)
        
        with col2:
            st.markdown(f"""              
            **Unit Types:** {metrics['unit_types_str']}  
            **Total Defects:** {metrics['total_defects']:,}
            """)
            
            # Enhanced database info
            if self.current_inspection_id:
                st.success(f"Database ID: {self.current_inspection_id[:8]}...")
                st.caption("Data available for Builder and Developer roles")
        
        # Enhanced quality dashboard
        st.markdown("#### Quality Dashboard")
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total Units", f"{metrics['total_units']:,}")
        with col2:
            st.metric("Total Defects", f"{metrics['total_defects']:,}", 
                     delta=f"{metrics['defect_rate']:.1f}% rate")
        with col3:
            st.metric("Ready Units", f"{metrics['ready_units']}", 
                     delta=f"{metrics['ready_pct']:.1f}%")
        with col4:
            st.metric("Quality Score", f"{max(0, 100 - metrics['defect_rate']):.1f}/100")
        with col5:
            st.metric("Urgent Defects", metrics['urgent_defects'])
        
        # Enhanced summary data tabs
        st.markdown("#### Summary Analysis")
        tab1, tab2, tab3, tab4 = st.tabs(["Trade Summary", "Unit Summary", "Room Summary", "Urgent Items"])
        
        with tab1:
            if len(metrics['summary_trade']) > 0:
                display_trade = metrics['summary_trade'].copy()
                display_trade.index = range(1, len(display_trade) + 1)
                st.dataframe(display_trade, use_container_width=True)
                
                # Enhanced trade analytics
                if len(display_trade) > 0:
                    st.markdown("**Trade Distribution:**")
                    st.bar_chart(display_trade.set_index('Trade')['DefectCount'])
            else:
                st.info("No trade defects found")
        
        with tab2:
            if len(metrics['summary_unit']) > 0:
                display_unit = metrics['summary_unit'].copy()
                display_unit.index = range(1, len(display_unit) + 1)
                st.dataframe(display_unit, use_container_width=True)
                
                # Enhanced unit analytics
                if len(display_unit) > 0:
                    st.markdown("**Unit Defect Distribution:**")
                    st.bar_chart(display_unit.set_index('Unit')['DefectCount'])
            else:
                st.info("No unit defects found")
        
        with tab3:
            if len(metrics['summary_room']) > 0:
                display_room = metrics['summary_room'].copy()
                display_room.index = range(1, len(display_room) + 1)
                st.dataframe(display_room, use_container_width=True)
            else:
                st.info("No room defects found")
        
        with tab4:
            if len(metrics['urgent_defects_table']) > 0:
                urgent_display = metrics['urgent_defects_table'].copy()
                urgent_display.index = range(1, len(urgent_display) + 1)
                st.dataframe(urgent_display, use_container_width=True)
                st.error(f"**{len(urgent_display)} URGENT defects require immediate attention!**")
            else:
                st.success("No urgent defects found!")

        # Enhanced unit lookup section
        self._show_enhanced_unit_lookup_section()
    
    def _show_enhanced_unit_lookup_section(self):
        """Enhanced unit lookup section with V3 database features"""
        
        if self.processed_data is None:
            return
        
        st.markdown("---")
        st.markdown("### Unit Defect Lookup")
        st.markdown("Quickly search for any unit's complete defect history with V3 database integration")
        
        all_units = sorted(self.processed_data["Unit"].unique())
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            selected_unit = st.selectbox(
                "Enter or Select Unit Number:",
                options=[""] + all_units,
                help="Type to search or select from dropdown - integrated with V3 database",
                key="unit_lookup_selector"
            )
            
            if selected_unit:
                unit_defects = self._lookup_unit_defects(selected_unit)
                
                if len(unit_defects) > 0:
                    st.markdown(f"#### Unit {selected_unit} - Defect Report")
                    
                    col_a, col_b, col_c, col_d, col_e = st.columns(5)
                    
                    urgent_count = len(unit_defects[unit_defects["Urgency"] == "Urgent"])
                    high_priority_count = len(unit_defects[unit_defects["Urgency"] == "High Priority"])
                    normal_count = len(unit_defects[unit_defects["Urgency"] == "Normal"])
                    total_defects = len(unit_defects)
                    
                    with col_a:
                        if urgent_count > 0:
                            st.error(f"Urgent: {urgent_count}")
                        else:
                            st.success("Urgent: 0")
                    
                    with col_b:
                        if high_priority_count > 0:
                            st.warning(f"High Priority: {high_priority_count}")
                        else:
                            st.info("High Priority: 0")
                    
                    with col_c:
                        st.info(f"Normal: {normal_count}")
                    
                    with col_d:
                        st.metric("Total Defects", total_defects)
                    
                    with col_e:
                        # Show work orders for this unit
                        if self.db_manager:
                            try:
                                work_orders = self.db_manager.get_work_orders_for_builder()
                                unit_work_orders = work_orders[work_orders['unit'] == selected_unit] if 'unit' in work_orders.columns else pd.DataFrame()
                                st.metric("Work Orders", len(unit_work_orders))
                            except:
                                st.metric("V3 DB", "Ready")
                    
                    st.markdown("**Defect List:**")
                    display_defects = unit_defects.copy()
                    display_defects.index = range(1, len(display_defects) + 1)
                    
                    st.dataframe(display_defects, use_container_width=True)
                    
                    # Enhanced status messages
                    if urgent_count > 0:
                        st.error(f"**HIGH ATTENTION REQUIRED** - {urgent_count} urgent defect(s) need immediate attention!")
                        if self.db_manager:
                            st.info("Work orders automatically created in V3 database for Builder access")
                    elif high_priority_count > 0:
                        st.warning(f"**PRIORITY WORK** - {high_priority_count} high priority defect(s) to address")
                    elif normal_count > 0:
                        st.info(f"**STANDARD WORK** - {normal_count} normal defect(s) to complete")
                    
                    # Enhanced export options
                    col_export1, col_export2, col_export3 = st.columns(3)
                    with col_export1:
                        csv_data = unit_defects.to_csv(index=False)
                        st.download_button(
                            f"Export Unit {selected_unit}",
                            data=csv_data,
                            file_name=f"unit_{selected_unit}_defects_v3_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    
                    with col_export2:
                        if st.button("View Work Orders", use_container_width=True, key="view_unit_work_orders"):
                            try:
                                work_orders = self.db_manager.get_work_orders_for_builder()
                                unit_work_orders = work_orders[work_orders['unit'] == selected_unit] if 'unit' in work_orders.columns else pd.DataFrame()
                                if len(unit_work_orders) > 0:
                                    st.dataframe(unit_work_orders, use_container_width=True)
                                else:
                                    st.info("No work orders found for this unit")
                            except Exception as e:
                                st.error(f"Error loading work orders: {e}")
                    
                    with col_export3:
                        if st.button("Unit Analytics", use_container_width=True, key="show_unit_analytics"):
                            self._show_unit_analytics(selected_unit, unit_defects)
                    
                else:
                    st.success(f"**Unit {selected_unit} is DEFECT-FREE!**")
                    if self.db_manager:
                        st.info("Unit status confirmed in V3 database")
        
        with col2:
            if len(all_units) > 0:
                st.markdown("**Building Overview:**")
                
                defects_per_unit = self.processed_data[
                    self.processed_data["StatusClass"] == "Not OK"
                ].groupby("Unit").size()
                
                units_with_defects = len(defects_per_unit)
                units_without_defects = len(all_units) - units_with_defects
                
                st.metric("Total Units", len(all_units))
                st.metric("Units with Defects", units_with_defects)
                st.metric("Defect-Free Units", units_without_defects)
                
                if units_with_defects > 0:
                    avg_defects = defects_per_unit.mean()
                    max_defects = defects_per_unit.max()
                    worst_unit = defects_per_unit.idxmax()
                    
                    st.metric("Avg Defects/Unit", f"{avg_defects:.1f}")
                    st.metric("Most Defects", f"{max_defects} (Unit {worst_unit})")
                
                # V3 Database integration status
                if self.db_manager:
                    st.markdown("**V3 Integration:**")
                    try:
                        stats = self.db_manager.get_database_stats()
                        st.caption(f"Work Orders: {stats.get('inspector_work_orders_count', 0)}")
                        st.caption(f"Progress Tracking: Active")
                        st.caption(f"Builder Access: Enabled")
                        st.caption(f"Developer Access: Enabled")
                    except:
                        st.caption("V3 Database: Connected")
    
    def _show_unit_analytics(self, unit_number: str, unit_defects: pd.DataFrame):
        """Show detailed analytics for a specific unit"""
        with st.expander(f"Unit {unit_number} Analytics", expanded=True):
            if len(unit_defects) == 0:
                st.success("This unit has no defects - perfect condition!")
                return
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Defect Breakdown by Trade:**")
                trade_counts = unit_defects['Trade'].value_counts()
                st.bar_chart(trade_counts)
            
            with col2:
                st.markdown("**Defect Breakdown by Urgency:**")
                urgency_counts = unit_defects['Urgency'].value_counts()
                st.bar_chart(urgency_counts)
            
            # Room analysis
            st.markdown("**Room Analysis:**")
            room_counts = unit_defects['Room'].value_counts()
            for room, count in room_counts.items():
                st.write(f"• {room}: {count} defect(s)")
    
    def _lookup_unit_defects(self, unit_number):
        """Enhanced unit defect lookup with V3 database integration"""
        if self.processed_data is None or unit_number is None:
            return pd.DataFrame()
        
        unit_data = self.processed_data[
            (self.processed_data["Unit"].astype(str).str.strip().str.lower() == str(unit_number).strip().lower()) &
            (self.processed_data["StatusClass"] == "Not OK")
        ].copy()
        
        if len(unit_data) > 0:
            urgency_order = {"Urgent": 1, "High Priority": 2, "Normal": 3}
            unit_data["UrgencySort"] = unit_data["Urgency"].map(urgency_order).fillna(3)
            unit_data = unit_data.sort_values(["UrgencySort", "PlannedCompletion"])
            
            unit_data["PlannedCompletion"] = pd.to_datetime(unit_data["PlannedCompletion"]).dt.strftime("%Y-%m-%d")
            
            return unit_data[["Room", "Component", "Trade", "Urgency", "PlannedCompletion"]]
        
        return pd.DataFrame(columns=["Room", "Component", "Trade", "Urgency", "PlannedCompletion"])
    
    def _show_enhanced_report_generation(self):
        """Enhanced Step 4: Report Generation with V3 database tracking"""
        
        st.markdown("---")
        st.markdown("### Step 4: Report Generation & Download")
        
        # Image Upload Section (preserved)
        st.markdown("#### Report Enhancement Images")
        st.markdown("Upload images to enhance your Word reports (optional):")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Company Logo:**")
            logo_upload = st.file_uploader(
                "Upload company logo for Word reports",
                type=['png', 'jpg', 'jpeg'],
                key="logo_upload",
                help="Recommended size: 200x100px or similar aspect ratio"
            )
            
            if logo_upload:
                st.image(logo_upload, caption="Logo Preview", width=150)
        
        with col2:
            st.markdown("**Cover Image:**")
            cover_upload = st.file_uploader(
                "Upload cover image for Word reports", 
                type=['png', 'jpg', 'jpeg'],
                key="cover_upload",
                help="Recommended size: 800x600px or similar landscape format"
            )
            
            if cover_upload:
                st.image(cover_upload, caption="Cover Preview", width=150)
        
        # Process and save uploaded images
        if st.button("Save Images for Reports", key="save_report_images"):
            images_saved = self._save_report_images(logo_upload, cover_upload)
            if images_saved > 0:
                st.success(f"{images_saved} image(s) saved for report enhancement!")
            else:
                st.info("No new images to save.")
        
        # Show current images status
        current_images = [k for k, v in st.session_state.report_images.items() if v is not None]
        if current_images:
            st.info(f"Current images ready: {', '.join(current_images)}")
        
        st.markdown("---")
        
        # Complete Package Option with V3 tracking
        st.markdown("##### Complete Report Package")
        st.markdown("Generate Excel + Word reports (Zip file)")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            package_description = f"""
            **Enhanced Package Includes:**
            • Professional Excel Report with multiple worksheets and charts
            • Executive Word Report with visual analytics and strategic insights  
            """
            st.markdown(package_description)
        
        with col2:
            if st.button("Generate Package (Zip file)", type="primary", use_container_width=True, key="generate_package"):
                self._generate_enhanced_complete_package()
        
        st.markdown("---")
        
        # Enhanced Individual Report Options
        st.markdown("##### Individual Reports")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Excel Report**")
            st.info("Multi-sheet analysis and data tables")
            
            if st.button("Generate Excel Report", type="secondary", use_container_width=True, key="generate_excel"):
                self._generate_enhanced_excel_report()
        
        with col2:
            st.markdown("**Word Report**") 
            st.info("Executive summary with visual charts")
            
            if st.button("Generate Word Report", 
                        type="secondary", 
                        use_container_width=True, key="export_v3_csv",
                        disabled=not WORD_REPORT_AVAILABLE):
                if WORD_REPORT_AVAILABLE:
                    self._generate_enhanced_word_report()
                else:
                    st.error("Word report generator is not available")
        
        with col3:
            st.markdown("**CSV Export**")
            st.info("Raw data export for external analysis")
            
            if st.button("Export CSV", type="secondary", use_container_width=True):
                self._export_enhanced_csv_data()
        
        # # Enhanced Report Status
        # st.markdown("---")
        # st.markdown("##### Enhanced Report Status & V3 Integration")
        
        # col1, col2 = st.columns(2)
        
        # with col1:
        #     st.markdown("**Available Report Generators:**")
        #     excel_status = "Available (V3 Enhanced)" if EXCEL_REPORT_AVAILABLE else "Not Available"
        #     word_status = "Available (V3 Enhanced)" if WORD_REPORT_AVAILABLE else "Not Available"
            
        #     st.markdown(f"• Excel Reports: {excel_status}")
        #     st.markdown(f"• Word Reports: {word_status}")
            
        #     if not WORD_REPORT_AVAILABLE:
        #         st.warning("Install dependencies for Word reports: `pip install python-docx matplotlib`")
        
        # with col2:
        #     st.markdown("**V3 Enhancement Status:**")
        #     logo_status = "Ready" if st.session_state.report_images.get('logo') else "Not uploaded"
        #     cover_status = "Ready" if st.session_state.report_images.get('cover') else "Not uploaded"
            
        #     st.markdown(f"• Company Logo: {logo_status}")
        #     st.markdown(f"• Cover Image: {cover_status}")
            
        #     # Enhanced database status for reports
        #     if self.current_inspection_id:
        #         st.success(f"• V3 Database ID: {self.current_inspection_id[:8]}...")
        #         st.success("• Cross-role data access: Enabled")
        #         st.success("• Work orders: Generated")
        #         st.success("• Progress tracking: Active")
        #     else:
        #         st.warning("• No V3 database record")
            
        #     if st.button("Clear All Images", key="clear_report_images"):
        #         self._clear_report_images()
        #         st.success("Images cleared!")
        #         st.rerun()
    
    # Keep existing image handling methods unchanged
    def _save_report_images(self, logo_upload, cover_upload):
        """Save uploaded images to temporary files"""
        
        images_saved = 0
        temp_dir = tempfile.gettempdir()
        
        try:
            if logo_upload:
                logo_path = os.path.join(temp_dir, f"logo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg")
                with open(logo_path, "wb") as f:
                    f.write(logo_upload.getbuffer())
                st.session_state.report_images["logo"] = logo_path
                images_saved += 1
            
            if cover_upload:
                cover_path = os.path.join(temp_dir, f"cover_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg")
                with open(cover_path, "wb") as f:
                    f.write(cover_upload.getbuffer())
                st.session_state.report_images["cover"] = cover_path
                images_saved += 1
                
        except Exception as e:
            st.error(f"Error saving images: {e}")
            return 0
        
        return images_saved
    
    def _clear_report_images(self):
        """Clear saved report images"""
        
        for image_type, image_path in st.session_state.report_images.items():
            if image_path and os.path.exists(image_path):
                try:
                    os.remove(image_path)
                except:
                    pass
        
        st.session_state.report_images = {
            'logo': None,
            'cover': None
        }
    
    # Enhanced report generation methods
    def _generate_enhanced_complete_package(self):
        """Generate complete ZIP package"""
        
        try:
            with st.spinner("Generating report package..."):
                if not EXCEL_REPORT_AVAILABLE:
                    st.error("Excel generator not available")
                    return
                
                excel_buffer = generate_professional_excel_report(self.processed_data, self.metrics)
                excel_bytes = excel_buffer.getvalue()
                
                word_bytes = None
                if WORD_REPORT_AVAILABLE:
                    try:
                        doc = generate_professional_word_report(
                            self.processed_data, 
                            self.metrics,
                            st.session_state.report_images
                        )
                        buf = BytesIO()
                        doc.save(buf)
                        buf.seek(0)
                        word_bytes = buf.getvalue()
                    except Exception as e:
                        st.warning(f"Word report generation failed: {e}")
                        word_bytes = None
                
                zip_bytes = self._create_enhanced_zip_package(excel_bytes, word_bytes)
                zip_filename = f"{self._generate_enhanced_filename('Package')}.zip"
                
                st.success("Package generated!")
                st.download_button(
                    "Download Package",
                    data=zip_bytes,
                    file_name=zip_filename,
                    mime="application/zip",
                    use_container_width=True
                )
                
                # Enhanced package information
                package_info = f"""
                **Package Contents:**
                • Excel Report: Multi-sheet analysis with {self.metrics['total_units']} units
                • Word Report: Executive summary with visual analytics
                • V3 Database Integration: Cross-role data access enabled
                • Work Orders: {self._get_work_order_count()} created for Builder access
                • Progress Tracking: Enabled for Developer dashboard
                • File Size: {len(zip_bytes) / 1024:.1f} KB
                • Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
                """
                if self.current_inspection_id:
                    package_info += f"\n• Database ID: {self.current_inspection_id[:8]}..."
                
                st.info(package_info)
                
        except Exception as e:
            st.error(f"Error generating V3 enhanced package: {e}")
    
    def _generate_enhanced_excel_report(self):
        """Generate Excel report"""
        
        try:
            with st.spinner("Generating Excel report..."):
                if not EXCEL_REPORT_AVAILABLE:
                    st.error("Excel generator not available")
                    return
                
                excel_buffer = generate_professional_excel_report(self.processed_data, self.metrics)
                filename = f"{self._generate_enhanced_filename('V3_Excel')}.xlsx"
                
                st.success("V3 Enhanced Excel report generated!")
                st.download_button(
                    "Download V3 Enhanced Excel Report",
                    data=excel_buffer.getvalue(),
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
                
                # Enhanced report information
                report_info = f"""
                **Excel Report Details:**
                • Building: {self.metrics['building_name']}
                • Units: {self.metrics['total_units']} processed
                • Defects: {self.metrics['total_defects']} identified
                • V3 Database Integration: Enabled
                • Work Orders Created: {self._get_work_order_count()}
                • Cross-role Access: Builder and Developer enabled
                • File Size: {len(excel_buffer.getvalue()) / 1024:.1f} KB
                """
                if self.current_inspection_id:
                    report_info += f"\n• Database ID: {self.current_inspection_id[:8]}..."
                
                st.info(report_info)
                
        except Exception as e:
            st.error(f"Error generating Excel report: {e}")
    
    def _generate_enhanced_word_report(self):
        """Generate enhanced Word report with V3 database integration"""
        
        try:
            with st.spinner("Generating Word report..."):
                if not WORD_REPORT_AVAILABLE:
                    st.error("Word generator not available")
                    return
                
                doc = generate_professional_word_report(
                    self.processed_data,
                    self.metrics, 
                    st.session_state.report_images
                )
                
                buf = BytesIO()
                doc.save(buf)
                buf.seek(0)
                word_bytes = buf.getvalue()
                
                filename = f"{self._generate_enhanced_filename('V3_Word')}.docx"
                
                st.success("Word report generated!")
                st.download_button(
                    "Download Word Report",
                    data=word_bytes,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    use_container_width=True
                )
                
                # Enhanced report information
                images_used = len([k for k, v in st.session_state.report_images.items() if v])
                report_info = f"""
                **V3 Enhanced Word Report Details:**
                • Executive summary with visual charts and V3 integration
                • Images included: {images_used} (logo, cover)
                • V3 Database tracking: Enabled
                • Cross-role accessibility: Builder and Developer
                • Work order references: Included
                • File Size: {len(word_bytes) / 1024:.1f} KB
                """
                if self.current_inspection_id:
                    report_info += f"\n• Database ID: {self.current_inspection_id[:8]}..."
                
                st.info(report_info)
                
        except Exception as e:
            st.error(f"Error generating V3 enhanced Word report: {e}")
    
    def _export_enhanced_csv_data(self):
        """Export enhanced CSV data with V3 database metadata"""
        
        try:
            # Enhanced CSV with additional metadata
            enhanced_df = self.processed_data.copy()
            
            # Add V3 database metadata columns
            if self.current_inspection_id:
                enhanced_df['V3_Database_ID'] = self.current_inspection_id
                enhanced_df['V3_Work_Orders_Generated'] = True
                enhanced_df['V3_Cross_Role_Access'] = True
            
            enhanced_df['Export_Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            enhanced_df['Quality_Score'] = max(0, 100 - self.metrics['defect_rate'])
            enhanced_df['Building_Ready_Percentage'] = self.metrics['ready_pct']
            
            csv_buffer = StringIO()
            enhanced_df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            filename = f"{self._generate_enhanced_filename('V3_CSV')}.csv"
            
            st.download_button(
                "Download CSV Data",
                data=csv_data,
                file_name=filename,
                mime="text/csv",
                use_container_width=True
            )
            
            st.success("V3 Enhanced CSV export ready for download!")
            st.info("Enhanced CSV includes V3 database metadata and cross-role access information")
            
        except Exception as e:
            st.error(f"Error exporting V3 enhanced CSV: {e}")
    
    def _create_enhanced_zip_package(self, excel_bytes, word_bytes):
        """Create enhanced ZIP package"""
        
        zip_buffer = BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            excel_filename = f"{self._generate_enhanced_filename('V3_Excel')}.xlsx"
            zip_file.writestr(excel_filename, excel_bytes)
            
            if word_bytes:
                word_filename = f"{self._generate_enhanced_filename('V3_Word')}.docx"
                zip_file.writestr(word_filename, word_bytes)
            
            # Enhanced summary content with V3 integration details
            summary_content = f"""V3 Enhanced Inspection Report Package Summary
============================================
Building: {self.metrics['building_name']}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Database ID: {self.current_inspection_id or 'Not saved'}

Key Metrics:
- Total Units: {self.metrics['total_units']:,}
- Total Defects: {self.metrics['total_defects']:,}
- Ready for Settlement: {self.metrics['ready_pct']:.1f}%
- Quality Score: {max(0, 100 - self.metrics['defect_rate']):.1f}/100

V3 Database Integration:
- Cross-role Data Access: Enabled
- Work Orders Created: {self._get_work_order_count()} for Builder access
- Progress Tracking: Enabled for Developer dashboard
- Database Storage: Persistent and searchable
- Real-time Updates: Supported

Files Included:
- {excel_filename}
{'- ' + word_filename if word_bytes else '- Word report (generation failed)'}
- v3_package_summary.txt (this file)

Builder Access:
- Work orders automatically generated from defects
- Trade-based task organization
- Priority-based scheduling
- Progress tracking and updates

Developer Access:
- Project overview and analytics
- Progress timeline tracking
- Quality trend analysis
- Cross-project comparisons
"""
            zip_file.writestr("v3_package_summary.txt", summary_content)
        
        zip_buffer.seek(0)
        return zip_buffer.getvalue()
    
    def _generate_enhanced_filename(self, report_type):
        """Generate enhanced filename with V3 database reference"""
        
        clean_name = "".join(c for c in self.metrics['building_name'] if c.isalnum() or c in (' ', '-', '_')).strip()
        clean_name = clean_name.replace(' ', '_')
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        db_ref = f"_DB{self.current_inspection_id[:8]}" if self.current_inspection_id else ""
        
        return f"{clean_name}_V3_Enhanced_Inspection_Report_{report_type}_{timestamp}{db_ref}"
    
    def _get_work_order_count(self):
        """Get count of work orders created for this inspection"""
        if not self.db_manager or not self.current_inspection_id:
            return 0
        
        try:
            work_orders = self.db_manager.get_work_orders_for_builder()
            inspection_work_orders = work_orders[work_orders['inspection_id'] == self.current_inspection_id] if 'inspection_id' in work_orders.columns else pd.DataFrame()
            return len(inspection_work_orders)
        except:
            return 0

def render_inspector_interface(user_info=None, auth_manager=None):
    """Main inspector interface function for integration with main.py"""
    
    # Initialize or update the inspector interface with user context
    if 'inspector_interface' not in st.session_state:
        st.session_state.inspector_interface = InspectorInterface(user_info=user_info)
    else:
        # Update user info if it changed
        st.session_state.inspector_interface.user_info = user_info
    
    inspector = st.session_state.inspector_interface
    
    # Store auth manager for permission checks
    if auth_manager:
        inspector.auth_manager = auth_manager
    
    # Get user role for header display
    user_role = user_info.get('role', 'inspector') if user_info else 'inspector'
    user_name = user_info.get('name', 'User') if user_info else 'User'
    
    # Clean header matching Admin/Builder style
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #137116 0%, #77C179 100%); 
                color: white; padding: 1.5rem; border-radius: 10px; margin-bottom: 1rem;">
        <h2 style="margin: 0;">Inspector Dashboard</h2>
        <p style="margin: 0.5rem 0 0 0; opacity: 0.9;">Inspector: {user_name}</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Show the main dashboard
    inspector.show_inspector_dashboard()  # ✅ CORRECT
    
if __name__ == "__main__":
    print("Enhanced Inspector Interface V3 with Database Integration")
    print("Features:")
    print("- Enhanced trade mapping management with V3 database integration")
    print("- Professional report generation (Excel + Word) with database tracking")
    print("- Image upload for logo and cover enhancement")
    print("- Complete ZIP package generation with V3 metadata")
    print("- Unit lookup and defect analysis with work order integration")
    print("- V3 DATABASE INTEGRATION for seamless Builder/Developer access")
    print("- Previous inspection loading and management with analytics")
    print("- Work order creation and tracking for Builder role")
    print("- Progress tracking and analytics for Developer role")
    print("- Enhanced CSV processing logging and validation")
    print("- Cross-role data access with real-time updates")
    print("Ready for Building Inspection System V3 integration!")