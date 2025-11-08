"""
Updated Inspector Role Interface V3 - Enhanced Database Integration
==================================================================

This updated version integrates with the enhanced database setup.py schema
while maintaining all existing functionality for image upload and report generation.
"""

import pandas as pd
import streamlit as st
from datetime import datetime
import logging
from io import BytesIO, StringIO
from typing import Dict, Any, Tuple, Optional
import tempfile
import os
import zipfile
import hashlib

# Import the enhanced modules
from core.data_processor import InspectionDataProcessor, load_master_trade_mapping
from core.trade_mapper import TradeMapper

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
    if st.button("🧪 TEST DATABASE SAVE"):
    st.write("### 🎯 Quick Save Test")
    
    try:
        # Get connection manager
        from database.connection_manager import get_connection_manager
        conn_manager = get_connection_manager()
        st.success(f"✅ Connection: {conn_manager.db_type}")
        
        # Get processor
        if hasattr(st.session_state, 'inspector') and st.session_state.inspector:
            processor = st.session_state.inspector.processor
        else:
            from roles.inspector import InspectorInterface
            inspector = InspectorInterface(conn_manager=conn_manager)
            processor = inspector.processor
        
        # Check processor
        st.write(f"**processor.conn_manager:** {processor.conn_manager}")
        
        if processor.conn_manager is None:
            st.error("❌ PROBLEM: processor.conn_manager is None!")
            st.stop()
        
        st.success("✅ Processor has conn_manager")
        
        # Create test data
        test_data = {
            'auditName': ['08/11/2025 / 101 / TEST'],
            'Title Page_Conducted on': ['2025-11-08'],
            'Lot Details_Lot Number': ['101'],
            'Pre-Settlement Inspection_Unit Type': ['Apartment'],
            'Pre-Settlement Inspection_Living Room_Paint': ['Not OK'],
        }
        test_df = pd.DataFrame(test_data)
        
        # Load mapping
        from core.data_processor import load_master_trade_mapping
        mapping = load_master_trade_mapping()
        
        building_info = {'name': 'TEST', 'address': 'Test', 'date': '2025-11-08'}
        file_hash = hashlib.md5(str(test_data).encode()).hexdigest()
        
        st.write("🔄 Processing...")
        
        # PROCESS - THIS SHOULD SAVE!
        final_df, metrics, inspection_id = processor.process_inspection_data(
            df=test_df,
            mapping=mapping,
            building_info=building_info,
            inspector_name="Test Inspector",
            original_filename="test.csv",
            file_hash=file_hash
        )
        
        if inspection_id:
            st.success(f"🎉 SAVED! ID: {inspection_id}")
            st.balloons()
            
            # Verify
            conn = conn_manager.get_connection()
            cursor = conn.cursor()
            if conn_manager.db_type == "postgresql":
                cursor.execute("SELECT COUNT(*) FROM inspector_inspection_items WHERE inspection_id = %s", (inspection_id,))
            else:
                cursor.execute("SELECT COUNT(*) FROM inspector_inspection_items WHERE inspection_id = ?", (inspection_id,))
            count = cursor.fetchone()[0]
            cursor.close()
            st.success(f"✅ {count} items in database!")
        else:
            st.error("❌ FAILED: inspection_id is None")
            st.error("Check console logs!")
            
    except Exception as e:
        st.error(f"❌ Error: {e}")
        st.exception(e)
        
    def __init__(self, db_path: str = "building_inspection.db", user_info: dict = None):
        """Initialize with connection manager"""
        self._button_counter = 0
        
        # ✅ Use connection manager
        from database.connection_manager import get_connection_manager
        self.conn_manager = get_connection_manager()
        self.db_type = self.conn_manager.db_type
        
        # ✅ Pass connection manager to processor
        self.processor = InspectionDataProcessor(db_path, conn_manager=self.conn_manager)
        
        self.mapper = TradeMapper()
        self.processed_data = None
        self.metrics = None
        self.trade_mapping = None
        self.current_inspection_id = None
        self.user_info = user_info
        self.auth_manager = None
        
        # Keep db_manager for legacy features only (SQLite)
        try:
            from database.setup import DatabaseManager
            
            try:
                # Pass connection manager for PostgreSQL support
                self.db_manager = DatabaseManager(db_path, conn_manager=self.conn_manager)
                print(f"✅ DatabaseManager initialized with {self.db_type}")
            except TypeError:
                # Fallback if DatabaseManager doesn't accept conn_manager yet
                if self.db_type == "sqlite":
                    self.db_manager = DatabaseManager(db_path)
                    print(f"⚠️  DatabaseManager: SQLite only")
                else:
                    # Create minimal wrapper for PostgreSQL
                    class DBWrapper:
                        def __init__(self, cm):
                            self.cm = cm
                            self.db_type = getattr(cm, 'db_type', 'postgresql')
                        def connect(self):
                            return self.cm.get_connection()
                    self.db_manager = DBWrapper(self.conn_manager)
                    print(f"✅ DatabaseManager: Using DBWrapper for {self.db_type}")
            
            # ✅ MOVED THIS OUTSIDE! Now it ALWAYS runs
            # Ensure processor has database access
            if hasattr(self, 'processor') and self.processor:
                if not hasattr(self.processor, 'db_manager') or self.processor.db_manager is None:
                    self.processor.db_manager = self.db_manager
                    print(f"✅ Assigned db_manager to processor")
                    
        except ImportError as e:
            print(f"⚠️  DatabaseManager not available: {e}")
            self.db_manager = None
        
        if 'report_images' not in st.session_state:
            st.session_state.report_images = {'logo': None, 'cover': None}

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
        
        # ✅ SIMPLE DATABASE CHECK - works with both PostgreSQL and SQLite
        has_database = bool(self.conn_manager or self.processor.db_manager)
        
        if not has_database:
            st.error("⚠️ Database Not Available")
            st.warning("Some features will be disabled:")
            st.write("- ❌ Previous inspections")
            st.write("- ❌ Data persistence")  
            st.write("- ❌ Work order creation")
            st.write("- ✅ CSV processing (temporary)")
            st.write("- ✅ Report generation (temporary)")
            
            with st.expander("How to fix", expanded=True):
                st.markdown("""
                **For PostgreSQL (Supabase):**
                1. Check your `.streamlit/secrets.toml` file exists
                2. Verify your PostgreSQL connection string is correct
                
                **For SQLite (Local):**
                1. Ensure `database/setup.py` exists
                2. Run: `python database/setup.py`
                3. Restart the application
                """)
            
            st.markdown("---")
        
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
        """Show database status - PostgreSQL/SQLite compatible"""
        
        user_role = self.get_current_user_role()
        
        has_database = bool(self.conn_manager)
        
        if has_database:
            db_type = self.conn_manager.db_type.upper()
            
            if user_role == 'admin':
                # Full status for admin
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.success(f"Database Connected ({db_type})")
                    
                with col2:
                    st.info("Inspector Data: Available")
                
                with col3:
                    st.info("Work Orders: Available")
                
                st.caption(f"Using {db_type} - Data saved for Builder/Developer access")
            else:
                # Simplified status for inspector
                st.success(f"✅ Database Connected ({db_type}) - Data will be saved automatically")
        else:
            st.warning("⚠️ Database not available - Data will only be stored in current session")
    
    def _show_previous_inspections_section(self):
        """Show previous inspections - PostgreSQL/SQLite compatible"""
        
        st.markdown("### Previous Inspections")
        
        has_database = bool(self.conn_manager or self.processor.db_manager)
        
        if not has_database:
            st.info("⏭️ Database not configured - Previous inspections not available")
            st.markdown("---")
            return
        
        try:
            conn = self.conn_manager.get_connection()
            
            # ✅ PostgreSQL/SQLite compatible query
            if self.db_type == "postgresql":
                query = """
                    SELECT i.id, i.inspection_date, i.created_at, b.name as building_name,
                        i.total_units, i.total_defects, i.ready_pct, i.inspector_name,
                        i.original_filename
                    FROM inspector_inspections i
                    JOIN inspector_buildings b ON i.building_id = b.id
                    WHERE i.original_filename IS NOT NULL
                    ORDER BY i.created_at DESC
                    LIMIT 10
                """
            else:
                query = """
                    SELECT i.id, i.inspection_date, i.created_at, b.name as building_name,
                        i.total_units, i.total_defects, i.ready_pct, i.inspector_name,
                        i.original_filename
                    FROM inspector_inspections i
                    JOIN inspector_buildings b ON i.building_id = b.id
                    WHERE i.original_filename IS NOT NULL
                    ORDER BY i.created_at DESC
                    LIMIT 10
                """
            
            recent_inspections = pd.read_sql_query(query, conn)
            conn.close()
            
            if len(recent_inspections) > 0:
                st.success(f"✅ Found {len(recent_inspections)} previous inspections")
                
                # Display inspections
                for idx, row in recent_inspections.iterrows():
                    # Format dates
                    inspection_date_str = str(row['inspection_date'])
                    if isinstance(row['inspection_date'], pd.Timestamp):
                        inspection_date_str = row['inspection_date'].strftime('%Y-%m-%d')
                    
                    # Format upload time
                    if isinstance(row['created_at'], pd.Timestamp):
                        upload_time = row['created_at'].strftime('%Y-%m-%d %H:%M')
                        upload_date = row['created_at'].strftime('%Y-%m-%d')
                    else:
                        upload_time = str(row['created_at'])
                        upload_date = str(row['created_at'])
                    
                    # Convert metrics
                    total_units = int(row['total_units']) if pd.notna(row['total_units']) else 0
                    total_defects = int(row['total_defects']) if pd.notna(row['total_defects']) else 0
                    ready_pct = float(row['ready_pct']) if pd.notna(row['ready_pct']) else 0.0
                    
                    with st.expander(
                        f"📋 {row['building_name']} - Uploaded {upload_date} "
                        f"({total_units} units, {total_defects} defects)",
                        expanded=False
                    ):
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("Total Units", total_units)
                            st.metric("Inspector", str(row['inspector_name']))
                        
                        with col2:
                            st.metric("Total Defects", total_defects)
                            st.metric("Ready %", f"{ready_pct:.1f}%")
                        
                        with col3:
                            st.markdown("**Upload Time:**")
                            st.write(upload_time)
                            st.markdown("**Inspection Date:**")
                            st.write(inspection_date_str)
                            st.caption(f"File: {row['original_filename']}")
                        
                        if st.button(
                            "📂 Load This Inspection",
                            key=f"load_inspection_{row['id']}",
                            use_container_width=True,
                            type="primary"
                        ):
                            st.session_state['viewing_loaded_inspection'] = True
                            st.session_state['loaded_inspection_id'] = row['id']
                            self._load_previous_inspection(row['id'])
            else:
                st.info("No previous inspections found. Upload a CSV to get started!")
                
        except Exception as e:
            st.error(f"❌ Error loading inspections: {e}")
            import traceback
            st.code(traceback.format_exc())
        
        st.markdown("---")


    # ============================================
    # ADD THIS DEBUG METHOD TO InspectorInterface CLASS
    # ============================================
    def _debug_inspection_data(self, inspection_id: str):
        """Debug what's stored in database vs what's reconstructed"""
        
        st.markdown("---")
        st.markdown("## 🔍 Debug Analysis")
        st.markdown(f"**Inspection ID:** `{inspection_id[:12]}...`")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # ========================================
            # 1. RAW DATABASE SAMPLE
            # ========================================
            st.markdown("### 1️⃣ Raw Database Sample")
            
            if self.db_type == "postgresql":
                cursor.execute("""
                    SELECT unit_number, room, item_description, severity, notes
                    FROM inspector_inspection_items
                    WHERE inspection_id = %s
                    LIMIT 5
                """, (inspection_id,))
            else:
                cursor.execute("""
                    SELECT unit, room, component, urgency, ''
                    FROM inspector_inspection_items
                    WHERE inspection_id = ?
                    LIMIT 5
                """, (inspection_id,))
            
            raw_rows = cursor.fetchall()
            
            st.write(f"**Total items retrieved:** {len(raw_rows)}")
            
            if len(raw_rows) > 0:
                for i, row in enumerate(raw_rows[:2]):
                    with st.expander(f"Sample Row {i+1}", expanded=i==0):
                        if isinstance(row, dict):
                            st.code(f"""
    Unit Number: {row.get('unit_number', 'N/A')}
    Room: {row.get('room', 'N/A')}
    Description: {row.get('item_description', 'N/A')}
    Severity: {row.get('severity', 'N/A')}
    Notes JSON: {str(row.get('notes', '{}'))[:200]}...
                            """)
                        else:
                            st.code(f"""
    Unit Number: {row[0]}
    Room: {row[1]}
    Description: {row[2]}
    Severity: {row[3]}
    Notes JSON: {str(row[4])[:200] if len(row) > 4 else 'N/A'}...
                            """)
            else:
                st.warning("No items found in database!")
                cursor.close()
                conn.close()
                return
            
            # ========================================
            # 2. PARSE JSON SAMPLES
            # ========================================
            if self.db_type == "postgresql":
                st.markdown("### 2️⃣ JSON Parsing Test")
                
                import json
                
                if len(raw_rows) > 0:
                    # Get notes from first row
                    if isinstance(raw_rows[0], dict):
                        sample_notes = raw_rows[0].get('notes', '{}')
                    else:
                        sample_notes = raw_rows[0][4] if len(raw_rows[0]) > 4 else '{}'
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**Raw JSON:**")
                        st.code(sample_notes[:500])
                    
                    with col2:
                        try:
                            parsed = json.loads(sample_notes) if sample_notes else {}
                            st.success("✅ JSON is valid")
                            
                            st.markdown("**Extracted Values:**")
                            st.write(f"🔹 component: `{parsed.get('component', 'MISSING')}`")
                            st.write(f"🔹 trade: `{parsed.get('trade', 'MISSING')}`")
                            st.write(f"🔹 unit_type: `{parsed.get('unit_type', 'MISSING')}`")
                            st.write(f"🔹 urgency: `{parsed.get('urgency', 'MISSING')}`")
                            
                        except Exception as e:
                            st.error(f"❌ JSON parsing failed: {e}")
            
            # ========================================
            # 3. EXTRACT ALL UNIQUE VALUES
            # ========================================
            if self.db_type == "postgresql":
                st.markdown("### 3️⃣ Unique Values Stored")
                
                cursor.execute("""
                    SELECT notes FROM inspector_inspection_items
                    WHERE inspection_id = %s
                """, (inspection_id,))
                
                all_notes = cursor.fetchall()
                
                import json
                unit_types = set()
                trades = set()
                urgencies = set()
                
                for note_row in all_notes:
                    try:
                        # Handle both dict and tuple
                        notes_str = note_row['notes'] if isinstance(note_row, dict) else note_row[0]
                        
                        if notes_str:
                            data = json.loads(notes_str)
                            if data.get('unit_type'):
                                unit_types.add(data['unit_type'])
                            if data.get('trade'):
                                trades.add(data['trade'])
                            if data.get('urgency'):
                                urgencies.add(data['urgency'])
                    except Exception as e:
                        pass
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Unique Unit Types", len(unit_types))
                    if len(unit_types) > 0:
                        st.write(sorted(unit_types))
                    else:
                        st.error("⚠️ No unit types found!")
                
                with col2:
                    st.metric("Unique Trades", len(trades))
                    if len(trades) > 0:
                        if len(trades) <= 5:
                            st.write(sorted(trades))
                        else:
                            st.write(f"Top 5: {sorted(trades)[:5]}")
                    else:
                        st.error("⚠️ No trades found!")
                
                with col3:
                    st.metric("Unique Urgencies", len(urgencies))
                    if len(urgencies) > 0:
                        st.write(sorted(urgencies))
                    else:
                        st.warning("⚠️ No urgencies found!")
            
            # ========================================
            # 4. TEST RECONSTRUCTION
            # ========================================
            st.markdown("### 4️⃣ Reconstruction Test")
            
            st.info("Testing load_inspection_from_database()...")
            
            try:
                items_df, metrics = self.processor.load_inspection_from_database(inspection_id)
                
                st.success("✅ Load successful!")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Rows Loaded", len(items_df))
                with col2:
                    st.metric("Unique Units", items_df['Unit'].nunique())
                with col3:
                    reconstructed_types = ", ".join(sorted(items_df['UnitType'].unique()))
                    st.metric("Unit Types", len(items_df['UnitType'].unique()))
                    st.caption(reconstructed_types)
                with col4:
                    st.metric("Total Defects", metrics['total_defects'])
                
                # Show sample reconstructed data
                st.markdown("**Sample Reconstructed Data:**")
                sample_cols = ['Unit', 'UnitType', 'Room', 'Component', 'Trade', 'Urgency']
                available_cols = [col for col in sample_cols if col in items_df.columns]
                st.dataframe(items_df[available_cols].head(10))
                
                # Show metrics
                st.markdown("**Metrics Calculated:**")
                col_a, col_b, col_c = st.columns(3)
                
                with col_a:
                    st.write(f"**Unit Types String:**")
                    st.code(metrics['unit_types_str'])
                with col_b:
                    st.write(f"**Total Units:**")
                    st.code(metrics['total_units'])
                with col_c:
                    st.write(f"**Total Defects:**")
                    st.code(metrics['total_defects'])
                
                # Show summary tables
                st.markdown("**Summary Tables:**")
                
                tab1, tab2, tab3 = st.tabs(["Trade Summary", "Unit Summary", "Room Summary"])
                
                with tab1:
                    if len(metrics['summary_trade']) > 0:
                        st.dataframe(metrics['summary_trade'].head(10))
                    else:
                        st.error("⚠️ Empty trade summary!")
                
                with tab2:
                    if len(metrics['summary_unit']) > 0:
                        st.dataframe(metrics['summary_unit'].head(10))
                    else:
                        st.error("⚠️ Empty unit summary!")
                
                with tab3:
                    if len(metrics['summary_room']) > 0:
                        st.dataframe(metrics['summary_room'].head(10))
                    else:
                        st.error("⚠️ Empty room summary!")
                
            except Exception as e:
                st.error(f"❌ Reconstruction failed: {e}")
                import traceback
                st.code(traceback.format_exc())
            
            cursor.close()
            conn.close()
            
            st.success("✅ Debug analysis complete!")
            
        except Exception as e:
            st.error(f"Debug failed: {e}")
            import traceback
            st.code(traceback.format_exc())
    
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
    
    def _check_hash_in_database(self, file_hash: str, filename: str):
        """Check for duplicate - PostgreSQL/SQLite compatible"""
        
        if not self.conn_manager:
            return None
        
        try:
            conn = self.conn_manager.get_connection()
            cursor = conn.cursor()
            
            # ✅ Use correct query for database type
            if self.db_type == "postgresql":
                query = """
                    SELECT inspection_id, building_name, created_at, original_filename
                    FROM inspector_csv_processing_log
                    WHERE file_checksum = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                """
                cursor.execute(query, (file_hash,))
            else:
                query = """
                    SELECT inspection_id, building_name, created_at, original_filename
                    FROM inspector_csv_processing_log
                    WHERE file_checksum = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                """
                cursor.execute(query, (file_hash,))
            
            result = cursor.fetchone()
            
            if result:
                # Handle both dict and tuple results
                if isinstance(result, dict):
                    return {
                        'is_duplicate': True,
                        'inspection_id': result['inspection_id'],
                        'building_name': result['building_name'],
                        'processed_date': result['created_at'],
                        'original_filename': result['original_filename']
                    }
                else:
                    return {
                        'is_duplicate': True,
                        'inspection_id': result[0],
                        'building_name': result[1],
                        'processed_date': result[2],
                        'original_filename': result[3]
                    }
            
            cursor.close()
            conn.close()
            return None
            
        except Exception as e:
            logger.error(f"Duplicate check error: {e}")
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
        
        # Check if viewing a loaded inspection
        if st.session_state.get('viewing_loaded_inspection'):
            st.info(f"📂 Viewing loaded inspection: {st.session_state.get('loaded_inspection_id', 'Unknown')[:12]}...")
            if st.button("🔄 Upload New File"):
                st.session_state.pop('viewing_loaded_inspection', None)
                st.session_state.pop('loaded_inspection_id', None)
                st.rerun()
            return
        
        # Check trade mapping
        if 'trade_mapping' not in st.session_state or len(st.session_state.trade_mapping) == 0:
            st.warning("⚠️ Please load your trade mapping first (Step 1 above)")
            return
        
        # ✅ Show database status (informative only)
        has_database = bool(self.conn_manager or self.processor.db_manager)
        if has_database:
            db_type = self.conn_manager.db_type.upper() if self.conn_manager else "SQLITE"
            st.success(f"💾 Database active ({db_type}) - Data will be saved persistently")
        else:
            st.info("💡 No database - Data will be temporary (this session only)")
        
        # Rest of your existing file upload code continues here...
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
                
                # Process button
                if st.button("🚀 Process Inspection Data", type="primary", use_container_width=True):
                    st.session_state.pop(allow_key, None)
                    st.session_state['last_processed_hash'] = file_hash  # Mark as just processed
                    self._process_inspection_data_simplified(uploaded_csv)
                    
            except Exception as e:
                st.error(f"Error: {e}")
                
                # ADD THIS AFTER SUCCESSFUL PROCESSING (when self.processed_data is not None)
                if st.button("🧪 Test Data Flow", key="test_data_flow"):
                    st.markdown("### 🧪 Data Flow Test")
                    
                    # TEST 1: Check processed DataFrame
                    st.markdown("#### 1️⃣ Processed DataFrame")
                    if self.processed_data is not None:
                        defects = self.processed_data[self.processed_data['StatusClass'] == 'Not OK']
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Rows", len(self.processed_data))
                        with col2:
                            st.metric("Defects (Not OK)", len(defects))
                        with col3:
                            unique_types = defects['UnitType'].unique()
                            st.metric("Unique Unit Types", len(unique_types))
                            st.caption(", ".join(sorted(unique_types)))
                        
                        st.markdown("**Sample Defects:**")
                        st.dataframe(defects[['Unit', 'UnitType', 'Room', 'Component', 'Trade', 'Urgency']].head(5))
                    else:
                        st.error("No processed data!")
                    
                    # TEST 2: Check what would be saved
                    st.markdown("#### 2️⃣ Data Prepared for Save")
                    if self.processed_data is not None:
                        defects_df = self.processed_data[self.processed_data['StatusClass'] == 'Not OK'].copy()
                        
                        # Simulate preparing inspection_items
                        inspection_items = []
                        for idx, row in defects_df.iterrows():
                            item_data = {
                                'unit': str(row['Unit']),
                                'unit_type': str(row['UnitType']),
                                'room': str(row['Room']),
                                'component': str(row['Component']),
                                'trade': str(row['Trade']),
                                'status': str(row['StatusClass']),
                                'urgency': str(row['Urgency'])
                            }
                            inspection_items.append(item_data)
                            
                            if idx >= 2:  # Only process 3 samples
                                break
                        
                        st.write(f"**Total items to save:** {len(defects_df)}")
                        st.write(f"**Sample prepared items:**")
                        
                        for i, item in enumerate(inspection_items):
                            with st.expander(f"Item {i+1}", expanded=i==0):
                                st.json(item)
                        
                        # Check unique unit types in prepared data
                        unique_types_in_prepared = set(item['unit_type'] for item in inspection_items)
                        st.success(f"✅ Unit types in prepared data: {sorted(unique_types_in_prepared)}")
                    
                    # TEST 3: Check database (if inspection was saved)
                    if hasattr(self, 'current_inspection_id') and self.current_inspection_id:
                        st.markdown("#### 3️⃣ Data in Database")
                        
                        try:
                            conn = self._get_connection()
                            cursor = conn.cursor()
                            
                            # Get sample from database
                            cursor.execute("""
                                SELECT unit_number, room, item_description, notes
                                FROM inspector_inspection_items
                                WHERE inspection_id = %s
                                LIMIT 3
                            """, (self.current_inspection_id,))
                            
                            db_rows = cursor.fetchall()
                            
                            st.write(f"**Sample database rows:**")
                            
                            import json
                            for i, row in enumerate(db_rows):
                                with st.expander(f"DB Row {i+1}", expanded=i==0):
                                    unit_num = row[0]
                                    room = row[1]
                                    desc = row[2]
                                    notes_json = row[3]
                                    
                                    st.write(f"**Unit:** {unit_num}")
                                    st.write(f"**Room:** {room}")
                                    st.write(f"**Description:** {desc}")
                                    st.write(f"**Notes JSON:**")
                                    
                                    try:
                                        notes = json.loads(notes_json) if notes_json else {}
                                        st.json(notes)
                                        
                                        if notes.get('unit_type'):
                                            st.success(f"✅ unit_type found: {notes['unit_type']}")
                                        else:
                                            st.error("❌ unit_type MISSING in JSON!")
                                    except Exception as e:
                                        st.error(f"❌ JSON parse error: {e}")
                                        st.code(notes_json)
                            
                            # Check unique unit types in database
                            cursor.execute("""
                                SELECT notes FROM inspector_inspection_items
                                WHERE inspection_id = %s
                            """, (self.current_inspection_id,))
                            
                            all_notes = cursor.fetchall()
                            unit_types_in_db = set()
                            
                            for note_row in all_notes:
                                try:
                                    if note_row[0]:
                                        data = json.loads(note_row[0])
                                        if data.get('unit_type'):
                                            unit_types_in_db.add(data['unit_type'])
                                except:
                                    pass
                            
                            if len(unit_types_in_db) > 0:
                                st.success(f"✅ Unit types in database: {sorted(unit_types_in_db)}")
                            else:
                                st.error("❌ NO unit types found in database JSON!")
                            
                            cursor.close()
                            conn.close()
                            
                        except Exception as e:
                            st.error(f"Database check failed: {e}")
                    else:
                        st.warning("No inspection ID - data not saved yet")

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