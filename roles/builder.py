"""
Builder Interface - PostgreSQL/SQLite Compatible Version
=========================================================
Updated to work with connection_manager for both database types
"""

import pandas as pd
import streamlit as st
from datetime import datetime
import logging
from pathlib import Path
import os
import uuid
from reports.builder_report import add_builder_report_ui

# ✅ Import connection manager
from database.connection_manager import get_connection_manager
from core.file_storage import FileStorageManager

logger = logging.getLogger(__name__)


class BuilderInterface:
    """PostgreSQL/SQLite compatible builder interface"""
    
    def __init__(self, db_path: str = "building_inspection.db", user_info: dict = None):
        self.user_info = user_info or {}
        
        # Use connection manager instead of DatabaseManager
        self.conn_manager = get_connection_manager()
        self.db_type = self.conn_manager.db_type
        
        # Initialize file storage with connection manager
        self.file_storage = FileStorageManager(self.conn_manager)
        
        # Session state
        if 'b_building' not in st.session_state:
            st.session_state.b_building = None
        if 'b_open_form' not in st.session_state:
            st.session_state.b_open_form = None
        if 'b_active_tab' not in st.session_state:
            st.session_state.b_active_tab = 'pending'

    def show(self):
        """Main dashboard"""
        
        st.markdown("""
        <div style="background: linear-gradient(135deg, #FF6B35 0%, #F7931E 100%); 
                    color: white; padding: 1.5rem; border-radius: 10px; margin-bottom: 1rem;">
            <h2 style="margin: 0;">🏗️ Builder Work Management</h2>
            <p style="margin: 0.5rem 0 0 0;">Welcome, {}</p>
        </div>
        """.format(self.user_info.get('name', 'Builder')), unsafe_allow_html=True)
        
        # ✅ Database status indicator
        st.success(f"✅ Connected to {self.db_type.upper()} database")
                
        # Building selector
        building_id = self._select_building()
        if not building_id:
            st.info("Please select a building")
            return
        
        # Get all work orders once
        all_orders = self._get_orders(building_id)
        if all_orders.empty:
            st.info("📋 No work orders yet for this building")
            st.write("""
            **Work orders will appear here when:**
            1. Inspection data is processed
            2. Defects are identified (items marked "Not OK")
            3. Work orders are automatically created from defects
            """)
            
            # Show inspection data even without work orders
            with st.expander("🔍 View Inspection Data"):
                try:
                    inspection_query = """
                        SELECT i.id, i.inspection_date, i.inspector_name, 
                            i.total_units, i.total_defects
                        FROM inspector_inspections i
                        WHERE i.building_id = %s if self.db_type == "postgresql" else ?
                        ORDER BY i.created_at DESC
                    """
                    
                    if self.db_type == "postgresql":
                        inspections = pd.read_sql_query(
                            inspection_query.replace("?", "%s"),
                            self.conn_manager.get_connection(),
                            params=[building_id]
                        )
                    else:
                        inspections = pd.read_sql_query(
                            inspection_query.replace("%s", "?"),
                            self.conn_manager.get_connection(),
                            params=[building_id]
                        )
                    
                    if not inspections.empty:
                        st.dataframe(inspections, use_container_width=True)
                    else:
                        st.info("No inspections found")
                except Exception as e:
                    st.error(f"Error loading inspections: {e}")
            
            return
        
        # Reports section
        st.markdown("---")
        
        with st.expander("📊 Generate Defect Management Report", expanded=False):
            st.markdown("""
            <div style="background: linear-gradient(135deg, #4ECDC4 0%, #44A08D 100%); 
                        color: white; padding: 0.8rem; border-radius: 8px; margin-bottom: 1rem;">
                <h4 style="margin: 0;">📊 Reports & Analytics</h4>
                <p style="margin: 0.3rem 0 0 0; font-size: 0.85rem;">
                    Generate comprehensive Excel reports with defect analysis
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            # ✅ Pass connection manager to report UI
            add_builder_report_ui(self.conn_manager)
        
        st.markdown("---")
        
        # Show tabs with manual control
        self._show_tabs(all_orders, building_id)
    
    def _select_building(self):
        """Building selector with work order counts - shows all buildings"""
        try:
            # Use LEFT JOIN to show buildings even without work orders
            query = """
                SELECT DISTINCT b.id, b.name,
                    COUNT(CASE WHEN wo.status = 'pending' THEN 1 END) as p,
                    COUNT(CASE WHEN wo.status = 'in_progress' AND (wo.builder_notes NOT LIKE '%REJECTED%' OR wo.builder_notes IS NULL) THEN 1 END) as a,
                    COUNT(CASE WHEN wo.status = 'in_progress' AND wo.builder_notes LIKE '%REJECTED%' THEN 1 END) as r,
                    COUNT(CASE WHEN wo.status = 'waiting_approval' THEN 1 END) as w
                FROM inspector_buildings b
                LEFT JOIN inspector_inspections i ON b.id = i.building_id
                LEFT JOIN inspector_work_orders wo ON i.id = wo.inspection_id
                GROUP BY b.id, b.name
                ORDER BY b.name
            """
            
            buildings = pd.read_sql_query(query, self.conn_manager.get_connection())
            
            if buildings.empty:
                st.warning("No buildings found in the database")
                st.info("Buildings will appear here after inspection data is processed")
                return None
            
            # Create options with counts
            options = {}
            for _, r in buildings.iterrows():
                total_wo = r['p'] + r['a'] + r['r'] + r['w']
                if total_wo > 0:
                    # Show counts if work orders exist
                    label = f"{r['name']} (P:{r['p']} A:{r['a']} R:{r['r']} W:{r['w']})"
                else:
                    # Show "No work orders" if none exist
                    label = f"{r['name']} (No work orders yet)"
                options[label] = r['id']
            
            selected = st.selectbox("Building:", list(options.keys()), key="builder_building_selector")
            return options[selected]
            
        except Exception as e:
            logger.error(f"Building selector error: {e}")
            st.error(f"Error loading buildings: {str(e)}")
            return None
    
    def _get_orders(self, building_id):
        """Get all work orders - PostgreSQL/SQLite compatible"""
        # ✅ Use correct parameter placeholder
        if self.db_type == "postgresql":
            query = """
                SELECT wo.* 
                FROM inspector_work_orders wo
                JOIN inspector_inspections i ON wo.inspection_id = i.id
                WHERE i.building_id = %s
                ORDER BY wo.updated_at DESC, wo.unit
            """
        else:
            query = """
                SELECT wo.* 
                FROM inspector_work_orders wo
                JOIN inspector_inspections i ON wo.inspection_id = i.id
                WHERE i.building_id = ?
                ORDER BY wo.updated_at DESC, wo.unit
            """
        
        return pd.read_sql_query(query, self.conn_manager.get_connection(), params=[building_id])
    
    # Keep all your existing _show_tabs, _render_item, _show_form methods unchanged
    # They work with DataFrames so don't need database-specific changes
    
    def _show_tabs(self, all_orders, building_id):
        """Manual tabs - unchanged"""
        # ... keep your existing implementation ...
        
        # Filter by status
        pending = all_orders[all_orders['status'] == 'pending']
        active = all_orders[
            (all_orders['status'] == 'in_progress') & 
            (~all_orders['builder_notes'].str.contains('REJECTED', na=False))
        ]
        rejected = all_orders[
            (all_orders['status'] == 'in_progress') & 
            (all_orders['builder_notes'].str.contains('REJECTED', na=False))
        ]
        waiting = all_orders[all_orders['status'] == 'waiting_approval']
        approved = all_orders[all_orders['status'] == 'approved']
        
        # ... rest of your tabs implementation ...
        
        # Tab buttons
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            if st.button(f"📋 Pending ({len(pending)})", 
                        type="primary" if st.session_state.b_active_tab == 'pending' else "secondary",
                        use_container_width=True):
                st.session_state.b_active_tab = 'pending'
                st.rerun()
        
        with col2:
            if st.button(f"🔨 Active ({len(active)})", 
                        type="primary" if st.session_state.b_active_tab == 'in_progress' else "secondary",
                        use_container_width=True):
                st.session_state.b_active_tab = 'in_progress'
                st.rerun()
        
        with col3:
            if st.button(f"❌ Rejected ({len(rejected)})", 
                        type="primary" if st.session_state.b_active_tab == 'rejected' else "secondary",
                        use_container_width=True):
                st.session_state.b_active_tab = 'rejected'
                st.rerun()
        
        with col4:
            if st.button(f"⏳ Awaiting ({len(waiting)})", 
                        type="primary" if st.session_state.b_active_tab == 'waiting_approval' else "secondary",
                        use_container_width=True):
                st.session_state.b_active_tab = 'waiting_approval'
                st.rerun()
        
        with col5:
            if st.button(f"✅ Approved ({len(approved)})", 
                        type="primary" if st.session_state.b_active_tab == 'approved' else "secondary",
                        use_container_width=True):
                st.session_state.b_active_tab = 'approved'
                st.rerun()
        
        st.markdown("---")
        
        # Show content based on active tab
        if st.session_state.b_active_tab == 'pending':
            self._show_list(pending, 'pending', building_id)
        elif st.session_state.b_active_tab == 'in_progress':
            self._show_list(active, 'in_progress', building_id)
        elif st.session_state.b_active_tab == 'rejected':
            self._show_rejected_list(rejected, building_id)
        elif st.session_state.b_active_tab == 'waiting_approval':
            self._show_list(waiting, 'waiting_approval', building_id)
        elif st.session_state.b_active_tab == 'approved':
            self._show_list(approved, 'approved', building_id)

    def _show_rejected_list(self, orders, building_id):
        """Show rejected work orders list"""
        
        if orders.empty:
            st.success("✅ No rejected items - excellent work quality!")
            return
        
        # Alert for rejected items
        urgent_rejected = orders[orders['urgency'] == 'Urgent']
        if len(urgent_rejected) > 0:
            st.error(f"⚠️ URGENT: {len(urgent_rejected)} rejected items need immediate attention!")
        else:
            st.warning(f"⚠️ {len(orders)} items were rejected by developer - please review and fix")
        
        # Filters
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            units = ['All'] + sorted(orders['unit'].unique().tolist())
            unit_filter = st.selectbox("Unit:", units, key="u_rejected")
        
        with col2:
            trades = ['All'] + sorted(orders['trade'].unique().tolist())
            trade_filter = st.selectbox("Trade:", trades, key="t_rejected")
        
        with col3:
            rooms = ['All'] + sorted(orders['room'].unique().tolist())
            room_filter = st.selectbox("Room:", rooms, key="r_rejected")
        
        with col4:
            priorities = ['All'] + sorted(orders['urgency'].unique().tolist())
            priority_filter = st.selectbox("Priority:", priorities, key="p_rejected")
        
        # Apply filters
        filtered = orders.copy()
        if unit_filter != 'All':
            filtered = filtered[filtered['unit'] == unit_filter]
        if trade_filter != 'All':
            filtered = filtered[filtered['trade'] == trade_filter]
        if room_filter != 'All':
            filtered = filtered[filtered['room'] == room_filter]
        if priority_filter != 'All':
            filtered = filtered[filtered['urgency'] == priority_filter]
        
        st.caption(f"Showing {len(filtered)} of {len(orders)} rejected items (sorted by most recent)")
        
        # Render all items in order
        for idx, (_, order) in enumerate(filtered.iterrows()):
            self._render_rejected_item(order, idx)
    
    def _render_rejected_item(self, order, idx):
        """Render rejected work order item"""
        
        oid = order['id']
        is_open = st.session_state.b_open_form == oid
        
        # Extract rejection reason from notes
        rejection_reason = "No reason provided"
        rejection_date = "Recently"
        
        if pd.notna(order.get('builder_notes')):
            notes = str(order['builder_notes'])
            entries = notes.split('\n\n---')
            for entry in entries:
                if '❌ REJECTED' in entry or 'REJECTED' in entry:
                    lines = entry.strip().split('\n')
                    if lines:
                        first_line = lines[0].replace('---', '').strip()
                        if ' - ' in first_line:
                            parts = first_line.split(' - ')
                            if len(parts) >= 1:
                                rejection_date = parts[0].strip()
                    
                    for line in lines:
                        if line.startswith('Reason:'):
                            rejection_reason = line.replace('Reason:', '').strip()
                            break
                    break
        
        # Priority color
        priority_colors = {
            'Urgent': '🔴',
            'High Priority': '🟡',
            'Medium Priority': '🟢',
            'Low Priority': '⚪'
        }
        priority_icon = priority_colors.get(order['urgency'], '⚪')
        
        # Item row
        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
        
        with col1:
            st.markdown(f"{priority_icon} **Unit {order['unit']}** - {order['room']} - {order['component']}")
            st.caption(f"{order['trade']} | {order['urgency']}")
            st.error(f"❌ REJECTED: {rejection_reason}")
        
        with col2:
            st.caption(f"🕒 {rejection_date}")
        
        with col3:
            pass
        
        with col4:
            btn_label = "Close ✕" if is_open else "Fix ⚠️"
            btn_type = "secondary" if is_open else "primary"
            if st.button(btn_label, key=f"rej_{oid}_{idx}", type=btn_type, use_container_width=True):
                if is_open:
                    st.session_state.b_open_form = None
                else:
                    st.session_state.b_open_form = oid
                st.rerun()
        
        if is_open:
            st.markdown("---")
            with st.container():
                st.markdown("### ⚠️ Fix Rejected Work")
                self._show_rejected_form(order, idx, rejection_reason)
            st.markdown("---")
        
        st.divider()
    
    def _show_rejected_form(self, order, idx, rejection_reason):
        """Form for fixing rejected work"""
        
        oid = order['id']
        
        st.error(f"**Developer Feedback:** {rejection_reason}")
        st.info("**Action Required:** Address the rejection reason, update your work, and resubmit")
        
        st.markdown("---")
        
        with st.container():
            st.markdown(f"### 🔧 {order['unit']} - {order['room']} - {order['component']}")
            col1, col2 = st.columns(2)
            with col1:
                st.caption(f"**Trade:** {order['trade']}")
            with col2:
                st.caption(f"**Priority:** {order['urgency']}")
        
        st.markdown("---")
        
        col_left, col_right = st.columns([3, 2])
        
        with col_left:
            notes = st.text_area(
                "**Rework Notes:**", 
                height=150, 
                placeholder="Describe how you fixed the issues...",
                key=f"rej_notes_{oid}_{idx}"
            )
            
            st.markdown("**Upload New Photos/Files:**")
            files = st.file_uploader(
                "Select files", 
                type=['png', 'jpg', 'jpeg', 'pdf'],
                accept_multiple_files=True,
                key=f"rej_files_{oid}_{idx}",
                label_visibility="collapsed"
            )
            
            if files and len(files) > 0:
                st.success(f"✓ {len(files)} new file(s) selected")
                cols = st.columns(3)
                for i, file in enumerate(files):
                    with cols[i % 3]:
                        if file.type and 'image' in file.type:
                            st.image(file, use_container_width=True)
                        st.caption(f"{file.name[:20]}...")
        
        with col_right:
            target = st.date_input(
                "**Updated Completion Date:**", 
                datetime.now().date(), 
                key=f"rej_date_{oid}_{idx}"
            )
            
            st.markdown("**Status:**")
            mark_complete = st.checkbox(
                "Work Fixed & Ready",
                key=f"rej_complete_{oid}_{idx}",
                help="Check when all issues are resolved"
            )
            
            if pd.notna(order.get('builder_notes')):
                file_count = self._get_file_count(oid)
                
                with st.expander(f"📋 Full History ({file_count} files)", expanded=False):
                    notes_text = str(order['builder_notes'])
                    entries = notes_text.split('\n\n---')
                    
                    for entry in entries:
                        if not entry.strip():
                            continue
                        
                        if '❌ REJECTED' in entry or 'REJECTED' in entry:
                            st.error(entry.strip())
                        else:
                            st.text(entry.strip())
                        st.markdown("")
                    
                    if file_count > 0:
                        st.markdown("**Previous Files:**")
                        self._show_files(oid)
        
        with st.form(f"rej_form_{oid}_{idx}", clear_on_submit=False):
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                if mark_complete:
                    submit_btn = st.form_submit_button("✓ Resubmit for Approval", type="primary", use_container_width=True)
                else:
                    submit_btn = st.form_submit_button("💾 Save Rework Progress", type="primary", use_container_width=True)
            
            with col3:
                cancel_btn = st.form_submit_button("Cancel", use_container_width=True)
            
            if submit_btn:
                files_to_save = st.session_state.get(f"rej_files_{oid}_{idx}", None)
                
                if mark_complete:
                    if not notes or not notes.strip():
                        st.error("Please describe what you fixed")
                    else:
                        success, message = self._complete(oid, notes, files_to_save, target)
                        if success:
                            st.success(message)
                            st.balloons()
                            st.session_state.b_open_form = None
                            st.rerun()
                        else:
                            st.error(message)
                else:
                    success, message = self._save(oid, notes, files_to_save, target)
                    if success:
                        st.success(message)
                        st.session_state.b_open_form = None
                        st.rerun()
                    else:
                        st.error(message)
            
            elif cancel_btn:
                st.session_state.b_open_form = None
                st.rerun()
    
    def _show_list(self, orders, status, building_id):
        """Show work order list with filters"""
        
        if orders.empty:
            st.info(f"No {status.replace('_', ' ')} items")
            return
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            units = ['All'] + sorted(orders['unit'].unique().tolist())
            unit_filter = st.selectbox("Unit:", units, key=f"u_{status}")
        
        with col2:
            trades = ['All'] + sorted(orders['trade'].unique().tolist())
            trade_filter = st.selectbox("Trade:", trades, key=f"t_{status}")
        
        with col3:
            rooms = ['All'] + sorted(orders['room'].unique().tolist())
            room_filter = st.selectbox("Room:", rooms, key=f"r_{status}")
        
        with col4:
            priorities = ['All'] + sorted(orders['urgency'].unique().tolist())
            priority_filter = st.selectbox("Priority:", priorities, key=f"p_{status}")
        
        filtered = orders.copy()
        if unit_filter != 'All':
            filtered = filtered[filtered['unit'] == unit_filter]
        if trade_filter != 'All':
            filtered = filtered[filtered['trade'] == trade_filter]
        if room_filter != 'All':
            filtered = filtered[filtered['room'] == room_filter]
        if priority_filter != 'All':
            filtered = filtered[filtered['urgency'] == priority_filter]
        
        st.caption(f"Showing {len(filtered)} of {len(orders)} items (sorted by last modified)")
        
        for idx, (_, order) in enumerate(filtered.iterrows()):
            self._render_item(order, idx, status)
    
    def _render_item(self, order, idx, status):
        """Render work order item"""
        
        oid = order['id']
        is_open = st.session_state.b_open_form == oid
        
        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
        
        with col1:
            st.markdown(f"**Unit {order['unit']}** - {order['room']} - {order['component']}")
            st.caption(f"{order['trade']} | {order['urgency']}")
        
        with col2:
            if status in ['in_progress', 'waiting_approval'] and pd.notna(order.get('updated_at')):
                try:
                    updated = pd.to_datetime(order['updated_at'])
                    st.caption(f"🕒 {updated.strftime('%d/%m/%Y %H:%M')}")
                except:
                    pass
            
            if status == 'waiting_approval' and pd.notna(order.get('planned_date')):
                try:
                    planned = pd.to_datetime(order['planned_date'])
                    st.caption(f"📅 {planned.strftime('%d/%m/%Y')}")
                except:
                    st.caption(f"📅 {order['planned_date']}")
        
        with col3:
            pass
        
        with col4:
            if status == 'pending':
                if st.button("Start", key=f"start_{oid}_{idx}", type="primary", use_container_width=True):
                    self._start_work(oid)
                    return
            
            elif status == 'in_progress':
                btn_label = "Close ✕" if is_open else "Open ▼"
                btn_type = "secondary" if is_open else "primary"
                if st.button(btn_label, key=f"upd_{oid}_{idx}", type=btn_type, use_container_width=True):
                    if is_open:
                        st.session_state.b_open_form = None
                    else:
                        st.session_state.b_open_form = oid
                    st.rerun()
            
            elif status == 'waiting_approval':
                btn_label = "Close ✕" if is_open else "View 👁"
                btn_type = "secondary" if is_open else "primary"
                if st.button(btn_label, key=f"view_{oid}_{idx}", type=btn_type, use_container_width=True):
                    if is_open:
                        st.session_state.b_open_form = None
                    else:
                        st.session_state.b_open_form = oid
                    st.rerun()
            
            elif status == 'approved':
                btn_label = "Close ✕" if is_open else "View ✓"
                btn_type = "secondary" if is_open else "primary"
                if st.button(btn_label, key=f"appr_{oid}_{idx}", type=btn_type, use_container_width=True):
                    if is_open:
                        st.session_state.b_open_form = None
                    else:
                        st.session_state.b_open_form = oid
                    st.rerun()
        
        if is_open:
            st.markdown("---")
            with st.container():
                if status in ['waiting_approval', 'approved']:
                    st.markdown("### 👁 View Details (Read Only)")
                    self._show_readonly_form(order, idx)
                else:
                    st.markdown("### 📝 Work Details")
                    self._show_form(order, idx)
            st.markdown("---")
        
        st.divider()
    
    def _show_readonly_form(self, order, idx):
        """Read-only form for viewing completed/approved work"""
        
        oid = order['id']
        
        with st.container():
            st.markdown(f"### 📝 {order['unit']} - {order['room']} - {order['component']}")
            col1, col2 = st.columns(2)
            with col1:
                st.caption(f"**Trade:** {order['trade']}")
            with col2:
                st.caption(f"**Priority:** {order['urgency']}")
        
        st.markdown("---")
        
        if order['status'] == 'approved':
            st.success("✅ Work approved by developer")
        else:
            st.info("⏳ Work completed - Awaiting developer approval")
        
        col_left, col_right = st.columns([3, 2])
        
        with col_left:
            if pd.notna(order.get('builder_notes')) and str(order['builder_notes']).strip():
                st.markdown("**Work History:**")
                st.text_area(
                    "Work History", 
                    value=order['builder_notes'], 
                    height=250, 
                    disabled=True, 
                    label_visibility="collapsed", 
                    key=f"readonly_hist_{oid}_{idx}"
                )
        
        with col_right:
            if pd.notna(order.get('completed_date')):
                try:
                    completed = pd.to_datetime(order['completed_date'])
                    st.markdown(f"**Completed:** {completed.strftime('%d/%m/%Y %H:%M')}")
                except:
                    pass
            
            if pd.notna(order.get('planned_date')):
                try:
                    planned = pd.to_datetime(order['planned_date'])
                    st.markdown(f"**Planned Completion:** {planned.strftime('%d/%m/%Y')}")
                except:
                    st.markdown(f"**Planned Completion:** {order['planned_date']}")
            
            # CRITICAL: This is where files should be shown!
            st.markdown("---")
            file_count = self._get_file_count(oid)
            
            if file_count > 0:
                st.markdown(f"**📎 Uploaded Files ({file_count}):**")
                st.markdown("---")
                
                # Call _show_files to display the files
                self._show_files(oid)
            else:
                st.info("No files uploaded for this work order")
        
        # Close button
        col1, col2, col3 = st.columns([2, 1, 1])
        with col3:
            if st.button("Close", key=f"readonly_close_{oid}_{idx}", use_container_width=True):
                st.session_state.b_open_form = None
                st.rerun()
    
    def _show_form(self, order, idx):
        """Detail form for active work"""
        
        oid = order['id']
        
        with st.container():
            st.markdown(f"### 🔧 {order['unit']} - {order['room']} - {order['component']}")
            col1, col2 = st.columns(2)
            with col1:
                st.caption(f"**Trade:** {order['trade']}")
            with col2:
                st.caption(f"**Priority:** {order['urgency']}")
        
        st.markdown("---")
        
        col_left, col_right = st.columns([3, 2])
        
        with col_left:
            notes = st.text_area(
                "**Work Notes:**", 
                height=150, 
                placeholder="Describe the work you performed...",
                key=f"form_notes_{oid}_{idx}"
            )
            
            st.markdown("**Upload Photos/Files:**")
            files = st.file_uploader(
                "Select files", 
                type=['png', 'jpg', 'jpeg', 'pdf'],
                accept_multiple_files=True,
                key=f"form_files_{oid}_{idx}",
                label_visibility="collapsed"
            )
            
            if files and len(files) > 0:
                st.success(f"✓ {len(files)} file(s) selected")
                cols = st.columns(3)
                for i, file in enumerate(files):
                    with cols[i % 3]:
                        if file.type and 'image' in file.type:
                            st.image(file, use_container_width=True)
                        st.caption(f"{file.name[:20]}...")
        
        with col_right:
            target = st.date_input(
                "**Planned Completion:**", 
                datetime.now().date(), 
                key=f"form_date_{oid}_{idx}",
                help="When do you plan to complete this work?"
            )
            
            st.markdown("**Status:**")
            mark_complete = st.checkbox(
                "Completed Work",
                key=f"form_complete_{oid}_{idx}",
                help="Check when all work is finished"
            )
            
            # FIXED: Show history with files
            if pd.notna(order.get('builder_notes')) and str(order['builder_notes']).strip():
                file_count = self._get_file_count(oid)
                history_label = f"📋 History & Files ({file_count} files)" if file_count > 0 else "📋 History"
                
                with st.expander(history_label, expanded=False):
                    # Show work history notes
                    st.markdown("**Work History:**")
                    st.text_area(
                        "History", 
                        value=order['builder_notes'], 
                        height=200, 
                        disabled=True, 
                        label_visibility="collapsed", 
                        key=f"form_hist_{oid}_{idx}"
                    )
                    
                    # CRITICAL: Show uploaded files
                    if file_count > 0:
                        st.markdown("---")
                        st.markdown("**Previously Uploaded Files:**")
                        self._show_files(oid)
        
        with st.form(f"work_form_{oid}_{idx}", clear_on_submit=False):
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                if mark_complete:
                    submit_btn = st.form_submit_button("✓ Complete & Submit", type="primary", use_container_width=True)
                else:
                    submit_btn = st.form_submit_button("💾 Save Progress", type="primary", use_container_width=True)
            
            with col3:
                cancel_btn = st.form_submit_button("Cancel", use_container_width=True)
            
            if submit_btn:
                files_to_save = st.session_state.get(f"form_files_{oid}_{idx}", None)
                
                if mark_complete:
                    if not notes or not notes.strip():
                        st.error("Notes required to complete")
                    else:
                        success, message = self._complete(oid, notes, files_to_save, target)
                        if success:
                            st.success(message)
                            st.balloons()
                            st.session_state.b_open_form = None
                            st.rerun()
                        else:
                            st.error(message)
                else:
                    success, message = self._save(oid, notes, files_to_save, target)
                    if success:
                        st.success(message)
                        st.session_state.b_open_form = None
                        st.rerun()
                    else:
                        st.error(message)
            
            elif cancel_btn:
                st.session_state.b_open_form = None
                st.rerun()
    
    def _start_work(self, oid):
        """Start work and auto-open form"""
        try:
            with self.conn_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                if self.db_type == "postgresql":
                    cursor.execute("""
                        UPDATE inspector_work_orders 
                        SET status = 'in_progress', started_date = NOW(), updated_at = NOW()
                        WHERE id = %s
                    """, (oid,))
                else:
                    cursor.execute("""
                        UPDATE inspector_work_orders 
                        SET status = 'in_progress', started_date = datetime('now'), updated_at = datetime('now')
                        WHERE id = ?
                    """, (oid,))
                
                conn.commit()
            
            st.session_state.b_active_tab = 'in_progress'
            st.session_state.b_open_form = oid
            
            st.success("Work started! Switching to Active tab...")
            st.rerun()
            
        except Exception as e:
            logger.error(f"Start work error: {e}")
            st.error(f"Failed to start work: {str(e)}")
    
    def _save(self, oid, notes, files, target):
        """Save progress - PostgreSQL/SQLite compatible"""
        try:
            with self.conn_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get current notes
                if self.db_type == "postgresql":
                    cursor.execute("SELECT builder_notes FROM inspector_work_orders WHERE id = %s", (oid,))
                else:
                    cursor.execute("SELECT builder_notes FROM inspector_work_orders WHERE id = ?", (oid,))
                
                result = cursor.fetchone()
                old_notes = result[0] if result and result[0] else ""
                
                # Build new entry
                ts = datetime.now().strftime("%d/%m/%Y %H:%M")
                user = self.user_info.get('name', 'Builder')
                
                entry = f"\n\n--- {ts} - {user} ---"
                if notes and notes.strip():
                    entry += f"\n{notes.strip()}"
                
                # CRITICAL: Save files if provided
                if files and len(files) > 0:
                    saved_filenames = self._save_files(oid, files, cursor)
                    if saved_filenames:
                        entry += f"\n📎 Uploaded {len(saved_filenames)} file(s): {', '.join(saved_filenames)}"
                
                new_notes = f"{old_notes}{entry}"
                
                # Update database
                if self.db_type == "postgresql":
                    query = """
                        UPDATE inspector_work_orders 
                        SET builder_notes = %s, planned_date = %s, updated_at = NOW()
                        WHERE id = %s
                    """
                    cursor.execute(query, (new_notes, str(target), oid))
                else:
                    query = """
                        UPDATE inspector_work_orders 
                        SET builder_notes = ?, planned_date = ?, updated_at = datetime('now')
                        WHERE id = ?
                    """
                    cursor.execute(query, (new_notes, str(target), oid))
                
                conn.commit()
            
            return True, "Progress saved!"
                
        except Exception as e:
            logger.error(f"Save error: {e}")
            return False, f"Save failed: {str(e)}"
    
    def _complete(self, oid, notes, files, target):
        """Complete work - PostgreSQL/SQLite compatible"""
        try:
            with self.conn_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get current notes
                if self.db_type == "postgresql":
                    cursor.execute("SELECT builder_notes FROM inspector_work_orders WHERE id = %s", (oid,))
                else:
                    cursor.execute("SELECT builder_notes FROM inspector_work_orders WHERE id = ?", (oid,))
                
                result = cursor.fetchone()
                old_notes = result[0] if result and result[0] else ""
                
                # Build completion entry
                ts = datetime.now().strftime("%d/%m/%Y %H:%M")
                user = self.user_info.get('name', 'Builder')
                
                entry = f"\n\n--- {ts} - {user} ---"
                entry += f"\n{notes.strip()}"
                
                # CRITICAL: Save files if provided
                if files and len(files) > 0:
                    saved_filenames = self._save_files(oid, files, cursor)
                    if saved_filenames:
                        entry += f"\n📎 Uploaded {len(saved_filenames)} file(s): {', '.join(saved_filenames)}"
                
                entry += f"\n✓ STATUS: COMPLETED - Awaiting Approval"
                
                new_notes = f"{old_notes}{entry}"
                
                # Update database
                if self.db_type == "postgresql":
                    query = """
                        UPDATE inspector_work_orders 
                        SET builder_notes = %s, planned_date = %s, 
                            status = 'waiting_approval', completed_date = NOW(), updated_at = NOW()
                        WHERE id = %s
                    """
                    cursor.execute(query, (new_notes, str(target), oid))
                else:
                    query = """
                        UPDATE inspector_work_orders 
                        SET builder_notes = ?, planned_date = ?, 
                            status = 'waiting_approval', completed_date = datetime('now'), updated_at = datetime('now')
                        WHERE id = ?
                    """
                    cursor.execute(query, (new_notes, str(target), oid))
                
                conn.commit()
            
            return True, "Work completed!"
                
        except Exception as e:
            logger.error(f"Complete error: {e}")
            return False, f"Complete failed: {str(e)}"
    
    def _save_files(self, oid, files, cursor):
        """Save files using FileStorageManager - returns list of filenames"""
        if not files or len(files) == 0:
            return []
        
        try:
            user = self.user_info.get('name', 'Builder')
            
            # Use FileStorageManager to save files
            saved_files = self.file_storage.save_files(
                work_order_id=oid,
                uploaded_files=files,
                uploaded_by=user,
                category='progress'
            )
            
            # Return list of original filenames for notes
            return [f['original_filename'] for f in saved_files]
            
        except Exception as e:
            logger.error(f"Error saving files: {e}")
            return []
            return []
    
    def _get_file_count(self, oid):
        """Get count of uploaded files using FileStorageManager"""
        try:
            return self.file_storage.get_file_count(oid)
        except Exception as e:
            logger.error(f"File count error: {e}")
            return 0
    
    def _show_files(self, oid):
        """Display uploaded files using FileStorageManager - FIXED for correct column names"""
        import streamlit as st
        from pathlib import Path
        
        try:
            # Get files from FileStorageManager
            files = self.file_storage.get_files(oid)
            
            if not files:
                st.info("No files uploaded yet")
                return
            
            # Separate images and documents
            images = []
            documents = []
            
            for file in files:
                # Use 'file_path' from your file_storage.py
                file_path = Path(file['file_path'])
                
                if not file_path.exists():
                    logger.warning(f"File not found: {file_path}")
                    st.warning(f"⚠️ File not found: {file['original_filename']}")
                    continue
                
                file_type = file.get('file_type', '').lower()
                
                # Check if it's an image (by MIME type or extension)
                is_image = (
                    'image' in file_type or 
                    file_path.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
                )
                
                if is_image:
                    images.append({
                        'filename': file['original_filename'],
                        'path': file_path,
                        'created': file.get('created_at', 'Unknown'),
                        'uploaded_by': file.get('uploaded_by', 'Unknown'),
                        'category': file.get('upload_category', 'progress')
                    })
                else:
                    documents.append({
                        'filename': file['original_filename'],
                        'path': file_path,
                        'created': file.get('created_at', 'Unknown'),
                        'uploaded_by': file.get('uploaded_by', 'Unknown'),
                        'category': file.get('upload_category', 'progress')
                    })
            
            # Display images in grid
            if images:
                st.markdown("**📸 Images:**")
                cols = st.columns(3)
                
                for idx, img in enumerate(images):
                    with cols[idx % 3]:
                        try:
                            # Read and display image
                            with open(img['path'], 'rb') as f:
                                image_data = f.read()
                            
                            st.image(image_data, caption=img['filename'], use_container_width=True)
                            st.caption(f"👤 {img['uploaded_by']}")
                            
                            # Format timestamp
                            try:
                                from datetime import datetime
                                if isinstance(img['created'], str):
                                    # Handle both formats
                                    created_str = img['created'].replace('Z', '+00:00')
                                    if 'T' in created_str:
                                        created_dt = datetime.fromisoformat(created_str)
                                    else:
                                        created_dt = datetime.strptime(created_str, '%Y-%m-%d %H:%M:%S')
                                    st.caption(f"🕒 {created_dt.strftime('%d/%m/%Y %H:%M')}")
                                else:
                                    st.caption(f"🕒 {img['created']}")
                            except Exception as date_err:
                                st.caption(f"🕒 {img['created']}")
                            
                            # Show category badge
                            if img['category'] != 'progress':
                                st.caption(f"🏷️ {img['category']}")
                                
                        except Exception as e:
                            logger.error(f"Cannot display {img['filename']}: {e}")
                            st.error(f"❌ Error displaying image")
                            st.caption(f"{img['filename']}")
            
            # Display documents as list
            if documents:
                st.markdown("**📄 Documents:**")
                
                for doc in documents:
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.text(f"📄 {doc['filename']}")
                        st.caption(f"👤 {doc['uploaded_by']}")
                        
                        # Format timestamp
                        try:
                            from datetime import datetime
                            if isinstance(doc['created'], str):
                                created_str = doc['created'].replace('Z', '+00:00')
                                if 'T' in created_str:
                                    created_dt = datetime.fromisoformat(created_str)
                                else:
                                    created_dt = datetime.strptime(created_str, '%Y-%m-%d %H:%M:%S')
                                st.caption(f"🕒 {created_dt.strftime('%d/%m/%Y %H:%M')}")
                            else:
                                st.caption(f"🕒 {doc['created']}")
                        except:
                            st.caption(f"🕒 {doc['created']}")
                        
                        # Show category badge
                        if doc['category'] != 'progress':
                            st.caption(f"🏷️ {doc['category']}")
                    
                    with col2:
                        # Download button
                        try:
                            with open(doc['path'], 'rb') as f:
                                file_data = f.read()
                                st.download_button(
                                    "⬇️ Download",
                                    data=file_data,
                                    file_name=doc['filename'],
                                    mime="application/octet-stream",
                                    key=f"download_{oid}_{doc['path'].stem}",
                                    use_container_width=True
                                )
                        except Exception as e:
                            logger.error(f"Cannot prepare download for {doc['filename']}: {e}")
                            st.error("Download error")
            
            # Summary
            total_files = len(images) + len(documents)
            if total_files > 0:
                st.success(f"✅ {len(images)} image(s) and {len(documents)} document(s)")
            
        except Exception as e:
            logger.error(f"Error displaying files: {e}")
            st.error(f"⚠️ Error loading files: {str(e)}")
            
            # Debug helper
            import traceback
            with st.expander("🔧 Debug Info"):
                st.code(f"""
    Work Order ID: {oid}
    Error: {str(e)}

    Stack trace:
    {traceback.format_exc()}
                """)


def render_builder_interface(user_info=None, auth_manager=None):
    """Entry point"""
    if 'builder_int' not in st.session_state:
        st.session_state.builder_int = BuilderInterface(user_info=user_info)
    
    st.session_state.builder_int.user_info = user_info
    st.session_state.builder_int.show()