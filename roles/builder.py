"""
Builder Interface - Complete with Rejected Status
=================================================

Fixed: Manual tab control, file preview, Start workflow, Rejected items view
"""

import pandas as pd
import streamlit as st
from datetime import datetime
import logging
from pathlib import Path
import os
import uuid
from io import BytesIO

try:
    from database.setup import DatabaseManager
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False

logger = logging.getLogger(__name__)


class BuilderInterface:
    """Working builder interface with rejected items support"""
    
    def __init__(self, db_path: str = "building_inspection.db", user_info: dict = None):
        self.user_info = user_info or {}
        self.db = DatabaseManager(db_path) if DATABASE_AVAILABLE else None
        
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
            <h2 style="margin: 0;">Builder Work Management</h2>
            <p style="margin: 0.5rem 0 0 0;">Welcome, {}</p>
        </div>
        """.format(self.user_info.get('name', 'Builder')), unsafe_allow_html=True)
        
        # Building selector
        building_id = self._select_building()
        if not building_id:
            st.info("Please select a building")
            return
        
        # ⭐ ADD THIS LINE ⭐
        self._show_download_section()
        
        # Get all work orders once
        all_orders = self._get_orders(building_id)
        if all_orders.empty:
            st.warning("No work orders found")
            return
        
        # Show tabs with manual control
        self._show_tabs(all_orders, building_id)
    
    def _select_building(self):
        """Building selector with rejected count"""
        conn = self.db.connect()
        
        buildings = pd.read_sql_query("""
            SELECT DISTINCT b.id, b.name,
                COUNT(CASE WHEN wo.status = 'pending' THEN 1 END) as p,
                COUNT(CASE WHEN wo.status = 'in_progress' AND (wo.builder_notes NOT LIKE '%REJECTED%' OR wo.builder_notes IS NULL) THEN 1 END) as a,
                COUNT(CASE WHEN wo.status = 'in_progress' AND wo.builder_notes LIKE '%REJECTED%' THEN 1 END) as r,
                COUNT(CASE WHEN wo.status = 'waiting_approval' THEN 1 END) as w
            FROM inspector_buildings b
            JOIN inspector_inspections i ON b.id = i.building_id
            JOIN inspector_work_orders wo ON i.id = wo.inspection_id
            GROUP BY b.id, b.name
        """, conn)
        
        if buildings.empty:
            return None
        
        options = {f"{r['name']} (P:{r['p']} A:{r['a']} R:{r['r']} W:{r['w']})": r['id'] 
                   for _, r in buildings.iterrows()}
        
        selected = st.selectbox("Building:", list(options.keys()))
        return options[selected]
    
    def _get_orders(self, building_id):
        """Get all work orders"""
        conn = self.db.connect()
        return pd.read_sql_query("""
            SELECT wo.* 
            FROM inspector_work_orders wo
            JOIN inspector_inspections i ON wo.inspection_id = i.id
            WHERE i.building_id = ?
            ORDER BY 
                wo.updated_at DESC,
                CASE wo.urgency WHEN 'Urgent' THEN 1 WHEN 'High Priority' THEN 2 ELSE 3 END,
                wo.unit
        """, conn, params=[building_id])
    
    def _show_tabs(self, all_orders, building_id):
        """Manual tabs with buttons for switching - INCLUDES REJECTED TAB"""
        
        # Filter by status - SEPARATE REJECTED FROM IN_PROGRESS
        pending = all_orders[all_orders['status'] == 'pending']
        
        # Active = in_progress WITHOUT rejection
        active = all_orders[
            (all_orders['status'] == 'in_progress') & 
            (~all_orders['builder_notes'].str.contains('REJECTED', na=False))
        ]
        
        # Rejected = in_progress WITH rejection
        rejected = all_orders[
            (all_orders['status'] == 'in_progress') & 
            (all_orders['builder_notes'].str.contains('REJECTED', na=False))
        ]
        
        waiting = all_orders[all_orders['status'] == 'waiting_approval']
        approved = all_orders[all_orders['status'] == 'approved']
        
        # Tab buttons - ADDED REJECTED BUTTON
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
            # REJECTED TAB
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
        
        # Show content based on active tab - ADDED REJECTED CASE
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
        """Show rejected work orders list - SPECIAL HANDLING"""
        
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
        """Render rejected work order item - WITH REJECTION DETAILS"""
        
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
        
        # Item row with rejection warning
        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
        
        with col1:
            st.markdown(f"{priority_icon} **Unit {order['unit']}** - {order['room']} - {order['component']}")
            st.caption(f"{order['trade']} | {order['urgency']}")
            st.error(f"❌ REJECTED: {rejection_reason}")
        
        with col2:
            st.caption(f"🕒 {rejection_date}")
        
        with col3:
            # Empty column
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
        
        # Show form INLINE when opened
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
        
        # Show rejection details prominently
        st.error(f"**Developer Feedback:** {rejection_reason}")
        st.info("**Action Required:** Address the rejection reason, update your work, and resubmit")
        
        st.markdown("---")
        
        # DEFECT INFO
        with st.container():
            st.markdown(f"### 🔧 {order['unit']} - {order['room']} - {order['component']}")
            col1, col2 = st.columns(2)
            with col1:
                st.caption(f"**Trade:** {order['trade']}")
            with col2:
                st.caption(f"**Priority:** {order['urgency']}")
        
        st.markdown("---")
        
        # MAIN FORM
        col_left, col_right = st.columns([3, 2])
        
        with col_left:
            # Work Notes - for rework
            notes = st.text_area(
                "**Rework Notes:**", 
                height=150, 
                placeholder="Describe how you fixed the issues...",
                key=f"rej_notes_{oid}_{idx}"
            )
            
            # File Upload
            st.markdown("**Upload New Photos/Files:**")
            files = st.file_uploader(
                "Select files", 
                type=['png', 'jpg', 'jpeg', 'pdf'],
                accept_multiple_files=True,
                key=f"rej_files_{oid}_{idx}",
                label_visibility="collapsed"
            )
            
            # File Preview
            if files and len(files) > 0:
                st.success(f"✓ {len(files)} new file(s) selected")
                cols = st.columns(3)
                for i, file in enumerate(files):
                    with cols[i % 3]:
                        if file.type and 'image' in file.type:
                            st.image(file, use_container_width=True)
                        st.caption(f"{file.name[:20]}...")
        
        with col_right:
            # Planned Finish Date
            target = st.date_input(
                "**Updated Completion Date:**", 
                datetime.now().date(), 
                key=f"rej_date_{oid}_{idx}"
            )
            
            # Completion Status
            st.markdown("**Status:**")
            mark_complete = st.checkbox(
                "Work Fixed & Ready",
                key=f"rej_complete_{oid}_{idx}",
                help="Check when all issues are resolved"
            )
            
            # Work History with rejection highlighted
            if pd.notna(order.get('builder_notes')):
                file_count = self._get_file_count(oid)
                
                with st.expander(f"📋 Full History ({file_count} files)", expanded=False):
                    # Parse and display history with rejection highlighted
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
        
        # Form Buttons
        with st.form(f"rej_form_{oid}_{idx}", clear_on_submit=False):
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                if mark_complete:
                    submit_btn = st.form_submit_button("✓ Resubmit for Approval", type="primary", use_container_width=True)
                else:
                    submit_btn = st.form_submit_button("💾 Save Rework Progress", type="primary", use_container_width=True)
            
            with col3:
                cancel_btn = st.form_submit_button("Cancel", use_container_width=True)
            
            # Handle submission
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
        
        # Filters
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
        
        st.caption(f"Showing {len(filtered)} of {len(orders)} items (sorted by last modified)")
        
        # Render all items in order
        for idx, (_, order) in enumerate(filtered.iterrows()):
            self._render_item(order, idx, status)
    
    def _render_item(self, order, idx, status):
        """Render work order item"""
        
        oid = order['id']
        is_open = st.session_state.b_open_form == oid
        
        # Item row
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
        
        # Show form INLINE when opened
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
            st.markdown(f"### 🔍 {order['unit']} - {order['room']} - {order['component']}")
            col1, col2 = st.columns(2)
            with col1:
                st.caption(f"**Trade:** {order['trade']}")
            with col2:
                st.caption(f"**Priority:** {order['urgency']}")
        
        st.markdown("---")
        
        # Show status
        if order['status'] == 'approved':
            st.success("✅ Work approved by developer")
        else:
            st.success("✓ Work completed - Awaiting developer approval")
        
        col_left, col_right = st.columns([3, 2])
        
        with col_left:
            if pd.notna(order.get('builder_notes')) and str(order['builder_notes']).strip():
                st.markdown("**Work History:**")
                st.text_area("", value=order['builder_notes'], 
                           height=250, disabled=True, 
                           label_visibility="collapsed", key=f"readonly_hist_{oid}_{idx}")
        
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
            
            file_count = self._get_file_count(oid)
            if file_count > 0:
                st.markdown("---")
                st.markdown(f"**Uploaded Files ({file_count}):**")
                self._show_files(oid)
        
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
            
            if pd.notna(order.get('builder_notes')) and str(order['builder_notes']).strip():
                file_count = self._get_file_count(oid)
                history_label = f"History ({file_count} files)" if file_count > 0 else "History"
                
                with st.expander(f"📋 {history_label}"):
                    st.text_area("", value=order['builder_notes'], 
                               height=200, disabled=True, 
                               label_visibility="collapsed", key=f"form_hist_{oid}_{idx}")
                    
                    if file_count > 0:
                        st.markdown("**Files:**")
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
            conn = self.db.connect()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE inspector_work_orders 
                SET status = 'in_progress', started_date = ?, updated_at = ?
                WHERE id = ?
            """, (datetime.now().isoformat(), datetime.now().isoformat(), oid))
            
            conn.commit()
            
            st.session_state.b_active_tab = 'in_progress'
            st.session_state.b_open_form = oid
            
            st.success("✓ Work started! Switching to Active tab...")
            st.rerun()
            
        except Exception as e:
            logger.error(f"Start work error: {e}")
            st.error(f"Failed to start work: {str(e)}")
    
    def _save(self, oid, notes, files, target):
        """Save progress - returns (success, message)"""
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            
            file_names = []
            if files and len(files) > 0:
                try:
                    file_names = self._save_files(oid, files, cursor)
                except Exception as e:
                    logger.error(f"File save error: {e}")
                    return False, f"File save failed: {str(e)}"
            
            ts = datetime.now().strftime("%d/%m/%Y %H:%M")
            user = self.user_info.get('name', 'Builder')
            
            entry = f"\n\n--- {ts} - {user} ---"
            if notes and notes.strip():
                entry += f"\n{notes.strip()}"
            else:
                entry += "\n(Progress update - no notes)"
            
            if file_names:
                entry += f"\n📎 Files: {', '.join(file_names)}"
            
            if isinstance(target, str):
                try:
                    target_formatted = pd.to_datetime(target).strftime('%d/%m/%Y')
                except:
                    target_formatted = target
            else:
                target_formatted = target.strftime('%d/%m/%Y')
            
            entry += f"\n📅 Planned Completion: {target_formatted}"
            entry += f"\n📊 Status: Progress Saved"
            
            cursor.execute("SELECT builder_notes FROM inspector_work_orders WHERE id = ?", (oid,))
            result = cursor.fetchone()
            old_notes = result[0] if result and result[0] else ""
            
            new_notes = f"{old_notes}{entry}"
            cursor.execute("""
                UPDATE inspector_work_orders 
                SET builder_notes = ?, planned_date = ?, updated_at = ?
                WHERE id = ?
            """, (new_notes, str(target), datetime.now().isoformat(), oid))
            
            conn.commit()
            
            if cursor.rowcount > 0:
                return True, "Progress saved successfully!"
            else:
                return False, "No changes made"
                
        except Exception as e:
            logger.error(f"Save error: {e}")
            return False, f"Save failed: {str(e)}"
    
    def _complete(self, oid, notes, files, target):
        """Complete work - returns (success, message)"""
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            
            file_names = []
            if files and len(files) > 0:
                try:
                    file_names = self._save_files(oid, files, cursor)
                except Exception as e:
                    logger.error(f"File save error: {e}")
                    return False, f"File save failed: {str(e)}"
            
            ts = datetime.now().strftime("%d/%m/%Y %H:%M")
            user = self.user_info.get('name', 'Builder')
            
            entry = f"\n\n--- {ts} - {user} ---"
            entry += f"\n{notes.strip()}"
            if file_names:
                entry += f"\n📎 Files: {', '.join(file_names)}"
            
            if isinstance(target, str):
                try:
                    target_formatted = pd.to_datetime(target).strftime('%d/%m/%Y')
                except:
                    target_formatted = target
            else:
                target_formatted = target.strftime('%d/%m/%Y')
            
            entry += f"\n✓ Planned Completion: {target_formatted}"
            entry += f"\n📊 STATUS: COMPLETED - Awaiting Developer Approval"
            
            cursor.execute("SELECT builder_notes FROM inspector_work_orders WHERE id = ?", (oid,))
            result = cursor.fetchone()
            old_notes = result[0] if result and result[0] else ""
            
            new_notes = f"{old_notes}{entry}"
            cursor.execute("""
                UPDATE inspector_work_orders 
                SET builder_notes = ?, planned_date = ?, 
                    status = 'waiting_approval', completed_date = ?, updated_at = ?
                WHERE id = ?
            """, (new_notes, str(target), datetime.now().isoformat(), datetime.now().isoformat(), oid))
            
            conn.commit()
            
            if cursor.rowcount > 0:
                return True, "Work completed and submitted for approval!"
            else:
                return False, "No changes made"
                
        except Exception as e:
            logger.error(f"Complete error: {e}")
            return False, f"Complete failed: {str(e)}"
    
    def _save_files(self, oid, files, cursor):
        """Save files to disk and database - returns list of filenames"""
        upload_dir = Path("uploads/work_orders") / str(oid)
        upload_dir.mkdir(parents=True, exist_ok=True)
        
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='work_order_files'
        """)
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            cursor.execute("""
                CREATE TABLE work_order_files (
                    id TEXT PRIMARY KEY,
                    work_order_id TEXT,
                    original_filename TEXT,
                    file_path TEXT,
                    file_type TEXT,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        
        cursor.execute("PRAGMA table_info(work_order_files)")
        columns = {row[1] for row in cursor.fetchall()}
        
        saved = []
        for f in files:
            try:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                ext = Path(f.name).suffix
                stored = f"{ts}_{uuid.uuid4().hex[:6]}{ext}"
                fpath = upload_dir / stored
                
                with open(fpath, "wb") as out:
                    out.write(f.getbuffer())
                
                if 'original_filename' in columns and 'file_path' in columns and 'file_type' in columns:
                    cursor.execute("""
                        INSERT INTO work_order_files (id, work_order_id, original_filename, file_path, file_type)
                        VALUES (?, ?, ?, ?, ?)
                    """, (str(uuid.uuid4()), str(oid), f.name, str(fpath), f.type or 'file'))
                else:
                    cursor.execute("""
                        INSERT INTO work_order_files (id, work_order_id)
                        VALUES (?, ?)
                    """, (str(uuid.uuid4()), str(oid)))
                
                saved.append(f.name)
                logger.info(f"Saved file: {f.name} -> {fpath}")
            except Exception as e:
                logger.error(f"Error saving file {f.name}: {e}")
                raise
        
        return saved
    
    def _get_file_count(self, oid):
        """Get count of uploaded files for a work order"""
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='work_order_files'
            """)
            if not cursor.fetchone():
                return 0
            
            cursor.execute("""
                SELECT COUNT(*) FROM work_order_files 
                WHERE work_order_id = ?
            """, (str(oid),))
            
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"File count error: {e}")
            return 0
    
    def _show_files(self, oid):
        """Display uploaded files from database"""
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='work_order_files'
            """)
            if not cursor.fetchone():
                return
            
            cursor.execute("PRAGMA table_info(work_order_files)")
            columns = {row[1] for row in cursor.fetchall()}
            
            if 'original_filename' in columns and 'file_path' in columns and 'file_type' in columns:
                cursor.execute("""
                    SELECT original_filename, file_path, file_type 
                    FROM work_order_files 
                    WHERE work_order_id = ?
                    ORDER BY uploaded_at DESC
                """, (str(oid),))
                
                files = cursor.fetchall()
                
                if not files:
                    return
                
                images = []
                others = []
                
                for fname, fpath, ftype in files:
                    if not fpath or fpath == 'NULL' or fpath == 'None':
                        if fname:
                            others.append(f"{fname} (no path)")
                        continue
                    
                    fpath_str = str(fpath)
                    if os.path.exists(fpath_str):
                        if ftype and 'image' in str(ftype).lower():
                            images.append((fname or "Image", fpath_str))
                        else:
                            others.append(fname or "File")
                    else:
                        logger.warning(f"File not found: {fpath_str}")
                        others.append(f"{fname or 'File'} (missing)")
                
                if images:
                    st.markdown("**Images:**")
                    cols = st.columns(2)
                    for i, (fname, fpath) in enumerate(images):
                        with cols[i % 2]:
                            try:
                                st.image(fpath, caption=fname, use_container_width=True)
                            except Exception as e:
                                logger.error(f"Error displaying {fpath}: {e}")
                                st.caption(f"📄 {fname} (can't display)")
                
                if others:
                    if images:
                        st.markdown("**Other Files:**")
                    for fname in others:
                        st.caption(f"📄 {fname}")
                        
        except Exception as e:
            logger.error(f"File display error: {e}")

    # ============================================================================
    # DOWNLOAD SECTION - COPY FROM HERE
    # ============================================================================

    def _show_download_section(self):
        """Download reports section - WORKING VERSION"""
        
        if not st.session_state.b_building:
            return
        
        st.markdown("### 📥 Export Reports")
        st.caption("Download work lists and reports for offline use")
        
        col1, col2, col3, col4 = st.columns(4)
        
        # DAILY WORK LIST
        with col1:
            with st.spinner(""):
                excel_data = self._generate_daily_work_list(st.session_state.b_building)
            
            if excel_data:
                st.download_button(
                    "📋 Daily Work List",
                    data=excel_data,
                    file_name=f"daily_worklist_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary"
                )
            else:
                st.button("📋 Daily Work List", disabled=True, use_container_width=True, 
                        help="No pending/active items")
        
        # REJECTED ITEMS
        with col2:
            with st.spinner(""):
                excel_data = self._generate_rejected_report(st.session_state.b_building)
            
            if excel_data:
                st.download_button(
                    "⚠️ Rejected Items",
                    data=excel_data,
                    file_name=f"rejected_items_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            else:
                st.button("⚠️ Rejected Items", disabled=True, use_container_width=True,
                        help="No rejected items")
        
        # COMPLETION PACKAGE
        with col3:
            with st.spinner(""):
                excel_data = self._generate_completion_package(st.session_state.b_building)
            
            if excel_data:
                st.download_button(
                    "✅ Completion Package",
                    data=excel_data,
                    file_name=f"completion_package_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            else:
                st.button("✅ Completion Package", disabled=True, use_container_width=True,
                        help="No items awaiting approval")
        
        # PERFORMANCE REPORT
        with col4:
            with st.spinner(""):
                excel_data = self._generate_performance_report(st.session_state.b_building)
            
            if excel_data:
                st.download_button(
                    "📊 My Performance",
                    data=excel_data,
                    file_name=f"performance_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            else:
                st.button("📊 My Performance", disabled=True, use_container_width=True,
                        help="No data available")
        
        st.markdown("---")

    def _generate_daily_work_list(self, building_id):
        """Generate Excel daily work list"""
        try:
            conn = self.db.connect()
            
            work_df = pd.read_sql_query("""
                SELECT 
                    wo.unit, wo.room, wo.component, wo.trade, wo.urgency,
                    wo.status, wo.planned_date, wo.notes, b.name as building_name
                FROM inspector_work_orders wo
                JOIN inspector_inspections i ON wo.inspection_id = i.id
                JOIN inspector_buildings b ON i.building_id = b.id
                WHERE i.building_id = ?
                AND wo.status IN ('pending', 'in_progress')
                AND (wo.builder_notes NOT LIKE '%REJECTED%' OR wo.builder_notes IS NULL)
                ORDER BY 
                    CASE wo.urgency WHEN 'Urgent' THEN 1 WHEN 'High Priority' THEN 2 ELSE 3 END,
                    wo.unit
            """, conn, params=[building_id])
            
            if work_df.empty:
                return None
            
            output = BytesIO()
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Summary sheet
                summary_data = {
                    'Report': ['Builder Daily Work List'],
                    'Building': [work_df['building_name'].iloc[0]],
                    'Builder': [self.user_info.get('name', 'Builder')],
                    'Date': [datetime.now().strftime('%d/%m/%Y %H:%M')],
                    'Total Items': [len(work_df)],
                    'Urgent': [len(work_df[work_df['urgency'] == 'Urgent'])],
                    'High Priority': [len(work_df[work_df['urgency'] == 'High Priority'])],
                    'Normal': [len(work_df[work_df['urgency'] == 'Normal'])]
                }
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                
                # Urgent items
                urgent = work_df[work_df['urgency'] == 'Urgent']
                if not urgent.empty:
                    urgent[['unit', 'room', 'component', 'trade', 'status', 'planned_date']].to_excel(
                        writer, sheet_name='URGENT', index=False
                    )
                
                # All items
                work_df[['unit', 'room', 'component', 'trade', 'urgency', 'status', 'planned_date']].to_excel(
                    writer, sheet_name='All Items', index=False
                )
            
            output.seek(0)
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Error generating work list: {e}")
            st.error(f"Failed to generate: {e}")
            return None


    def _generate_rejected_report(self, building_id):
        """Generate rejected items report"""
        try:
            conn = self.db.connect()
            
            rejected_df = pd.read_sql_query("""
                SELECT 
                    wo.unit, wo.room, wo.component, wo.trade, wo.urgency,
                    wo.builder_notes, b.name as building_name
                FROM inspector_work_orders wo
                JOIN inspector_inspections i ON wo.inspection_id = i.id
                JOIN inspector_buildings b ON i.building_id = b.id
                WHERE i.building_id = ?
                AND wo.status = 'in_progress'
                AND wo.builder_notes LIKE '%REJECTED%'
                ORDER BY 
                    CASE wo.urgency WHEN 'Urgent' THEN 1 WHEN 'High Priority' THEN 2 ELSE 3 END
            """, conn, params=[building_id])
            
            if rejected_df.empty:
                return None
            
            def extract_reason(notes):
                if pd.isna(notes):
                    return "No reason"
                for entry in str(notes).split('\n\n---'):
                    if 'REJECTED' in entry:
                        for line in entry.split('\n'):
                            if line.startswith('Reason:'):
                                return line.replace('Reason:', '').strip()
                return "No reason"
            
            rejected_df['rejection_reason'] = rejected_df['builder_notes'].apply(extract_reason)
            
            output = BytesIO()
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                summary_data = {
                    'Report': ['Rejected Items Report'],
                    'Building': [rejected_df['building_name'].iloc[0]],
                    'Total Rejected': [len(rejected_df)],
                    'Urgent': [len(rejected_df[rejected_df['urgency'] == 'Urgent'])]
                }
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                
                rejected_df[['unit', 'room', 'component', 'trade', 'urgency', 'rejection_reason']].to_excel(
                    writer, sheet_name='All Rejected', index=False
                )
            
            output.seek(0)
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Error generating rejected report: {e}")
            return None


    def _generate_completion_package(self, building_id):
        """Generate completion package"""
        try:
            conn = self.db.connect()
            
            completion_df = pd.read_sql_query("""
                SELECT 
                    wo.unit, wo.room, wo.component, wo.trade, wo.urgency,
                    wo.completed_date, b.name as building_name
                FROM inspector_work_orders wo
                JOIN inspector_inspections i ON wo.inspection_id = i.id
                JOIN inspector_buildings b ON i.building_id = b.id
                WHERE i.building_id = ?
                AND wo.status = 'waiting_approval'
                ORDER BY wo.completed_date DESC
            """, conn, params=[building_id])
            
            if completion_df.empty:
                return None
            
            output = BytesIO()
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                cover_data = {
                    'Document': ['BUILDER COMPLETION PACKAGE'],
                    'Building': [completion_df['building_name'].iloc[0]],
                    'Builder': [self.user_info.get('name', 'Builder')],
                    'Date': [datetime.now().strftime('%d/%m/%Y')],
                    'Items': [len(completion_df)]
                }
                pd.DataFrame(cover_data).to_excel(writer, sheet_name='Cover', index=False, header=False)
                
                completion_df[['unit', 'room', 'component', 'trade', 'urgency', 'completed_date']].to_excel(
                    writer, sheet_name='Summary', index=False
                )
            
            output.seek(0)
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Error generating completion package: {e}")
            return None


    def _generate_performance_report(self, building_id):
        """Generate performance report"""
        try:
            conn = self.db.connect()
            
            perf_df = pd.read_sql_query("""
                SELECT 
                    wo.status, wo.builder_notes, b.name as building_name
                FROM inspector_work_orders wo
                JOIN inspector_inspections i ON wo.inspection_id = i.id
                JOIN inspector_buildings b ON i.building_id = b.id
                WHERE i.building_id = ?
            """, conn, params=[building_id])
            
            if perf_df.empty:
                return None
            
            total = len(perf_df)
            approved = len(perf_df[perf_df['status'] == 'approved'])
            rejected = len(perf_df[perf_df['builder_notes'].str.contains('REJECTED', na=False)])
            
            approval_rate = (approved / total * 100) if total > 0 else 0
            rejection_rate = (rejected / total * 100) if total > 0 else 0
            
            output = BytesIO()
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                summary_data = {
                    'Metric': ['Total Items', 'Approved', 'Rejected', 'Approval Rate', 'Rejection Rate'],
                    'Value': [total, approved, rejected, f'{approval_rate:.1f}%', f'{rejection_rate:.1f}%']
                }
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            output.seek(0)
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Error generating performance report: {e}")
            return None
    
def render_builder_interface(user_info=None, auth_manager=None):
    """Entry point"""
    if 'builder_int' not in st.session_state:
        st.session_state.builder_int = BuilderInterface(user_info=user_info)
    
    st.session_state.builder_int.user_info = user_info
    st.session_state.builder_int.show()