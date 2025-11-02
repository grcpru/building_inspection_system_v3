"""
Developer Role Interface - Enhanced with Approval System
========================================================

Executive-level interface for property developers to:
- Approve/reject work completed by builders
- Monitor portfolio across all buildings
- View detailed building analytics
- Track settlement readiness
"""

import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
from pathlib import Path
import os

try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

try:
    from database.setup import DatabaseManager
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False

logger = logging.getLogger(__name__)


class DeveloperInterface:
    """Developer interface with approval workflow and portfolio analytics"""
    
    def __init__(self, db_path: str = "building_inspection.db", user_info: dict = None):
        self.user_info = user_info or {}
        self.db = DatabaseManager(db_path) if DATABASE_AVAILABLE else None
        
        # Session state
        if 'dev_active_view' not in st.session_state:
            st.session_state.dev_active_view = 'approvals'
        if 'dev_selected_building' not in st.session_state:
            st.session_state.dev_selected_building = None
        if 'dev_open_item' not in st.session_state:
            st.session_state.dev_open_item = None
    
    def show(self):
        """Main developer dashboard"""
        
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); 
                    color: white; padding: 1.5rem; border-radius: 10px; margin-bottom: 1rem;">
            <h2 style="margin: 0;">Developer Dashboard</h2>
            <p style="margin: 0.5rem 0 0 0;">Welcome, {self.user_info.get('name', 'Developer')}</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Main navigation
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            pending_count = self._get_pending_approval_count()
            if st.button(f"🔔 Approvals ({pending_count})", 
                        type="primary" if st.session_state.dev_active_view == 'approvals' else "secondary",
                        use_container_width=True):
                st.session_state.dev_active_view = 'approvals'
                st.rerun()
        
        with col2:
            approved_count = self._get_approved_count()
            if st.button(f"✅ Approved ({approved_count})", 
                        type="primary" if st.session_state.dev_active_view == 'approved' else "secondary",
                        use_container_width=True):
                st.session_state.dev_active_view = 'approved'
                st.rerun()
        
        with col3:
            rejected_count = self._get_rejected_count()
            if st.button(f"❌ Rejected ({rejected_count})", 
                        type="primary" if st.session_state.dev_active_view == 'rejected' else "secondary",
                        use_container_width=True):
                st.session_state.dev_active_view = 'rejected'
                st.rerun()
        
        with col4:
            if st.button("📊 Portfolio", 
                        type="primary" if st.session_state.dev_active_view == 'portfolio' else "secondary",
                        use_container_width=True):
                st.session_state.dev_active_view = 'portfolio'
                st.rerun()
        
        with col5:
            if st.button("🏢 Buildings", 
                        type="primary" if st.session_state.dev_active_view == 'buildings' else "secondary",
                        use_container_width=True):
                st.session_state.dev_active_view = 'buildings'
                st.rerun()
        
        with col6:
            if st.button("📈 Analytics", 
                        type="primary" if st.session_state.dev_active_view == 'analytics' else "secondary",
                        use_container_width=True):
                st.session_state.dev_active_view = 'analytics'
                st.rerun()
        
        st.markdown("---")
        
        # Show selected view
        if st.session_state.dev_active_view == 'approvals':
            self._show_approvals_view()
        elif st.session_state.dev_active_view == 'approved':
            self._show_approved_view()
        elif st.session_state.dev_active_view == 'rejected':
            self._show_rejected_view()
        elif st.session_state.dev_active_view == 'portfolio':
            self._show_portfolio_view()
        elif st.session_state.dev_active_view == 'buildings':
            self._show_buildings_view()
        elif st.session_state.dev_active_view == 'analytics':
            self._show_analytics_view()
    
    # =================================================================
    # COUNTER METHODS
    # =================================================================
    
    def _get_pending_approval_count(self):
        """Get count of items awaiting approval"""
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM inspector_work_orders 
                WHERE status = 'waiting_approval'
            """)
            result = cursor.fetchone()
            return result[0] if result else 0
        except:
            return 0
    
    def _get_approved_count(self):
        """Get count of approved items"""
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM inspector_work_orders 
                WHERE status = 'approved'
            """)
            result = cursor.fetchone()
            return result[0] if result else 0
        except:
            return 0
    
    def _get_rejected_count(self):
        """Get count of rejected items"""
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM inspector_work_orders 
                WHERE builder_notes LIKE '%REJECTED%' 
                AND status = 'in_progress'
            """)
            result = cursor.fetchone()
            return result[0] if result else 0
        except:
            return 0
    
    # =================================================================
    # APPROVALS VIEW (Pending Items)
    # =================================================================
    
    def _show_approvals_view(self):
        """Show work items awaiting approval"""
        
        st.markdown("### 🔔 Pending Approvals")
        
        try:
            conn = self.db.connect()
            
            approvals_df = pd.read_sql_query("""
                SELECT 
                    wo.*,
                    b.name as building_name,
                    i.id as inspection_id
                FROM inspector_work_orders wo
                JOIN inspector_inspections i ON wo.inspection_id = i.id
                JOIN inspector_buildings b ON i.building_id = b.id
                WHERE wo.status = 'waiting_approval'
                ORDER BY 
                    wo.updated_at DESC,
                    CASE wo.urgency 
                        WHEN 'Urgent' THEN 1 
                        WHEN 'High Priority' THEN 2 
                        ELSE 3 
                    END
            """, conn)
            
            if approvals_df.empty:
                st.success("✅ No items pending approval - all caught up!")
                return
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Pending Items", len(approvals_df))
            with col2:
                st.metric("Buildings Affected", approvals_df['building_name'].nunique())
            with col3:
                st.metric("Urgent Items", len(approvals_df[approvals_df['urgency'] == 'Urgent']))
            with col4:
                st.metric("Trades Involved", approvals_df['trade'].nunique())
            
            st.markdown("---")
            
            # Filters
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                buildings = ['All'] + sorted(approvals_df['building_name'].unique().tolist())
                building_filter = st.selectbox("Building:", buildings)
            with col2:
                trades = ['All'] + sorted(approvals_df['trade'].unique().tolist())
                trade_filter = st.selectbox("Trade:", trades)
            with col3:
                priorities = ['All'] + sorted(approvals_df['urgency'].unique().tolist())
                priority_filter = st.selectbox("Priority:", priorities)
            with col4:
                units = ['All'] + sorted(approvals_df['unit'].unique().tolist())
                unit_filter = st.selectbox("Unit:", units)
            
            # Apply filters
            filtered = approvals_df.copy()
            if building_filter != 'All':
                filtered = filtered[filtered['building_name'] == building_filter]
            if trade_filter != 'All':
                filtered = filtered[filtered['trade'] == trade_filter]
            if priority_filter != 'All':
                filtered = filtered[filtered['urgency'] == priority_filter]
            if unit_filter != 'All':
                filtered = filtered[filtered['unit'] == unit_filter]
            
            st.caption(f"Showing {len(filtered)} of {len(approvals_df)} items")
            
            # Render items
            for idx, (_, item) in enumerate(filtered.iterrows()):
                self._render_approval_item(item, idx)
                
        except Exception as e:
            st.error(f"Error loading approvals: {e}")
            logger.error(f"Approvals error: {e}")
    
    def _render_approval_item(self, item, idx):
        """Render single approval item"""
        
        oid = item['id']
        is_open = st.session_state.dev_open_item == oid
        
        priority_colors = {
            'Urgent': '🔴',
            'High Priority': '🟡',
            'Medium Priority': '🟢',
            'Low Priority': '⚪'
        }
        priority_icon = priority_colors.get(item['urgency'], '⚪')
        
        # Item header
        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
        
        with col1:
            st.markdown(f"{priority_icon} **{item['building_name']}** - Unit {item['unit']} - {item['room']}")
            st.caption(f"{item['component']} | {item['trade']}")
        
        with col2:
            if pd.notna(item.get('completed_date')):
                try:
                    completed = pd.to_datetime(item['completed_date'])
                    st.caption(f"✓ {completed.strftime('%d/%m/%Y')}")
                except:
                    pass
        
        with col3:
            if pd.notna(item.get('planned_date')):
                try:
                    planned = pd.to_datetime(item['planned_date'])
                    st.caption(f"📅 {planned.strftime('%d/%m/%Y')}")
                except:
                    st.caption(f"📅 {item['planned_date']}")
        
        with col4:
            btn_label = "Close ✕" if is_open else "Review 👁"
            btn_type = "secondary" if is_open else "primary"
            if st.button(btn_label, key=f"review_{oid}_{idx}", type=btn_type, use_container_width=True):
                if is_open:
                    st.session_state.dev_open_item = None
                else:
                    st.session_state.dev_open_item = oid
                st.rerun()
        
        # Show review form inline
        if is_open:
            st.markdown("---")
            with st.container():
                self._show_approval_form(item, idx)
            st.markdown("---")
        
        st.divider()
    
    def _show_approval_form(self, item, idx):
        """Show approval/rejection form with immediate action buttons"""
        
        oid = item['id']
        
        st.markdown(f"### 🔍 Review: {item['building_name']} - Unit {item['unit']}")
        
        # Work details
        col1, col2 = st.columns([3, 2])
        
        with col1:
            st.markdown("**Original Issue:**")
            if pd.notna(item.get('notes')):
                st.info(item['notes'])
            
            st.markdown(f"**Trade:** {item['trade']} | **Priority:** {item['urgency']}")
            st.markdown(f"**Location:** Unit {item['unit']}, {item['room']}, {item['component']}")
            
            if pd.notna(item.get('builder_notes')):
                st.markdown("**Work History:**")
                st.text_area("", value=item['builder_notes'], 
                           height=200, disabled=True, 
                           label_visibility="collapsed", key=f"dev_hist_{oid}_{idx}")
        
        with col2:
            if pd.notna(item.get('completed_date')):
                try:
                    completed = pd.to_datetime(item['completed_date'])
                    st.markdown(f"**Completed:** {completed.strftime('%d/%m/%Y %H:%M')}")
                except:
                    pass
            
            if pd.notna(item.get('planned_date')):
                try:
                    planned = pd.to_datetime(item['planned_date'])
                    st.markdown(f"**Planned:** {planned.strftime('%d/%m/%Y')}")
                except:
                    st.markdown(f"**Planned:** {item['planned_date']}")
            
            file_count = self._get_file_count(oid)
            if file_count > 0:
                st.markdown(f"**Files ({file_count}):**")
                self._show_files(oid)
        
        st.markdown("---")
        
        # Decision form - IMMEDIATE ACTION BUTTONS
        st.markdown("**Developer Review:**")
        
        comments = st.text_area(
            "Comments (optional for approval, required for rejection):",
            placeholder="Add any notes about this decision...",
            key=f"dev_comments_{oid}_{idx}",
            height=100
        )
        
        # Action buttons with color coding
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            # GREEN APPROVE BUTTON
            if st.button("✅ Approve", key=f"approve_btn_{oid}_{idx}", 
                        type="primary", use_container_width=True,
                        help="Approve this work immediately"):
                success, message = self._approve_work(oid, comments)
                if success:
                    st.success(message)
                    st.balloons()
                    st.session_state.dev_open_item = None
                    st.rerun()
                else:
                    st.error(message)
        
        with col2:
            # RED REJECT BUTTON
            if st.button("❌ Reject & Return", key=f"reject_btn_{oid}_{idx}", 
                        use_container_width=True,
                        help="Reject and send back to builder immediately"):
                if not comments or not comments.strip():
                    st.error("❌ Rejection reason required in comments field")
                else:
                    success, message = self._reject_work(oid, comments)
                    if success:
                        st.warning(message)
                        st.session_state.dev_open_item = None
                        st.rerun()
                    else:
                        st.error(message)
        
        with col3:
            if st.button("Cancel", key=f"cancel_btn_{oid}_{idx}", use_container_width=True):
                st.session_state.dev_open_item = None
                st.rerun()
        
        # Add custom CSS for button colors
        st.markdown("""
        <style>
        div[data-testid="column"]:nth-child(2) button {
            background-color: #dc3545 !important;
            color: white !important;
            border-color: #dc3545 !important;
        }
        div[data-testid="column"]:nth-child(2) button:hover {
            background-color: #c82333 !important;
            border-color: #bd2130 !important;
        }
        div[data-testid="column"]:nth-child(1) button[kind="primary"] {
            background-color: #28a745 !important;
            border-color: #28a745 !important;
        }
        div[data-testid="column"]:nth-child(1) button[kind="primary"]:hover {
            background-color: #218838 !important;
            border-color: #1e7e34 !important;
        }
        </style>
        """, unsafe_allow_html=True)
    
    def _approve_work(self, oid, notes):
        """Approve completed work"""
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            
            ts = datetime.now().strftime("%d/%m/%Y %H:%M")
            user = self.user_info.get('name', 'Developer')
            
            entry = f"\n\n--- {ts} - {user} (Developer) ---"
            entry += f"\n✅ APPROVED"
            if notes and notes.strip():
                entry += f"\nNotes: {notes.strip()}"
            entry += f"\n📊 STATUS: APPROVED - Work Accepted"
            
            cursor.execute("SELECT builder_notes FROM inspector_work_orders WHERE id = ?", (oid,))
            result = cursor.fetchone()
            old_notes = result[0] if result and result[0] else ""
            
            new_notes = f"{old_notes}{entry}"
            cursor.execute("""
                UPDATE inspector_work_orders 
                SET builder_notes = ?, status = 'approved', updated_at = ?
                WHERE id = ?
            """, (new_notes, datetime.now().isoformat(), oid))
            
            conn.commit()
            return True, "Work approved successfully!"
            
        except Exception as e:
            logger.error(f"Approval error: {e}")
            return False, f"Approval failed: {str(e)}"
    
    def _reject_work(self, oid, notes):
        """Reject work and send back to builder"""
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            
            ts = datetime.now().strftime("%d/%m/%Y %H:%M")
            user = self.user_info.get('name', 'Developer')
            
            entry = f"\n\n--- {ts} - {user} (Developer) ---"
            entry += f"\n❌ REJECTED - REQUIRES REWORK"
            entry += f"\nReason: {notes.strip()}"
            entry += f"\n📊 STATUS: Returned to Builder"
            
            cursor.execute("SELECT builder_notes FROM inspector_work_orders WHERE id = ?", (oid,))
            result = cursor.fetchone()
            old_notes = result[0] if result and result[0] else ""
            
            new_notes = f"{old_notes}{entry}"
            cursor.execute("""
                UPDATE inspector_work_orders 
                SET builder_notes = ?, status = 'in_progress', updated_at = ?
                WHERE id = ?
            """, (new_notes, datetime.now().isoformat(), oid))
            
            conn.commit()
            return True, "Work rejected and returned to builder for rework"
            
        except Exception as e:
            logger.error(f"Rejection error: {e}")
            return False, f"Rejection failed: {str(e)}"
    
    # =================================================================
    # APPROVED VIEW
    # =================================================================
    
    def _show_approved_view(self):
        """Show approved/completed items with full details"""
        
        st.markdown("### ✅ Approved Work Orders")
        
        try:
            conn = self.db.connect()
            
            approved_df = pd.read_sql_query("""
                SELECT 
                    wo.*,
                    b.name as building_name,
                    i.id as inspection_id
                FROM inspector_work_orders wo
                JOIN inspector_inspections i ON wo.inspection_id = i.id
                JOIN inspector_buildings b ON i.building_id = b.id
                WHERE wo.status = 'approved'
                ORDER BY wo.updated_at DESC
            """, conn)
            
            if approved_df.empty:
                st.info("No approved work orders yet")
                return
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Approved", len(approved_df))
            with col2:
                st.metric("Buildings", approved_df['building_name'].nunique())
            with col3:
                completed_times = []
                for _, row in approved_df.iterrows():
                    if pd.notna(row.get('completed_date')) and pd.notna(row.get('updated_at')):
                        try:
                            completed = pd.to_datetime(row['completed_date'])
                            approved = pd.to_datetime(row['updated_at'])
                            hours = (approved - completed).total_seconds() / 3600
                            if hours >= 0:
                                completed_times.append(hours)
                        except:
                            pass
                
                if completed_times:
                    avg_hours = sum(completed_times) / len(completed_times)
                    st.metric("Avg Approval Time", f"{avg_hours:.1f}h")
                else:
                    st.metric("Avg Approval Time", "N/A")
            with col4:
                st.metric("Trades", approved_df['trade'].nunique())
            
            st.markdown("---")
            
            # Filters
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                buildings = ['All'] + sorted(approved_df['building_name'].unique().tolist())
                building_filter = st.selectbox("Building:", buildings, key="approved_building")
            with col2:
                trades = ['All'] + sorted(approved_df['trade'].unique().tolist())
                trade_filter = st.selectbox("Trade:", trades, key="approved_trade")
            with col3:
                units = ['All'] + sorted(approved_df['unit'].unique().tolist())
                unit_filter = st.selectbox("Unit:", units, key="approved_unit")
            with col4:
                date_range = st.selectbox("Approved:", 
                    ["All Time", "Today", "This Week", "This Month"],
                    key="approved_date")
            
            # Apply filters
            filtered = approved_df.copy()
            if building_filter != 'All':
                filtered = filtered[filtered['building_name'] == building_filter]
            if trade_filter != 'All':
                filtered = filtered[filtered['trade'] == trade_filter]
            if unit_filter != 'All':
                filtered = filtered[filtered['unit'] == unit_filter]
            
            if date_range != "All Time":
                now = datetime.now()
                if date_range == "Today":
                    filtered = filtered[pd.to_datetime(filtered['updated_at']).dt.date == now.date()]
                elif date_range == "This Week":
                    week_start = now - timedelta(days=now.weekday())
                    filtered = filtered[pd.to_datetime(filtered['updated_at']) >= week_start]
                elif date_range == "This Month":
                    month_start = now.replace(day=1)
                    filtered = filtered[pd.to_datetime(filtered['updated_at']) >= month_start]
            
            st.caption(f"Showing {len(filtered)} of {len(approved_df)} approved items")
            
            if len(filtered) > 0:
                filtered['approval_date'] = pd.to_datetime(filtered['updated_at']).dt.date
                
                for approval_date, day_group in filtered.groupby('approval_date', sort=False):
                    st.markdown(f"#### 📅 {approval_date.strftime('%A, %B %d, %Y')} ({len(day_group)} items)")
                    
                    for idx, (_, item) in enumerate(day_group.iterrows()):
                        self._render_approved_item(item, idx)
                    
                    st.markdown("---")
                    
        except Exception as e:
            st.error(f"Error loading approved items: {e}")
            logger.error(f"Approved items error: {e}")
    
    def _render_approved_item(self, item, idx):
        """Render single approved item with full details"""
        
        oid = item['id']
        
        with st.expander(
            f"✅ {item['building_name']} - Unit {item['unit']} - {item['room']} - {item['component']} ({item['trade']})",
            expanded=False
        ):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("**Work Details:**")
                st.markdown(f"**Building:** {item['building_name']}")
                st.markdown(f"**Location:** Unit {item['unit']}, {item['room']}, {item['component']}")
                st.markdown(f"**Trade:** {item['trade']}")
                st.markdown(f"**Priority:** {item['urgency']}")
                
                if pd.notna(item.get('builder_notes')):
                    st.markdown("---")
                    st.markdown("**Complete Work History:**")
                    
                    notes = str(item['builder_notes'])
                    entries = notes.split('\n\n---')
                    
                    for entry in entries:
                        if not entry.strip():
                            continue
                        
                        if '✅ APPROVED' in entry or 'APPROVED' in entry:
                            st.success(entry.strip())
                        elif '❌ REJECTED' in entry or 'REJECTED' in entry:
                            st.error(entry.strip())
                        else:
                            st.text(entry.strip())
                        
                        st.markdown("")
            
            with col2:
                st.markdown("**Timeline:**")
                
                if pd.notna(item.get('started_date')):
                    try:
                        started = pd.to_datetime(item['started_date'])
                        st.markdown(f"🔨 **Started:** {started.strftime('%d/%m/%Y %H:%M')}")
                    except:
                        pass
                
                if pd.notna(item.get('completed_date')):
                    try:
                        completed = pd.to_datetime(item['completed_date'])
                        st.markdown(f"✓ **Completed:** {completed.strftime('%d/%m/%Y %H:%M')}")
                    except:
                        pass
                
                if pd.notna(item.get('updated_at')):
                    try:
                        approved = pd.to_datetime(item['updated_at'])
                        st.markdown(f"✅ **Approved:** {approved.strftime('%d/%m/%Y %H:%M')}")
                        
                        if pd.notna(item.get('completed_date')):
                            completed = pd.to_datetime(item['completed_date'])
                            time_diff = (approved - completed).total_seconds() / 3600
                            if time_diff >= 0:
                                st.caption(f"⏱ Approval time: {time_diff:.1f} hours")
                    except:
                        pass
                
                if pd.notna(item.get('planned_date')):
                    try:
                        planned = pd.to_datetime(item['planned_date'])
                        st.markdown(f"📅 **Planned:** {planned.strftime('%d/%m/%Y')}")
                    except:
                        pass
                
                file_count = self._get_file_count(oid)
                if file_count > 0:
                    st.markdown("---")
                    st.markdown(f"**Files ({file_count}):**")
                    self._show_files(oid)
                
                st.markdown("---")
                st.success("Status: APPROVED ✅")
    
    # =================================================================
    # REJECTED VIEW
    # =================================================================
    
    def _show_rejected_view(self):
        """Show rejected items with rejection reasons and history"""
        
        st.markdown("### ❌ Rejected Work Orders")
        
        try:
            conn = self.db.connect()
            
            rejected_df = pd.read_sql_query("""
                SELECT 
                    wo.*,
                    b.name as building_name,
                    i.id as inspection_id
                FROM inspector_work_orders wo
                JOIN inspector_inspections i ON wo.inspection_id = i.id
                JOIN inspector_buildings b ON i.building_id = b.id
                WHERE wo.builder_notes LIKE '%REJECTED%'
                AND wo.status = 'in_progress'
                ORDER BY wo.updated_at DESC
            """, conn)
            
            if rejected_df.empty:
                st.success("✅ No rejected items - great quality work!")
                return
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Rejected", len(rejected_df), delta="Needs rework", delta_color="inverse")
            with col2:
                st.metric("Buildings Affected", rejected_df['building_name'].nunique())
            with col3:
                st.metric("Urgent Items", len(rejected_df[rejected_df['urgency'] == 'Urgent']))
            with col4:
                st.metric("Trades Involved", rejected_df['trade'].nunique())
            
            st.markdown("---")
            
            # Filters
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                buildings = ['All'] + sorted(rejected_df['building_name'].unique().tolist())
                building_filter = st.selectbox("Building:", buildings, key="rejected_building")
            with col2:
                trades = ['All'] + sorted(rejected_df['trade'].unique().tolist())
                trade_filter = st.selectbox("Trade:", trades, key="rejected_trade")
            with col3:
                priorities = ['All'] + sorted(rejected_df['urgency'].unique().tolist())
                priority_filter = st.selectbox("Priority:", priorities, key="rejected_priority")
            with col4:
                units = ['All'] + sorted(rejected_df['unit'].unique().tolist())
                unit_filter = st.selectbox("Unit:", units, key="rejected_unit")
            
            # Apply filters
            filtered = rejected_df.copy()
            if building_filter != 'All':
                filtered = filtered[filtered['building_name'] == building_filter]
            if trade_filter != 'All':
                filtered = filtered[filtered['trade'] == trade_filter]
            if priority_filter != 'All':
                filtered = filtered[filtered['urgency'] == priority_filter]
            if unit_filter != 'All':
                filtered = filtered[filtered['unit'] == unit_filter]
            
            st.caption(f"Showing {len(filtered)} of {len(rejected_df)} rejected items")
            
            urgent_filtered = filtered[filtered['urgency'] == 'Urgent']
            if len(urgent_filtered) > 0:
                st.error(f"⚠️ {len(urgent_filtered)} URGENT items need attention!")
            
            st.markdown("---")
            
            for idx, (_, item) in enumerate(filtered.iterrows()):
                self._render_rejected_item(item, idx)
                
        except Exception as e:
            st.error(f"Error loading rejected items: {e}")
            logger.error(f"Rejected items error: {e}")
    
    def _render_rejected_item(self, item, idx):
        """Render single rejected item with rejection details"""
        
        oid = item['id']
        
        priority_colors = {
            'Urgent': '🔴',
            'High Priority': '🟡',
            'Medium Priority': '🟢',
            'Low Priority': '⚪'
        }
        priority_icon = priority_colors.get(item['urgency'], '⚪')
        
        is_urgent = item['urgency'] == 'Urgent'
        
        with st.expander(
            f"❌ {priority_icon} {item['building_name']} - Unit {item['unit']} - {item['room']} - {item['component']} ({item['trade']})",
            expanded=is_urgent
        ):
            rejection_reason = "No reason provided"
            rejection_date = None
            rejected_by = "Developer"
            
            if pd.notna(item.get('builder_notes')):
                notes = str(item['builder_notes'])
                entries = notes.split('\n\n---')
                for entry in entries:
                    if '❌ REJECTED' in entry or 'REJECTED' in entry:
                        lines = entry.strip().split('\n')
                        if lines:
                            first_line = lines[0].replace('---', '').strip()
                            if ' - ' in first_line:
                                parts = first_line.split(' - ')
                                if len(parts) >= 2:
                                    rejection_date = parts[0].strip()
                                    rejected_by = parts[1].replace('(Developer)', '').strip()
                        
                        for line in lines:
                            if line.startswith('Reason:'):
                                rejection_reason = line.replace('Reason:', '').strip()
                                break
                        break
            
            st.error(f"**REJECTED by {rejected_by}**" + (f" on {rejection_date}" if rejection_date else ""))
            st.markdown(f"**Rejection Reason:** {rejection_reason}")
            
            st.markdown("---")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("**Work Details:**")
                st.markdown(f"**Building:** {item['building_name']}")
                st.markdown(f"**Location:** Unit {item['unit']}, {item['room']}, {item['component']}")
                st.markdown(f"**Trade:** {item['trade']}")
                st.markdown(f"**Priority:** {item['urgency']}")
                
                if pd.notna(item.get('builder_notes')):
                    st.markdown("---")
                    st.markdown("**Complete Work History:**")
                    
                    notes = str(item['builder_notes'])
                    entries = notes.split('\n\n---')
                    
                    for entry in entries:
                        if not entry.strip():
                            continue
                        
                        if '❌ REJECTED' in entry or 'REJECTED' in entry:
                            st.error(entry.strip())
                        elif '✅ APPROVED' in entry or 'APPROVED' in entry:
                            st.success(entry.strip())
                        else:
                            st.text(entry.strip())
                        
                        st.markdown("")
            
            with col2:
                st.markdown("**Timeline:**")
                
                if pd.notna(item.get('started_date')):
                    try:
                        started = pd.to_datetime(item['started_date'])
                        st.markdown(f"🔨 **Started:** {started.strftime('%d/%m/%Y %H:%M')}")
                    except:
                        pass
                
                if pd.notna(item.get('updated_at')):
                    try:
                        rejected = pd.to_datetime(item['updated_at'])
                        st.markdown(f"❌ **Rejected:** {rejected.strftime('%d/%m/%Y %H:%M')}")
                        
                        time_since = datetime.now() - rejected
                        hours_since = time_since.total_seconds() / 3600
                        
                        if hours_since < 24:
                            st.caption(f"⏱ {hours_since:.1f} hours ago")
                        else:
                            days_since = time_since.days
                            st.caption(f"⏱ {days_since} days ago")
                    except:
                        pass
                
                if pd.notna(item.get('planned_date')):
                    try:
                        planned = pd.to_datetime(item['planned_date'])
                        st.markdown(f"📅 **Planned:** {planned.strftime('%d/%m/%Y')}")
                    except:
                        pass
                
                file_count = self._get_file_count(oid)
                if file_count > 0:
                    st.markdown("---")
                    st.markdown(f"**Previous Files ({file_count}):**")
                    self._show_files(oid)
                
                st.markdown("---")
                st.warning("**Status:** REJECTED - Awaiting Rework")
                st.info("**Action:** Builder must address the rejection reason and resubmit")
            
            st.divider()
    
    # =================================================================
    # PORTFOLIO VIEW
    # =================================================================
    
    def _show_portfolio_view(self):
        """Show portfolio overview across all buildings"""
        
        st.markdown("### 📊 Portfolio Overview")
        
        try:
            conn = self.db.connect()
            
            portfolio_df = pd.read_sql_query("""
                SELECT 
                    b.id,
                    b.name as building_name,
                    COUNT(DISTINCT wo.id) as total_work_orders,
                    COUNT(CASE WHEN wo.status = 'pending' THEN 1 END) as pending,
                    COUNT(CASE WHEN wo.status = 'in_progress' THEN 1 END) as in_progress,
                    COUNT(CASE WHEN wo.status = 'waiting_approval' THEN 1 END) as waiting_approval,
                    COUNT(CASE WHEN wo.status = 'approved' THEN 1 END) as approved,
                    COUNT(CASE WHEN wo.urgency = 'Urgent' THEN 1 END) as urgent_count
                FROM inspector_buildings b
                LEFT JOIN inspector_inspections i ON b.id = i.building_id
                LEFT JOIN inspector_work_orders wo ON i.id = wo.inspection_id
                GROUP BY b.id, b.name
                HAVING total_work_orders > 0
                ORDER BY waiting_approval DESC, urgent_count DESC
            """, conn)
            
            if portfolio_df.empty:
                st.info("No buildings with work orders found")
                return
            
            total_work = portfolio_df['total_work_orders'].sum()
            total_approved = portfolio_df['approved'].sum()
            total_pending_approval = portfolio_df['waiting_approval'].sum()
            total_urgent = portfolio_df['urgent_count'].sum()
            
            completion_rate = (total_approved / total_work * 100) if total_work > 0 else 0
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Total Buildings", len(portfolio_df))
            with col2:
                st.metric("Total Work Orders", total_work)
            with col3:
                st.metric("Awaiting Approval", total_pending_approval)
            with col4:
                st.metric("Completion Rate", f"{completion_rate:.1f}%")
            with col5:
                st.metric("Urgent Items", total_urgent)
            
            st.progress(completion_rate / 100, text=f"Portfolio Completion: {completion_rate:.1f}%")
            
            st.markdown("---")
            st.markdown("#### Buildings Status")
            
            portfolio_df['completion_pct'] = (portfolio_df['approved'] / portfolio_df['total_work_orders'] * 100).round(1)
            
            display_df = portfolio_df[[
                'building_name', 'total_work_orders', 'pending', 
                'in_progress', 'waiting_approval', 'approved', 
                'completion_pct', 'urgent_count'
            ]].copy()
            
            display_df.columns = [
                'Building', 'Total', 'Pending', 'Active', 
                'Awaiting', 'Approved', 'Complete %', 'Urgent'
            ]
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
        except Exception as e:
            st.error(f"Error loading portfolio: {e}")
            logger.error(f"Portfolio error: {e}")
    
    # =================================================================
    # BUILDINGS VIEW
    # =================================================================
    
    def _show_buildings_view(self):
        """Show detailed view of individual buildings"""
        
        st.markdown("### 🏢 Building Details")
        
        try:
            conn = self.db.connect()
            
            buildings_df = pd.read_sql_query("""
                SELECT DISTINCT b.id, b.name
                FROM inspector_buildings b
                JOIN inspector_inspections i ON b.id = i.building_id
                JOIN inspector_work_orders wo ON i.id = wo.inspection_id
                ORDER BY b.name
            """, conn)
            
            if buildings_df.empty:
                st.info("No buildings with work orders found")
                return
            
            building_names = buildings_df['name'].tolist()
            selected_building = st.selectbox("Select Building:", building_names)
            
            if selected_building:
                building_id = buildings_df[buildings_df['name'] == selected_building]['id'].iloc[0]
                self._show_building_details(building_id, selected_building)
                
        except Exception as e:
            st.error(f"Error loading buildings: {e}")
            logger.error(f"Buildings error: {e}")
    
    def _show_building_details(self, building_id, building_name):
        """Show detailed stats and work orders for a specific building"""
        
        try:
            conn = self.db.connect()
            
            # Updated query to separate rejected from in_progress
            stats_df = pd.read_sql_query("""
                SELECT 
                    COUNT(wo.id) as total_items,
                    COUNT(CASE WHEN wo.status = 'pending' THEN 1 END) as pending,
                    COUNT(CASE WHEN wo.status = 'in_progress' AND (wo.builder_notes NOT LIKE '%REJECTED%' OR wo.builder_notes IS NULL) THEN 1 END) as in_progress,
                    COUNT(CASE WHEN wo.status = 'in_progress' AND wo.builder_notes LIKE '%REJECTED%' THEN 1 END) as rejected,
                    COUNT(CASE WHEN wo.status = 'waiting_approval' THEN 1 END) as waiting_approval,
                    COUNT(CASE WHEN wo.status = 'approved' THEN 1 END) as approved,
                    COUNT(CASE WHEN wo.urgency = 'Urgent' THEN 1 END) as urgent,
                    COUNT(DISTINCT wo.unit) as affected_units,
                    COUNT(DISTINCT wo.trade) as trades_involved
                FROM inspector_inspections i
                JOIN inspector_work_orders wo ON i.id = wo.inspection_id
                WHERE i.building_id = ?
            """, conn, params=[building_id])
            
            if stats_df.empty or stats_df['total_items'].iloc[0] == 0:
                st.info(f"No work orders for {building_name}")
                return
            
            stats = stats_df.iloc[0]
            completion = (stats['approved'] / stats['total_items'] * 100) if stats['total_items'] > 0 else 0
            
            # Display metrics with rejected shown separately
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Total Items", stats['total_items'])
                st.metric("Pending", stats['pending'])
            
            with col2:
                st.metric("In Progress", stats['in_progress'])
                # Show rejected with warning if any exist
                if stats['rejected'] > 0:
                    st.metric("Rejected", stats['rejected'], delta="Needs rework", delta_color="inverse")
                else:
                    st.metric("Rejected", stats['rejected'])
            
            with col3:
                st.metric("Awaiting Approval", stats['waiting_approval'])
                st.metric("Approved", stats['approved'])
            
            with col4:
                st.metric("Completion Rate", f"{completion:.1f}%")
                st.metric("Urgent Items", stats['urgent'])
            
            with col5:
                st.metric("Affected Units", stats['affected_units'])
                st.metric("Trades Involved", stats['trades_involved'])
            
            st.progress(completion / 100, text=f"Building Completion: {completion:.1f}%")
            
            st.markdown("---")
            
            orders_df = pd.read_sql_query("""
                SELECT 
                    wo.id, wo.unit, wo.room, wo.component, wo.trade, 
                    wo.urgency, wo.status, wo.builder_notes, wo.updated_at
                FROM inspector_inspections i
                JOIN inspector_work_orders wo ON i.id = wo.inspection_id
                WHERE i.building_id = ?
                ORDER BY 
                    CASE wo.status 
                        WHEN 'waiting_approval' THEN 1
                        WHEN 'in_progress' THEN 2
                        WHEN 'pending' THEN 3
                        ELSE 4
                    END,
                    CASE wo.urgency 
                        WHEN 'Urgent' THEN 1 
                        WHEN 'High Priority' THEN 2 
                        ELSE 3 
                    END,
                    wo.unit
            """, conn, params=[building_id])
            
            # Filters
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                # Add "Rejected" as a status filter option
                status_options = ['All', 'pending', 'in_progress', 'rejected', 'waiting_approval', 'approved']
                status_filter = st.selectbox("Status:", status_options)
            with col2:
                unit_filter = st.selectbox("Unit:", ['All'] + sorted(orders_df['unit'].unique().tolist()))
            with col3:
                trade_filter = st.selectbox("Trade:", ['All'] + sorted(orders_df['trade'].unique().tolist()))
            with col4:
                urgency_filter = st.selectbox("Priority:", ['All'] + sorted(orders_df['urgency'].unique().tolist()))
            
            # Apply filters
            filtered = orders_df.copy()
            
            # Handle rejected status filter specially
            if status_filter == 'rejected':
                filtered = filtered[
                    (filtered['status'] == 'in_progress') & 
                    (filtered['builder_notes'].str.contains('REJECTED', na=False))
                ]
            elif status_filter == 'in_progress':
                # Show only non-rejected in_progress items
                filtered = filtered[
                    (filtered['status'] == 'in_progress') & 
                    (~filtered['builder_notes'].str.contains('REJECTED', na=False))
                ]
            elif status_filter != 'All':
                filtered = filtered[filtered['status'] == status_filter]
            
            if unit_filter != 'All':
                filtered = filtered[filtered['unit'] == unit_filter]
            if trade_filter != 'All':
                filtered = filtered[filtered['trade'] == trade_filter]
            if urgency_filter != 'All':
                filtered = filtered[filtered['urgency'] == urgency_filter]
            
            st.caption(f"Showing {len(filtered)} of {len(orders_df)} items")
            
            # Add status display logic to show "Rejected" for rejected items
            display_orders = filtered[['unit', 'room', 'component', 'trade', 'urgency', 'status', 'builder_notes']].copy()
            
            # Create a display status that shows "Rejected" for rejected items
            display_orders['display_status'] = display_orders.apply(
                lambda row: 'rejected' if row['status'] == 'in_progress' and 
                           isinstance(row['builder_notes'], str) and 'REJECTED' in row['builder_notes']
                           else row['status'],
                axis=1
            )
            
            # Select columns for display
            display_orders = display_orders[['unit', 'room', 'component', 'trade', 'urgency', 'display_status']].copy()
            display_orders.columns = ['Unit', 'Room', 'Component', 'Trade', 'Priority', 'Status']
            
            st.dataframe(display_orders, use_container_width=True, hide_index=True)
            
        except Exception as e:
            st.error(f"Error loading building details: {e}")
            logger.error(f"Building details error: {e}")
    
    # =================================================================
    # ANALYTICS VIEW
    # =================================================================
    
    def _show_analytics_view(self):
        """Show analytics and insights"""
        
        st.markdown("### 📈 Analytics & Insights")
        
        try:
            conn = self.db.connect()
            
            trade_df = pd.read_sql_query("""
                SELECT 
                    wo.trade,
                    COUNT(wo.id) as total,
                    COUNT(CASE WHEN wo.status = 'approved' THEN 1 END) as approved,
                    COUNT(CASE WHEN wo.status = 'waiting_approval' THEN 1 END) as waiting,
                    AVG(CASE WHEN wo.urgency = 'Urgent' THEN 1 ELSE 0 END) * 100 as urgent_pct
                FROM inspector_work_orders wo
                GROUP BY wo.trade
                HAVING total > 0
                ORDER BY total DESC
            """, conn)
            
            if not trade_df.empty:
                trade_df['completion_pct'] = (trade_df['approved'] / trade_df['total'] * 100).round(1)
                
                st.markdown("#### Trade Performance")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.dataframe(trade_df[['trade', 'total', 'approved', 'completion_pct']], 
                               use_container_width=True, hide_index=True)
                
                with col2:
                    if PLOTLY_AVAILABLE and len(trade_df) > 1:
                        fig = px.bar(trade_df.head(10), x='trade', y='completion_pct',
                                   title="Trade Completion Rates",
                                   labels={'completion_pct': 'Completion %', 'trade': 'Trade'})
                        st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("---")
            st.markdown("#### Completion Timeline")
            
            timeline_df = pd.read_sql_query("""
                SELECT 
                    DATE(wo.updated_at) as date,
                    COUNT(CASE WHEN wo.status = 'approved' THEN 1 END) as approved_count
                FROM inspector_work_orders wo
                WHERE wo.updated_at IS NOT NULL
                GROUP BY DATE(wo.updated_at)
                ORDER BY date DESC
                LIMIT 30
            """, conn)
            
            if not timeline_df.empty and PLOTLY_AVAILABLE:
                fig = px.line(timeline_df, x='date', y='approved_count',
                            title="Approvals Over Time (Last 30 Days)",
                            labels={'approved_count': 'Approvals', 'date': 'Date'})
                st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error loading analytics: {e}")
            logger.error(f"Analytics error: {e}")
    
    # =================================================================
    # HELPER METHODS
    # =================================================================
    
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
        """Display uploaded files from database with download capability"""
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
                documents = []
                
                for fname, fpath, ftype in files:
                    if not fpath or fpath == 'NULL' or fpath == 'None':
                        if fname:
                            documents.append({
                                'name': fname,
                                'path': None,
                                'status': 'no path'
                            })
                        continue
                    
                    fpath_str = str(fpath)
                    file_exists = os.path.exists(fpath_str)
                    
                    if ftype and 'image' in str(ftype).lower():
                        images.append({
                            'name': fname or "Image",
                            'path': fpath_str if file_exists else None,
                            'status': 'ok' if file_exists else 'missing'
                        })
                    else:
                        documents.append({
                            'name': fname or "File",
                            'path': fpath_str if file_exists else None,
                            'status': 'ok' if file_exists else 'missing',
                            'type': ftype
                        })
                
                # Display images
                if images:
                    st.markdown("**Images:**")
                    cols = st.columns(2)
                    for i, img_info in enumerate(images):
                        with cols[i % 2]:
                            if img_info['status'] == 'ok' and img_info['path']:
                                try:
                                    st.image(img_info['path'], 
                                        caption=img_info['name'], 
                                        use_container_width=True)
                                except Exception as e:
                                    logger.error(f"Error displaying {img_info['path']}: {e}")
                                    st.caption(f"📄 {img_info['name']} (can't display)")
                            else:
                                st.caption(f"📄 {img_info['name']} ({img_info['status']})")
                
                # Display documents with download buttons
                if documents:
                    if images:
                        st.markdown("**Documents:**")
                    
                    for idx, doc_info in enumerate(documents):
                        if doc_info['status'] == 'ok' and doc_info['path']:
                            # Create download button for the file
                            try:
                                # Read file content
                                with open(doc_info['path'], 'rb') as f:
                                    file_data = f.read()
                                
                                # Get file extension for mime type
                                file_ext = doc_info['name'].split('.')[-1].lower() if '.' in doc_info['name'] else ''
                                
                                # Determine mime type
                                mime_types = {
                                    'pdf': 'application/pdf',
                                    'doc': 'application/msword',
                                    'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                                    'xls': 'application/vnd.ms-excel',
                                    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                    'txt': 'text/plain',
                                    'csv': 'text/csv',
                                    'zip': 'application/zip'
                                }
                                mime_type = mime_types.get(file_ext, 'application/octet-stream')
                                
                                # Get file size
                                file_size = len(file_data)
                                if file_size < 1024:
                                    size_str = f"{file_size} B"
                                elif file_size < 1024 * 1024:
                                    size_str = f"{file_size / 1024:.1f} KB"
                                else:
                                    size_str = f"{file_size / (1024 * 1024):.1f} MB"
                                
                                # File type icon
                                icon = "📄"
                                if file_ext == 'pdf':
                                    icon = "📄"
                                elif file_ext in ['doc', 'docx']:
                                    icon = "📝"
                                elif file_ext in ['xls', 'xlsx']:
                                    icon = "📊"
                                elif file_ext in ['zip', 'rar']:
                                    icon = "📦"
                                elif file_ext in ['mp4', 'avi', 'mov']:
                                    icon = "🎥"
                                
                                # Display file info and download button
                                col1, col2 = st.columns([3, 1])
                                with col1:
                                    st.caption(f"{icon} {doc_info['name']} ({size_str})")
                                with col2:
                                    st.download_button(
                                        label="📥",
                                        data=file_data,
                                        file_name=doc_info['name'],
                                        mime=mime_type,
                                        key=f"download_{oid}_{idx}",
                                        help=f"Download {doc_info['name']}"
                                    )
                                
                            except Exception as e:
                                logger.error(f"Error reading file {doc_info['path']}: {e}")
                                st.caption(f"📄 {doc_info['name']} (error reading file)")
                        else:
                            st.caption(f"📄 {doc_info['name']} ({doc_info['status']})")
                        
        except Exception as e:
            logger.error(f"File display error: {e}")
            st.error(f"Error loading files: {e}")


def render_developer_interface(user_info=None, auth_manager=None):
    """Entry point for developer interface"""
    if 'developer_int' not in st.session_state:
        st.session_state.developer_int = DeveloperInterface(user_info=user_info)
    
    st.session_state.developer_int.user_info = user_info
    st.session_state.developer_int.show()