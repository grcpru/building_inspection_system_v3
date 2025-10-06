"""
Building Inspection System V2 - Owner Role Implementation
Handles defect approval, DLP defect reporting, photo review, and owner-specific workflows
"""

import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import json
import os
from typing import Dict, List, Optional, Tuple
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

class OwnerDashboard:
    """Comprehensive dashboard for Owner role with approval workflows and DLP management"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.setup_owner_tables()
    
    def setup_owner_tables(self):
        """Create Owner-specific database tables"""
        cursor = self.db.cursor()
        
        # DLP (Defects Liability Period) defects table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dlp_defects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_defect_id INTEGER,
                inspection_id INTEGER NOT NULL,
                defect_id TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                location TEXT,
                trade TEXT,
                severity TEXT NOT NULL,
                dlp_category TEXT CHECK (dlp_category IN (
                    'structural', 'waterproofing', 'mechanical', 'electrical', 
                    'finishing', 'external_works'
                )),
                reported_by INTEGER NOT NULL,
                reported_date DATE NOT NULL,
                discovery_method TEXT CHECK (discovery_method IN (
                    'routine_inspection', 'tenant_complaint', 'maintenance_visit', 
                    'weather_event', 'warranty_claim'
                )),
                warranty_status TEXT DEFAULT 'active' CHECK (warranty_status IN (
                    'active', 'expired', 'disputed', 'void'
                )),
                contractor_notified BOOLEAN DEFAULT FALSE,
                contractor_response_date DATE,
                estimated_repair_cost REAL,
                actual_repair_cost REAL,
                insurance_claim_number TEXT,
                priority INTEGER DEFAULT 3,
                status TEXT DEFAULT 'reported' CHECK (status IN (
                    'reported', 'acknowledged', 'investigating', 'approved', 
                    'work_scheduled', 'in_progress', 'completed', 'closed', 'rejected'
                )),
                completion_deadline DATE,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (original_defect_id) REFERENCES defects (id),
                FOREIGN KEY (inspection_id) REFERENCES inspections (id),
                FOREIGN KEY (reported_by) REFERENCES users (id)
            )
        """)
        
        # Defect approvals table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS defect_approvals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                defect_id INTEGER NOT NULL,
                owner_id INTEGER NOT NULL,
                approval_type TEXT NOT NULL CHECK (approval_type IN (
                    'initial_approval', 'completion_approval', 'cost_approval', 
                    'extension_approval', 'final_signoff'
                )),
                status TEXT DEFAULT 'pending' CHECK (status IN (
                    'pending', 'approved', 'rejected', 'conditional', 'expired'
                )),
                decision_date DATE,
                conditions TEXT,
                comments TEXT,
                approved_cost REAL,
                approved_timeline TEXT,
                review_required_by DATE,
                expires_at DATE,
                notification_sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (defect_id) REFERENCES defects (id) ON DELETE CASCADE,
                FOREIGN KEY (owner_id) REFERENCES users (id)
            )
        """)
        
        # Photo reviews table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS photo_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                photo_id INTEGER NOT NULL,
                reviewer_id INTEGER NOT NULL,
                review_type TEXT CHECK (review_type IN (
                    'quality_check', 'compliance_review', 'damage_assessment', 'progress_review'
                )),
                status TEXT DEFAULT 'pending' CHECK (status IN (
                    'pending', 'approved', 'rejected', 'needs_revision'
                )),
                quality_score INTEGER CHECK (quality_score BETWEEN 1 AND 5),
                clarity_score INTEGER CHECK (clarity_score BETWEEN 1 AND 5),
                relevance_score INTEGER CHECK (relevance_score BETWEEN 1 AND 5),
                comments TEXT,
                tags TEXT, -- JSON array of tags
                flagged_issues TEXT, -- JSON array of flagged issues
                review_date DATE DEFAULT (DATE('now')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (photo_id) REFERENCES defect_photos (id) ON DELETE CASCADE,
                FOREIGN KEY (reviewer_id) REFERENCES users (id)
            )
        """)
        
        # Owner preferences
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS owner_preferences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                auto_approve_minor BOOLEAN DEFAULT FALSE,
                notification_frequency TEXT DEFAULT 'daily' CHECK (notification_frequency IN (
                    'immediate', 'daily', 'weekly', 'monthly'
                )),
                approval_timeout_days INTEGER DEFAULT 7,
                cost_approval_threshold REAL DEFAULT 1000.0,
                email_notifications BOOLEAN DEFAULT TRUE,
                sms_notifications BOOLEAN DEFAULT FALSE,
                dashboard_layout TEXT, -- JSON
                report_preferences TEXT, -- JSON
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                UNIQUE(user_id)
            )
        """)
        
        self.db.commit()
    
    def render_main_dashboard(self, user_id: int):
        """Render the main Owner dashboard"""
        st.title("🏢 Owner Dashboard")
        
        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        metrics = self.get_owner_metrics(user_id)
        
        with col1:
            st.metric(
                "Pending Approvals", 
                metrics['pending_approvals'],
                delta=metrics['approvals_delta']
            )
        
        with col2:
            st.metric(
                "Active DLP Defects", 
                metrics['active_dlp_defects'],
                delta=metrics['dlp_delta']
            )
        
        with col3:
            st.metric(
                "Photos to Review", 
                metrics['photos_to_review'],
                delta=metrics['photos_delta']
            )
        
        with col4:
            st.metric(
                "Total Investment", 
                f"${metrics['total_investment']:,.0f}",
                delta=f"${metrics['investment_delta']:,.0f}"
            )
        
        # Quick action buttons
        st.subheader("🚀 Quick Actions")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("📋 Review Pending Approvals", use_container_width=True):
                st.session_state.active_tab = "approvals"
                st.rerun()
        
        with col2:
            if st.button("📸 Review Photos", use_container_width=True):
                st.session_state.active_tab = "photos"
                st.rerun()
        
        with col3:
            if st.button("🔧 Report DLP Defect", use_container_width=True):
                st.session_state.active_tab = "dlp"
                st.rerun()
        
        with col4:
            if st.button("📊 Generate Reports", use_container_width=True):
                st.session_state.active_tab = "reports"
                st.rerun()
        
        # Main content tabs
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📊 Overview", 
            "✅ Approvals", 
            "📸 Photo Review", 
            "🔧 DLP Management",
            "📈 Reports & Analytics", 
            "⚙️ Settings"
        ])
        
        with tab1:
            self.render_overview_tab(user_id)
        
        with tab2:
            self.render_approvals_tab(user_id)
        
        with tab3:
            self.render_photo_review_tab(user_id)
        
        with tab4:
            self.render_dlp_management_tab(user_id)
        
        with tab5:
            self.render_reports_tab(user_id)
        
        with tab6:
            self.render_settings_tab(user_id)
    
    def get_owner_metrics(self, user_id: int) -> Dict:
        """Calculate key metrics for the owner"""
        cursor = self.db.cursor()
        
        # Pending approvals
        cursor.execute("""
            SELECT COUNT(*) FROM defect_approvals 
            WHERE owner_id = ? AND status = 'pending'
        """, (user_id,))
        pending_approvals = cursor.fetchone()[0]
        
        # Active DLP defects
        cursor.execute("""
            SELECT COUNT(*) FROM dlp_defects 
            WHERE reported_by = ? AND status NOT IN ('completed', 'closed')
        """, (user_id,))
        active_dlp_defects = cursor.fetchone()[0]
        
        # Photos to review
        cursor.execute("""
            SELECT COUNT(*) FROM photo_reviews pr
            JOIN defect_photos dp ON pr.photo_id = dp.id
            JOIN defects d ON dp.defect_id = d.id
            JOIN inspections i ON d.inspection_id = i.id
            WHERE pr.reviewer_id = ? AND pr.status = 'pending'
        """, (user_id,))
        photos_to_review = cursor.fetchone()[0]
        
        # Total investment (estimated costs)
        cursor.execute("""
            SELECT COALESCE(SUM(estimated_cost), 0) FROM defects d
            JOIN inspections i ON d.inspection_id = i.id
            WHERE d.status IN ('open', 'in_progress')
        """)
        total_investment = cursor.fetchone()[0]
        
        # Calculate deltas (simplified for demo)
        return {
            'pending_approvals': pending_approvals,
            'active_dlp_defects': active_dlp_defects,
            'photos_to_review': photos_to_review,
            'total_investment': total_investment,
            'approvals_delta': 2,  # Mock delta
            'dlp_delta': -1,
            'photos_delta': 5,
            'investment_delta': 15000
        }
    
    def render_overview_tab(self, user_id: int):
        """Render the overview tab with key insights and charts"""
        st.subheader("📊 Owner Portfolio Overview")
        
        # Property portfolio summary
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("🏢 Property Portfolio Performance")
            portfolio_data = self.get_portfolio_performance(user_id)
            
            if not portfolio_data.empty:
                fig = px.bar(
                    portfolio_data, 
                    x='project_name', 
                    y='quality_score',
                    color='status',
                    title="Quality Scores by Project",
                    hover_data=['total_defects', 'critical_defects']
                )
                fig.update_layout(height=400, xaxis_tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No portfolio data available.")
        
        with col2:
            st.subheader("📈 Defect Status Distribution")
            defect_status_data = self.get_defect_status_distribution(user_id)
            
            if not defect_status_data.empty:
                fig = px.pie(
                    defect_status_data, 
                    values='count', 
                    names='status',
                    title="Current Defect Status"
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No defect data available.")
        
        # Recent activity timeline
        st.subheader("📅 Recent Activity Timeline")
        timeline_data = self.get_recent_activity_timeline(user_id)
        
        if not timeline_data.empty:
            fig = px.timeline(
                timeline_data,
                x_start='start_date',
                x_end='end_date',
                y='activity_type',
                color='priority',
                title="Recent Activities Requiring Attention",
                hover_data=['description']
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        # Critical alerts
        critical_alerts = self.get_critical_alerts(user_id)
        if critical_alerts:
            st.subheader("🚨 Critical Alerts")
            for alert in critical_alerts:
                alert_type = alert['type']
                if alert_type == 'critical':
                    st.error(f"🔴 **{alert['title']}** - {alert['message']}")
                elif alert_type == 'warning':
                    st.warning(f"🟡 **{alert['title']}** - {alert['message']}")
                else:
                    st.info(f"🔵 **{alert['title']}** - {alert['message']}")
    
    def render_approvals_tab(self, user_id: int):
        """Render the defect approvals management tab"""
        st.subheader("✅ Defect Approval Management")
        
        # Approval filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            approval_filter = st.selectbox(
                "Filter by Status",
                ["All", "Pending", "Approved", "Rejected", "Conditional"]
            )
        
        with col2:
            priority_filter = st.selectbox(
                "Filter by Priority",
                ["All", "Critical", "High", "Medium", "Low"]
            )
        
        with col3:
            date_range = st.date_input(
                "Date Range",
                value=[datetime.now().date() - timedelta(days=30), datetime.now().date()],
                max_value=datetime.now().date()
            )
        
        # Get pending approvals
        pending_approvals = self.get_pending_approvals(user_id, approval_filter, priority_filter)
        
        if not pending_approvals.empty:
            st.subheader("📋 Pending Approvals")
            
            for _, approval in pending_approvals.iterrows():
                with st.expander(
                    f"🔍 {approval['defect_title']} - {approval['project_name']} "
                    f"(Priority: {approval['priority']}) - ${approval.get('estimated_cost', 0):,.0f}"
                ):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.write(f"**Description:** {approval['description']}")
                        st.write(f"**Location:** {approval['location']}")
                        st.write(f"**Trade:** {approval['trade']}")
                        st.write(f"**Severity:** {approval['severity']}")
                        st.write(f"**Reported:** {approval['created_at']}")
                        
                        if approval.get('builder_response'):
                            st.write(f"**Builder Response:** {approval['builder_response']}")
                    
                    with col2:
                        # Approval actions
                        st.write("**Approval Actions:**")
                        
                        approval_decision = st.radio(
                            "Decision",
                            ["Approve", "Reject", "Conditional", "Request More Info"],
                            key=f"decision_{approval['defect_id']}"
                        )
                        
                        if approval_decision == "Conditional":
                            conditions = st.text_area(
                                "Conditions",
                                placeholder="Specify conditions for approval...",
                                key=f"conditions_{approval['defect_id']}"
                            )
                        
                        comments = st.text_area(
                            "Comments",
                            placeholder="Add your comments...",
                            key=f"comments_{approval['defect_id']}"
                        )
                        
                        if approval_decision == "Approve":
                            approved_cost = st.number_input(
                                "Approved Cost ($)",
                                min_value=0.0,
                                value=float(approval.get('estimated_cost', 0)),
                                key=f"cost_{approval['defect_id']}"
                            )
                        
                        col_a, col_b = st.columns(2)
                        
                        with col_a:
                            if st.button(
                                "✅ Submit Decision", 
                                key=f"submit_{approval['defect_id']}",
                                type="primary"
                            ):
                                success = self.process_approval_decision(
                                    approval['approval_id'],
                                    approval_decision,
                                    comments,
                                    conditions if approval_decision == "Conditional" else None,
                                    approved_cost if approval_decision == "Approve" else None
                                )
                                
                                if success:
                                    st.success("Decision submitted successfully!")
                                    st.rerun()
                                else:
                                    st.error("Failed to submit decision.")
                        
                        with col_b:
                            if st.button(
                                "📸 View Photos", 
                                key=f"photos_{approval['defect_id']}"
                            ):
                                st.session_state[f"show_photos_{approval['defect_id']}"] = True
                        
                        # Show photos if requested
                        if st.session_state.get(f"show_photos_{approval['defect_id']}", False):
                            photos = self.get_defect_photos(approval['defect_id'])
                            if photos:
                                st.write("**Associated Photos:**")
                                for photo in photos:
                                    st.image(photo['file_path'], caption=photo['caption'], width=200)
                            else:
                                st.info("No photos available for this defect.")
        else:
            st.info("No pending approvals found.")
        
        # Bulk approval actions
        if not pending_approvals.empty:
            st.subheader("⚡ Bulk Actions")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("✅ Approve All Minor Defects"):
                    count = self.bulk_approve_minor_defects(user_id)
                    st.success(f"Approved {count} minor defects.")
                    st.rerun()
            
            with col2:
                if st.button("📧 Send Reminder Notifications"):
                    count = self.send_approval_reminders(user_id)
                    st.success(f"Sent {count} reminder notifications.")
            
            with col3:
                export_format = st.selectbox("Export Format", ["Excel", "PDF", "CSV"])
                if st.button(f"📥 Export to {export_format}"):
                    file_path = self.export_approvals_data(user_id, export_format.lower())
                    st.success(f"Data exported to {file_path}")
    
    def render_photo_review_tab(self, user_id: int):
        """Render the photo review and quality assessment tab"""
        st.subheader("📸 Photo Review & Quality Assessment")
        
        # Photo review filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            review_filter = st.selectbox(
                "Review Status",
                ["Pending Review", "All Photos", "Approved", "Rejected", "Needs Revision"]
            )
        
        with col2:
            project_filter = st.selectbox(
                "Project",
                ["All Projects"] + self.get_user_projects(user_id)
            )
        
        with col3:
            photo_type_filter = st.selectbox(
                "Photo Type",
                ["All Types", "Before", "After", "Progress", "Defect", "Completion"]
            )
        
        # Get photos for review
        photos_for_review = self.get_photos_for_review(
            user_id, review_filter, project_filter, photo_type_filter
        )
        
        if not photos_for_review.empty:
            # Photo review interface
            st.subheader("🔍 Photo Review Interface")
            
            # Pagination
            photos_per_page = 6
            total_photos = len(photos_for_review)
            total_pages = (total_photos - 1) // photos_per_page + 1
            
            if total_pages > 1:
                page = st.slider("Page", 1, total_pages, 1)
                start_idx = (page - 1) * photos_per_page
                end_idx = min(start_idx + photos_per_page, total_photos)
                photos_subset = photos_for_review.iloc[start_idx:end_idx]
            else:
                photos_subset = photos_for_review
            
            # Display photos in grid
            cols = st.columns(3)
            
            for idx, (_, photo) in enumerate(photos_subset.iterrows()):
                col = cols[idx % 3]
                
                with col:
                    with st.container():
                        # Display photo (placeholder for actual image)
                        st.image(
                            "https://via.placeholder.com/300x200?text=Photo", 
                            caption=f"{photo['defect_title']} - {photo['location']}",
                            use_column_width=True
                        )
                        
                        st.write(f"**Defect:** {photo['defect_title']}")
                        st.write(f"**Project:** {photo['project_name']}")
                        st.write(f"**Uploaded:** {photo['created_at']}")
                        
                        # Quick review actions
                        review_action = st.selectbox(
                            "Quick Review",
                            ["Select Action", "Approve", "Reject", "Needs Revision", "Detailed Review"],
                            key=f"quick_review_{photo['photo_id']}"
                        )
                        
                        if review_action != "Select Action":
                            if st.button(f"Submit", key=f"quick_submit_{photo['photo_id']}"):
                                if review_action == "Detailed Review":
                                    st.session_state[f"detailed_review_{photo['photo_id']}"] = True
                                else:
                                    self.quick_photo_review(photo['photo_id'], user_id, review_action)
                                    st.success(f"Photo {review_action.lower()}d!")
                                    st.rerun()
                        
                        # Detailed review modal
                        if st.session_state.get(f"detailed_review_{photo['photo_id']}", False):
                            self.render_detailed_photo_review(photo, user_id)
        else:
            st.info("No photos available for review.")
        
        # Photo review statistics
        st.subheader("📊 Review Statistics")
        review_stats = self.get_photo_review_statistics(user_id)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Photos Reviewed Today", review_stats['reviewed_today'])
        with col2:
            st.metric("Average Quality Score", f"{review_stats['avg_quality']:.1f}/5")
        with col3:
            st.metric("Approval Rate", f"{review_stats['approval_rate']:.1f}%")
        with col4:
            st.metric("Pending Reviews", review_stats['pending_count'])
    
    def render_detailed_photo_review(self, photo: pd.Series, user_id: int):
        """Render detailed photo review interface"""
        with st.expander(f"🔍 Detailed Review: {photo['defect_title']}", expanded=True):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Larger photo display
                st.image(
                    "https://via.placeholder.com/600x400?text=Detailed+Photo",
                    caption=f"Detailed view: {photo['caption'] or 'No caption'}",
                    use_column_width=True
                )
                
                # Photo metadata
                st.write("**Photo Information:**")
                st.write(f"- File: {photo['file_name']}")
                st.write(f"- Size: {photo.get('file_size', 'Unknown')} KB")
                st.write(f"- Type: {photo.get('photo_type', 'Unknown')}")
                st.write(f"- Uploaded: {photo['created_at']}")
            
            with col2:
                st.write("**Quality Assessment:**")
                
                # Quality scoring
                quality_score = st.slider(
                    "Overall Quality", 1, 5, 3,
                    help="Rate the overall quality of the photo",
                    key=f"quality_{photo['photo_id']}"
                )
                
                clarity_score = st.slider(
                    "Clarity & Focus", 1, 5, 3,
                    help="How clear and well-focused is the photo?",
                    key=f"clarity_{photo['photo_id']}"
                )
                
                relevance_score = st.slider(
                    "Relevance", 1, 5, 3,
                    help="How relevant is this photo to the defect?",
                    key=f"relevance_{photo['photo_id']}"
                )
                
                # Review decision
                review_decision = st.radio(
                    "Review Decision",
                    ["Approved", "Rejected", "Needs Revision"],
                    key=f"decision_{photo['photo_id']}"
                )
                
                # Tags and categories
                photo_tags = st.multiselect(
                    "Photo Tags",
                    ["Before", "After", "Close-up", "Overview", "Progress", "Issue", "Resolution"],
                    key=f"tags_{photo['photo_id']}"
                )
                
                # Flagged issues
                if review_decision in ["Rejected", "Needs Revision"]:
                    flagged_issues = st.multiselect(
                        "Flagged Issues",
                        ["Poor Lighting", "Blurry", "Wrong Angle", "Incomplete Coverage", 
                         "Not Relevant", "Safety Concern", "Other"],
                        key=f"issues_{photo['photo_id']}"
                    )
                
                # Comments
                review_comments = st.text_area(
                    "Review Comments",
                    placeholder="Add detailed feedback about this photo...",
                    key=f"review_comments_{photo['photo_id']}"
                )
                
                # Submit review
                if st.button(
                    "📝 Submit Detailed Review", 
                    key=f"submit_detailed_{photo['photo_id']}",
                    type="primary"
                ):
                    success = self.submit_detailed_photo_review(
                        photo['photo_id'], user_id, review_decision,
                        quality_score, clarity_score, relevance_score,
                        photo_tags, 
                        flagged_issues if review_decision in ["Rejected", "Needs Revision"] else [],
                        review_comments
                    )
                    
                    if success:
                        st.success("Detailed review submitted successfully!")
                        st.session_state[f"detailed_review_{photo['photo_id']}"] = False
                        st.rerun()
                    else:
                        st.error("Failed to submit review.")
    
    def render_dlp_management_tab(self, user_id: int):
        """Render the DLP (Defects Liability Period) management tab"""
        st.subheader("🔧 DLP Defect Management")
        
        # DLP overview metrics
        dlp_metrics = self.get_dlp_metrics(user_id)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Active DLP Defects", dlp_metrics['active_defects'])
        with col2:
            st.metric("This Month", dlp_metrics['this_month'], dlp_metrics['month_delta'])
        with col3:
            st.metric("Average Resolution Time", f"{dlp_metrics['avg_resolution_days']} days")
        with col4:
            st.metric("Total DLP Cost", f"${dlp_metrics['total_cost']:,.0f}")
        
        # Create two main sections
        tab1, tab2, tab3 = st.tabs(["📝 Report New DLP Defect", "📋 Manage Existing", "📊 DLP Analytics"])
        
        with tab1:
            self.render_new_dlp_defect_form(user_id)
        
        with tab2:
            self.render_existing_dlp_defects(user_id)
        
        with tab3:
            self.render_dlp_analytics(user_id)
    
    def render_new_dlp_defect_form(self, user_id: int):
        """Render form for reporting new DLP defects"""
        st.subheader("📝 Report New DLP Defect")
        
        with st.form("new_dlp_defect"):
            col1, col2 = st.columns(2)
            
            with col1:
                # Basic defect information
                defect_title = st.text_input("Defect Title*", placeholder="Brief description of the defect")
                
                defect_description = st.text_area(
                    "Detailed Description*",
                    placeholder="Provide detailed description of the defect, including when it was discovered and how it affects the property...",
                    height=100
                )
                
                location = st.text_input("Location*", placeholder="e.g., Unit 12, Kitchen, South Wall")
                
                trade = st.selectbox("Trade Category*", [
                    "Structural", "Waterproofing", "Plumbing", "Electrical", 
                    "HVAC", "Painting", "Tiling", "Flooring", "Roofing", 
                    "Windows/Doors", "Landscaping", "Other"
                ])
                
                severity = st.selectbox("Severity Level*", [
                    "Critical - Immediate safety/structural concern",
                    "Major - Significant functionality impact", 
                    "Minor - Cosmetic or minor functionality issue",
                    "Observation - Monitoring required"
                ])
            
            with col2:
                # DLP specific information
                dlp_category = st.selectbox("DLP Category*", [
                    "Structural", "Waterproofing", "Mechanical", 
                    "Electrical", "Finishing", "External Works"
                ])
                
                discovery_method = st.selectbox("How was this discovered?", [
                    "Routine Inspection", "Tenant Complaint", "Maintenance Visit",
                    "Weather Event", "Warranty Claim", "Other"
                ])
                
                related_to_weather = st.checkbox("Related to weather event")
                
                if related_to_weather:
                    weather_event = st.text_input("Weather Event Details")
                
                estimated_cost = st.number_input(
                    "Estimated Repair Cost ($)", 
                    min_value=0.0, 
                    value=0.0,
                    help="Estimated cost to repair this defect"
                )
                
                priority = st.selectbox("Priority Level", [
                    "1 - Critical (Immediate attention)",
                    "2 - High (Within 1 week)", 
                    "3 - Medium (Within 1 month)",
                    "4 - Low (Next scheduled maintenance)",
                    "5 - Deferred (End of DLP)"
                ])
                
                # Link to original inspection
                available_inspections = self.get_available_inspections_for_dlp(user_id)
                original_inspection = st.selectbox(
                    "Link to Original Inspection (Optional)",
                    ["None"] + [f"ID: {i[0]} - {i[1]}" for i in available_inspections]
                )
            
            # Photo upload
            st.subheader("📸 Upload Photos")
            uploaded_photos = st.file_uploader(
                "Defect Photos", 
                type=['jpg', 'jpeg', 'png'],
                accept_multiple_files=True,
                help="Upload photos showing the defect clearly"
            )
            
            # Additional notes
            notes = st.text_area(
                "Additional Notes",
                placeholder="Any additional information, previous repair attempts, contractor details, etc."
            )
            
            # Submit button
            submitted = st.form_submit_button("🚀 Submit DLP Defect Report", type="primary")
            
            if submitted:
                if defect_title and defect_description and location:
                    # Process the DLP defect submission
                    dlp_defect_id = self.create_dlp_defect(
                        user_id, defect_title, defect_description, location,
                        trade, severity, dlp_category, discovery_method,
                        estimated_cost, priority, notes, uploaded_photos,
                        original_inspection
                    )
                    
                    if dlp_defect_id:
                        st.success(f"✅ DLP Defect reported successfully! ID: DLP-{dlp_defect_id:05d}")
                        
                        # Show next steps
                        st.info("""
                        **Next Steps:**
                        1. Your DLP defect has been logged and assigned ID: DLP-{:05d}
                        2. The relevant contractor will be notified within 24 hours
                        3. You will receive updates on repair scheduling and progress
                        4. Track progress in the 'Manage Existing' tab
                        """.format(dlp_defect_id))
                    else:
                        st.error("Failed to submit DLP defect. Please try again.")
                else:
                    st.error("Please fill in all required fields marked with *")
    
    def render_existing_dlp_defects(self, user_id: int):
        """Render existing DLP defects management"""
        st.subheader("📋 Existing DLP Defects")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.selectbox(
                "Status Filter",
                ["All", "Reported", "Acknowledged", "In Progress", "Completed", "Closed"]
            )
        
        with col2:
            severity_filter = st.selectbox(
                "Severity Filter", 
                ["All", "Critical", "Major", "Minor", "Observation"]
            )
        
        with col3:
            sort_by = st.selectbox(
                "Sort By",
                ["Date Reported", "Priority", "Status", "Cost", "Due Date"]
            )
        
        # Get DLP defects
        dlp_defects = self.get_dlp_defects(user_id, status_filter, severity_filter, sort_by)
        
        if not dlp_defects.empty:
            for _, defect in dlp_defects.iterrows():
                # Create status-based styling
                status_color = {
                    'reported': '🟡', 'acknowledged': '🔵', 'investigating': '🟠',
                    'approved': '🟢', 'work_scheduled': '🟣', 'in_progress': '⚙️',
                    'completed': '✅', 'closed': '⚫', 'rejected': '🔴'
                }.get(defect['status'], '⚪')
                
                with st.expander(
                    f"{status_color} DLP-{defect['id']:05d}: {defect['title']} "
                    f"({defect['severity']}) - ${defect.get('estimated_repair_cost', 0):,.0f}"
                ):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.write(f"**Description:** {defect['description']}")
                        st.write(f"**Location:** {defect['location']}")
                        st.write(f"**Category:** {defect['dlp_category']}")
                        st.write(f"**Reported:** {defect['reported_date']}")
                        st.write(f"**Discovery Method:** {defect['discovery_method']}")
                        
                        if defect['contractor_response_date']:
                            st.write(f"**Contractor Response:** {defect['contractor_response_date']}")
                        
                        if defect['completion_deadline']:
                            st.write(f"**Deadline:** {defect['completion_deadline']}")
                        
                        if defect['notes']:
                            st.write(f"**Notes:** {defect['notes']}")
                    
                    with col2:
                        # Status updates and actions
                        st.write("**Current Status:**")
                        st.write(f"{status_color} {defect['status'].replace('_', ' ').title()}")
                        
                        if defect['warranty_status']:
                            warranty_color = '🟢' if defect['warranty_status'] == 'active' else '🔴'
                            st.write(f"**Warranty:** {warranty_color} {defect['warranty_status'].title()}")
                        
                        # Action buttons
                        col_a, col_b = st.columns(2)
                        
                        with col_a:
                            if st.button(f"📞 Contact Contractor", key=f"contact_{defect['id']}"):
                                self.initiate_contractor_contact(defect['id'])
                                st.success("Contractor contacted!")
                        
                        with col_b:
                            if st.button(f"📈 Update Status", key=f"update_{defect['id']}"):
                                st.session_state[f"update_status_{defect['id']}"] = True
                        
                        # Status update form
                        if st.session_state.get(f"update_status_{defect['id']}", False):
                            with st.form(f"status_update_{defect['id']}"):
                                new_status = st.selectbox(
                                    "New Status",
                                    ["reported", "acknowledged", "investigating", "approved", 
                                     "work_scheduled", "in_progress", "completed", "closed"]
                                )
                                
                                status_notes = st.text_area("Update Notes")
                                
                                if st.form_submit_button("Update Status"):
                                    self.update_dlp_defect_status(defect['id'], new_status, status_notes)
                                    st.success("Status updated!")
                                    st.session_state[f"update_status_{defect['id']}"] = False
                                    st.rerun()
        else:
            st.info("No DLP defects found matching the current filters.")
    
    def render_dlp_analytics(self, user_id: int):
        """Render DLP analytics and insights"""
        st.subheader("📊 DLP Analytics & Insights")
        
        # Time-based analysis
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 DLP Defects Over Time")
            timeline_data = self.get_dlp_timeline_data(user_id)
            
            if not timeline_data.empty:
                fig = px.line(
                    timeline_data, 
                    x='month', 
                    y='defect_count',
                    color='severity',
                    title="DLP Defects Reported by Month"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("💰 Cost Analysis")
            cost_data = self.get_dlp_cost_analysis(user_id)
            
            if not cost_data.empty:
                fig = px.bar(
                    cost_data,
                    x='category',
                    y='total_cost',
                    title="DLP Costs by Category"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Performance metrics
        st.subheader("⏱️ Performance Metrics")
        
        col1, col2, col3 = st.columns(3)
        
        performance_metrics = self.get_dlp_performance_metrics(user_id)
        
        with col1:
            st.metric(
                "Average Resolution Time",
                f"{performance_metrics['avg_resolution_days']:.1f} days",
                delta=f"{performance_metrics['resolution_trend']:.1f} days vs last period"
            )
        
        with col2:
            st.metric(
                "Response Rate",
                f"{performance_metrics['response_rate']:.1f}%",
                delta=f"{performance_metrics['response_trend']:.1f}%"
            )
        
        with col3:
            st.metric(
                "Cost Variance",
                f"{performance_metrics['cost_variance']:.1f}%",
                delta=f"{performance_metrics['cost_trend']:.1f}%"
            )
        
        # Detailed analytics table
        st.subheader("📋 Detailed Analytics")
        
        analytics_data = self.get_detailed_dlp_analytics(user_id)
        
        if not analytics_data.empty:
            st.dataframe(
                analytics_data,
                use_container_width=True,
                hide_index=True
            )
        
        # Export analytics
        if st.button("📥 Export Analytics Report"):
            report_path = self.export_dlp_analytics(user_id)
            st.success(f"Analytics report exported to {report_path}")
    
    def render_reports_tab(self, user_id: int):
        """Render reports and analytics tab"""
        st.subheader("📈 Owner Reports & Analytics")
        
        # Report type selection
        report_type = st.selectbox(
            "Select Report Type",
            [
                "Executive Summary", 
                "DLP Status Report", 
                "Photo Review Summary",
                "Financial Impact Analysis",
                "Quality Trends Report",
                "Contractor Performance"
            ]
        )
        
        # Report parameters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            date_range = st.date_input(
                "Report Period",
                value=[datetime.now().date() - timedelta(days=90), datetime.now().date()]
            )
        
        with col2:
            format_type = st.selectbox("Format", ["PDF", "Excel", "Word"])
        
        with col3:
            include_photos = st.checkbox("Include Photos", value=True)
        
        # Generate report
        if st.button("📊 Generate Report", type="primary"):
            with st.spinner("Generating report..."):
                report_path = self.generate_owner_report(
                    user_id, report_type, date_range, format_type, include_photos
                )
            
            if report_path:
                st.success("Report generated successfully!")
                with open(report_path, 'rb') as file:
                    st.download_button(
                        f"📥 Download {report_type}",
                        data=file.read(),
                        file_name=os.path.basename(report_path),
                        mime=self.get_mime_type(format_type)
                    )
            else:
                st.error("Failed to generate report.")
        
        # Recent reports
        st.subheader("📋 Recent Reports")
        recent_reports = self.get_recent_reports(user_id)
        
        if not recent_reports.empty:
            for _, report in recent_reports.iterrows():
                with st.expander(f"📄 {report['report_name']} ({report['created_at']})"):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.write(f"**Type:** {report['report_type']}")
                        st.write(f"**Generated:** {report['created_at']}")
                        st.write(f"**Status:** {report['status']}")
                        if report['file_size']:
                            st.write(f"**Size:** {report['file_size']} KB")
                    
                    with col2:
                        if report['status'] == 'completed':
                            if st.button(f"📥 Download", key=f"download_{report['id']}"):
                                # Handle download
                                st.success("Download started!")
                        
                        if st.button(f"🗑️ Delete", key=f"delete_{report['id']}"):
                            self.delete_report(report['id'])
                            st.success("Report deleted!")
                            st.rerun()
        else:
            st.info("No recent reports found.")
    
    def render_settings_tab(self, user_id: int):
        """Render owner preferences and settings"""
        st.subheader("⚙️ Owner Preferences & Settings")
        
        # Get current preferences
        current_prefs = self.get_owner_preferences(user_id)
        
        # Approval settings
        st.subheader("✅ Approval Settings")
        
        col1, col2 = st.columns(2)
        
        with col1:
            auto_approve_minor = st.checkbox(
                "Auto-approve minor defects",
                value=current_prefs.get('auto_approve_minor', False),
                help="Automatically approve defects with cost under threshold"
            )
            
            cost_threshold = st.number_input(
                "Auto-approval cost threshold ($)",
                min_value=0.0,
                value=current_prefs.get('cost_approval_threshold', 1000.0),
                help="Maximum cost for auto-approval"
            )
        
        with col2:
            approval_timeout = st.number_input(
                "Approval timeout (days)",
                min_value=1,
                max_value=30,
                value=current_prefs.get('approval_timeout_days', 7),
                help="Days before approval request expires"
            )
            
            notification_frequency = st.selectbox(
                "Notification frequency",
                ["Immediate", "Daily", "Weekly", "Monthly"],
                index=["immediate", "daily", "weekly", "monthly"].index(
                    current_prefs.get('notification_frequency', 'daily')
                )
            )
        
        # Notification preferences
        st.subheader("🔔 Notification Preferences")
        
        col1, col2 = st.columns(2)
        
        with col1:
            email_notifications = st.checkbox(
                "Email notifications",
                value=current_prefs.get('email_notifications', True)
            )
            
            sms_notifications = st.checkbox(
                "SMS notifications",
                value=current_prefs.get('sms_notifications', False)
            )
        
        with col2:
            # Notification types
            st.write("**Notification Types:**")
            
            notify_new_defects = st.checkbox("New defects reported", value=True)
            notify_approvals = st.checkbox("Approval requests", value=True)
            notify_completion = st.checkbox("Work completion", value=True)
            notify_dlp = st.checkbox("DLP defects", value=True)
            notify_photos = st.checkbox("Photo reviews", value=False)
        
        # Dashboard preferences
        st.subheader("📊 Dashboard Preferences")
        
        dashboard_layout = st.radio(
            "Dashboard layout",
            ["Compact", "Detailed", "Analytics-focused"],
            index=["compact", "detailed", "analytics"].index(
                current_prefs.get('dashboard_layout', 'detailed')
            )
        )
        
        default_date_range = st.selectbox(
            "Default date range for reports",
            ["Last 30 days", "Last 90 days", "Last 6 months", "Last year"],
            index=2
        )
        
        # Photo review preferences
        st.subheader("📸 Photo Review Preferences")
        
        col1, col2 = st.columns(2)
        
        with col1:
            auto_quality_check = st.checkbox(
                "Automatic quality scoring",
                value=True,
                help="Use AI to pre-score photo quality"
            )
            
            require_captions = st.checkbox(
                "Require photo captions",
                value=False,
                help="Require captions for all uploaded photos"
            )
        
        with col2:
            photo_review_priority = st.selectbox(
                "Photo review priority order",
                ["By upload date", "By defect severity", "By project priority"]
            )
            
            batch_review_size = st.number_input(
                "Batch review size",
                min_value=5,
                max_value=50,
                value=20,
                help="Number of photos to review in each batch"
            )
        
        # Save preferences
        if st.button("💾 Save Preferences", type="primary"):
            preferences = {
                'auto_approve_minor': auto_approve_minor,
                'cost_approval_threshold': cost_threshold,
                'approval_timeout_days': approval_timeout,
                'notification_frequency': notification_frequency.lower(),
                'email_notifications': email_notifications,
                'sms_notifications': sms_notifications,
                'dashboard_layout': dashboard_layout.lower().replace('-', '_'),
                'default_date_range': default_date_range,
                'auto_quality_check': auto_quality_check,
                'require_captions': require_captions,
                'photo_review_priority': photo_review_priority,
                'batch_review_size': batch_review_size,
                'notify_new_defects': notify_new_defects,
                'notify_approvals': notify_approvals,
                'notify_completion': notify_completion,
                'notify_dlp': notify_dlp,
                'notify_photos': notify_photos
            }
            
            success = self.save_owner_preferences(user_id, preferences)
            
            if success:
                st.success("✅ Preferences saved successfully!")
            else:
                st.error("❌ Failed to save preferences.")
        
        # Export/Import settings
        st.subheader("🔄 Settings Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📥 Export Settings"):
                settings_file = self.export_owner_settings(user_id)
                st.success(f"Settings exported to {settings_file}")
        
        with col2:
            uploaded_settings = st.file_uploader("Import Settings", type=['json'])
            
            if uploaded_settings and st.button("📤 Import Settings"):
                success = self.import_owner_settings(user_id, uploaded_settings)
                if success:
                    st.success("Settings imported successfully!")
                    st.rerun()
                else:
                    st.error("Failed to import settings.")
    
    # Helper methods for data operations
    def get_portfolio_performance(self, user_id: int) -> pd.DataFrame:
        """Get portfolio performance data"""
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT project_name, quality_score, status, total_defects, critical_defects
            FROM inspections 
            ORDER BY inspection_date DESC
            LIMIT 10
        """)
        
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=[
            'project_name', 'quality_score', 'status', 'total_defects', 'critical_defects'
        ])
    
    def get_defect_status_distribution(self, user_id: int) -> pd.DataFrame:
        """Get defect status distribution"""
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT status, COUNT(*) as count
            FROM defects
            GROUP BY status
        """)
        
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=['status', 'count'])
    
    def get_recent_activity_timeline(self, user_id: int) -> pd.DataFrame:
        """Get recent activity timeline data"""
        # Mock data for demonstration
        data = [
            {
                'activity_type': 'Defect Approval',
                'start_date': datetime.now() - timedelta(days=5),
                'end_date': datetime.now() - timedelta(days=3),
                'priority': 'High',
                'description': 'Critical defect requiring immediate approval'
            },
            {
                'activity_type': 'Photo Review',
                'start_date': datetime.now() - timedelta(days=3),
                'end_date': datetime.now() - timedelta(days=1),
                'priority': 'Medium',
                'description': 'Batch photo review for Project ABC'
            }
        ]
        
        return pd.DataFrame(data)
    
    def get_critical_alerts(self, user_id: int) -> List[Dict]:
        """Get critical alerts for the owner"""
        alerts = [
            {
                'type': 'critical',
                'title': 'Urgent Approval Required',
                'message': 'Critical structural defect requires immediate approval'
            },
            {
                'type': 'warning', 
                'title': 'DLP Deadline Approaching',
                'message': '3 DLP defects approaching warranty expiration'
            }
        ]
        
        return alerts
    
    def process_approval_decision(self, approval_id: int, decision: str, 
                                comments: str, conditions: str = None, 
                                approved_cost: float = None) -> bool:
        """Process approval decision"""
        cursor = self.db.cursor()
        
        try:
            cursor.execute("""
                UPDATE defect_approvals SET
                    status = ?, decision_date = DATE('now'),
                    comments = ?, conditions = ?, approved_cost = ?
                WHERE id = ?
            """, (decision.lower(), comments, conditions, approved_cost, approval_id))
            
            self.db.commit()
            return True
        except Exception as e:
            print(f"Error processing approval: {e}")
            return False
    
    def create_dlp_defect(self, user_id: int, title: str, description: str,
                         location: str, trade: str, severity: str, 
                         dlp_category: str, discovery_method: str,
                         estimated_cost: float, priority: str, notes: str,
                         photos: List, original_inspection: str) -> Optional[int]:
        """Create new DLP defect"""
        cursor = self.db.cursor()
        
        try:
            # Extract priority number
            priority_num = int(priority.split(' - ')[0])
            
            # Extract severity
            severity_clean = severity.split(' - ')[0].lower()
            
            cursor.execute("""
                INSERT INTO dlp_defects (
                    defect_id, title, description, location, trade, severity,
                    dlp_category, reported_by, reported_date, discovery_method,
                    estimated_repair_cost, priority, notes, inspection_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, DATE('now'), ?, ?, ?, ?, ?)
            """, (
                f"DLP-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                title, description, location, trade, severity_clean,
                dlp_category.lower(), user_id, discovery_method.lower(),
                estimated_cost, priority_num, notes, 1  # Mock inspection_id
            ))
            
            dlp_id = cursor.lastrowid
            
            # Process photos if any
            if photos:
                self.process_dlp_photos(dlp_id, photos)
            
            self.db.commit()
            return dlp_id
            
        except Exception as e:
            print(f"Error creating DLP defect: {e}")
            return None
    
    def save_owner_preferences(self, user_id: int, preferences: Dict) -> bool:
        """Save owner preferences"""
        cursor = self.db.cursor()
        
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO owner_preferences (
                    user_id, auto_approve_minor, notification_frequency,
                    approval_timeout_days, cost_approval_threshold,
                    email_notifications, sms_notifications,
                    dashboard_layout, report_preferences
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id,
                preferences['auto_approve_minor'],
                preferences['notification_frequency'],
                preferences['approval_timeout_days'],
                preferences['cost_approval_threshold'],
                preferences['email_notifications'],
                preferences['sms_notifications'],
                preferences['dashboard_layout'],
                json.dumps(preferences)  # Store full preferences as JSON
            ))
            
            self.db.commit()
            return True
            
        except Exception as e:
            print(f"Error saving preferences: {e}")
            return False
    
    def get_owner_preferences(self, user_id: int) -> Dict:
        """Get owner preferences"""
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT * FROM owner_preferences WHERE user_id = ?
        """, (user_id,))
        
        result = cursor.fetchone()
        
        if result:
            # Return preferences with defaults
            return {
                'auto_approve_minor': result[2] if len(result) > 2 else False,
                'notification_frequency': result[3] if len(result) > 3 else 'daily',
                'approval_timeout_days': result[4] if len(result) > 4 else 7,
                'cost_approval_threshold': result[5] if len(result) > 5 else 1000.0,
                'email_notifications': result[6] if len(result) > 6 else True,
                'sms_notifications': result[7] if len(result) > 7 else False,
                'dashboard_layout': result[8] if len(result) > 8 else 'detailed'
            }
        else:
            # Return defaults
            return {
                'auto_approve_minor': False,
                'notification_frequency': 'daily',
                'approval_timeout_days': 7,
                'cost_approval_threshold': 1000.0,
                'email_notifications': True,
                'sms_notifications': False,
                'dashboard_layout': 'detailed'
            }
    
    # Placeholder methods for demonstration
    def get_pending_approvals(self, user_id: int, approval_filter: str, priority_filter: str) -> pd.DataFrame:
        """Get pending approvals (mock data)"""
        data = [
            {
                'approval_id': 1, 'defect_id': 1, 'defect_title': 'Paint finish uneven',
                'description': 'Uneven paint application on living room walls',
                'location': 'Unit 12, Living Room', 'trade': 'Painting',
                'severity': 'Minor', 'priority': 3, 'estimated_cost': 500,
                'project_name': 'Harbour Views', 'created_at': '2024-01-15',
                'builder_response': 'Will schedule touch-up work next week'
            }
        ]
        return pd.DataFrame(data)
    
    def get_photos_for_review(self, user_id: int, review_filter: str, 
                            project_filter: str, photo_type_filter: str) -> pd.DataFrame:
        """Get photos for review (mock data)"""
        data = [
            {
                'photo_id': 1, 'defect_title': 'Paint finish uneven',
                'project_name': 'Harbour Views', 'location': 'Living Room',
                'created_at': '2024-01-15', 'file_name': 'paint_defect_001.jpg',
                'caption': 'Uneven paint finish on south wall'
            }
        ]
        return pd.DataFrame(data)
    
    def get_dlp_defects(self, user_id: int, status_filter: str, 
                       severity_filter: str, sort_by: str) -> pd.DataFrame:
        """Get DLP defects (mock data)"""
        data = [
            {
                'id': 1, 'title': 'Water stain on ceiling',
                'description': 'Water stain appeared after recent rain',
                'location': 'Unit 5, Bedroom', 'dlp_category': 'waterproofing',
                'severity': 'major', 'status': 'reported',
                'reported_date': '2024-01-10', 'discovery_method': 'tenant_complaint',
                'estimated_repair_cost': 1200, 'warranty_status': 'active',
                'contractor_response_date': None, 'completion_deadline': None,
                'notes': 'Tenant noticed stain after heavy rainfall'
            }
        ]
        return pd.DataFrame(data)
    
    # Additional placeholder methods
    def get_user_projects(self, user_id: int) -> List[str]:
        return ["Harbour Views", "Cityscape Towers", "Garden Grove"]
    
    def get_dlp_metrics(self, user_id: int) -> Dict:
        return {
            'active_defects': 5, 'this_month': 2, 'month_delta': 1,
            'avg_resolution_days': 14, 'total_cost': 15000
        }
    
    def get_mime_type(self, format_type: str) -> str:
        mime_types = {
            'PDF': 'application/pdf',
            'Excel': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'Word': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        }
        return mime_types.get(format_type, 'application/octet-stream')


def render_owner_interface(db_connection, user_id: int):
    """Main function to render Owner role interface"""
    dashboard = OwnerDashboard(db_connection)
    dashboard.render_main_dashboard(user_id)


# Example usage in main Streamlit app:
if __name__ == "__main__":
    st.set_page_config(
        page_title="Building Inspection System - Owner",
        page_icon="🏢",
        layout="wide"
    )
    
    # Mock database connection for testing
    import sqlite3
    db = sqlite3.connect(':memory:')
    
    # Render the Owner interface
    render_owner_interface(db, user_id=1)