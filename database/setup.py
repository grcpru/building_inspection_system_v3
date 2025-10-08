"""
Building Inspection System V3 - Enhanced Database Setup
======================================================
Updated to integrate Inspector data processing with existing advanced schema.
Maintains all existing functionality while adding specific tables for cross-role data access.
"""

import sqlite3
import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any  # Added Any here
import pandas as pd
import logging
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Enhanced database management system with Inspector integration"""
    
    def __init__(self, db_path: str = "building_inspection.db"):
        self.db_path = db_path
        self.migrations_dir = Path("migrations")
        self.migrations_dir.mkdir(exist_ok=True)
        self.connection = None
        
    def connect(self) -> sqlite3.Connection:
        """Establish database connection with optimizations"""
        if not self.connection:
            self.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            # Enable foreign keys and optimize settings
            self.connection.execute("PRAGMA foreign_keys = ON")
            self.connection.execute("PRAGMA journal_mode = WAL")
            self.connection.execute("PRAGMA synchronous = NORMAL")
            self.connection.execute("PRAGMA cache_size = 10000")
            self.connection.execute("PRAGMA temp_store = MEMORY")
        
        return self.connection
    
    def initialize_database(self, force_recreate: bool = False):
        """Initialize database with complete schema including Inspector integration"""
        if force_recreate and os.path.exists(self.db_path):
            os.remove(self.db_path)
            logger.info(f"Removed existing database: {self.db_path}")
        
        conn = self.connect()
        logger.info("Initializing Building Inspection System database...")
        
        # Create all existing tables
        self.create_core_tables()
        self.create_user_tables()
        self.create_inspection_tables()
        self.create_defect_tables()
        self.create_workflow_tables()
        self.create_report_tables()
        self.create_audit_tables()
        
        # NEW: Create Inspector integration tables
        self.create_inspector_integration_tables()
        
        # Create indexes for performance
        self.create_indexes()
        
        # Create initial data
        self.seed_initial_data()
        
        # Create migration tracking
        self.setup_migration_tracking()
        
        conn.commit()
        logger.info("Database initialization completed successfully!")
    
    def create_inspector_integration_tables(self):
        """Create tables specifically for Inspector data processing integration"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Buildings table for Inspector processed data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_buildings (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                address TEXT,
                inspection_date DATE,
                inspector_id INTEGER,
                total_units INTEGER DEFAULT 0,
                total_defects INTEGER DEFAULT 0,
                defect_rate REAL DEFAULT 0.0,
                ready_units INTEGER DEFAULT 0,
                ready_pct REAL DEFAULT 0.0,
                quality_score REAL DEFAULT 0.0,
                unit_types TEXT,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (inspector_id) REFERENCES users (id)
            )
        """)
        
        # Inspector inspection records
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_inspections (
                id TEXT PRIMARY KEY,
                building_id TEXT NOT NULL,
                inspection_date DATE NOT NULL,
                inspector_name TEXT NOT NULL,
                total_units INTEGER DEFAULT 0,
                total_defects INTEGER DEFAULT 0,
                defect_rate REAL DEFAULT 0.0,
                ready_units INTEGER DEFAULT 0,
                ready_pct REAL DEFAULT 0.0,
                urgent_defects INTEGER DEFAULT 0,
                high_priority_defects INTEGER DEFAULT 0,
                avg_defects_per_unit REAL DEFAULT 0.0,
                original_filename TEXT,
                processing_metadata TEXT,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (building_id) REFERENCES inspector_buildings (id)
            )
        """)
        
        # Inspector inspection items (detailed defect data from CSV processing)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_inspection_items (
                id TEXT PRIMARY KEY,
                inspection_id TEXT NOT NULL,
                unit TEXT NOT NULL,
                unit_type TEXT,
                inspection_date DATE,
                room TEXT,
                component TEXT,
                trade TEXT,
                status_class TEXT CHECK (status_class IN ('OK', 'Not OK', 'Blank')),
                urgency TEXT CHECK (urgency IN ('Normal', 'High Priority', 'Urgent')),
                planned_completion DATE,
                owner_signoff_timestamp TIMESTAMP,
                original_status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (inspection_id) REFERENCES inspector_inspections (id)
            )
        """)
        
        # Trade mappings for Inspector processing
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_trade_mappings (
                id TEXT PRIMARY KEY,
                room TEXT NOT NULL,
                component TEXT NOT NULL,
                trade TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(room, component)
            )
        """)
        
        # Work orders generated from Inspector data for Builder role
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_work_orders (
                id TEXT PRIMARY KEY,
                inspection_id TEXT NOT NULL,
                unit TEXT NOT NULL,
                trade TEXT NOT NULL,
                component TEXT,
                room TEXT,
                urgency TEXT,
                status TEXT DEFAULT 'pending' CHECK (status IN (
                    'pending',
                    'in_progress',
                    'waiting_approval',
                    'approved',
                    'rejected',
                    'completed',
                    'cancelled'
                )),
                assigned_to INTEGER,
                planned_date DATE,
                started_date DATE,
                completed_date DATE,
                estimated_hours REAL,
                actual_hours REAL,
                notes TEXT,
                builder_notes TEXT,
                photos_required BOOLEAN DEFAULT FALSE,
                safety_requirements TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (inspection_id) REFERENCES inspector_inspections (id),
                FOREIGN KEY (assigned_to) REFERENCES users (id)
            )
        """)
        
        # Work order files/attachments (photos, documents)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_work_order_files (
                id TEXT PRIMARY KEY,
                work_order_id TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_type TEXT CHECK (file_type IN ('photo', 'document', 'before', 'after', 'other')),
                file_size INTEGER,
                mime_type TEXT,
                description TEXT,
                uploaded_by INTEGER,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (work_order_id) REFERENCES inspector_work_orders (id) ON DELETE CASCADE,
                FOREIGN KEY (uploaded_by) REFERENCES users (id)
            )
        """)
        
        # Indexes for performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_work_orders_status_trade 
            ON inspector_work_orders(status, trade)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_work_order_files_work_order 
            ON inspector_work_order_files(work_order_id)
        """)
        
        logger.info("✅ Inspector integration tables created with CORRECT status values")
        
        # Project progress tracking for Developer role
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_project_progress (
                id TEXT PRIMARY KEY,
                building_id TEXT NOT NULL,
                inspection_id TEXT,
                progress_date DATE DEFAULT CURRENT_DATE,
                total_defects INTEGER DEFAULT 0,
                resolved_defects INTEGER DEFAULT 0,
                pending_defects INTEGER DEFAULT 0,
                progress_pct REAL DEFAULT 0.0,
                quality_improvement REAL DEFAULT 0.0,
                estimated_completion_date DATE,
                milestone TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (building_id) REFERENCES inspector_buildings (id),
                FOREIGN KEY (inspection_id) REFERENCES inspector_inspections (id)
            )
        """)
        
        # Inspection metrics summary
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_metrics_summary (
                id TEXT PRIMARY KEY,
                inspection_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value TEXT,
                metric_type TEXT CHECK (metric_type IN ('scalar', 'dataframe', 'list')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (inspection_id) REFERENCES inspector_inspections (id)
            )
        """)
        
        # CSV processing log for tracking uploads
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_csv_processing_log (
                id TEXT PRIMARY KEY,
                original_filename TEXT NOT NULL,
                file_checksum TEXT,
                file_size INTEGER,
                inspector_name TEXT,
                inspector_id INTEGER,
                building_name TEXT,
                total_rows INTEGER DEFAULT 0,
                processed_rows INTEGER DEFAULT 0,
                defects_found INTEGER DEFAULT 0,
                mapping_success_rate REAL DEFAULT 0.0,
                processing_time_seconds REAL,
                status TEXT DEFAULT 'processing' CHECK (status IN ('processing', 'completed', 'failed')),
                error_message TEXT,
                inspection_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (inspector_id) REFERENCES users (id),
                FOREIGN KEY (inspection_id) REFERENCES inspector_inspections (id)
            )
        """)
        
        logger.info("✅ Inspector integration tables created")
    
    # ============================================================================
    # Database Migration Script - Run this ONCE to fix existing database
    # ============================================================================

    def migrate_work_orders_status_constraint():
        """Migration script to fix work orders status constraint"""
        import sqlite3
        
        conn = sqlite3.connect("building_inspection.db")
        cursor = conn.cursor()
        
        try:
            # Check if migration needed
            cursor.execute("""
                SELECT sql FROM sqlite_master 
                WHERE type='table' AND name='inspector_work_orders'
            """)
            result = cursor.fetchone()
            
            if result and 'waiting_approval' not in result[0]:
                print("⚠️  Migration needed - fixing work orders status constraint...")
                
                # SQLite doesn't support ALTER CONSTRAINT, so recreate table
                cursor.execute("BEGIN TRANSACTION")
                
                # 1. Rename old table
                cursor.execute("""
                    ALTER TABLE inspector_work_orders 
                    RENAME TO inspector_work_orders_old
                """)
                
                # 2. Create new table with correct constraint
                cursor.execute("""
                    CREATE TABLE inspector_work_orders (
                        id TEXT PRIMARY KEY,
                        inspection_id TEXT NOT NULL,
                        unit TEXT NOT NULL,
                        trade TEXT NOT NULL,
                        component TEXT,
                        room TEXT,
                        urgency TEXT,
                        status TEXT DEFAULT 'pending' CHECK (status IN (
                            'pending', 'in_progress', 'waiting_approval', 
                            'approved', 'rejected', 'completed', 'cancelled'
                        )),
                        assigned_to INTEGER,
                        planned_date DATE,
                        started_date DATE,
                        completed_date DATE,
                        estimated_hours REAL,
                        actual_hours REAL,
                        notes TEXT,
                        builder_notes TEXT,
                        photos_required BOOLEAN DEFAULT FALSE,
                        safety_requirements TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (inspection_id) REFERENCES inspector_inspections (id),
                        FOREIGN KEY (assigned_to) REFERENCES users (id)
                    )
                """)
                
                # 3. Copy data from old table
                cursor.execute("""
                    INSERT INTO inspector_work_orders 
                    SELECT * FROM inspector_work_orders_old
                """)
                
                # 4. Drop old table
                cursor.execute("DROP TABLE inspector_work_orders_old")
                
                # 5. Recreate indexes
                cursor.execute("""
                    CREATE INDEX idx_inspector_work_orders_trade 
                    ON inspector_work_orders(trade)
                """)
                cursor.execute("""
                    CREATE INDEX idx_inspector_work_orders_status 
                    ON inspector_work_orders(status)
                """)
                cursor.execute("""
                    CREATE INDEX idx_inspector_work_orders_assigned 
                    ON inspector_work_orders(assigned_to)
                """)
                
                conn.commit()
                print("✅ Migration completed successfully!")
                print("✅ Work orders table now supports all Builder workflow statuses")
                
            else:
                print("✅ Database already up to date - no migration needed")
                
        except Exception as e:
            conn.rollback()
            print(f"❌ Migration failed: {e}")
            raise
        finally:
            conn.close()
        
    # Keep all existing methods from your original setup.py
    def create_core_tables(self):
        """Create core system tables"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # System configuration table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_key TEXT UNIQUE NOT NULL,
                config_value TEXT,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # File storage tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_storage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_type TEXT NOT NULL,
                file_size INTEGER,
                mime_type TEXT,
                checksum TEXT,
                uploaded_by INTEGER,
                related_type TEXT,
                related_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (uploaded_by) REFERENCES users (id)
            )
        """)
        
        logger.info("✅ Core tables created")
    
    def create_user_tables(self):
        """Create user management tables"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Users table with comprehensive role system
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                role TEXT NOT NULL CHECK (role IN ('admin', 'inspector', 'developer', 'builder', 'owner')),
                is_active BOOLEAN DEFAULT TRUE,
                last_login TIMESTAMP,
                failed_login_attempts INTEGER DEFAULT 0,
                account_locked_until TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # User profiles with additional information
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                phone TEXT,
                company TEXT,
                department TEXT,
                job_title TEXT,
                address TEXT,
                city TEXT,
                state TEXT,
                postal_code TEXT,
                country TEXT DEFAULT 'Australia',
                timezone TEXT DEFAULT 'Australia/Sydney',
                language TEXT DEFAULT 'en',
                notification_preferences TEXT, -- JSON
                profile_picture_path TEXT,
                bio TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            )
        """)
        
        # User sessions for security tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                session_token TEXT UNIQUE NOT NULL,
                ip_address TEXT,
                user_agent TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            )
        """)
        
        # Role permissions (for future expansion)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS role_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                role TEXT NOT NULL,
                permission TEXT NOT NULL,
                resource TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(role, permission, resource)
            )
        """)
        
        logger.info("✅ User management tables created")
    
    def create_inspection_tables(self):
        """Create inspection management tables"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Main inspections table (keep existing structure)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_name TEXT NOT NULL,
                project_code TEXT,
                inspection_date DATE NOT NULL,
                inspection_type TEXT NOT NULL CHECK (inspection_type IN (
                    'initial', 're_inspection', 'final', 'dlp', 'handover', 'maintenance'
                )),
                building_phase TEXT CHECK (building_phase IN (
                    'pre_construction', 'foundation', 'structure', 'fit_out', 'completion', 'dlp'
                )),
                inspector_id INTEGER NOT NULL,
                assigned_to INTEGER,
                client_name TEXT,
                client_contact TEXT,
                project_address TEXT,
                project_city TEXT,
                project_state TEXT,
                project_postal_code TEXT,
                status TEXT DEFAULT 'pending' CHECK (status IN (
                    'pending', 'in_progress', 'completed', 'reviewed', 'approved', 'rejected'
                )),
                priority TEXT DEFAULT 'medium' CHECK (priority IN ('low', 'medium', 'high', 'critical')),
                expected_completion DATE,
                actual_completion DATE,
                total_defects INTEGER DEFAULT 0,
                critical_defects INTEGER DEFAULT 0,
                major_defects INTEGER DEFAULT 0,
                minor_defects INTEGER DEFAULT 0,
                quality_score REAL DEFAULT 0.0,
                weather_conditions TEXT,
                temperature REAL,
                humidity REAL,
                notes TEXT,
                internal_notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (inspector_id) REFERENCES users (id),
                FOREIGN KEY (assigned_to) REFERENCES users (id)
            )
        """)
        
        # Inspection participants (for team inspections)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspection_participants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inspection_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                role TEXT NOT NULL CHECK (role IN ('lead', 'assistant', 'observer', 'client_rep')),
                attended BOOLEAN DEFAULT FALSE,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (inspection_id) REFERENCES inspections (id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users (id),
                UNIQUE(inspection_id, user_id)
            )
        """)
        
        # Inspection areas/zones
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspection_areas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inspection_id INTEGER NOT NULL,
                area_name TEXT NOT NULL,
                area_type TEXT CHECK (area_type IN (
                    'apartment', 'common_area', 'external', 'basement', 'rooftop', 'balcony'
                )),
                floor_level TEXT,
                area_code TEXT,
                description TEXT,
                completion_percentage REAL DEFAULT 0.0,
                defect_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'in_progress', 'completed')),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (inspection_id) REFERENCES inspections (id) ON DELETE CASCADE
            )
        """)
        
        logger.info("✅ Inspection tables created")
    
    def create_defect_tables(self):
        """Create defect management tables"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Main defects table (keep existing structure)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS defects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inspection_id INTEGER NOT NULL,
                area_id INTEGER,
                defect_id TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                location TEXT,
                unit_number TEXT,
                room TEXT,
                trade TEXT,
                trade_category TEXT,
                severity TEXT NOT NULL CHECK (severity IN ('critical', 'major', 'minor', 'observation')),
                priority INTEGER DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
                status TEXT DEFAULT 'open' CHECK (status IN (
                    'open', 'in_progress', 'pending_review', 'resolved', 'closed', 'rejected'
                )),
                estimated_cost REAL,
                actual_cost REAL,
                assigned_to INTEGER,
                reported_by INTEGER NOT NULL,
                verified_by INTEGER,
                approved_by INTEGER,
                rectification_date DATE,
                completion_date DATE,
                due_date DATE,
                photo_count INTEGER DEFAULT 0,
                has_photos BOOLEAN DEFAULT FALSE,
                gps_latitude REAL,
                gps_longitude REAL,
                floor_plan_x REAL,
                floor_plan_y REAL,
                weather_related BOOLEAN DEFAULT FALSE,
                safety_issue BOOLEAN DEFAULT FALSE,
                warranty_issue BOOLEAN DEFAULT FALSE,
                compliance_issue BOOLEAN DEFAULT FALSE,
                notes TEXT,
                inspector_notes TEXT,
                builder_response TEXT,
                client_comments TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (inspection_id) REFERENCES inspections (id) ON DELETE CASCADE,
                FOREIGN KEY (area_id) REFERENCES inspection_areas (id),
                FOREIGN KEY (assigned_to) REFERENCES users (id),
                FOREIGN KEY (reported_by) REFERENCES users (id),
                FOREIGN KEY (verified_by) REFERENCES users (id),
                FOREIGN KEY (approved_by) REFERENCES users (id)
            )
        """)
        
        # Defect photos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS defect_photos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                defect_id INTEGER NOT NULL,
                file_storage_id INTEGER NOT NULL,
                photo_type TEXT CHECK (photo_type IN ('before', 'during', 'after', 'closeup', 'overview')),
                caption TEXT,
                is_primary BOOLEAN DEFAULT FALSE,
                sequence_order INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (defect_id) REFERENCES defects (id) ON DELETE CASCADE,
                FOREIGN KEY (file_storage_id) REFERENCES file_storage (id)
            )
        """)
        
        # Defect history/status changes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS defect_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                defect_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                field_name TEXT,
                comments TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (defect_id) REFERENCES defects (id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        """)
        
        # Trade categories master data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                code TEXT UNIQUE,
                description TEXT,
                parent_category_id INTEGER,
                is_active BOOLEAN DEFAULT TRUE,
                sort_order INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parent_category_id) REFERENCES trade_categories (id)
            )
        """)
        
        logger.info("✅ Defect management tables created")
    
    def create_workflow_tables(self):
        """Create workflow and processing tables"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Data processing queue
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processing_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_type TEXT NOT NULL,
                inspector_id INTEGER NOT NULL,
                inspection_id INTEGER,
                status TEXT DEFAULT 'queued' CHECK (status IN (
                    'queued', 'processing', 'completed', 'failed', 'cancelled'
                )),
                progress INTEGER DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
                total_records INTEGER DEFAULT 0,
                processed_records INTEGER DEFAULT 0,
                error_count INTEGER DEFAULT 0,
                error_message TEXT,
                processing_options TEXT,
                result_summary TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (inspector_id) REFERENCES users (id),
                FOREIGN KEY (inspection_id) REFERENCES inspections (id)
            )
        """)
        
        # Approval workflows
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS approval_workflows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL CHECK (entity_type IN ('inspection', 'defect', 'report')),
                entity_id INTEGER NOT NULL,
                workflow_type TEXT NOT NULL,
                current_step INTEGER DEFAULT 1,
                total_steps INTEGER NOT NULL,
                status TEXT DEFAULT 'pending' CHECK (status IN (
                    'pending', 'in_review', 'approved', 'rejected', 'cancelled'
                )),
                requested_by INTEGER NOT NULL,
                assigned_to INTEGER,
                priority TEXT DEFAULT 'medium',
                due_date DATE,
                comments TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (requested_by) REFERENCES users (id),
                FOREIGN KEY (assigned_to) REFERENCES users (id)
            )
        """)
        
        # Approval steps
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS approval_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id INTEGER NOT NULL,
                step_number INTEGER NOT NULL,
                step_name TEXT NOT NULL,
                assigned_to INTEGER NOT NULL,
                status TEXT DEFAULT 'pending' CHECK (status IN (
                    'pending', 'in_review', 'approved', 'rejected', 'skipped'
                )),
                decision TEXT,
                comments TEXT,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (workflow_id) REFERENCES approval_workflows (id) ON DELETE CASCADE,
                FOREIGN KEY (assigned_to) REFERENCES users (id)
            )
        """)
        
        # Notifications
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                type TEXT NOT NULL CHECK (type IN (
                    'info', 'warning', 'error', 'success', 'reminder'
                )),
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                entity_type TEXT,
                entity_id INTEGER,
                is_read BOOLEAN DEFAULT FALSE,
                is_dismissed BOOLEAN DEFAULT FALSE,
                action_url TEXT,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
            )
        """)
        
        logger.info("✅ Workflow tables created")
    
    def create_report_tables(self):
        """Create reporting and analytics tables"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Generated reports tracking
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS generated_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_type TEXT NOT NULL CHECK (report_type IN ('excel', 'word', 'pdf', 'csv')),
                report_name TEXT NOT NULL,
                inspection_id INTEGER,
                generated_by INTEGER NOT NULL,
                file_storage_id INTEGER,
                report_config TEXT,
                status TEXT DEFAULT 'generating' CHECK (status IN (
                    'generating', 'completed', 'failed', 'expired'
                )),
                file_size INTEGER,
                download_count INTEGER DEFAULT 0,
                expires_at TIMESTAMP,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (inspection_id) REFERENCES inspections (id),
                FOREIGN KEY (generated_by) REFERENCES users (id),
                FOREIGN KEY (file_storage_id) REFERENCES file_storage (id)
            )
        """)
        
        # Report templates
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS report_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                description TEXT,
                template_data TEXT,
                is_default BOOLEAN DEFAULT FALSE,
                is_active BOOLEAN DEFAULT TRUE,
                created_by INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users (id)
            )
        """)
        
        # Analytics data
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS analytics_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_name TEXT NOT NULL,
                metric_value REAL NOT NULL,
                metric_type TEXT NOT NULL,
                entity_type TEXT,
                entity_id INTEGER,
                user_id INTEGER,
                metadata TEXT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        """)
        
        logger.info("✅ Report tables created")
    
    def create_audit_tables(self):
        """Create audit and logging tables"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # System audit log
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT NOT NULL,
                entity_type TEXT,
                entity_id INTEGER,
                old_values TEXT,
                new_values TEXT,
                ip_address TEXT,
                user_agent TEXT,
                session_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        """)
        
        # System logs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT NOT NULL CHECK (level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')),
                module TEXT NOT NULL,
                message TEXT NOT NULL,
                stack_trace TEXT,
                user_id INTEGER,
                request_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        """)
        
        logger.info("✅ Audit tables created")
    
    def create_indexes(self):
        """Create database indexes for performance optimization"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # User indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active)")
        
        # Inspection indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspections_date ON inspections(inspection_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspections_inspector ON inspections(inspector_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspections_status ON inspections(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspections_project ON inspections(project_name)")
        
        # Defect indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_defects_inspection ON defects(inspection_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_defects_status ON defects(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_defects_severity ON defects(severity)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_defects_trade ON defects(trade)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_defects_assigned ON defects(assigned_to)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_defects_created ON defects(created_at)")
        
        # NEW: Inspector integration indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspector_buildings_name ON inspector_buildings(name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspector_inspections_building ON inspector_inspections(building_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspector_inspections_date ON inspector_inspections(inspection_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspector_items_inspection ON inspector_inspection_items(inspection_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspector_items_unit ON inspector_inspection_items(unit)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspector_items_trade ON inspector_inspection_items(trade)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspector_work_orders_trade ON inspector_work_orders(trade)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspector_work_orders_status ON inspector_work_orders(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspector_work_orders_assigned ON inspector_work_orders(assigned_to)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspector_progress_building ON inspector_project_progress(building_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_inspector_progress_date ON inspector_project_progress(progress_date)")
        
        # File storage indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_file_storage_type ON file_storage(file_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_file_storage_related ON file_storage(related_type, related_id)")
        
        # Processing queue indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_processing_queue_status ON processing_queue(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_processing_queue_inspector ON processing_queue(inspector_id)")
        
        # Notification indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_notifications_read ON notifications(is_read)")
        
        # Audit indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at)")
        
        logger.info("✅ Database indexes created")
    
    # NEW: Inspector Integration Methods
    def save_inspector_data(self, processed_data: pd.DataFrame, metrics: dict, 
                   inspector_name: str, original_filename: str = None) -> Optional[str]:
        """
        Save inspection data with comprehensive error logging and validation
        
        Returns:
            inspection_id if successful, None if failed
        """
        
        logger.info("=" * 80)
        logger.info("STARTING SAVE_INSPECTOR_DATA")
        logger.info("=" * 80)
        logger.info(f"Data rows: {len(processed_data)}")
        logger.info(f"Inspector: {inspector_name}")
        logger.info(f"Filename: {original_filename}")
        
        # Validate inputs
        if processed_data is None or len(processed_data) == 0:
            logger.error("❌ SAVE FAILED: processed_data is empty")
            return None
        
        if not metrics:
            logger.error("❌ SAVE FAILED: metrics dictionary is empty")
            return None
        
        # Check required columns
        required_cols = ['Unit', 'UnitType', 'Room', 'Component', 'Trade', 
                        'StatusClass', 'Urgency', 'PlannedCompletion']
        missing_cols = [col for col in required_cols if col not in processed_data.columns]
        
        if missing_cols:
            logger.error(f"❌ SAVE FAILED: Missing required columns: {missing_cols}")
            return None
        
        logger.info("✅ Input validation passed")
        
        # Get database connection
        try:
            conn = self.connect()
            cursor = conn.cursor()
            logger.info("✅ Database connection established")
        except Exception as e:
            logger.error(f"❌ SAVE FAILED: Cannot connect to database: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
        
        # Generate IDs
        building_id = str(uuid.uuid4())
        inspection_id = str(uuid.uuid4())
        logger.info(f"✅ Generated IDs - Building: {building_id[:8]}..., Inspection: {inspection_id[:8]}...")
        
        try:
            # Start transaction
            cursor.execute("BEGIN IMMEDIATE TRANSACTION")
            logger.info("✅ Transaction started")
            
            # STEP 1: Insert building
            try:
                inspection_date = metrics.get('inspection_date', datetime.now().strftime('%Y-%m-%d'))
                
                logger.info("Inserting building record...")
                cursor.execute("""
                    INSERT INTO inspector_buildings (
                        id, name, address, inspection_date, total_units, total_defects, 
                        defect_rate, ready_units, ready_pct, quality_score, unit_types, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    building_id,
                    str(metrics.get('building_name', 'Unknown')),
                    str(metrics.get('address', '')),
                    inspection_date,
                    int(metrics.get('total_units', 0)),
                    int(metrics.get('total_defects', 0)),
                    float(metrics.get('defect_rate', 0.0)),
                    int(metrics.get('ready_units', 0)),
                    float(metrics.get('ready_pct', 0.0)),
                    float(max(0, 100 - metrics.get('defect_rate', 0))),
                    str(metrics.get('unit_types_str', '')),
                    'active'
                ))
                logger.info(f"✅ Building record inserted: {metrics.get('building_name')}")
                
            except Exception as e:
                logger.error(f"❌ Building insert failed: {e}")
                raise
            
            # STEP 2: Insert inspection
            try:
                logger.info("Inserting inspection record...")
                cursor.execute("""
                    INSERT INTO inspector_inspections (
                        id, building_id, inspection_date, inspector_name,
                        total_units, total_defects, defect_rate, ready_units, ready_pct,
                        urgent_defects, high_priority_defects, avg_defects_per_unit,
                        original_filename, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    inspection_id, 
                    building_id,
                    inspection_date,
                    str(inspector_name),
                    int(metrics.get('total_units', 0)),
                    int(metrics.get('total_defects', 0)),
                    float(metrics.get('defect_rate', 0.0)),
                    int(metrics.get('ready_units', 0)),
                    float(metrics.get('ready_pct', 0.0)),
                    int(metrics.get('urgent_defects', 0)),
                    int(metrics.get('high_priority_defects', 0)),
                    float(metrics.get('avg_defects_per_unit', 0.0)),
                    str(original_filename or 'uploaded.csv'),
                    'active'
                ))
                logger.info(f"✅ Inspection record inserted")
                
            except Exception as e:
                logger.error(f"❌ Inspection insert failed: {e}")
                raise
            
            # STEP 3: Insert inspection items (REMOVED inspector_unit_inspections step)
            try:
                logger.info("Inserting inspection items...")
                items_batch = []
                
                for _, row in processed_data.iterrows():
                    # Handle inspection date
                    inspection_date_val = row.get('InspectionDate', inspection_date)
                    
                    # Handle signoff timestamp
                    signoff_str = None
                    if 'OwnerSignoffTimestamp' in row and pd.notna(row['OwnerSignoffTimestamp']):
                        try:
                            dt = pd.to_datetime(row['OwnerSignoffTimestamp'], errors='coerce')
                            signoff_str = dt.isoformat() if pd.notna(dt) else None
                        except:
                            pass
                    
                    items_batch.append((
                        str(uuid.uuid4()),
                        inspection_id,
                        str(row.get('Unit', '')),
                        str(row.get('UnitType', '')),
                        str(inspection_date_val),
                        str(row.get('Room', '')),
                        str(row.get('Component', '')),
                        str(row.get('Trade', '')),
                        str(row.get('StatusClass', '')),
                        str(row.get('Urgency', '')),
                        str(row.get('PlannedCompletion', '')),
                        signoff_str,
                        str(row.get('Status', '')),
                        datetime.now()
                    ))
                
                cursor.executemany("""
                    INSERT INTO inspector_inspection_items (
                        id, inspection_id, unit, unit_type, inspection_date,
                        room, component, trade, status_class, urgency,
                        planned_completion, owner_signoff_timestamp, original_status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, items_batch)
                
                logger.info(f"✅ Inserted {len(items_batch)} inspection items")
                
            except Exception as e:
                logger.error(f"❌ Inspection items insert failed: {e}")
                raise
            
            # Commit transaction
            conn.commit()
            logger.info("✅ Transaction committed successfully")
            
            logger.info("=" * 80)
            logger.info(f"✅ SAVE COMPLETE - Inspection ID: {inspection_id}")
            logger.info("=" * 80)
            
            return inspection_id
            
        except Exception as e:
            conn.rollback()
            logger.error("=" * 80)
            logger.error("❌ SAVE FAILED - Transaction rolled back")
            logger.error(f"Error: {e}")
            logger.error("=" * 80)
            
            import traceback
            logger.error("Full traceback:")
            logger.error(traceback.format_exc())
            
            return None
        
    def check_save_readiness(self) -> Dict[str, Any]:
        """
        Check if database is ready for Inspector data save
        
        Returns:
            Dict with readiness status and any issues found
        """
        readiness = {
            'ready': True,
            'issues': [],
            'warnings': []
        }
        
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Check critical tables exist
            required_tables = [
                'inspector_buildings',
                'inspector_inspections', 
                'inspector_inspection_items',
                'inspector_work_orders',
                'inspector_project_progress'
            ]
            
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN ({})
            """.format(','.join(['?' for _ in required_tables])), required_tables)
            
            existing_tables = [row[0] for row in cursor.fetchall()]
            missing_tables = set(required_tables) - set(existing_tables)
            
            if missing_tables:
                readiness['ready'] = False
                readiness['issues'].append(f"Missing tables: {', '.join(missing_tables)}")
            
            # Check write permissions
            try:
                test_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO inspector_buildings 
                    (id, name, status, created_at, updated_at)
                    VALUES (?, 'readiness_test', 'test', ?, ?)
                """, (test_id, datetime.now(), datetime.now()))
                
                cursor.execute("DELETE FROM inspector_buildings WHERE id = ?", (test_id,))
                conn.commit()
            except Exception as write_error:
                readiness['ready'] = False
                readiness['issues'].append(f"Write test failed: {write_error}")
            
            # Check foreign key constraints
            cursor.execute("PRAGMA foreign_keys")
            fk_enabled = cursor.fetchone()[0]
            if not fk_enabled:
                readiness['warnings'].append("Foreign keys not enabled")
            
        except Exception as e:
            readiness['ready'] = False
            readiness['issues'].append(f"Connection error: {e}")
        
        return readiness

    def get_inspector_inspections(self, limit: int = 10) -> pd.DataFrame:
        """Get recent real inspector inspections (excluding sample data)"""
        conn = self.connect()
        query = """
            SELECT 
                i.id, i.inspection_date, b.name as building_name,
                i.total_units, i.total_defects, i.ready_pct, i.inspector_name,
                i.original_filename, i.created_at
            FROM inspector_inspections i
            JOIN inspector_buildings b ON i.building_id = b.id
            WHERE i.original_filename IS NOT NULL  -- Only real CSV uploads
            ORDER BY i.created_at DESC
            LIMIT ?
        """
        return pd.read_sql_query(query, conn, params=[limit])
    
    def get_work_orders_for_builder(self, trade: str = None, status: str = None) -> pd.DataFrame:
        """Get work orders for Builder role"""
        conn = self.connect()
        query = """
            SELECT wo.*, i.inspection_date, b.name as building_name
            FROM inspector_work_orders wo
            JOIN inspector_inspections i ON wo.inspection_id = i.id
            JOIN inspector_buildings b ON i.building_id = b.id
            WHERE 1=1
        """
        params = []
        
        if trade:
            query += " AND wo.trade = ?"
            params.append(trade)
        
        if status:
            query += " AND wo.status = ?"
            params.append(status)
        
        query += " ORDER BY wo.urgency, wo.planned_date"
        return pd.read_sql_query(query, conn, params=params)
    
    def get_project_overview_for_developer(self) -> pd.DataFrame:
        """Get project overview for Developer role"""
        conn = self.connect()
        query = """
            SELECT 
                b.name as building_name, b.address,
                COUNT(DISTINCT i.id) as total_inspections,
                MAX(i.inspection_date) as latest_inspection,
                AVG(i.ready_pct) as avg_ready_pct,
                SUM(i.total_defects) as total_defects,
                SUM(CASE WHEN wo.status = 'completed' THEN 1 ELSE 0 END) as resolved_defects
            FROM inspector_buildings b
            LEFT JOIN inspector_inspections i ON b.id = i.building_id
            LEFT JOIN inspector_work_orders wo ON i.id = wo.inspection_id
            GROUP BY b.id, b.name, b.address
            ORDER BY latest_inspection DESC
        """
        return pd.read_sql_query(query, conn)
    
    def save_trade_mapping(self, mapping_df: pd.DataFrame) -> bool:
        """Save trade mapping to database"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Clear existing mappings
            cursor.execute("DELETE FROM inspector_trade_mappings")
            
            # Insert new mappings
            for _, row in mapping_df.iterrows():
                mapping_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO inspector_trade_mappings (id, room, component, trade)
                    VALUES (?, ?, ?, ?)
                """, (mapping_id, row['Room'], row['Component'], row['Trade']))
            
            conn.commit()
            logger.info(f"Saved {len(mapping_df)} trade mappings")
            return True
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error saving trade mapping: {e}")
            return False
    
    def get_trade_mapping(self) -> pd.DataFrame:
        """Get current trade mapping from database"""
        conn = self.connect()
        query = "SELECT room as Room, component as Component, trade as Trade FROM inspector_trade_mappings"
        return pd.read_sql_query(query, conn)
    
    # Keep all existing methods from your original setup.py
    def seed_initial_data(self):
        """Seed database with initial configuration and master data"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # System configuration
        config_data = [
            ('app_name', 'Building Inspection System V3', 'Application name'),
            ('app_version', '3.0.0', 'Application version with Inspector integration'),
            ('max_file_size', '50MB', 'Maximum file upload size'),
            ('session_timeout', '3600', 'Session timeout in seconds'),
            ('password_min_length', '8', 'Minimum password length'),
            ('max_login_attempts', '5', 'Maximum failed login attempts'),
            ('account_lockout_duration', '1800', 'Account lockout duration in seconds'),
            ('report_retention_days', '365', 'Report retention period in days'),
            ('photo_quality', 'high', 'Default photo quality setting'),
            ('default_timezone', 'Australia/Sydney', 'Default system timezone'),
            ('inspector_csv_max_size', '100MB', 'Maximum Inspector CSV file size'),
            ('auto_create_work_orders', 'true', 'Automatically create work orders from defects'),
        ]
        
        cursor.executemany("""
            INSERT OR IGNORE INTO system_config (config_key, config_value, description)
            VALUES (?, ?, ?)
        """, config_data)
        
        # Trade categories with Inspector integration
        trade_categories = [
            ('Carpentry & Joinery', 'CARP', 'Structural and finish carpentry, joinery work'),
            ('Plumbing', 'PLUM', 'Water, drainage, and gas plumbing systems'),
            ('Electrical', 'ELEC', 'Electrical installations and systems'),
            ('Painting', 'PAIN', 'Interior and exterior painting and finishes'),
            ('Flooring - Tiles', 'TILE', 'Floor and wall tiling work'),
            ('Flooring - Carpets', 'CARP_FLOOR', 'Carpet installation and repairs'),
            ('Flooring - External', 'EXT_FLOOR', 'External flooring and surfaces'),
            ('Roofing', 'ROOF', 'Roof and gutter systems'),
            ('Windows', 'WIND', 'Window installations and glazing'),
            ('Doors', 'DOOR', 'Door installations and hardware'),
            ('Concrete', 'CONC', 'Concrete and structural work'),
            ('Landscaping', 'LAND', 'External landscaping and gardens'),
            ('HVAC', 'HVAC', 'Heating, ventilation, and air conditioning'),
            ('Waterproofing', 'WATER', 'Waterproofing and sealing systems'),
            ('Insulation', 'INSUL', 'Thermal and acoustic insulation'),
            ('Stone Work', 'STONE', 'Natural and engineered stone work'),
            ('Appliances', 'APPL', 'Kitchen and laundry appliances'),
            ('Fire Safety', 'FIRE', 'Fire safety systems and equipment'),
            ('Garage Doors', 'GARAGE', 'Garage door systems'),
            ('Accessories', 'ACCESS', 'Fixtures and accessories'),
            ('Glazing', 'GLAZ', 'Glass installations and glazing'),
            ('Unknown Trade', 'UNK', 'Unmapped or unknown trade category'),
        ]
        
        cursor.executemany("""
            INSERT OR IGNORE INTO trade_categories (name, code, description)
            VALUES (?, ?, ?)
        """, trade_categories)
        
        # Enhanced role permissions for Inspector integration
        permissions_data = [
            # Admin permissions
            ('admin', 'create', 'user'),
            ('admin', 'read', 'user'),
            ('admin', 'update', 'user'),
            ('admin', 'delete', 'user'),
            ('admin', 'manage', 'system'),
            ('admin', 'view', 'analytics'),
            ('admin', 'manage', 'inspector_data'),
            
            # Inspector permissions (enhanced)
            ('inspector', 'create', 'inspection'),
            ('inspector', 'read', 'inspection'),
            ('inspector', 'update', 'inspection'),
            ('inspector', 'create', 'defect'),
            ('inspector', 'update', 'defect'),
            ('inspector', 'generate', 'report'),
            ('inspector', 'upload', 'csv_data'),
            ('inspector', 'process', 'csv_data'),
            ('inspector', 'manage', 'trade_mapping'),
            ('inspector', 'create', 'work_order'),
            
            # Developer permissions (enhanced)
            ('developer', 'read', 'inspection'),
            ('developer', 'view', 'analytics'),
            ('developer', 'generate', 'report'),
            ('developer', 'read', 'defect'),
            ('developer', 'view', 'project_progress'),
            ('developer', 'read', 'inspector_data'),
            ('developer', 'view', 'trends'),
            
            # Builder permissions (enhanced)
            ('builder', 'read', 'defect'),
            ('builder', 'update', 'defect'),
            ('builder', 'comment', 'defect'),
            ('builder', 'upload', 'photo'),
            ('builder', 'read', 'work_order'),
            ('builder', 'update', 'work_order'),
            ('builder', 'complete', 'work_order'),
            ('builder', 'read', 'inspector_data'),
            
            # Owner permissions
            ('owner', 'read', 'inspection'),
            ('owner', 'read', 'defect'),
            ('owner', 'approve', 'defect'),
            ('owner', 'comment', 'defect'),
            ('owner', 'generate', 'report'),
            ('owner', 'view', 'progress'),
        ]
        
        cursor.executemany("""
            INSERT OR IGNORE INTO role_permissions (role, permission, resource)
            VALUES (?, ?, ?)
        """, permissions_data)
        
        # Create default admin user
        self.create_default_admin()
        
        # Seed default trade mapping
        self.seed_default_trade_mapping()
        
        conn.commit()
        logger.info("✅ Initial data seeded with Inspector integration")
    
    def seed_default_trade_mapping(self):
        """Seed default trade mapping for Inspector processing"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Check if trade mappings exist
        cursor.execute("SELECT COUNT(*) FROM inspector_trade_mappings")
        mapping_count = cursor.fetchone()[0]
        
        if mapping_count == 0:
            # Default comprehensive trade mapping
            default_mappings = [
                ("Apartment Entry Door", "Door Handle", "Doors"),
                ("Apartment Entry Door", "Door Locks and Keys", "Doors"),
                ("Apartment Entry Door", "Paint", "Painting"),
                ("Apartment Entry Door", "Door Frame", "Carpentry & Joinery"),
                ("Balcony", "Balustrade", "Carpentry & Joinery"),
                ("Balcony", "Drainage Point", "Plumbing"),
                ("Balcony", "Flooring", "Flooring - External"),
                ("Balcony", "Waterproofing", "Waterproofing"),
                ("Bathroom", "Bathtub (if applicable)", "Plumbing"),
                ("Bathroom", "Ceiling", "Painting"),
                ("Bathroom", "Exhaust Fan", "Electrical"),
                ("Bathroom", "Tiles", "Flooring - Tiles"),
                ("Bathroom", "Toilet", "Plumbing"),
                ("Bathroom", "Shower", "Plumbing"),
                ("Bathroom", "Vanity", "Carpentry & Joinery"),
                ("Bathroom", "Tapware", "Plumbing"),
                ("Bathroom", "Mirror", "Glazing"),
                ("Bathroom", "Towel Rails", "Accessories"),
                ("Bathroom", "Light Fixtures", "Electrical"),
                ("Bathroom", "Power Outlets", "Electrical"),
                ("Bathroom", "Walls", "Painting"),
                ("Bathroom", "Flooring", "Flooring - Tiles"),
                ("Kitchen Area", "Cabinets", "Carpentry & Joinery"),
                ("Kitchen Area", "Kitchen Sink", "Plumbing"),
                ("Kitchen Area", "Stovetop and Oven", "Appliances"),
                ("Kitchen Area", "Rangehood", "Appliances"),
                ("Kitchen Area", "Benchtop", "Stone Work"),
                ("Kitchen Area", "Splashback", "Flooring - Tiles"),
                ("Kitchen Area", "Dishwasher", "Appliances"),
                ("Kitchen Area", "Light Fixtures", "Electrical"),
                ("Kitchen Area", "Power Outlets", "Electrical"),
                ("Kitchen Area", "Walls", "Painting"),
                ("Kitchen Area", "Ceiling", "Painting"),
                ("Kitchen Area", "Windows", "Windows"),
                ("Bedroom", "Carpets", "Flooring - Carpets"),
                ("Bedroom", "Windows", "Windows"),
                ("Bedroom", "Light Fixtures", "Electrical"),
                ("Bedroom", "Power Outlets", "Electrical"),
                ("Bedroom", "Built-in Robes", "Carpentry & Joinery"),
                ("Bedroom", "Ceiling", "Painting"),
                ("Bedroom", "Walls", "Painting"),
                ("Bedroom", "Flooring", "Flooring"),
                ("Living Room", "Flooring", "Flooring"),
                ("Living Room", "Windows", "Windows"),
                ("Living Room", "Ceiling", "Painting"),
                ("Living Room", "Walls", "Painting"),
                ("Living Room", "Light Fixtures", "Electrical"),
                ("Living Room", "Power Outlets", "Electrical"),
                ("Living Room", "Air Conditioning", "HVAC"),
                ("Laundry", "Washing Machine Taps", "Plumbing"),
                ("Laundry", "Laundry Sink", "Plumbing"),
                ("Laundry", "Cabinets", "Carpentry & Joinery"),
                ("Laundry", "Flooring", "Flooring"),
                ("Laundry", "Exhaust Fan", "Electrical"),
                ("Laundry", "Light Fixtures", "Electrical"),
                ("Laundry", "Walls", "Painting"),
                ("Laundry", "Ceiling", "Painting"),
                ("Entry", "Door", "Doors"),
                ("Entry", "Intercom", "Electrical"),
                ("Entry", "Flooring", "Flooring"),
                ("Entry", "Light Fixtures", "Electrical"),
                ("Hallway", "Light Switches", "Electrical"),
                ("Hallway", "Smoke Detector", "Fire Safety"),
                ("Hallway", "Flooring", "Flooring"),
                ("Hallway", "Walls", "Painting"),
                ("Hallway", "Ceiling", "Painting"),
                ("External", "Hot Water System", "Plumbing"),
                ("External", "Meter Box", "Electrical"),
                ("Car Space", "Remote Control", "Garage Doors"),
                ("Car Space", "Lighting", "Electrical"),
                ("Garage", "Door", "Garage Doors"),
                ("Garage", "Lighting", "Electrical"),
                ("Storage", "Shelving", "Carpentry & Joinery"),
                ("General", "Smoke Detector", "Fire Safety"),
                ("General", "Air Conditioning", "HVAC"),
            ]
            
            for room, component, trade in default_mappings:
                mapping_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO inspector_trade_mappings (id, room, component, trade)
                    VALUES (?, ?, ?, ?)
                """, (mapping_id, room, component, trade))
            
            logger.info(f"✅ Seeded {len(default_mappings)} default trade mappings")
    
    def create_default_admin(self):
        """Create default admin user"""
        conn = self.connect()
        cursor = conn.cursor()
        
        # Check if admin exists
        cursor.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'")
        admin_count = cursor.fetchone()[0]
        
        if admin_count == 0:
            import secrets
            import hashlib
            
            # Generate secure password
            password = "admin123"  # Change in production
            salt = secrets.token_hex(32)
            password_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
            
            cursor.execute("""
                INSERT INTO users 
                (username, email, password_hash, salt, first_name, last_name, role)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                'admin', 'admin@buildinginspection.com', 
                password_hash.hex(), salt,
                'System', 'Administrator', 'admin'
            ))
            
            user_id = cursor.lastrowid
            
            # Create profile
            cursor.execute("""
                INSERT INTO user_profiles (user_id, company, job_title)
                VALUES (?, ?, ?)
            """, (user_id, 'Building Inspection System V3', 'System Administrator'))
            
            logger.info("✅ Default admin user created (admin/admin123)")
    
    def setup_migration_tracking(self):
        """Setup migration tracking system"""
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS migrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                migration_name TEXT UNIQUE NOT NULL,
                version TEXT NOT NULL,
                description TEXT,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rollback_sql TEXT
            )
        """)
        
        # Record initial schema as migration
        cursor.execute("""
            INSERT OR IGNORE INTO migrations (migration_name, version, description)
            VALUES (?, ?, ?)
        """, ('initial_schema_v3', '3.0.0', 'Initial database schema with Inspector integration'))
        
        conn.commit()
        logger.info("✅ Migration tracking setup")
    
    def backup_database(self, backup_path: Optional[str] = None) -> str:
        """Create database backup"""
        if not backup_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"backup_building_inspection_v3_{timestamp}.db"
        
        conn = self.connect()
        with open(backup_path, 'wb') as backup_file:
            for line in conn.iterdump():
                backup_file.write(f"{line}\n".encode())
        
        logger.info(f"✅ Database backup created: {backup_path}")
        return backup_path
    
    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics"""
        conn = self.connect()
        cursor = conn.cursor()
        
        stats = {}
        
        # Table counts
        tables = [
            'users', 'inspections', 'defects', 'processing_queue',
            'file_storage', 'notifications', 'audit_log',
            'inspector_buildings', 'inspector_inspections', 'inspector_inspection_items',
            'inspector_work_orders', 'inspector_project_progress', 'inspector_trade_mappings'
        ]
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                stats[f'{table}_count'] = cursor.fetchone()[0]
            except:
                stats[f'{table}_count'] = 0
        
        # Database size
        cursor.execute("PRAGMA page_count")
        page_count = cursor.fetchone()[0]
        cursor.execute("PRAGMA page_size")
        page_size = cursor.fetchone()[0]
        stats['database_size_mb'] = (page_count * page_size) / (1024 * 1024)
        
        # Recent activity
        cursor.execute("""
            SELECT COUNT(*) FROM inspector_inspections 
            WHERE created_at >= date('now', '-7 days')
        """)
        stats['inspector_inspections_last_week'] = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM inspector_work_orders 
            WHERE created_at >= date('now', '-7 days')
        """)
        stats['work_orders_last_week'] = cursor.fetchone()[0]
        
        return stats
    
    def validate_database_integrity(self) -> Dict:
        """Validate database integrity and relationships"""
        conn = self.connect()
        cursor = conn.cursor()
        
        issues = []
        
        # Check foreign key constraints
        cursor.execute("PRAGMA foreign_key_check")
        fk_violations = cursor.fetchall()
        if fk_violations:
            issues.extend([f"Foreign key violation: {violation}" for violation in fk_violations])
        
        # Check for orphaned inspector data
        cursor.execute("""
            SELECT COUNT(*) FROM inspector_inspection_items ii
            LEFT JOIN inspector_inspections i ON ii.inspection_id = i.id
            WHERE i.id IS NULL
        """)
        orphaned_items = cursor.fetchone()[0]
        if orphaned_items > 0:
            issues.append(f"Found {orphaned_items} orphaned inspector inspection items")
        
        # Check for orphaned work orders
        cursor.execute("""
            SELECT COUNT(*) FROM inspector_work_orders wo
            LEFT JOIN inspector_inspections i ON wo.inspection_id = i.id
            WHERE i.id IS NULL
        """)
        orphaned_work_orders = cursor.fetchone()[0]
        if orphaned_work_orders > 0:
            issues.append(f"Found {orphaned_work_orders} orphaned work orders")
        
        # Check for missing user profiles
        cursor.execute("""
            SELECT COUNT(*) FROM users u
            LEFT JOIN user_profiles p ON u.id = p.user_id
            WHERE p.user_id IS NULL
        """)
        missing_profiles = cursor.fetchone()[0]
        if missing_profiles > 0:
            issues.append(f"Found {missing_profiles} users without profiles")
        
        return {
            'is_valid': len(issues) == 0,
            'issues': issues,
            'checked_at': datetime.now().isoformat()
        }


# Enhanced seeder with Inspector integration
class DatabaseSeeder:
    """Advanced database seeding with Inspector integration testing"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.conn = db_manager.connect()
    
    def seed_test_data(self, num_inspections: int = 5, num_defects_per_inspection: int = 10):
        """Seed database with realistic test data including Inspector data"""
        logger.info(f"Seeding test data with Inspector integration: {num_inspections} inspections")
        
        # Create test users
        test_users = self.create_test_users()
        
        # Create test inspections (both regular and inspector)
        test_inspections = self.create_test_inspections(test_users, num_inspections)
        
        # Create test defects
        self.create_test_defects(test_inspections, test_users, num_defects_per_inspection)
        
        # Create test Inspector data
        self.create_test_inspector_data(test_users)
        
        # Create test processing queue items
        self.create_test_processing_queue(test_users)
        
        # Create test notifications
        self.create_test_notifications(test_users)
        
        self.conn.commit()
        logger.info("✅ Test data seeding completed with Inspector integration")
    
    def create_test_inspector_data(self, user_ids: List[int]):
        """Create test Inspector processed data"""
        cursor = self.conn.cursor()
        
        inspector_ids = [uid for uid in user_ids if self.get_user_role(uid) == 'inspector']
        if not inspector_ids:
            logger.warning("No inspector users found for test data")
            return
        
        # Create test buildings
        buildings = [
            {
                "name": "Harbour Views Apartments",
                "address": "123 Harbour Street, Sydney, NSW 2000",
                "total_units": 45,
                "total_defects": 89,
                "defect_rate": 12.5,
                "ready_units": 38,
                "ready_pct": 84.4,
                "quality_score": 87.5
            },
            {
                "name": "City Central Complex",
                "address": "456 Collins Street, Melbourne, VIC 3000",
                "total_units": 62,
                "total_defects": 134,
                "defect_rate": 18.2,
                "ready_units": 45,
                "ready_pct": 72.6,
                "quality_score": 81.8
            }
        ]
        
        for i, building_data in enumerate(buildings):
            building_id = str(uuid.uuid4())
            inspection_id = str(uuid.uuid4())
            
            # Create building
            cursor.execute("""
                INSERT INTO inspector_buildings (
                    id, name, address, total_units, total_defects, defect_rate,
                    ready_units, ready_pct, quality_score, unit_types
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                building_id, building_data["name"], building_data["address"],
                building_data["total_units"], building_data["total_defects"],
                building_data["defect_rate"], building_data["ready_units"],
                building_data["ready_pct"], building_data["quality_score"],
                "1-Bedroom Apartment, 2-Bedroom Apartment"
            ))
            
            # Create inspection
            inspection_date = (datetime.now() - timedelta(days=i*7)).date()
            cursor.execute("""
                INSERT INTO inspector_inspections (
                    id, building_id, inspection_date, inspector_name,
                    total_units, total_defects, defect_rate, ready_units, ready_pct,
                    urgent_defects, high_priority_defects, avg_defects_per_unit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                inspection_id, building_id, inspection_date, "John Smith",
                building_data["total_units"], building_data["total_defects"],
                building_data["defect_rate"], building_data["ready_units"],
                building_data["ready_pct"], 5, 15, 
                building_data["total_defects"] / building_data["total_units"]
            ))
            
            # Create sample inspection items
            self.create_sample_inspection_items(inspection_id, building_data["total_defects"])
            
            # Create work orders
            self.create_sample_work_orders(inspection_id)
            
            # Create progress tracking
            progress_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO inspector_project_progress (
                    id, building_id, inspection_id, total_defects, pending_defects, progress_pct
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                progress_id, building_id, inspection_id, 
                building_data["total_defects"], 
                int(building_data["total_defects"] * 0.3),  # 30% pending
                building_data["ready_pct"]
            ))
        
        logger.info("✅ Created test Inspector data")
    
    def create_sample_inspection_items(self, inspection_id: str, total_defects: int):
        """Create sample inspection items"""
        cursor = self.conn.cursor()
        
        sample_items = [
            ("Unit 01", "1-Bedroom Apartment", "Kitchen", "Cabinets", "Carpentry & Joinery", "Not OK", "Normal"),
            ("Unit 01", "1-Bedroom Apartment", "Bathroom", "Tiles", "Flooring - Tiles", "Not OK", "High Priority"),
            ("Unit 02", "2-Bedroom Apartment", "Living Room", "Windows", "Windows", "OK", "Normal"),
            ("Unit 02", "2-Bedroom Apartment", "Entry", "Door", "Doors", "Not OK", "Urgent"),
            ("Unit 03", "1-Bedroom Apartment", "Bedroom", "Flooring", "Flooring", "Not OK", "Normal"),
            ("Unit 03", "1-Bedroom Apartment", "Bathroom", "Exhaust Fan", "Electrical", "Not OK", "High Priority"),
        ]
        
        for i, (unit, unit_type, room, component, trade, status, urgency) in enumerate(sample_items):
            if i >= total_defects // 10:  # Create proportional amount
                break
                
            item_id = str(uuid.uuid4())
            planned_completion = datetime.now() + timedelta(days=7 if urgency == "Normal" else 3)
            
            cursor.execute("""
                INSERT INTO inspector_inspection_items (
                    id, inspection_id, unit, unit_type, room, component,
                    trade, status_class, urgency, planned_completion
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                item_id, inspection_id, unit, unit_type, room, component,
                trade, status, urgency, planned_completion.date()
            ))
    
    def create_sample_work_orders(self, inspection_id: str):
        """Create sample work orders"""
        cursor = self.conn.cursor()
        
        work_orders = [
            ("Unit 01", "Carpentry & Joinery", "Cabinets", "Kitchen", "Normal", "pending"),
            ("Unit 01", "Flooring - Tiles", "Tiles", "Bathroom", "High Priority", "in_progress"),
            ("Unit 02", "Doors", "Door", "Entry", "Urgent", "pending"),
            ("Unit 03", "Flooring", "Flooring", "Bedroom", "Normal", "completed"),
            ("Unit 03", "Electrical", "Exhaust Fan", "Bathroom", "High Priority", "in_progress"),
        ]
        
        for unit, trade, component, room, urgency, status in work_orders:
            work_order_id = str(uuid.uuid4())
            planned_date = datetime.now() + timedelta(days=7)
            
            cursor.execute("""
                INSERT INTO inspector_work_orders (
                    id, inspection_id, unit, trade, component, room, urgency, status, planned_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                work_order_id, inspection_id, unit, trade, component, room, urgency, status, planned_date.date()
            ))
    
    # Keep existing methods from original setup.py
    def create_test_users(self) -> List[int]:
        """Create test users for each role"""
        cursor = self.conn.cursor()
        import secrets
        import hashlib
        
        test_users = [
            ('inspector1', 'inspector1@test.com', 'John', 'Smith', 'inspector', 'ABC Inspections', 'Senior Inspector'),
            ('inspector2', 'inspector2@test.com', 'Sarah', 'Johnson', 'inspector', 'XYZ Building Services', 'Lead Inspector'),
            ('developer1', 'developer1@test.com', 'Mike', 'Chen', 'developer', 'Prestige Developments', 'Project Manager'),
            ('builder1', 'builder1@test.com', 'David', 'Wilson', 'builder', 'Premium Constructions', 'Site Supervisor'),
            ('builder2', 'builder2@test.com', 'Lisa', 'Taylor', 'builder', 'Elite Builders', 'Quality Manager'),
            ('owner1', 'owner1@test.com', 'Robert', 'Anderson', 'owner', 'Anderson Properties', 'Property Owner'),
        ]
        
        user_ids = []
        
        for username, email, first_name, last_name, role, company, job_title in test_users:
            # Generate password hash
            password = "test123"
            salt = secrets.token_hex(32)
            password_hash = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
            
            cursor.execute("""
                INSERT INTO users 
                (username, email, password_hash, salt, first_name, last_name, role)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (username, email, password_hash.hex(), salt, first_name, last_name, role))
            
            user_id = cursor.lastrowid
            user_ids.append(user_id)
            
            # Create profile
            cursor.execute("""
                INSERT INTO user_profiles (user_id, company, job_title, phone, city)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, company, job_title, f"+61 4{user_id:02d}8 {user_id:03d} {user_id:03d}", 'Sydney'))
        
        logger.info(f"✅ Created {len(test_users)} test users")
        return user_ids
    
    def create_test_inspections(self, user_ids: List[int], count: int) -> List[int]:
        """Create test inspections"""
        cursor = self.conn.cursor()
        
        project_names = [
            "Harbour Views Apartments", "Cityscape Towers", "Garden Grove Residences",
            "Metropolitan Complex", "Riverside Development"
        ]
        
        inspection_types = ['initial', 're_inspection', 'final', 'dlp']
        building_phases = ['foundation', 'structure', 'fit_out', 'completion', 'dlp']
        priorities = ['low', 'medium', 'high', 'critical']
        
        inspection_ids = []
        inspector_ids = [uid for uid in user_ids if self.get_user_role(uid) == 'inspector']
        
        for i in range(count):
            project_name = project_names[i % len(project_names)]
            inspection_date = (datetime.now() - timedelta(days=i*3)).date()
            inspector_id = inspector_ids[i % len(inspector_ids)] if inspector_ids else user_ids[0]
            
            cursor.execute("""
                INSERT INTO inspections (
                    project_name, project_code, inspection_date, inspection_type,
                    building_phase, inspector_id, status, priority,
                    project_address, project_city, project_state,
                    total_defects, quality_score, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                project_name, f"PRJ-{i+1:03d}", inspection_date,
                inspection_types[i % len(inspection_types)],
                building_phases[i % len(building_phases)],
                inspector_id, 'completed' if i > 2 else 'in_progress',
                priorities[i % len(priorities)],
                f"{100 + i*10} Collins Street", "Melbourne", "VIC",
                0, round(75 + (i % 20), 1), f"Test inspection notes for {project_name}"
            ))
            
            inspection_ids.append(cursor.lastrowid)
        
        logger.info(f"✅ Created {count} test inspections")
        return inspection_ids
    
    def create_test_defects(self, inspection_ids: List[int], user_ids: List[int], defects_per_inspection: int):
        """Create test defects for inspections"""
        cursor = self.conn.cursor()
        
        defect_templates = [
            ("Paint finish uneven", "Painting", "minor", "Uneven paint application on walls"),
            ("Tile grout missing", "Tiling", "minor", "Missing grout between floor tiles"),
            ("Door not closing properly", "Carpentry", "major", "Entry door not closing flush"),
            ("Electrical outlet loose", "Electrical", "major", "Wall outlet not secured properly"),
            ("Water leak visible", "Plumbing", "critical", "Water leak from pipe connection"),
            ("Crack in concrete", "Concrete", "major", "Hairline crack in concrete slab"),
        ]
        
        locations = ["Living Room", "Kitchen", "Bathroom", "Bedroom 1", "Bedroom 2", "Balcony"]
        severities = ["critical", "major", "minor", "observation"]
        statuses = ["open", "in_progress", "resolved", "closed"]
        
        inspector_ids = [uid for uid in user_ids if self.get_user_role(uid) == 'inspector']
        builder_ids = [uid for uid in user_ids if self.get_user_role(uid) == 'builder']
        
        if not inspector_ids:
            inspector_ids = [user_ids[0]]  # Fallback
        if not builder_ids:
            builder_ids = [user_ids[0]]  # Fallback
        
        defect_count = 0
        
        for inspection_id in inspection_ids:
            num_defects = defects_per_inspection + (inspection_id % 3) - 1
            
            for i in range(num_defects):
                template = defect_templates[i % len(defect_templates)]
                
                cursor.execute("""
                    INSERT INTO defects (
                        inspection_id, defect_id, title, description, location,
                        trade, severity, status, priority, assigned_to, reported_by,
                        unit_number, room, has_photos, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    inspection_id, f"DEF-{defect_count+1:05d}",
                    template[0], template[3],
                    locations[i % len(locations)],
                    template[1], template[2],
                    statuses[i % len(statuses)],
                    (i % 5) + 1,
                    builder_ids[i % len(builder_ids)] if i % 3 == 0 else None,
                    inspector_ids[0],
                    f"Unit {(i % 10) + 1:02d}",
                    locations[i % len(locations)],
                    i % 4 == 0,
                    datetime.now() - timedelta(days=i, hours=i*2)
                ))
                
                defect_count += 1
        
        logger.info(f"✅ Created {defect_count} test defects")
    
    def create_test_processing_queue(self, user_ids: List[int]):
        """Create test processing queue items"""
        cursor = self.conn.cursor()
        
        inspector_ids = [uid for uid in user_ids if self.get_user_role(uid) == 'inspector']
        if not inspector_ids:
            return
        
        queue_items = [
            ("inspection_data_001.csv", "completed", 100, "Processed 45 defects successfully"),
            ("building_defects_002.csv", "processing", 60, "Processing defects..."),
            ("site_inspection_003.csv", "queued", 0, "Waiting for processing"),
        ]
        
        for i, (filename, status, progress, message) in enumerate(queue_items):
            cursor.execute("""
                INSERT INTO processing_queue (
                    file_name, file_path, file_type, inspector_id, status, progress,
                    total_records, processed_records, error_count, error_message, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                filename, f"uploads/{filename}", "csv",
                inspector_ids[i % len(inspector_ids)], status, progress,
                45 if status == "completed" else 0,
                45 if status == "completed" else 0,
                0, message if status == "failed" else None,
                datetime.now() - timedelta(hours=i*2)
            ))
        
        logger.info("✅ Created test processing queue items")
    
    def create_test_notifications(self, user_ids: List[int]):
        """Create test notifications"""
        cursor = self.conn.cursor()
        
        notifications = [
            ("info", "New Inspection Assigned", "You have been assigned to inspect Harbour Views Apartments"),
            ("warning", "Work Order Requires Attention", "Critical work order #WO-00001 needs immediate review"),
            ("success", "Report Generated", "Excel report for Project PRJ-001 is ready for download"),
            ("reminder", "Inspection Due", "Final inspection for Cityscape Towers is due tomorrow"),
        ]
        
        for i, (type_, title, message) in enumerate(notifications):
            for user_id in user_ids[:3]:
                cursor.execute("""
                    INSERT INTO notifications (
                        user_id, type, title, message, is_read, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    user_id, type_, title, message,
                    i % 3 == 0,
                    datetime.now() - timedelta(hours=i*6)
                ))
        
        logger.info("✅ Created test notifications")
    
    def get_user_role(self, user_id: int) -> str:
        """Get user role by ID"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT role FROM users WHERE id = ?", (user_id,))
        result = cursor.fetchone()
        return result[0] if result else None

def migrate_existing_database(db_path: str = "building_inspection.db"):
    """
    Migrate existing database to add missing columns
    Run this once if you have an existing database
    """
    import os
    
    if not os.path.exists(db_path):
        logger.info("No existing database found - no migration needed")
        return
    
    logger.info("🔄 Starting database migration...")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if inspector_csv_processing_log exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='inspector_csv_processing_log'
        """)
        
        if cursor.fetchone():
            # Add inspector_name column if missing
            try:
                cursor.execute("""
                    ALTER TABLE inspector_csv_processing_log 
                    ADD COLUMN inspector_name TEXT
                """)
                logger.info("✅ Added inspector_name column")
            except sqlite3.OperationalError as e:
                if "duplicate column" in str(e).lower():
                    logger.info("ℹ️  inspector_name column already exists")
                else:
                    raise
            
            # Add file_checksum column if missing
            try:
                cursor.execute("""
                    ALTER TABLE inspector_csv_processing_log 
                    ADD COLUMN file_checksum TEXT
                """)
                logger.info("✅ Added file_checksum column")
            except sqlite3.OperationalError as e:
                if "duplicate column" in str(e).lower():
                    logger.info("ℹ️  file_checksum column already exists")
                else:
                    raise
        
        # Check inspector_inspection_items table
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='inspector_inspection_items'
        """)
        
        if cursor.fetchone():
            # Add inspection_date column if missing
            try:
                cursor.execute("""
                    ALTER TABLE inspector_inspection_items 
                    ADD COLUMN inspection_date DATE
                """)
                logger.info("✅ Added inspection_date column to inspector_inspection_items")
            except sqlite3.OperationalError as e:
                if "duplicate column" in str(e).lower():
                    logger.info("ℹ️  inspection_date column already exists")
                else:
                    raise
            
            # Add owner_signoff_timestamp column if missing
            try:
                cursor.execute("""
                    ALTER TABLE inspector_inspection_items 
                    ADD COLUMN owner_signoff_timestamp TIMESTAMP
                """)
                logger.info("✅ Added owner_signoff_timestamp column to inspector_inspection_items")
            except sqlite3.OperationalError as e:
                if "duplicate column" in str(e).lower():
                    logger.info("ℹ️  owner_signoff_timestamp column already exists")
                else:
                    raise
        
        conn.commit()
        conn.close()
        
        logger.info("✅ Database migration completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
# Main database setup function with Inspector integration
def setup_database(db_path: str = "building_inspection.db", force_recreate: bool = False, 
                  seed_test_data: bool = False) -> DatabaseManager:
    """
    Enhanced database setup function with Inspector integration
    
    Args:
        db_path: Path to database file
        force_recreate: Whether to recreate database from scratch
        seed_test_data: Whether to populate with test data including Inspector data
    
    Returns:
        DatabaseManager instance with Inspector integration
    """
    logger.info("🚀 Starting Building Inspection System V3 database setup with Inspector integration...")
    
    # Run migration first if database exists and not forcing recreate
    if not force_recreate and os.path.exists(db_path):
        migrate_existing_database(db_path)
    
    # Initialize database manager
    db_manager = DatabaseManager(db_path)
    
    # Initialize database schema
    db_manager.initialize_database(force_recreate)
    
    # Seed test data if requested
    if seed_test_data:
        seeder = DatabaseSeeder(db_manager)
        seeder.seed_test_data(num_inspections=3, num_defects_per_inspection=8)
    
    # Validate database integrity
    integrity_check = db_manager.validate_database_integrity()
    if not integrity_check['is_valid']:
        logger.warning(f"Database integrity issues found: {integrity_check['issues']}")
    else:
        logger.info("✅ Database integrity validation passed")
    
    # Get and log database statistics
    stats = db_manager.get_database_stats()
    logger.info("📊 Database Statistics:")
    for key, value in stats.items():
        logger.info(f"  {key}: {value}")
    
    logger.info("🎉 Database setup completed successfully with Inspector integration!")
    return db_manager

# Example usage and testing
if __name__ == "__main__":
    # Setup database with test data including Inspector integration
    db_manager = setup_database(
        db_path="building_inspection_v3.db",
        force_recreate=True,
        seed_test_data=True
    )
    
    # Example: Test Inspector integration
    print("\n=== Testing Inspector Integration ===")
    
    # Test getting Inspector inspections
    inspector_inspections = db_manager.get_inspector_inspections(5)
    print(f"Inspector inspections: {len(inspector_inspections)}")
    if len(inspector_inspections) > 0:
        print(inspector_inspections.head())
    
    # Test getting work orders for Builder
    work_orders = db_manager.get_work_orders_for_builder()
    print(f"\nWork orders for Builder: {len(work_orders)}")
    if len(work_orders) > 0:
        print(work_orders.head())
    
    # Test getting project overview for Developer
    project_overview = db_manager.get_project_overview_for_developer()
    print(f"\nProject overview for Developer: {len(project_overview)}")
    if len(project_overview) > 0:
        print(project_overview.head())
    
    # Test trade mapping
    trade_mapping = db_manager.get_trade_mapping()
    print(f"\nTrade mappings: {len(trade_mapping)}")
    
    # Create backup
    backup_path = db_manager.backup_database()
    print(f"\nBackup created: {backup_path}")
    
    # Get database stats
    stats = db_manager.get_database_stats()
    print("\nDatabase Statistics:", json.dumps(stats, indent=2))
    
    # Close connection
    if db_manager.connection:
        db_manager.connection.close()
    
    print("\n✅ Inspector integration testing completed!")