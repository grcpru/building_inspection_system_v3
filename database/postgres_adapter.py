"""
PostgreSQL Schema Adapter
Converts SQLite schema to PostgreSQL and creates all tables
"""

from database.connection_manager import get_connection_manager
import streamlit as st


class PostgresAdapter:
    """Handles PostgreSQL-specific schema creation and operations"""
    
    def __init__(self):
        self.conn_manager = get_connection_manager()
    
    def initialize_schema(self):
        """Create all tables in PostgreSQL - ONLY IF THEY DON'T EXIST"""
        
        conn = self.conn_manager.get_connection()
        cursor = conn.cursor()
        
        try:
            # ✅ CHECK: Do tables already exist?
            cursor.execute("""
                SELECT COUNT(*) as table_count
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'inspector_inspections';
            """)
            result = cursor.fetchone()
            
            # 🔧 FIX: Handle RealDictRow, tuple, or dict properly
            if result is None:
                count = 0
            elif isinstance(result, dict):
                # RealDictRow behaves like a dict
                count = result.get('table_count', result.get('count', 0))
            elif isinstance(result, (list, tuple)):
                count = result[0]
            else:
                # Fallback: try to access as dict first, then as index
                try:
                    count = result['table_count']
                except (TypeError, KeyError):
                    try:
                        count = result[0]
                    except (TypeError, IndexError):
                        count = 0
            
            print(f"🔍 Table check result: {result} -> count={count}")
            
            if count > 0:
                print("✅ Schema already exists - skipping initialization")
                print(f"   inspector_inspections table found in database")
                cursor.close()
                conn.close()
                return  # ✅ STOP HERE - Don't recreate anything!
            
            # Only create if tables don't exist
            print("⚙️ First run - creating schema...")
            
            # Enable UUID extension if available
            try:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
                conn.commit()
            except Exception as e:
                print(f"UUID extension not available: {e}")
                conn.rollback()
            
            # Create all tables
            print("Creating users table...")
            self._create_users_table(cursor)
            conn.commit()
            
            print("Creating inspector tables...")
            self._create_inspector_tables(cursor)
            conn.commit()
            
            print("Creating builder tables...")
            self._create_builder_tables(cursor)
            conn.commit()
            
            print("Creating developer tables...")
            self._create_developer_tables(cursor)
            conn.commit()
            
            print("Creating owner tables...")
            self._create_owner_tables(cursor)
            conn.commit()
            
            print("Creating admin tables...")
            self._create_admin_tables(cursor)
            conn.commit()
            
            # Create indexes (safely - skip if they fail)
            print("Creating indexes...")
            self._create_indexes(cursor)
            conn.commit()
            
            print("✅ PostgreSQL schema initialized successfully")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error initializing PostgreSQL schema: {e}")
            import traceback
            print(traceback.format_exc())
            raise
        finally:
            cursor.close()
            conn.close()
    
    def _create_users_table(self, cursor):
        """Create users table"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(20) NOT NULL,
                email VARCHAR(100),
                full_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
    
    def _create_inspector_tables(self, cursor):
        """Create inspector-related tables"""
        
        # Buildings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_buildings (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                address TEXT,
                developer_name TEXT,
                total_units INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Inspections
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_inspections (
                id TEXT PRIMARY KEY,
                building_id TEXT REFERENCES inspector_buildings(id),
                inspection_date DATE NOT NULL,
                inspector_name TEXT,
                total_units INTEGER,
                total_defects INTEGER,
                ready_pct DECIMAL(5,2),
                original_filename TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Inspection Items
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_inspection_items (
                id TEXT PRIMARY KEY,
                inspection_id TEXT REFERENCES inspector_inspections(id),
                building_id TEXT REFERENCES inspector_buildings(id),
                unit_number TEXT,
                floor TEXT,
                zone TEXT,
                room TEXT,
                item_description TEXT,
                defect_type TEXT,
                severity TEXT,
                status TEXT DEFAULT 'pending',
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Work Orders
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_work_orders (
                id TEXT PRIMARY KEY,
                inspection_id TEXT REFERENCES inspector_inspections(id),
                building_id TEXT REFERENCES inspector_buildings(id),
                work_order_number TEXT UNIQUE,
                title TEXT NOT NULL,
                description TEXT,
                priority TEXT DEFAULT 'medium',
                status TEXT DEFAULT 'open',
                assigned_to TEXT,
                defect_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                due_date DATE,
                completed_at TIMESTAMP
            )
        """)
        
        # Work Order Items (linking)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_work_order_items (
                id SERIAL PRIMARY KEY,
                work_order_id TEXT REFERENCES inspector_work_orders(id),
                inspection_item_id TEXT REFERENCES inspector_inspection_items(id),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Unit Inspections
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_unit_inspections (
                id TEXT PRIMARY KEY,
                inspection_id TEXT REFERENCES inspector_inspections(id),
                building_id TEXT REFERENCES inspector_buildings(id),
                unit_number TEXT,
                floor TEXT,
                zone TEXT,
                total_defects INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                is_ready BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # CSV Processing Log
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_csv_processing_log (
                id SERIAL PRIMARY KEY,
                filename TEXT NOT NULL,
                inspection_id TEXT,
                inspector_name TEXT,
                rows_processed INTEGER,
                defects_found INTEGER,
                work_orders_created INTEGER,
                status TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def _create_builder_tables(self, cursor):
        """Create builder-related tables"""
        
        # Work Order Updates/Comments
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS builder_work_order_updates (
                id SERIAL PRIMARY KEY,
                work_order_id TEXT REFERENCES inspector_work_orders(id),
                updated_by TEXT,
                old_status TEXT,
                new_status TEXT,
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Builder Tasks
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS builder_tasks (
                id TEXT PRIMARY KEY,
                work_order_id TEXT REFERENCES inspector_work_orders(id),
                task_description TEXT NOT NULL,
                assigned_to TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)
    
    def _create_developer_tables(self, cursor):
        """Create developer-related tables"""
        
        # Projects
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS developer_projects (
                id TEXT PRIMARY KEY,
                project_name TEXT NOT NULL,
                building_id TEXT REFERENCES inspector_buildings(id),
                status TEXT DEFAULT 'active',
                start_date DATE,
                target_completion DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Analytics/Metrics
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS developer_metrics (
                id SERIAL PRIMARY KEY,
                project_id TEXT,
                metric_name TEXT NOT NULL,
                metric_value DECIMAL(10,2),
                metric_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def _create_owner_tables(self, cursor):
        """Create owner-related tables"""
        
        # Approvals
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS owner_approvals (
                id SERIAL PRIMARY KEY,
                inspection_id TEXT REFERENCES inspector_inspections(id),
                work_order_id TEXT,
                approved_by TEXT,
                approval_status TEXT,
                comments TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def _create_admin_tables(self, cursor):
        """Create admin-related tables"""
        
        # Audit Log
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admin_audit_log (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                action TEXT NOT NULL,
                table_name TEXT,
                record_id TEXT,
                old_value TEXT,
                new_value TEXT,
                ip_address TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # System Settings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admin_settings (
                id SERIAL PRIMARY KEY,
                setting_key TEXT UNIQUE NOT NULL,
                setting_value TEXT,
                description TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def _create_indexes(self, cursor):
        """Create indexes - with error handling for each"""
        
        indexes = [
            # Inspector tables
            ("CREATE INDEX IF NOT EXISTS idx_inspections_building ON inspector_inspections(building_id)", "idx_inspections_building"),
            ("CREATE INDEX IF NOT EXISTS idx_inspections_date ON inspector_inspections(inspection_date)", "idx_inspections_date"),
            ("CREATE INDEX IF NOT EXISTS idx_inspection_items_inspection ON inspector_inspection_items(inspection_id)", "idx_inspection_items_inspection"),
            ("CREATE INDEX IF NOT EXISTS idx_work_orders_inspection ON inspector_work_orders(inspection_id)", "idx_work_orders_inspection"),
            ("CREATE INDEX IF NOT EXISTS idx_work_orders_status ON inspector_work_orders(status)", "idx_work_orders_status"),
            
            # User indexes
            ("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)", "idx_users_username"),
            ("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role)", "idx_users_role"),
        ]
        
        for sql, idx_name in indexes:
            try:
                cursor.execute(sql)
                print(f"  ✅ Created index: {idx_name}")
            except Exception as e:
                print(f"  ⚠️  Skipped index {idx_name}: {e}")
                # Continue with other indexes even if one fails
                continue
    
    def create_default_users(self):
        """Create default demo users with proper password hashing"""
        
        conn = self.conn_manager.get_connection()
        cursor = conn.cursor()
        
        try:
            # Import password hashing (adjust based on your auth system)
            from werkzeug.security import generate_password_hash
            
            # Generate hashes for default passwords
            admin_hash = generate_password_hash('admin123')
            test_hash = generate_password_hash('test123')
            
            default_users = [
                ('admin', admin_hash, 'admin', 'admin@ecm.com', 'System Admin'),
                ('inspector', test_hash, 'inspector', 'inspector@ecm.com', 'Inspector User'),
                ('builder', test_hash, 'builder', 'builder@ecm.com', 'Builder User'),
                ('developer', test_hash, 'developer', 'developer@ecm.com', 'Developer User'),
                ('owner', test_hash, 'owner', 'owner@ecm.com', 'Owner User'),
            ]
            
            for username, pwd_hash, role, email, full_name in default_users:
                try:
                    cursor.execute("""
                        INSERT INTO users (username, password_hash, role, email, full_name)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (username) DO NOTHING
                    """, (username, pwd_hash, role, email, full_name))
                    print(f"  ✅ Created user: {username}")
                except Exception as e:
                    print(f"  ⚠️  User {username} already exists or error: {e}")
                    continue
            
            conn.commit()
            print("✅ Default users created/verified")
            
        except ImportError:
            # If werkzeug not available, create users with placeholder hashes
            print("⚠️  werkzeug not available, creating users with basic hashes")
            
            # Use a simple hash as fallback
            import hashlib
            admin_hash = hashlib.sha256('admin123'.encode()).hexdigest()
            test_hash = hashlib.sha256('test123'.encode()).hexdigest()
            
            default_users = [
                ('admin', admin_hash, 'admin', 'admin@ecm.com', 'System Admin'),
                ('inspector', test_hash, 'inspector', 'inspector@ecm.com', 'Inspector User'),
                ('builder', test_hash, 'builder', 'builder@ecm.com', 'Builder User'),
                ('developer', test_hash, 'developer', 'developer@ecm.com', 'Developer User'),
                ('owner', test_hash, 'owner', 'owner@ecm.com', 'Owner User'),
            ]
            
            for username, pwd_hash, role, email, full_name in default_users:
                try:
                    cursor.execute("""
                        INSERT INTO users (username, password_hash, role, email, full_name)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (username) DO NOTHING
                    """, (username, pwd_hash, role, email, full_name))
                except Exception as e:
                    continue
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            print(f"⚠️  Error creating default users: {e}")
        finally:
            cursor.close()
            conn.close()