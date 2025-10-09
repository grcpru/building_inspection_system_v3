"""
Database Schema Fix for Building Inspection System V3
======================================================
Run this script to fix the missing columns and tables causing save failures.
"""

import sqlite3
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fix_database_schema(db_path: str = "building_inspection.db"):
    """
    Fix database schema to match save_inspector_data() expectations
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        logger.info("Starting database schema fix...")
        
        # FIX 1: Add missing owner_signoff_timestamp column to inspector_inspection_items
        try:
            cursor.execute("""
                ALTER TABLE inspector_inspection_items 
                ADD COLUMN owner_signoff_timestamp TIMESTAMP
            """)
            logger.info("✅ Added owner_signoff_timestamp column")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                logger.info("✓ owner_signoff_timestamp column already exists")
            else:
                raise
        
        # FIX 2: Add missing inspection_date column to inspector_inspection_items
        try:
            cursor.execute("""
                ALTER TABLE inspector_inspection_items 
                ADD COLUMN inspection_date DATE
            """)
            logger.info("✅ Added inspection_date column")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                logger.info("✓ inspection_date column already exists")
            else:
                raise
        
        # FIX 3: Create missing inspector_unit_inspections table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inspector_unit_inspections (
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
        logger.info("✅ Created inspector_unit_inspections table")
        
        # FIX 4: Add index for performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_inspector_unit_inspections_inspection 
            ON inspector_unit_inspections(inspection_id)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_inspector_unit_inspections_unit 
            ON inspector_unit_inspections(unit)
        """)
        
        # FIX 5: Add file_checksum column to inspector_csv_processing_log
        try:
            cursor.execute("""
                ALTER TABLE inspector_csv_processing_log 
                ADD COLUMN file_checksum TEXT
            """)
            logger.info("✅ Added file_checksum column")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                logger.info("✓ file_checksum column already exists")
            else:
                raise
        
        conn.commit()
        
        # Verify the fixes
        logger.info("\n=== Verification ===")
        
        # Check inspector_inspection_items columns
        cursor.execute("PRAGMA table_info(inspector_inspection_items)")
        items_columns = [row[1] for row in cursor.fetchall()]
        
        required_columns = ['owner_signoff_timestamp', 'inspection_date']
        for col in required_columns:
            if col in items_columns:
                logger.info(f"✓ inspector_inspection_items.{col} exists")
            else:
                logger.error(f"✗ inspector_inspection_items.{col} MISSING")
        
        # Check inspector_unit_inspections exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='inspector_unit_inspections'
        """)
        if cursor.fetchone():
            logger.info("✓ inspector_unit_inspections table exists")
        else:
            logger.error("✗ inspector_unit_inspections table MISSING")
        
        logger.info("\n✅ Database schema fix complete!")
        logger.info("You can now process inspection data successfully.")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Schema fix failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        conn.close()


def verify_save_readiness(db_path: str = "building_inspection.db"):
    """
    Verify database is ready for save operations
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    issues = []
    
    try:
        # Check critical tables
        required_tables = [
            'inspector_buildings',
            'inspector_inspections',
            'inspector_inspection_items',
            'inspector_unit_inspections',
            'inspector_work_orders',
            'inspector_csv_processing_log'
        ]
        
        cursor.execute("""
            SELECT name FROM sqlite_master WHERE type='table'
        """)
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        for table in required_tables:
            if table not in existing_tables:
                issues.append(f"Missing table: {table}")
        
        # Check critical columns
        cursor.execute("PRAGMA table_info(inspector_inspection_items)")
        items_columns = [row[1] for row in cursor.fetchall()]
        
        required_item_columns = [
            'id', 'inspection_id', 'unit', 'unit_type', 'inspection_date',
            'room', 'component', 'trade', 'status_class', 'urgency',
            'planned_completion', 'owner_signoff_timestamp'
        ]
        
        for col in required_item_columns:
            if col not in items_columns:
                issues.append(f"Missing column: inspector_inspection_items.{col}")
        
        # Check foreign keys enabled
        cursor.execute("PRAGMA foreign_keys")
        if cursor.fetchone()[0] != 1:
            issues.append("Foreign keys not enabled")
        
        if issues:
            logger.error("\n❌ Database NOT ready for save:")
            for issue in issues:
                logger.error(f"  - {issue}")
            return False
        else:
            logger.info("\n✅ Database is ready for save operations")
            logger.info(f"  - All {len(required_tables)} required tables exist")
            logger.info(f"  - All {len(required_item_columns)} required columns exist")
            logger.info("  - Foreign keys enabled")
            return True
            
    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
        return False
    finally:
        conn.close()


if __name__ == "__main__":
    print("=" * 70)
    print("Building Inspection System V3 - Database Schema Fix")
    print("=" * 70)
    
    # Run the fix
    fix_database_schema()
    
    # Verify
    print("\n" + "=" * 70)
    verify_save_readiness()
    print("=" * 70)