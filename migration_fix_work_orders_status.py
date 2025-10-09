# migration_fix_work_orders_status.py
"""
Database Migration - Fix Work Orders Status Constraint
Run this ONCE to update your existing database
"""

import sqlite3

def migrate_work_orders_status():
    """Fix work orders table to support 'approved' status"""
    
    conn = sqlite3.connect("building_inspection.db")
    cursor = conn.cursor()
    
    try:
        print("🔧 Starting migration: Fix work orders status constraint...")
        
        # Check current schema
        cursor.execute("""
            SELECT sql FROM sqlite_master 
            WHERE type='table' AND name='inspector_work_orders'
        """)
        result = cursor.fetchone()
        
        if result and 'approved' not in result[0]:
            print("⚠️  Migration needed - fixing status constraint...")
            
            cursor.execute("BEGIN TRANSACTION")
            
            # 1. Rename old table
            cursor.execute("""
                ALTER TABLE inspector_work_orders 
                RENAME TO inspector_work_orders_old
            """)
            print("✓ Renamed old table")
            
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
            print("✓ Created new table with correct constraint")
            
            # 3. Copy all data
            cursor.execute("""
                INSERT INTO inspector_work_orders 
                SELECT * FROM inspector_work_orders_old
            """)
            print("✓ Copied data to new table")
            
            # 4. Drop old table
            cursor.execute("DROP TABLE inspector_work_orders_old")
            print("✓ Dropped old table")
            
            # 5. Recreate indexes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_work_orders_status_trade 
                ON inspector_work_orders(status, trade)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_inspector_work_orders_trade 
                ON inspector_work_orders(trade)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_inspector_work_orders_status 
                ON inspector_work_orders(status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_inspector_work_orders_assigned 
                ON inspector_work_orders(assigned_to)
            """)
            print("✓ Recreated indexes")
            
            conn.commit()
            print("✅ Migration completed successfully!")
            print("✅ Work orders table now supports 'approved' status")
            
        else:
            print("✅ Database already up to date - no migration needed")
            
    except Exception as e:
        conn.rollback()
        print(f"❌ Migration failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_work_orders_status()
    print("\n✨ You can now approve work orders in the Developer interface!")