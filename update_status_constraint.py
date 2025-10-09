import sqlite3

def update_status_constraint():
    """Add 'waiting_approval' to status constraint"""
    conn = sqlite3.connect("building_inspection.db")
    cursor = conn.cursor()
    
    try:
        # Backup data
        print("Backing up data...")
        cursor.execute("CREATE TABLE inspector_work_orders_backup AS SELECT * FROM inspector_work_orders")
        
        # Drop old table
        print("Dropping old table...")
        cursor.execute("DROP TABLE inspector_work_orders")
        
        # Recreate with new constraint
        print("Creating new table with updated constraint...")
        cursor.execute("""
            CREATE TABLE inspector_work_orders (
                id TEXT PRIMARY KEY,
                inspection_id TEXT NOT NULL,
                unit TEXT,
                trade TEXT,
                component TEXT,
                room TEXT,
                urgency TEXT,
                status TEXT CHECK (status IN ('pending', 'in_progress', 'waiting_approval', 'completed', 'cancelled')) NOT NULL,
                assigned_to TEXT,
                planned_date TEXT,
                started_date TEXT,
                completed_date TEXT,
                estimated_hours REAL,
                actual_hours REAL,
                notes TEXT,
                builder_notes TEXT,
                photos_required INTEGER DEFAULT 0,
                safety_requirements TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (inspection_id) REFERENCES inspector_inspections(id)
            )
        """)
        
        # Restore data
        print("Restoring data...")
        cursor.execute("INSERT INTO inspector_work_orders SELECT * FROM inspector_work_orders_backup")
        
        # Clean up
        cursor.execute("DROP TABLE inspector_work_orders_backup")
        
        conn.commit()
        print("✅ Database updated successfully!")
        print("✅ Status constraint now includes: 'pending', 'in_progress', 'waiting_approval', 'completed', 'cancelled'")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    update_status_constraint()