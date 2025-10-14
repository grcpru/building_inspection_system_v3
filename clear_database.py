"""
Safe Database Clear Script for Local SQLite
===========================================
Run this script to clear your local building_inspection.db database
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = "building_inspection.db"

def clear_file_tracking():
    """Clear file tracking cache/session"""
    import glob
    
    # Clear any .streamlit cache files
    cache_files = glob.glob(".streamlit/cache/**", recursive=True)
    for f in cache_files:
        if os.path.isfile(f):
            try:
                os.remove(f)
                print(f"   - Removed cache file: {f}")
            except:
                pass
    
    # Clear uploads folder if it exists
    if os.path.exists("uploads"):
        import shutil
        try:
            shutil.rmtree("uploads")
            print("   - Cleared uploads folder")
        except:
            pass
    
    print("   - File tracking cleared")

def backup_database():
    """Create a backup before clearing"""
    if os.path.exists(DB_PATH):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"building_inspection_backup_{timestamp}.db"
        
        import shutil
        shutil.copy2(DB_PATH, backup_path)
        print(f"✅ Backup created: {backup_path}")
        return backup_path
    return None

def clear_all_data():
    """Clear ALL data from all tables (except users)"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    tables_to_clear = [
        'inspector_work_orders',
        'inspector_inspection_items',
        'inspector_inspections',
        'inspector_buildings',
        'inspector_unit_inspections',
        'inspector_metrics_summary',
        'inspector_project_progress',
        'inspector_work_order_files',
        'work_order_files',
        'inspector_csv_processing_log',  # ✅ THIS is the file tracking table
        'processed_files',  # Also try this in case it exists
        'processing_queue',  # Clear upload queue
        # NOTE: 'users' and 'user_profiles' tables are NOT cleared - users are preserved
    ]
    
    print("\n🗑️ Clearing all data (keeping users)...")
    for table in tables_to_clear:
        try:
            cursor.execute(f"DELETE FROM {table}")
            count = cursor.rowcount
            if count > 0:
                print(f"   - Cleared {count} rows from {table}")
        except sqlite3.OperationalError as e:
            # Table doesn't exist, skip silently
            pass
    
    conn.commit()
    conn.close()
    
    # Also clear any session state file tracking if it exists
    clear_file_tracking()
    
    print("✅ All data cleared (users preserved)!\n")

def clear_work_orders_only():
    """Clear only work orders, keep inspections and buildings"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("\n🗑️ Clearing work orders and related data...")
    
    # Clear work order files first (foreign key)
    try:
        cursor.execute("DELETE FROM work_order_files")
        print(f"   - Cleared {cursor.rowcount} work order files")
    except sqlite3.OperationalError:
        print("   - work_order_files table not found")
    
    # Clear work orders
    cursor.execute("DELETE FROM inspector_work_orders")
    print(f"   - Cleared {cursor.rowcount} work orders")
    
    # Clear inspection items (defects)
    cursor.execute("DELETE FROM inspector_inspection_items")
    print(f"   - Cleared {cursor.rowcount} inspection items")
    
    conn.commit()
    conn.close()
    print("✅ Work orders and defects cleared!\n")

def clear_pending_only():
    """Clear only pending work orders"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("\n🗑️ Clearing pending work orders only...")
    
    cursor.execute("DELETE FROM inspector_work_orders WHERE status = 'pending'")
    count = cursor.rowcount
    
    conn.commit()
    conn.close()
    print(f"✅ Cleared {count} pending work orders!\n")

def show_current_stats():
    """Show current database statistics"""
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found: {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("\n📊 Current Database Statistics:")
    print("=" * 50)
    
    tables = [
        ('Buildings', 'inspector_buildings'),
        ('Inspections', 'inspector_inspections'),
        ('Work Orders', 'inspector_work_orders'),
        ('Inspection Items', 'inspector_inspection_items'),
        ('Unit Inspections', 'inspector_unit_inspections'),
        ('Work Order Files', 'work_order_files'),
        ('CSV Processing Log', 'inspector_csv_processing_log'),
        ('Processing Queue', 'processing_queue'),
        ('Users', 'users')
    ]
    
    for name, table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"   {name:20s}: {count:,}")
        except sqlite3.OperationalError:
            print(f"   {name:20s}: Table not found")
    
    # Show work order status breakdown
    try:
        cursor.execute("""
            SELECT status, COUNT(*) as count 
            FROM inspector_work_orders 
            GROUP BY status
        """)
        results = cursor.fetchall()
        if results:
            print("\n   Work Order Status Breakdown:")
            for status, count in results:
                print(f"      - {status}: {count:,}")
    except:
        pass
    
    conn.close()
    print("=" * 50 + "\n")

def main():
    """Main menu"""
    print("\n" + "=" * 50)
    print("🗄️  DATABASE CLEAR UTILITY")
    print("=" * 50)
    
    # Show current stats
    show_current_stats()
    
    print("What would you like to do?")
    print("\n1. Clear ALL data except users (buildings, inspections, work orders, file tracking)")
    print("2. Clear work orders and defects only (keep buildings & inspections)")
    print("3. Clear pending work orders only")
    print("4. Show statistics only (no changes)")
    print("5. Delete database file completely")
    print("6. Clear EVERYTHING including users")
    print("0. Cancel")
    
    choice = input("\nEnter your choice (0-6): ").strip()
    
    if choice == '0':
        print("❌ Cancelled. No changes made.")
        return
    
    if choice == '4':
        return
    
    # Confirm action
    print("\n⚠️  WARNING: This action cannot be undone!")
    confirm = input("Type 'YES' to confirm: ").strip()
    
    if confirm != 'YES':
        print("❌ Cancelled. No changes made.")
        return
    
    # Create backup first
    backup_path = backup_database()
    if backup_path:
        print(f"💾 Backup saved as: {backup_path}")
    
    # Execute chosen action
    if choice == '1':
        clear_all_data()
    elif choice == '2':
        clear_work_orders_only()
    elif choice == '3':
        clear_pending_only()
    elif choice == '5':
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
            print(f"✅ Database file deleted: {DB_PATH}")
        else:
            print(f"❌ Database file not found: {DB_PATH}")
    elif choice == '6':
        # Clear everything INCLUDING users
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        tables = ['inspector_work_orders', 'inspector_inspection_items', 
                  'inspector_inspections', 'inspector_buildings', 'work_order_files',
                  'processed_files', 'users']
        print("\n🗑️ Clearing EVERYTHING including users...")
        for table in tables:
            try:
                cursor.execute(f"DELETE FROM {table}")
                print(f"   - Cleared {cursor.rowcount} rows from {table}")
            except:
                pass
        conn.commit()
        conn.close()
        clear_file_tracking()
        print("✅ Everything cleared including users!\n")
    else:
        print("❌ Invalid choice")
        return
    
    # Show final stats
    show_current_stats()
    
    print("✅ Done!")

if __name__ == "__main__":
    main()