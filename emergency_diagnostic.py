"""
Emergency Diagnostic - Check what's salvageable
"""

import os
import sys

def main():
    print("=" * 80)
    print("EMERGENCY DIAGNOSTIC")
    print("=" * 80)
    print()
    
    # Check Git status
    print("📂 GIT STATUS:")
    print("-" * 80)
    os.system("git branch --show-current")
    os.system("git log --oneline -5")
    print()
    
    # Check files exist
    print("📄 CRITICAL FILES:")
    print("-" * 80)
    
    files = {
        "main.py": False,
        "database/setup.py": False,
        "core/data_processor.py": False,
        "roles/inspector.py": False,
        "requirements.txt": False,
        "building_inspection.db": False,
    }
    
    for filepath in files:
        exists = os.path.exists(filepath)
        files[filepath] = exists
        status = "✅" if exists else "❌"
        
        size = ""
        if exists and os.path.isfile(filepath):
            size = f"({os.path.getsize(filepath)} bytes)"
        
        print(f"{status} {filepath} {size}")
    
    print()
    
    # Check database
    print("🗄️  DATABASE CHECK:")
    print("-" * 80)
    
    if files["building_inspection.db"]:
        try:
            import sqlite3
            conn = sqlite3.connect("building_inspection.db")
            cursor = conn.cursor()
            
            # List all tables
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            print(f"✅ Database has {len(tables)} tables")
            
            # Check inspector tables
            inspector_tables = [t for t in tables if t.startswith('inspector_')]
            print(f"✅ Inspector tables: {len(inspector_tables)}")
            
            # Check data
            if 'inspector_inspections' in tables:
                cursor.execute("SELECT COUNT(*) FROM inspector_inspections")
                count = cursor.fetchone()[0]
                print(f"✅ Inspections: {count}")
                
                if count > 0:
                    cursor.execute("""
                        SELECT id, building_id, inspector_name, created_at 
                        FROM inspector_inspections 
                        LIMIT 3
                    """)
                    print("\n   Recent inspections:")
                    for row in cursor.fetchall():
                        print(f"   - {row[2]} ({row[3]})")
            
            if 'inspector_inspection_items' in tables:
                cursor.execute("SELECT COUNT(*) FROM inspector_inspection_items")
                count = cursor.fetchone()[0]
                print(f"✅ Inspection items: {count}")
            
            if 'inspector_work_orders' in tables:
                cursor.execute("SELECT COUNT(*) FROM inspector_work_orders")
                count = cursor.fetchone()[0]
                print(f"✅ Work orders: {count}")
            
            conn.close()
            
            print("\n💾 YOUR DATA IS SAFE!")
            
        except Exception as e:
            print(f"❌ Database error: {e}")
    else:
        print("❌ No database file found")
    
    print()
    
    # Check if we can import anything
    print("🔧 IMPORT CHECK:")
    print("-" * 80)
    
    try:
        sys.path.insert(0, '.')
        import main
        print("✅ main.py can be imported")
    except Exception as e:
        print(f"❌ main.py import failed: {str(e)[:100]}")
    
    try:
        from database import setup
        print("✅ database.setup can be imported")
    except Exception as e:
        print(f"❌ database.setup import failed: {str(e)[:100]}")
    
    print()
    
    # Check requirements
    print("📦 DEPENDENCIES:")
    print("-" * 80)
    
    if os.path.exists("requirements.txt"):
        with open("requirements.txt", 'r') as f:
            lines = f.readlines()
        print(f"✅ requirements.txt has {len(lines)} lines")
        
        # Check for critical packages
        content = ''.join(lines).lower()
        critical = ['streamlit', 'pandas', 'openpyxl', 'python-docx']
        for pkg in critical:
            if pkg in content:
                print(f"   ✅ {pkg}")
            else:
                print(f"   ❌ {pkg} missing!")
    
    print()
    
    # Summary
    print("=" * 80)
    print("DIAGNOSIS:")
    print("=" * 80)
    
    if files["building_inspection.db"]:
        print("✅ Your DATA exists and appears recoverable")
    
    working_files = sum(1 for v in files.values() if v)
    total_files = len(files)
    
    if working_files >= 4:
        print("✅ Most files exist - likely fixable with targeted repairs")
        print("\n📋 NEXT STEP: Fix imports and syntax errors")
    elif working_files >= 2:
        print("⚠️  Some files missing - need to restore from Git history")
        print("\n📋 NEXT STEP: Restore missing files from Git")
    else:
        print("❌ Most files missing - need full restore")
        print("\n📋 NEXT STEP: Restore from Git or rebuild")
    
    print("=" * 80)

if __name__ == "__main__":
    main()

