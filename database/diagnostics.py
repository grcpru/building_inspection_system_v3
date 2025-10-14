"""
Database Diagnostics Script
Run this to verify database connection and table existence
"""

from database.connection_manager import get_connection_manager

def run_diagnostics():
    """Run comprehensive database diagnostics"""
    
    print("\n" + "="*60)
    print("🔍 DATABASE DIAGNOSTICS")
    print("="*60 + "\n")
    
    conn_manager = get_connection_manager()
    conn = conn_manager.get_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Check database connection
        print("1️⃣ Testing database connection...")
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"   ✅ Connected to: {version}")
        print()
        
        # 2. Check current database and schema
        print("2️⃣ Checking current database and schema...")
        cursor.execute("SELECT current_database(), current_schema();")
        db_info = cursor.fetchone()
        print(f"   Database: {db_info}")
        print()
        
        # 3. List all tables in public schema
        print("3️⃣ Tables in 'public' schema:")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        if tables:
            for table in tables:
                table_name = table[0] if isinstance(table, (list, tuple)) else table.get('table_name', table)
                print(f"   📋 {table_name}")
        else:
            print("   ⚠️  No tables found!")
        print()
        
        # 4. Check inspector_inspections table specifically
        print("4️⃣ Checking inspector_inspections table...")
        cursor.execute("""
            SELECT COUNT(*) as table_count
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'inspector_inspections';
        """)
        result = cursor.fetchone()
        print(f"   Raw result: {result}")
        print(f"   Result type: {type(result)}")
        
        # Try different access methods
        if result:
            if isinstance(result, dict):
                count = result.get('table_count', result.get('count', 0))
                print(f"   Dict access: count={count}")
            elif isinstance(result, (list, tuple)):
                count = result[0]
                print(f"   Index access: count={count}")
        print()
        
        # 5. Count rows in key tables (if they exist)
        print("5️⃣ Row counts in key tables:")
        key_tables = [
            'inspector_inspections',
            'inspector_buildings',
            'inspector_inspection_items',
            'inspector_work_orders',
            'users'
        ]
        
        for table in key_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) as row_count FROM {table};")
                row_result = cursor.fetchone()
                
                # Handle different result types
                if row_result:
                    if isinstance(row_result, dict):
                        row_count = row_result.get('row_count', row_result.get('count', 0))
                    elif isinstance(row_result, (list, tuple)):
                        row_count = row_result[0]
                    else:
                        row_count = "Unknown"
                    print(f"   📊 {table}: {row_count} rows")
            except Exception as e:
                print(f"   ❌ {table}: Table doesn't exist or error: {e}")
        print()
        
        # 6. Check for data in inspector_inspections
        print("6️⃣ Sample data from inspector_inspections:")
        try:
            cursor.execute("""
                SELECT id, building_id, inspection_date, inspector_name 
                FROM inspector_inspections 
                LIMIT 3;
            """)
            samples = cursor.fetchall()
            if samples:
                for sample in samples:
                    print(f"   📝 {sample}")
            else:
                print("   ⚠️  No data found")
        except Exception as e:
            print(f"   ❌ Error: {e}")
        print()
        
        # 7. Check all schemas (maybe data is in wrong schema?)
        print("7️⃣ All available schemas:")
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            ORDER BY schema_name;
        """)
        schemas = cursor.fetchall()
        for schema in schemas:
            schema_name = schema[0] if isinstance(schema, (list, tuple)) else schema.get('schema_name', schema)
            print(f"   📁 {schema_name}")
        print()
        
        print("="*60)
        print("✅ DIAGNOSTICS COMPLETE")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"❌ Diagnostic error: {e}")
        import traceback
        print(traceback.format_exc())
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_diagnostics()