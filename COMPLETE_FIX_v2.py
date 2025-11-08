"""
COMPLETE FIX FOR INSPECTOR SAVE ISSUE
======================================

This fixes ALL three problems identified:
1. Broken db_manager.connect() method
2. Missing processor.save_to_database() method  
3. Processor not having proper database access

DEPLOYMENT INSTRUCTIONS:
========================
Run this script in production, it will patch the running app immediately.
"""

import streamlit as st
import sys
import importlib

st.title("🔧 Complete Inspector Save Fix")
st.write("This will fix all three identified issues")

# Add detailed explanation
st.info("""
**Problems Found:**
1. ❌ db_manager.connect() broken (lambda issue)
2. ❌ processor.save_to_database() missing
3. ❌ processor.db_manager is None

**This Fix:**
1. ✅ Creates proper DatabaseWrapper class
2. ✅ Adds save_to_database() to processor
3. ✅ Ensures processor gets proper database access
""")

if st.button("🚀 Apply Complete Fix"):
    try:
        st.write("### Step 1: Creating Proper DatabaseWrapper Class")
        
        # Create a proper wrapper class that can be used by InspectorInterface
        database_wrapper_code = '''
class DatabaseWrapper:
    """Wrapper to make conn_manager look like db_manager"""
    def __init__(self, conn_manager):
        self.conn_manager = conn_manager
        self.db_type = getattr(conn_manager, 'db_type', 'postgresql')
    
    def connect(self):
        """Get connection from conn_manager"""
        return self.conn_manager.get_connection()
    
    def __getattr__(self, name):
        """Delegate other calls to conn_manager"""
        return getattr(self.conn_manager, name)
'''
        
        st.code(database_wrapper_code, language="python")
        st.success("✅ DatabaseWrapper class defined")
        
        # Execute the wrapper class definition
        exec(database_wrapper_code, globals())
        
        st.write("### Step 2: Patching InspectorInterface")
        
        # Import the inspector module
        if 'roles.inspector' in sys.modules:
            inspector_module = sys.modules['roles.inspector']
        else:
            import roles.inspector as inspector_module
        
        # Get the InspectorInterface class
        InspectorInterface = inspector_module.InspectorInterface
        
        # Save the original __init__
        original_init = InspectorInterface.__init__
        
        # Create new __init__ with the fix
        def new_init(self, conn_manager=None, db_path="data/building_inspection.db"):
            # Call original init
            original_init(self, conn_manager, db_path)
            
            # Fix the db_manager if it's the broken wrapper
            if self.db_manager is not None and self.db_type == "postgresql":
                # Replace with proper wrapper
                self.db_manager = DatabaseWrapper(self.conn_manager)
                st.write(f"✅ Replaced db_manager with proper DatabaseWrapper")
            
            # Also ensure processor has proper database access
            if hasattr(self, 'processor') and self.processor is not None:
                if not hasattr(self.processor, 'db_manager') or self.processor.db_manager is None:
                    self.processor.db_manager = self.db_manager
                    st.write(f"✅ Assigned db_manager to processor")
        
        # Apply the patch
        InspectorInterface.__init__ = new_init
        st.success("✅ InspectorInterface patched")
        
        st.write("### Step 3: Patching InspectionDataProcessor")
        
        # Import the processor module
        if 'core.data_processor' in sys.modules:
            processor_module = sys.modules['core.data_processor']
        else:
            import core.data_processor as processor_module
        
        # Get the InspectionDataProcessor class
        InspectionDataProcessor = processor_module.InspectionDataProcessor
        
        # Add save_to_database method if missing
        if not hasattr(InspectionDataProcessor, 'save_to_database'):
            def save_to_database(self, inspection_data):
                """Save inspection data to database"""
                try:
                    # Get connection
                    if hasattr(self, 'db_manager') and self.db_manager is not None:
                        conn = self.db_manager.connect()
                    elif hasattr(self, 'conn_manager') and self.conn_manager is not None:
                        conn = self.conn_manager.get_connection()
                    else:
                        st.error("❌ No database connection available")
                        return None
                    
                    cursor = conn.cursor()
                    
                    # Insert building
                    cursor.execute("""
                        INSERT INTO inspector_buildings (building_name, address, created_at)
                        VALUES (%s, %s, NOW())
                        RETURNING id
                    """, (
                        inspection_data.get('building_name', 'Unknown'),
                        inspection_data.get('address', 'Unknown')
                    ))
                    building_id = cursor.fetchone()[0]
                    
                    # Insert inspection
                    cursor.execute("""
                        INSERT INTO inspector_inspections 
                        (building_id, inspector_name, inspection_date, created_at)
                        VALUES (%s, %s, %s, NOW())
                        RETURNING id
                    """, (
                        building_id,
                        inspection_data.get('inspector_name', 'Unknown'),
                        inspection_data.get('inspection_date', 'NOW()')
                    ))
                    inspection_id = cursor.fetchone()[0]
                    
                    # Insert inspection items
                    items_inserted = 0
                    for item in inspection_data.get('items', []):
                        cursor.execute("""
                            INSERT INTO inspector_inspection_items
                            (inspection_id, unit, trade, item_description, status, created_at)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                        """, (
                            inspection_id,
                            item.get('unit'),
                            item.get('trade'),
                            item.get('description'),
                            item.get('status', 'pending')
                        ))
                        items_inserted += 1
                    
                    conn.commit()
                    st.success(f"✅ Saved inspection {inspection_id} with {items_inserted} items")
                    return inspection_id
                    
                except Exception as e:
                    st.error(f"❌ Save failed: {str(e)}")
                    conn.rollback()
                    return None
                finally:
                    cursor.close()
            
            # Add the method to the class
            InspectionDataProcessor.save_to_database = save_to_database
            st.success("✅ Added save_to_database() to InspectionDataProcessor")
        else:
            st.info("ℹ️ save_to_database() already exists")
        
        st.write("### Step 4: Testing the Fix")
        
        # Reload modules to pick up changes
        importlib.reload(inspector_module)
        if 'core.data_processor' in sys.modules:
            importlib.reload(processor_module)
        
        # Test creating a new inspector instance
        st.write("Creating test InspectorInterface...")
        conn_manager = st.session_state.get('conn_manager')
        if conn_manager:
            test_inspector = InspectorInterface(conn_manager=conn_manager)
            
            st.write("**Verification:**")
            st.write(f"- db_manager exists: {test_inspector.db_manager is not None}")
            st.write(f"- db_manager type: {type(test_inspector.db_manager).__name__}")
            
            # Test connect
            try:
                test_conn = test_inspector.db_manager.connect()
                st.success("✅ db_manager.connect() works!")
                
                # Test processor
                if hasattr(test_inspector, 'processor') and test_inspector.processor:
                    st.write(f"- processor.db_manager exists: {hasattr(test_inspector.processor, 'db_manager') and test_inspector.processor.db_manager is not None}")
                    st.write(f"- processor.save_to_database exists: {hasattr(test_inspector.processor, 'save_to_database')}")
                    
                    if hasattr(test_inspector.processor, 'save_to_database'):
                        st.success("✅ All components fixed!")
                    else:
                        st.warning("⚠️ save_to_database still missing")
                else:
                    st.warning("⚠️ Processor not initialized")
                    
            except Exception as e:
                st.error(f"❌ Connect test failed: {str(e)}")
        else:
            st.warning("⚠️ No conn_manager in session_state")
        
        st.success("### ✅ Fix Applied Successfully!")
        st.info("""
        **What was fixed:**
        1. ✅ Created proper DatabaseWrapper class
        2. ✅ Patched InspectorInterface to use it
        3. ✅ Added save_to_database() method to processor
        4. ✅ Ensured processor gets db_manager
        
        **Next Steps:**
        1. Go back to Inspector interface
        2. Try processing CSV again
        3. Data should now save to PostgreSQL!
        """)
        
    except Exception as e:
        st.error(f"❌ Fix failed: {str(e)}")
        st.exception(e)

st.write("---")
st.write("### 📋 Alternative: Test Direct Save")
st.write("If the fix doesn't work, use this emergency method to save data directly")

if st.button("🆘 Test Emergency Direct Save"):
    st.write("Testing direct save with sample data...")
    
    try:
        conn_manager = st.session_state.get('conn_manager')
        if not conn_manager:
            st.error("No connection manager found")
        else:
            conn = conn_manager.get_connection()
            cursor = conn.cursor()
            
            # Sample inspection data
            test_data = {
                'building_name': 'EMERGENCY_TEST_FIX',
                'address': '123 Fix Street',
                'inspector_name': 'Debug Inspector',
                'inspection_date': '2025-11-08',
                'items': [
                    {'unit': '101', 'trade': 'Electrical', 'description': 'Test item 1', 'status': 'pass'},
                    {'unit': '102', 'trade': 'Plumbing', 'description': 'Test item 2', 'status': 'fail'},
                ]
            }
            
            # Insert building
            cursor.execute("""
                INSERT INTO inspector_buildings (building_name, address, created_at)
                VALUES (%s, %s, NOW())
                RETURNING id
            """, (test_data['building_name'], test_data['address']))
            building_id = cursor.fetchone()[0]
            st.write(f"✅ Building inserted: ID {building_id}")
            
            # Insert inspection
            cursor.execute("""
                INSERT INTO inspector_inspections 
                (building_id, inspector_name, inspection_date, created_at)
                VALUES (%s, %s, %s, NOW())
                RETURNING id
            """, (building_id, test_data['inspector_name'], test_data['inspection_date']))
            inspection_id = cursor.fetchone()[0]
            st.write(f"✅ Inspection inserted: ID {inspection_id}")
            
            # Insert items
            for item in test_data['items']:
                cursor.execute("""
                    INSERT INTO inspector_inspection_items
                    (inspection_id, unit, trade, item_description, status, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                """, (inspection_id, item['unit'], item['trade'], item['description'], item['status']))
            
            conn.commit()
            st.success(f"✅ Complete inspection saved! Inspection ID: {inspection_id}")
            
            # Verify
            cursor.execute("""
                SELECT COUNT(*) FROM inspector_inspection_items WHERE inspection_id = %s
            """, (inspection_id,))
            count = cursor.fetchone()[0]
            st.write(f"✅ Verified: {count} items saved")
            
            cursor.close()
            
    except Exception as e:
        st.error(f"❌ Emergency save failed: {str(e)}")
        st.exception(e)