"""
🎯 COMPREHENSIVE END-TO-END TEST
=================================

This tests the COMPLETE flow from connection manager → processor → save.
"""

import streamlit as st
import pandas as pd
from datetime import datetime

st.title("🎯 Complete Flow Test")

st.info("""
This will test the ENTIRE save flow:
1. Connection Manager
2. InspectorInterface creation
3. Processor creation
4. Test CSV data preparation
5. Actual save to database
""")

if st.button("🚀 Run Complete Flow Test"):
    
    # ============================================
    # STEP 1: Connection Manager
    # ============================================
    st.write("### Step 1: Connection Manager")
    try:
        from database.connection_manager import get_connection_manager
        conn_manager = get_connection_manager()
        st.success(f"✅ Connection Manager created: {conn_manager.db_type}")
        st.write(f"- Database type: **{conn_manager.db_type}**")
    except Exception as e:
        st.error(f"❌ Connection Manager failed: {e}")
        st.stop()
    
    # Test connection
    try:
        conn = conn_manager.get_connection()
        cursor = conn.cursor()
        
        if conn_manager.db_type == "postgresql":
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            st.write(f"- PostgreSQL version: {version[:50]}...")
        else:
            cursor.execute("SELECT sqlite_version()")
            version = cursor.fetchone()[0]
            st.write(f"- SQLite version: {version}")
        
        cursor.close()
        conn.close()
        st.success("✅ Connection test passed")
    except Exception as e:
        st.error(f"❌ Connection test failed: {e}")
        st.stop()
    
    # ============================================
    # STEP 2: InspectorInterface
    # ============================================
    st.write("### Step 2: InspectorInterface")
    try:
        from roles.inspector import InspectorInterface
        
        st.write("Creating InspectorInterface with conn_manager...")
        inspector = InspectorInterface(conn_manager=conn_manager)
        
        st.success("✅ InspectorInterface created")
        st.write(f"- inspector.db_type: **{inspector.db_type}**")
        st.write(f"- inspector.conn_manager: **{inspector.conn_manager}**")
    except Exception as e:
        st.error(f"❌ InspectorInterface creation failed: {e}")
        st.exception(e)
        st.stop()
    
    # ============================================
    # STEP 3: Processor Check
    # ============================================
    st.write("### Step 3: Processor Check")
    
    if not hasattr(inspector, 'processor'):
        st.error("❌ Inspector has no processor attribute!")
        st.stop()
    
    if inspector.processor is None:
        st.error("❌ inspector.processor is None!")
        st.stop()
    
    processor = inspector.processor
    st.success("✅ Processor exists")
    
    # Check processor attributes
    st.write("**Processor attributes:**")
    st.write(f"- processor.conn_manager: **{processor.conn_manager}**")
    st.write(f"- processor.db_type: **{getattr(processor, 'db_type', 'NOT SET')}**")
    st.write(f"- processor.db_manager: **{getattr(processor, 'db_manager', 'NOT SET')}**")
    
    # Critical check
    if processor.conn_manager is None:
        st.error("🚨 CRITICAL: processor.conn_manager is None!")
        st.error("This is why data isn't saving!")
        st.info("""
        **To fix this:**
        
        In roles/inspector.py, check that you're passing conn_manager:
        
        ```python
        self.processor = InspectionDataProcessor(
            db_path, 
            conn_manager=self.conn_manager  # ← Make sure this line exists!
        )
        ```
        """)
        st.stop()
    else:
        st.success("✅ processor.conn_manager is set correctly")
    
    # ============================================
    # STEP 4: Prepare Test Data
    # ============================================
    st.write("### Step 4: Prepare Test Data")
    
    try:
        # Create minimal test CSV data
        test_data = {
            'auditName': ['08/11/2025 / 101 / TEST BUILDING'],
            'Title Page_Conducted on': ['2025-11-08'],
            'Lot Details_Lot Number': ['101'],
            'Pre-Settlement Inspection_Unit Type': ['Apartment'],
            'Pre-Settlement Inspection_Living Room_Paint': ['Not OK'],
            'Sign Off_Owner/Agent Signature_timestamp': [None]
        }
        
        test_df = pd.DataFrame(test_data)
        st.write(f"✅ Test DataFrame created: {len(test_df)} rows")
        
        # Load trade mapping
        from core.data_processor import load_master_trade_mapping
        mapping = load_master_trade_mapping()
        st.write(f"✅ Trade mapping loaded: {len(mapping)} entries")
        
        building_info = {
            'name': 'TEST_BUILDING',
            'address': 'Test Address',
            'date': '2025-11-08'
        }
        
        st.success("✅ Test data prepared")
        
    except Exception as e:
        st.error(f"❌ Test data preparation failed: {e}")
        st.exception(e)
        st.stop()
    
    # ============================================
    # STEP 5: Process Data (This calls save!)
    # ============================================
    st.write("### Step 5: Process Data (THE CRITICAL STEP)")
    
    st.write("Calling processor.process_inspection_data()...")
    st.write("This should:")
    st.write("1. Process the CSV")
    st.write("2. Prepare inspection data")
    st.write("3. Call _save_to_database_with_conn_manager()")
    st.write("4. Return inspection_id")
    
    try:
        # Calculate file hash for the test
        import hashlib
        file_hash = hashlib.md5(str(test_data).encode()).hexdigest()
        
        # Process the data - THIS IS WHERE THE SAVE SHOULD HAPPEN
        final_df, metrics, inspection_id = processor.process_inspection_data(
            df=test_df,
            mapping=mapping,
            building_info=building_info,
            inspector_name="Test Inspector",
            original_filename="test_diagnostic.csv",
            file_hash=file_hash
        )
        
        st.write("**Results:**")
        st.write(f"- final_df: {'✅ Created' if final_df is not None else '❌ None'}")
        st.write(f"- metrics: {'✅ Created' if metrics is not None else '❌ None'}")
        st.write(f"- inspection_id: **{inspection_id}**")
        
        if inspection_id:
            st.success(f"🎉 SUCCESS! Inspection saved with ID: {inspection_id}")
            st.balloons()
            
            # ============================================
            # STEP 6: Verify in Database
            # ============================================
            st.write("### Step 6: Verify in Database")
            
            try:
                conn = conn_manager.get_connection()
                cursor = conn.cursor()
                
                # Check inspection exists
                if conn_manager.db_type == "postgresql":
                    cursor.execute("""
                        SELECT COUNT(*) FROM inspector_inspections 
                        WHERE id = %s
                    """, (inspection_id,))
                else:
                    cursor.execute("""
                        SELECT COUNT(*) FROM inspector_inspections 
                        WHERE id = ?
                    """, (inspection_id,))
                
                count = cursor.fetchone()[0]
                
                if count > 0:
                    st.success(f"✅ Inspection found in database!")
                    
                    # Check items
                    if conn_manager.db_type == "postgresql":
                        cursor.execute("""
                            SELECT COUNT(*) FROM inspector_inspection_items 
                            WHERE inspection_id = %s
                        """, (inspection_id,))
                    else:
                        cursor.execute("""
                            SELECT COUNT(*) FROM inspector_inspection_items 
                            WHERE inspection_id = ?
                        """, (inspection_id,))
                    
                    items_count = cursor.fetchone()[0]
                    st.success(f"✅ {items_count} inspection items found!")
                    
                    # Show sample items
                    if conn_manager.db_type == "postgresql":
                        cursor.execute("""
                            SELECT unit_number, room, status, item_description
                            FROM inspector_inspection_items 
                            WHERE inspection_id = %s
                            LIMIT 3
                        """, (inspection_id,))
                    else:
                        cursor.execute("""
                            SELECT unit, room, status_class, component
                            FROM inspector_inspection_items 
                            WHERE inspection_id = ?
                            LIMIT 3
                        """, (inspection_id,))
                    
                    sample_items = cursor.fetchall()
                    
                    st.write("**Sample items:**")
                    for item in sample_items:
                        st.write(f"- Unit: {item[0]}, Room: {item[1]}, Status: {item[2]}")
                    
                else:
                    st.error("❌ Inspection NOT found in database!")
                    st.error("Data was processed but not saved!")
                
                cursor.close()
                conn.close()
                
            except Exception as e:
                st.error(f"❌ Database verification failed: {e}")
                st.exception(e)
            
            # Cleanup option
            if st.checkbox("🗑️ Delete test data"):
                try:
                    conn = conn_manager.get_connection()
                    cursor = conn.cursor()
                    
                    if conn_manager.db_type == "postgresql":
                        cursor.execute("DELETE FROM inspector_inspection_items WHERE inspection_id = %s", (inspection_id,))
                        cursor.execute("DELETE FROM inspector_inspections WHERE id = %s", (inspection_id,))
                    else:
                        cursor.execute("DELETE FROM inspector_inspection_items WHERE inspection_id = ?", (inspection_id,))
                        cursor.execute("DELETE FROM inspector_inspections WHERE id = ?", (inspection_id,))
                    
                    conn.commit()
                    cursor.close()
                    conn.close()
                    
                    st.success("✅ Test data deleted")
                except Exception as e:
                    st.error(f"❌ Cleanup failed: {e}")
        
        else:
            st.error("❌ SAVE FAILED: inspection_id is None")
            st.error("The process_inspection_data completed but didn't save!")
            
            st.write("### 🔍 Debugging Information")
            st.write("Check your console/logs for these messages:")
            st.code("""
            Look for:
            - "🔄 Saving to PostgreSQL using connection manager..."
            - "📊 Preparing ALL X items for database..."
            - "✅ Saved to PostgreSQL: <inspection_id>"
            
            Or error messages like:
            - "❌ PostgreSQL save failed: ..."
            """)
            
            st.info("""
            **If you don't see any save messages in the logs:**
            
            The issue is that processor.conn_manager might be None at runtime,
            even though it looks correct here.
            
            **Add this logging to data_processor.py line ~760:**
            
            ```python
            # Before: if self.conn_manager:
            
            print(f"🔍 SAVE CHECK: self.conn_manager = {self.conn_manager}")
            print(f"🔍 SAVE CHECK: self.db_type = {self.db_type}")
            
            if self.conn_manager:
                print("🔍 SAVE CHECK: Entering save block...")
                # ... existing save code
            else:
                print("❌ SAVE CHECK: conn_manager is None - SKIPPING SAVE")
            ```
            """)
        
    except Exception as e:
        st.error(f"❌ Processing failed: {e}")
        st.exception(e)
        
        st.write("### 🔍 Error Analysis")
        st.write("The error occurred during process_inspection_data()")
        st.write("Check the stack trace above to see where it failed")

st.write("---")
st.write("### 📋 Summary")

st.info("""
**This test checks the COMPLETE flow:**

1. ✅ Connection Manager works
2. ✅ InspectorInterface created with conn_manager
3. ✅ Processor has conn_manager
4. ✅ Test data prepared
5. ❓ Does process_inspection_data save correctly?
6. ❓ Does data appear in database?

If Step 5 or 6 fails, we know exactly where the problem is!
""")

st.write("---")
st.write("### 🎯 What to Look For")

st.success("""
**If ALL steps pass:**
- Your setup is 100% correct
- CSV processing SHOULD work
- If real CSV still doesn't save, the issue is in the CSV upload flow

**If Step 5 returns None:**
- Check console logs for error messages
- The save method is being called but failing
- Look for PostgreSQL errors in the logs

**If Step 6 shows 0 items:**
- Save returned inspection_id but didn't actually save
- Database transaction might have been rolled back
- Check for exceptions in the save method
""")