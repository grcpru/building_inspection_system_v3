# minimal_test.py - Save this file and run it directly

import pandas as pd
import sys
import os
from datetime import datetime
import uuid

# Add your project path if needed
sys.path.append('.')

try:
    from database.setup import DatabaseManager
    print("✓ DatabaseManager imported successfully")
except ImportError as e:
    print(f"✗ Import error: {e}")
    exit(1)

# Create test data
test_data = pd.DataFrame({
    'Unit': ['Unit01', 'Unit02'],
    'UnitType': ['1-Bedroom', '2-Bedroom'], 
    'Room': ['Kitchen', 'Bathroom'],
    'Component': ['Cabinets', 'Tiles'],
    'Trade': ['Carpentry', 'Tiling'],
    'StatusClass': ['Not OK', 'OK'],
    'Urgency': ['Normal', 'High Priority'],
    'PlannedCompletion': [datetime.now().date(), datetime.now().date()]
})

test_metrics = {
    'building_name': 'Test Building',
    'address': 'Test Address',
    'inspection_date': datetime.now().strftime("%Y-%m-%d"),
    'unit_types_str': '1-Bedroom, 2-Bedroom',
    'total_units': 2,
    'total_defects': 1,
    'defect_rate': 50.0,
    'ready_units': 1,
    'ready_pct': 50.0,
    'urgent_defects': 0,
    'high_priority_defects': 1,
    'avg_defects_per_unit': 0.5
}

print("✓ Test data created")

# Test database operations
try:
    db_manager = DatabaseManager()
    print("✓ DatabaseManager created")
    
    # Test connection
    conn = db_manager.connect()
    print("✓ Database connection established")
    
    # Test save operation
    print("Testing save operation...")
    inspection_id = db_manager.save_inspector_data(
        test_data, 
        test_metrics, 
        "Test Inspector",
        "test_file.csv"
    )
    print(f"✓ Save completed! Inspection ID: {inspection_id}")
    
    # Verify save
    cursor = conn.cursor()
    cursor.execute("SELECT original_filename, inspector_name FROM inspector_inspections WHERE id = ?", (inspection_id,))
    result = cursor.fetchone()
    
    if result:
        print(f"✓ Verification successful! File: {result[0]}, Inspector: {result[1]}")
    else:
        print("✗ Verification failed - record not found")
    
    # Check total real records
    cursor.execute("SELECT COUNT(*) FROM inspector_inspections WHERE original_filename IS NOT NULL")
    real_count = cursor.fetchone()[0]
    print(f"✓ Total real inspections in database: {real_count}")
    
except Exception as e:
    print(f"✗ Error during test: {e}")
    import traceback
    traceback.print_exc()

print("\nTest completed.")