import sqlite3

conn = sqlite3.connect('building_inspection.db')
cursor = conn.cursor()

# Add column if doesn't exist
try:
    cursor.execute("ALTER TABLE inspector_csv_processing_log ADD COLUMN file_checksum TEXT")
    conn.commit()
    print("✓ Added file_checksum column")
except sqlite3.OperationalError as e:
    if "duplicate column" in str(e):
        print("✓ Column already exists")
    else:
        raise

conn.close()