"""
Clear all inspector inspection data from database
"""
import sqlite3

conn = sqlite3.connect('building_inspection.db')
cursor = conn.cursor()

print("Clearing all inspector data...")

# Delete in correct order (respecting foreign keys)
cursor.execute("DELETE FROM inspector_metrics_summary")
cursor.execute("DELETE FROM inspector_project_progress")
cursor.execute("DELETE FROM inspector_work_orders")
cursor.execute("DELETE FROM inspector_unit_inspections")
cursor.execute("DELETE FROM inspector_inspection_items")
cursor.execute("DELETE FROM inspector_inspections")
cursor.execute("DELETE FROM inspector_buildings")
cursor.execute("DELETE FROM inspector_csv_processing_log")

conn.commit()
conn.close()

print("✓ All inspector data deleted")