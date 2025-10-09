"""
Initialize Enhanced V3 Database
==============================
Run this script to set up the complete V3 database with Inspector integration
"""

from database.setup import setup_database

def main():
    print("Initializing Building Inspection System V3 Database...")
    print("This will create all tables needed for Inspector, Builder, and Developer roles.")
    
    # Initialize the enhanced database
    db_manager = setup_database(
        db_path="building_inspection.db",
        force_recreate=True,    # Creates fresh database with V3 schema
        seed_test_data=True     # Adds sample data for testing
    )
    
    print("\nDatabase initialization complete!")
    print("\nWhat was created:")
    print("- All original tables (users, inspections, defects, etc.)")
    print("- New Inspector integration tables")
    print("- Trade mappings with 70+ default entries")
    print("- Sample test data for all roles")
    print("- Performance indexes")
    print("- Default admin user (admin/admin123)")
    print("- Sample Inspector, Builder, Developer users")
    
    print("\nYou can now:")
    print("1. Run your Streamlit app")
    print("2. Login as Inspector and upload CSV files")
    print("3. Access data from Builder and Developer roles")
    print("4. Generate enhanced reports with database tracking")

if __name__ == "__main__":
    main()