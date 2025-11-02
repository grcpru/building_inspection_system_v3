"""
File Storage Manager for Building Inspection System
===================================================
PostgreSQL & SQLite compatible file storage with database tracking
Matches YOUR actual database structure (6 columns)

USAGE:
------
from core.file_storage import FileStorageManager

# Initialize
file_mgr = FileStorageManager(conn_manager)

# Save files
saved = file_mgr.save_files(
    work_order_id="abc-123",
    uploaded_files=streamlit_files,
    uploaded_by="John Builder",
    category="progress"
)

# Get files
files = file_mgr.get_files("abc-123")

# Get count
count = file_mgr.get_file_count("abc-123")
"""

import os
import uuid
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class FileStorageManager:
    """
    Manages file storage for work orders with database integration.
    
    Features:
    - Stores files on disk (not in database)
    - Tracks metadata in database
    - Works with both PostgreSQL and SQLite
    - Simple 6-column structure
    - Organized folder structure
    """
    
    def __init__(self, conn_manager, base_path: str = "uploads"):
        """
        Initialize file storage manager.
        
        Args:
            conn_manager: Database connection manager
            base_path: Base directory for file uploads (default: "uploads")
        """
        self.conn_manager = conn_manager
        self.db_type = conn_manager.db_type
        self.base_path = Path(base_path)
        
        # Create directory structure
        self.work_orders_path = self.base_path / "work_orders"
        self.work_orders_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"✅ File storage initialized: {self.work_orders_path} ({self.db_type.upper()})")
        
        # Ensure database table exists
        self._ensure_file_table_exists()
    
    def _ensure_file_table_exists(self):
        """Create work_order_files table if it doesn't exist - same for both DBs"""
        try:
            conn = self.conn_manager.get_connection()
            cursor = conn.cursor()
            
            if self.db_type == "postgresql":
                # PostgreSQL version
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS work_order_files (
                        id TEXT PRIMARY KEY,
                        work_order_id TEXT NOT NULL,
                        original_filename TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        file_type TEXT,
                        uploaded_at TIMESTAMP DEFAULT NOW()
                    );
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_work_order_files_wo_id 
                    ON work_order_files(work_order_id);
                """)
                
            else:  # SQLite
                # SQLite version (matches your current structure)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS work_order_files (
                        id TEXT PRIMARY KEY,
                        work_order_id TEXT NOT NULL,
                        original_filename TEXT NOT NULL,
                        file_path TEXT NOT NULL,
                        file_type TEXT,
                        uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_work_order_files_wo_id 
                    ON work_order_files(work_order_id);
                """)
            
            conn.commit()
            cursor.close()
            logger.info(f"✅ File table ready ({self.db_type})")
            
        except Exception as e:
            logger.error(f"❌ Error creating file table: {e}")
    
    def save_files(self, work_order_id: str, uploaded_files: List, 
                   uploaded_by: str = "Builder", 
                   category: str = "progress") -> List[Dict]:
        """
        Save uploaded files to disk and database.
        
        Args:
            work_order_id: ID of the work order
            uploaded_files: List of Streamlit uploaded file objects
            uploaded_by: Name of user uploading
            category: Category (progress, before, after, completion)
        
        Returns:
            List of saved file info dictionaries with keys:
            - id: File ID
            - filename: Stored filename
            - original_filename: Original filename
            - path: Full path on disk
            - type: MIME type
            - size: Size in bytes
        """
        if not uploaded_files:
            return []
        
        # Create work order directory
        wo_dir = self.work_orders_path / str(work_order_id)
        wo_dir.mkdir(parents=True, exist_ok=True)
        
        saved_files = []
        
        try:
            conn = self.conn_manager.get_connection()
            cursor = conn.cursor()
            
            for uploaded_file in uploaded_files:
                try:
                    # Read file content
                    file_content = uploaded_file.read()
                    uploaded_file.seek(0)  # Reset for potential re-read
                    
                    # Generate unique filename
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    file_ext = Path(uploaded_file.name).suffix
                    unique_id = uuid.uuid4().hex[:6]
                    
                    # Format: category_timestamp_uniqueid.ext
                    new_filename = f"{category}_{timestamp}_{unique_id}{file_ext}"
                    file_path = wo_dir / new_filename
                    
                    # Save to disk
                    with open(file_path, "wb") as f:
                        f.write(file_content)
                    
                    # Save metadata to database
                    file_id = str(uuid.uuid4())
                    
                    if self.db_type == "postgresql":
                        cursor.execute("""
                            INSERT INTO work_order_files 
                            (id, work_order_id, original_filename, file_path, file_type, uploaded_at)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                        """, (
                            file_id, work_order_id, uploaded_file.name,
                            str(file_path), uploaded_file.type
                        ))
                    else:  # SQLite
                        cursor.execute("""
                            INSERT INTO work_order_files 
                            (id, work_order_id, original_filename, file_path, file_type, uploaded_at)
                            VALUES (?, ?, ?, ?, ?, datetime('now'))
                        """, (
                            file_id, work_order_id, uploaded_file.name,
                            str(file_path), uploaded_file.type
                        ))
                    
                    saved_files.append({
                        'id': file_id,
                        'filename': new_filename,
                        'original_filename': uploaded_file.name,
                        'path': str(file_path),
                        'type': uploaded_file.type,
                        'size': len(file_content)
                    })
                    
                    logger.info(f"✅ Saved: {uploaded_file.name} → {new_filename}")
                    
                except Exception as file_error:
                    logger.error(f"❌ Error saving file {uploaded_file.name}: {file_error}")
                    continue
            
            conn.commit()
            cursor.close()
            
            logger.info(f"Saved {len(saved_files)} file(s) for work order {work_order_id}")
            return saved_files
            
        except Exception as e:
            logger.error(f"❌ Error in save_files: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return saved_files  # Return what we managed to save
    
    def get_files(self, work_order_id: str, 
                  category: Optional[str] = None) -> List[Dict]:
        """
        Get all files for a work order.
        
        Args:
            work_order_id: Work order ID
            category: Optional category filter (not used, for compatibility)
        
        Returns:
            List of file metadata dictionaries with keys:
            - id, original_filename, file_path, file_type, uploaded_at
        """
        try:
            conn = self.conn_manager.get_connection()
            cursor = conn.cursor()
            
            if self.db_type == "postgresql":
                query = """
                    SELECT id, work_order_id, original_filename, file_path, 
                           file_type, uploaded_at
                    FROM work_order_files
                    WHERE work_order_id = %s
                    ORDER BY uploaded_at DESC
                """
                cursor.execute(query, (work_order_id,))
            else:  # SQLite
                query = """
                    SELECT id, work_order_id, original_filename, file_path, 
                           file_type, uploaded_at
                    FROM work_order_files
                    WHERE work_order_id = ?
                    ORDER BY uploaded_at DESC
                """
                cursor.execute(query, (work_order_id,))
            
            rows = cursor.fetchall()
            cursor.close()
            
            files = []
            for row in rows:
                if isinstance(row, dict):
                    # PostgreSQL with RealDictCursor
                    files.append({
                        'id': row['id'],
                        'work_order_id': row['work_order_id'],
                        'original_filename': row['original_filename'],
                        'file_path': row['file_path'],
                        'file_type': row['file_type'],
                        'uploaded_at': row.get('uploaded_at'),
                        'created_at': row.get('uploaded_at'),  # Alias
                    })
                else:
                    # SQLite or standard cursor
                    files.append({
                        'id': row[0],
                        'work_order_id': row[1],
                        'original_filename': row[2],
                        'file_path': row[3],
                        'file_type': row[4],
                        'uploaded_at': row[5] if len(row) > 5 else None,
                        'created_at': row[5] if len(row) > 5 else None,
                    })
            
            logger.info(f"Retrieved {len(files)} files for work order {work_order_id}")
            return files
            
        except Exception as e:
            logger.error(f"Error getting files: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def get_file_count(self, work_order_id: str) -> int:
        """Get count of files for a work order"""
        try:
            conn = self.conn_manager.get_connection()
            cursor = conn.cursor()
            
            if self.db_type == "postgresql":
                cursor.execute("""
                    SELECT COUNT(*) FROM work_order_files WHERE work_order_id = %s
                """, (work_order_id,))
            else:
                cursor.execute("""
                    SELECT COUNT(*) FROM work_order_files WHERE work_order_id = ?
                """, (work_order_id,))
            
            result = cursor.fetchone()
            count = result[0] if result else 0
            
            cursor.close()
            return count
            
        except Exception as e:
            logger.error(f"Error getting file count: {e}")
            return 0
    
    def delete_file(self, file_id: str) -> bool:
        """
        Delete a file from both disk and database.
        
        Args:
            file_id: File ID
        
        Returns:
            True if successful
        """
        try:
            conn = self.conn_manager.get_connection()
            cursor = conn.cursor()
            
            # Get file path first
            if self.db_type == "postgresql":
                cursor.execute("SELECT file_path FROM work_order_files WHERE id = %s", (file_id,))
            else:
                cursor.execute("SELECT file_path FROM work_order_files WHERE id = ?", (file_id,))
            
            result = cursor.fetchone()
            if not result:
                return False
            
            file_path = Path(result[0] if isinstance(result, tuple) else result['file_path'])
            
            # Delete from disk
            if file_path.exists():
                file_path.unlink()
                logger.info(f"🗑️ Deleted file from disk: {file_path}")
            
            # Delete from database
            if self.db_type == "postgresql":
                cursor.execute("DELETE FROM work_order_files WHERE id = %s", (file_id,))
            else:
                cursor.execute("DELETE FROM work_order_files WHERE id = ?", (file_id,))
            
            conn.commit()
            cursor.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            return False
    
    def cleanup_orphaned_files(self) -> Tuple[int, int]:
        """
        Clean up files that exist on disk but not in database, and vice versa.
        
        Returns:
            Tuple of (disk_cleaned, db_cleaned) counts
        """
        disk_cleaned = 0
        db_cleaned = 0
        
        try:
            conn = self.conn_manager.get_connection()
            cursor = conn.cursor()
            
            # Get all file paths from database
            cursor.execute("SELECT id, file_path FROM work_order_files")
            rows = cursor.fetchall()
            
            db_files = {}
            for row in rows:
                if isinstance(row, dict):
                    db_files[row['id']] = row['file_path']
                else:
                    db_files[row[0]] = row[1]
            
            # Check database entries
            for file_id, file_path in db_files.items():
                if not Path(file_path).exists():
                    # File missing from disk, remove from database
                    if self.db_type == "postgresql":
                        cursor.execute("DELETE FROM work_order_files WHERE id = %s", (file_id,))
                    else:
                        cursor.execute("DELETE FROM work_order_files WHERE id = ?", (file_id,))
                    db_cleaned += 1
            
            # Check disk files
            valid_paths = set(db_files.values())
            for wo_dir in self.work_orders_path.iterdir():
                if wo_dir.is_dir():
                    for file_path in wo_dir.iterdir():
                        if file_path.is_file() and str(file_path) not in valid_paths:
                            # File on disk but not in database
                            file_path.unlink()
                            disk_cleaned += 1
            
            conn.commit()
            cursor.close()
            
            logger.info(f"🧹 Cleanup complete: {disk_cleaned} disk files, {db_cleaned} database entries")
            return disk_cleaned, db_cleaned
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return disk_cleaned, db_cleaned
    
    def get_storage_stats(self) -> dict:
        """Get storage statistics"""
        try:
            conn = self.conn_manager.get_connection()
            cursor = conn.cursor()
            
            # Total files
            cursor.execute("SELECT COUNT(*) FROM work_order_files")
            total_files = cursor.fetchone()[0]
            
            # Total disk usage
            total_size = 0
            file_count_disk = 0
            
            if self.work_orders_path.exists():
                for wo_dir in self.work_orders_path.iterdir():
                    if wo_dir.is_dir():
                        for file_path in wo_dir.iterdir():
                            if file_path.is_file():
                                total_size += file_path.stat().st_size
                                file_count_disk += 1
            
            cursor.close()
            
            return {
                'total_files_db': total_files,
                'total_files_disk': file_count_disk,
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
            }
            
        except Exception as e:
            logger.error(f"Error getting storage stats: {e}")
            return {
                'total_files_db': 0,
                'total_files_disk': 0,
                'total_size_bytes': 0,
                'total_size_mb': 0,
            }


if __name__ == "__main__":
    print("""
File Storage Manager for Building Inspection System
===================================================

✅ PostgreSQL & SQLite compatible
✅ Stores files on disk (fast, scalable)
✅ Tracks metadata in database (searchable)
✅ Simple 6-column structure

Storage Structure:
uploads/work_orders/{work_order_id}/{category}_{timestamp}_{id}.ext

Table Structure (6 columns):
- id (TEXT PRIMARY KEY)
- work_order_id (TEXT)
- original_filename (TEXT)
- file_path (TEXT)
- file_type (TEXT)
- uploaded_at (TIMESTAMP)

Ready for production!
    """)