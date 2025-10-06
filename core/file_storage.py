"""
File Storage System for Work Orders
===================================

Handles file uploads, storage, and retrieval for builder work orders.
"""

import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class WorkOrderFileManager:
    """Manages file uploads and storage for work orders"""
    
    def __init__(self, base_upload_path: str = "uploads/work_orders"):
        self.base_upload_path = Path(base_upload_path)
        self.base_upload_path.mkdir(parents=True, exist_ok=True)
    
    def save_files(self, work_order_id: str, uploaded_files: list, 
                   db_manager) -> Tuple[bool, List[str]]:
        """
        Save uploaded files to disk and database.
        
        Args:
            work_order_id: ID of the work order
            uploaded_files: List of Streamlit UploadedFile objects
            db_manager: Database manager instance
            
        Returns:
            Tuple of (success: bool, saved_file_names: List[str])
        """
        if not uploaded_files:
            return True, []
        
        saved_files = []
        work_order_dir = self.base_upload_path / work_order_id
        work_order_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            conn = db_manager.connect()
            cursor = conn.cursor()
            
            for file in uploaded_files:
                # Generate unique filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_extension = Path(file.name).suffix
                unique_name = f"{timestamp}_{uuid.uuid4().hex[:8]}{file_extension}"
                
                # Save file to disk
                file_path = work_order_dir / unique_name
                with open(file_path, "wb") as f:
                    f.write(file.getbuffer())
                
                # Get file info
                file_size = file.size
                file_type = file.type or 'application/octet-stream'
                
                # Save to database
                file_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO work_order_files (
                        id, work_order_id, original_filename, stored_filename,
                        file_path, file_size, file_type, uploaded_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    file_id, work_order_id, file.name, unique_name,
                    str(file_path), file_size, file_type, datetime.now()
                ))
                
                saved_files.append(file.name)
                logger.info(f"Saved file: {file.name} -> {unique_name}")
            
            conn.commit()
            return True, saved_files
            
        except Exception as e:
            logger.error(f"Error saving files: {e}")
            # Clean up any partially saved files
            if work_order_dir.exists():
                shutil.rmtree(work_order_dir, ignore_errors=True)
            return False, []
    
    def get_files(self, work_order_id: str, db_manager) -> List[dict]:
        """
        Get all files for a work order.
        
        Returns:
            List of dicts with file info: {id, original_filename, file_path, file_type, uploaded_at}
        """
        try:
            conn = db_manager.connect()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, original_filename, file_path, file_type, file_size, uploaded_at
                FROM work_order_files
                WHERE work_order_id = ?
                ORDER BY uploaded_at DESC
            """, (work_order_id,))
            
            rows = cursor.fetchall()
            
            files = []
            for row in rows:
                files.append({
                    'id': row[0],
                    'original_filename': row[1],
                    'file_path': row[2],
                    'file_type': row[3],
                    'file_size': row[4],
                    'uploaded_at': row[5]
                })
            
            return files
            
        except Exception as e:
            logger.error(f"Error getting files: {e}")
            return []
    
    def file_exists(self, file_path: str) -> bool:
        """Check if file exists on disk"""
        return Path(file_path).exists()
    
    def get_file_bytes(self, file_path: str) -> Optional[bytes]:
        """Read file bytes for download or preview"""
        try:
            with open(file_path, 'rb') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return None


def create_file_storage_table(db_manager):
    """Create the work_order_files table if it doesn't exist"""
    
    try:
        conn = db_manager.connect()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS work_order_files (
                id TEXT PRIMARY KEY,
                work_order_id TEXT NOT NULL,
                original_filename TEXT NOT NULL,
                stored_filename TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER,
                file_type TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (work_order_id) REFERENCES inspector_work_orders (id)
            )
        """)
        
        # Create index for faster queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_work_order_files_order_id 
            ON work_order_files(work_order_id)
        """)
        
        conn.commit()
        logger.info("Work order files table created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error creating files table: {e}")
        return False


if __name__ == "__main__":
    print("Work Order File Storage System")
    print("=" * 50)
    print("\nFeatures:")
    print("  - Save uploaded files to organized directories")
    print("  - Store file metadata in database")
    print("  - Retrieve files for preview/download")
    print("  - Clean file naming with timestamps")
    print("  - Support for images, PDFs, and other files")
    print("\nReady for integration with Builder interface!")