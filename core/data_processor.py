"""
Building Inspection Data Processor - With Automatic Work Order Creation
=====================================================================
Complete data processor with automatic work order generation for Builder role.
Preserves all existing logic and adds seamless work order creation.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import logging
from io import StringIO
import hashlib
import uuid

# ✅ CRITICAL FIX: Import connection manager
from database.connection_manager import get_connection_manager

# Import database manager
try:
    from database.setup import DatabaseManager
    DATABASE_AVAILABLE = True
except ImportError:
    try:
        from core.database_manager import DatabaseManager
        DATABASE_AVAILABLE = True
    except ImportError:
        DATABASE_AVAILABLE = False
        logging.warning("Database manager not available - data will not be persisted")

# Set up logging
logger = logging.getLogger(__name__)

class InspectionDataProcessor:
    """Data processor with database integration and automatic work order creation"""
    
    def __init__(self, db_path: str = "building_inspection.db", conn_manager=None):
        """Initialize the data processor with database support
        
        Args:
            db_path: Path to SQLite database (for backward compatibility)
            conn_manager: Connection manager instance (for PostgreSQL support)
        """
        self.processed_data = None
        self.metrics = None
        self.building_info = {}
        
        # ✅ Use connection manager if provided, otherwise fall back to DatabaseManager
        if conn_manager:
            self.conn_manager = conn_manager
            self.db_type = conn_manager.get_db_type()
            logger.info(f"✅ Data processor initialized with {self.db_type.upper()}")
            
            # Keep db_manager for backward compatibility methods
            if DATABASE_AVAILABLE and self.db_type == "sqlite":
                self.db_manager = DatabaseManager(db_path)
            else:
                self.db_manager = None
        else:
            # Backward compatibility - use DatabaseManager
            self.conn_manager = None
            self.db_type = "sqlite"
            
            if DATABASE_AVAILABLE:
                self.db_manager = DatabaseManager(db_path)
                logger.info("Database manager initialized (legacy mode)")
            else:
                self.db_manager = None
                logger.warning("Database not available - data will only be stored in memory")
    
    def _get_connection(self):
        """Get database connection using connection manager"""
        if self.conn_manager:
            return self.conn_manager.get_connection()
        elif self.db_manager:
            return self.db_manager.connect()
        else:
            raise Exception("No database connection available")
    
    # ✅ Add this method to save inspection data using connection manager
    def _save_to_database_with_conn_manager(self, inspection_data: Dict) -> Optional[str]:
        """Save inspection to database using correct PostgreSQL schema
        
        PostgreSQL schema (from postgres_adapter.py):
        - id, inspection_id, building_id
        - unit_number (not 'unit')
        - room, component, trade  
        - status_class (not 'status')
        - urgency
        - planned_completion
        - inspection_date
        - owner_signoff_timestamp
        - original_status (not 'description')
        - item_description (separate column)
        - defect_type (separate column)
        - created_at
        
        NO unit_type column in PostgreSQL!
        """
        if not self.conn_manager:
            logger.error("❌ No connection manager available")
            return None
        
        conn = None
        cursor = None
        
        try:
            logger.info(f"📊 Starting save process...")
            logger.info(f"   Building: {inspection_data.get('building_name', 'Unknown')}")
            logger.info(f"   Total items: {len(inspection_data.get('inspection_items', []))}")
            logger.info(f"   Database type: {self.db_type}")
            
            # Get connection
            logger.info("🔌 Getting database connection...")
            conn = self._get_connection()
            logger.info("✅ Connection obtained")
            
            cursor = conn.cursor()
            logger.info("✅ Cursor created")
            
            # Generate IDs
            inspection_id = str(uuid.uuid4())
            building_id = str(uuid.uuid4())
            logger.info(f"🔑 Generated IDs - Inspection: {inspection_id[:8]}..., Building: {building_id[:8]}...")
            
            # Insert building
            logger.info(f"🏢 Inserting building record...")
            if self.db_type == "postgresql":
                cursor.execute("""
                    INSERT INTO inspector_buildings (id, name, address, total_units, created_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (id) DO NOTHING
                """, (building_id, inspection_data['building_name'], 
                    inspection_data.get('address', ''), inspection_data['total_units']))
            else:
                cursor.execute("""
                    INSERT OR IGNORE INTO inspector_buildings (id, name, address, total_units, created_at)
                    VALUES (?, ?, ?, ?, datetime('now'))
                """, (building_id, inspection_data['building_name'], 
                    inspection_data.get('address', ''), inspection_data['total_units']))
            
            logger.info(f"✅ Building record inserted")
            
            # Insert inspection
            logger.info(f"📝 Inserting inspection record...")
            if self.db_type == "postgresql":
                cursor.execute("""
                    INSERT INTO inspector_inspections 
                    (id, building_id, inspection_date, inspector_name, total_units, 
                    total_defects, ready_pct, original_filename, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (inspection_id, building_id, inspection_data['inspection_date'],
                    inspection_data['inspector_name'], inspection_data['total_units'],
                    inspection_data['total_defects'], inspection_data['ready_pct'],
                    inspection_data.get('original_filename', '')))
            else:
                cursor.execute("""
                    INSERT INTO inspector_inspections 
                    (id, building_id, inspection_date, inspector_name, total_units, 
                    total_defects, ready_pct, original_filename, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """, (inspection_id, building_id, inspection_data['inspection_date'],
                    inspection_data['inspector_name'], inspection_data['total_units'],
                    inspection_data['total_defects'], inspection_data['ready_pct'],
                    inspection_data.get('original_filename', '')))
            
            logger.info(f"✅ Inspection record inserted")
            
            # Insert inspection items in BATCHES
            items = inspection_data.get('inspection_items', [])
            if len(items) > 0:
                logger.info(f"💾 Preparing to save {len(items)} inspection items...")
                
                batch_size = 100
                total_saved = 0
                
                for i in range(0, len(items), batch_size):
                    batch = items[i:i + batch_size]
                    logger.info(f"   Processing batch {i//batch_size + 1} ({len(batch)} items)...")
                    
                    if self.db_type == "postgresql":
                        # ✅ CORRECTED: Match actual PostgreSQL schema from postgres_adapter.py
                        values = []
                        for item in batch:
                            values.append((
                                str(uuid.uuid4()),              # id
                                inspection_id,                  # inspection_id
                                building_id,                    # building_id
                                item.get('unit', ''),          # unit_number
                                item.get('room', ''),          # room
                                item.get('component', ''),     # component
                                item.get('trade', ''),         # trade
                                item.get('status', 'pending'), # status_class
                                item.get('urgency', 'Normal'), # urgency
                                item.get('planned_completion'), # planned_completion
                                item.get('inspection_date'),   # inspection_date
                                item.get('owner_signoff_timestamp'), # owner_signoff_timestamp
                                item.get('status', ''),        # original_status
                                item.get('description', ''),   # item_description
                                item.get('defect_type', '')    # defect_type
                            ))
                        
                        logger.info(f"   Executing PostgreSQL batch insert...")
                        # ✅ CORRECTED: Match postgres_adapter.py schema exactly
                        cursor.executemany("""
                            INSERT INTO inspector_inspection_items
                            (id, inspection_id, building_id, unit_number, room, component, 
                            trade, status_class, urgency, planned_completion, 
                            inspection_date, owner_signoff_timestamp, original_status,
                            item_description, defect_type, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """, values)
                    else:
                        # SQLite version (has unit_type column)
                        logger.info(f"   Executing SQLite batch insert...")
                        for item in batch:
                            cursor.execute("""
                                INSERT INTO inspector_inspection_items
                                (id, inspection_id, building_id, unit, unit_type, room, 
                                component, trade, status_class, urgency, 
                                planned_completion, inspection_date, owner_signoff_timestamp,
                                original_status, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                            """, (str(uuid.uuid4()), inspection_id, building_id,
                                item.get('unit', ''), item.get('unit_type', ''),
                                item.get('room', ''), item.get('component', ''),
                                item.get('trade', ''), item.get('status', 'pending'),
                                item.get('urgency', 'Normal'),
                                item.get('planned_completion'),
                                item.get('inspection_date'),
                                item.get('owner_signoff_timestamp'),
                                item.get('description', '')))
                    
                    total_saved += len(batch)
                    logger.info(f"   ✓ Batch complete: {total_saved}/{len(items)} items saved")
            else:
                logger.warning("⚠️ No inspection items to save")
            
            logger.info("💫 Committing transaction...")
            conn.commit()
            logger.info("✅ Transaction committed successfully")
            
            logger.info(f"✅ SAVE COMPLETE: Inspection {inspection_id[:8]}... saved to {self.db_type.upper()}")
            
            return inspection_id
            
        except Exception as e:
            logger.error(f"❌ SAVE FAILED")
            logger.error(f"❌ Error type: {type(e).__name__}")
            logger.error(f"❌ Error message: {e}")
            import traceback
            logger.error(f"❌ Full traceback:\n{traceback.format_exc()}")
            
            # Rollback on error
            if conn:
                try:
                    logger.warning("⚠️ Rolling back transaction...")
                    conn.rollback()
                    logger.info("✅ Rollback complete")
                except Exception as rollback_error:
                    logger.error(f"❌ Rollback failed: {rollback_error}")
            
            return None
            
        finally:
            # Always close cursor and connection
            if cursor:
                try:
                    cursor.close()
                    logger.info("🔌 Cursor closed")
                except:
                    pass
            if conn:
                try:
                    conn.close()
                    logger.info("🔌 Database connection closed")
                except:
                    pass
    
    def check_duplicate_file(self, file_bytes: bytes, filename: str) -> Optional[Dict]:
        """
        Check if this exact file was already processed.
        Returns dict with duplicate info if found, None otherwise.
        """
        if not self.db_manager:
            return None
        
        # Calculate file hash
        file_hash = hashlib.md5(file_bytes).hexdigest()
        
        try:
            conn = self.db_manager.connect()
            cursor = conn.cursor()
            
            # Check by hash first (most reliable)
            cursor.execute("""
                SELECT inspection_id, building_name, created_at, original_filename
                FROM inspector_csv_processing_log
                WHERE file_checksum = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (file_hash,))
            
            result = cursor.fetchone()
            
            if result:
                return {
                    'is_duplicate': True,
                    'inspection_id': result[0],
                    'building_name': result[1],
                    'processed_date': result[2],
                    'original_filename': result[3],
                    'file_hash': file_hash
                }
            
            # If no hash match, check by exact filename (less reliable but helpful)
            cursor.execute("""
                SELECT inspection_id, building_name, created_at, file_checksum
                FROM inspector_csv_processing_log
                WHERE original_filename = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (filename,))
            
            result = cursor.fetchone()
            
            if result and result[3] != file_hash:
                # Same filename but different content
                return {
                    'is_duplicate': False,
                    'warning': 'same_filename_different_content',
                    'previous_inspection_id': result[0],
                    'previous_date': result[2]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking duplicate: {e}")
            return None
    
    def _create_work_orders_from_defects(self, inspection_id: str, processed_data: pd.DataFrame) -> int:
        """
        Create work orders from defects for Builder role.
        
        Args:
            inspection_id: ID of the inspection
            processed_data: DataFrame with processed inspection data
            
        Returns:
            Count of work orders created
        """
        if not self.db_manager:
            logger.warning("Database not available - cannot create work orders")
            return 0
        
        try:
            conn = self.db_manager.connect()
            cursor = conn.cursor()
            
            # Get only defects (Not OK items)
            defects = processed_data[processed_data['StatusClass'] == 'Not OK'].copy()
            
            if len(defects) == 0:
                logger.info("No defects found - no work orders created")
                return 0
            
            # Prepare work orders for batch insert
            work_orders = []
            for _, defect in defects.iterrows():
                work_order_id = str(uuid.uuid4())
                
                # Map urgency to planned date offset and estimated hours
                if defect['Urgency'] == 'Urgent':
                    days_offset = 3
                    estimated_hours = 2.0
                elif defect['Urgency'] == 'High Priority':
                    days_offset = 7
                    estimated_hours = 4.0
                else:
                    days_offset = 14
                    estimated_hours = 3.0
                
                planned_date = datetime.now() + timedelta(days=days_offset)
                
                # Determine if photos are required (for certain trades/components)
                photo_required_trades = ['Flooring - Tiles', 'Painting', 'Waterproofing', 'Concrete']
                photos_required = str(defect['Trade']) in photo_required_trades
                
                # Initial notes from inspection
                initial_notes = f"Defect identified during inspection on {defect.get('InspectionDate', 'N/A')}"
                
                work_orders.append((
                    work_order_id,
                    inspection_id,
                    str(defect['Unit']),
                    str(defect['Trade']),
                    str(defect['Component']),
                    str(defect['Room']),
                    str(defect['Urgency']),
                    'pending',  # Initial status
                    planned_date.date(),
                    estimated_hours,
                    initial_notes,
                    photos_required,
                    datetime.now(),
                    datetime.now()
                ))
            
            # Batch insert all work orders
            cursor.executemany("""
                INSERT INTO inspector_work_orders (
                    id, inspection_id, unit, trade, component, room, 
                    urgency, status, planned_date, estimated_hours, notes,
                    photos_required, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, work_orders)
            
            conn.commit()
            
            logger.info(f"Created {len(work_orders)} work orders from {len(defects)} defects")
            
            # Log summary by urgency
            urgency_counts = defects['Urgency'].value_counts()
            for urgency, count in urgency_counts.items():
                logger.info(f"  - {urgency}: {count} work orders")
            
            return len(work_orders)
            
        except Exception as e:
            logger.error(f"Error creating work orders: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return 0

    def process_inspection_data(self, df: pd.DataFrame, mapping: pd.DataFrame, 
                    building_info: Dict[str, str], 
                    inspector_name: str = "Inspector",
                    original_filename: str = None,
                    file_hash: str = None) -> Tuple[pd.DataFrame, Dict[str, Any], Optional[str]]:
        """Process inspection data with date and signoff tracking + work order creation"""
        
        logger.info("Starting data processing with work order creation")
        
        try:
            df = df.copy()
            
            # STEP 1: Extract dates and signoffs FIRST
            logger.info("Extracting inspection dates...")
            df["InspectionDate"] = self._extract_unit_inspection_dates(df)
            
            logger.info("Extracting owner signoff timestamps...")
            df["OwnerSignoffTimestamp"] = self._extract_signoff_timestamp(df)
            
            # Log date range
            date_range = f"{df['InspectionDate'].min()} to {df['InspectionDate'].max()}"
            unique_dates = df['InspectionDate'].nunique()
            logger.info(f"Date range: {date_range} ({unique_dates} unique dates)")
            
            # Log signoff status
            signoff_count = df['OwnerSignoffTimestamp'].notna().sum()
            logger.info(f"Units with signoff: {signoff_count}/{len(df)}")
            
            # STEP 2: Extract unit numbers
            if "Lot Details_Lot Number" in df.columns and df["Lot Details_Lot Number"].notna().any():
                df["Unit"] = df["Lot Details_Lot Number"].astype(str).str.strip()
            elif "auditName" in df.columns:
                def extract_unit(audit_name):
                    if pd.isna(audit_name):
                        return "Unknown"
                    parts = str(audit_name).split("/")
                    if len(parts) >= 3:
                        candidate = parts[1].strip()
                        if len(candidate) <= 6 and any(ch.isdigit() for ch in candidate):
                            return candidate
                    return f"Unit_{hash(str(audit_name)) % 1000}"
                df["Unit"] = df["auditName"].apply(extract_unit)
            else:
                df["Unit"] = [f"Unit_{i}" for i in range(1, len(df) + 1)]

            # STEP 3: Derive unit type
            def derive_unit_type(row):
                unit_type = str(row.get("Pre-Settlement Inspection_Unit Type", "")).strip()
                if unit_type.lower() == "apartment":
                    return "Apartment"
                elif unit_type.lower() == "townhouse":
                    return "Townhouse"
                elif unit_type:
                    return unit_type
                return "Unknown Type"

            df["UnitType"] = df.apply(derive_unit_type, axis=1)

            # STEP 4: Get inspection columns
            inspection_cols = [c for c in df.columns 
                            if c.startswith("Pre-Settlement Inspection_") 
                            and not c.endswith("_notes")]
            
            if not inspection_cols:
                raise ValueError("No inspection columns found in CSV")

            # STEP 5: Melt data
            all_chunks = []
            chunk_size = 50
            
            for i in range(0, len(inspection_cols), chunk_size):
                chunk_cols = inspection_cols[i:i+chunk_size]
                
                chunk_df = df.melt(
                    id_vars=["Unit", "UnitType", "InspectionDate", "OwnerSignoffTimestamp"],
                    value_vars=chunk_cols,
                    var_name="InspectionItem",
                    value_name="Status"
                )
                all_chunks.append(chunk_df)

            long_df = pd.concat(all_chunks, ignore_index=True)

            # STEP 6: Split Room and Component
            parts = long_df["InspectionItem"].str.split("_", n=2, expand=True)
            if len(parts.columns) >= 3:
                long_df["Room"] = parts[1]
                long_df["Component"] = parts[2].str.replace(r"\.\d+$", "", regex=True)
                long_df["Component"] = long_df["Component"].apply(
                    lambda x: x.split("_")[-1] if isinstance(x, str) else x
                )
            else:
                long_df["Room"] = "General"
                long_df["Component"] = long_df["InspectionItem"].str.replace(
                    "Pre-Settlement Inspection_", ""
                )

            # STEP 7: Remove metadata
            metadata_rooms = ["Unit Type", "Building Type", "Townhouse Type", "Apartment Type"]
            metadata_components = ["Room Type"]
            long_df = long_df[~long_df["Room"].isin(metadata_rooms)]
            long_df = long_df[~long_df["Component"].isin(metadata_components)]

            # STEP 8: Classify status and urgency
            def classify_status(val):
                if pd.isna(val):
                    return "Blank"
                val_str = str(val).strip().lower()
                if val_str in ["✓", "✔", "ok", "pass", "passed", "good", "satisfactory"]:
                    return "OK"
                elif val_str in ["✗", "✘", "x", "fail", "failed", "not ok", "defect", "issue"]:
                    return "Not OK"
                elif val_str == "":
                    return "Blank"
                return "Not OK"

            def classify_urgency(val, component, room):
                if pd.isna(val):
                    return "Normal"
                val_str = str(val).strip().lower()
                component_str = str(component).lower()
                room_str = str(room).lower()
                
                urgent_keywords = ["urgent", "immediate", "safety", "hazard", "dangerous"]
                safety_components = ["fire", "smoke", "electrical", "gas", "water", "security"]
                
                if any(kw in val_str for kw in urgent_keywords):
                    return "Urgent"
                if any(sc in component_str for sc in safety_components):
                    return "High Priority"
                return "Normal"

            long_df["StatusClass"] = long_df["Status"].apply(classify_status)
            long_df["Urgency"] = long_df.apply(
                lambda row: classify_urgency(row["Status"], row["Component"], row["Room"]), 
                axis=1
            )

            # STEP 9: Merge with trade mapping
            merged = long_df.merge(mapping, on=["Room", "Component"], how="left")
            merged["Trade"] = merged["Trade"].fillna("Unknown Trade")
            
            mapping_success_rate = ((merged["Trade"] != "Unknown Trade").sum() / len(merged) * 100) if len(merged) > 0 else 0

            # STEP 10: Add planned completion
            def assign_planned_completion(urgency, inspection_date):
                base_date = pd.to_datetime(inspection_date)
                if urgency == "Urgent":
                    return base_date + timedelta(days=3)
                elif urgency == "High Priority":
                    return base_date + timedelta(days=7)
                return base_date + timedelta(days=14)

            merged["PlannedCompletion"] = merged.apply(
                lambda row: assign_planned_completion(row["Urgency"], row["InspectionDate"]), 
                axis=1
            )

            # STEP 11: Create final DataFrame
            final_df = merged[[
                "Unit", "UnitType", "InspectionDate", "OwnerSignoffTimestamp",
                "Room", "Component", "StatusClass", "Trade", "Urgency", "PlannedCompletion"
            ]]

            # STEP 12: Calculate metrics
            metrics = self._calculate_comprehensive_metrics(final_df, building_info, df)

            # STEP 13: Save to database - 🔧 THIS IS THE CRITICAL FIX
            inspection_id = None
            work_order_count = 0
            
            # ✅ Use connection manager if available (PostgreSQL)
            if self.conn_manager:
                try:
                    logger.info("🔄 Saving to PostgreSQL using connection manager...")
                    
                    # Prepare inspection data for saving
                    inspection_data = {
                        'building_name': metrics.get('building_name', 'Unknown'),
                        'address': metrics.get('address', ''),
                        'inspection_date': metrics.get('inspection_date'),
                        'inspector_name': inspector_name,
                        'total_units': metrics.get('total_units', 0),
                        'total_defects': metrics.get('total_defects', 0),
                        'ready_pct': metrics.get('ready_pct', 0),
                        'original_filename': original_filename or 'uploaded_file.csv',
                        'inspection_items': []
                    }
                    
                    # Add inspection items
                    for _, row in final_df.iterrows():
                        inspection_data['inspection_items'].append({
                            'unit': str(row['Unit']),
                            'unit_type': str(row['UnitType']),
                            'room': str(row['Room']),
                            'component': str(row['Component']),
                            'trade': str(row['Trade']),
                            'status': str(row['StatusClass']),
                            'urgency': str(row['Urgency']),
                            'planned_completion': row['PlannedCompletion'].strftime('%Y-%m-%d') if pd.notna(row['PlannedCompletion']) else None,
                            'inspection_date': str(row['InspectionDate']),
                            'owner_signoff_timestamp': str(row['OwnerSignoffTimestamp']) if pd.notna(row['OwnerSignoffTimestamp']) else None,
                            'description': f"{row['Room']} - {row['Component']}"
                        })
                    
                    # Save using connection manager
                    inspection_id = self._save_to_database_with_conn_manager(inspection_data)
                    
                    if inspection_id:
                        logger.info(f"✅ Saved to PostgreSQL: {inspection_id}")
                        
                        # STEP 13b: Create work orders from defects
                        defects_df = final_df[final_df['StatusClass'] == 'Not OK'].copy()
                        if len(defects_df) > 0:
                            work_order_count = self._create_work_orders_with_conn_manager(
                                inspection_id, 
                                defects_df
                            )
                            
                            if work_order_count > 0:
                                logger.info(f"✅ Created {work_order_count} work orders")
                                metrics['work_orders_created'] = work_order_count
                            else:
                                logger.info("No work orders created")
                                metrics['work_orders_created'] = 0
                        else:
                            logger.info("No defects - no work orders needed")
                            metrics['work_orders_created'] = 0
                    else:
                        logger.error("❌ Failed to save to PostgreSQL")
                        
                except Exception as e:
                    logger.error(f"❌ PostgreSQL save failed: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
            
            # Fallback to old db_manager (SQLite) if no connection manager
            elif self.db_manager:
                try:
                    logger.info("💾 Using legacy SQLite save method...")
                    final_df_for_db = final_df.copy()
                    final_df_for_db["PlannedCompletion"] = pd.to_datetime(
                        final_df_for_db["PlannedCompletion"]
                    ).dt.strftime('%Y-%m-%d')
                    
                    inspection_id = self.db_manager.save_inspector_data(
                        final_df_for_db, metrics, inspector_name, original_filename
                    )
                    
                    if inspection_id:
                        logger.info(f"Saved to SQLite: {inspection_id}")
                        work_order_count = self._create_work_orders_from_defects(
                            inspection_id, 
                            final_df_for_db
                        )
                        metrics['work_orders_created'] = work_order_count
                        
                except Exception as e:
                    logger.error(f"SQLite save failed: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
            
            # STEP 14: Log CSV processing
            try:
                if inspection_id and file_hash:
                    self._log_csv_processing(
                        df, metrics, inspection_id, mapping_success_rate, 
                        inspector_name, original_filename, file_hash, work_order_count
                    )
            except Exception as log_error:
                logger.warning(f"CSV logging failed (non-critical): {log_error}")
            
            # STEP 15: Store results and return
            self.processed_data = final_df
            self.metrics = metrics
            
            logger.info(f"✅ Processing complete: {len(final_df)} items, {metrics['total_defects']} defects, {work_order_count} work orders")
            return final_df, metrics, inspection_id
            
        except Exception as e:
            logger.error(f"❌ Processing failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None, None, None


    # ADD THIS NEW METHOD (after _save_to_database_with_conn_manager)
    def _create_work_orders_with_conn_manager(self, inspection_id: str, defects_df: pd.DataFrame) -> int:
        """Create work orders using connection manager (PostgreSQL compatible)"""
        
        if not self.conn_manager:
            logger.warning("No connection manager - cannot create work orders")
            return 0
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            work_orders_created = 0
            
            for _, defect in defects_df.iterrows():
                work_order_id = str(uuid.uuid4())
                
                # Map urgency to planned date offset
                if defect['Urgency'] == 'Urgent':
                    days_offset = 3
                    estimated_hours = 2.0
                elif defect['Urgency'] == 'High Priority':
                    days_offset = 7
                    estimated_hours = 4.0
                else:
                    days_offset = 14
                    estimated_hours = 3.0
                
                planned_date = datetime.now() + timedelta(days=days_offset)
                
                # Determine if photos required
                photo_required_trades = ['Flooring - Tiles', 'Painting', 'Waterproofing', 'Concrete']
                photos_required = str(defect['Trade']) in photo_required_trades
                
                initial_notes = f"Defect from inspection on {defect.get('InspectionDate', 'N/A')}"
                
                # PostgreSQL vs SQLite syntax
                if self.db_type == "postgresql":
                    cursor.execute("""
                        INSERT INTO inspector_work_orders (
                            id, inspection_id, unit, trade, component, room, 
                            urgency, status, planned_date, estimated_hours, notes,
                            photos_required, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """, (
                        work_order_id, inspection_id, str(defect['Unit']),
                        str(defect['Trade']), str(defect['Component']), str(defect['Room']),
                        str(defect['Urgency']), 'pending', planned_date.date(),
                        estimated_hours, initial_notes, photos_required
                    ))
                else:
                    cursor.execute("""
                        INSERT INTO inspector_work_orders (
                            id, inspection_id, unit, trade, component, room, 
                            urgency, status, planned_date, estimated_hours, notes,
                            photos_required, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                    """, (
                        work_order_id, inspection_id, str(defect['Unit']),
                        str(defect['Trade']), str(defect['Component']), str(defect['Room']),
                        str(defect['Urgency']), 'pending', planned_date.date(),
                        estimated_hours, initial_notes, photos_required
                    ))
                
                work_orders_created += 1
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Created {work_orders_created} work orders in {self.db_type.upper()}")
            
            # Log by urgency
            urgency_counts = defects_df['Urgency'].value_counts()
            for urgency, count in urgency_counts.items():
                logger.info(f"  - {urgency}: {count} orders")
            
            return work_orders_created
            
        except Exception as e:
            logger.error(f"❌ Error creating work orders: {e}")
            import traceback
            logger.error(traceback.format_exc())
            try:
                conn.rollback()
                cursor.close()
                conn.close()
            except:
                pass
            return 0
    
    def _log_csv_processing(self, original_df, metrics, inspection_id, mapping_success_rate, 
                       inspector_name, original_filename=None, file_hash=None, work_order_count=0):
        """Log CSV processing with file hash and work order count"""
        
        if not self.db_manager:
            logger.error("Database manager not available")
            return
        
        try:
            conn = self.db_manager.connect()
            cursor = conn.cursor()
            
            log_id = str(uuid.uuid4())
            filename = original_filename or "uploaded_file.csv"
            
            cursor.execute("""
                INSERT INTO inspector_csv_processing_log (
                    id, original_filename, file_checksum, file_size, inspector_id, 
                    building_name, total_rows, processed_rows, defects_found, 
                    mapping_success_rate, status, inspection_id, created_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                log_id, 
                filename, 
                file_hash, 
                len(original_df) * 100,
                None,  # inspector_id
                metrics.get('building_name', 'Unknown Building'), 
                len(original_df), 
                len(original_df), 
                metrics.get('total_defects', 0), 
                mapping_success_rate,
                'completed', 
                inspection_id, 
                datetime.now(), 
                datetime.now()
            ))
            
            conn.commit()
            logger.info(f"CSV processing logged: {work_order_count} work orders created")
            
        except Exception as e:
            logger.error(f"Failed to log CSV processing: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
    def get_inspection_history(self, building_name: str = None, limit: int = 10) -> pd.DataFrame:
        """Get inspection history using database methods"""
        if not self.db_manager:
            logger.warning("Database not available for inspection history")
            return pd.DataFrame()
        
        try:
            return self.db_manager.get_inspector_inspections(limit)
        except Exception as e:
            logger.error(f"Error retrieving inspection history: {e}")
            return pd.DataFrame()
    
    def load_inspection_from_database(self, inspection_id: str) -> Tuple[pd.DataFrame, Dict]:
        """Load previously processed inspection from database"""
        if not self.db_manager:
            raise ValueError("Database not available")
        
        try:
            conn = self.db_manager.connect()
            
            # Get inspection metadata
            inspection_query = """
                SELECT i.id, i.building_id, i.inspection_date, 
                    i.inspector_name, i.total_units, i.total_defects,
                    i.ready_pct, i.original_filename,
                    b.name as building_name, b.address
                FROM inspector_inspections i
                JOIN inspector_buildings b ON i.building_id = b.id
                WHERE i.id = ?
            """
            
            inspection_df = pd.read_sql_query(
                inspection_query, 
                conn, 
                params=[inspection_id],
                parse_dates=False  # Disable automatic date parsing
            )
            
            if inspection_df.empty:
                raise ValueError(f"Inspection {inspection_id} not found")
            
            # Get inspection items with CORRECT column names
            items_query = """
                SELECT 
                    id,
                    inspection_id,
                    unit as Unit,
                    unit_type as UnitType,
                    room as Room,
                    component as Component,
                    trade as Trade,
                    status_class as StatusClass,
                    urgency as Urgency,
                    planned_completion as PlannedCompletion,
                    owner_signoff_timestamp as OwnerSignoffTimestamp,
                    inspection_date as InspectionDate,
                    original_status,
                    created_at
                FROM inspector_inspection_items
                WHERE inspection_id = ?
            """
            
            # Load without automatic date parsing to avoid conversion errors
            items_df = pd.read_sql_query(
                items_query, 
                conn, 
                params=[inspection_id],
                parse_dates=False  # Keep dates as strings initially
            )
            
            logger.info(f"Loaded {len(items_df)} inspection items from database")
            
            # Manually parse dates with error handling
            # These dates are in different formats, so we'll keep them as strings for now
            # and only parse when needed for calculations
            
            inspection_row = inspection_df.iloc[0]
            
            # Calculate metrics from actual data
            defects_only = items_df[items_df["StatusClass"] == "Not OK"]
            total_units = len(items_df["Unit"].unique()) if len(items_df) > 0 else 1
            
            # Calculate settlement readiness
            if len(defects_only) > 0:
                defects_per_unit = defects_only.groupby("Unit").size()
                ready_units_with_few_defects = (defects_per_unit <= 2).sum()
                
                units_with_defects = set(defects_per_unit.index)
                all_units = set(items_df["Unit"].dropna())
                units_with_no_defects = len(all_units - units_with_defects)
                ready_units_calculated = ready_units_with_few_defects + units_with_no_defects
                
                minor_work_units = ((defects_per_unit > 2) & (defects_per_unit <= 7)).sum()
                major_work_units = ((defects_per_unit > 7) & (defects_per_unit <= 15)).sum()
                extensive_work_units = (defects_per_unit > 15).sum()
            else:
                ready_units_calculated = total_units
                minor_work_units = 0
                major_work_units = 0
                extensive_work_units = 0
            
            ready_pct_calculated = (ready_units_calculated / total_units * 100) if total_units > 0 else 0
            minor_pct = (minor_work_units / total_units * 100) if total_units > 0 else 0
            major_pct = (major_work_units / total_units * 100) if total_units > 0 else 0
            extensive_pct = (extensive_work_units / total_units * 100) if total_units > 0 else 0
            
            # Calculate urgency metrics
            urgent_defects = defects_only[defects_only["Urgency"] == "Urgent"] if len(defects_only) > 0 else pd.DataFrame()
            high_priority_defects = defects_only[defects_only["Urgency"] == "High Priority"] if len(defects_only) > 0 else pd.DataFrame()
            
            # Extract date information safely (keep as string)
            inspection_date_str = str(inspection_row.get('inspection_date', '2025-01-01'))
            if ' ' in inspection_date_str:
                inspection_date_str = inspection_date_str.split(' ')[0]  # Take date part only
            
            # Check if there are multiple inspection dates in the items
            if 'InspectionDate' in items_df.columns and len(items_df) > 0:
                unique_dates = items_df['InspectionDate'].dropna().unique()
                if len(unique_dates) > 1:
                    # Convert to date strings for comparison
                    date_strs = [str(d).split('T')[0].split(' ')[0] for d in unique_dates]
                    inspection_date_range = f"{min(date_strs)} to {max(date_strs)}"
                    is_multi_day = True
                else:
                    inspection_date_range = inspection_date_str
                    is_multi_day = False
            else:
                inspection_date_range = inspection_date_str
                is_multi_day = False
            
            # Build complete metrics dictionary
            metrics = {
                'building_name': str(inspection_row['building_name']),
                'address': str(inspection_row.get('address', 'N/A')),
                'inspection_date': inspection_date_str,
                'inspection_date_range': inspection_date_range,
                'is_multi_day_inspection': is_multi_day,
                'unit_types_str': ", ".join(sorted(items_df["UnitType"].astype(str).unique())) if len(items_df) > 0 else "Unknown",
                
                'total_units': total_units,
                'total_defects': len(defects_only),
                'total_inspections': len(items_df),
                'defect_rate': (len(defects_only) / len(items_df) * 100) if len(items_df) > 0 else 0.0,
                'avg_defects_per_unit': len(defects_only) / max(total_units, 1),
                
                'ready_units': ready_units_calculated,
                'ready_pct': ready_pct_calculated,
                'minor_work_units': minor_work_units,
                'major_work_units': major_work_units,
                'extensive_work_units': extensive_work_units,
                'minor_pct': minor_pct,
                'major_pct': major_pct,
                'extensive_pct': extensive_pct,
                
                'urgent_defects': len(urgent_defects),
                'high_priority_defects': len(high_priority_defects),
                
                'inspection_id': inspection_id
            }
            
            # Generate summary tables
            if len(defects_only) > 0:
                metrics['summary_trade'] = defects_only.groupby("Trade").size().reset_index(name="DefectCount").sort_values("DefectCount", ascending=False)
                metrics['summary_unit'] = defects_only.groupby("Unit").size().reset_index(name="DefectCount").sort_values("DefectCount", ascending=False)
                metrics['summary_room'] = defects_only.groupby("Room").size().reset_index(name="DefectCount").sort_values("DefectCount", ascending=False)
                
                # Create urgent defects table
                if len(urgent_defects) > 0:
                    urgent_cols = ["Unit", "Room", "Component", "Trade", "PlannedCompletion"]
                    metrics['urgent_defects_table'] = urgent_defects[urgent_cols].copy()
                else:
                    metrics['urgent_defects_table'] = pd.DataFrame(columns=["Unit", "Room", "Component", "Trade", "PlannedCompletion"])
            else:
                metrics['summary_trade'] = pd.DataFrame(columns=["Trade", "DefectCount"])
                metrics['summary_unit'] = pd.DataFrame(columns=["Unit", "DefectCount"])
                metrics['summary_room'] = pd.DataFrame(columns=["Room", "DefectCount"])
                metrics['urgent_defects_table'] = pd.DataFrame(columns=["Unit", "Room", "Component", "Trade", "PlannedCompletion"])
            
            self.processed_data = items_df
            self.metrics = metrics
            
            conn.close()
            
            logger.info(f"✅ Successfully loaded inspection {inspection_id}")
            
            return items_df, metrics
            
        except Exception as e:
            logger.error(f"Error loading inspection from database: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def get_work_orders_for_builder(self, trade: str = None, status: str = None) -> pd.DataFrame:
        """Get work orders for Builder role"""
        if not self.db_manager:
            return pd.DataFrame()
        
        try:
            return self.db_manager.get_work_orders_for_builder(trade, status)
        except Exception as e:
            logger.error(f"Error retrieving work orders: {e}")
            return pd.DataFrame()
    
    def get_project_overview_for_developer(self) -> pd.DataFrame:
        """Get project overview for Developer role"""
        if not self.db_manager:
            return pd.DataFrame()
        
        try:
            return self.db_manager.get_project_overview_for_developer()
        except Exception as e:
            logger.error(f"Error retrieving project overview: {e}")
            return pd.DataFrame()
    
    def _calculate_comprehensive_metrics(self, final_df: pd.DataFrame, building_info: Dict, original_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate comprehensive metrics"""
        
        # Extract building name from auditName
        sample_audit = original_df.loc[0, "auditName"] if "auditName" in original_df.columns and len(original_df) > 0 else ""
        if sample_audit:
            audit_parts = str(sample_audit).split("/")
            extracted_building_name = audit_parts[2].strip() if len(audit_parts) >= 3 else building_info["name"]
        else:
            extracted_building_name = building_info["name"]
        
        # Extract date
        if 'InspectionDate' in final_df.columns:
            inspection_dates = pd.to_datetime(final_df['InspectionDate'], errors='coerce').dropna()
            
            if len(inspection_dates) > 0:
                min_date = inspection_dates.min()
                max_date = inspection_dates.max()
                primary_date = inspection_dates.mode()[0] if len(inspection_dates.mode()) > 0 else min_date
                
                is_multi_day = (min_date != max_date)
                
                extracted_inspection_date = primary_date.strftime('%Y-%m-%d')
                inspection_date_range = f"{min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"
            else:
                extracted_inspection_date = building_info.get("date", datetime.now().strftime("%Y-%m-%d"))
                inspection_date_range = extracted_inspection_date
                is_multi_day = False
        else:
            extracted_inspection_date = building_info.get("date", datetime.now().strftime("%Y-%m-%d"))
            inspection_date_range = extracted_inspection_date
            is_multi_day = False
        
        # Address extraction
        location = ""
        area = ""
        region = ""
        
        if "Title Page_Site conducted_Location" in original_df.columns:
            location_series = original_df["Title Page_Site conducted_Location"].dropna()
            location = location_series.astype(str).str.strip().iloc[0] if len(location_series) > 0 else ""
        if "Title Page_Site conducted_Area" in original_df.columns:
            area_series = original_df["Title Page_Site conducted_Area"].dropna()
            area = area_series.astype(str).str.strip().iloc[0] if len(area_series) > 0 else ""
        if "Title Page_Site conducted_Region" in original_df.columns:
            region_series = original_df["Title Page_Site conducted_Region"].dropna()
            region = region_series.astype(str).str.strip().iloc[0] if len(region_series) > 0 else ""
        
        address_parts = [part for part in [location, area, region] if part]
        extracted_address = ", ".join(address_parts) if address_parts else building_info["address"]
        
        # Calculate settlement readiness
        defects_per_unit = final_df[final_df["StatusClass"] == "Not OK"].groupby("Unit").size()
        
        ready_units = (defects_per_unit <= 2).sum() if len(defects_per_unit) > 0 else 0
        minor_work_units = ((defects_per_unit > 2) & (defects_per_unit <= 7)).sum() if len(defects_per_unit) > 0 else 0
        major_work_units = ((defects_per_unit > 7) & (defects_per_unit <= 15)).sum() if len(defects_per_unit) > 0 else 0
        extensive_work_units = (defects_per_unit > 15).sum() if len(defects_per_unit) > 0 else 0
        
        # Add units with zero defects
        units_with_defects = set(defects_per_unit.index)
        all_units = set(final_df["Unit"].dropna())
        units_with_no_defects = len(all_units - units_with_defects)
        ready_units += units_with_no_defects
        
        total_units = final_df["Unit"].nunique()
        
        # Calculate basic metrics
        defects_only = final_df[final_df["StatusClass"] == "Not OK"]
        urgent_defects = defects_only[defects_only["Urgency"] == "Urgent"]
        high_priority_defects = defects_only[defects_only["Urgency"] == "High Priority"]
        
        # Planned work
        next_two_weeks = datetime.now() + timedelta(days=14)
        planned_work_2weeks = defects_only[defects_only["PlannedCompletion"] <= next_two_weeks]
        
        next_month = datetime.now() + timedelta(days=30)
        planned_work_month = defects_only[
            (defects_only["PlannedCompletion"] > next_two_weeks) & 
            (defects_only["PlannedCompletion"] <= next_month)
        ]
        
        # Helper for Python types
        def ensure_python_type(value):
            if hasattr(value, 'item'):
                return value.item()
            elif isinstance(value, (np.integer, np.floating)):
                return value.item()
            else:
                return value
        
        # Build metrics dictionary
        metrics = {
            "building_name": str(extracted_building_name),
            "address": str(extracted_address),
            "inspection_date": str(extracted_inspection_date),
            "inspection_date_range": str(inspection_date_range),
            "is_multi_day_inspection": bool(is_multi_day),
            "unit_types_str": ", ".join(sorted(final_df["UnitType"].astype(str).unique())),
            "total_units": ensure_python_type(total_units),
            "total_inspections": ensure_python_type(len(final_df)),
            "total_defects": ensure_python_type(len(defects_only)),
            "defect_rate": ensure_python_type((len(defects_only) / len(final_df) * 100) if len(final_df) > 0 else 0.0),
            "avg_defects_per_unit": ensure_python_type((len(defects_only) / max(total_units, 1))),
            "ready_units": ensure_python_type(ready_units),
            "minor_work_units": ensure_python_type(minor_work_units),
            "major_work_units": ensure_python_type(major_work_units),
            "extensive_work_units": ensure_python_type(extensive_work_units),
            "ready_pct": ensure_python_type((ready_units / total_units * 100) if total_units > 0 else 0),
            "minor_pct": ensure_python_type((minor_work_units / total_units * 100) if total_units > 0 else 0),
            "major_pct": ensure_python_type((major_work_units / total_units * 100) if total_units > 0 else 0),
            "extensive_pct": ensure_python_type((extensive_work_units / total_units * 100) if total_units > 0 else 0),
            "urgent_defects": ensure_python_type(len(urgent_defects)),
            "high_priority_defects": ensure_python_type(len(high_priority_defects)),
            "planned_work_2weeks": ensure_python_type(len(planned_work_2weeks)),
            "planned_work_month": ensure_python_type(len(planned_work_month)),
            
            # Summary tables
            "summary_trade": defects_only.groupby("Trade").size().reset_index(name="DefectCount").sort_values("DefectCount", ascending=False) if len(defects_only) > 0 else pd.DataFrame(columns=["Trade", "DefectCount"]),
            "summary_unit": defects_only.groupby("Unit").size().reset_index(name="DefectCount").sort_values("DefectCount", ascending=False) if len(defects_only) > 0 else pd.DataFrame(columns=["Unit", "DefectCount"]),
            "summary_room": defects_only.groupby("Room").size().reset_index(name="DefectCount").sort_values("DefectCount", ascending=False) if len(defects_only) > 0 else pd.DataFrame(columns=["Room", "DefectCount"]),
            "urgent_defects_table": urgent_defects[["Unit", "Room", "Component", "Trade", "PlannedCompletion"]].copy() if len(urgent_defects) > 0 else pd.DataFrame(columns=["Unit", "Room", "Component", "Trade", "PlannedCompletion"]),
            "planned_work_2weeks_table": planned_work_2weeks[["Unit", "Room", "Component", "Trade", "Urgency", "PlannedCompletion"]].copy() if len(planned_work_2weeks) > 0 else pd.DataFrame(columns=["Unit", "Room", "Component", "Trade", "Urgency", "PlannedCompletion"]),
            "planned_work_month_table": planned_work_month[["Unit", "Room", "Component", "Trade", "Urgency", "PlannedCompletion"]].copy() if len(planned_work_month) > 0 else pd.DataFrame(columns=["Unit", "Room", "Component", "Trade", "Urgency", "PlannedCompletion"]),
            "component_details_summary": defects_only.groupby(["Trade", "Room", "Component"])["Unit"].apply(lambda s: ", ".join(sorted(s.astype(str).unique()))).reset_index().rename(columns={"Unit": "Units with Defects"}) if len(defects_only) > 0 else pd.DataFrame(columns=["Trade", "Room", "Component", "Units with Defects"])
        }
        
        return metrics

    def _extract_unit_inspection_dates(self, df: pd.DataFrame) -> pd.Series:
        """Extract inspection date for each unit"""
        
        # Method 1: Title Page_Conducted on
        if "Title Page_Conducted on" in df.columns:
            dates = pd.to_datetime(df["Title Page_Conducted on"], errors='coerce')
            valid_count = dates.notna().sum()
            
            if valid_count > 0:
                logger.info(f"Found {valid_count} valid dates in 'Title Page_Conducted on'")
                if dates.notna().any():
                    mode_date = dates.mode()[0] if len(dates.mode()) > 0 else dates.dropna().iloc[0]
                    dates = dates.fillna(mode_date)
                    return dates.dt.strftime('%Y-%m-%d')
        
        # Method 2: Extract from auditName
        if "auditName" in df.columns:
            def extract_date(audit_name):
                if pd.isna(audit_name):
                    return None
                parts = str(audit_name).split("/")
                if len(parts) >= 1:
                    try:
                        date_obj = pd.to_datetime(parts[0].strip(), format='%d/%m/%Y', errors='coerce')
                        if pd.notna(date_obj):
                            return date_obj.strftime('%Y-%m-%d')
                    except:
                        pass
                return None
            
            dates = df["auditName"].apply(extract_date)
            valid_count = dates.notna().sum()
            
            if valid_count > 0:
                logger.info(f"Extracted {valid_count} dates from auditName")
                mode_date = dates.mode()[0] if len(dates.mode()) > 0 else dates.dropna().iloc[0]
                dates = dates.fillna(mode_date)
                return dates
        
        # Fallback
        logger.warning("Could not extract dates, using current date")
        return pd.Series([datetime.now().strftime('%Y-%m-%d')] * len(df))

    def _extract_signoff_timestamp(self, df: pd.DataFrame) -> pd.Series:
        """Extract owner/agent signature timestamp"""
        
        signoff_col = "Sign Off_Owner/Agent Signature_timestamp"
        
        if signoff_col in df.columns:
            timestamps = pd.to_datetime(df[signoff_col], errors='coerce')
            valid_count = timestamps.notna().sum()
            
            if valid_count > 0:
                logger.info(f"Found {valid_count} owner/agent sign-off timestamps")
            else:
                logger.info("Sign-off column exists but no valid timestamps")
            
            return timestamps
        else:
            logger.info("No sign-off timestamp column found")
            return pd.Series([None] * len(df))


def load_master_trade_mapping() -> pd.DataFrame:
    """Load master trade mapping - file-based only (no database dependency)"""
    import os
    
    # Try CSV file first
    if os.path.exists("MasterTradeMapping.csv"):
        try:
            mapping = pd.read_csv("MasterTradeMapping.csv")
            logger.info(f"Loaded master trade mapping from file: {len(mapping)} entries")
            return mapping
        except Exception as e:
            logger.warning(f"Could not load MasterTradeMapping.csv: {e}")
    
    # Fallback to embedded mapping
    logger.info("Using embedded fallback trade mapping")
    mapping_data = """Room,Component,Trade
Apartment Entry Door,Door Handle,Doors
Apartment Entry Door,Door Locks and Keys,Doors
Apartment Entry Door,Door Stopper,Doors
Apartment Entry Door,Door Frame,Doors
Balcony,Balustrade,Carpentry & Joinery
Balcony,Floor Tiles,Flooring - Tiles
Balcony,Waterproofing,Waterproofing
Bathroom,Tiles,Flooring - Tiles
Bathroom,Wall Tiles,Flooring - Tiles
Bathroom,Toilet,Plumbing
Bathroom,Basin,Plumbing
Bathroom,Shower,Plumbing
Bathroom,Exhaust Fan,Electrical
Bathroom,Mirror,Carpentry & Joinery
Kitchen Area,Cabinets,Carpentry & Joinery
Kitchen Area,Benchtop,Carpentry & Joinery
Kitchen Area,Splashback,Flooring - Tiles
Kitchen Area,Sink,Plumbing
Kitchen Area,Cooktop,Appliances
Kitchen Area,Oven,Appliances
Kitchen Area,Rangehood,Appliances
Kitchen Area,Dishwasher,Appliances
Bedroom,Carpets,Flooring - Carpets
Bedroom,Windows,Windows
Bedroom,Wardrobe,Carpentry & Joinery
Bedroom,Doors,Doors
Bedroom,Paint,Painting
Living Room,Windows,Windows
Living Room,Flooring,Flooring - Timber
Living Room,Air Conditioning,HVAC
Living Room,Paint,Painting
Laundry,Cabinets,Carpentry & Joinery
Laundry,Sink,Plumbing
Laundry,Taps,Plumbing
Laundry,Tiles,Flooring - Tiles
General,Smoke Detector,Fire Safety
General,Light Fittings,Electrical
General,Power Points,Electrical
General,Switches,Electrical"""
    
    return pd.read_csv(StringIO(mapping_data))


if __name__ == "__main__":
    print("Building Inspection Data Processor - With Work Order Creation")
    print("=" * 70)
    print("\nFeatures:")
    print("  - Complete CSV processing logic")
    print("  - Automatic work order creation from defects")
    print("  - Database integration for multi-role access")
    print("  - Inspector data persisted for Builder and Developer")
    print("  - Work orders ready immediately after processing")
    print("\nReady for Building Inspection System V3!")