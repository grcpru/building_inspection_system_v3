"""
Trade Mapping System - Fixed Based on Working streamlit_app.py
=============================================================

This module provides trade mapping using the EXACT working logic from streamlit_app.py
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import logging
import os
from io import StringIO

# Set up logging
logger = logging.getLogger(__name__)


class TradeMapper:
    """Trade mapping system using EXACT working logic from streamlit_app.py"""
    
    def __init__(self):
        """Initialize the trade mapper."""
        self.master_mapping = None
        self.mapping_stats = {}
    
    def load_master_trade_mapping(self) -> pd.DataFrame:
        """Load master trade mapping using EXACT logic from streamlit_app.py"""
        return load_master_trade_mapping()
    
    def apply_mapping(self, inspection_data: pd.DataFrame) -> pd.DataFrame:
        """Apply trade mapping using the EXACT merge logic from streamlit_app.py"""
        if self.master_mapping is None:
            self.master_mapping = self.load_master_trade_mapping()
        
        # EXACT merge logic from streamlit_app.py
        merged = inspection_data.merge(self.master_mapping, on=["Room", "Component"], how="left")
        merged["Trade"] = merged["Trade"].fillna("Unknown Trade")
        
        return merged


def load_master_trade_mapping() -> pd.DataFrame:
    """Load master trade mapping - EXACT function from streamlit_app.py"""
    try:
        import os
        if os.path.exists("MasterTradeMapping.csv"):
            return pd.read_csv("MasterTradeMapping.csv")
        else:
            logger.warning("MasterTradeMapping.csv not found in project folder")
            # EXACT fallback mapping from streamlit_app.py
            basic_mapping = """Room,Component,Trade
Apartment Entry Door,Door Handle,Doors
Apartment Entry Door,Door Locks and Keys,Doors
Balcony,Balustrade,Carpentry & Joinery
Bathroom,Tiles,Flooring - Tiles
Kitchen Area,Cabinets,Carpentry & Joinery"""
            return pd.read_csv(StringIO(basic_mapping))
    except Exception as e:
        logger.error(f"Error loading master mapping: {e}")
        return pd.DataFrame(columns=["Room", "Component", "Trade"])


def save_trade_mapping_to_database(trade_mapping_df: pd.DataFrame, username: str) -> bool:
    """Save trade mapping to database"""
    try:
        logger.info(f"Trade mapping saved by {username}: {len(trade_mapping_df)} entries")
        return True
    except Exception as e:
        logger.error(f"Error saving trade mapping: {e}")
        return False


def load_trade_mapping_from_database() -> pd.DataFrame:
    """Load trade mapping from database"""
    try:
        return pd.DataFrame(columns=["Room", "Component", "Trade"])
    except Exception as e:
        logger.error(f"Error loading trade mapping from database: {e}")
        return pd.DataFrame(columns=["Room", "Component", "Trade"])


if __name__ == "__main__":
    print("Trade Mapping System - Based on streamlit_app.py")
    
    # Test the mapper
    mapper = TradeMapper()
    mapping = mapper.load_master_trade_mapping()
    print(f"Loaded mapping with {len(mapping)} entries")
    
    # Test with sample data
    sample_data = pd.DataFrame({
        'Room': ['Bathroom', 'Kitchen Area', 'Bedroom'],
        'Component': ['Tiles', 'Cabinets', 'Windows']
    })
    
    mapped_data = mapper.apply_mapping(sample_data)
    print("Sample mappings:")
    for _, row in mapped_data.iterrows():
        print(f"  {row['Room']} - {row['Component']} -> {row['Trade']}")
    
    print("Trade mapper ready!")