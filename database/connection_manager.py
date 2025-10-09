"""
Database Connection Manager
Handles both SQLite (local) and PostgreSQL (production) connections
"""

import os
import sqlite3
from typing import Any, Optional
import streamlit as st

# Try to import psycopg2, but don't fail if not available
try:
    import psycopg2
    import psycopg2.extras
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


class ConnectionManager:
    """
    Manages database connections with automatic fallback.
    
    Uses PostgreSQL if DATABASE_URL is set (production/Streamlit Cloud)
    Falls back to SQLite for local development
    """
    
    def __init__(self):
        self.db_type = self._detect_database_type()
        self.sqlite_path = "building_inspection.db"
        
    def _detect_database_type(self) -> str:
        """Detect which database to use"""
        # Check environment variable (Streamlit Cloud secrets)
        database_url = os.getenv('DATABASE_URL')
        
        # Check Streamlit secrets
        if database_url is None:
            try:
                database_url = st.secrets.get("DATABASE_URL")
            except:
                pass
        
        if database_url and POSTGRES_AVAILABLE:
            return "postgresql"
        else:
            return "sqlite"
    
    def get_connection(self):
        """
        Get a database connection based on environment
        
        Returns:
            Database connection object (sqlite3.Connection or psycopg2.connection)
        """
        if self.db_type == "postgresql":
            return self._get_postgres_connection()
        else:
            return self._get_sqlite_connection()
    
    def _get_postgres_connection(self):
        """Create PostgreSQL connection"""
        database_url = os.getenv('DATABASE_URL')
        
        if database_url is None:
            try:
                database_url = st.secrets["DATABASE_URL"]
            except:
                raise ValueError("DATABASE_URL not found in environment or secrets")
        
        try:
            conn = psycopg2.connect(
                database_url,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            return conn
        except Exception as e:
            st.error(f"PostgreSQL connection failed: {e}")
            raise
    
    def _get_sqlite_connection(self):
        """Create SQLite connection"""
        conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row  # Return rows as dict-like objects
        return conn
    
    def get_db_type(self) -> str:
        """Return current database type"""
        return self.db_type
    
    def execute_query(self, query: str, params: tuple = None, fetch: str = None) -> Any:
        """
        Execute a query with automatic connection handling
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch: 'one', 'all', or None (for INSERT/UPDATE/DELETE)
        
        Returns:
            Query results based on fetch parameter
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch == 'one':
                result = cursor.fetchone()
            elif fetch == 'all':
                result = cursor.fetchall()
            else:
                conn.commit()
                result = cursor.rowcount
            
            return result
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
    
    def convert_sql_for_db(self, sql: str) -> str:
        """
        Convert SQL syntax between SQLite and PostgreSQL if needed
        
        Common conversions:
        - AUTOINCREMENT -> SERIAL (for PostgreSQL)
        - DATETIME -> TIMESTAMP
        - TEXT -> VARCHAR for PostgreSQL
        """
        if self.db_type == "postgresql":
            # Replace SQLite-specific syntax with PostgreSQL
            sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            sql = sql.replace("AUTOINCREMENT", "")
            sql = sql.replace("DATETIME DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            sql = sql.replace("DATETIME", "TIMESTAMP")
            
        return sql


# Singleton instance
_connection_manager = None

def get_connection_manager() -> ConnectionManager:
    """Get singleton ConnectionManager instance"""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = ConnectionManager()
    return _connection_manager