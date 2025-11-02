"""
Database Connection Manager - FIXED VERSION
============================================
Handles both SQLite (local) and PostgreSQL (production) connections
WITH PROPER TIMEOUT AND SSL HANDLING
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
    
    Uses PostgreSQL if database connection string is available
    Falls back to SQLite for local development without secrets
    """
    
    def __init__(self):
        self.db_type = self._detect_database_type()
        self.sqlite_path = "building_inspection.db"
        
    def _detect_database_type(self) -> str:
        """Detect which database to use"""
        # Try multiple ways to get the database URL
        database_url = self._get_database_url()
        
        if database_url and POSTGRES_AVAILABLE:
            return "postgresql"
        else:
            return "sqlite"
    
    def _get_database_url(self) -> Optional[str]:
        """
        Get database URL from multiple sources
        Priority: Streamlit secrets > Environment variables
        """
        # 1. Try Streamlit secrets first (most common in production)
        try:
            # Check for postgresql_url (our new format)
            if "database" in st.secrets:
                if "postgresql_url" in st.secrets["database"]:
                    return st.secrets["database"]["postgresql_url"]
                # Also check DATABASE_URL for backward compatibility
                if "DATABASE_URL" in st.secrets["database"]:
                    return st.secrets["database"]["DATABASE_URL"]
            
            # Check direct key (alternative format)
            if "postgresql_url" in st.secrets:
                return st.secrets["postgresql_url"]
            if "DATABASE_URL" in st.secrets:
                return st.secrets["DATABASE_URL"]
        except Exception as e:
            print(f"Note: Could not read from st.secrets: {e}")
        
        # 2. Try environment variables
        database_url = os.getenv('DATABASE_URL') or os.getenv('POSTGRESQL_URL')
        if database_url:
            return database_url
        
        # 3. Not found
        return None
    
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
        """Create PostgreSQL connection - FIXED VERSION"""
        database_url = self._get_database_url()
        
        if database_url is None:
            raise ValueError(
                "PostgreSQL connection string not found. "
                "Please add 'postgresql_url' to .streamlit/secrets.toml under [database] section"
            )
        
        # 🔧 FIX 1: Add connection timeout and SSL mode to URL if not present
        if "?" not in database_url:
            database_url += "?connect_timeout=15&sslmode=prefer&application_name=building_inspection"
        elif "connect_timeout" not in database_url:
            database_url += "&connect_timeout=15"
        
        if "sslmode" not in database_url:
            database_url += "&sslmode=prefer"
        
        if "application_name" not in database_url:
            database_url += "&application_name=building_inspection"
        
        try:
            # 🔧 FIX 2: Use connection with explicit timeout
            conn = psycopg2.connect(
                database_url,
                connect_timeout=15,  # Explicit timeout
                options='-c statement_timeout=30000'  # 30 second query timeout
            )
            conn.autocommit = False  # Explicit transaction control
            
            # 🔧 FIX 3: Quick connection test with timeout
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            return conn
            
        except psycopg2.OperationalError as e:
            error_msg = str(e)
            
            # 🔧 FIX 4: Better error messages
            if "timeout" in error_msg.lower():
                # If timeout, try one more time with different SSL mode
                print("⚠️ Connection timeout, retrying with sslmode=disable...")
                try:
                    database_url_no_ssl = database_url.replace("sslmode=prefer", "sslmode=disable")
                    database_url_no_ssl = database_url_no_ssl.replace("sslmode=require", "sslmode=disable")
                    
                    conn = psycopg2.connect(
                        database_url_no_ssl,
                        connect_timeout=10
                    )
                    print("✅ Connected with SSL disabled (development only)")
                    return conn
                except Exception as retry_error:
                    raise ValueError(
                        f"PostgreSQL connection timeout. Possible causes:\n"
                        f"1. Connection pooler is blocked by firewall\n"
                        f"2. IP address not whitelisted in Supabase\n"
                        f"3. Network routing issues\n"
                        f"Original error: {error_msg}"
                    )
            
            elif "password authentication failed" in error_msg:
                raise ValueError(
                    "PostgreSQL authentication failed. Please check your password in secrets.toml. "
                    "Note: Special characters in password may need URL encoding."
                )
            elif "could not connect to server" in error_msg:
                raise ValueError(
                    "Could not connect to PostgreSQL server. "
                    "Please check your internet connection and Supabase status."
                )
            else:
                raise ValueError(f"PostgreSQL connection error: {error_msg}")
        except Exception as e:
            raise ValueError(f"Unexpected PostgreSQL error: {str(e)}")
    
    def _get_sqlite_connection(self):
        """Create SQLite connection"""
        print(f"📁 Using SQLite database: {self.sqlite_path}")
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
        - ? -> %s (parameter placeholders)
        """
        if self.db_type == "postgresql":
            # Replace SQLite-specific syntax with PostgreSQL
            sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            sql = sql.replace("AUTOINCREMENT", "")
            sql = sql.replace("DATETIME DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            sql = sql.replace("DATETIME", "TIMESTAMP")
            # Note: Parameter placeholders (? -> %s) should be handled at query time
            
        return sql


# Singleton instance
_connection_manager = None

def get_connection_manager() -> ConnectionManager:
    """Get singleton ConnectionManager instance"""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = ConnectionManager()
        print(f"🔧 Database type detected: {_connection_manager.db_type}")
    return _connection_manager