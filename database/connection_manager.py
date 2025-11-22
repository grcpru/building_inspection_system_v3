"""
Database Connection Manager - IMPROVED VERSION
============================================
Handles both SQLite (local) and PostgreSQL (production) connections
CAN BUILD CONNECTION STRING FROM INDIVIDUAL PARAMETERS
"""

import os
import sqlite3
from typing import Any, Optional
from urllib.parse import quote_plus
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
            print(f"✅ PostgreSQL detected - URL starts with: {database_url[:30]}...")
            return "postgresql"
        else:
            print("📁 Falling back to SQLite (local mode)")
            return "sqlite"
    
    def _get_database_url(self) -> Optional[str]:
        """
        Get database URL from multiple sources
        Priority: Connection string > Build from parameters > Environment variables
        """
        # 1. Try to find a pre-built connection string
        try:
            # Check for postgresql_url (preferred format)
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
            print(f"Note: Could not read connection string from secrets: {e}")
        
        # 2. ✅ NEW: Try to BUILD URL from individual parameters
        try:
            if "database" in st.secrets:
                db_config = st.secrets["database"]
                
                # Check if we have individual parameters
                if all(key in db_config for key in ["host", "port", "database", "user", "password"]):
                    print("🔧 Building PostgreSQL URL from individual parameters...")
                    
                    # URL-encode password to handle special characters
                    password_encoded = quote_plus(db_config["password"])
                    
                    # Build the connection string
                    connection_url = (
                        f"postgresql://{db_config['user']}:{password_encoded}"
                        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
                        f"?sslmode=require&connect_timeout=15"
                    )
                    
                    print(f"✅ Built connection URL from parameters")
                    return connection_url
        except Exception as e:
            print(f"Could not build URL from parameters: {e}")
        
        # 3. Try environment variables
        database_url = os.getenv('DATABASE_URL') or os.getenv('POSTGRESQL_URL')
        if database_url:
            print("✅ Using DATABASE_URL from environment")
            return database_url
        
        # 4. Not found
        print("⚠️ No PostgreSQL configuration found")
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
        """Create PostgreSQL connection"""
        database_url = self._get_database_url()
        
        if database_url is None:
            raise ValueError(
                "PostgreSQL connection string not found. "
                "Please check your .streamlit/secrets.toml configuration"
            )
        
        # Add connection parameters if not present
        if "?" not in database_url:
            database_url += "?connect_timeout=15&sslmode=require"
        elif "connect_timeout" not in database_url:
            database_url += "&connect_timeout=15"
        
        if "sslmode" not in database_url:
            database_url += "&sslmode=require"
        
        try:
            # Create connection with explicit timeout
            conn = psycopg2.connect(
                database_url,
                connect_timeout=15,
                options='-c statement_timeout=30000'
            )
            conn.autocommit = False
            
            # Quick connection test
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            print("✅ PostgreSQL connection successful!")
            return conn
            
        except psycopg2.OperationalError as e:
            error_msg = str(e)
            
            # Better error messages
            if "timeout" in error_msg.lower():
                raise ValueError(
                    f"PostgreSQL connection timeout. Possible causes:\n"
                    f"1. Firewall blocking connection\n"
                    f"2. IP address not whitelisted in Supabase\n"
                    f"3. Network routing issues\n"
                    f"Original error: {error_msg}"
                )
            elif "password authentication failed" in error_msg:
                raise ValueError(
                    "PostgreSQL authentication failed. Please check:\n"
                    "1. Username and password in secrets.toml\n"
                    "2. Special characters in password (use URL encoding)\n"
                    "   Example: ! should be %21"
                )
            elif "could not connect to server" in error_msg:
                raise ValueError(
                    "Could not connect to PostgreSQL server. Check:\n"
                    "1. Internet connection\n"
                    "2. Supabase project status\n"
                    "3. Host and port in secrets.toml"
                )
            else:
                raise ValueError(f"PostgreSQL connection error: {error_msg}")
        except Exception as e:
            raise ValueError(f"Unexpected PostgreSQL error: {str(e)}")
    
    def _get_sqlite_connection(self):
        """Create SQLite connection"""
        print(f"📁 Using SQLite database: {self.sqlite_path}")
        conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
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
        """
        if self.db_type == "postgresql":
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
        print(f"🔧 Database type detected: {_connection_manager.db_type}")
    return _connection_manager