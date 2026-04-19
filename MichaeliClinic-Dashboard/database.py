"""
Database Connection Manager
Handles SQLite connection and common operations
"""

import sqlite3
from typing import List, Dict, Any, Optional


class Database:
    """SQLite database connection manager"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.connect()
    
    def connect(self):
        """Connect to database"""
        self.conn = sqlite3.connect(
            self.db_path, 
            check_same_thread=False,
            timeout=10.0  # Wait up to 10 seconds if database is locked
        )
        self.conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        
        # Enable foreign keys
        self.conn.execute("PRAGMA foreign_keys = ON")
        
        # Enable WAL mode for better concurrent access
        self.conn.execute("PRAGMA journal_mode=WAL")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def execute(self, query: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute a query and return cursor"""
        return self.conn.execute(query, params)
    
    def fetchone(self, query: str, params: tuple = ()) -> Optional[Dict]:
        """Execute query and fetch one result as dict"""
        cursor = self.execute(query, params)
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def fetchall(self, query: str, params: tuple = ()) -> List[Dict]:
        """Execute query and fetch all results as list of dicts"""
        cursor = self.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    
    def commit(self):
        """Commit transaction"""
        self.conn.commit()
    
    def rollback(self):
        """Rollback transaction"""
        self.conn.rollback()
