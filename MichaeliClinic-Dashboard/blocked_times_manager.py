"""
Blocked Times Manager
Handles time blocking operations
"""

from typing import List, Dict, Optional
from datetime import datetime


class BlockedTimesManager:
    """Manages blocked time slots"""
    
    def __init__(self, db):
        self.db = db
    
    def get_by_date(self, date_str: str) -> List[Dict]:
        """Get all blocked times for a specific date
        
        Args:
            date_str: Date in YYYY-MM-DD format
            
        Returns:
            List of blocked time dictionaries
        """
        return self.db.fetchall("""
            SELECT id, date, startTime, endTime, title, notes, createdAt, createdBy
            FROM blocked_times
            WHERE date = ?
            ORDER BY startTime
        """, (date_str,))
    
    def get_by_date_range(self, start_date: str, end_date: str) -> List[Dict]:
        """Get all blocked times within a date range
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of blocked time dictionaries
        """
        return self.db.fetchall("""
            SELECT id, date, startTime, endTime, title, notes, createdAt, createdBy
            FROM blocked_times
            WHERE date >= ? AND date <= ?
            ORDER BY date, startTime
        """, (start_date, end_date))
    
    def get_by_id(self, block_id: int) -> Optional[Dict]:
        """Get a specific blocked time by ID
        
        Args:
            block_id: The blocked time ID
            
        Returns:
            Blocked time dictionary or None
        """
        return self.db.fetchone("""
            SELECT id, date, startTime, endTime, title, notes, createdAt, createdBy
            FROM blocked_times
            WHERE id = ?
        """, (block_id,))
    
    def create(self, date_str: str, start_time: str, end_time: str, 
               title: str, notes: str = None, created_by: str = None) -> Optional[int]:
        """Create a new blocked time
        
        Args:
            date_str: Date in YYYY-MM-DD format
            start_time: Start time in HH:MM format
            end_time: End time in HH:MM format
            title: Title/name for the block
            notes: Optional notes
            created_by: Username who created it
            
        Returns:
            ID of created block, or None on error
        """
        try:
            created_at = datetime.now().isoformat() + 'Z'
            
            self.db.execute("""
                INSERT INTO blocked_times (date, startTime, endTime, title, notes, createdAt, createdBy)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (date_str, start_time, end_time, title, notes, created_at, created_by))
            
            self.db.commit()
            
            # Get the ID of the inserted row
            result = self.db.fetchone("SELECT last_insert_rowid() as id")
            return result['id'] if result else None
            
        except Exception as e:
            self.db.rollback()
            print(f"Error creating blocked time: {e}")
            return None
    
    def update(self, block_id: int, date_str: str, start_time: str, 
               end_time: str, title: str, notes: str = None) -> bool:
        """Update an existing blocked time
        
        Args:
            block_id: The blocked time ID
            date_str: Date in YYYY-MM-DD format
            start_time: Start time in HH:MM format
            end_time: End time in HH:MM format
            title: Title/name for the block
            notes: Optional notes
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.db.execute("""
                UPDATE blocked_times
                SET date = ?, startTime = ?, endTime = ?, title = ?, notes = ?
                WHERE id = ?
            """, (date_str, start_time, end_time, title, notes, block_id))
            
            self.db.commit()
            return True
            
        except Exception as e:
            self.db.rollback()
            print(f"Error updating blocked time: {e}")
            return False
    
    def delete(self, block_id: int) -> bool:
        """Delete a blocked time
        
        Args:
            block_id: The blocked time ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.db.execute("""
                DELETE FROM blocked_times WHERE id = ?
            """, (block_id,))
            
            self.db.commit()
            return True
            
        except Exception as e:
            self.db.rollback()
            print(f"Error deleting blocked time: {e}")
            return False
    
    def check_conflict(self, date_str: str, time_str: str) -> Optional[Dict]:
        """Check if a given date/time conflicts with any blocked time
        
        Args:
            date_str: Date in YYYY-MM-DD format
            time_str: Time in HH:MM format
            
        Returns:
            Conflicting block dictionary or None
        """
        return self.db.fetchone("""
            SELECT id, date, startTime, endTime, title
            FROM blocked_times
            WHERE date = ?
              AND ? >= startTime
              AND ? < endTime
            ORDER BY startTime
            LIMIT 1
        """, (date_str, time_str, time_str))
