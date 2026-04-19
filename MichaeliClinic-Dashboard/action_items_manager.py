"""
Action Items Manager
Handles task/action items operations
"""

from typing import List, Dict
from datetime import datetime
import time


class ActionItemsManager:
    """Manages action items (tasks)"""
    
    def __init__(self, db):
        self.db = db
    
    def get_by_tab(self, tab: str) -> List[Dict]:
        """Get all action items for a specific tab"""
        return self.db.fetchall("""
            SELECT id, text, priority, addedAt, done, doneAt
            FROM action_items
            WHERE tab = ?
            ORDER BY 
                done ASC,
                CASE priority
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 3
                END,
                addedAt DESC
        """, (tab,))
    
    def add(self, tab: str, text: str, priority: str) -> Dict:
        """Add new action item"""
        try:
            # Generate unique ID (timestamp-based)
            item_id = str(int(time.time() * 1000))
            added_at = datetime.now().isoformat() + 'Z'
            
            self.db.execute("""
                INSERT INTO action_items (id, tab, text, priority, addedAt, done)
                VALUES (?, ?, ?, ?, ?, 0)
            """, (item_id, tab, text, priority, added_at))
            
            self.db.commit()
            
            return {
                'id': item_id,
                'tab': tab,
                'text': text,
                'priority': priority,
                'addedAt': added_at,
                'done': 0,
                'doneAt': None
            }
        except Exception as e:
            self.db.rollback()
            print(f"Error adding action item: {e}")
            return None
    
    def toggle(self, item_id: str) -> bool:
        """Toggle action item done/undone"""
        try:
            # Get current state
            item = self.db.fetchone("""
                SELECT done FROM action_items WHERE id = ?
            """, (item_id,))
            
            if not item:
                return False
            
            # Toggle
            new_done = 0 if item['done'] else 1
            done_at = datetime.now().isoformat() + 'Z' if new_done else None
            
            self.db.execute("""
                UPDATE action_items
                SET done = ?, doneAt = ?
                WHERE id = ?
            """, (new_done, done_at, item_id))
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error toggling action item: {e}")
            return False
    
    def delete(self, item_id: str) -> bool:
        """Delete action item"""
        try:
            self.db.execute("""
                DELETE FROM action_items WHERE id = ?
            """, (item_id,))
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error deleting action item: {e}")
            return False
    
    def count_pending(self) -> int:
        """Count pending (not done) action items"""
        result = self.db.fetchone("""
            SELECT COUNT(*) as count
            FROM action_items
            WHERE done = 0
        """)
        return result['count'] if result else 0
