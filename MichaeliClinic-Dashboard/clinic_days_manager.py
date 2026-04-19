"""
Clinic Days Manager
Handles clinic schedule configuration
"""

from typing import Dict, Optional


class ClinicDaysManager:
    """Manages clinic day configurations"""
    
    def __init__(self, db):
        self.db = db
    
    def get(self, date_str: str) -> Optional[Dict]:
        """Get clinic configuration for a specific date"""
        config = self.db.fetchone("""
            SELECT downtown, vaughan, ivf, survivorship, md2
            FROM clinic_days
            WHERE date = ?
        """, (date_str,))
        
        if config:
            # Convert to boolean
            return {
                'downtown': bool(config['downtown']),
                'vaughan': bool(config['vaughan']),
                'ivf': bool(config['ivf']),
                'survivorship': bool(config['survivorship']),
                'md2': bool(config['md2'])
            }
        else:
            # Default: all available except special clinics
            return {
                'downtown': True,
                'vaughan': True,
                'ivf': False,
                'survivorship': False,
                'md2': False
            }
    
    def update(self, date_str: str, config: Dict) -> bool:
        """Update clinic configuration for a date"""
        try:
            # Check if exists
            exists = self.db.fetchone("""
                SELECT date FROM clinic_days WHERE date = ?
            """, (date_str,))
            
            if exists:
                # Update
                self.db.execute("""
                    UPDATE clinic_days
                    SET downtown = ?, vaughan = ?, ivf = ?, survivorship = ?, md2 = ?
                    WHERE date = ?
                """, (
                    1 if config.get('downtown', False) else 0,
                    1 if config.get('vaughan', False) else 0,
                    1 if config.get('ivf', False) else 0,
                    1 if config.get('survivorship', False) else 0,
                    1 if config.get('md2', False) else 0,
                    date_str
                ))
            else:
                # Insert
                self.db.execute("""
                    INSERT INTO clinic_days (date, downtown, vaughan, ivf, survivorship, md2)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    date_str,
                    1 if config.get('downtown', False) else 0,
                    1 if config.get('vaughan', False) else 0,
                    1 if config.get('ivf', False) else 0,
                    1 if config.get('survivorship', False) else 0,
                    1 if config.get('md2', False) else 0
                ))
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error updating clinic day: {e}")
            return False
