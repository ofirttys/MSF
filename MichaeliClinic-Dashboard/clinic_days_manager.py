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
    
    def get_all(self) -> Dict[str, Dict]:
        """Get all clinic day configurations"""
        rows = self.db.fetchall("""
            SELECT date, downtown, vaughan, ivf, survivorship, md2
            FROM clinic_days
        """)
        
        result = {}
        for row in rows:
            result[row['date']] = {
                'downtown': bool(row['downtown']),
                'vaughan': bool(row['vaughan']),
                'ivf': bool(row['ivf']),
                'survivorship': bool(row['survivorship']),
                'md2': bool(row['md2'])
            }
        
        return result
    
    def get_month(self, year: int, month: int) -> Dict[str, Dict]:
        """Get clinic day configurations for a specific month"""
        # Create date range for the month
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year + 1}-01-01"
        else:
            end_date = f"{year}-{month + 1:02d}-01"
        
        rows = self.db.fetchall("""
            SELECT date, downtown, vaughan, ivf, survivorship, md2
            FROM clinic_days
            WHERE date >= ? AND date < ?
        """, (start_date, end_date))
        
        result = {}
        for row in rows:
            result[row['date']] = {
                'downtown': bool(row['downtown']),
                'vaughan': bool(row['vaughan']),
                'ivf': bool(row['ivf']),
                'survivorship': bool(row['survivorship']),
                'md2': bool(row['md2'])
            }
        
        return result
