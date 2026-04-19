"""
Appointment Manager
Handles appointment-related operations
"""

from typing import List, Dict, Optional
from datetime import datetime, date


class AppointmentManager:
    """Manages appointments"""
    
    def __init__(self, db):
        self.db = db
    
    def get_by_date(self, date_str: str) -> List[Dict]:
        """Get all appointments for a specific date"""
        return self.db.fetchall("""
            SELECT 
                p.patientID,
                p.patientName,
                p.partnerName,
                p.appointmentTime,
                p.appointmentLocation,
                p.currentState,
                p.notes
            FROM patients p
            WHERE p.nextAppointment = ?
            ORDER BY p.appointmentTime
        """, (date_str,))
    
    def get_today(self) -> List[Dict]:
        """Get today's appointments"""
        today = date.today().isoformat()
        return self.get_by_date(today)
    
    def update_next(self, patient_id: str, appt_date: str, appt_time: str, location: Optional[str] = None) -> bool:
        """Update patient's next appointment"""
        try:
            self.db.execute("""
                UPDATE patients
                SET nextAppointment = ?,
                    appointmentTime = ?,
                    appointmentLocation = ?
                WHERE patientID = ?
            """, (appt_date, appt_time, location, patient_id))
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error updating appointment: {e}")
            return False
    
    def count_today(self) -> int:
        """Count today's appointments"""
        today = date.today().isoformat()
        result = self.db.fetchone("""
            SELECT COUNT(*) as count
            FROM patients
            WHERE nextAppointment = ?
        """, (today,))
        return result['count'] if result else 0
