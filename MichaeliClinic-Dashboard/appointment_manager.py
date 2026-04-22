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
    
    def get_appointments_by_date(self, date_str: str) -> Dict:
        """Get both future and past appointments for a specific date
        
        Args:
            date_str: Date in YYYY-MM-DD format
        
        Returns:
            Dict with 'future' and 'past' appointment lists
        """
        # Future appointments (from patients table)
        future = self.db.fetchall("""
            SELECT 
                p.patientID,
                p.patientName,
                p.partnerName,
                p.nextAppointment as date,
                p.appointmentTime as time,
                p.appointmentLocation as location,
                p.currentState,
                p.notes,
                1 as isFuture,
                CASE WHEN p.currentState = 'WAITING_FIRST_APPT' THEN 1 ELSE 0 END as isFirstAppt
            FROM patients p
            WHERE p.nextAppointment = ?
            ORDER BY p.appointmentTime
        """, (date_str,))
        
        # Past appointments (from appointment_history table)
        # Exclude if patient has same date as nextAppointment (to avoid duplicates)
        past = self.db.fetchall("""
            SELECT 
                ah.patientID,
                p.patientName,
                p.partnerName,
                ah.date,
                ah.time,
                ah.location,
                p.currentState,
                p.notes,
                ah.summary,
                0 as isFuture,
                0 as isFirstAppt
            FROM appointment_history ah
            JOIN patients p ON ah.patientID = p.patientID
            WHERE ah.date = ?
                AND p.nextAppointment != ?
            ORDER BY ah.time
        """, (date_str, date_str))
        
        return {
            'future': future,
            'past': past
        }
    
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
    
    def add_history(self, patient_id: str, appt_date: str, appt_time: str, location: Optional[str], summary: str) -> bool:
        """Add appointment to history"""
        try:
            timestamp = datetime.now().isoformat() + 'Z'
            self.db.execute("""
                INSERT INTO appointment_history (patientID, date, time, location, summary, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (patient_id, appt_date, appt_time, location or '', summary, timestamp))
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error adding appointment history: {e}")
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
