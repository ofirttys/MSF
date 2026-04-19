"""
Patient Manager
Handles all patient-related database operations
"""

from typing import List, Dict, Optional
from datetime import datetime


class PatientManager:
    """Manages patient data and operations"""
    
    def __init__(self, db):
        self.db = db
    
    def get_all(self) -> List[Dict]:
        """Get all patients with basic info"""
        patients = self.db.fetchall("""
            SELECT 
                patientID, patientName, partnerName,
                patientPhone, patientEmail,
                currentState, nextAppointment, appointmentTime, appointmentLocation,
                isSurvivorshipClinic, isPriorityList, isOTC
            FROM patients
            ORDER BY patientName
        """)
        
        # Convert integer flags to booleans and add appointment history
        for patient in patients:
            patient['isSurvivorshipClinic'] = bool(patient.get('isSurvivorshipClinic', 0))
            patient['isPriorityList'] = bool(patient.get('isPriorityList', 0))
            patient['isOTC'] = bool(patient.get('isOTC', 0))
            
            # Load appointment history for sorting
            patient['appointmentHistory'] = self.db.fetchall("""
                SELECT date, time, location, summary
                FROM appointment_history
                WHERE patientID = ?
                ORDER BY date DESC
            """, (patient['patientID'],))
        
        return patients
    
    def get_by_id(self, patient_id: str) -> Optional[Dict]:
        """Get full patient details including histories"""
        # Get patient
        patient = self.db.fetchone("""
            SELECT * FROM patients WHERE patientID = ?
        """, (patient_id,))
        
        if not patient:
            return None
        
        # Get state history
        patient['stateHistory'] = self.db.fetchall("""
            SELECT state, timestamp, notes
            FROM state_history
            WHERE patientID = ?
            ORDER BY timestamp
        """, (patient_id,))
        
        # Get appointment history
        patient['appointmentHistory'] = self.db.fetchall("""
            SELECT date, time, location, summary, timestamp
            FROM appointment_history
            WHERE patientID = ?
            ORDER BY id
        """, (patient_id,))
        
        # Get notes history
        patient['notesHistory'] = self.db.fetchall("""
            SELECT timestamp, note
            FROM notes_history
            WHERE patientID = ?
            ORDER BY timestamp
        """, (patient_id,))
        
        return patient
    
    def search(self, query: str) -> List[Dict]:
        """Search patients by name, email, or phone"""
        search_term = f"%{query}%"
        return self.db.fetchall("""
            SELECT 
                patientID, patientName, partnerName,
                patientPhone, patientEmail,
                currentState, nextAppointment
            FROM patients
            WHERE patientName LIKE ?
               OR partnerName LIKE ?
               OR patientPhone LIKE ?
               OR patientEmail LIKE ?
            ORDER BY patientName
            LIMIT 50
        """, (search_term, search_term, search_term, search_term))
    
    def add(self, patient_data: Dict) -> bool:
        """Add a new patient"""
        try:
            timestamp = datetime.now().isoformat() + 'Z'
            
            self.db.execute("""
                INSERT INTO patients (
                    patientID, patientName, patientAlias, patientFirstName, patientMiddleName, patientLastName,
                    partnerName, partnerAlias, partnerFirstName, partnerMiddleName, partnerLastName,
                    patientPhone, patientEmail, partnerPhone, partnerEmail,
                    currentState, notes,
                    isSurvivorshipClinic, isPriorityList, isOTC
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                patient_data['patientID'],
                patient_data['patientName'],
                patient_data.get('patientAlias', ''),
                patient_data.get('patientFirstName', ''),
                patient_data.get('patientMiddleName', ''),
                patient_data.get('patientLastName', ''),
                patient_data.get('partnerName', ''),
                patient_data.get('partnerAlias', ''),
                patient_data.get('partnerFirstName', ''),
                patient_data.get('partnerMiddleName', ''),
                patient_data.get('partnerLastName', ''),
                patient_data['patientPhone'],
                patient_data['patientEmail'],
                patient_data.get('partnerPhone', ''),
                patient_data.get('partnerEmail', ''),
                patient_data.get('currentState', 'WAITING_FIRST_APPT_SCHEDULE'),
                patient_data.get('notes', ''),
                1 if patient_data.get('isSurvivorshipClinic', False) else 0,
                1 if patient_data.get('isPriorityList', False) else 0,
                1 if patient_data.get('isOTC', False) else 0
            ))
            
            # Add initial state to history
            self.db.execute("""
                INSERT INTO state_history (patientID, state, timestamp, notes)
                VALUES (?, ?, ?, ?)
            """, (
                patient_data['patientID'],
                patient_data.get('currentState', 'WAITING_FIRST_APPT_SCHEDULE'),
                timestamp,
                'Initial state'
            ))
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error adding patient: {e}")
            return False
    
    def update(self, patient_id: str, patient_data: Dict) -> bool:
        """Update existing patient"""
        try:
            self.db.execute("""
                UPDATE patients SET
                    patientName = ?, patientAlias = ?, patientFirstName = ?, patientMiddleName = ?, patientLastName = ?,
                    partnerName = ?, partnerAlias = ?, partnerFirstName = ?, partnerMiddleName = ?, partnerLastName = ?,
                    patientPhone = ?, patientEmail = ?, partnerPhone = ?, partnerEmail = ?,
                    notes = ?,
                    isSurvivorshipClinic = ?, isPriorityList = ?, isOTC = ?
                WHERE patientID = ?
            """, (
                patient_data['patientName'],
                patient_data.get('patientAlias', ''),
                patient_data.get('patientFirstName', ''),
                patient_data.get('patientMiddleName', ''),
                patient_data.get('patientLastName', ''),
                patient_data.get('partnerName', ''),
                patient_data.get('partnerAlias', ''),
                patient_data.get('partnerFirstName', ''),
                patient_data.get('partnerMiddleName', ''),
                patient_data.get('partnerLastName', ''),
                patient_data['patientPhone'],
                patient_data['patientEmail'],
                patient_data.get('partnerPhone', ''),
                patient_data.get('partnerEmail', ''),
                patient_data.get('notes', ''),
                1 if patient_data.get('isSurvivorshipClinic', False) else 0,
                1 if patient_data.get('isPriorityList', False) else 0,
                1 if patient_data.get('isOTC', False) else 0,
                patient_id
            ))
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error updating patient: {e}")
            return False
    
    def update_state(self, patient_id: str, new_state: str, notes: Optional[str] = None) -> bool:
        """Update patient state and log to history"""
        try:
            # Update current state
            self.db.execute("""
                UPDATE patients
                SET currentState = ?
                WHERE patientID = ?
            """, (new_state, patient_id))
            
            # Add to state history
            timestamp = datetime.now().isoformat() + 'Z'
            self.db.execute("""
                INSERT INTO state_history (patientID, state, timestamp, notes)
                VALUES (?, ?, ?, ?)
            """, (patient_id, new_state, timestamp, notes))
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error updating patient state: {e}")
            return False
    
    def update_notes(self, patient_id: str, notes: str) -> bool:
        """Update patient notes"""
        try:
            self.db.execute("""
                UPDATE patients
                SET notes = ?
                WHERE patientID = ?
            """, (notes, patient_id))
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error updating notes: {e}")
            return False
    
    def count_total(self) -> int:
        """Count total patients"""
        result = self.db.fetchone("SELECT COUNT(*) as count FROM patients")
        return result['count'] if result else 0
    
    def count_by_state(self) -> Dict[str, int]:
        """Count patients by state"""
        rows = self.db.fetchall("""
            SELECT currentState, COUNT(*) as count
            FROM patients
            GROUP BY currentState
        """)
        return {row['currentState']: row['count'] for row in rows}
