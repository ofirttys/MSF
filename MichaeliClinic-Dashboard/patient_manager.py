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
        """Get all patients with basic info (NO HISTORIES - use get_by_id for full details)"""
        patients = self.db.fetchall("""
            SELECT 
                patientID, patientName, partnerName, partnerID,
                patientAlias, patientFirstName, patientMiddleName, patientLastName,
                partnerAlias, partnerFirstName, partnerMiddleName, partnerLastName,
                patientPhone, patientEmail, partnerPhone, partnerEmail,
                currentState, nextAppointment, appointmentTime, appointmentLocation,
                dateAdded, notes,
                isSurvivorshipClinic, isPriorityList, isOTC
            FROM patients
            ORDER BY patientName
        """)
        
        # Convert integer flags to booleans (NO HISTORIES!)
        for patient in patients:
            patient['isSurvivorshipClinic'] = bool(patient.get('isSurvivorshipClinic', 0))
            patient['isPriorityList'] = bool(patient.get('isPriorityList', 0))
            patient['isOTC'] = bool(patient.get('isOTC', 0))
        
        return patients
    
    def get_paginated(self, limit: int = 50, offset: int = 0) -> Dict:
        """Get patients with pagination (SQL LIMIT/OFFSET for efficiency)
        
        Args:
            limit: Number of patients to return
            offset: Starting position
            
        Returns:
            {
                'patients': [...],
                'total': total_count,
                'has_more': True/False
            }
        """
        # Get total count
        count_result = self.db.fetchone("SELECT COUNT(*) as count FROM patients")
        total = count_result['count'] if count_result else 0
        
        # Get paginated patients
        patients = self.db.fetchall("""
            SELECT 
                patientID, patientName, partnerName, partnerID,
                patientAlias, patientFirstName, patientMiddleName, patientLastName,
                partnerAlias, partnerFirstName, partnerMiddleName, partnerLastName,
                patientPhone, patientEmail, partnerPhone, partnerEmail,
                currentState, nextAppointment, appointmentTime, appointmentLocation,
                dateAdded, notes,
                isSurvivorshipClinic, isPriorityList, isOTC
            FROM patients
            ORDER BY patientName
            LIMIT ? OFFSET ?
        """, (limit, offset))
        
        # Convert integer flags to booleans
        for patient in patients:
            patient['isSurvivorshipClinic'] = bool(patient.get('isSurvivorshipClinic', 0))
            patient['isPriorityList'] = bool(patient.get('isPriorityList', 0))
            patient['isOTC'] = bool(patient.get('isOTC', 0))
        
        return {
            'patients': patients,
            'total': total,
            'has_more': (offset + limit) < total
        }
    
    def get_filtered(self, state_filters: List[str] = None, search_term: str = None, 
                    special_filters: List[str] = None, sort_by: str = None) -> List[Dict]:
        """Get filtered patients using SQL WHERE clauses
        
        Args:
            state_filters: List of state names to filter by (OR logic)
            search_term: Search term for name/email/phone (searches all fields)
            special_filters: List of special filters (SURVIVORSHIP, OTC, PRIORITY, OVERDUE_APPOINTMENT)
            sort_by: Sort mode - 'appt-new' or 'appt-old' for SQL sorting by last appointment
        
        Returns:
            List of patient dictionaries matching filters
        """
        
        # If sorting by last appointment, use JOIN to get max date from appointment_history
        if sort_by in ['appt-new', 'appt-old']:
            query = """
                SELECT 
                    p.patientID, p.patientName, p.partnerName, p.partnerID,
                    p.patientAlias, p.patientFirstName, p.patientMiddleName, p.patientLastName,
                    p.partnerAlias, p.partnerFirstName, p.partnerMiddleName, p.partnerLastName,
                    p.patientPhone, p.patientEmail, p.partnerPhone, p.partnerEmail,
                    p.currentState, p.nextAppointment, p.appointmentTime, p.appointmentLocation,
                    p.dateAdded, p.notes,
                    p.isSurvivorshipClinic, p.isPriorityList, p.isOTC,
                    MAX(ah.date) as lastAppointmentDate
                FROM patients p
                LEFT JOIN appointment_history ah ON p.patientID = ah.patientID
                WHERE 1=1
            """
        else:
            # Regular query without JOIN (faster when not sorting by last appointment)
            query = """
                SELECT 
                    patientID, patientName, partnerName, partnerID,
                    patientAlias, patientFirstName, patientMiddleName, patientLastName,
                    partnerAlias, partnerFirstName, partnerMiddleName, partnerLastName,
                    patientPhone, patientEmail, partnerPhone, partnerEmail,
                    currentState, nextAppointment, appointmentTime, appointmentLocation,
                    dateAdded, notes,
                    isSurvivorshipClinic, isPriorityList, isOTC
                FROM patients
                WHERE 1=1
            """
        
        params = []
        
        # State filters (OR logic - match any state)
        if state_filters and len(state_filters) > 0:
            placeholders = ','.join('?' * len(state_filters))
            query += f" AND currentState IN ({placeholders})"
            params.extend(state_filters)
        
        # Search term (searches multiple fields with AND logic for multiple words)
        if search_term and search_term.strip():
            # Split search term into words for AND matching (like HTA)
            search_words = [w.strip() for w in search_term.strip().split() if w.strip()]
            
            if search_words:
                # For each word, check if it appears in ANY of the searchable fields
                for word in search_words:
                    search = f"%{word}%"
                    query += """ AND (
                        patientName LIKE ? OR
                        patientAlias LIKE ? OR
                        patientFirstName LIKE ? OR
                        patientMiddleName LIKE ? OR
                        patientLastName LIKE ? OR
                        partnerName LIKE ? OR
                        partnerAlias LIKE ? OR
                        partnerFirstName LIKE ? OR
                        partnerMiddleName LIKE ? OR
                        partnerLastName LIKE ? OR
                        patientPhone LIKE ? OR
                        patientEmail LIKE ? OR
                        partnerPhone LIKE ? OR
                        partnerEmail LIKE ? OR
                        patientID LIKE ? OR
                        partnerID LIKE ?
                    )"""
                    params.extend([search] * 16)  # 16 fields to search
        
        # Special filters
        if special_filters:
            if 'SURVIVORSHIP' in special_filters:
                query += " AND isSurvivorshipClinic = 1"
            if 'OTC' in special_filters:
                query += " AND isOTC = 1"
            if 'PRIORITY' in special_filters:
                query += " AND isPriorityList = 1"
            if 'OVERDUE_APPOINTMENT' in special_filters:
                # Get today's date in SQL
                query += """ AND (
                    (currentState = 'WAITING_FIRST_APPT' OR currentState = 'WAITING_NEXT_APPT')
                    AND nextAppointment IS NOT NULL
                    AND nextAppointment < date('now', 'localtime')
                )"""
        
        # Add GROUP BY if we used JOIN (for MAX aggregate)
        if sort_by in ['appt-new', 'appt-old']:
            query += " GROUP BY p.patientID"
        
        # Add ORDER BY based on sort mode
        if sort_by == 'appt-new':
            # Newest appointment first, nulls at end
            query += " ORDER BY lastAppointmentDate DESC NULLS LAST, patientName"
        elif sort_by == 'appt-old':
            # Oldest appointment first, nulls at end
            query += " ORDER BY lastAppointmentDate ASC NULLS LAST, patientName"
        else:
            # Default: sort by name
            query += " ORDER BY patientName"
        
        patients = self.db.fetchall(query, tuple(params))
        
        # Convert integer flags to booleans (NO HISTORIES - loaded only when viewing details!)
        for patient in patients:
            patient['isSurvivorshipClinic'] = bool(patient.get('isSurvivorshipClinic', 0))
            patient['isPriorityList'] = bool(patient.get('isPriorityList', 0))
            patient['isOTC'] = bool(patient.get('isOTC', 0))
        
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
    
    def get_by_ids(self, patient_ids: List[str]) -> List[Dict]:
        """Get multiple patients with histories in optimized batch queries
        
        Args:
            patient_ids: List of patient IDs to retrieve
            
        Returns:
            List of patient dictionaries with histories
        """
        if not patient_ids:
            return []
        
        # Build SQL IN clause
        placeholders = ','.join(['?'] * len(patient_ids))
        
        # Get all patients in ONE query
        patients = self.db.fetchall(f"""
            SELECT * FROM patients
            WHERE patientID IN ({placeholders})
        """, tuple(patient_ids))
        
        if not patients:
            return []
        
        # Get all state histories in ONE query
        state_histories = self.db.fetchall(f"""
            SELECT patientID, state, timestamp, notes
            FROM state_history
            WHERE patientID IN ({placeholders})
            ORDER BY patientID, timestamp
        """, tuple(patient_ids))
        
        # Get all appointment histories in ONE query
        appointment_histories = self.db.fetchall(f"""
            SELECT patientID, date, time, location, summary, timestamp
            FROM appointment_history
            WHERE patientID IN ({placeholders})
            ORDER BY patientID, id
        """, tuple(patient_ids))
        
        # Get all notes histories in ONE query
        notes_histories = self.db.fetchall(f"""
            SELECT patientID, timestamp, note
            FROM notes_history
            WHERE patientID IN ({placeholders})
            ORDER BY patientID, timestamp
        """, tuple(patient_ids))
        
        # Group histories by patient ID
        state_by_patient = {}
        appt_by_patient = {}
        notes_by_patient = {}
        
        for state in state_histories:
            pid = state['patientID']
            if pid not in state_by_patient:
                state_by_patient[pid] = []
            state_by_patient[pid].append({
                'state': state['state'],
                'timestamp': state['timestamp'],
                'notes': state['notes']
            })
        
        for appt in appointment_histories:
            pid = appt['patientID']
            if pid not in appt_by_patient:
                appt_by_patient[pid] = []
            appt_by_patient[pid].append({
                'date': appt['date'],
                'time': appt['time'],
                'location': appt['location'],
                'summary': appt['summary'],
                'timestamp': appt['timestamp']
            })
        
        for note in notes_histories:
            pid = note['patientID']
            if pid not in notes_by_patient:
                notes_by_patient[pid] = []
            notes_by_patient[pid].append({
                'timestamp': note['timestamp'],
                'note': note['note']
            })
        
        # Attach histories to patients
        for patient in patients:
            pid = patient['patientID']
            patient['stateHistory'] = state_by_patient.get(pid, [])
            patient['appointmentHistory'] = appt_by_patient.get(pid, [])
            patient['notesHistory'] = notes_by_patient.get(pid, [])
        
        return patients
    
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
            date_added = datetime.now().strftime('%Y-%m-%d')
            
            self.db.execute("""
                INSERT INTO patients (
                    patientID, patientName, patientAlias, patientFirstName, patientMiddleName, patientLastName,
                    partnerID, partnerName, partnerAlias, partnerFirstName, partnerMiddleName, partnerLastName,
                    patientPhone, patientEmail, partnerPhone, partnerEmail,
                    currentState, notes, dateAdded,
                    isSurvivorshipClinic, isPriorityList, isOTC
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                patient_data['patientID'],
                patient_data['patientName'],
                patient_data.get('patientAlias', ''),
                patient_data.get('patientFirstName', ''),
                patient_data.get('patientMiddleName', ''),
                patient_data.get('patientLastName', ''),
                patient_data.get('partnerID', ''),
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
                date_added,
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
                    partnerID = ?, partnerName = ?, partnerAlias = ?, partnerFirstName = ?, partnerMiddleName = ?, partnerLastName = ?,
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
                patient_data.get('partnerID', ''),
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
    
    def add_note_history(self, patient_id: str, note: str) -> bool:
        """Add entry to notes history"""
        try:
            timestamp = datetime.now().isoformat() + 'Z'
            self.db.execute("""
                INSERT INTO notes_history (patientID, note, timestamp)
                VALUES (?, ?, ?)
            """, (patient_id, note, timestamp))
            
            self.db.commit()
            return True
        except Exception as e:
            self.db.rollback()
            print(f"Error adding note history: {e}")
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
