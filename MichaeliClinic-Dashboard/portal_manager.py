"""
Portal Manager
Handles patient portal access checking
"""

import xlrd
from datetime import datetime, timedelta
from typing import List, Dict


class PortalManager:
    """Manages portal access checking"""
    
    def __init__(self, db, portal_file_path: str):
        self.db = db
        self.portal_file_path = portal_file_path
    
    def get_missing_portal_access(self) -> Dict:
        """Find patients/partners missing portal access in next 3 weeks"""
        
        # 1. Read XLS and extract patient IDs from column A
        try:
            workbook = xlrd.open_workbook(self.portal_file_path)
            sheet = workbook.sheet_by_index(0)  # First sheet
            
            portal_user_ids = set()
            # Skip header row (row 0), start from row 1
            # Only read column A (column index 0)
            for row_idx in range(1, sheet.nrows):
                cell_value = sheet.cell_value(row_idx, 0)  # Column A
                if cell_value:
                    # Handle both string and numeric IDs
                    if isinstance(cell_value, float):
                        # Convert float to int to string (e.g., 123456.0 -> "123456")
                        patient_id = str(int(cell_value)).strip()
                    else:
                        # Already string, just strip
                        patient_id = str(cell_value).strip()
                    
                    if patient_id:  # Only add non-empty IDs
                        portal_user_ids.add(patient_id)
            
        except FileNotFoundError:
            return {
                'error': f'Portal file not found: {self.portal_file_path}',
                'missing': []
            }
        except Exception as e:
            return {
                'error': f'Error reading portal file: {str(e)}',
                'missing': []
            }
        
        # 2. Get date range (next 3 weeks)
        today = datetime.now().date()
        three_weeks_later = today + timedelta(days=21)
        
        # 3. Get patients with appointments in next 3 weeks
        patients_with_appts = self.db.fetchall("""
            SELECT 
                patientID, patientName, patientAlias, patientFirstName, 
                patientMiddleName, patientLastName, patientEmail,
                partnerID, partnerName, partnerAlias, partnerFirstName,
                partnerMiddleName, partnerLastName, partnerEmail,
                nextAppointment
            FROM patients
            WHERE nextAppointment IS NOT NULL
              AND nextAppointment >= ?
              AND nextAppointment <= ?
            ORDER BY nextAppointment
        """, (str(today), str(three_weeks_later)))
        
        # 4. Find who's missing portal access
        missing = []
        
        for patient in patients_with_appts:
            # Check patient
            if patient['patientID'] not in portal_user_ids:
                missing.append({
                    'id': patient['patientID'],
                    'name': self._format_name(patient, 'patient'),
                    'type': 'Patient',
                    'email': patient.get('patientEmail', ''),
                    'appointmentDate': patient['nextAppointment']
                })
            
            # Check partner if exists
            if patient.get('partnerID') and patient['partnerID'] not in portal_user_ids:
                missing.append({
                    'id': patient['partnerID'],
                    'name': self._format_name(patient, 'partner'),
                    'type': 'Partner',
                    'email': patient.get('partnerEmail', ''),
                    'appointmentDate': patient['nextAppointment']
                })
        
        return {
            'missing': missing,
            'total_portal_users': len(portal_user_ids)
        }
    
    def _format_name(self, patient: Dict, person_type: str) -> str:
        """Format name with alias support"""
        prefix = 'patient' if person_type == 'patient' else 'partner'
        
        # Use alias if available
        alias = patient.get(f'{prefix}Alias')
        if alias:
            return alias
        
        # Build from first/middle/last
        first = patient.get(f'{prefix}FirstName', '')
        middle = patient.get(f'{prefix}MiddleName', '')
        last = patient.get(f'{prefix}LastName', '')
        
        if first and last:
            name_parts = [first]
            if middle:
                name_parts.append(middle)
            name_parts.append(last)
            return ' '.join(name_parts)
        
        # Fallback to full name
        full_name = patient.get(f'{prefix}Name', '')
        if full_name:
            return full_name
        
        # Last resort: ID
        return patient.get(f'{prefix}ID', 'Unknown')
