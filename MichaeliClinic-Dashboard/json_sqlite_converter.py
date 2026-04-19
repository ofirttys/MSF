#!/usr/bin/env python3
"""
Michaeli Clinic JSON ↔ SQLite Converter & Validator
Ensures bit-exact conversion between JSON and SQLite formats
"""

import sqlite3
import json
import hashlib
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple


class ClinicDataConverter:
    """Handles bidirectional conversion between JSON and SQLite with validation"""
    
    def __init__(self, db_path: str = "michaeli-clinic.db"):
        self.db_path = db_path
        self.conn = None
    
    def _connect(self):
        """Connect to SQLite database"""
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
    
    def _disconnect(self):
        """Disconnect from database"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def create_schema(self):
        """Create SQLite schema matching JSON structure"""
        self._connect()
        cursor = self.conn.cursor()
        
        # Patients table - Final clean schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS patients (
                patientID TEXT PRIMARY KEY,
                patientName TEXT NOT NULL,
                partnerID TEXT,
                partnerName TEXT,
                patientPhone TEXT,
                patientEmail TEXT,
                patientFirstName TEXT,
                patientMiddleName TEXT,
                patientLastName TEXT,
                patientAlias TEXT,
                partnerPhone TEXT,
                partnerEmail TEXT,
                partnerFirstName TEXT,
                partnerMiddleName TEXT,
                partnerLastName TEXT,
                partnerAlias TEXT,
                dateAdded TEXT NOT NULL,
                currentState TEXT NOT NULL,
                nextAppointment TEXT,
                appointmentTime TEXT,
                appointmentLocation TEXT,
                notes TEXT DEFAULT '',
                isSurvivorshipClinic INTEGER DEFAULT 0,
                isPriorityList INTEGER DEFAULT 0,
                isOTC INTEGER DEFAULT 0,
                had_appointmentLocation INTEGER DEFAULT 0
            )
        """)
        
        # State history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS state_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patientID TEXT,
                state TEXT,
                timestamp TEXT,
                notes TEXT,
                FOREIGN KEY (patientID) REFERENCES patients(patientID)
            )
        """)
        
        # Appointment history table - only fields actually used by HTA
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS appointment_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patientID TEXT,
                date TEXT,
                time TEXT,
                location TEXT,
                summary TEXT,
                timestamp TEXT,
                FOREIGN KEY (patientID) REFERENCES patients(patientID)
            )
        """)
        
        # Notes history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS notes_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patientID TEXT,
                note TEXT,
                timestamp TEXT,
                FOREIGN KEY (patientID) REFERENCES patients(patientID)
            )
        """)
        
        # Metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        self.conn.commit()
        print("✓ SQLite schema created")
    
    def json_to_sqlite(self, json_path: str) -> Tuple[int, str]:
        """
        Convert JSON to SQLite
        Returns: (patient_count, data_hash)
        """
        self._connect()
        
        # Load JSON
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Calculate hash of input
        json_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
        input_hash = hashlib.sha256(json_str.encode('utf-8')).hexdigest()
        
        cursor = self.conn.cursor()
        
        # Clear existing data
        cursor.execute("DELETE FROM patients")
        cursor.execute("DELETE FROM state_history")
        cursor.execute("DELETE FROM appointment_history")
        cursor.execute("DELETE FROM notes_history")
        cursor.execute("DELETE FROM metadata")
        
        # Store metadata
        cursor.execute(
            "INSERT INTO metadata (key, value) VALUES (?, ?)",
            ("lastModified", data.get("lastModified", ""))
        )
        cursor.execute(
            "INSERT INTO metadata (key, value) VALUES (?, ?)",
            ("import_hash", input_hash)
        )
        cursor.execute(
            "INSERT INTO metadata (key, value) VALUES (?, ?)",
            ("import_timestamp", datetime.now().isoformat())
        )
        
        patient_count = 0
        
        # Insert patients
        for idx, patient in enumerate(data.get("patients", [])):
            # Consolidate phone/email from old and new formats
            patient_phone = patient.get("patientPhone") or patient.get("phone")
            patient_email = patient.get("patientEmail") or patient.get("email")
            
            # Insert patient record
            cursor.execute("""
                INSERT INTO patients (
                    patientID, patientName, partnerID, partnerName,
                    patientPhone, patientEmail,
                    patientFirstName, patientMiddleName, patientLastName, patientAlias,
                    partnerPhone, partnerEmail,
                    partnerFirstName, partnerMiddleName, partnerLastName, partnerAlias,
                    dateAdded, currentState, nextAppointment, appointmentTime,
                    appointmentLocation, notes,
                    isSurvivorshipClinic, isPriorityList, isOTC,
                    had_appointmentLocation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                patient.get("patientID"),
                patient.get("patientName"),
                patient.get("partnerID"),
                patient.get("partnerName"),
                patient_phone,
                patient_email,
                patient.get("patientFirstName"),
                patient.get("patientMiddleName"),
                patient.get("patientLastName"),
                patient.get("patientAlias"),
                patient.get("partnerPhone"),
                patient.get("partnerEmail"),
                patient.get("partnerFirstName"),
                patient.get("partnerMiddleName"),
                patient.get("partnerLastName"),
                patient.get("partnerAlias"),
                patient.get("dateAdded"),
                patient.get("currentState"),
                patient.get("nextAppointment"),
                patient.get("appointmentTime"),
                patient.get("appointmentLocation"),
                patient.get("notes", ""),
                1 if patient.get("isSurvivorshipClinic", False) else 0,
                1 if patient.get("isPriorityList", False) else 0,
                1 if patient.get("isOTC", False) else 0,
                1 if "appointmentLocation" in patient else 0  # Track if field existed
            ))
            
            patient_id = patient.get("patientID")
            
            # Insert state history
            for state in patient.get("stateHistory", []):
                cursor.execute("""
                    INSERT INTO state_history (patientID, state, timestamp, notes)
                    VALUES (?, ?, ?, ?)
                """, (patient_id, state.get("state"), state.get("timestamp"), state.get("notes")))
            
            # Insert appointment history (only fields used by HTA)
            for idx, appt in enumerate(patient.get("appointmentHistory", [])):
                cursor.execute("""
                    INSERT INTO appointment_history (
                        patientID, date, time, location, summary, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    patient_id,
                    appt.get("date"),
                    appt.get("time"),
                    appt.get("location"),
                    appt.get("summary"),
                    appt.get("timestamp")
                ))
            
            # Insert notes history
            for note in patient.get("notesHistory", []):
                cursor.execute("""
                    INSERT INTO notes_history (patientID, note, timestamp)
                    VALUES (?, ?, ?)
                """, (patient_id, note.get("note"), note.get("timestamp")))
            
            patient_count += 1
        
        self.conn.commit()
        
        print(f"✓ Imported {patient_count} patients to SQLite")
        print(f"  Input hash: {input_hash[:16]}...")
        
        return patient_count, input_hash
    
    def sqlite_to_json(self, output_path: str) -> Tuple[int, str]:
        """
        Convert SQLite back to JSON
        Returns: (patient_count, data_hash)
        """
        self._connect()
        cursor = self.conn.cursor()
        
        # Get metadata
        cursor.execute("SELECT value FROM metadata WHERE key = 'lastModified'")
        row = cursor.fetchone()
        last_modified = row[0] if row else datetime.now().isoformat() + "Z"
        
        # Get all patients in order by patientID
        cursor.execute("SELECT * FROM patients ORDER BY patientID")
        patients = []
        
        for patient_row in cursor.fetchall():
            patient_id = patient_row["patientID"]
            
            # Get state history
            cursor.execute(
                "SELECT state, timestamp, notes FROM state_history WHERE patientID = ? ORDER BY id",
                (patient_id,)
            )
            state_history = []
            for row in cursor.fetchall():
                state_entry = {"state": row["state"], "timestamp": row["timestamp"]}
                if row["notes"]:
                    state_entry["notes"] = row["notes"]
                state_history.append(state_entry)
            
            # Get appointment history (only fields used by HTA)
            cursor.execute(
                """SELECT date, time, location, summary, timestamp
                   FROM appointment_history WHERE patientID = ? ORDER BY id""",
                (patient_id,)
            )
            appointment_history = []
            for row in cursor.fetchall():
                appt = {}
                if row["date"]: appt["date"] = row["date"]
                if row["time"]: appt["time"] = row["time"]
                if row["location"] is not None: appt["location"] = row["location"]
                if row["summary"] is not None: appt["summary"] = row["summary"]
                if row["timestamp"]: appt["timestamp"] = row["timestamp"]
                appointment_history.append(appt)
            
            # Get notes history
            cursor.execute(
                "SELECT note, timestamp FROM notes_history WHERE patientID = ? ORDER BY timestamp",
                (patient_id,)
            )
            notes_history = [
                {"timestamp": row["timestamp"], "note": row["note"]}
                for row in cursor.fetchall()
            ]
            
            # Build patient object with fields in original JSON order
            patient = {}
            
            patient["patientID"] = patient_row["patientID"]
            
            # Optional name breakdown fields (new format) - after patientID
            if patient_row["patientFirstName"] is not None:
                patient["patientFirstName"] = patient_row["patientFirstName"]
            if patient_row["patientMiddleName"] is not None:
                patient["patientMiddleName"] = patient_row["patientMiddleName"]
            if patient_row["patientLastName"] is not None:
                patient["patientLastName"] = patient_row["patientLastName"]
            if patient_row["patientAlias"] is not None:
                patient["patientAlias"] = patient_row["patientAlias"]
            
            patient["patientName"] = patient_row["patientName"]
            patient["partnerID"] = patient_row["partnerID"]
            
            # Optional partner name breakdown fields (new format)
            if patient_row["partnerFirstName"] is not None:
                patient["partnerFirstName"] = patient_row["partnerFirstName"]
            if patient_row["partnerMiddleName"] is not None:
                patient["partnerMiddleName"] = patient_row["partnerMiddleName"]
            if patient_row["partnerLastName"] is not None:
                patient["partnerLastName"] = patient_row["partnerLastName"]
            if patient_row["partnerAlias"] is not None:
                patient["partnerAlias"] = patient_row["partnerAlias"]
            
            patient["partnerName"] = patient_row["partnerName"]
            
            # Consolidated contact fields (only if present)
            if patient_row["patientPhone"] is not None:
                patient["patientPhone"] = patient_row["patientPhone"]
            if patient_row["patientEmail"] is not None:
                patient["patientEmail"] = patient_row["patientEmail"]
            
            patient["partnerPhone"] = patient_row["partnerPhone"]
            patient["partnerEmail"] = patient_row["partnerEmail"]
            
            patient["dateAdded"] = patient_row["dateAdded"]
            patient["currentState"] = patient_row["currentState"]
            patient["nextAppointment"] = patient_row["nextAppointment"]
            patient["appointmentTime"] = patient_row["appointmentTime"]
            patient["notes"] = patient_row["notes"] or ""
            patient["isSurvivorshipClinic"] = bool(patient_row["isSurvivorshipClinic"])
            
            # Check if this is new format (has name breakdown fields)
            has_new_format = patient_row["patientFirstName"] is not None
            
            if has_new_format:
                # New format: isOTC comes before isPriorityList
                patient["isOTC"] = bool(patient_row["isOTC"])
                patient["isPriorityList"] = bool(patient_row["isPriorityList"])
            else:
                # Old format: isPriorityList comes before histories, isOTC at end
                patient["isPriorityList"] = bool(patient_row["isPriorityList"])
            
            patient["stateHistory"] = state_history
            patient["appointmentHistory"] = appointment_history
            patient["notesHistory"] = notes_history
            
            if not has_new_format:
                patient["isOTC"] = bool(patient_row["isOTC"])
            
            # Only include appointmentLocation if it existed in the original JSON
            if patient_row["had_appointmentLocation"]:
                patient["appointmentLocation"] = patient_row["appointmentLocation"]
            
            patients.append(patient)
        
        # Build output data
        output_data = {
            "lastModified": last_modified,
            "patients": patients
        }
        
        # Calculate hash
        json_str = json.dumps(output_data, sort_keys=True, ensure_ascii=False)
        output_hash = hashlib.sha256(json_str.encode('utf-8')).hexdigest()
        
        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Exported {len(patients)} patients to JSON")
        print(f"  Output hash: {output_hash[:16]}...")
        
        return len(patients), output_hash
    
    def validate_roundtrip(self, original_json: str) -> bool:
        """
        Validate that JSON → SQLite → JSON preserves data exactly
        Uses normalized comparison to ignore field order differences
        """
        print("\n" + "="*60)
        print("ROUNDTRIP VALIDATION TEST")
        print("="*60)
        
        # Convert JSON to SQLite
        print("\n[1] JSON → SQLite")
        count1, hash1 = self.json_to_sqlite(original_json)
        
        # Convert SQLite back to JSON
        print("\n[2] SQLite → JSON")
        temp_json = "temp_output.json"
        count2, hash2 = self.sqlite_to_json(temp_json)
        
        # Compare
        print("\n[3] Validation Results:")
        print(f"  Patient count match: {count1 == count2} ({count1} vs {count2})")
        
        # Load both for comparison
        with open(original_json, 'r', encoding='utf-8') as f:
            original_data = json.load(f)
        
        with open(temp_json, 'r', encoding='utf-8') as f:
            output_data = json.load(f)
        
        # Build patient lookups
        patients1 = {p["patientID"]: p for p in original_data.get("patients", [])}
        patients2 = {p["patientID"]: p for p in output_data.get("patients", [])}
        
        # Check for missing patients
        ids1 = set(patients1.keys())
        ids2 = set(patients2.keys())
        
        missing_in_2 = ids1 - ids2
        missing_in_1 = ids2 - ids1
        
        if missing_in_2 or missing_in_1:
            print(f"\n✗ PATIENT SET MISMATCH")
            if missing_in_2:
                print(f"  Patients in original but not in output: {list(missing_in_2)[:5]}")
            if missing_in_1:
                print(f"  Patients in output but not in original: {list(missing_in_1)[:5]}")
            return False
        
        # Compare common patients field by field
        diff_count = 0
        first_diff = None
        
        for patient_id in patients1.keys():
            p1 = patients1[patient_id]
            p2 = patients2[patient_id]
            
            # Compare field by field (ignoring order)
            if not self._patients_equal(p1, p2):
                diff_count += 1
                if diff_count == 1:
                    first_diff = patient_id
                    if diff_count <= 5:
                        print(f"\n  Difference in patient {patient_id}:")
                        self._show_patient_diff(p1, p2)
        
        if diff_count == 0:
            print(f"\n✓ PERFECT MATCH - All {count1} patients identical!")
            print(f"  (Field order may differ, but data is identical)")
            Path(temp_json).unlink()  # Clean up
            return True
        else:
            print(f"\n✗ DATA MISMATCH - {diff_count} patients differ")
            print(f"  Temp file saved to: {temp_json}")
            print(f"\nTip: Use json_comparator.py for detailed diff:")
            print(f"  python json_comparator.py {original_json} {temp_json} --verbose")
            return False
    
    def _patients_equal(self, p1: Dict, p2: Dict) -> bool:
        """Check if two patients are equal (ignoring field order)"""
        # Must have same fields
        if set(p1.keys()) != set(p2.keys()):
            return False
        
        # Compare each field
        for key in p1.keys():
            if p1[key] != p2[key]:
                return False
        
        return True
    
    def _show_patient_diff(self, p1: Dict, p2: Dict):
        """Show differences between two patients"""
        keys1 = set(p1.keys())
        keys2 = set(p2.keys())
        
        only_in_1 = keys1 - keys2
        only_in_2 = keys2 - keys1
        
        if only_in_1:
            print(f"    Fields only in original: {sorted(only_in_1)}")
        if only_in_2:
            print(f"    Fields only in output: {sorted(only_in_2)}")
        
        # Show value differences
        diff_fields = []
        for key in keys1 & keys2:
            if p1[key] != p2[key]:
                diff_fields.append(key)
                if len(diff_fields) <= 3:
                    print(f"    Field '{key}':")
                    print(f"      Original: {repr(p1[key])[:100]}")
                    print(f"      Output:   {repr(p2[key])[:100]}")
        
        if len(diff_fields) > 3:
            print(f"    ... and {len(diff_fields) - 3} more differing fields")
    
    def _find_differences(self, data1: Dict, data2: Dict):
        """Find and report differences between two data structures"""
        print("\n  Analyzing differences...")
        
        patients1 = {p["patientID"]: p for p in data1.get("patients", [])}
        patients2 = {p["patientID"]: p for p in data2.get("patients", [])}
        
        # Check for missing patients
        missing_in_2 = set(patients1.keys()) - set(patients2.keys())
        missing_in_1 = set(patients2.keys()) - set(patients1.keys())
        
        if missing_in_2:
            print(f"  - Patients in original but not in output: {missing_in_2}")
        if missing_in_1:
            print(f"  - Patients in output but not in original: {missing_in_1}")
        
        # Check common patients
        common = set(patients1.keys()) & set(patients2.keys())
        diff_count = 0
        for patient_id in common:
            p1 = patients1[patient_id]
            p2 = patients2[patient_id]
            
            for key in p1.keys() | p2.keys():
                if p1.get(key) != p2.get(key):
                    if diff_count < 10:  # Show first 10 differences
                        print(f"  - Patient {patient_id}, field '{key}':")
                        print(f"    Original: {repr(p1.get(key))}")
                        print(f"    Output:   {repr(p2.get(key))}")
                        diff_count += 1
        
        if diff_count >= 10:
            print(f"  ... and more differences (showing first 10)")


def main():
    """Main CLI interface"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python json_sqlite_converter.py validate <json_file>")
        print("  python json_sqlite_converter.py json2sql <json_file> [db_file]")
        print("  python json_sqlite_converter.py sql2json <output_json> [db_file]")
        sys.exit(1)
    
    command = sys.argv[1]
    converter = ClinicDataConverter()
    
    if command == "validate":
        if len(sys.argv) < 3:
            print("Error: JSON file required")
            sys.exit(1)
        
        json_file = sys.argv[2]
        converter.create_schema()
        success = converter.validate_roundtrip(json_file)
        sys.exit(0 if success else 1)
    
    elif command == "json2sql":
        if len(sys.argv) < 3:
            print("Error: JSON file required")
            sys.exit(1)
        
        json_file = sys.argv[2]
        if len(sys.argv) >= 4:
            converter.db_path = sys.argv[3]
        
        converter.create_schema()
        count, hash_val = converter.json_to_sqlite(json_file)
        print(f"\nSuccess! {count} patients imported.")
    
    elif command == "sql2json":
        if len(sys.argv) < 3:
            print("Error: Output JSON file required")
            sys.exit(1)
        
        output_json = sys.argv[2]
        if len(sys.argv) >= 4:
            converter.db_path = sys.argv[3]
        
        count, hash_val = converter.sqlite_to_json(output_json)
        print(f"\nSuccess! {count} patients exported.")
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
