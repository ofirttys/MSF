#!/usr/bin/env python3
"""
JSON Normalizer for Michaeli Clinic Data
Sorts patients by ID and standardizes field order for easy comparison
"""

import json
import sys
from typing import Dict, List, Any


def normalize_patient(patient: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a single patient record with consistent field order
    """
    normalized = {}
    
    # Define field order (all possible fields in logical groups)
    field_order = [
        # IDs
        "patientID",
        
        # Patient name fields (new format)
        "patientFirstName",
        "patientMiddleName", 
        "patientLastName",
        "patientAlias",
        "patientName",
        
        # Partner ID
        "partnerID",
        
        # Partner name fields (new format)
        "partnerFirstName",
        "partnerMiddleName",
        "partnerLastName",
        "partnerAlias",
        "partnerName",
        
        # Contact info (new format)
        "patientPhone",
        "patientEmail",
        "partnerPhone",
        "partnerEmail",
        
        # Contact info (old format)
        "phone",
        "email",
        
        # Dates and state
        "dateAdded",
        "currentState",
        "nextAppointment",
        "appointmentTime",
        
        # Notes and flags
        "notes",
        "isSurvivorshipClinic",
        "isOTC",
        "isPriorityList",
        
        # Histories
        "stateHistory",
        "appointmentHistory",
        "notesHistory",
        
        # Location
        "appointmentLocation"
    ]
    
    # Add fields in defined order (only if they exist in original)
    for field in field_order:
        if field in patient:
            normalized[field] = patient[field]
    
    # Add any remaining fields that weren't in our order (just in case)
    for field, value in patient.items():
        if field not in normalized:
            normalized[field] = value
            print(f"Warning: Unexpected field '{field}' in patient {patient.get('patientID', 'unknown')}")
    
    return normalized


def normalize_appointment(appt: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize appointment history entry"""
    normalized = {}
    
    field_order = [
        "date",
        "time",
        "visitType",
        "status",
        "location",
        "summary",
        "timestamp"
    ]
    
    for field in field_order:
        if field in appt:
            normalized[field] = appt[field]
    
    return normalized


def normalize_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize state history entry"""
    normalized = {}
    
    field_order = [
        "state",
        "timestamp",
        "notes"
    ]
    
    for field in field_order:
        if field in state:
            normalized[field] = state[field]
    
    return normalized


def normalize_note(note: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize notes history entry"""
    normalized = {}
    
    field_order = [
        "timestamp",
        "note"
    ]
    
    for field in field_order:
        if field in note:
            normalized[field] = note[field]
    
    return normalized


def normalize_json(input_path: str, output_path: str, sort_patients: bool = True):
    """
    Normalize JSON file - sort patients and standardize field order
    """
    print(f"Loading {input_path}...")
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Normalize each patient
    normalized_patients = []
    for patient in data.get("patients", []):
        normalized = normalize_patient(patient)
        
        # Normalize nested arrays
        if "stateHistory" in normalized:
            normalized["stateHistory"] = [
                normalize_state(s) for s in normalized["stateHistory"]
            ]
        
        if "appointmentHistory" in normalized:
            normalized["appointmentHistory"] = [
                normalize_appointment(a) for a in normalized["appointmentHistory"]
            ]
        
        if "notesHistory" in normalized:
            normalized["notesHistory"] = [
                normalize_note(n) for n in normalized["notesHistory"]
            ]
        
        normalized_patients.append(normalized)
    
    # Sort patients by ID if requested
    if sort_patients:
        print("Sorting patients by ID...")
        normalized_patients.sort(key=lambda p: p.get("patientID", ""))
    
    # Build output
    output = {
        "lastModified": data.get("lastModified", ""),
        "patients": normalized_patients
    }
    
    # Write normalized JSON
    print(f"Writing to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Normalized {len(normalized_patients)} patients")
    print(f"  Output: {output_path}")


def main():
    if len(sys.argv) < 3:
        print("Usage: python json_normalizer.py <input.json> <output.json> [--no-sort]")
        print()
        print("Normalizes JSON by:")
        print("  1. Sorting patients by ID (unless --no-sort)")
        print("  2. Standardizing field order within each patient")
        print("  3. Standardizing field order in histories")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    sort_patients = "--no-sort" not in sys.argv
    
    normalize_json(input_file, output_file, sort_patients)


if __name__ == "__main__":
    main()
