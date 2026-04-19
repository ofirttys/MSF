#!/usr/bin/env python3
"""
JSON Comparator for Michaeli Clinic Data
Compares two JSON files patient-by-patient, field-by-field
"""

import json
import sys
from typing import Dict, List, Any, Tuple


class JSONComparator:
    """Compare two clinic JSON files"""
    
    def __init__(self):
        self.differences = []
        self.stats = {
            "patients_in_1_only": [],
            "patients_in_2_only": [],
            "patients_different": [],
            "patients_identical": 0,
            "total_differences": 0
        }
    
    def compare_values(self, val1: Any, val2: Any, path: str) -> bool:
        """Compare two values recursively, return True if identical"""
        if type(val1) != type(val2):
            self.add_diff(path, f"Type mismatch: {type(val1).__name__} vs {type(val2).__name__}", val1, val2)
            return False
        
        if isinstance(val1, dict):
            return self.compare_dicts(val1, val2, path)
        elif isinstance(val1, list):
            return self.compare_lists(val1, val2, path)
        else:
            if val1 != val2:
                self.add_diff(path, "Value mismatch", val1, val2)
                return False
            return True
    
    def compare_dicts(self, dict1: Dict, dict2: Dict, path: str) -> bool:
        """Compare two dictionaries"""
        identical = True
        
        # Check for missing keys
        keys1 = set(dict1.keys())
        keys2 = set(dict2.keys())
        
        only_in_1 = keys1 - keys2
        only_in_2 = keys2 - keys1
        
        if only_in_1:
            self.add_diff(path, f"Fields only in first: {sorted(only_in_1)}", None, None)
            identical = False
        
        if only_in_2:
            self.add_diff(path, f"Fields only in second: {sorted(only_in_2)}", None, None)
            identical = False
        
        # Compare common keys
        for key in keys1 & keys2:
            new_path = f"{path}.{key}" if path else key
            if not self.compare_values(dict1[key], dict2[key], new_path):
                identical = False
        
        return identical
    
    def compare_lists(self, list1: List, list2: List, path: str) -> bool:
        """Compare two lists"""
        if len(list1) != len(list2):
            self.add_diff(path, f"Length mismatch: {len(list1)} vs {len(list2)}", None, None)
            return False
        
        identical = True
        for i, (item1, item2) in enumerate(zip(list1, list2)):
            new_path = f"{path}[{i}]"
            if not self.compare_values(item1, item2, new_path):
                identical = False
        
        return identical
    
    def add_diff(self, path: str, reason: str, val1: Any, val2: Any):
        """Record a difference"""
        self.differences.append({
            "path": path,
            "reason": reason,
            "value1": val1,
            "value2": val2
        })
        self.stats["total_differences"] += 1
    
    def compare_patients(self, p1: Dict, p2: Dict, patient_id: str) -> bool:
        """Compare two patient records"""
        self.differences = []  # Reset for this patient
        
        identical = self.compare_dicts(p1, p2, f"patient[{patient_id}]")
        
        if not identical:
            self.stats["patients_different"].append(patient_id)
        else:
            self.stats["patients_identical"] += 1
        
        return identical
    
    def compare_files(self, file1: str, file2: str, verbose: bool = False, max_diffs: int = 10):
        """Compare two JSON files"""
        print(f"Loading {file1}...")
        with open(file1, 'r', encoding='utf-8') as f:
            data1 = json.load(f)
        
        print(f"Loading {file2}...")
        with open(file2, 'r', encoding='utf-8') as f:
            data2 = json.load(f)
        
        # Compare metadata
        if data1.get("lastModified") != data2.get("lastModified"):
            print(f"\nℹ lastModified differs:")
            print(f"  File 1: {data1.get('lastModified')}")
            print(f"  File 2: {data2.get('lastModified')}")
        
        # Build patient lookup by ID
        patients1 = {p["patientID"]: p for p in data1.get("patients", [])}
        patients2 = {p["patientID"]: p for p in data2.get("patients", [])}
        
        print(f"\nPatient counts:")
        print(f"  File 1: {len(patients1)} patients")
        print(f"  File 2: {len(patients2)} patients")
        
        # Find patients only in one file
        ids1 = set(patients1.keys())
        ids2 = set(patients2.keys())
        
        self.stats["patients_in_1_only"] = sorted(ids1 - ids2)
        self.stats["patients_in_2_only"] = sorted(ids2 - ids1)
        
        if self.stats["patients_in_1_only"]:
            print(f"\n⚠ {len(self.stats['patients_in_1_only'])} patients only in file 1:")
            print(f"  {self.stats['patients_in_1_only'][:10]}")
            if len(self.stats["patients_in_1_only"]) > 10:
                print(f"  ... and {len(self.stats['patients_in_1_only']) - 10} more")
        
        if self.stats["patients_in_2_only"]:
            print(f"\n⚠ {len(self.stats['patients_in_2_only'])} patients only in file 2:")
            print(f"  {self.stats['patients_in_2_only'][:10]}")
            if len(self.stats["patients_in_2_only"]) > 10:
                print(f"  ... and {len(self.stats['patients_in_2_only']) - 10} more")
        
        # Compare common patients
        common_ids = sorted(ids1 & ids2)
        print(f"\nComparing {len(common_ids)} common patients...")
        
        diff_count = 0
        for patient_id in common_ids:
            identical = self.compare_patients(patients1[patient_id], patients2[patient_id], patient_id)
            
            if not identical:
                diff_count += 1
                if verbose and diff_count <= max_diffs:
                    print(f"\n  Patient {patient_id} differs:")
                    for diff in self.differences[:5]:  # Show first 5 diffs per patient
                        print(f"    - {diff['path']}: {diff['reason']}")
                        if diff['value1'] is not None or diff['value2'] is not None:
                            print(f"      File 1: {repr(diff['value1'])}")
                            print(f"      File 2: {repr(diff['value2'])}")
                    if len(self.differences) > 5:
                        print(f"    ... and {len(self.differences) - 5} more differences")
        
        # Print summary
        print("\n" + "="*60)
        print("COMPARISON SUMMARY")
        print("="*60)
        print(f"Identical patients: {self.stats['patients_identical']}")
        print(f"Different patients: {len(self.stats['patients_different'])}")
        print(f"Only in file 1: {len(self.stats['patients_in_1_only'])}")
        print(f"Only in file 2: {len(self.stats['patients_in_2_only'])}")
        
        if self.stats['patients_different']:
            print(f"\nPatients with differences ({len(self.stats['patients_different'])}):")
            print(f"  {self.stats['patients_different'][:20]}")
            if len(self.stats['patients_different']) > 20:
                print(f"  ... and {len(self.stats['patients_different']) - 20} more")
        
        if self.stats['patients_identical'] == len(common_ids) and \
           not self.stats['patients_in_1_only'] and \
           not self.stats['patients_in_2_only']:
            print("\n✓ FILES ARE IDENTICAL (except possibly lastModified)")
            return True
        else:
            print(f"\n✗ FILES DIFFER")
            return False


def main():
    if len(sys.argv) < 3:
        print("Usage: python json_comparator.py <file1.json> <file2.json> [--verbose] [--max-diffs=N]")
        print()
        print("Compares two JSON files patient-by-patient")
        print("  --verbose: Show detailed differences")
        print("  --max-diffs=N: Show max N different patients (default: 10)")
        sys.exit(1)
    
    file1 = sys.argv[1]
    file2 = sys.argv[2]
    verbose = "--verbose" in sys.argv
    
    max_diffs = 10
    for arg in sys.argv:
        if arg.startswith("--max-diffs="):
            max_diffs = int(arg.split("=")[1])
    
    comparator = JSONComparator()
    identical = comparator.compare_files(file1, file2, verbose, max_diffs)
    
    sys.exit(0 if identical else 1)


if __name__ == "__main__":
    main()
