#!/usr/bin/env python3
"""
4-Step Validation Test for Michaeli Clinic Converter
Tests the complete conversion pipeline with normalization
"""

import os
import sys
import subprocess

def run_command(cmd, description):
    """Run a command and report status"""
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print()
    
    result = subprocess.run(cmd, capture_output=False)
    
    if result.returncode != 0:
        print(f"\n❌ FAILED: {description}")
        return False
    else:
        print(f"\n✅ SUCCESS: {description}")
        return True

def main():
    print("""
╔══════════════════════════════════════════════════════════════╗
║  4-STEP VALIDATION TEST                                      ║
║  Michaeli Clinic Dashboard Converter                         ║
╚══════════════════════════════════════════════════════════════╝
""")
    
    # File names
    original_json = "michaeli-clinic.json"
    normalized_original = "michaeli-clinic-norm.json"
    temp_output = "temp_output.json"
    normalized_output = "temp_output-norm.json"
    db_file = "michaeli-clinic.db"
    
    # Check if original exists
    if not os.path.exists(original_json):
        print(f"❌ ERROR: {original_json} not found")
        print(f"Please run this script from the directory containing {original_json}")
        sys.exit(1)
    
    # Clean up old files
    for f in [db_file, normalized_original, temp_output, normalized_output]:
        if os.path.exists(f):
            os.remove(f)
            print(f"Cleaned up: {f}")
    
    # STEP 1: Convert original JSON to SQLite
    if not run_command(
        ["python", "json_sqlite_converter.py", "json2sql", original_json, db_file],
        "STEP 1: Convert original JSON → SQLite"
    ):
        sys.exit(1)
    
    # STEP 2: Normalize original JSON
    if not run_command(
        ["python", "json_normalizer.py", original_json, normalized_original],
        "STEP 2: Normalize original JSON"
    ):
        sys.exit(1)
    
    # STEP 3: Convert SQLite back to JSON
    if not run_command(
        ["python", "json_sqlite_converter.py", "sql2json", temp_output, db_file],
        "STEP 3: Convert SQLite → JSON (output)"
    ):
        sys.exit(1)
    
    # STEP 4: Normalize the output JSON
    if not run_command(
        ["python", "json_normalizer.py", temp_output, normalized_output],
        "STEP 4: Normalize SQLite output JSON"
    ):
        sys.exit(1)
    
    # STEP 5: Compare normalized files
    print(f"\n{'='*60}")
    print("STEP 5: Compare normalized original vs normalized output")
    print(f"{'='*60}\n")
    
    result = subprocess.run([
        "python", "json_comparator.py", 
        normalized_original, normalized_output,
        "--verbose", "--max-diffs=20"
    ])
    
    print(f"\n{'='*60}")
    if result.returncode == 0:
        print("✅ VALIDATION PASSED - Files are identical!")
        print(f"{'='*60}")
        print("\n🎉 Conversion is bit-exact (after normalization)")
        print(f"   Original → SQLite → JSON preserves all data")
        print(f"\nKey changes in normalized output:")
        print(f"   • phone/email → patientPhone/patientEmail (consolidated)")
        print(f"   • visitType/status removed (not used by HTA)")
        print(f"   • Patients sorted by ID")
        print(f"   • Fields in consistent order")
        sys.exit(0)
    else:
        print("⚠️  DIFFERENCES DETECTED")
        print(f"{'='*60}")
        print(f"\nReview the differences above.")
        print(f"If acceptable, these are the schema cleanups we planned:")
        print(f"   • Consolidated contact fields")
        print(f"   • Removed unused legacy fields")
        print(f"   • Normalized field order")
        print(f"\nFiles preserved for review:")
        print(f"   Original (normalized):  {normalized_original}")
        print(f"   Output (normalized):    {normalized_output}")
        sys.exit(1)

if __name__ == "__main__":
    main()
