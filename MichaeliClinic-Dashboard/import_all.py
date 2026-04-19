#!/usr/bin/env python3
"""
Master Import Script - Import All Data to SQLite
Imports patients, clinic days, and action items
"""

import os
import sys
import subprocess


def run_import(script, json_file, db_file, description):
    """Run an import script"""
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"{'='*60}")
    
    if not os.path.exists(json_file):
        print(f"⚠️  Skipping: {json_file} not found")
        return False
    
    result = subprocess.run(["python", script, json_file, db_file])
    
    if result.returncode == 0:
        print(f"✅ Success")
        return True
    else:
        print(f"❌ Failed")
        return False


def main():
    print("""
╔══════════════════════════════════════════════════════════════╗
║  MASTER IMPORT - All Data to SQLite                         ║
║  Michaeli Clinic Dashboard                                  ║
╚══════════════════════════════════════════════════════════════╝
""")
    
    db_file = "michaeli-clinic.db"
    
    # Delete existing database for clean import
    if os.path.exists(db_file):
        print(f"Removing existing database: {db_file}")
        os.remove(db_file)
    
    results = []
    
    # 1. Import patients (main data)
    print(f"\n{'='*60}")
    print("Step 1: Import Patients & Histories")
    print(f"{'='*60}")
    
    if not os.path.exists("michaeli-clinic.json"):
        print(f"⚠️  Skipping: michaeli-clinic.json not found")
        results.append(False)
    else:
        result = subprocess.run([
            "python", "json_sqlite_converter.py", 
            "json2sql", "michaeli-clinic.json", db_file
        ])
        if result.returncode == 0:
            print(f"✅ Success")
            results.append(True)
        else:
            print(f"❌ Failed")
            results.append(False)
    
    # 2. Import clinic days
    results.append(run_import(
        "import_clinic_days.py",
        "clinic-days.json",
        db_file,
        "Step 2: Import Clinic Days Configuration"
    ))
    
    # 3. Import action items
    results.append(run_import(
        "import_action_items.py",
        "action-items.json",
        db_file,
        "Step 3: Import Action Items"
    ))
    
    # Summary
    print(f"\n{'='*60}")
    print("IMPORT SUMMARY")
    print(f"{'='*60}")
    
    if all(results):
        print("✅ All data imported successfully!")
        print(f"\nDatabase ready: {db_file}")
        print("\nTables created:")
        print("  • patients (core data)")
        print("  • state_history")
        print("  • appointment_history")
        print("  • notes_history")
        print("  • clinic_days")
        print("  • action_items")
        print("  • metadata")
        print("\nReady for Python/Eel application development!")
    else:
        print("⚠️  Some imports failed or files were missing")
        print("Check messages above for details")
    
    print()


if __name__ == "__main__":
    main()
