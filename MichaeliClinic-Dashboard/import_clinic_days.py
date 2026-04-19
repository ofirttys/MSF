#!/usr/bin/env python3
"""
Import clinic-days.json into SQLite
"""

import sqlite3
import json
import sys


def import_clinic_days(json_path: str, db_path: str = "michaeli-clinic.db"):
    """Import clinic days configuration from JSON to SQLite"""
    
    # Load JSON
    print(f"Loading {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        clinic_days = json.load(f)
    
    # Connect to database
    print(f"Connecting to {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clinic_days (
            date TEXT PRIMARY KEY,
            downtown INTEGER DEFAULT 1,
            vaughan INTEGER DEFAULT 1,
            ivf INTEGER DEFAULT 1,
            survivorship INTEGER DEFAULT 0,
            md2 INTEGER DEFAULT 0
        )
    """)
    
    # Clear existing data
    cursor.execute("DELETE FROM clinic_days")
    
    # Import data
    count = 0
    for date_str, config in clinic_days.items():
        cursor.execute("""
            INSERT INTO clinic_days (date, downtown, vaughan, ivf, survivorship, md2)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            date_str,
            1 if config.get("downtown", False) else 0,  # Default FALSE
            1 if config.get("vaughan", False) else 0,   # Default FALSE
            1 if config.get("ivf", False) else 0,       # Default FALSE
            1 if config.get("survivorship", False) else 0,
            1 if config.get("md2", False) else 0
        ))
        count += 1
    
    conn.commit()
    conn.close()
    
    print(f"✓ Imported {count} clinic day configurations")
    return count


def main():
    if len(sys.argv) < 2:
        print("Usage: python import_clinic_days.py <clinic-days.json> [database.db]")
        sys.exit(1)
    
    json_file = sys.argv[1]
    db_file = sys.argv[2] if len(sys.argv) > 2 else "michaeli-clinic.db"
    
    import_clinic_days(json_file, db_file)
    print(f"\nSuccess! Clinic days imported to {db_file}")


if __name__ == "__main__":
    main()
