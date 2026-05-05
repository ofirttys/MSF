#!/usr/bin/env python3
"""
Add blocked_times table to MichaeliClinic database
Run this once to add time blocking capability
"""

import sqlite3
import sys
from pathlib import Path

# Database path
DB_PATH = "DB/michaeli-clinic.db"

def main():
    if not Path(DB_PATH).exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        print("Please ensure the database exists before running this migration.")
        sys.exit(1)
    
    print("=" * 60)
    print("MichaeliClinic - Add Blocked Times Table")
    print("=" * 60)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if table already exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='blocked_times'")
    if cursor.fetchone():
        print("⚠️  blocked_times table already exists!")
        print("Skipping table creation.")
    else:
        print("Creating blocked_times table...")
        cursor.execute("""
            CREATE TABLE blocked_times (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                startTime TEXT NOT NULL,
                endTime TEXT NOT NULL,
                title TEXT NOT NULL,
                notes TEXT,
                createdAt TEXT,
                createdBy TEXT
            )
        """)
        print("✓ blocked_times table created")
        
        # Create index for fast date lookups
        print("Creating index on date column...")
        cursor.execute("""
            CREATE INDEX idx_blocked_times_date ON blocked_times(date)
        """)
        print("✓ Index created")
    
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 60)
    print("Migration Complete!")
    print("=" * 60)
    print("\nYou can now use the time blocking feature:")
    print("  • Click 'Block' button to create time blocks")
    print("  • Blocks appear in day and week views in red")
    print("  • Click blocks to edit or delete them")
    print("")

if __name__ == '__main__':
    main()
