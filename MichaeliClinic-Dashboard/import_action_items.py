#!/usr/bin/env python3
"""
Import action-items.json into SQLite
"""

import sqlite3
import json
import sys


def import_action_items(json_path: str, db_path: str = "michaeli-clinic.db"):
    """Import action items from JSON to SQLite"""
    
    # Load JSON
    print(f"Loading {json_path}...")
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Connect to database
    print(f"Connecting to {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS action_items (
            id TEXT PRIMARY KEY,
            tab TEXT NOT NULL,
            text TEXT NOT NULL,
            priority TEXT NOT NULL,
            addedAt TEXT NOT NULL,
            done INTEGER DEFAULT 0,
            doneAt TEXT
        )
    """)
    
    # Clear existing data
    cursor.execute("DELETE FROM action_items")
    
    # Import data from all tabs
    count = 0
    tabs = ['appointment', 'general', 'phone', 'email']
    
    for tab in tabs:
        items = data.get(tab, [])
        for item in items:
            cursor.execute("""
                INSERT INTO action_items (id, tab, text, priority, addedAt, done, doneAt)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                item.get("id"),
                tab,
                item.get("text"),
                item.get("priority"),
                item.get("addedAt"),
                1 if item.get("done", False) else 0,
                item.get("doneAt")
            ))
            count += 1
    
    conn.commit()
    conn.close()
    
    print(f"✓ Imported {count} action items across {len(tabs)} tabs")
    return count


def main():
    if len(sys.argv) < 2:
        print("Usage: python import_action_items.py <action-items.json> [database.db]")
        sys.exit(1)
    
    json_file = sys.argv[1]
    db_file = sys.argv[2] if len(sys.argv) > 2 else "michaeli-clinic.db"
    
    import_action_items(json_file, db_file)
    print(f"\nSuccess! Action items imported to {db_file}")


if __name__ == "__main__":
    main()
