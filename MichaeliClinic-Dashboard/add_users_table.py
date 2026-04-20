#!/usr/bin/env python3
"""
Add users table to MichaeliClinic database
Run this once to add user management capability
"""

import sqlite3
import sys
from pathlib import Path

# Database path
DB_PATH = "DB/michaeli-clinic.db"

def hash_password(password):
    """Simple hash function for passwords - matches referrals dashboard"""
    hash_val = 0
    salt = 'michaeli_clinic_2025'
    combined = password + salt
    
    for char in combined:
        hash_val = ((hash_val << 5) - hash_val) + ord(char)
        if hash_val > 0x7FFFFFFF:
            hash_val = hash_val - 0x100000000
        elif hash_val < -0x80000000:
            hash_val = hash_val + 0x100000000
    
    hex_hash = format(hash_val & 0xFFFFFFFF, 'x')
    while len(hex_hash) < 8:
        hex_hash = '0' + hex_hash
    
    extended = hex_hash
    for i in range(7):
        extended += _simple_hash(hex_hash + str(i))
    
    return extended[:64]

def _simple_hash(s):
    """Helper function for hash extension"""
    h = 0
    for char in s:
        h = ((h << 5) - h) + ord(char)
        if h > 0x7FFFFFFF:
            h = h - 0x100000000
        elif h < -0x80000000:
            h = h + 0x100000000
    
    hex_result = format(h & 0xFFFFFFFF, 'x')
    while len(hex_result) < 8:
        hex_result = '0' + hex_result
    return hex_result[:8]

def main():
    if not Path(DB_PATH).exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        print("Please ensure the database exists before running this migration.")
        sys.exit(1)
    
    print("=" * 60)
    print("MichaeliClinic - User Management Migration")
    print("=" * 60)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if users table already exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    if cursor.fetchone():
        print("⚠️  Users table already exists!")
        print("Skipping table creation.")
    else:
        print("Creating users table...")
        cursor.execute("""
            CREATE TABLE users (
                username TEXT PRIMARY KEY,
                passwordHash TEXT NOT NULL,
                lastLogin INTEGER,
                isAdmin INTEGER DEFAULT 0
            )
        """)
        print("✓ Users table created")
    
    # Migrate existing users from VALID_USERS (admin and jennia)
    existing_users = {
        'admin': '5f8eb2b05a1678d45a1678d55a1678d65a1678d75a1678d85a1678d95a1678da',
        'jennia': '5f8eb2b05a1678d45a1678d55a1678d65a1678d75a1678d85a1678d95a1678da'
    }
    
    print("\nMigrating existing users to database...")
    for username, password_hash in existing_users.items():
        cursor.execute("SELECT username FROM users WHERE username = ?", (username,))
        if cursor.fetchone():
            print(f"  ⚠️  User '{username}' already exists - skipping")
        else:
            cursor.execute(
                "INSERT INTO users (username, passwordHash, lastLogin, isAdmin) VALUES (?, ?, NULL, 1)",
                (username, password_hash)
            )
            print(f"  ✓ Migrated user '{username}' (admin)")
    
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 60)
    print("Migration Complete!")
    print("=" * 60)
    print("\nUsers table created with admin privileges:")
    print("  • admin (password: same as before)")
    print("  • jennia (password: same as before)")
    print("\nYou can now:")
    print("  1. Login as admin or jennia")
    print("  2. Click 'Users' button to manage users")
    print("  3. Add new users, change passwords, etc.")
    print("\nNote: The code-based VALID_USERS will still work as fallback.")
    print("")

if __name__ == '__main__':
    main()
