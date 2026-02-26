#!/usr/bin/env python3
"""
MSF Referrals Data Converter - CSV to SQLite
Converts old CSV format directly to SQLite database
"""

import csv
import sqlite3
import sys
import os
from datetime import datetime
from pathlib import Path

def parse_date_to_timestamp(date_str):
    """Convert date string (DD-MMM-YY format) to Unix timestamp"""
    if not date_str or date_str.strip() == '':
        return None
    
    try:
        # Handle formats like "13-Oct-83", "17-Sep-24", "1-Jan-25"
        date_obj = datetime.strptime(date_str.strip(), '%d-%b-%y')
        
        # Adjust 2-digit year: 00-49 = 2000s, 50-99 = 1900s
        # (strptime already does this, but be explicit)
        
        # For dates before 1970, timestamp() may fail on Windows
        # Use alternative calculation
        try:
            return int(date_obj.timestamp())
        except (OSError, OverflowError):
            # Windows can't handle dates before 1970 with timestamp()
            # Calculate manually from epoch
            epoch = datetime(1970, 1, 1)
            delta = date_obj - epoch
            return int(delta.total_seconds())
    except ValueError:
        try:
            # Fallback to general parse
            date_obj = datetime.strptime(date_str.strip(), '%Y-%m-%d')
            try:
                return int(date_obj.timestamp())
            except (OSError, OverflowError):
                epoch = datetime(1970, 1, 1)
                delta = date_obj - epoch
                return int(delta.total_seconds())
        except:
            return None

def clean_phone(phone):
    """Clean phone number"""
    return phone.strip() if phone else ""

def clean_email(email):
    """Clean email address"""
    return email.strip().lower() if email else ""

def create_database(db_path):
    """Create SQLite database with schema"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables
    cursor.executescript("""
    -- Main referrals table
    CREATE TABLE referrals (
        referralID INTEGER PRIMARY KEY AUTOINCREMENT,
        addedToDBDate INTEGER,
        
        -- Referral Info
        referralDate INTEGER,
        receivedDate INTEGER,
        fileName TEXT,
        referringPhysicianName TEXT,
        referringPhysicianBilling TEXT,
        referringPhysicianFax TEXT,
        referringPhysicianPhone TEXT,
        referringPhysicianEmail TEXT,
        
        -- Service Info
        urgent INTEGER,
        requestedLocation TEXT,
        requestedPhysician TEXT,
        serviceRequested TEXT,
        subServiceRequested TEXT,
        referralType TEXT,
        
        -- Patient Info
        patientPID TEXT,
        patientMRN TEXT,
        patientFirstName TEXT,
        patientMiddleName TEXT,
        patientLastName TEXT,
        patientDOB INTEGER,
        patientPhone TEXT,
        patientEmail TEXT,
        patientAddress TEXT,
        patientHC TEXT,
        patientGenderAtBirth TEXT,
        emergencyContact TEXT,
        emergencyContactRelationship TEXT,
        
        -- Partner Info
        partnerPID TEXT,
        partnerMRN TEXT,
        partnerFirstName TEXT,
        partnerMiddleName TEXT,
        partnerLastName TEXT,
        partnerDOB INTEGER,
        partnerPhone TEXT,
        partnerEmail TEXT,
        partnerAddress TEXT,
        partnerHC TEXT,
        partnerGenderAtBirth TEXT,
        partnerEmergencyContact TEXT,
        partnerEmergencyContactRelationship TEXT,
        
        -- Referral Handling
        referralStatus TEXT,
        lastAttemptDate INTEGER,
        lastAttemptTime TEXT,
        lastAttemptMode TEXT,
        lastAttemptComment TEXT,
        phoneAttempts INTEGER DEFAULT 0,
        emailAttempts INTEGER DEFAULT 0,
        faxedBackDate INTEGER,
        completeInfoReceivedDate INTEGER,
        taskedToPhysicianAdmin TEXT,
        referralCompleteDate INTEGER,
        notes TEXT,
        notesDate INTEGER
    );

    -- Attempt history
    CREATE TABLE attempt_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referralID INTEGER,
        attemptDate INTEGER,
        attemptTime TEXT,
        attemptMode TEXT,
        attemptComment TEXT,
        FOREIGN KEY (referralID) REFERENCES referrals(referralID) ON DELETE CASCADE
    );

    -- Notes history
    CREATE TABLE notes_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referralID INTEGER,
        noteDate INTEGER,
        noteText TEXT,
        addedBy TEXT,
        FOREIGN KEY (referralID) REFERENCES referrals(referralID) ON DELETE CASCADE
    );

    -- Status change history
    CREATE TABLE status_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referralID INTEGER,
        oldStatus TEXT,
        newStatus TEXT,
        changedDate INTEGER,
        changedBy TEXT,
        FOREIGN KEY (referralID) REFERENCES referrals(referralID) ON DELETE CASCADE
    );

    -- Select options
    CREATE TABLE select_options (
        category TEXT,
        value TEXT,
        displayOrder INTEGER
    );

    -- Indexes
    CREATE INDEX idx_status ON referrals(referralStatus);
    CREATE INDEX idx_received_date ON referrals(receivedDate);
    CREATE INDEX idx_type ON referrals(referralType);
    CREATE INDEX idx_physician ON referrals(requestedPhysician);
    CREATE INDEX idx_last_name ON referrals(patientLastName);
    CREATE INDEX idx_service ON referrals(serviceRequested);
    CREATE INDEX idx_phone_attempts ON referrals(phoneAttempts);
    CREATE INDEX idx_email_attempts ON referrals(emailAttempts);
    CREATE INDEX idx_attempt_referral ON attempt_history(referralID);
    CREATE INDEX idx_notes_referral ON notes_history(referralID);
    CREATE INDEX idx_status_referral ON status_history(referralID);

    -- Enable WAL mode for better concurrency
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;
    PRAGMA cache_size=10000;
    """)
    
    # Insert select options
    select_options = [
        ('requestedLocations', ['Any', 'Downtown', 'Mississauga', 'Vaughan']),
        ('requestedPhysicians', ['First Available', 'Dr. Bacal', 'Dr. Greenblatt', 'Dr. Jones', 'Dr. Liu', 'Dr. Michaeli', 'Dr. Pereira', 'Dr. Russo', 'Dr. Shapiro']),
        ('servicesRequested', ['Infertility', 'EEF', 'ONC', 'SB', 'RPL', 'Donor', 'ARA', 'PGD', 'Gyne', 'Other']),
        ('referralType', ['New', 'Previous', 'Partner']),
        ('lastAttemptModes', ['Phone', 'E-Mail', 'Fax', 'In-Person', 'Mail', 'Text/SMS', 'Other']),
        ('physicianAdmins', ['CJ Admin', 'EG Admin', 'HS Admin', 'JM Admin', 'KL Admin', 'MR Admin', 'NP Admin', 'VB Admin', 'NursePrac Admin', 'Fellow Admin']),
        ('genderAtBirth', ['Female', 'Male', 'Other'])
    ]
    
    for category, values in select_options:
        for order, value in enumerate(values):
            cursor.execute(
                "INSERT INTO select_options (category, value, displayOrder) VALUES (?, ?, ?)",
                (category, value, order)
            )
    
    conn.commit()
    return conn

def convert_csv_to_sqlite(csv_path, db_path):
    """Main conversion function"""
    
    print(f"Converting {csv_path} to SQLite database: {db_path}...")
    
    # Ensure DB directory exists
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Delete existing database
    if os.path.exists(db_path):
        print(f"Removing existing database...")
        os.remove(db_path)
    
    # Create database
    print("Creating database schema...")
    conn = create_database(db_path)
    cursor = conn.cursor()
    print("Schema created successfully!")
    
    # Load CSV with auto-detect encoding
    print("Loading CSV data...")
    
    # Try different encodings
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'windows-1252', 'iso-8859-1']
    rows = None
    
    for encoding in encodings:
        try:
            with open(csv_path, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            print(f"Successfully loaded CSV with {encoding} encoding")
            break
        except UnicodeDecodeError:
            continue
    
    if rows is None:
        print("Error: Could not decode CSV file with any known encoding")
        sys.exit(1)
    
    print(f"Loaded {len(rows)} rows from CSV")
    
    # Convert rows
    print("Importing referrals...")
    row_count = 0
    inserted_count = 0
    now_timestamp = int(datetime.now().timestamp())
    
    for row in rows:
        row_count += 1
        
        # Skip empty rows
        if not row.get('LAST NAME', '').strip():
            print(f"Skipping empty row {row_count}")
            continue
        
        # Determine referral status
        old_status = row.get('Referral Complete', '').strip()
        has_first_attempt = bool(row.get('1st Attempt to reach Patient/Referring MD', '').strip())
        has_complete_info = bool(row.get('Date Complete Information received', '').strip())
        
        status = "New"
        if old_status == "Complete":
            status = "Completed"
        elif old_status in ["Cancelled", "Deferred"]:
            status = "Deferred"
        elif old_status == "Pending":
            if has_complete_info:
                status = "Info Received"
            elif has_first_attempt:
                status = "Pending"
            else:
                status = "New"
        
        # Determine referral type
        referral_type = "New"
        new_or_returning = row.get('New or Returning', '').strip()
        if new_or_returning:
            if any(x in new_or_returning.lower() for x in ['prev pt', 'previous', 'returning', 'return']):
                referral_type = "Previous"
            elif 'partner' in new_or_returning.lower():
                referral_type = "Partner"
            elif 'new' in new_or_returning.lower():
                referral_type = "New"
        
        # Determine gender based on service
        service_requested = row.get('Service Requested', '').strip()
        gender = ""
        if service_requested == "SB":
            gender = "Male"
        elif service_requested in ["EEF", "ONC", "Gyne", "RPL"]:
            gender = "Female"
        
        # Build attempt history
        attempts = []
        
        # Helper function to determine contact mode
        def get_contact_mode(type_field, email_field=None):
            """Determine contact mode from Type of Contact field"""
            if not type_field or not type_field.strip():
                # Try to infer from email field (for 1st attempt)
                if email_field and 'email' in email_field.lower():
                    return "E-Mail"
                return "Phone"  # Default
            
            mode_lower = type_field.strip().lower()
            if 'email' in mode_lower or 'e-mail' in mode_lower:
                return "E-Mail"
            elif 'phone' in mode_lower or 'call' in mode_lower:
                return "Phone"
            elif 'fax' in mode_lower:
                return "Fax"
            elif 'person' in mode_lower or 'visit' in mode_lower or 'in person' in mode_lower:
                return "In-Person"
            elif 'mail' in mode_lower or 'post' in mode_lower:
                return "Mail"
            elif 'text' in mode_lower or 'sms' in mode_lower:
                return "Text/SMS"
            else:
                return type_field.strip()  # Keep original if not recognized
        
        # 1st attempt - add if date OR comment exists
        attempt_1_date = row.get('1st Attempt to reach Patient/Referring MD', '').strip()
        attempt_1_comment = row.get('Comments', '').strip()
        
        if attempt_1_date or attempt_1_comment:
            attempts.append({
                'date': parse_date_to_timestamp(attempt_1_date) if attempt_1_date else None,
                'time': '',
                'mode': get_contact_mode(row.get('Email', ''), row.get('Email', '')),
                'comment': attempt_1_comment
            })
        
        # 2nd attempt - add if date OR comment exists
        attempt_2_date = row.get('2nd Attempt to reach Patient/Referring MD', '').strip()
        attempt_2_comment = row.get('Comments2', '').strip()
        
        if attempt_2_date or attempt_2_comment:
            attempts.append({
                'date': parse_date_to_timestamp(attempt_2_date) if attempt_2_date else None,
                'time': '',
                'mode': get_contact_mode(row.get('Type of Contact', '')),
                'comment': attempt_2_comment
            })
        
        # 3rd attempt - add if date OR comment exists
        attempt_3_date = row.get('3rd Attempt to reach Patient/Referring MD', '').strip()
        attempt_3_comment = row.get('Comments4', '').strip()
        
        if attempt_3_date or attempt_3_comment:
            attempts.append({
                'date': parse_date_to_timestamp(attempt_3_date) if attempt_3_date else None,
                'time': '',
                'mode': get_contact_mode(row.get('Type of Contact3', '')),
                'comment': attempt_3_comment
            })
        
        # Get last attempt
        last_attempt = attempts[-1] if attempts else None
        
        # Calculate attempt counts
        phone_count = sum(1 for a in attempts if a['mode'] in ['Phone', 'Phone call'])
        email_count = sum(1 for a in attempts if a['mode'] in ['E-Mail', 'Email'])
        
        # Insert referral
        cursor.execute("""
            INSERT INTO referrals (
                addedToDBDate, referralDate, receivedDate, fileName,
                referringPhysicianName, referringPhysicianBilling, referringPhysicianFax,
                referringPhysicianPhone, referringPhysicianEmail,
                urgent, requestedLocation, requestedPhysician, serviceRequested,
                subServiceRequested, referralType,
                patientPID, patientMRN, patientFirstName, patientMiddleName, patientLastName,
                patientDOB, patientPhone, patientEmail, patientAddress, patientHC, patientGenderAtBirth,
                emergencyContact, emergencyContactRelationship,
                partnerPID, partnerMRN, partnerFirstName, partnerMiddleName, partnerLastName,
                partnerDOB, partnerPhone, partnerEmail, partnerAddress, partnerHC, partnerGenderAtBirth,
                partnerEmergencyContact, partnerEmergencyContactRelationship,
                referralStatus, lastAttemptDate, lastAttemptTime, lastAttemptMode, lastAttemptComment,
                phoneAttempts, emailAttempts,
                faxedBackDate, completeInfoReceivedDate, taskedToPhysicianAdmin,
                referralCompleteDate, notes, notesDate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now_timestamp,
            parse_date_to_timestamp(row.get('Date Referral Received', '')),
            parse_date_to_timestamp(row.get('Date Referral Received', '')),
            '',  # fileName
            row.get('Referring MD/NP', '').strip(),
            '',  # referringPhysicianBilling
            '',  # referringPhysicianFax
            '',  # referringPhysicianPhone
            '',  # referringPhysicianEmail
            0,   # urgent
            '',  # requestedLocation
            row.get('Requested Physician', '').strip() or 'First Available',
            service_requested,
            row.get('Sub Service Requested', '').strip(),
            referral_type,
            row.get('PID', '').strip(),
            '',  # patientMRN
            row.get('FIRST NAME', '').strip(),
            '',  # patientMiddleName
            row.get('LAST NAME', '').strip(),
            parse_date_to_timestamp(row.get('DOB', '')),
            clean_phone(row.get('Phone', '')),
            clean_email(row.get('E-Mail', '')),
            '',  # patientAddress
            '',  # patientHC
            gender,
            '',  # emergencyContact
            '',  # emergencyContactRelationship
            '',  # partnerPID
            '',  # partnerMRN
            '',  # partnerFirstName
            '',  # partnerMiddleName
            '',  # partnerLastName
            None,  # partnerDOB
            '',  # partnerPhone
            '',  # partnerEmail
            '',  # partnerAddress
            '',  # partnerHC
            '',  # partnerGenderAtBirth
            '',  # partnerEmergencyContact
            '',  # partnerEmergencyContactRelationship
            status,
            last_attempt['date'] if last_attempt else None,
            last_attempt['time'] if last_attempt else '',
            last_attempt['mode'] if last_attempt else None,
            last_attempt['comment'] if last_attempt else None,
            phone_count,  # phoneAttempts
            email_count,  # emailAttempts
            None,  # faxedBackDate
            parse_date_to_timestamp(row.get('Date Complete Information received', '')),
            row.get('Tasked To', '').strip() or None,
            parse_date_to_timestamp(row.get('Date Complete Information received', '')) if old_status == "Complete" else None,
            row.get('Notes', '').strip() or None,
            parse_date_to_timestamp(row.get('Date', ''))
        ))
        
        referral_id = cursor.lastrowid
        
        # Insert attempt history
        for attempt in attempts:
            cursor.execute("""
                INSERT INTO attempt_history (referralID, attemptDate, attemptTime, attemptMode, attemptComment)
                VALUES (?, ?, ?, ?, ?)
            """, (
                referral_id,
                attempt['date'],
                attempt['time'],
                attempt['mode'],
                attempt['comment']
            ))
        
        inserted_count += 1
        
        if row_count % 100 == 0:
            print(f"Processed {row_count} rows...")
    
    conn.commit()
    
    print(f"\nConversion complete!")
    print(f"Converted {inserted_count} referrals from {row_count} total rows")
    print(f"Database saved to: {db_path}")
    
    # Show statistics
    cursor.execute("SELECT COUNT(*) FROM referrals")
    total = cursor.fetchone()[0]
    print(f"\nTotal referrals: {total}")
    
    cursor.execute("SELECT referralStatus, COUNT(*) FROM referrals GROUP BY referralStatus")
    print("\nStatus breakdown:")
    for status, count in cursor.fetchall():
        print(f"  {status}: {count}")
    
    cursor.execute("SELECT referralType, COUNT(*) FROM referrals GROUP BY referralType")
    print("\nReferral type breakdown:")
    for rtype, count in cursor.fetchall():
        print(f"  {rtype}: {count}")
    
    conn.close()
    print("\nDatabase ready for use!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python Convert-CSV-To-SQLite.py <input_csv> [output_db]")
        print("Example: python Convert-CSV-To-SQLite.py referral-status.csv DB/referrals.db")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else "referrals.db"
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    convert_csv_to_sqlite(csv_path, db_path)
