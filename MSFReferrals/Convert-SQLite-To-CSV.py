#!/usr/bin/env python3
"""
MSF Referrals Data Converter - SQLite to CSV
Converts SQLite database back to original CSV format for verification
"""

import sqlite3
import csv
import sys
import os
from datetime import datetime

def timestamp_to_date(timestamp):
    """Convert Unix timestamp to DD-MMM-YY format"""
    if not timestamp:
        return ''
    try:
        dt = datetime.fromtimestamp(timestamp)
        # Format as DD-MMM-YY (e.g., "13-Oct-83")
        return dt.strftime('%d-%b-%y')
    except:
        return ''

def convert_sqlite_to_csv(db_path, csv_path):
    """Convert SQLite database back to CSV format"""
    
    print(f"Converting {db_path} to CSV: {csv_path}...")
    
    if not os.path.exists(db_path):
        print(f"Error: Database file not found: {db_path}")
        sys.exit(1)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Access columns by name
    cursor = conn.cursor()
    
    # Get all referrals
    cursor.execute("""
        SELECT * FROM referrals
        ORDER BY referralID
    """)
    referrals = cursor.fetchall()
    
    print(f"Found {len(referrals)} referrals in database")
    
    # CSV column headers (matching original format)
    csv_columns = [
        'PID',
        'LAST NAME',
        'FIRST NAME',
        'E-Mail',
        'Phone',
        'DOB',
        'Age',
        'Service Requested',
        'Sub Service Requested',
        'New or Returning',
        'Referring MD/NP',
        'Requested Physician',
        'Date Referral Received',
        'Month of Referral',
        '1st Attempt to reach Patient/Referring MD',
        'Email',
        'Comments',
        '2nd Attempt to reach Patient/Referring MD',
        'Type of Contact',
        'Comments2',
        '3rd Attempt to reach Patient/Referring MD',
        'Type of Contact3',
        'Comments4',
        'Referral Complete',
        'Tasked To',
        'Date Complete Information received',
        'Notes',
        'Date'
    ]
    
    # Open CSV for writing
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_columns)
        writer.writeheader()
        
        for ref in referrals:
            # Calculate age from DOB
            age = ''
            if ref['patientDOB']:
                try:
                    dob = datetime.fromtimestamp(ref['patientDOB'])
                    today = datetime.now()
                    age = today.year - dob.year
                    if today.month < dob.month or (today.month == dob.month and today.day < dob.day):
                        age -= 1
                    age = str(age)
                except:
                    age = ''
            
            # Get attempt history for this referral
            cursor.execute("""
                SELECT attemptDate, attemptTime, attemptMode, attemptComment
                FROM attempt_history
                WHERE referralID = ?
                ORDER BY id
            """, (ref['referralID'],))
            attempts = cursor.fetchall()
            
            # Parse attempts (up to 3)
            attempt_1_date = ''
            attempt_1_mode = ''
            attempt_1_comment = ''
            attempt_2_date = ''
            attempt_2_mode = ''
            attempt_2_comment = ''
            attempt_3_date = ''
            attempt_3_mode = ''
            attempt_3_comment = ''
            
            if len(attempts) >= 1:
                attempt_1_date = timestamp_to_date(attempts[0]['attemptDate'])
                attempt_1_mode = attempts[0]['attemptMode'] or 'Phone'
                attempt_1_comment = attempts[0]['attemptComment'] or ''
            
            if len(attempts) >= 2:
                attempt_2_date = timestamp_to_date(attempts[1]['attemptDate'])
                attempt_2_mode = attempts[1]['attemptMode'] or 'Phone'
                attempt_2_comment = attempts[1]['attemptComment'] or ''
            
            if len(attempts) >= 3:
                attempt_3_date = timestamp_to_date(attempts[2]['attemptDate'])
                attempt_3_mode = attempts[2]['attemptMode'] or 'Phone'
                attempt_3_comment = attempts[2]['attemptComment'] or ''
            
            # Map referralStatus back to old "Referral Complete" values
            old_status = ''
            if ref['referralStatus'] == 'Completed':
                old_status = 'Complete'
            elif ref['referralStatus'] == 'Deferred':
                old_status = 'Deferred'
            elif ref['referralStatus'] in ['New', 'Pending', 'Info Received']:
                old_status = 'Pending'
            
            # Map referralType back to "New or Returning"
            new_or_returning = ''
            if ref['referralType'] == 'New':
                new_or_returning = 'New'
            elif ref['referralType'] == 'Previous':
                new_or_returning = 'Prev Pt'
            elif ref['referralType'] == 'Partner':
                new_or_returning = 'Partner'
            
            # Calculate month of referral
            month_of_referral = ''
            if ref['receivedDate']:
                try:
                    dt = datetime.fromtimestamp(ref['receivedDate'])
                    month_of_referral = str(dt.month)
                except:
                    pass
            
            # Build CSV row
            row = {
                'PID': ref['patientPID'] or '',
                'LAST NAME': ref['patientLastName'] or '',
                'FIRST NAME': ref['patientFirstName'] or '',
                'E-Mail': ref['patientEmail'] or '',
                'Phone': ref['patientPhone'] or '',
                'DOB': timestamp_to_date(ref['patientDOB']),
                'Age': age,
                'Service Requested': ref['serviceRequested'] or '',
                'Sub Service Requested': ref['subServiceRequested'] or '',
                'New or Returning': new_or_returning,
                'Referring MD/NP': ref['referringPhysicianName'] or '',
                'Requested Physician': ref['requestedPhysician'] or '',
                'Date Referral Received': timestamp_to_date(ref['receivedDate']),
                'Month of Referral': month_of_referral,
                '1st Attempt to reach Patient/Referring MD': attempt_1_date,
                'Email': attempt_1_mode,
                'Comments': attempt_1_comment,
                '2nd Attempt to reach Patient/Referring MD': attempt_2_date,
                'Type of Contact': attempt_2_mode,
                'Comments2': attempt_2_comment,
                '3rd Attempt to reach Patient/Referring MD': attempt_3_date,
                'Type of Contact3': attempt_3_mode,
                'Comments4': attempt_3_comment,
                'Referral Complete': old_status,
                'Tasked To': ref['taskedToPhysicianAdmin'] or '',
                'Date Complete Information received': timestamp_to_date(ref['completeInfoReceivedDate']),
                'Notes': ref['notes'] or '',
                'Date': timestamp_to_date(ref['notesDate'])
            }
            
            writer.writerow(row)
    
    conn.close()
    
    print(f"\nConversion complete!")
    print(f"Exported {len(referrals)} referrals to CSV")
    print(f"CSV saved to: {csv_path}")
    
    # Show some statistics
    print(f"\nVerification tips:")
    print(f"1. Compare row counts: Original CSV vs exported CSV")
    print(f"2. Spot check a few rows for accuracy")
    print(f"3. Check date formats are consistent")
    print(f"4. Verify status mappings (Pending/Complete/Deferred)")
    print(f"5. Verify referral types (New/Prev Pt/Partner)")

def compare_csv_files(original_csv, exported_csv):
    """Compare two CSV files and report differences"""
    
    if not os.path.exists(original_csv):
        print(f"Original CSV not found: {original_csv}")
        return
    
    if not os.path.exists(exported_csv):
        print(f"Exported CSV not found: {exported_csv}")
        return
    
    print(f"\nComparing CSV files...")
    print(f"Original: {original_csv}")
    print(f"Exported: {exported_csv}")
    
    # Read both files
    with open(original_csv, 'r', encoding='utf-8') as f:
        original_rows = list(csv.DictReader(f))
    
    with open(exported_csv, 'r', encoding='utf-8') as f:
        exported_rows = list(csv.DictReader(f))
    
    print(f"\nRow counts:")
    print(f"  Original: {len(original_rows)}")
    print(f"  Exported: {len(exported_rows)}")
    
    if len(original_rows) != len(exported_rows):
        print(f"  ⚠️  Row count mismatch! Difference: {abs(len(original_rows) - len(exported_rows))}")
    else:
        print(f"  ✓ Row counts match!")
    
    # Sample comparison (first 5 non-empty rows)
    print(f"\nSample comparison (first 5 rows with data):")
    
    original_data_rows = [r for r in original_rows if r.get('LAST NAME', '').strip()]
    exported_data_rows = [r for r in exported_rows if r.get('LAST NAME', '').strip()]
    
    for i in range(min(5, len(original_data_rows), len(exported_data_rows))):
        orig = original_data_rows[i]
        exp = exported_data_rows[i]
        
        print(f"\n  Row {i+1}:")
        
        # Compare key fields
        key_fields = ['LAST NAME', 'FIRST NAME', 'DOB', 'Service Requested', 'New or Returning', 'Referral Complete']
        
        all_match = True
        for field in key_fields:
            orig_val = orig.get(field, '').strip()
            exp_val = exp.get(field, '').strip()
            
            if orig_val == exp_val:
                print(f"    ✓ {field}: {orig_val}")
            else:
                print(f"    ✗ {field}: '{orig_val}' → '{exp_val}'")
                all_match = False
        
        if all_match:
            print(f"    ✓ All key fields match!")
    
    # Check for common issues
    print(f"\nChecking for common issues...")
    
    # Check date formats
    date_fields = ['DOB', 'Date Referral Received', '1st Attempt to reach Patient/Referring MD']
    date_format_ok = True
    
    for field in date_fields:
        for i, row in enumerate(exported_data_rows[:10]):
            date_val = row.get(field, '').strip()
            if date_val and not (len(date_val.split('-')) == 3):
                print(f"  ⚠️  Row {i+1}, {field}: Unexpected date format: {date_val}")
                date_format_ok = False
                break
    
    if date_format_ok:
        print(f"  ✓ Date formats look correct (DD-MMM-YY)")
    
    print(f"\nComparison complete!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python Convert-SQLite-To-CSV.py <database_file> [output_csv] [--compare original_csv]")
        print("\nExamples:")
        print("  python Convert-SQLite-To-CSV.py DB/referrals.db exported.csv")
        print("  python Convert-SQLite-To-CSV.py DB/referrals.db exported.csv --compare original.csv")
        sys.exit(1)
    
    db_path = sys.argv[1]
    csv_path = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] != '--compare' else "exported_referrals.csv"
    
    # Convert database to CSV
    convert_sqlite_to_csv(db_path, csv_path)
    
    # If --compare flag provided, compare with original
    if '--compare' in sys.argv:
        compare_idx = sys.argv.index('--compare')
        if compare_idx + 1 < len(sys.argv):
            original_csv = sys.argv[compare_idx + 1]
            compare_csv_files(original_csv, csv_path)
        else:
            print("\nError: --compare requires original CSV path")
            print("Example: python Convert-SQLite-To-CSV.py DB/referrals.db exported.csv --compare original.csv")
