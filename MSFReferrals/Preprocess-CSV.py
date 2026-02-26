#!/usr/bin/env python3
"""
CSV Pre-Processor for MSF Referrals
Cleans and enriches CSV data before SQLite conversion
"""

import csv
import sys
import re
from datetime import datetime

def extract_email(text):
    """Extract email address from text using regex"""
    if not text:
        return None
    
    # Simple email regex
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    matches = re.findall(email_pattern, text)
    return matches[0] if matches else None

def parse_date(date_str):
    """Parse date string to check if valid"""
    if not date_str or not date_str.strip():
        return None
    
    try:
        # Try DD-MMM-YY format
        return datetime.strptime(date_str.strip(), '%d-%b-%y')
    except:
        try:
            # Try D-MMM-YY format
            return datetime.strptime(date_str.strip(), '%d-%b-%y')
        except:
            return None

def preprocess_csv(input_file, output_file):
    """Preprocess CSV with smart cleanup and enrichment"""
    
    print(f"Processing {input_file}...")
    
    # Read input CSV
    with open(input_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"Loaded {len(rows)} rows")
    
    # Track changes
    changes = {
        'inferred_dates': 0,
        'extracted_emails': 0,
        'set_other_mode': 0,
        'flagged_missing_dates': []
    }
    
    cleaned_rows = []
    
    for idx, row in enumerate(rows, start=2):  # Start at 2 (row 1 is header)
        # Skip empty rows
        if not row.get('LAST NAME', '').strip():
            continue
        
        # Standardize contact methods
        for field in ['Email', 'Type of Contact', 'Type of Contact3']:
            if field in row and row[field]:
                value = row[field].strip()
                if value.lower() in ['email', 'e-mail']:
                    row[field] = 'Email'
                elif value.lower() in ['phone call', 'phone', 'call']:
                    row[field] = 'Phone call'
                elif value.lower() in ['fax']:
                    row[field] = 'Fax'
                elif value.lower() in ['other']:
                    row[field] = 'Other'
        
        # Extract email from comments if E-Mail column is empty
        if not row.get('E-Mail', '').strip():
            # Check Comments, Comments2, Comments4
            for comment_field in ['Comments', 'Comments2', 'Comments4']:
                email = extract_email(row.get(comment_field, ''))
                if email:
                    row['E-Mail'] = email
                    changes['extracted_emails'] += 1
                    print(f"  Row {idx}: Extracted email '{email}' from {comment_field}")
                    break
        
        # Process attempts - infer missing dates/modes
        attempts = [
            {
                'date_field': '1st Attempt to reach Patient/Referring MD',
                'mode_field': 'Email',
                'comment_field': 'Comments'
            },
            {
                'date_field': '2nd Attempt to reach Patient/Referring MD',
                'mode_field': 'Type of Contact',
                'comment_field': 'Comments2'
            },
            {
                'date_field': '3rd Attempt to reach Patient/Referring MD',
                'mode_field': 'Type of Contact3',
                'comment_field': 'Comments4'
            }
        ]
        
        for attempt in attempts:
            date_field = attempt['date_field']
            mode_field = attempt['mode_field']
            comment_field = attempt['comment_field']
            
            has_comment = bool(row.get(comment_field, '').strip())
            has_date = bool(row.get(date_field, '').strip())
            has_mode = bool(row.get(mode_field, '').strip())
            
            # If there's a comment but no date or mode
            if has_comment and (not has_date or not has_mode):
                
                # Set mode to "Other" if missing
                if not has_mode:
                    row[mode_field] = 'Other'
                    changes['set_other_mode'] += 1
                
                # Infer date if missing
                if not has_date:
                    inferred_date = None
                    date_source = None
                    
                    # Try "Date Complete Information received"
                    complete_date = row.get('Date Complete Information received', '').strip()
                    if complete_date and parse_date(complete_date):
                        inferred_date = complete_date
                        date_source = 'Date Complete Information received'
                    
                    # Try previous attempt dates
                    if not inferred_date:
                        for prev_attempt in attempts:
                            if prev_attempt == attempt:
                                break  # Stop at current attempt
                            prev_date = row.get(prev_attempt['date_field'], '').strip()
                            if prev_date and parse_date(prev_date):
                                inferred_date = prev_date
                                date_source = prev_attempt['date_field']
                                break
                    
                    # Try "Date Referral Received"
                    if not inferred_date:
                        received_date = row.get('Date Referral Received', '').strip()
                        if received_date and parse_date(received_date):
                            inferred_date = received_date
                            date_source = 'Date Referral Received'
                    
                    if inferred_date:
                        row[date_field] = inferred_date
                        changes['inferred_dates'] += 1
                        
                        # Flag this row
                        patient_name = f"{row.get('FIRST NAME', '')} {row.get('LAST NAME', '')}".strip()
                        flag_info = {
                            'row': idx,
                            'patient': patient_name,
                            'attempt': date_field,
                            'inferred_date': inferred_date,
                            'date_source': date_source,
                            'comment': row.get(comment_field, '')[:50]
                        }
                        changes['flagged_missing_dates'].append(flag_info)
                        print(f"  Row {idx}: Inferred date '{inferred_date}' from '{date_source}' for {date_field}")
        
        cleaned_rows.append(row)
    
    # Write output CSV
    if cleaned_rows:
        fieldnames = rows[0].keys()  # Use original column order
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_rows)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"Processing Complete!")
    print(f"{'='*60}")
    print(f"Output saved to: {output_file}")
    print(f"Rows processed: {len(cleaned_rows)}")
    print(f"\nChanges made:")
    print(f"  • Inferred dates: {changes['inferred_dates']}")
    print(f"  • Extracted emails: {changes['extracted_emails']}")
    print(f"  • Set mode to 'Other': {changes['set_other_mode']}")
    print(f"  • Flagged missing dates: {len(changes['flagged_missing_dates'])}")
    
    # Print flagged rows
    if changes['flagged_missing_dates']:
        print(f"\n{'='*60}")
        print(f"FLAGGED ROWS - Missing Dates (Inferred):")
        print(f"{'='*60}")
        for flag in changes['flagged_missing_dates']:
            print(f"\nRow {flag['row']}: {flag['patient']}")
            print(f"  Attempt: {flag['attempt']}")
            print(f"  Inferred Date: {flag['inferred_date']}")
            print(f"  Date Source: {flag['date_source']}")
            print(f"  Comment: {flag['comment']}")
    
    # Save flagged rows to file for review
    if changes['flagged_missing_dates']:
        flag_file = output_file.replace('.csv', '_FLAGGED_ROWS.txt')
        with open(flag_file, 'w', encoding='utf-8') as f:
            f.write("FLAGGED ROWS - Missing Dates (Inferred)\n")
            f.write("="*60 + "\n\n")
            for flag in changes['flagged_missing_dates']:
                f.write(f"Row {flag['row']}: {flag['patient']}\n")
                f.write(f"  Attempt: {flag['attempt']}\n")
                f.write(f"  Inferred Date: {flag['inferred_date']}\n")
                f.write(f"  Date Source: {flag['date_source']}\n")
                f.write(f"  Comment: {flag['comment']}\n\n")
        
        print(f"\nFlagged rows saved to: {flag_file}")
    
    print(f"\n{'='*60}")
    print(f"Ready for SQLite conversion!")
    print(f"{'='*60}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python Preprocess-CSV.py <input_csv> [output_csv]")
        print("Example: python Preprocess-CSV.py referral-status-20260224.csv referral-status-cleaned.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else input_file.replace('.csv', '-cleaned.csv')
    
    preprocess_csv(input_file, output_file)
