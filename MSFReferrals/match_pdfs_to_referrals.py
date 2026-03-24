#!/usr/bin/env python3
"""
Match PDF files to referrals in the database
Handles various naming conventions: LastName.FirstName, LastName_FirstName, etc.
"""

import sqlite3
import re
import json
from pathlib import Path
from difflib import SequenceMatcher

# Configuration
DB_PATH = 'DB/referrals.db'
PDF_LIST_FILE = 'pdf_list.txt'
OUTPUT_MATCHES = 'matched_pdfs.json'
OUTPUT_BATCH = 'copy_pdfs.bat'
OUTPUT_REPORT = 'matching_report.txt'

# Destination for copying files
LINKED_FOLDER = r'X:\IVF\Referrals\MSFReferralsDashBoard\Referrals\Linked'


def normalize_name(name):
    """Normalize a name for comparison (lowercase, no spaces/punctuation)"""
    if not name:
        return ''
    # Remove all non-alphanumeric characters and convert to lowercase
    return re.sub(r'[^a-z0-9]', '', name.lower())


def extract_name_from_filename(filename):
    """
    Extract first and last name from PDF filename
    Handles: LastName.FirstName, LastName_FirstName, FirstName LastName, etc.
    Returns: (normalized_last, normalized_first, original_filename)
    """
    # Get just the filename without path and extension
    basename = Path(filename).stem
    
    # Remove common suffixes
    basename = re.sub(r'[_\-\s]*(referral|ref|form)s?[_\-\s]*', '', basename, flags=re.IGNORECASE)
    
    # Split on common separators (., _, -, space)
    parts = re.split(r'[._\-\s]+', basename)
    
    # Filter out empty parts
    parts = [p for p in parts if p]
    
    if len(parts) >= 2:
        # Assume first part is last name, second part is first name
        last_name = normalize_name(parts[0])
        first_name = normalize_name(parts[1])
        return (last_name, first_name)
    elif len(parts) == 1:
        # Only one name part - treat as last name
        return (normalize_name(parts[0]), '')
    else:
        return ('', '')


def similarity_score(str1, str2):
    """Calculate similarity between two strings (0.0 to 1.0)"""
    return SequenceMatcher(None, str1, str2).ratio()


def find_best_match(pdf_last, pdf_first, referrals):
    """
    Find the best matching referral for a PDF
    Returns: (referral_id, score, referral_info) or (None, 0, None)
    """
    best_match = None
    best_score = 0
    best_referral = None
    
    for ref_id, ref_first, ref_last in referrals:
        ref_first_norm = normalize_name(ref_first)
        ref_last_norm = normalize_name(ref_last)
        
        # Calculate similarity scores
        last_score = similarity_score(pdf_last, ref_last_norm)
        first_score = similarity_score(pdf_first, ref_first_norm) if pdf_first and ref_first_norm else 0
        
        # Weighted score (last name more important)
        if pdf_first and ref_first_norm:
            total_score = (last_score * 0.6) + (first_score * 0.4)
        else:
            total_score = last_score
        
        # Require high confidence for last name
        if last_score >= 0.85 and total_score > best_score:
            best_score = total_score
            best_match = ref_id
            best_referral = (ref_id, ref_first, ref_last)
    
    return (best_match, best_score, best_referral)


def main():
    # Connect to database
    print(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all referrals with first and last names
    cursor.execute("""
        SELECT referralID, patientFirstName, patientLastName 
        FROM referrals 
        WHERE patientFirstName IS NOT NULL 
        AND patientLastName IS NOT NULL
        AND TRIM(patientFirstName) != ''
        AND TRIM(patientLastName) != ''
    """)
    referrals = cursor.fetchall()
    print(f"Found {len(referrals)} referrals in database\n")
    
    # Read PDF list
    print(f"Reading PDF list from: {PDF_LIST_FILE}")
    with open(PDF_LIST_FILE, 'r', encoding='utf-8') as f:
        pdf_paths = [line.strip() for line in f if line.strip().endswith('.pdf')]
    print(f"Found {len(pdf_paths)} PDF files\n")
    
    # Match PDFs to referrals
    matches = []
    unmatched_pdfs = []
    matched_referral_ids = set()
    
    print("Matching PDFs to referrals...")
    for pdf_path in pdf_paths:
        pdf_last, pdf_first = extract_name_from_filename(pdf_path)
        
        if not pdf_last:
            unmatched_pdfs.append({
                'path': pdf_path,
                'reason': 'Could not extract name from filename'
            })
            continue
        
        ref_id, score, ref_info = find_best_match(pdf_last, pdf_first, referrals)
        
        if ref_id and score >= 0.85:  # High confidence threshold
            matches.append({
                'referral_id': ref_id,
                'referral_first': ref_info[1],
                'referral_last': ref_info[2],
                'pdf_path': pdf_path,
                'pdf_filename': Path(pdf_path).name,
                'confidence': round(score, 3)
            })
            matched_referral_ids.add(ref_id)
        else:
            unmatched_pdfs.append({
                'path': pdf_path,
                'reason': f'No confident match (best score: {score:.2f})',
                'extracted_name': f"{pdf_last} {pdf_first}".strip()
            })
    
    # Find referrals with no matching PDF
    unmatched_referrals = [
        {'id': r[0], 'first': r[1], 'last': r[2]}
        for r in referrals
        if r[0] not in matched_referral_ids
    ]
    
    conn.close()
    
    # Save matches as JSON
    print(f"\nSaving matches to: {OUTPUT_MATCHES}")
    with open(OUTPUT_MATCHES, 'w', encoding='utf-8') as f:
        json.dump(matches, f, indent=2)
    
    # Generate batch file to copy PDFs
    print(f"Generating batch file: {OUTPUT_BATCH}")
    with open(OUTPUT_BATCH, 'w', encoding='utf-8') as f:
        f.write('@echo off\n')
        f.write('REM Copy matched PDFs to Linked folder\n')
        f.write(f'REM Generated by match_pdfs_to_referrals.py\n\n')
        f.write(f'set DEST={LINKED_FOLDER}\n\n')
        f.write('echo Copying matched PDFs...\n')
        f.write('echo.\n\n')
        
        for i, match in enumerate(matches, 1):
            src = match['pdf_path']
            # Create new filename: LastName.FirstName_Referral_RefID.pdf
            new_name = f"{match['referral_last']}.{match['referral_first']}_Referral_{match['referral_id']}.pdf"
            new_name = re.sub(r'[<>:"|?*]', '_', new_name)  # Remove invalid filename chars
            
            f.write(f'echo [{i}/{len(matches)}] Copying: {match["pdf_filename"]}\n')
            f.write(f'copy /Y "{src}" "%DEST%\\{new_name}"\n')
            f.write(f'if errorlevel 1 echo ERROR copying {match["pdf_filename"]}\n')
            f.write('echo.\n\n')
        
        f.write('echo.\n')
        f.write(f'echo Completed! Copied {len(matches)} files.\n')
        f.write('pause\n')
    
    # Generate report
    print(f"Generating report: {OUTPUT_REPORT}")
    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write('=' * 80 + '\n')
        f.write('PDF MATCHING REPORT\n')
        f.write('=' * 80 + '\n\n')
        
        f.write(f"Total PDFs scanned: {len(pdf_paths)}\n")
        f.write(f"Total referrals in DB: {len(referrals)}\n\n")
        
        f.write(f"✓ Matched PDFs: {len(matches)}\n")
        f.write(f"✗ Unmatched PDFs: {len(unmatched_pdfs)}\n")
        f.write(f"✗ Referrals without PDF: {len(unmatched_referrals)}\n\n")
        
        f.write('=' * 80 + '\n')
        f.write('MATCHED PDFs (High Confidence)\n')
        f.write('=' * 80 + '\n\n')
        
        for match in sorted(matches, key=lambda x: x['confidence'], reverse=True):
            f.write(f"[{match['confidence']:.1%}] {match['referral_last']}, {match['referral_first']} (ID: {match['referral_id']})\n")
            f.write(f"         → {match['pdf_filename']}\n\n")
        
        f.write('\n' + '=' * 80 + '\n')
        f.write('UNMATCHED PDFs\n')
        f.write('=' * 80 + '\n\n')
        
        for item in unmatched_pdfs:
            f.write(f"✗ {Path(item['path']).name}\n")
            f.write(f"  Reason: {item['reason']}\n")
            if 'extracted_name' in item:
                f.write(f"  Extracted: {item['extracted_name']}\n")
            f.write(f"  Path: {item['path']}\n\n")
        
        f.write('\n' + '=' * 80 + '\n')
        f.write('REFERRALS WITHOUT MATCHING PDF\n')
        f.write('=' * 80 + '\n\n')
        
        for ref in sorted(unmatched_referrals, key=lambda x: (x['last'], x['first'])):
            f.write(f"✗ ID {ref['id']}: {ref['last']}, {ref['first']}\n")
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✓ Matched PDFs: {len(matches)}")
    print(f"✗ Unmatched PDFs: {len(unmatched_pdfs)}")
    print(f"✗ Referrals without PDF: {len(unmatched_referrals)}")
    print("\nFiles generated:")
    print(f"  - {OUTPUT_MATCHES} (matched PDFs data)")
    print(f"  - {OUTPUT_BATCH} (batch file to copy PDFs)")
    print(f"  - {OUTPUT_REPORT} (detailed report)")
    print("\nNext steps:")
    print(f"  1. Review {OUTPUT_REPORT}")
    print(f"  2. Run {OUTPUT_BATCH} to copy matched PDFs")
    print("=" * 80)


if __name__ == '__main__':
    main()
