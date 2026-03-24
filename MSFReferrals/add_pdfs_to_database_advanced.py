#!/usr/bin/env python3
"""
Add matched PDFs to the database - ADVANCED VERSION
Reads matched_pdfs.json and updates the referrals table with fileName field

Options:
- DRY_RUN: Preview changes without modifying database
- OVERWRITE_EXISTING: Update referrals that already have files
- BACKUP_DB: Create database backup before updating
"""

import sqlite3
import json
import os
import shutil
from pathlib import Path
from datetime import datetime

# Configuration
DB_PATH = 'DB/referrals.db'
MATCHED_JSON = 'matched_pdfs.json'

# Options
DRY_RUN = True  # Set to False to actually update the database
OVERWRITE_EXISTING = False  # Set to True to update referrals that already have files
BACKUP_DB = True  # Set to True to backup database before updating

def backup_database():
    """Create a backup of the database"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f"{DB_PATH}.backup_{timestamp}"
    
    print(f"Creating backup: {backup_path}")
    shutil.copy2(DB_PATH, backup_path)
    print(f"✓ Backup created\n")
    return backup_path

def main():
    # Check if files exist
    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found at {DB_PATH}")
        return
    
    if not os.path.exists(MATCHED_JSON):
        print(f"ERROR: Matched PDFs JSON not found at {MATCHED_JSON}")
        print("Run match_pdfs_to_referrals.py first to generate this file.")
        return
    
    # Load matched PDFs
    print(f"Loading matched PDFs from: {MATCHED_JSON}")
    with open(MATCHED_JSON, 'r', encoding='utf-8') as f:
        matches = json.load(f)
    
    print(f"Found {len(matches)} matched PDFs\n")
    
    # Show configuration
    print("=" * 80)
    print("CONFIGURATION")
    print("=" * 80)
    print(f"DRY_RUN: {DRY_RUN} {'(Preview only - no changes)' if DRY_RUN else '(Will update database)'}")
    print(f"OVERWRITE_EXISTING: {OVERWRITE_EXISTING} {'(Will replace existing files)' if OVERWRITE_EXISTING else '(Skip referrals with files)'}")
    print(f"BACKUP_DB: {BACKUP_DB}")
    print("=" * 80 + "\n")
    
    if DRY_RUN:
        print("⚠ DRY RUN MODE - No changes will be made to the database\n")
    
    # Create backup if not dry run and backup enabled
    if not DRY_RUN and BACKUP_DB:
        try:
            backup_database()
        except Exception as e:
            print(f"ERROR: Failed to create backup: {e}")
            print("Aborting to protect your data.")
            return
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    updated_count = 0
    skipped_already_has_file = 0
    skipped_not_found = 0
    
    for i, match in enumerate(matches, 1):
        referral_id = match['referral_id']
        pdf_filename = match['pdf_filename']
        confidence = match['confidence']
        
        # Check if referral exists
        cursor.execute("SELECT fileName FROM referrals WHERE referralID = ?", (referral_id,))
        result = cursor.fetchone()
        
        if not result:
            print(f"[{i}/{len(matches)}] ⚠ Referral ID {referral_id} not found in database")
            skipped_not_found += 1
            continue
        
        current_filename = result[0]
        
        # Check if referral already has a file
        if current_filename and current_filename.strip():
            if not OVERWRITE_EXISTING:
                if i <= 10:  # Only show first 10 skips to avoid clutter
                    print(f"[{i}/{len(matches)}] ⊘ SKIP: Referral {referral_id} already has file: {current_filename}")
                skipped_already_has_file += 1
                continue
            else:
                action = "REPLACE"
        else:
            action = "ADD"
        
        # Prepare the new filename (just the filename, not full path)
        new_filename = f"{match['referral_last']}.{match['referral_first']}_Referral_{referral_id}.pdf"
        # Remove invalid characters
        new_filename = ''.join(c for c in new_filename if c not in '<>:"|?*\\/').replace(' ', '_')
        
        if DRY_RUN:
            if updated_count < 20:  # Show first 20 in dry run
                print(f"[{i}/{len(matches)}] {action}: Referral {referral_id} ({match['referral_last']}, {match['referral_first']})")
                print(f"             Confidence: {confidence:.1%}")
                if current_filename:
                    print(f"             Old: {current_filename}")
                print(f"             New: {new_filename}")
        else:
            try:
                cursor.execute(
                    "UPDATE referrals SET fileName = ? WHERE referralID = ?",
                    (new_filename, referral_id)
                )
                if updated_count < 20:  # Show first 20 updates
                    print(f"[{i}/{len(matches)}] ✓ {action}: Referral {referral_id} → {new_filename}")
                updated_count += 1
            except Exception as e:
                print(f"[{i}/{len(matches)}] ✗ ERROR: Failed to update referral {referral_id}: {e}")
    
    # Show continuation indicator if there were more
    if DRY_RUN and len(matches) > 20:
        print(f"... and {len(matches) - 20} more referrals would be updated")
    elif not DRY_RUN and updated_count > 20:
        print(f"... {updated_count - 20} more referrals updated")
    
    if skipped_already_has_file > 10:
        print(f"... {skipped_already_has_file - 10} more skipped (already have files)")
    
    # Commit changes (only if not dry run)
    if not DRY_RUN:
        conn.commit()
        print("\n✓ Changes committed to database")
    
    conn.close()
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total matched PDFs: {len(matches)}")
    
    if DRY_RUN:
        would_update = len(matches) - skipped_already_has_file - skipped_not_found
        print(f"\nDRY RUN MODE - No actual changes were made")
        print(f"Would update: {would_update} referrals")
    else:
        print(f"\n✓ Updated: {updated_count} referrals")
    
    if skipped_already_has_file > 0:
        print(f"⊘ Skipped (already have files): {skipped_already_has_file}")
    if skipped_not_found > 0:
        print(f"⚠ Skipped (not found in DB): {skipped_not_found}")
    
    print("=" * 80)
    
    if DRY_RUN:
        print("\n📋 To actually update the database:")
        print("   1. Review the output above")
        print("   2. Edit this script and set: DRY_RUN = False")
        print("   3. Optionally set: OVERWRITE_EXISTING = True (to replace existing files)")
        print("   4. Run the script again")
        print("\n⚠  IMPORTANT: Run copy_pdfs.bat AFTER updating the database!")
    else:
        print("\n✓ Database updated successfully!")
        print("\n📋 Next steps:")
        print("   1. Run copy_pdfs.bat to copy the PDF files to the Linked folder")
        print("   2. Verify the PDFs are showing correctly in the dashboard")
        if BACKUP_DB:
            print("\n💾 A backup of your database was created before updating")


if __name__ == '__main__':
    main()
