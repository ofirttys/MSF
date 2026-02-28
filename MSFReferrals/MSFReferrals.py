import eel
import sqlite3
import sys
import os
import json
import tempfile
import threading
import time
import traceback
import shutil
from pathlib import Path
from datetime import datetime
import psutil

# DB Folder configuration
if getattr(sys, 'frozen', False):
    # Running as compiled exe - DB folder should be next to the exe
    exe_dir = Path(sys.executable).parent
else:
    # Running as script - DB folder next to the .py file
    exe_dir = Path(__file__).parent

DB_FOLDER = str(exe_dir / 'DB')
DATABASE_FILE = str(Path(DB_FOLDER) / 'referrals.db')
LOCK_FILE = str(Path(DB_FOLDER) / 'referrals.lock')
LOCK_STALE_HOURS = 4

# DEBUG MODE: Set to True to see password hashes for setup
DEBUG_MODE = False

# User credentials (hashed passwords)
VALID_USERS = {
    'admin': '5f8eb2b05a1678d45a1678d55a1678d65a1678d75a1678d85a1678d95a1678da',
    'jennia': '5f8eb2b05a1678d45a1678d55a1678d65a1678d75a1678d85a1678d95a1678da'
}

# Global state
current_user = None
is_read_only = False
lock_owner = None

# Shutdown flag
_shutting_down = False

def hash_password(password):
    """Simple hash function for passwords - matches HTA version exactly"""
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

def get_db_connection():
    """Get database connection with WAL mode enabled"""
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row  # Access columns by name
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn

def timestamp_to_date(timestamp):
    """Convert Unix timestamp to date string"""
    if not timestamp:
        return ''
    try:
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
    except:
        return ''

def row_to_dict(row):
    """Convert sqlite3.Row to dict"""
    return {key: row[key] for key in row.keys()}

@eel.expose
def login(username, password):
    """Handle user login"""
    global current_user, is_read_only, lock_owner
    
    username = username.lower()
    entered_hash = hash_password(password)
    
    if DEBUG_MODE:
        print(f'\n=== DEBUG MODE ===')
        print(f'Password: {password}')
        print(f'Hash: {entered_hash}')
        print(f'==================\n')
    
    if username not in VALID_USERS or entered_hash != VALID_USERS[username]:
        return {'status': 'error', 'message': 'Invalid username or password'}
    
    lock_status = check_lock_file()
    
    if lock_status['locked'] and not lock_status['stale']:
        current_user = username
        is_read_only = True
        lock_owner = lock_status['user']
        return {
            'status': 'locked',
            'user': lock_status['user'],
            'timestamp': lock_status['timestamp'],
            'message': f"Database is locked by {lock_status['user']}"
        }
    
    if lock_status.get('stale', False):
        delete_lock_file()
    
    create_lock_file(username)
    current_user = username
    is_read_only = False
    
    return {'status': 'success', 'username': username, 'readOnly': False}

@eel.expose
def login_readonly(username):
    """Login in read-only mode"""
    global current_user, is_read_only
    current_user = username
    is_read_only = True
    return {'status': 'success', 'username': username, 'readOnly': True}

@eel.expose
def logout():
    """Handle user logout"""
    global current_user, is_read_only, lock_owner
    
    if not is_read_only:
        delete_lock_file()
    
    current_user = None
    is_read_only = False
    lock_owner = None
    
    return {'status': 'success'}

def check_lock_file():
    """Check if lock file exists and is valid"""
    try:
        if not os.path.exists(LOCK_FILE):
            return {'locked': False}
        
        with open(LOCK_FILE, 'r') as f:
            lock_data = json.load(f)
        
        lock_time = datetime.fromisoformat(lock_data['timestamp'])
        now = datetime.now()
        hours_old = (now - lock_time).total_seconds() / 3600
        
        return {
            'locked': True,
            'user': lock_data['user'],
            'timestamp': lock_data['timestamp'],
            'stale': hours_old > LOCK_STALE_HOURS
        }
    except Exception as e:
        print(f"Error checking lock file: {e}")
        return {'locked': False}

def create_lock_file(username):
    """Create lock file"""
    try:
        os.makedirs(DB_FOLDER, exist_ok=True)
        lock_data = {
            'user': username,
            'timestamp': datetime.now().isoformat()
        }
        with open(LOCK_FILE, 'w') as f:
            json.dump(lock_data, f)
    except Exception as e:
        print(f"Error creating lock file: {e}")

def delete_lock_file():
    """Delete lock file"""
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception as e:
        print(f"Error deleting lock file: {e}")

def refresh_lock_file():
    """Refresh lock file timestamp"""
    if not is_read_only and current_user:
        create_lock_file(current_user)

@eel.expose
def get_referrals(filters=None, sort_by='id', sort_order='asc', offset=0, limit=100):
    """Get referrals with filtering, sorting, and pagination - OPTIMIZED
    
    Only returns 17 fields needed for dashboard display (not all 52 fields)
    
    Args:
        filters: dict with keys like 'status', 'type', 'search', 'dateFrom', 'dateTo'
        sort_by: 'id', 'name', 'received', 'lastAttempt'
        sort_order: 'asc' or 'desc'
        offset: Starting row (for infinite scroll)
        limit: Number of rows to return (default 100)
    
    Returns:
        {
            'status': 'success',
            'referrals': [...],
            'total': 5447,
            'hasMore': true/false
        }
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build WHERE clause
        where_clauses = []
        params = []
        
        if filters:
            # Status filters - organized into mutually exclusive groups
            if filters.get('statuses'):
                status_list = filters['statuses']
                if isinstance(status_list, str):
                    status_list = [status_list]
                
                # Group 0: Urgent (mutually exclusive)
                urgent_filters = []
                if 'urgent' in status_list:
                    urgent_filters.append("urgent = 1")
                
                if urgent_filters:
                    where_clauses.append(f"({urgent_filters[-1]})")
                
                # Group 1: Referral Type (mutually exclusive)
                type_filters = []
                if 'new' in status_list:
                    type_filters.append("referralType = 'New'")
                if 'previous' in status_list:
                    type_filters.append("referralType = 'Previous'")
                if 'partner' in status_list:
                    type_filters.append("referralType = 'Partner'")
                
                # If multiple type filters selected, use only the last one (shouldn't happen with mutual exclusive UI)
                if type_filters:
                    where_clauses.append(f"({type_filters[-1]})")
                
                # Group 2: Referral Status (mutually exclusive)
                status_filters = []
                if 'new-referral' in status_list:
                    # Just check status = New (removed lastAttemptDate check)
                    status_filters.append("referralStatus = 'New'")
                if 'pending' in status_list:
                    status_filters.append("referralStatus = 'Pending'")
                if 'info-received' in status_list:
                    status_filters.append("referralStatus = 'Information Completed'")
                if 'physician-assigned' in status_list:
                    status_filters.append("referralStatus = 'Physician Assigned'")
                if 'cerner-done' in status_list:
                    status_filters.append("referralStatus = 'Cerner Done'")
                if 'eivf-done' in status_list:
                    status_filters.append("referralStatus = 'eIVF Done'")
                if 'completed' in status_list:
                    status_filters.append("referralStatus = 'Completed'")
                if 'deferred' in status_list:
                    status_filters.append("referralStatus = 'Deferred'")
                
                # If multiple status filters selected, use only the last one
                if status_filters:
                    where_clauses.append(f"({status_filters[-1]})")
                
                # Group 3: Contact timing (mutually exclusive)
                contact_filters = []
                if 'contact-2days' in status_list:
                    contact_filters.append("(lastAttemptDate IS NOT NULL AND (strftime('%s', 'now') - lastAttemptDate) / 86400 > 2)")
                if 'contact-3days' in status_list:
                    contact_filters.append("(lastAttemptDate IS NOT NULL AND (strftime('%s', 'now') - lastAttemptDate) / 86400 > 3)")
                if 'contact-7days' in status_list:
                    contact_filters.append("(lastAttemptDate IS NOT NULL AND (strftime('%s', 'now') - lastAttemptDate) / 86400 > 7)")
                if 'no-contact' in status_list:
                    contact_filters.append("(lastAttemptDate IS NULL)")
                
                # If multiple contact filters selected, use only the last one
                if contact_filters:
                    where_clauses.append(f"({contact_filters[-1]})")
                
                # Group 4: Email presence (mutually exclusive)
                email_filters = []
                if 'no-email' in status_list:
                    email_filters.append("(patientEmail IS NULL OR patientEmail = '')")
                
                if email_filters:
                    where_clauses.append(f"({email_filters[-1]})")
                
                # Group 5: File presence (mutually exclusive)
                file_filters = []
                if 'no-file' in status_list:
                    file_filters.append("(fileName IS NULL OR fileName = '')")
                
                if file_filters:
                    where_clauses.append(f"({file_filters[-1]})")
            
            # Date range filter
            if filters.get('dateFrom'):
                try:
                    from_date = datetime.strptime(filters['dateFrom'], '%Y-%m-%d')
                    from_timestamp = int(from_date.timestamp())
                    where_clauses.append("receivedDate >= ?")
                    params.append(from_timestamp)
                except:
                    pass
            
            if filters.get('dateTo'):
                try:
                    to_date = datetime.strptime(filters['dateTo'], '%Y-%m-%d')
                    to_date = to_date.replace(hour=23, minute=59, second=59)
                    to_timestamp = int(to_date.timestamp())
                    where_clauses.append("receivedDate <= ?")
                    params.append(to_timestamp)
                except:
                    pass
            
            # Search filter
            if filters.get('search'):
                search_term = f"%{filters['search']}%"
                where_clauses.append(
                    "(patientFirstName LIKE ? OR patientLastName LIKE ? OR patientPhone LIKE ? OR patientEmail LIKE ? OR CAST(referralID AS TEXT) LIKE ?)"
                )
                params.extend([search_term, search_term, search_term, search_term, search_term])
        
        # Build ORDER BY clause
        order_map = {
            'id': 'referralID',
            'name': 'patientLastName, patientFirstName',
            'received': 'receivedDate',
            'lastAttempt': 'lastAttemptDate'
        }
        order_column = order_map.get(sort_by, 'referralID')
        order_direction = 'DESC' if sort_order == 'desc' else 'ASC'
        
        # Get total count
        count_sql = "SELECT COUNT(*) FROM referrals"
        if where_clauses:
            count_sql += " WHERE " + " AND ".join(where_clauses)
        
        cursor.execute(count_sql, params)
        total_count = cursor.fetchone()[0]
        
        # OPTIMIZED: Get only fields displayed in dashboard (17 fields instead of 52)
        sql = f"""
            SELECT 
                referralID,
                urgent,
                fileName,
                patientFirstName,
                patientLastName,
                referralStatus,
                referralType,
                patientDOB,
                receivedDate,
                lastAttemptDate,
                lastAttemptMode,
                phoneAttempts,
                emailAttempts,
                serviceRequested,
                requestedPhysician,
                requestedLocation,
                patientPhone,
                patientEmail
            FROM referrals
            {" WHERE " + " AND ".join(where_clauses) if where_clauses else ""}
            ORDER BY {order_column} {order_direction}
            LIMIT ? OFFSET ?
        """
        
        params.extend([limit, offset])
        cursor.execute(sql, params)
        
        referrals = []
        for row in cursor.fetchall():
            ref_dict = row_to_dict(row)
            
            # Convert timestamps to date strings for frontend
            ref_dict['receivedDate'] = timestamp_to_date(ref_dict['receivedDate'])
            ref_dict['patientDOB'] = timestamp_to_date(ref_dict['patientDOB'])
            ref_dict['lastAttemptDate'] = timestamp_to_date(ref_dict['lastAttemptDate'])
            
            # phoneAttempts and emailAttempts already in row - no need to query!
            
            referrals.append(ref_dict)
        
        conn.close()
        
        return {
            'status': 'success',
            'referrals': referrals,
            'total': total_count,
            'hasMore': (offset + limit) < total_count
        }
        
    except Exception as e:
        print(f"Error getting referrals: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}

@eel.expose
def get_referral_details(referral_id):
    """Get complete referral with all fields and attempt history
    
    Called when user clicks to view/edit a referral
    Loads all 52 fields + attempt history (only when needed)
    
    Args:
        referral_id: ID of referral to load
        
    Returns:
        {
            'status': 'success',
            'referral': {...}  # All fields + attemptHistory array
        }
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get full referral data (all 52 fields)
        cursor.execute("SELECT * FROM referrals WHERE referralID = ?", (referral_id,))
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            return {'status': 'error', 'message': 'Referral not found'}
        
        referral = row_to_dict(row)
        
        # Convert timestamps to date strings
        date_fields = ['addedToDBDate', 'referralDate', 'receivedDate', 'patientDOB', 
                      'partnerDOB', 'lastAttemptDate', 'faxedBackDate', 
                      'completeInfoReceivedDate', 'referralCompleteDate', 'notesDate']
        
        for field in date_fields:
            if referral.get(field):
                referral[field] = timestamp_to_date(referral[field])
        
        # Get attempt history
        cursor.execute("""
            SELECT attemptDate, attemptTime, attemptMode, attemptComment
            FROM attempt_history
            WHERE referralID = ?
            ORDER BY id
        """, (referral_id,))
        
        attempts = []
        for attempt_row in cursor.fetchall():
            attempts.append({
                'date': timestamp_to_date(attempt_row['attemptDate']),
                'time': attempt_row['attemptTime'] or '',
                'mode': attempt_row['attemptMode'] or '',
                'comment': attempt_row['attemptComment'] or ''
            })
        
        referral['attemptHistory'] = attempts
        
        conn.close()
        
        return {'status': 'success', 'referral': referral}
        
    except Exception as e:
        print(f"Error getting referral details: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}

@eel.expose
def get_kpi_counts(date_filters=None):
    """Get KPI counts with optional date filtering
    
    Args:
        date_filters: dict with 'dateFrom' and 'dateTo'
    
    Returns:
        {
            'total': 5447,
            'new': 65,
            'pending': 632,
            'completed': 4039,
            'deferred': 705,
            'waitingContact': 123
        }
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build date filter
        where_clause = ""
        params = []
        
        if date_filters:
            conditions = []
            if date_filters.get('dateFrom'):
                try:
                    from_date = datetime.strptime(date_filters['dateFrom'], '%Y-%m-%d')
                    from_timestamp = int(from_date.timestamp())
                    conditions.append("receivedDate >= ?")
                    params.append(from_timestamp)
                except:
                    pass
            
            if date_filters.get('dateTo'):
                try:
                    to_date = datetime.strptime(date_filters['dateTo'], '%Y-%m-%d')
                    to_date = to_date.replace(hour=23, minute=59, second=59)
                    to_timestamp = int(to_date.timestamp())
                    conditions.append("receivedDate <= ?")
                    params.append(to_timestamp)
                except:
                    pass
            
            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
        
        # Get counts
        cursor.execute(f"SELECT COUNT(*) FROM referrals{where_clause}", params)
        total = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM referrals{where_clause} {'AND' if where_clause else 'WHERE'} referralType = 'New'", params)
        new = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM referrals{where_clause} {'AND' if where_clause else 'WHERE'} referralStatus = 'Pending'", params)
        pending = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM referrals{where_clause} {'AND' if where_clause else 'WHERE'} referralStatus = 'Completed'", params)
        completed = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM referrals{where_clause} {'AND' if where_clause else 'WHERE'} referralStatus = 'Deferred'", params)
        deferred = cursor.fetchone()[0]
        
        # Waiting for contact: (New OR Pending) AND (no lastAttemptDate OR > 2 days old)
        cursor.execute(f"""
            SELECT COUNT(*) FROM referrals
            {where_clause}
            {"AND" if where_clause else "WHERE"} (referralStatus = 'New' OR referralStatus = 'Pending')
            AND (lastAttemptDate IS NULL OR (strftime('%s', 'now') - lastAttemptDate) / 86400 > 2)
        """, params)
        waiting_contact = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total': total,
            'new': new,
            'pending': pending,
            'completed': completed,
            'deferred': deferred,
            'waitingContact': waiting_contact
        }
    except Exception as e:
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}

@eel.expose
def get_status_history(referral_id):
    """Get status change history for a referral"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT oldStatus, newStatus, changedDate, changedBy
            FROM status_history
            WHERE referralID = ?
            ORDER BY changedDate DESC
        """, (referral_id,))
        
        rows = cursor.fetchall()
        history = []
        for row in rows:
            history.append({
                'oldStatus': row[0],
                'newStatus': row[1],
                'changedDate': row[2],
                'changedBy': row[3]
            })
        
        conn.close()
        
        return {
            'status': 'success',
            'history': history
        }
    except Exception as e:
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}

@eel.expose
def get_attempt_history(referral_id):
    """Get contact attempt history for a referral"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT attemptDate, attemptTime, attemptMode, attemptComment
            FROM attempt_history
            WHERE referralID = ?
            ORDER BY attemptDate DESC, attemptTime DESC
        """, (referral_id,))
        
        rows = cursor.fetchall()
        history = []
        for row in rows:
            history.append({
                'attemptDate': row[0],
                'attemptTime': row[1],
                'attemptMode': row[2],
                'attemptComment': row[3]
            })
        
        conn.close()
        
        return {
            'status': 'success',
            'history': history
        }
    except Exception as e:
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}

@eel.expose
def get_notes_history(referral_id):
    """Get notes history for a referral"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT noteDate, noteText, addedBy
            FROM notes_history
            WHERE referralID = ?
            ORDER BY noteDate DESC
        """, (referral_id,))
        
        rows = cursor.fetchall()
        history = []
        for row in rows:
            history.append({
                'noteDate': row[0],
                'noteText': row[1],
                'addedBy': row[2]
            })
        
        conn.close()
        
        return {
            'status': 'success',
            'history': history
        }
    except Exception as e:
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}

@eel.expose
def get_select_options():
    """Get dropdown options from database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT category, value
            FROM select_options
            ORDER BY category, displayOrder
        """)
        
        options = {}
        for row in cursor.fetchall():
            category = row['category']
            value = row['value']
            if category not in options:
                options[category] = []
            options[category].append(value)
        
        conn.close()
        return {'status': 'success', 'options': options}
        
    except Exception as e:
        print(f"Error getting select options: {e}")
        return {'status': 'error', 'message': str(e)}

@eel.expose
def add_referral(referral_data):
    """Add new referral to database"""
    if is_read_only:
        return {'status': 'error', 'message': 'Cannot add referrals in read-only mode'}
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Add timestamp
        referral_data['addedToDBDate'] = int(datetime.now().timestamp())
        
        # Extract attempt history
        attempt_history = referral_data.pop('attemptHistory', [])
        
        # Calculate attempt counts (OPTIMIZED - store counts instead of querying)
        phone_count = sum(1 for a in attempt_history if a.get('mode') in ['Phone', 'Phone call'])
        email_count = sum(1 for a in attempt_history if a.get('mode') in ['E-Mail', 'Email'])
        
        referral_data['phoneAttempts'] = phone_count
        referral_data['emailAttempts'] = email_count
        
        # Set lastAttemptMode from last attempt
        if attempt_history:
            referral_data['lastAttemptMode'] = attempt_history[-1].get('mode', '')
        
        # Convert date strings to timestamps
        date_fields = ['referralDate', 'receivedDate', 'patientDOB', 'partnerDOB', 
                      'lastAttemptDate', 'faxedBackDate', 'completeInfoReceivedDate', 
                      'referralCompleteDate', 'notesDate']
        
        for field in date_fields:
            if field in referral_data and referral_data[field]:
                try:
                    dt = datetime.strptime(referral_data[field], '%Y-%m-%d')
                    referral_data[field] = int(dt.timestamp())
                except:
                    referral_data[field] = None
        
        # Insert referral
        columns = ', '.join(referral_data.keys())
        placeholders = ', '.join(['?' for _ in referral_data])
        sql = f"INSERT INTO referrals ({columns}) VALUES ({placeholders})"
        
        cursor.execute(sql, list(referral_data.values()))
        referral_id = cursor.lastrowid
        
        # Insert attempt history
        for attempt in attempt_history:
            attempt_date = None
            if attempt.get('date'):
                try:
                    dt = datetime.strptime(attempt['date'], '%Y-%m-%d')
                    attempt_date = int(dt.timestamp())
                except:
                    pass
            
            cursor.execute("""
                INSERT INTO attempt_history (referralID, attemptDate, attemptTime, attemptMode, attemptComment)
                VALUES (?, ?, ?, ?, ?)
            """, (
                referral_id,
                attempt_date,
                attempt.get('time', ''),
                attempt.get('mode', ''),
                attempt.get('comment', '')
            ))
        
        conn.commit()
        conn.close()
        
        refresh_lock_file()
        
        referral_data['referralID'] = referral_id
        return {'status': 'success', 'referral': referral_data}
        
    except Exception as e:
        print(f"Error adding referral: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}

@eel.expose
def update_referral(referral_id, referral_data):
    """Update existing referral"""
    if is_read_only:
        return {'status': 'error', 'message': 'Cannot update referrals in read-only mode'}
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Extract attempt history
        attempt_history = referral_data.pop('attemptHistory', [])
        
        # Calculate attempt counts (OPTIMIZED - store counts instead of querying)
        phone_count = sum(1 for a in attempt_history if a.get('mode') in ['Phone', 'Phone call'])
        email_count = sum(1 for a in attempt_history if a.get('mode') in ['E-Mail', 'Email'])
        
        referral_data['phoneAttempts'] = phone_count
        referral_data['emailAttempts'] = email_count
        
        # Set lastAttemptMode from last attempt
        if attempt_history:
            referral_data['lastAttemptMode'] = attempt_history[-1].get('mode', '')
        
        # Convert date strings to timestamps
        date_fields = ['referralDate', 'receivedDate', 'patientDOB', 'partnerDOB',
                      'lastAttemptDate', 'faxedBackDate', 'completeInfoReceivedDate',
                      'referralCompleteDate', 'notesDate']
        
        for field in date_fields:
            if field in referral_data and referral_data[field]:
                try:
                    dt = datetime.strptime(referral_data[field], '%Y-%m-%d')
                    referral_data[field] = int(dt.timestamp())
                except:
                    referral_data[field] = None
        
        # Update referral
        set_clause = ', '.join([f"{k} = ?" for k in referral_data.keys()])
        sql = f"UPDATE referrals SET {set_clause} WHERE referralID = ?"
        
        cursor.execute(sql, list(referral_data.values()) + [referral_id])
        
        # Delete old attempt history
        cursor.execute("DELETE FROM attempt_history WHERE referralID = ?", (referral_id,))
        
        # Insert new attempt history
        for attempt in attempt_history:
            attempt_date = None
            if attempt.get('date'):
                try:
                    dt = datetime.strptime(attempt['date'], '%Y-%m-%d')
                    attempt_date = int(dt.timestamp())
                except:
                    pass
            
            cursor.execute("""
                INSERT INTO attempt_history (referralID, attemptDate, attemptTime, attemptMode, attemptComment)
                VALUES (?, ?, ?, ?, ?)
            """, (
                referral_id,
                attempt_date,
                attempt.get('time', ''),
                attempt.get('mode', ''),
                attempt.get('comment', '')
            ))
        
        conn.commit()
        conn.close()
        
        refresh_lock_file()
        
        return {'status': 'success', 'referral': referral_data}
        
    except Exception as e:
        print(f"Error updating referral: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}

@eel.expose
def delete_referral(referral_id):
    """Delete referral from database"""
    if is_read_only:
        return {'status': 'error', 'message': 'Cannot delete referrals in read-only mode'}
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Delete referral (CASCADE will delete attempt_history)
        cursor.execute("DELETE FROM referrals WHERE referralID = ?", (referral_id,))
        
        conn.commit()
        conn.close()
        
        refresh_lock_file()
        
        return {'status': 'success'}
        
    except Exception as e:
        print(f"Error deleting referral: {e}")
        return {'status': 'error', 'message': str(e)}

@eel.expose
def open_file_dialog():
    """Open a native file dialog and return full path"""
    try:
        import tkinter as tk
        from tkinter import filedialog
        
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        root.update()
        root.winfo_toplevel().lift()
        root.focus_force()
        
        file_path = filedialog.askopenfilename(
            parent=root,
            title='Select Referral File',
            filetypes=[
                ('PDF Files', '*.pdf'),
                ('Image Files', '*.jpg *.jpeg *.png'),
                ('All Files', '*.*')
            ]
        )
        root.destroy()
        return file_path or ''
    except Exception as e:
        print(f"Error opening file dialog: {e}")
        return ''

@eel.expose
def get_file_content(file_path):
    """Read a file and return as base64 for display"""
    try:
        import base64
        if not file_path or not os.path.exists(file_path):
            return {'status': 'error', 'message': 'File not found: ' + str(file_path)}
        
        with open(file_path, 'rb') as f:
            content = base64.b64encode(f.read()).decode('utf-8')
        
        ext = Path(file_path).suffix.lower()
        mime_types = {
            '.pdf': 'application/pdf',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif'
        }
        mime = mime_types.get(ext, 'application/octet-stream')
        
        return {
            'status': 'success',
            'content': content,
            'mime': mime,
            'filename': Path(file_path).name
        }
    except Exception as e:
        print(f"Error reading file: {e}")
        return {'status': 'error', 'message': str(e)}

@eel.expose
def select_file():
    """Open file dialog in Referrals/ folder and move selected file to Linked/"""
    try:
        import tkinter as tk
        from tkinter import filedialog
        
        # Create Referrals and Linked folders if they don't exist
        referrals_folder = os.path.join(exe_dir, 'Referrals')
        linked_folder = os.path.join(referrals_folder, 'Linked')
        os.makedirs(linked_folder, exist_ok=True)
        
        # Open file dialog starting in Referrals folder
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        
        filepath = filedialog.askopenfilename(
            initialdir=referrals_folder,
            title="Select Referral PDF",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
        )
        
        root.destroy()
        
        if not filepath:
            return {'status': 'cancelled'}
        
        # Get filename
        filename = os.path.basename(filepath)
        
        # Check if file is already in Linked/ folder
        if os.path.dirname(filepath) == linked_folder:
            # File already in Linked/, just return the relative path
            relative_path = f"Referrals/Linked/{filename}"
            return {
                'status': 'success',
                'fileName': relative_path,
                'message': f'File already in Linked folder: {filename}'
            }
        
        # Destination path in Linked/
        dest_path = os.path.join(linked_folder, filename)
        
        # Check if file with same name already exists in Linked/
        if os.path.exists(dest_path):
            return {
                'status': 'error',
                'message': f'A file named "{filename}" already exists in Referrals/Linked/. Please rename the original file or choose a different file.'
            }
        
        # Move file to Linked/
        shutil.move(filepath, dest_path)
        
        # Return relative path for DB storage
        relative_path = f"Referrals/Linked/{filename}"
        
        return {
            'status': 'success',
            'fileName': relative_path,
            'message': f'File moved to Linked folder: {filename}'
        }
        
    except PermissionError:
        return {
            'status': 'error',
            'message': 'Permission denied. The file may be in use or locked by another program.'
        }
    except FileNotFoundError:
        return {
            'status': 'error',
            'message': 'File not found. It may have been deleted or moved.'
        }
    except OSError as e:
        if 'No space left on device' in str(e) or e.errno == 28:
            return {
                'status': 'error',
                'message': 'Not enough disk space to move the file.'
            }
        return {
            'status': 'error',
            'message': f'Error moving file: {str(e)}'
        }
    except Exception as e:
        traceback.print_exc()
        return {
            'status': 'error',
            'message': f'Unexpected error: {str(e)}'
        }

@eel.expose
def copy_to_eivf(referral_id):
    """Copy file from Linked/ to eIVF/ with formatted name when transitioning to Cerner Done"""
    try:
        # Get referral details
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT fileName, patientMRN, receivedDate 
            FROM referrals 
            WHERE referralID = ?
        """, (referral_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return {'status': 'error', 'message': 'Referral not found'}
        
        source_relative, mrn, received_date = row
        
        # Validate required fields
        if not source_relative:
            return {'status': 'error', 'message': 'No file attached to this referral'}
        
        if not mrn:
            return {'status': 'error', 'message': 'MRN is required. Please enter MRN first.'}
        
        if not received_date:
            return {'status': 'error', 'message': 'Received date is missing'}
        
        # Build source path
        source_path = os.path.join(exe_dir, source_relative.replace('/', os.sep))
        
        if not os.path.exists(source_path):
            return {
                'status': 'error',
                'message': f'Source file not found: {source_relative}'
            }
        
        # Create eIVF folder
        eivf_folder = os.path.join(exe_dir, 'Referrals', 'eIVF')
        os.makedirs(eivf_folder, exist_ok=True)
        
        # Convert received date to YYMMDD format
        # received_date could be timestamp or string
        if isinstance(received_date, int):
            date_obj = datetime.fromtimestamp(received_date)
        elif isinstance(received_date, str):
            date_obj = datetime.strptime(received_date, '%Y-%m-%d')
        else:
            return {'status': 'error', 'message': 'Invalid received date format'}
        
        yymmdd = date_obj.strftime('%y%m%d')
        
        # Build new filename: MRN_Referral_YYMMDD.pdf
        new_filename = f"{mrn}_Referral_{yymmdd}.pdf"
        dest_path = os.path.join(eivf_folder, new_filename)
        
        # Copy file (keep original in Linked/)
        shutil.copy2(source_path, dest_path)
        
        # Get absolute path for clipboard
        abs_path = os.path.abspath(dest_path)
        
        return {
            'status': 'success',
            'fileName': new_filename,
            'fullPath': abs_path,
            'message': f'File copied to eIVF folder as: {new_filename}'
        }
        
    except Exception as e:
        traceback.print_exc()
        return {
            'status': 'error',
            'message': f'Error copying file: {str(e)}'
        }

def on_close(page, sockets):
    """Handle window close"""
    global _shutting_down
    if _shutting_down:
        return
    _shutting_down = True
    if not is_read_only:
        delete_lock_file()
    try:
        import gevent
        gevent.killall()
    except:
        pass
    os._exit(0)

@eel.expose
def export_to_csv():
    """Export database to CSV file in application folder"""
    try:
        from datetime import datetime
        import csv
        
        # Helper function to convert timestamp to date string
        def timestamp_to_date(ts):
            if ts is None or ts == '':
                return ''
            try:
                return datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d')
            except:
                return ''
        
        # Generate filename with current date
        current_date = datetime.now().strftime('%Y%m%d')
        filename = f'Referrals Master List - {current_date}.csv'
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all referrals with full details
        cursor.execute("""
            SELECT 
                referralID, addedToDBDate, referralDate, receivedDate, fileName,
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
                phoneAttempts, emailAttempts, assignedPhysician,
                faxedBackDate, completeInfoReceivedDate, taskedToPhysicianAdmin,
                referralCompleteDate, notes, notesDate
            FROM referrals
            ORDER BY referralID
        """)
        
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        
        # Date column indices (0-based)
        date_columns = [1, 2, 3, 40, 45, 46, 47, 49]  # addedToDBDate, referralDate, receivedDate, lastAttemptDate, faxedBackDate, completeInfoReceivedDate, referralCompleteDate, notesDate
        
        # Convert timestamps to dates
        converted_rows = []
        for row in rows:
            row_list = list(row)
            for idx in date_columns:
                if idx < len(row_list):
                    row_list[idx] = timestamp_to_date(row_list[idx])
            converted_rows.append(row_list)
        
        # Write CSV file in application folder (current directory)
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(converted_rows)
        
        conn.close()
        
        return {
            'status': 'success',
            'filename': filename,
            'rows': len(rows)
        }
        
    except Exception as e:
        traceback.print_exc()
        return {
            'status': 'error',
            'message': str(e)
        }

def shutdown():
    """Cleanup on shutdown"""
    global _shutting_down
    if _shutting_down:
        return
    _shutting_down = True
    if not is_read_only:
        delete_lock_file()

if __name__ == '__main__':
    # Check if database exists
    if not os.path.exists(DATABASE_FILE):
        print(f"ERROR: Database not found at {DATABASE_FILE}")
        print(f"Please run the CSV to SQLite converter first:")
        print(f"  python Convert-CSV-To-SQLite.py referral-status.csv DB/referrals.db")
        sys.exit(1)
    
    # Initialize Eel
    eel.init('web')
    
    # Register cleanup
    import atexit
    atexit.register(shutdown)
    
    # Get screen dimensions
    try:
        import ctypes
        user32 = ctypes.windll.user32
        screen_width = user32.GetSystemMetrics(0)
        screen_height = user32.GetSystemMetrics(1)
    except:
        screen_width = 1920
        screen_height = 1080
    
    # Start Eel
    try:
        eel.start(
            'index.html',
            mode='edge',
            size=(screen_width, screen_height),
            position=(0, 0),
            close_callback=on_close
        )
    except Exception as e:
        print(f"Error starting dashboard: {e}")
        if not is_read_only:
            delete_lock_file()
        sys.exit(1)
