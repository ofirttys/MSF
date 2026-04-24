#!/usr/bin/env python3
"""
MichaeliClinic-Dashboard - Main Application
Python + Eel + SQLite desktop application
"""

import eel
import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Import our modules
from database import Database
from patient_manager import PatientManager
from appointment_manager import AppointmentManager
from action_items_manager import ActionItemsManager
from clinic_days_manager import ClinicDaysManager
from portal_manager import PortalManager

# Eel will be initialized in main() with correct web folder path

# Global managers
db = None
patient_mgr = None
appointment_mgr = None
action_items_mgr = None
clinic_days_mgr = None
portal_mgr = None

# Session tracking
SESSIONS_FILE = "DB/active_sessions.json"

# User management - keep ONLY admin as code-based fallback
VALID_USERS = {
    'admin': '5f8eb2b05a1678d45a1678d55a1678d65a1678d75a1678d85a1678d95a1678da',
    'jennia': '5f8eb2b05a1678d45a1678d55a1678d65a1678d75a1678d85a1678d95a1678da'
}

# Current user state
current_user = None
current_user_is_admin = False


def initialize_app():
    """Initialize database and managers"""
    global db, patient_mgr, appointment_mgr, action_items_mgr, clinic_days_mgr, portal_mgr
    
    # Database path - use DB subfolder like HTA
    import os
    db_folder = "DB"
    if not os.path.exists(db_folder):
        os.makedirs(db_folder)
    
    db_path = os.path.join(db_folder, "michaeli-clinic.db")
    
    if not os.path.exists(db_path):
        print(f"ERROR: Database not found: {db_path}")
        print("Please run 'python import_all.py' first to create the database.")
        sys.exit(1)
    
    # Initialize database connection
    db = Database(db_path)
    
    # Portal file path - same folder as database
    portal_file_path = os.path.join(db_folder, "Patient Portal Users.xls")
    
    # Initialize managers
    patient_mgr = PatientManager(db)
    appointment_mgr = AppointmentManager(db)
    action_items_mgr = ActionItemsManager(db)
    clinic_days_mgr = ClinicDaysManager(db)
    portal_mgr = PortalManager(db, portal_file_path)
    
    print("✓ Application initialized")


# ============================================================================
# PATIENT API
# ============================================================================

@eel.expose
def get_all_patients():
    """Get all patients"""
    return patient_mgr.get_all()

@eel.expose
def get_patients_paginated(limit=50, offset=0):
    """
    Get patients in pages for progressive loading
    
    Args:
        limit: Number of patients to return (default 50)
        offset: Starting position (default 0)
        
    Returns:
        {
            'patients': [...],
            'total': total_count,
            'has_more': True/False
        }
    """
    all_patients = patient_mgr.get_all()
    total = len(all_patients)
    
    # Get slice
    patients_slice = all_patients[offset:offset + limit]
    
    return {
        'patients': patients_slice,
        'total': total,
        'has_more': (offset + limit) < total
    }

@eel.expose
def get_all_appointment_dates():
    """
    Get all unique dates that have appointments (for calendar highlighting)
    Uses SQL UNION for fast retrieval
    
    Returns:
        List of date strings (YYYY-MM-DD)
    """
    query = """
        SELECT DISTINCT date 
        FROM (
            SELECT nextAppointment as date 
            FROM patients 
            WHERE nextAppointment IS NOT NULL
            UNION
            SELECT date 
            FROM appointment_history
        )
        ORDER BY date
    """
    results = db.fetchall(query)
    return [row['date'] for row in results]

@eel.expose
def get_todays_appointments():
    """Get just today's appointments for fast initial load"""
    from datetime import datetime
    today = datetime.now().strftime('%Y-%m-%d')
    return appointment_mgr.get_appointments_by_date(today)

@eel.expose
def get_filtered_patients(state_filters=None, search_term=None, special_filters=None):
    """Get filtered patients using SQL
    
    Args:
        state_filters: List of state names (e.g. ['WAITING_FIRST_APPT', 'ACTIVE'])
        search_term: Search string for name/email/phone
        special_filters: List of special filters (e.g. ['SURVIVORSHIP', 'OTC', 'PRIORITY'])
    
    Returns:
        List of filtered patients
    """
    return patient_mgr.get_filtered(
        state_filters=state_filters or [],
        search_term=search_term,
        special_filters=special_filters or []
    )

@eel.expose
def get_patient(patient_id):
    """Get single patient by ID"""
    return patient_mgr.get_by_id(patient_id)

@eel.expose
def search_patients(query):
    """Search patients by name, email, phone"""
    return patient_mgr.search(query)

@eel.expose
def add_patient(patient_data):
    """Add a new patient"""
    return patient_mgr.add(patient_data)

@eel.expose
def update_patient(patient_id, patient_data):
    """Update existing patient"""
    return patient_mgr.update(patient_id, patient_data)

@eel.expose
def update_patient_state(patient_id, new_state, notes=None):
    """Update patient state"""
    return patient_mgr.update_state(patient_id, new_state, notes)

@eel.expose
def update_patient_state_with_version(patient_id, new_state, notes, last_known_timestamp):
    """
    Update patient state with optimistic locking (conflict detection)
    
    Args:
        patient_id: Patient ID
        new_state: New state to set
        notes: Optional notes
        last_known_timestamp: Last state change timestamp this user knows about
        
    Returns:
        {
            'success': True/False,
            'conflict': True/False,
            'message': Error message if conflict,
            'patient': Updated or current patient data
        }
    """
    try:
        # Get current patient state
        current = patient_mgr.get_by_id(patient_id)
        
        if not current:
            return {
                'success': False,
                'conflict': False,
                'message': 'Patient not found',
                'patient': None
            }
        
        # Check for conflict - has another user modified this patient?
        if current.get('stateHistory') and len(current['stateHistory']) > 0:
            current_timestamp = current['stateHistory'][-1]['timestamp']
            
            # Conflict detected! Patient was modified since this user last saw it
            if last_known_timestamp and current_timestamp != last_known_timestamp:
                return {
                    'success': False,
                    'conflict': True,
                    'message': 'Patient was modified by another user',
                    'current_state': current.get('currentState'),
                    'patient': current
                }
        
        # No conflict - proceed with update
        patient_mgr.update_state(patient_id, new_state, notes)
        updated = patient_mgr.get_by_id(patient_id)
        
        return {
            'success': True,
            'conflict': False,
            'message': None,
            'patient': updated
        }
        
    except Exception as e:
        print(f"Error in update_patient_state_with_version: {e}")
        return {
            'success': False,
            'conflict': False,
            'message': str(e),
            'patient': None
        }

@eel.expose
def update_patient_notes(patient_id, notes):
    """Update patient notes"""
    return patient_mgr.update_notes(patient_id, notes)

@eel.expose
def add_note_history(patient_id, note):
    """Add entry to notes history"""
    return patient_mgr.add_note_history(patient_id, note)


# ============================================================================
# APPOINTMENT API
# ============================================================================

@eel.expose
def get_appointments_by_date(date_str):
    """Get all appointments for a specific date"""
    return appointment_mgr.get_by_date(date_str)

@eel.expose
def get_appointments_for_date(date_str):
    """Get both future and past appointments for a specific date
    
    Args:
        date_str: Date in YYYY-MM-DD format
    
    Returns:
        Dict with 'future' and 'past' appointment lists
    """
    return appointment_mgr.get_appointments_by_date(date_str)

@eel.expose
def update_next_appointment(patient_id, date, time, location):
    """Update patient's next appointment"""
    return appointment_mgr.update_next(patient_id, date, time, location)

@eel.expose
def schedule_appointment_with_conflict_check(patient_id, date, time, location):
    """
    Schedule appointment with atomic conflict detection to prevent double-booking
    
    Args:
        patient_id: Patient ID to schedule
        date: Appointment date (YYYY-MM-DD)
        time: Appointment time (HH:MM)
        location: Appointment location
        
    Returns:
        {
            'success': True/False,
            'conflict': True/False,
            'conflicting_patient': {...} if conflict, None otherwise,
            'patient': Updated patient data
        }
    """
    try:
        # Check if slot is already taken by another patient
        conflict_query = """
            SELECT patientID, patientName, patientFirstName, patientLastName
            FROM patients
            WHERE nextAppointment = ? 
              AND appointmentTime = ?
              AND patientID != ?
              AND currentState NOT IN ('INACTIVE', 'PREGNANT', 'COMPLETED', 'CANCELLED')
        """
        
        conflicting = db.fetchone(conflict_query, (date, time, patient_id))
        
        if conflicting:
            # Slot is taken!
            return {
                'success': False,
                'conflict': True,
                'conflicting_patient': conflicting,
                'patient': patient_mgr.get_by_id(patient_id)
            }
        
        # Slot appears free - use transaction to ensure atomicity
        # This prevents race condition if two users click simultaneously
        try:
            # Start transaction using the connection's isolation level
            db.conn.isolation_level = 'IMMEDIATE'
            
            # Double-check inside transaction (prevents race between check and update)
            conflicting = db.fetchone(conflict_query, (date, time, patient_id))
            
            if conflicting:
                db.rollback()
                return {
                    'success': False,
                    'conflict': True,
                    'conflicting_patient': conflicting,
                    'patient': patient_mgr.get_by_id(patient_id)
                }
            
            # Actually schedule the appointment
            appointment_mgr.update_next(patient_id, date, time, location)
            
            # Commit the transaction
            db.commit()
            
            # Restore default isolation level
            db.conn.isolation_level = ''
            
            # Get updated patient data
            updated_patient = patient_mgr.get_by_id(patient_id)
            
            return {
                'success': True,
                'conflict': False,
                'conflicting_patient': None,
                'patient': updated_patient
            }
            
        except Exception as e:
            db.rollback()
            db.conn.isolation_level = ''  # Restore default
            raise e
            
    except Exception as e:
        print(f"Error in schedule_appointment_with_conflict_check: {e}")
        return {
            'success': False,
            'conflict': False,
            'message': str(e),
            'conflicting_patient': None,
            'patient': patient_mgr.get_by_id(patient_id)
        }

@eel.expose
def add_appointment_history(patient_id, date, time, location, summary):
    """Add entry to appointment history"""
    return appointment_mgr.add_history(patient_id, date, time, location, summary)


# ============================================================================
# ACTION ITEMS API
# ============================================================================

@eel.expose
def get_action_items(tab):
    """Get action items for a specific tab"""
    return action_items_mgr.get_by_tab(tab)

@eel.expose
def add_action_item(tab, text, priority):
    """Add new action item"""
    return action_items_mgr.add(tab, text, priority)

@eel.expose
def toggle_action_item(item_id):
    """Toggle action item done/undone"""
    return action_items_mgr.toggle(item_id)

@eel.expose
def delete_action_item(item_id):
    """Delete action item"""
    return action_items_mgr.delete(item_id)


# ============================================================================
# CLINIC DAYS API
# ============================================================================

@eel.expose
def get_clinic_day(date_str):
    """Get clinic configuration for a date"""
    return clinic_days_mgr.get(date_str)

@eel.expose
def get_all_clinic_days():
    """Get all clinic day configurations"""
    return clinic_days_mgr.get_all()

@eel.expose
def get_month_clinic_days(year, month):
    """Get clinic days for a specific month"""
    return clinic_days_mgr.get_month(year, month)

@eel.expose
def update_clinic_day(date_str, config):
    """Update clinic configuration for a date"""
    return clinic_days_mgr.update(date_str, config)


# ============================================================================
# STATISTICS API
# ============================================================================

@eel.expose
def get_dashboard_stats():
    """Get statistics for dashboard"""
    return {
        'total_patients': patient_mgr.count_total(),
        'by_state': patient_mgr.count_by_state(),
        'appointments_today': appointment_mgr.count_today(),
        'action_items_pending': action_items_mgr.count_pending()
    }


# ============================================================================
# SESSION MANAGEMENT
# ============================================================================

def load_sessions():
    """Load active sessions from file"""
    try:
        if os.path.exists(SESSIONS_FILE):
            with open(SESSIONS_FILE, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        print(f"Error loading sessions: {e}")
        return {}

def save_sessions(sessions):
    """Save active sessions to file"""
    try:
        with open(SESSIONS_FILE, 'w') as f:
            json.dump(sessions, f, indent=2)
    except Exception as e:
        print(f"Error saving sessions: {e}")

@eel.expose
def register_session(username):
    """Register user session on login"""
    try:
        sessions = load_sessions()
        now = datetime.now().isoformat()
        
        # Check if user already has an active session
        if username in sessions:
            last_active = datetime.fromisoformat(sessions[username]['last_active'])
            time_diff = (datetime.now() - last_active).total_seconds() / 60
            
            # If last active > 5 minutes ago, consider session expired
            if time_diff > 5:
                print(f"✓ Previous session for {username} expired ({time_diff:.0f}min ago)")
            else:
                print(f"⚠ User {username} already has active session (last active {time_diff:.0f}min ago)")
                return {
                    'success': False,
                    'error': f'User already logged in (active {time_diff:.0f} minutes ago). Please wait {5 - int(time_diff)} more minute(s) or close the other session.'
                }
        
        sessions[username] = {
            'login_time': now,
            'last_active': now,
            'last_heartbeat': now
        }
        save_sessions(sessions)
        
        print(f"✓ Session registered: {username}")
        return {'success': True}
    except Exception as e:
        print(f"Error registering session: {e}")
        return {'success': False, 'error': str(e)}

@eel.expose
def update_session_heartbeat(username):
    """Update user's last active timestamp"""
    try:
        sessions = load_sessions()
        if username in sessions:
            sessions[username]['last_heartbeat'] = datetime.now().isoformat()
            sessions[username]['last_active'] = datetime.now().isoformat()
            save_sessions(sessions)
        return {'success': True}
    except Exception as e:
        print(f"Error updating heartbeat: {e}")
        return {'success': False}

@eel.expose
def unregister_session(username):
    """Unregister user session on logout"""
    try:
        sessions = load_sessions()
        if username in sessions:
            del sessions[username]
            save_sessions(sessions)
            print(f"✓ Session closed: {username}")
        return {'success': True}
    except Exception as e:
        print(f"Error unregistering session: {e}")
        return {'success': False}

@eel.expose
def force_logout_user(username):
    """Force logout a user (self-recovery when session is stuck)"""
    try:
        sessions = load_sessions()
        if username in sessions:
            del sessions[username]
            save_sessions(sessions)
            print(f"✓ Force logout: {username}")
            return {'success': True, 'message': f'Previous session for {username} has been terminated'}
        return {'success': False, 'error': 'No active session found'}
    except Exception as e:
        print(f"Error force logging out user: {e}")
        return {'success': False, 'error': str(e)}

@eel.expose
def get_active_users():
    """Get list of currently active users"""
    try:
        sessions = load_sessions()
        active_users = []
        now = datetime.now()
        
        # Clean up stale sessions (> 5 minutes inactive)
        for username in list(sessions.keys()):
            last_active = datetime.fromisoformat(sessions[username]['last_active'])
            time_diff = (now - last_active).total_seconds() / 60
            
            if time_diff > 5:
                del sessions[username]
            else:
                active_users.append({
                    'username': username,
                    'login_time': sessions[username]['login_time'],
                    'last_active': sessions[username]['last_active'],
                    'minutes_ago': int(time_diff)
                })
        
        save_sessions(sessions)
        return active_users
    except Exception as e:
        print(f"Error getting active users: {e}")
        return []


# ============================================================================
# BACKUP API
# ============================================================================

@eel.expose
def create_backup():
    """Create a backup of the database"""
    import shutil
    from datetime import datetime
    
    try:
        # Create backups folder if it doesn't exist
        backup_folder = "DB/backups"
        if not os.path.exists(backup_folder):
            os.makedirs(backup_folder)
        
        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = os.path.join(backup_folder, f"michaeli-clinic_{timestamp}.db")
        
        # Copy database file
        db_path = "DB/michaeli-clinic.db"
        shutil.copy2(db_path, backup_path)
        
        # Get file size
        file_size = os.path.getsize(backup_path)
        size_mb = file_size / (1024 * 1024)
        
        print(f"✓ Backup created: {backup_path} ({size_mb:.2f} MB)")
        
        return {
            'success': True,
            'filename': os.path.basename(backup_path),
            'path': backup_path,
            'size_mb': round(size_mb, 2)
        }
    except Exception as e:
        print(f"Error creating backup: {e}")
        return {
            'success': False,
            'error': str(e)
        }

@eel.expose
def get_last_backup_date():
    """Get the date of the most recent backup"""
    try:
        backup_folder = "DB/backups"
        if not os.path.exists(backup_folder):
            return None
        
        # Get all backup files
        backup_files = [f for f in os.listdir(backup_folder) if f.endswith('.db')]
        if not backup_files:
            return None
        
        # Get most recent file
        backup_files.sort(reverse=True)
        most_recent = backup_files[0]
        
        # Extract date from filename (michaeli-clinic_YYYYMMDD_HHMMSS.db)
        # Split: ['michaeli-clinic', 'YYYYMMDD', 'HHMMSS.db']
        parts = most_recent.split('_')
        if len(parts) >= 2:
            date_part = parts[1]  # YYYYMMDD (was incorrectly [2])
            return date_part
        
        return None
    except Exception as e:
        print(f"Error getting last backup date: {e}")
        return None

@eel.expose
def get_last_modified_timestamp():
    """
    Get the most recent modification timestamp from the database.
    Checks state_history, appointment_history, and notes_history tables.
    Returns ISO timestamp of the most recent change.
    """
    try:
        # Query to get the most recent timestamp from all history tables
        query = """
        SELECT MAX(last_modified) as last_mod FROM (
            SELECT MAX(timestamp) as last_modified
            FROM state_history
            
            UNION ALL
            
            SELECT MAX(timestamp) as last_modified
            FROM appointment_history
            
            UNION ALL
            
            SELECT MAX(timestamp) as last_modified
            FROM notes_history
        )
        """
        
        result = db.fetchone(query)
        
        if result and result.get('last_mod'):
            return result['last_mod']
        else:
            # Return current time if no data
            return datetime.now().isoformat()
            
    except Exception as e:
        print(f"Error getting last modified timestamp: {e}")
        # Return current time on error to force refresh
        return datetime.now().isoformat()


# ============================================================================
# PASSWORD HASHING
# ============================================================================

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

@eel.expose
def login(username, password):
    """Handle user login - checks database users first, then code-based fallback"""
    global current_user, current_user_is_admin
    
    username = username.lower()
    entered_hash = hash_password(password)
    
    # Try database users first
    try:
        user = db.fetchone("SELECT username, passwordHash, isAdmin FROM users WHERE username = ?", (username,))
        
        if user and user['passwordHash'] == entered_hash:
            # Valid database user - update last login
            current_user = username
            current_user_is_admin = bool(user.get('isAdmin', 0))
            
            db.execute("UPDATE users SET lastLogin = ? WHERE username = ?", 
                      (int(datetime.now().timestamp()), username))
            db.commit()
            
            # Create daily backup (if not already created today)
            try:
                last_backup = get_last_backup_date()
                today = datetime.now().strftime('%Y%m%d')
                
                if last_backup != today:
                    print(f"Creating daily backup...")
                    create_backup()
                else:
                    print(f"Daily backup already exists for {today}")
            except Exception as e:
                print(f"Backup check/create error: {e}")
            
            print(f"✓ User logged in: {username} (admin: {current_user_is_admin})")
            
            return {
                'status': 'success',
                'username': username,
                'isAdmin': current_user_is_admin
            }
    except Exception as e:
        print(f"Database user check error: {e}")
    
    # Fall back to code-based users (admin/jennia as backup)
    if username in VALID_USERS and entered_hash == VALID_USERS[username]:
        current_user = username
        current_user_is_admin = True  # Code-based users are always admin
        
        # Create daily backup for code-based users too
        try:
            last_backup = get_last_backup_date()
            today = datetime.now().strftime('%Y%m%d')
            
            if last_backup != today:
                print(f"Creating daily backup...")
                create_backup()
            else:
                print(f"Daily backup already exists for {today}")
        except Exception as e:
            print(f"Backup check/create error: {e}")
        
        print(f"✓ User logged in (code-based): {username}")
        
        return {
            'status': 'success',
            'username': username,
            'isAdmin': True
        }
    
    # Login failed
    return {
        'status': 'error',
        'message': 'Invalid username or password'
    }


# ============================================================================
# USER MANAGEMENT API
# ============================================================================

@eel.expose
def get_users():
    """Get all users (admin only)"""
    global current_user_is_admin
    
    if not current_user_is_admin:
        return {'status': 'error', 'message': 'Admin access required'}
    
    try:
        users = db.fetchall("SELECT username, lastLogin, isAdmin FROM users ORDER BY username")
        
        users_list = []
        for user in users:
            users_list.append({
                'username': user['username'],
                'lastLogin': user['lastLogin'],
                'isAdmin': bool(user.get('isAdmin', 0))
            })
        
        return {'status': 'success', 'users': users_list}
    except Exception as e:
        print(f"Error getting users: {e}")
        return {'status': 'error', 'message': f'Database error: {str(e)}'}

@eel.expose
def add_user(username, password, is_admin):
    """Add new user (admin only)"""
    global current_user_is_admin
    
    if not current_user_is_admin:
        return {'status': 'error', 'message': 'Admin access required'}
    
    username = username.lower().strip()
    
    if not username:
        return {'status': 'error', 'message': 'Username cannot be empty'}
    
    # Validate password
    if len(password) < 8:
        return {'status': 'error', 'message': 'Password must be at least 8 characters'}
    
    has_lower = any(c.islower() for c in password)
    has_upper = any(c.isupper() for c in password)
    has_digit = any(c.isdigit() for c in password)
    
    if not (has_lower and has_upper and has_digit):
        return {'status': 'error', 'message': 'Password must contain lowercase, uppercase, and numbers'}
    
    try:
        # Check if user already exists
        existing = db.fetchone("SELECT username FROM users WHERE username = ?", (username,))
        if existing:
            return {'status': 'error', 'message': 'Username already exists'}
        
        # Insert new user
        password_hash = hash_password(password)
        db.execute(
            "INSERT INTO users (username, passwordHash, lastLogin, isAdmin) VALUES (?, ?, NULL, ?)",
            (username, password_hash, 1 if is_admin else 0)
        )
        db.commit()
        
        return {'status': 'success', 'message': f'User {username} added successfully'}
    except Exception as e:
        db.rollback()
        print(f"Error adding user: {e}")
        return {'status': 'error', 'message': f'Database error: {str(e)}'}

@eel.expose
def update_user_password(username, new_password):
    """Update user password (admin only)"""
    global current_user_is_admin
    
    if not current_user_is_admin:
        return {'status': 'error', 'message': 'Admin access required'}
    
    # Validate password
    if len(new_password) < 8:
        return {'status': 'error', 'message': 'Password must be at least 8 characters'}
    
    has_lower = any(c.islower() for c in new_password)
    has_upper = any(c.isupper() for c in new_password)
    has_digit = any(c.isdigit() for c in new_password)
    
    if not (has_lower and has_upper and has_digit):
        return {'status': 'error', 'message': 'Password must contain lowercase, uppercase, and numbers'}
    
    try:
        new_hash = hash_password(new_password)
        cursor = db.execute("UPDATE users SET passwordHash = ? WHERE username = ?", (new_hash, username))
        
        if cursor.rowcount == 0:
            return {'status': 'error', 'message': 'User not found'}
        
        db.commit()
        return {'status': 'success', 'message': f'Password updated for {username}'}
    except Exception as e:
        db.rollback()
        print(f"Error updating password: {e}")
        return {'status': 'error', 'message': f'Database error: {str(e)}'}

@eel.expose
def change_password(old_password, new_password):
    """Change password for current user"""
    global current_user
    
    if not current_user:
        return {'status': 'error', 'message': 'Not logged in'}
    
    # Validate new password
    if len(new_password) < 8:
        return {'status': 'error', 'message': 'Password must be at least 8 characters'}
    
    has_lower = any(c.islower() for c in new_password)
    has_upper = any(c.isupper() for c in new_password)
    has_digit = any(c.isdigit() for c in new_password)
    
    if not (has_lower and has_upper and has_digit):
        return {'status': 'error', 'message': 'Password must contain lowercase, uppercase, and numbers'}
    
    try:
        # Verify old password
        old_hash = hash_password(old_password)
        user = db.fetchone("SELECT passwordHash FROM users WHERE username = ?", (current_user,))
        
        if not user:
            return {'status': 'error', 'message': 'User not found in database'}
        
        if user['passwordHash'] != old_hash:
            return {'status': 'error', 'message': 'Current password is incorrect'}
        
        # Update password
        new_hash = hash_password(new_password)
        db.execute("UPDATE users SET passwordHash = ? WHERE username = ?", (new_hash, current_user))
        db.commit()
        
        return {'status': 'success', 'message': 'Password changed successfully'}
    except Exception as e:
        db.rollback()
        print(f"Error changing password: {e}")
        return {'status': 'error', 'message': f'Database error: {str(e)}'}

@eel.expose
def update_user_admin(username, is_admin):
    """Update user admin status (admin only)"""
    global current_user_is_admin
    
    if not current_user_is_admin:
        return {'status': 'error', 'message': 'Admin access required'}
    
    if username == 'admin':
        return {'status': 'error', 'message': 'Cannot modify admin user'}
    
    try:
        cursor = db.execute("UPDATE users SET isAdmin = ? WHERE username = ?", (1 if is_admin else 0, username))
        
        if cursor.rowcount == 0:
            return {'status': 'error', 'message': 'User not found'}
        
        db.commit()
        return {'status': 'success', 'message': f'Admin status updated for {username}'}
    except Exception as e:
        db.rollback()
        print(f"Error updating admin status: {e}")
        return {'status': 'error', 'message': f'Database error: {str(e)}'}

@eel.expose
def remove_user(username):
    """Remove user (admin only)"""
    global current_user_is_admin
    
    if not current_user_is_admin:
        return {'status': 'error', 'message': 'Admin access required'}
    
    if username == 'admin':
        return {'status': 'error', 'message': 'Cannot delete admin user'}
    
    try:
        cursor = db.execute("DELETE FROM users WHERE username = ?", (username,))
        
        if cursor.rowcount == 0:
            return {'status': 'error', 'message': 'User not found'}
        
        db.commit()
        return {'status': 'success', 'message': f'User {username} removed successfully'}
    except Exception as e:
        db.rollback()
        print(f"Error removing user: {e}")
        return {'status': 'error', 'message': f'Database error: {str(e)}'}

# ============================================================================
# EMAIL TEMPLATES API
# ============================================================================

@eel.expose
def get_email_templates():
    """Load email templates from JSON file"""
    templates_file = "DB/email-templates.json"
    try:
        if os.path.exists(templates_file):
            with open(templates_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            print(f"Email templates file not found: {templates_file}")
            return None
    except Exception as e:
        print(f"Error loading email templates: {e}")
        return None

@eel.expose
def save_email_to_file(content, filename=None):
    """Save email content to Downloads folder"""
    global current_user
    
    try:
        # Get Downloads folder path
        if os.name == 'nt':  # Windows
            downloads_path = os.path.join(os.environ['USERPROFILE'], 'Downloads')
        else:  # Mac/Linux
            downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads')
        
        # Use HTA filename: pending-emails.json
        if not filename:
            filename = 'pending-emails.json'
        
        # Full path
        filepath = os.path.join(downloads_path, filename)
        
        # Write content (will overwrite existing file)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✓ Email saved to: {filepath}")
        
        return {
            'status': 'success',
            'filepath': filepath,
            'filename': filename
        }
        
    except Exception as e:
        print(f"Error saving email: {e}")
        return {
            'status': 'error',
            'message': str(e)
        }



# ============================================================================
# SYSTEM API
# ============================================================================

@eel.expose
def get_command_line_args():
    """Get command line arguments for debug mode detection"""
    return sys.argv

@eel.expose
def get_changed_patients_since(timestamp):
    """
    Get list of patient IDs that changed since given timestamp
    Used for smart auto-refresh (only reload changed patients)
    Returns: [patientID1, patientID2, ...]
    """
    if not timestamp:
        timestamp = '2000-01-01'
    
    query = """
        SELECT DISTINCT patientID 
        FROM (
            SELECT patientID FROM state_history WHERE timestamp > ?
            UNION
            SELECT patientID FROM appointment_history WHERE timestamp > ?
            UNION
            SELECT patientID FROM notes_history WHERE timestamp > ?
        )
    """
    results = db.fetchall(query, (timestamp, timestamp, timestamp))
    return [r['patientID'] for r in results]


# ============================================================================
# PORTAL API
# ============================================================================

@eel.expose
def get_missing_portal_access(force_refresh=False):
    """
    Get list of patients/partners missing portal access
    
    Args:
        force_refresh: If True, ignore cache and read XLS file again
    """
    try:
        return portal_mgr.get_missing_portal_access(force_refresh)
    except Exception as e:
        import traceback
        error_msg = str(e)
        traceback_msg = traceback.format_exc()
        print(f"Portal error: {error_msg}")
        print(traceback_msg)
        
        # Check if it's an xlrd import error
        if 'xlrd' in error_msg or 'xlrd' in traceback_msg:
            error_msg = "xlrd library not installed. Please run: pip install xlrd --break-system-packages"
        
        return {
            'error': error_msg,
            'missing': []
        }


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point"""
    import sys
    import socket
    
    # Check for debug flag
    debug_mode = '--debug' in sys.argv or '-d' in sys.argv
    
    # Hide console window unless in debug mode (Windows only)
    if not debug_mode and os.name == 'nt':
        try:
            import ctypes
            ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)
        except:
            pass  # Silently fail if hiding console doesn't work
    
    print("="*60)
    print("MICHAELI CLINIC DASHBOARD")
    print("="*60)
    print()
    
    if debug_mode:
        print("🐛 DEBUG MODE: Console will stay visible")
        print()
    
    # Initialize
    initialize_app()
    
    # Find available port (in case another instance is running)
    def find_free_port(start_port=8080):
        """Find a free port starting from start_port"""
        port = start_port
        while port < start_port + 100:  # Try up to 100 ports
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(('localhost', port))
                sock.close()
                return port
            except OSError:
                port += 1
        return start_port  # Fallback to original
    
    port = find_free_port(8080)
    if port != 8080:
        print(f"⚠️  Port 8080 in use, using port {port} instead")
    
    # Determine web folder location
    # When running as EXE, web folder is next to exe
    # When running as script, web folder is in same directory
    if getattr(sys, 'frozen', False):
        # Running as compiled exe
        web_folder = os.path.join(os.path.dirname(sys.executable), 'web')
    else:
        # Running as script
        web_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'web')
    
    # Initialize Eel with web folder
    eel.init(web_folder)
    print(f"📁 Web folder: {web_folder}")
    print(f"🌐 Starting server on port {port}...")
    print()
    
    # Start Eel with Edge as default (faster on corporate computers)
    try:
        # Try browsers in order: Edge (default) -> Chrome -> Default browser
        try:
            print("Starting with Microsoft Edge...")
            eel.start('index.html', mode='edge', size=(1920, 1080), port=port)
        except (OSError, Exception) as e:
            if debug_mode:
                print(f"Edge not available: {e}")
            try:
                print("Attempting to start with Chrome...")
                eel.start('index.html', mode='chrome', size=(1920, 1080), port=port)
            except (OSError, Exception) as e2:
                if debug_mode:
                    print(f"Chrome not available: {e2}")
                print("Starting with default browser...")
                eel.start('index.html', mode=None, size=(1920, 1080), port=port)
    except (SystemExit, KeyboardInterrupt):
        print("\nShutting down...")
        if db:
            db.close()


if __name__ == '__main__':
    main()
