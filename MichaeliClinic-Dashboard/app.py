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

# Initialize Eel with allowed file extensions
eel.init('web', allowed_extensions=['.js', '.html', '.css'])

# Global managers
db = None
patient_mgr = None
appointment_mgr = None
action_items_mgr = None
clinic_days_mgr = None

# Session tracking
SESSIONS_FILE = "DB/active_sessions.json"


def initialize_app():
    """Initialize database and managers"""
    global db, patient_mgr, appointment_mgr, action_items_mgr, clinic_days_mgr
    
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
    
    # Initialize managers
    patient_mgr = PatientManager(db)
    appointment_mgr = AppointmentManager(db)
    action_items_mgr = ActionItemsManager(db)
    clinic_days_mgr = ClinicDaysManager(db)
    
    print("✓ Application initialized")


# ============================================================================
# PATIENT API
# ============================================================================

@eel.expose
def get_all_patients():
    """Get all patients"""
    return patient_mgr.get_all()

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
def update_patient_notes(patient_id, notes):
    """Update patient notes"""
    return patient_mgr.update_notes(patient_id, notes)


# ============================================================================
# APPOINTMENT API
# ============================================================================

@eel.expose
def get_appointments_by_date(date_str):
    """Get all appointments for a specific date"""
    return appointment_mgr.get_by_date(date_str)

@eel.expose
def get_todays_appointments():
    """Get today's appointments"""
    return appointment_mgr.get_today()

@eel.expose
def update_next_appointment(patient_id, date, time, location):
    """Update patient's next appointment"""
    return appointment_mgr.update_next(patient_id, date, time, location)


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
            
            # If last active > 30 minutes ago, consider session expired
            if time_diff > 30:
                print(f"✓ Previous session for {username} expired ({time_diff:.0f}min ago)")
            else:
                print(f"⚠ User {username} already has active session (last active {time_diff:.0f}min ago)")
                return {
                    'success': False,
                    'error': f'User already logged in (active {time_diff:.0f} minutes ago)'
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
def get_active_users():
    """Get list of currently active users"""
    try:
        sessions = load_sessions()
        active_users = []
        now = datetime.now()
        
        # Clean up stale sessions (> 30 minutes inactive)
        for username in list(sessions.keys()):
            last_active = datetime.fromisoformat(sessions[username]['last_active'])
            time_diff = (now - last_active).total_seconds() / 60
            
            if time_diff > 30:
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
        date_part = most_recent.split('_')[2]  # YYYYMMDD
        return date_part
    except Exception as e:
        print(f"Error getting last backup date: {e}")
        return None


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point"""
    print("="*60)
    print("MichaeliClinic-Dashboard")
    print("="*60)
    
    # Initialize
    initialize_app()
    
    # Start Eel
    try:
        # Start in app mode (no browser chrome) with maximized window
        eel.start('index.html', mode='chrome', size=(1920, 1080), port=8080)
    except (SystemExit, KeyboardInterrupt):
        print("\nShutting down...")
        if db:
            db.close()


if __name__ == '__main__':
    main()
