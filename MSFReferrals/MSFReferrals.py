import eel
import json
import sys
import os
import tempfile
import threading
import time
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
DATABASE_FILE = str(Path(DB_FOLDER) / 'referrals.json')
LOCK_FILE = str(Path(DB_FOLDER) / 'referrals.lock')
LOCK_STALE_HOURS = 4

# DEBUG MODE: Set to True to see password hashes for setup
DEBUG_MODE = False

# User credentials (hashed passwords)
# To add a user: Set DEBUG_MODE=True, login with password, copy hash shown in alert
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
        # Force to 32-bit signed integer like JavaScript does
        if hash_val > 0x7FFFFFFF:
            hash_val = hash_val - 0x100000000
        elif hash_val < -0x80000000:
            hash_val = hash_val + 0x100000000
    
    # Convert to positive hex string with padding
    hex_hash = format(hash_val & 0xFFFFFFFF, 'x')  # Ensure positive
    while len(hex_hash) < 8:
        hex_hash = '0' + hex_hash
    
    # Extend to 64 characters using helper function
    extended = hex_hash
    for i in range(7):
        extended += _simple_hash(hex_hash + str(i))
    
    return extended[:64]

def _simple_hash(s):
    """Helper function for hash extension - matches HTA version"""
    h = 0
    for char in s:
        h = ((h << 5) - h) + ord(char)
        # Force to 32-bit signed integer
        if h > 0x7FFFFFFF:
            h = h - 0x100000000
        elif h < -0x80000000:
            h = h + 0x100000000
    
    hex_result = format(h & 0xFFFFFFFF, 'x')  # Ensure positive
    while len(hex_result) < 8:
        hex_result = '0' + hex_result
    return hex_result[:8]

@eel.expose
def login(username, password):
    """Handle user login"""
    global current_user, is_read_only, lock_owner
    
    username = username.lower()
    entered_hash = hash_password(password)
    
    # DEBUG MODE: Show the hash for the entered password
    if DEBUG_MODE:
        print(f'\n=== DEBUG MODE ===')
        print(f'Password: {password}')
        print(f'Hash: {entered_hash}')
        print(f'\nTo use this hash:')
        print(f'1. Copy the hash above')
        print(f'2. Update VALID_USERS in the Python file')
        print(f'3. Set DEBUG_MODE = False')
        print(f'4. Save and restart')
        print(f'==================\n')
    
    # Check credentials
    if username not in VALID_USERS or entered_hash != VALID_USERS[username]:
        return {'status': 'error', 'message': 'Invalid username or password'}
    
    # Check lock file
    lock_status = check_lock_file()
    
    if lock_status['locked'] and not lock_status['stale']:
        # Ask user if they want read-only access
        current_user = username
        is_read_only = True
        lock_owner = lock_status['user']
        return {
            'status': 'locked',
            'user': lock_status['user'],
            'timestamp': lock_status['timestamp'],
            'message': f"Database is locked by {lock_status['user']}"
        }
    
    # Clear stale lock if needed
    if lock_status.get('stale', False):
        delete_lock_file()
    
    # Create lock file
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
def load_database():
    """Load referrals database"""
    try:
        if not os.path.exists(DATABASE_FILE):
            # Create empty database
            os.makedirs(DB_FOLDER, exist_ok=True)
            empty_db = {
                'referrals': [],
                'nextId': 1,
                'selectOptions': get_default_select_options()
            }
            with open(DATABASE_FILE, 'w') as f:
                json.dump(empty_db, f, indent=2)
            return {'status': 'success', 'data': empty_db}
        
        with open(DATABASE_FILE, 'r') as f:
            data = json.load(f)
        
        # Ensure selectOptions exists
        if 'selectOptions' not in data:
            data['selectOptions'] = get_default_select_options()
        
        return {'status': 'success', 'data': data}
    except Exception as e:
        print(f"Error loading database: {e}")
        return {'status': 'error', 'message': str(e)}

@eel.expose
def save_database(data):
    """Save referrals database"""
    if is_read_only:
        return {'status': 'error', 'message': 'Cannot save in read-only mode'}
    
    try:
        with open(DATABASE_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Refresh lock file
        refresh_lock_file()
        
        return {'status': 'success'}
    except Exception as e:
        print(f"Error saving database: {e}")
        return {'status': 'error', 'message': str(e)}

@eel.expose
def add_referral(referral_data):
    """Add new referral to database"""
    if is_read_only:
        return {'status': 'error', 'message': 'Cannot add referrals in read-only mode'}
    
    try:
        # Load current database
        with open(DATABASE_FILE, 'r') as f:
            db = json.load(f)
        
        # Add referralID and timestamp
        referral_data['referralID'] = db['nextId']
        referral_data['addedToDBDate'] = datetime.now().isoformat()
        db['nextId'] += 1
        
        # Add to referrals list
        db['referrals'].append(referral_data)
        
        # Save
        with open(DATABASE_FILE, 'w') as f:
            json.dump(db, f, indent=2)
        
        refresh_lock_file()
        
        return {'status': 'success', 'referral': referral_data}
    except Exception as e:
        print(f"Error adding referral: {e}")
        return {'status': 'error', 'message': str(e)}

@eel.expose
def update_referral(referral_id, referral_data):
    """Update existing referral"""
    if is_read_only:
        return {'status': 'error', 'message': 'Cannot update referrals in read-only mode'}
    
    try:
        with open(DATABASE_FILE, 'r') as f:
            db = json.load(f)
        
        # Find and update referral
        for i, ref in enumerate(db['referrals']):
            if ref['referralID'] == referral_id:
                db['referrals'][i] = referral_data
                break
        
        # Save
        with open(DATABASE_FILE, 'w') as f:
            json.dump(db, f, indent=2)
        
        refresh_lock_file()
        
        return {'status': 'success', 'referral': referral_data}
    except Exception as e:
        print(f"Error updating referral: {e}")
        return {'status': 'error', 'message': str(e)}

@eel.expose
def delete_referral(referral_id):
    """Delete referral from database"""
    if is_read_only:
        return {'status': 'error', 'message': 'Cannot delete referrals in read-only mode'}
    
    try:
        with open(DATABASE_FILE, 'r') as f:
            db = json.load(f)
        
        # Remove referral
        db['referrals'] = [r for r in db['referrals'] if r['referralID'] != referral_id]
        
        # Save
        with open(DATABASE_FILE, 'w') as f:
            json.dump(db, f, indent=2)
        
        refresh_lock_file()
        
        return {'status': 'success'}
    except Exception as e:
        print(f"Error deleting referral: {e}")
        return {'status': 'error', 'message': str(e)}

def get_default_select_options():
    """Get default select options for dropdowns"""
    return {
        'requestedLocations': [
            'Any',
            'Downtown',
            'Mississauga',
            'Vaughan'
        ],
        'requestedPhysicians': [
            'First Available',
            'Dr. Bacal',
            'Dr. Greenblatt',
            'Dr. Jones',
            'Dr. Liu',
            'Dr. Michaeli',
            'Dr. Pereira',
            'Dr. Russo',
            'Dr. Shapiro'
        ],
        'servicesRequested': [
            'Infertility',
            'EEF',
            'ONC',
            'SB',
            'RPL',
            'Donor',
            'ARA',
            'PGD',
            'Gyne',
            'Other'
        ],
        'referralType': [
            'New',
            'Previous',
            'Partner'
        ],
        'lastAttemptModes': [
            'Phone',
            'E-Mail'
        ],
        'physicianAdmins': [
            'CJ Admin',
            'EG Admin',
            'HS Admin',
            'JM Admin',
            'KL Admin',
            'MR Admin',
            'NP Admin',
            'VB Admin',
            'NursePrac Admin',
            'Fellow Admin'
        ],
        'genderAtBirth': [
            'Female',
            'Male',
            'Other'
        ]
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

def shutdown():
    """Cleanup on shutdown"""
    global _shutting_down
    if _shutting_down:
        return
    _shutting_down = True
    if not is_read_only:
        delete_lock_file()

if __name__ == '__main__':
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
