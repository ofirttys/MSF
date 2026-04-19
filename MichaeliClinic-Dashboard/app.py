#!/usr/bin/env python3
"""
MichaeliClinic-Dashboard - Main Application
Python + Eel + SQLite desktop application
"""

import eel
import sys
import os
from pathlib import Path

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
        eel.start('index.html', size=(1400, 900), port=8080)
    except (SystemExit, KeyboardInterrupt):
        print("\nShutting down...")
        if db:
            db.close()


if __name__ == '__main__':
    main()
