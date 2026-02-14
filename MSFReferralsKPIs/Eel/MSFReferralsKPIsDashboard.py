import eel
import pandas as pd
import sys
import os
import tempfile
import threading
import time
from pathlib import Path
import psutil
import ctypes


# DB Folder configuration
if getattr(sys, 'frozen', False):
    # Running as compiled exe - DB folder should be next to the exe
    exe_dir = Path(sys.executable).parent
else:
    # Running as script - DB folder next to the .py file
    exe_dir = Path(__file__).parent

DB_FOLDER = str(exe_dir / 'DB')

# Global dataframe
df = None

# Lock file for single instance
username = os.getenv('USERNAME', 'user')
LOCK_FILE = Path(tempfile.gettempdir()) / f'.msf_dashboard_{username}.lock'
LOCK_TIMEOUT = 300  # 5 minutes

# Shutdown flag
_shutting_down = False

def is_lock_file_stale():
    """Check if lock file is older than LOCK_TIMEOUT"""
    if not LOCK_FILE.exists():
        return False
    age = time.time() - LOCK_FILE.stat().st_mtime
    return age > LOCK_TIMEOUT

def cleanup_lock_file():
    """Remove lock file if it exists"""
    if LOCK_FILE.exists():
        try:
            LOCK_FILE.unlink()
        except:
            pass

def create_lock_file():
    """Create lock file with current PID"""
    try:
        # Check for existing lock
        if LOCK_FILE.exists():
            if is_lock_file_stale():
                print("Removing stale lock file")
                LOCK_FILE.unlink()
            else:
                # Check if process still exists
                try:
                    with open(LOCK_FILE, 'r') as f:
                        old_pid = int(f.read().strip())
                    if not psutil.pid_exists(old_pid):
                        print("Process no longer exists, removing lock")
                        LOCK_FILE.unlink()
                    else:
                        print("Another instance is already running")
                        sys.exit(1)
                except:
                    LOCK_FILE.unlink()
        
        # Create new lock file
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
        
        # Auto-remove lock after window opens (5 seconds)
        def remove_lock():
            time.sleep(5)
            cleanup_lock_file()
        threading.Thread(target=remove_lock, daemon=True).start()
        
    except Exception as e:
        print(f"Lock file error: {e}")

@eel.expose
def read_csv_file(filename):
    """Read CSV file and return content"""
    try:
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8-sig') as f:
                return f.read()
    except Exception as e:
        print(f"Error reading file: {e}")
    return None

@eel.expose
def refresh_and_load():
    """
    One function that does EVERYTHING:
    1. Scan DB folder
    2. Pick newest CSV
    3. Load it
    4. Calculate all chart data
    5. Return everything
    """
    try:
        # Step 1 & 2: Scan and pick newest file
        csv_result = get_csv_files()
        if csv_result.get('error'):
            return {'status': 'error', 'message': csv_result['error']}
        
        filename = csv_result['files'][0]  # Newest file
        print(f"Loading newest file: {filename}")
        
        # Step 3: Load and process CSV
        load_result = load_and_process_csv(filename)
        if load_result.get('status') == 'error':
            return load_result
        
        # Step 4: Get all chart data (no filters initially)
        chart_data = get_all_chart_data({})
        if chart_data.get('status') == 'error':
            return chart_data
        
        # Step 5: Return everything in one response
        return {
            'status': 'ok',
            'filename': filename,
            'records': load_result['records'],
            'dateRange': load_result['dateRange'],
            'services': load_result.get('services', []),
            'physicians': load_result.get('physicians', []),
            'charts': chart_data
        }
    except Exception as e:
        print(f"Error in refresh_and_load: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}


@eel.expose
def get_csv_files():
    """Get list of CSV files in DB folder"""
    try:
        print(f"Looking for CSV files in: {DB_FOLDER}")
        
        if not os.path.exists(DB_FOLDER):
            os.makedirs(DB_FOLDER, exist_ok=True)
            print(f"Created DB folder at: {DB_FOLDER}")
            return {'error': f'No CSV files found.\n\nPlease add CSV files to:\n{DB_FOLDER}'}
        
        files = [f for f in os.listdir(DB_FOLDER) if f.lower().endswith('.csv')]
        
        if not files:
            print(f"DB folder exists but no CSV files found")
            return {'error': f'No CSV files found in DB folder.\n\nPlease add CSV files to:\n{DB_FOLDER}'}
        
        print(f"Found {len(files)} CSV file(s): {files}")
        files.sort(reverse=True)  # Newest first
        return {'files': files}
    except Exception as e:
        print(f"Error scanning DB folder: {e}")
        return {'error': f'Folder error: {str(e)}\n\nDB folder: {DB_FOLDER}'}

@eel.expose
def load_and_process_csv(filename):
    """Load CSV from DB folder and return metadata"""
    global df
    
    try:
        # Construct full path
        filepath = os.path.join(DB_FOLDER, filename)
        
        if not os.path.exists(filepath):
            return {'status': 'error', 'message': f'File not found: {filename}'}
        
        # Try multiple encodings (Excel often uses cp1252/Windows-1252)
        encodings = ['utf-8-sig', 'utf-8', 'cp1252', 'latin1', 'iso-8859-1']
        df = None
        last_error = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(filepath, encoding=encoding)
                print(f"Successfully loaded with encoding: {encoding}")
                break
            except UnicodeDecodeError as e:
                last_error = e
                continue
        
        if df is None:
            return {'status': 'error', 'message': f'Could not decode CSV file. Last error: {last_error}'}
        
        # Parse dates with multiple formats
        date_cols = ['Date Referral Received', '1st Attempt to reach Patient/Referring MD', 
                     'Date Complete Information received']
        
        for col in date_cols:
            if col in df.columns:
                # Try DD-MMM-YY format first (e.g., "17-Sep-24")
                df[col] = pd.to_datetime(df[col], format='%d-%b-%y', errors='coerce')
        
        # Add computed fields
        df['month'] = df['Date Referral Received'].dt.strftime('%b/%y')
        df['is_new'] = df['New or Returning'].str.lower().str.contains('new', na=False)
        
        # Completion status
        df['status_lower'] = df['Referral Complete'].fillna('').str.lower()
        df['is_complete'] = df['status_lower'] == 'complete'
        df['is_cancelled'] = df['status_lower'] == 'cancelled'
        df['is_deferred'] = df['status_lower'] == 'deferred'
        df['is_pending'] = (df['status_lower'] == 'pending') | (df['status_lower'] == '')
        
        # Get unique values for filters
        services = df['Service Requested'].dropna().unique().tolist()
        physicians = df['Requested Physician'].dropna().unique().tolist()
        
        # Date range
        min_date = df['Date Referral Received'].min()
        max_date = df['Date Referral Received'].max()
        
        return {
            'status': 'ok',
            'records': len(df),
            'dateRange': {
                'min': min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else None,
                'max': max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else None
            },
            'services': sorted([s for s in services if s]),
            'physicians': sorted([p for p in physicians if p])
        }
        
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return {'status': 'error', 'message': str(e)}

def apply_filters(filters):
    """Apply filters to dataframe"""
    global df
    
    if df is None:
        return pd.DataFrame()
    
    filtered = df.copy()
    
    # Date range filter
    if filters.get('startDate'):
        start = pd.to_datetime(filters['startDate'])
        filtered = filtered[filtered['Date Referral Received'] >= start]
    
    if filters.get('endDate'):
        end = pd.to_datetime(filters['endDate'])
        filtered = filtered[filtered['Date Referral Received'] <= end]
    
    # Service filter
    if filters.get('services') and len(filters['services']) > 0:
        filtered = filtered[filtered['Service Requested'].isin(filters['services'])]
    
    # Physician filter
    if filters.get('physicians') and len(filters['physicians']) > 0:
        filtered = filtered[filtered['Requested Physician'].isin(filters['physicians'])]
    
    return filtered

def calc_monthly_trends(filtered):
    """Calculate monthly trends data"""
    if filtered.empty:
        return {'months': [], 'new': [], 'returning': [], 'total': []}
    
    # Group by month
    monthly = filtered.groupby('month').agg({
        'is_new': 'sum'
    }).reset_index()
    monthly['total'] = filtered.groupby('month').size().values
    monthly.columns = ['month', 'new', 'total']
    monthly['returning'] = monthly['total'] - monthly['new']
    
    # Sort by date
    monthly['sort_date'] = pd.to_datetime(monthly['month'], format='%b/%y')
    monthly = monthly.sort_values('sort_date')
    
    return {
        'months': monthly['month'].tolist(),
        'new': monthly['new'].astype(int).tolist(),
        'returning': monthly['returning'].astype(int).tolist(),
        'total': monthly['total'].astype(int).tolist()
    }

def calc_service_trends(filtered):
    """Calculate service type trends"""
    if filtered.empty:
        return {'months': [], 'services': [], 'data': {}}
    
    # Group by month and service
    service_monthly = filtered.groupby(['month', 'Service Requested']).size().unstack(fill_value=0)
    
    # Sort by date
    service_monthly['sort_date'] = pd.to_datetime(service_monthly.index, format='%b/%y')
    service_monthly = service_monthly.sort_values('sort_date')
    service_monthly = service_monthly.drop('sort_date', axis=1)
    
    return {
        'months': service_monthly.index.tolist(),
        'services': service_monthly.columns.tolist(),
        'data': service_monthly.to_dict('index')
    }

def calc_physician_trends(filtered, limit=10):
    """Calculate top physicians trends"""
    if filtered.empty:
        return {'months': [], 'physicians': [], 'data': {}}
    
    # Get top physicians
    top_physicians = filtered['Requested Physician'].value_counts().head(limit).index.tolist()
    
    # Group by month and physician
    phys_monthly = filtered[filtered['Requested Physician'].isin(top_physicians)].groupby(
        ['month', 'Requested Physician']
    ).size().unstack(fill_value=0)
    
    # Sort by date
    if not phys_monthly.empty:
        phys_monthly['sort_date'] = pd.to_datetime(phys_monthly.index, format='%b/%y')
        phys_monthly = phys_monthly.sort_values('sort_date')
        phys_monthly = phys_monthly.drop('sort_date', axis=1)
    
    return {
        'months': phys_monthly.index.tolist() if not phys_monthly.empty else [],
        'physicians': phys_monthly.columns.tolist() if not phys_monthly.empty else [],
        'data': phys_monthly.to_dict('index') if not phys_monthly.empty else {}
    }

def calc_completion_status(filtered):
    """Calculate completion status trends"""
    if filtered.empty:
        return {'months': [], 'data': {}}
    
    # Group by month and status
    status_monthly = filtered.groupby('month').agg({
        'is_complete': 'sum',
        'is_pending': 'sum',
        'is_cancelled': 'sum',
        'is_deferred': 'sum'
    })
    
    # Sort by date
    status_monthly['sort_date'] = pd.to_datetime(status_monthly.index, format='%b/%y')
    status_monthly = status_monthly.sort_values('sort_date')
    status_monthly = status_monthly.drop('sort_date', axis=1)
    
    # Convert booleans to integers
    status_monthly = status_monthly.astype(int)
    
    return {
        'months': status_monthly.index.tolist(),
        'data': {
            'Complete': status_monthly['is_complete'].tolist(),
            'Pending': status_monthly['is_pending'].tolist(),
            'Cancelled': status_monthly['is_cancelled'].tolist(),
            'Deferred': status_monthly['is_deferred'].tolist()
        }
    }

def calc_time_to_contact(filtered):
    """Calculate time to first contact"""
    if filtered.empty:
        return {'months': [], 'bins': [], 'data': {}}
    
    temp = filtered.copy()
    
    # Calculate days difference
    temp['days'] = (temp['1st Attempt to reach Patient/Referring MD'] - 
                    temp['Date Referral Received']).dt.days
    
    # Fill NaN with 999 (no contact)
    temp['days'] = temp['days'].fillna(999)
    
    # Bin the days
    temp['bin'] = pd.cut(
        temp['days'],
        bins=[-1, 3, 7, 14, 999],
        labels=['<= 3 days', '3 days - 1 week', '1 week - 2 weeks', '> 2 weeks']
    )
    
    # Group by month and bin
    result = temp.groupby(['month', 'bin']).size().unstack(fill_value=0)
    
    # Sort by date
    if not result.empty:
        result['sort_date'] = pd.to_datetime(result.index, format='%b/%y')
        result = result.sort_values('sort_date')
        result = result.drop('sort_date', axis=1)
    
    return {
        'months': result.index.tolist() if not result.empty else [],
        'bins': result.columns.tolist() if not result.empty else [],
        'data': result.to_dict('index') if not result.empty else {}
    }

def calc_time_to_complete(filtered):
    """Calculate time to complete information"""
    if filtered.empty:
        return {'months': [], 'bins': [], 'data': {}}
    
    temp = filtered.copy()
    
    # Calculate days difference
    temp['days'] = (temp['Date Complete Information received'] - 
                    temp['Date Referral Received']).dt.days
    
    # Create bins (including None for missing dates)
    temp['bin'] = 'None'  # Default
    
    mask_has_date = temp['days'].notna()
    temp.loc[mask_has_date & (temp['days'] <= 3), 'bin'] = '<= 3 days'
    temp.loc[mask_has_date & (temp['days'] > 3) & (temp['days'] <= 7), 'bin'] = '3 days - 1 week'
    temp.loc[mask_has_date & (temp['days'] > 7) & (temp['days'] <= 14), 'bin'] = '1 week - 2 weeks'
    temp.loc[mask_has_date & (temp['days'] > 14) & (temp['days'] <= 28), 'bin'] = '2 weeks - 4 weeks'
    temp.loc[mask_has_date & (temp['days'] > 28) & (temp['days'] <= 56), 'bin'] = '4 weeks - 8 weeks'
    temp.loc[mask_has_date & (temp['days'] > 56), 'bin'] = '> 8 weeks'
    
    # Group by month and bin
    result = temp.groupby(['month', 'bin']).size().unstack(fill_value=0)
    
    # Ensure all bins are present
    all_bins = ['<= 3 days', '3 days - 1 week', '1 week - 2 weeks', '2 weeks - 4 weeks', 
                '4 weeks - 8 weeks', '> 8 weeks', 'None']
    for bin_name in all_bins:
        if bin_name not in result.columns:
            result[bin_name] = 0
    
    # Reorder columns
    result = result[all_bins]
    
    # Sort by date
    if not result.empty:
        result['sort_date'] = pd.to_datetime(result.index, format='%b/%y')
        result = result.sort_values('sort_date')
        result = result.drop('sort_date', axis=1)
    
    return {
        'months': result.index.tolist() if not result.empty else [],
        'bins': result.columns.tolist() if not result.empty else [],
        'data': result.to_dict('index') if not result.empty else {}
    }

def calc_kpis(filtered):
    """Calculate KPI numbers"""
    if filtered.empty:
        return {'total': 0, 'new': 0, 'returning': 0, 'complete': 0, 'pending': 0, 
                'cancelled': 0, 'deferred': 0}
    
    return {
        'total': int(len(filtered)),
        'new': int(filtered['is_new'].sum()),
        'returning': int((~filtered['is_new']).sum()),
        'complete': int(filtered['is_complete'].sum()),
        'pending': int(filtered['is_pending'].sum()),
        'cancelled': int(filtered['is_cancelled'].sum()),
        'deferred': int(filtered['is_deferred'].sum())
    }

@eel.expose
def get_all_chart_data(filters):
    """Get all chart data in one call"""
    try:
        filtered = apply_filters(filters)
        
        return {
            'status': 'ok',
            'monthlyTrends': calc_monthly_trends(filtered),
            'serviceTrends': calc_service_trends(filtered),
            'physicianTrends': calc_physician_trends(filtered),
            'completionStatus': calc_completion_status(filtered),
            'timeToContact': calc_time_to_contact(filtered),
            'timeToComplete': calc_time_to_complete(filtered),
            'kpis': calc_kpis(filtered)
        }
    except Exception as e:
        print(f"Error calculating chart data: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}

def on_close(page, sockets):
    """Handle window close"""
    global _shutting_down
    if _shutting_down:
        return
    _shutting_down = True
    cleanup_lock_file()
    try:
        import gevent
        gevent.killall()
    except:
        pass
    os._exit(0)  # Force exit

def shutdown():
    """Cleanup on shutdown"""
    global _shutting_down
    if _shutting_down:
        return
    _shutting_down = True
    cleanup_lock_file()

if __name__ == '__main__':
    # Create lock file
    create_lock_file()
    
    # Register cleanup
    import atexit
    atexit.register(shutdown)
    
    # Initialize Eel
    eel.init('web')
    
    # Get screen dimensions for maximized window
    try:
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
        cleanup_lock_file()
        sys.exit(1)
