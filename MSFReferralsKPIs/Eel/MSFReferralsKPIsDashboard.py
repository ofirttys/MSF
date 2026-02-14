#!/usr/bin/env python3
"""
MSF Referrals KPIs Dashboard - Eel Version (Clean & Stable)
"""

import eel
import os
import sys
import random
import atexit
import threading
import tempfile
import ctypes
import time
from pathlib import Path

# Process check import
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# Get DB folder path
if getattr(sys, 'frozen', False):
    exe_dir = Path(sys.executable).parent
else:
    exe_dir = Path(__file__).parent

DB_FOLDER = str(exe_dir / 'DB')

# Lock file per user in temp folder
try:
    username = os.getlogin()
except:
    username = os.environ.get('USERNAME', 'user')

LOCK_FILE = Path(tempfile.gettempdir()) / f'.msf_dashboard_{username}.lock'
LOCK_TIMEOUT = 300  # 5 minutes

# Global flag to prevent double-shutdown (must be at module level)
_shutting_down = False


def is_lock_file_stale():
    """Check if lock file is older than LOCK_TIMEOUT"""
    if not LOCK_FILE.exists():
        return False
    try:
        file_age = time.time() - LOCK_FILE.stat().st_mtime
        return file_age > LOCK_TIMEOUT
    except:
        return True


def check_already_running():
    """Check if another instance is already running"""
    if LOCK_FILE.exists():
        if is_lock_file_stale():
            try:
                LOCK_FILE.unlink()
            except:
                pass
        else:
            try:
                with open(LOCK_FILE, 'r') as f:
                    old_pid = int(f.read().strip())
                
                if HAS_PSUTIL:
                    if psutil.pid_exists(old_pid):
                        print("MSF Dashboard is already starting. Please wait...")
                        input("Press Enter to exit.")
                        sys.exit(0)
                    else:
                        LOCK_FILE.unlink()
            except:
                try:
                    LOCK_FILE.unlink()
                except:
                    pass
    
    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
    except:
        pass


def cleanup_lock_file():
    """Remove lock file on exit"""
    if LOCK_FILE.exists():
        try:
            LOCK_FILE.unlink()
        except:
            pass


@eel.expose
def get_db_folder():
    return DB_FOLDER


@eel.expose
def get_csv_files():
    try:
        if not os.path.exists(DB_FOLDER):
            return {'error': f'DB folder not found. Expected at: {DB_FOLDER}'}
        files = [f for f in os.listdir(DB_FOLDER) if f.lower().endswith('.csv')]
        if not files:
            return {'error': f'No CSV files in: {DB_FOLDER}'}
        files.sort(reverse=True)
        return {'files': files}
    except Exception as e:
        return {'error': f'Folder error: {str(e)}'}


@eel.expose
def read_csv_file(filename):
    try:
        filepath = os.path.join(DB_FOLDER, filename)
        if not os.path.exists(filepath):
            return None
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        return content
    except Exception as e:
        print(f"Error reading CSV file {filename}: {e}")
        return None


@eel.expose
def check_folder_exists():
    return os.path.exists(DB_FOLDER)


def main():
    """Main entry point - clean and simple"""
    global _shutting_down
    
    # Reset flag at start
    _shutting_down = False
    
    check_already_running()
    
    try:
        # Initialize Eel
        eel.init('web')
        port = random.randint(8000, 8999)
        
        # Get screen dimensions
        try:
            user32 = ctypes.windll.user32
            screen_width = user32.GetSystemMetrics(0)
            screen_height = user32.GetSystemMetrics(1)
        except:
            screen_width = 1920
            screen_height = 1080
        
        # Shutdown handler - CRITICAL FIX: prevent double execution
        def shutdown():
            global _shutting_down
            if _shutting_down:
                return  # Already shutting down, don't run again
            _shutting_down = True
            
            cleanup_lock_file()
            
            try:
                import gevent
                gevent.killall()
            except:
                pass
        
        # Register shutdown for abnormal exits only
        atexit.register(shutdown)
        
        # Close callback - DO NOT call shutdown() here to avoid double-run
        def on_close(page, sockets):
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
            sys.exit(0)
        
        # Remove lock file after window opens
        def remove_lock_delayed():
            time.sleep(5)
            cleanup_lock_file()
        
        threading.Thread(target=remove_lock_delayed, daemon=True).start()
        
        # Start Eel
        print("Starting MSF Dashboard...")
        print("Please wait for the browser window to open...")
        
        try:
            eel.start('index.html', 
                      size=(screen_width, screen_height),
                      position=(0, 0),
                      mode='edge',
                      port=port,
                      close_callback=on_close,
                      block=True)
        except OSError as e:
            if "10048" in str(e):
                port = random.randint(9000, 9999)
                try:
                    eel.start('index.html', 
                              size=(screen_width, screen_height),
                              position=(0, 0),
                              mode='edge',
                              port=port,
                              close_callback=on_close,
                              block=True)
                except:
                    try:
                        eel.start('index.html', 
                                  size=(screen_width, screen_height),
                                  position=(0, 0),
                                  mode='chrome',
                                  port=port,
                                  close_callback=on_close,
                                  block=True)
                    except Exception as ex:
                        print(f"Could not start: {ex}")
                        shutdown()
            else:
                print(f"Error: {e}")
                shutdown()
        except Exception as e:
            print(f"Error starting browser: {e}")
            try:
                eel.start('index.html', 
                          size=(screen_width, screen_height),
                          position=(0, 0),
                          mode='chrome',
                          port=port,
                          close_callback=on_close,
                          block=True)
            except Exception as ex:
                print(f"Could not start: {ex}")
                shutdown()
        
    except Exception as e:
        print(f"Fatal error: {e}")
        cleanup_lock_file()
        sys.exit(1)


if __name__ == '__main__':
    main()
