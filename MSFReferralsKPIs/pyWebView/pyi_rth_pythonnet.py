"""
PyInstaller runtime hook for pythonnet
Forces pythonnet to find its DLLs in the bundled location
"""
import os
import sys

# When running as a PyInstaller bundle
if hasattr(sys, '_MEIPASS'):
    # Set environment variable to help pythonnet find its runtime DLLs
    pythonnet_runtime = os.path.join(sys._MEIPASS, 'pythonnet', 'runtime')
    
    if os.path.exists(pythonnet_runtime):
        # Add to PATH so Windows can find the DLLs
        os.environ['PATH'] = pythonnet_runtime + os.pathsep + os.environ.get('PATH', '')
        
        # Try to set pythonnet-specific env var
        os.environ['PYTHONNET_RUNTIME'] = pythonnet_runtime
        
        print(f"[Runtime Hook] Set pythonnet runtime path to: {pythonnet_runtime}")
