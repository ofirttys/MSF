# -*- mode: python ; coding: utf-8 -*-

"""
PyInstaller spec file for Michaeli Clinic Dashboard
Builds a standalone Windows executable with all dependencies
"""

import sys
from pathlib import Path

block_cipher = None

# ============================================================================
# DATA FILES AND FOLDERS
# ============================================================================

# Web assets (HTML, CSS, JS)
web_files = [
    ('web', 'web'),
]

# Database folder structure (empty, will be created at runtime)
# User will need to copy their database after installation
db_files = []

# Email templates
email_templates = [
    ('DB/email-templates.json', 'DB'),
]

# Icon file
icon_file = 'dashboard.ico'

# All data files
datas = web_files + email_templates

# ============================================================================
# HIDDEN IMPORTS
# ============================================================================

hiddenimports = [
    # Eel framework
    'eel',
    'bottle',
    'bottle_websocket',
    'gevent',
    'gevent.monkey',
    'geventwebsocket',
    'geventwebsocket.handler',
    'geventwebsocket.websocket',
    
    # Database
    'sqlite3',
    
    # Standard library modules used dynamically
    'pathlib',
    'json',
    'datetime',
    'hashlib',
    'os',
    'sys',
    'shutil',
    
    # Application modules
    'database',
    'patient_manager',
    'appointment_manager',
    'action_items_manager',
    'clinic_days_manager',
]

# ============================================================================
# ANALYSIS
# ============================================================================

a = Analysis(
    ['app.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Exclude unnecessary packages to reduce size
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'PIL',
        'tkinter',
        'PyQt5',
        'PyQt6',
        'PySide2',
        'PySide6',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# ============================================================================
# PYZ (Python ZIP Archive)
# ============================================================================

pyz = PYZ(
    a.pure,
    a.zipped_data,
    cipher=block_cipher
)

# ============================================================================
# EXE (Executable)
# ============================================================================

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='MichaeliClinic',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,  # No console window in production (use --debug flag if needed)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=icon_file,
)

# ============================================================================
# COLLECT (Bundle all files)
# ============================================================================

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='MichaeliClinic',
)

# ============================================================================
# DISTRIBUTION NOTES
# ============================================================================

"""
After building, the distribution will be in: dist/MichaeliClinic/

The folder structure will be:
dist/MichaeliClinic/
├── MichaeliClinic.exe          # Main executable
├── web/                         # Web assets (HTML, CSS, JS)
│   ├── index.html
│   ├── css/
│   │   └── style.css
│   └── js/
│       └── main.js
├── DB/                          # Database folder
│   └── email-templates.json    # Email templates
└── [various DLLs and dependencies]

DEPLOYMENT STEPS:
1. Build: pyinstaller MichaeliClinic.spec
2. Create DB folder structure in dist/MichaeliClinic/DB/
3. Copy michaeli-clinic.db to dist/MichaeliClinic/DB/
4. Create dist/MichaeliClinic/DB/backups/ folder
5. Test the executable
6. Zip the entire MichaeliClinic folder for distribution

FIRST RUN SETUP:
- User needs to have their database file in DB/michaeli-clinic.db
- Backups folder will be created automatically: DB/backups/
- Email templates are included in the build
"""
