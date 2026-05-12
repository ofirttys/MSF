# -*- mode: python ; coding: utf-8 -*-

import os
import re
import shutil

block_cipher = None


a = Analysis(
    ['MSFReferrals.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('web', 'web'),
        ('msf_logo.png', '.'),
        ('MSFReferrals.ico', '.'),  # Icon in root for Windows API
        ('MSFReferrals.ico', 'web'),  # Icon in web folder for HTML favicon
        # DB folder will be copied manually in post-build to root level
    ],
    hiddenimports=[
        'bottle_websocket',
        # PIL REMOVED - not needed, reportlab doesn't require it for PDFs
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # GUI toolkits - not needed
        'tkinter',
        '_tkinter',
        'tcl',
        'tk',
        'PyQt5',
        'PyQt6',
        'PySide2',
        'PySide6',
        'wx',
        'gi',

        # Scientific/ML libraries - definitely not needed
        'numpy',
        'pandas',
        'scipy',
        'sklearn',
        'matplotlib',
        'PIL',
        'Pillow',
        'cv2',
        'tensorflow',
        'torch',
        
        # Package management - not needed at runtime
        'pip',
        
        # Development/testing tools - not needed at runtime
        'pydoc',
        'doctest',
        
        # Network protocols we don't use
        'ftplib',
        'imaplib',
        'poplib',
        'smtplib',
        'telnetlib',
        
        # Other unused stdlib
        'turtle',
        'curses',
        'antigravity',
        'this',
        
        # Text processing we don't need
        'pyphen',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='MSFReferrals',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='MSFReferrals.ico',  # ← Icon for .exe file itself
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='MSFReferrals',
)

# ============================================================================
# POST-BUILD: COPY DB FOLDER STRUCTURE
# ============================================================================
# After PyInstaller finishes, we manually copy the DB folder structure
# to the dist/MSFReferrals/ directory (same level as the .exe)
# This keeps the database separate from the _internal folder

print("\n" + "="*70)
print("POST-BUILD: Setting up DB folder structure...")
print("="*70)

dist_folder = os.path.join('dist', 'MSFReferrals')
db_dest = os.path.join(dist_folder, 'DB')

# Create DB folder structure if it doesn't exist
if not os.path.exists(db_dest):
    os.makedirs(db_dest)
    print(f"✓ Created: {db_dest}")
else:
    print(f"✓ DB folder already exists: {db_dest}")

# Create backups subfolder
backups_dest = os.path.join(db_dest, 'backups')
if not os.path.exists(backups_dest):
    os.makedirs(backups_dest)
    print(f"✓ Created: {backups_dest}")
else:
    print(f"✓ Backups folder already exists: {backups_dest}")

# Create Referrals folder structure
referrals_dest = os.path.join(dist_folder, 'Referrals')
linked_dest = os.path.join(referrals_dest, 'Linked')
pending_dest = os.path.join(referrals_dest, 'Pending')

for folder in [referrals_dest, linked_dest, pending_dest]:
    if not os.path.exists(folder):
        os.makedirs(folder)
        print(f"✓ Created: {folder}")
    else:
        print(f"✓ Folder already exists: {folder}")

# Copy templates.json if it exists
templates_src = os.path.join('DB', 'templates.json')
templates_dest = os.path.join(db_dest, 'templates.json')
if os.path.exists(templates_src):
    shutil.copy2(templates_src, templates_dest)
    print(f"✓ Copied: templates.json")
else:
    print(f"⚠ Warning: templates.json not found in DB folder")

print("\n" + "="*70)
print("Build complete!")
print("="*70)
print(f"\nExecutable location: {os.path.join(dist_folder, 'MSFReferrals.exe')}")
print(f"DB folder location: {db_dest}")
print(f"Referrals folder: {referrals_dest}")
print("\nREMINDER: Copy your actual database file (referrals.db) to the DB folder")
print("="*70 + "\n")
