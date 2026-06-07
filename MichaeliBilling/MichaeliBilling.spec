# -*- mode: python ; coding: utf-8 -*-

import os
import shutil

block_cipher = None

a = Analysis(
    ['app.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('web', 'web'),
        ('Billing.ico', '.'),       # Icon in root for Windows API
        ('Billing.ico', 'web'),     # Icon in web folder for HTML favicon
    ],
    hiddenimports=[
        'bottle_websocket',
        # PIL needed for reportlab image handling
        'PIL.Image',
        'PIL.PngImagePlugin',
        'PIL.JpegImagePlugin',
        # Billing rules engine
        'billing_rules',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter',
        'PyQt5',
        'PyQt6',
        'PySide2',
        'PySide6',
        'wx',
        'gi',
        'scipy',
        'sklearn',
        'matplotlib',
        'cv2',
        'tensorflow',
        'torch',
        'pip',
        'pydoc',
        'doctest',
        'ftplib',
        'imaplib',
        'poplib',
        'smtplib',
        'telnetlib',
        'turtle',
        'curses',
        'antigravity',
        'this',
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
    name='MichaeliBilling',
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
    icon='Billing.ico',
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='MichaeliBilling',
)

# ============================================================================
# POST-BUILD: CREATE RUNTIME FOLDERS NEXT TO EXE
# ============================================================================
print("\n" + "="*70)
print("POST-BUILD: Setting up folder structure...")
print("="*70)

dist_folder = os.path.join('dist', 'MichaeliBilling')

for folder in ['db', 'exports', 'logs']:
    path = os.path.join(dist_folder, folder)
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"✓ Created: {path}")
    else:
        print(f"✓ Already exists: {path}")

print("\n" + "="*70)
print("Build complete!")
print("="*70)
print(f"\nExecutable: {os.path.join(dist_folder, 'MichaeliBilling.exe')}")
print("="*70 + "\n")
