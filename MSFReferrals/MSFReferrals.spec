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
    ],
    hiddenimports=[
        'bottle_websocket',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # GUI toolkits - definitely not needed
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
    icon='MSFReferrals.ico',
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


# ── Post-build cleanup ────────────────────────────────────────────────────────

internal_dir = os.path.join(
    DISTPATH, 'MSFReferrals', '_internal'
)

if os.path.exists(internal_dir):
    total_removed = 0
    total_saved   = 0

    # 1. Remove .dist-info and .data metadata folders
    print('\n── Removing metadata folders ──')
    for item in os.listdir(internal_dir):
        item_path = os.path.join(internal_dir, item)
        if re.search(r'\.(dist-info|data)$', item) and os.path.isdir(item_path):
            size = sum(
                os.path.getsize(os.path.join(r, f))
                for r, _, files in os.walk(item_path)
                for f in files
            )
            shutil.rmtree(item_path)
            total_removed += 1
            total_saved   += size
            print(f'  Removed: {item}  ({size // 1024} KB)')

    # 2. Remove debug symbols and test files
    print('\n── Removing debug/test files ──')
    REMOVE_PATTERNS = [
        r'\.pdb$',         # Windows debug symbols
        r'\.chm$',         # Help files
        r'test_.*\.pyc$',  # Test bytecode
        r'.*_test\.pyc$',  # Test bytecode
    ]

    for root, dirs, files in os.walk(internal_dir):
        dirs[:] = [d for d in dirs if d != 'web']  # Don't touch web folder
        for fname in files:
            for pattern in REMOVE_PATTERNS:
                if re.search(pattern, fname, re.IGNORECASE):
                    fpath = os.path.join(root, fname)
                    size  = os.path.getsize(fpath)
                    os.remove(fpath)
                    total_removed += 1
                    total_saved   += size
                    print(f'  Removed: {fname}  ({size} B)')
                    break

    print(f'\n── Cleanup complete ──')
    print(f'  Items removed : {total_removed}')
    print(f'  Space saved   : {total_saved // 1024} KB  ({total_saved // 1024 // 1024} MB)')
