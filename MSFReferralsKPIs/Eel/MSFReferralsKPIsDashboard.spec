# -*- mode: python ; coding: utf-8 -*-

import os
import re
import shutil

block_cipher = None


a = Analysis(
    ['MSFReferralsKPIsDashboard.py'],
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

        # Scientific/ML libraries - not needed
        'scipy',
        'sklearn',
        'matplotlib',
        'PIL',
        'cv2',
        'tensorflow',
        'torch',

        # Unused stdlib modules
        'unittest',
        'pydoc',
        'doctest',
        'difflib',
        'ftplib',
        'imaplib',
        'poplib',
        'smtplib',
        'telnetlib',
        'xmlrpc',
        'xml',
        'html',
        'http.server',
        'turtle',
        'curses',
        'readline',
        'rlcompleter',
        'antigravity',
        'this',

        # Test modules
        'test',
        'tests',
        '_testcapi',
        '_testinternalcapi',
        '_testmultiphase',
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
    name='MSFReferralsKPIsDashboard',
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
    icon='MSFReferralsKPIsDashboard.ico',
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='MSFReferralsKPIsDashboard',
)


# ── Post-build cleanup ────────────────────────────────────────────────────────

internal_dir = os.path.join(
    DISTPATH, 'MSFReferralsKPIsDashboard', '_internal'
)

if os.path.exists(internal_dir):
    total_removed = 0
    total_saved   = 0

    # 1. Remove .dist-info and .data metadata folders
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
            print(f'  Removed metadata:  {item}  ({size // 1024} KB)')

    # 2. Remove unnecessary encoding files
    KEEP_ENCODINGS = {
        '__init__.py', 'aliases.py',
        'ascii.py', 'latin_1.py',
        'utf_8.py', 'utf_8_sig.py',
        'utf_16.py', 'utf_16_be.py', 'utf_16_le.py',
        'utf_32.py', 'utf_32_be.py', 'utf_32_le.py',
        'cp1252.py',             # Windows Western Europe default
        'cp437.py',              # Windows console
        'idna.py',               # Required for URLs / SSL
        'raw_unicode_escape.py', # Used internally by Python
        'unicode_escape.py',     # Used internally by Python
        'charmap.py',            # Base class for many codecs
        'mbcs.py',               # Windows ANSI code page
    }

    encodings_dir = os.path.join(internal_dir, 'encodings')
    if os.path.exists(encodings_dir):
        for fname in os.listdir(encodings_dir):
            if fname not in KEEP_ENCODINGS:
                fpath = os.path.join(encodings_dir, fname)
                if os.path.isfile(fpath):
                    size = os.path.getsize(fpath)
                    os.remove(fpath)
                    total_removed += 1
                    total_saved   += size
                    print(f'  Removed encoding:  {fname}  ({size} B)')

    # 3. Remove debug symbols and other safe-to-delete file types
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
                    print(f'  Removed file:      {fname}  ({size} B)')
                    break

    print(f'\nCleanup complete: {total_removed} items removed, '
          f'{total_saved // 1024 // 1024} MB saved')
