# MichaeliBilling.spec
# PyInstaller spec file for building MichaeliBilling.exe
#
# Prerequisites:
#   pip install pyinstaller
#
# Build command (run from the MichaeliBilling folder):
#   pyinstaller MichaeliBilling.spec
#
# Output:
#   dist/MichaeliBilling/MichaeliBilling.exe   (folder-based distribution)
#
# To run on another machine, copy the entire dist/MichaeliBilling/ folder.
# The exe expects Microsoft Edge to be installed (falls back to Chrome).

import os
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# ── Collect all data files needed at runtime ──────────────────────────────────

added_files = [
    # Web frontend (HTML / CSS / JS)
    ('web',             'web'),

    # Eel's own web assets (bottle, etc.)
    *collect_data_files('eel'),

    # reportlab fonts and data
    *collect_data_files('reportlab'),
]

# ── Hidden imports ────────────────────────────────────────────────────────────
# Modules that PyInstaller's static analysis misses

hidden_imports = [
    # Eel
    'eel',
    'bottle',
    'whbottle',

    # Pandas Excel engines
    'xlrd',
    'openpyxl',
    'openpyxl.styles',
    'openpyxl.utils',

    # python-docx internals
    'docx',
    'docx.oxml',
    'docx.oxml.ns',
    'docx.shared',
    'docx.enum.text',

    # reportlab
    'reportlab',
    'reportlab.lib',
    'reportlab.lib.pagesizes',
    'reportlab.lib.styles',
    'reportlab.lib.units',
    'reportlab.lib.enums',
    'reportlab.lib.colors',
    'reportlab.platypus',
    'reportlab.platypus.tables',
    'reportlab.pdfgen',

    # Billing rules (same folder as app.py)
    'billing_rules',
]

# ── Analysis ──────────────────────────────────────────────────────────────────

a = Analysis(
    ['app.py'],
    pathex=['.'],
    binaries=[],
    datas=added_files,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Trim unused heavy packages
        'tkinter',
        'matplotlib',
        'scipy',
        'PIL',
        'PyQt5',
        'wx',
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
    console=False,          # no console window
    icon='Billing.ico',     # taskbar / exe icon — place Billing.ico in project root
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
