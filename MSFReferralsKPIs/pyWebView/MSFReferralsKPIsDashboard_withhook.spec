# -*- mode: python ; coding: utf-8 -*-

# Import pythonnet to bundle its DLLs
import os
import pythonnet
pythonnet_dir = os.path.dirname(pythonnet.__file__)

block_cipher = None


a = Analysis(
    ['MSFReferralsKPIsDashboard.py'],
    pathex=[],
    binaries=[],
    datas=[
        (pythonnet_dir, 'pythonnet'),
    ],
    hiddenimports=['clr', 'System', 'System.Windows.Forms', 'pythonnet', 'clr_loader'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['pyi_rth_pythonnet.py'],  # ADD THIS LINE - custom runtime hook
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='MSFReferralsKPIsDashboard',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # Change to True temporarily to see error messages
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='MSFReferralsKPIsDashboard.ico',
)
