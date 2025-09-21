# -*- mode: python ; coding: utf-8 -*-
import os
block_cipher = None

# Hardcoded absolute path to tkdnd folder - adjust if your Python path differs
tkdnd_folder = r'C:\Users\***\AppData\Local\Programs\Python\Python313\Lib\site-packages\tkinterdnd2\tkdnd'

datas = [
    ('pak_manager_settings.json', '.'),  # Include your settings JSON
    (tkdnd_folder, 'tkinterdnd2/tkdnd'),  # Include full tkdnd folder for tkinterdnd2
]

a = Analysis(
    ['modmanager.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=[
        'tkinterdnd2',
        'tkinterdnd2.tkinterdnd2',
        'tkinterdnd2.dnd',
        'tkinterdnd2.constants',
        'tkinterdnd2._dnd',
        'tkinterdnd2._util',
        'tkinterdnd2._window',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='modmanager',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # GUI app - no console window
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='modmanager'
)
