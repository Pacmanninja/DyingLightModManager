# file: tool.py
"""
Dying Light: The Beast — Mod Manager + Merger (GUI)

Adds support for installing/merging mods packed as .zip/.rar/.7z (archives
that contain .pak inside), fixes startup/closing issues, and includes a
dark mode + simple settings persistence.

Usage:
  pip install rarfile py7zr
  # For .rar support also install one of: unrar / unar / bsdtar (on PATH)
  # Optional: install 7-Zip for CLI fallback (7z.exe on PATH)

  python tool.py
"""

from __future__ import annotations

import json
import os
import queue
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Iterator

import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog

# Optional archive libs (.rar/.7z)
try:
    import rarfile  # type: ignore
except Exception:
    rarfile = None  # type: ignore

try:
    import py7zr  # type: ignore
except Exception:
    py7zr = None  # type: ignore

# PyInstaller bundle TKDND init
if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
    _tcldir = Path(sys._MEIPASS) / "tcl"
    if _tcldir.exists():
        os.environ["TCL_LIBRARY"] = str(_tcldir)

# Optional tkinterdnd2 (drag & drop)
try:
    from tkinterdnd2 import TkinterDnD, DND_FILES  # type: ignore
    TKDND_OK = True
except Exception:
    TkinterDnD = None  # type: ignore
    DND_FILES = None  # type: ignore
    TKDND_OK = False

# ---- App constants
APP_NAME = "Dying Light The Beast Mod Manager and Merger"
SETTINGS_FILENAME = "settings.json"
DEFAULT_GAME_DIR = r"D:\Program Files (x86)\Steam\steamapps\common\Dying Light The Beast\ph_ft\source"

# ---- Small utils

def get_user_data_dir() -> Path:
    base = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~")
    path = Path(base) / "DLTBeast_Mod_Manager"
    path.mkdir(parents=True, exist_ok=True)
    return path

def get_settings_path() -> Path:
    return get_user_data_dir() / SETTINGS_FILENAME

def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def _ensure_writable(path: Path) -> None:
    try:
        if path.exists():
            os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
    except Exception:
        pass

def load_settings() -> Dict:
    path = get_settings_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}
    data.setdefault("game_folder", "")
    data.setdefault("files", {})
    data.setdefault("merge_queue", [])
    data.setdefault("dark_mode", False)
    return data

def save_settings(settings: Dict) -> Path:
    path = get_settings_path()
    _ensure_parent_dir(path)
    _ensure_writable(path)
    payload = json.dumps(settings, ensure_ascii=False, indent=2, sort_keys=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(payload)
    os.replace(tmp, path)
    return path

def next_data_name(folder: str) -> str:
    i = 0
    while True:
        name = f"data{i}.pak"
        if not os.path.exists(os.path.join(folder, name)):
            return name
        i += 1

def parse_drop_files(event_data: str) -> List[str]:
    r"""Parse the native DND_FILES string into a list of absolute paths.

    On Windows: {C:\path one\file.pak} {C:\path two\file.zip}
    On macOS/Linux it may be a space-separated list; we handle braces and quotes.
    """
    out: List[str] = []
    token: List[str] = []
    in_brace = False
    for ch in event_data:
        if ch == "{":
            in_brace = True
            if token:
                out.append("".join(token).strip())
                token = []
            continue
        if ch == "}":
            in_brace = False
            out.append("".join(token))
            token = []
            continue
        if ch == " " and not in_brace:
            if token:
                out.append("".join(token))
                token = []
            continue
        token.append(ch)
    if token:
        out.append("".join(token))
    return out

# ---- PAK reading & merging

@dataclass
class ScriptFile:
    rel_path: str
    content: str
    source: str

@dataclass
class GameStructure:
    known: set

def get_game_file_structure(game_pak_path: Path) -> GameStructure:
    known: set = set()
    with zipfile.ZipFile(game_pak_path, "r") as z:
        for n in z.namelist():
            if n.lower().endswith(".scr"):
                known.add(n.replace("\\", "/"))
    return GameStructure(known=known)

def read_scripts_from_single_pak(pak_path_or_stream, source_name: str) -> List[ScriptFile]:
    if isinstance(pak_path_or_stream, (str, os.PathLike, Path)):
        z = zipfile.ZipFile(pak_path_or_stream, "r")
        close = True
    else:
        z = zipfile.ZipFile(pak_path_or_stream, "r")
        close = True
    scripts: List[ScriptFile] = []
    try:
        for entry in z.namelist():
            if entry.lower().endswith(".scr"):
                with z.open(entry) as f:
                    try:
                        content = f.read().decode("utf-8")
                    except UnicodeDecodeError:
                        content = f.read().decode("utf-8", errors="replace")
                scripts.append(ScriptFile(rel_path=entry.replace("\\", "/"), content=content, source=source_name))
    finally:
        if close:
            z.close()
    return scripts

def read_scripts_from_single_pak_for_fixing(
    pak_path_or_stream, needs_fixing: List[bool], structure: GameStructure, unknown_files_out: List[str]
) -> List[Tuple[str, str, "tempfile.SpooledTemporaryFile"]]:
    z = zipfile.ZipFile(pak_path_or_stream, "r")
    scripts: List[Tuple[str, str, "tempfile.SpooledTemporaryFile"]] = []
    try:
        for entry in z.namelist():
            if entry.endswith("/"):
                continue
            with z.open(entry) as f:
                data = f.read()
            if entry.lower().endswith(".scr"):
                norm = entry.replace("\\", "/")
                if norm in structure.known:
                    corrected = norm
                else:
                    base = os.path.basename(norm)
                    matches = [p for p in structure.known if p.endswith("/" + base)]
                    if matches:
                        corrected = matches[0]
                        needs_fixing[0] = True
                    else:
                        unknown_files_out.append(norm)
                        corrected = norm
                s = tempfile.SpooledTemporaryFile(max_size=1024 * 1024)
                s.write(data)
                s.seek(0)
                scripts.append((norm, corrected, s))
    finally:
        z.close()
    return scripts

def load_all_scripts_from_all_pak_files(pak_file_paths: Iterable[Path]) -> List[ScriptFile]:
    all_scripts: List[ScriptFile] = []
    for path in pak_file_paths:
        all_scripts.extend(read_scripts_from_single_pak(path, os.path.basename(path)))
    return all_scripts

# --- Archive helpers (sniff + 7z fallback)

def _sniff_archive_type(path: Path) -> Optional[str]:
    try:
        with open(path, "rb") as f:
            sig = f.read(8)
        if sig.startswith(b"PK\x03\x04") or sig.startswith(b"PK\x05\x06") or sig.startswith(b"PK\x07\x08"):
            return "zip"
        if sig.startswith(b"7z\xBC\xAF'") or sig.startswith(b"7z\xBC\xAF\x27\x1C"):
            return "7z"
        if sig.startswith(b"Rar!\x1A\x07\x00") or sig.startswith(b"Rar!\x1A\x07\x01\x00"):
            return "rar"
    except Exception:
        return None
    return None

def _which_7z() -> Optional[str]:
    cand = shutil.which("7z")
    if cand:
        return cand
    if sys.platform.startswith("win"):
        for p in (r"C:\Program Files\7-Zip\7z.exe", r"C:\Program Files (x86)\7-Zip\7z.exe"):
            if os.path.exists(p):
                return p
    return None

def _yield_pak_streams_with_7z_cli(path: Path, log: Optional[Callable[[str], None]] = None) -> Iterator[Tuple[str, "BytesIO"]]:
    from io import BytesIO
    exe = _which_7z()
    if not exe:
        if log:
            log("7-Zip CLI not found on PATH; cannot fallback to 7z.")
        return
    try:
        proc = subprocess.run([exe, "l", "-slt", "-ba", str(path)], capture_output=True, text=True)
        if proc.returncode != 0:
            if log:
                log(f"7z list failed for '{path.name}': {proc.stderr.strip()}")
            return
        names: List[str] = []
        for line in proc.stdout.splitlines():
            if line.startswith("Path = "):
                name = line.split("Path = ", 1)[1].strip()
                if name.lower().endswith(".pak"):
                    names.append(name)
        if not names and log:
            log(f"No .pak inside '{path.name}' (via 7z).")
        for name in names:
            proc2 = subprocess.run([exe, "x", "-so", str(path), name], capture_output=True)
            if proc2.returncode != 0:
                if log:
                    log(f"7z extract failed for '{name}' in '{path.name}': {proc2.stderr.decode(errors='ignore').strip()}")
                continue
            yield name, BytesIO(proc2.stdout)
    except Exception as e:
        if log:
            log(f"7z fallback error for '{path.name}': {e}")

def _yield_pak_streams_from_archive(path: Path, log: Optional[Callable[[str], None]] = None) -> Iterator[Tuple[str, "BytesIO"]]:
    """Yield (inner_name, BytesIO) for each .pak found inside .zip/.rar/.7z (robust, with header sniff + 7z fallback)."""
    from io import BytesIO

    kind = _sniff_archive_type(path) or path.suffix.lower().lstrip(".")
    kind = (kind or "").lower()

    if kind == "zip":
        try:
            with zipfile.ZipFile(path, "r") as z:
                for entry in z.namelist():
                    if entry.endswith(".pak") and not entry.endswith("/"):
                        yield entry, BytesIO(z.read(entry))
        except Exception as e:
            if log:
                log(f"ERROR reading ZIP '{path.name}': {e}")

    elif kind == "rar":
        used = False
        if rarfile is not None:
            try:
                with rarfile.RarFile(path) as rf:  # type: ignore
                    for info in rf.infolist():
                        name = getattr(info, "filename", "")
                        if str(name).endswith(".pak"):
                            with rf.open(info) as f:
                                yield str(name), BytesIO(f.read())
                                used = True
            except Exception as e:
                if log:
                    log(f"ERROR reading RAR '{path.name}': {e}")
        if not used:
            for tup in _yield_pak_streams_with_7z_cli(path, log=log):
                yield tup

    elif kind in ("7z", "7zip"):
        used = False
        if py7zr is not None:
            try:
                with py7zr.SevenZipFile(path, "r") as sz:  # type: ignore
                    try:
                        names = [n for n in sz.getnames() if n.endswith(".pak")]
                    except Exception:
                        names = [i.filename for i in sz.list() if i.filename.endswith(".pak")]
                    if names:
                        data_map = sz.read(names)
                        for name, blob in data_map.items():
                            try:
                                data = blob.read()
                            except Exception:
                                data = bytes(blob)
                            yield name, BytesIO(data)
                            used = True
            except Exception as e:
                if log:
                    log(f"ERROR reading 7z '{path.name}': {e}")
        if not used:
            for tup in _yield_pak_streams_with_7z_cli(path, log=log):
                yield tup
    else:
        if log:
            log(f"Unsupported or unknown archive type: '{path.name}'")

def load_all_scripts_from_mods_folder(paths: Iterable[Path]) -> List[ScriptFile]:
    scripts: List[ScriptFile] = []
    for mod_file_path in paths:
        try:
            source_name = os.path.basename(mod_file_path)
            suf = (_sniff_archive_type(mod_file_path) or mod_file_path.suffix.lower().lstrip(".")).lower()
            if suf == "pak":
                scripts.extend(read_scripts_from_single_pak(mod_file_path, source_name))
            elif suf in ("zip", "rar", "7z", "7zip"):
                found = False
                for _, mem_stream in _yield_pak_streams_from_archive(mod_file_path):
                    found = True
                    scripts.extend(read_scripts_from_single_pak(mem_stream, source_name))
                if not found:
                    print(f"WARNING: No .pak found inside '{source_name}'. Skipping.")
            else:
                print(f"Unsupported mod archive type: {mod_file_path}")
        except Exception as e:
            print(f"ERROR: Could not read '{os.path.basename(mod_file_path)}'. Reason: {e}")
    return scripts

# ---- Merge logic (honors per-file preference)

def merge_scripts(
    original: ScriptFile,
    mods: List[ScriptFile],
    ask_user: Callable[[List[Tuple[str, str]], tk.Tk], Tuple[str, Optional[str]]],
    parent: tk.Tk,
) -> str:
    """Resolve conflicts by key; allow the user to pick and optionally apply preference for the rest of the file."""
    def try_parse_key_local(line: str) -> Optional[str]:
        line = line.strip()
        if not line or line.startswith("#"):
            return None
        parts = re.split(r"\s+", line, maxsplit=1)
        if len(parts) < 2:
            return None
        lhs, rhs = parts[0], parts[1]
        if lhs.isdigit():
            return None
        return f"{lhs}_{rhs.split()[0] if rhs else ''}"

    original_map: Dict[str, str] = {}
    for line in original.content.replace("\r\n", "\n").split("\n"):
        key = try_parse_key_local(line)
        if key and key not in original_map:
            original_map[key] = line

    preferred_source_for_file: Optional[str] = None  # set after first "apply" selection

    for mod in mods:
        for line in mod.content.replace("\r\n", "\n").split("\n"):
            key = try_parse_key_local(line)
            if not key:
                continue
            if key in original_map and original_map[key] != line:
                if preferred_source_for_file == original.source:
                    continue  # keep existing original
                if preferred_source_for_file == mod.source:
                    original_map[key] = line
                    continue
                choice, prefer_src = ask_user([(original_map[key], original.source), (line, mod.source)], parent)
                original_map[key] = choice
                if prefer_src:
                    preferred_source_for_file = prefer_src
            else:
                original_map[key] = line

    return "\n".join(original_map[k] for k in original_map.keys())

# ---- UI for conflict resolution (radio buttons + "apply to rest of file")

def ask_user_to_resolve_conflict(options: List[Tuple[str, str]], parent: tk.Tk) -> Tuple[str, Optional[str]]:
    """Return (chosen_line, preferred_source_or_None)."""
    dlg = tk.Toplevel(parent)
    dlg.title("Resolve Conflict")
    dlg.geometry("760x420")
    dlg.transient(parent)
    dlg.grab_set()

    tk.Label(dlg, text="A conflict was found. Choose which line to keep:").pack(anchor="w", padx=10, pady=8)

    frame = tk.Frame(dlg)
    frame.pack(fill="both", expand=True, padx=10)

    sel_var = tk.IntVar(value=0)
    prefer_var = tk.BooleanVar(value=False)

    # Option 0 (usually ORIGINAL)
    o0 = tk.Frame(frame)
    o0.pack(fill="x", pady=4)
    tk.Radiobutton(o0, text=f"Use from: {options[0][1]}", variable=sel_var, value=0).pack(anchor="w")
    t0 = tk.Text(o0, height=6, wrap="none")
    t0.pack(fill="x")
    t0.insert("1.0", options[0][0])
    t0.config(state="disabled")

    # Option 1 (MOD)
    o1 = tk.Frame(frame)
    o1.pack(fill="x", pady=6)
    tk.Radiobutton(o1, text=f"Use from: {options[1][1]}", variable=sel_var, value=1).pack(anchor="w")
    t1 = tk.Text(o1, height=6, wrap="none")
    t1.pack(fill="x")
    t1.insert("1.0", options[1][0])
    t1.config(state="disabled")

    tk.Checkbutton(dlg, text="Apply this choice to the rest of this file", variable=prefer_var).pack(
        anchor="w", padx=10, pady=4
    )

    btns = tk.Frame(dlg)
    btns.pack(fill="x", padx=10, pady=10)

    def on_ok():
        idx = sel_var.get()
        chosen_line = options[idx][0]
        chosen_source = options[idx][1] if prefer_var.get() else None
        dlg.__result__ = (chosen_line, chosen_source)
        dlg.destroy()

    tk.Button(btns, text="OK", command=on_ok).pack(side="right")
    tk.Button(btns, text="Cancel", command=lambda: dlg.destroy()).pack(side="right", padx=6)

    parent.wait_window(dlg)
    return getattr(dlg, "__result__", (options[0][0], None))

def generate_merged_file_content(
    original: ScriptFile,
    mods: List[ScriptFile],
    ui_sync: "UiSync",
    parent: tk.Tk,
) -> str:
    original_map: Dict[str, str] = {}
    for line in original.content.replace("\r\n", "\n").split("\n"):
        key = try_parse_key(line)
        if key and key not in original_map:
            original_map[key] = line

    preferred_source_for_file: Optional[str] = None

    def ask(opts: List[Tuple[str, str]]) -> Tuple[str, Optional[str]]:
        return ui_sync.call(ask_user_to_resolve_conflict, opts, parent)  # type: ignore

    for mod in mods:
        for line in mod.content.replace("\r\n", "\n").split("\n"):
            key = try_parse_key(line)
            if not key:
                continue
            if key in original_map and original_map[key] != line:
                if preferred_source_for_file == original.source:
                    continue
                if preferred_source_for_file == mod.source:
                    original_map[key] = line
                    continue
                choice, prefer = ask([(original_map[key], original.source), (line, mod.source)])
                original_map[key] = choice
                if prefer:
                    preferred_source_for_file = prefer
            else:
                original_map[key] = line

    return "\n".join(original_map.values())

# ---- Simple parsing / key extraction

_key_regex = re.compile(r"\s*([^\s]+)\s+([^\s]+)")

def try_parse_key(line: str) -> Optional[str]:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    m = _key_regex.match(line)
    if m:
        return f"{m.group(1)}_{m.group(2)}"
    return None

# ---- UI helpers for thread ↔ UI sync
class UiSync:
    """Run a callable in the Tk main thread and wait for its return value."""
    def __init__(self, root: tk.Tk):
        self.root = root
        self.q: "queue.Queue[Tuple[Callable, threading.Event, List, Dict]]" = queue.Queue()
        self.root.after(10, self._poll)

    def call(self, func: Callable, *args, **kwargs):
        ev = threading.Event()
        self.q.put((func, ev, list(args), dict(kwargs)))
        ev.wait()
        return getattr(self, "_last_result", None)

    def _poll(self):
        try:
            while True:
                func, ev, args, kwargs = self.q.get_nowait()
                try:
                    self._last_result = func(*args, **kwargs)
                except Exception as e:
                    print("UiSync error:", e)
                    self._last_result = None
                ev.set()
        except queue.Empty:
            pass
        self.root.after(10, self._poll)

# ---- Default directory validation helpers

def folder_has_data0(path: str | Path) -> bool:
    p = Path(path)
    return p.is_dir() and (p / "data0.pak").exists()

def validate_game_folder(path: str | Path) -> Tuple[bool, str]:
    p = Path(path)
    if not p.exists():
        return False, "Folder does not exist."
    if not p.is_dir():
        return False, "Path is not a directory."
    if not (p / "data0.pak").exists():
        return False, "data0.pak not found in this folder."
    return True, ""

# ---- Merge runner

def fix_mod_structures_ui(
    game_pak_path: Path, mod_paths: List[Path], temp_root: Path, ui_sync: UiSync, parent: tk.Tk, log: Callable[[str], None]
) -> List[Path]:
    structure = get_game_file_structure(game_pak_path)
    valid: List[Path] = []
    for mod_file in mod_paths:
        needs_fixing = [False]
        mod_name = Path(mod_file).stem
        temp_dir = Path(tempfile.mkdtemp(dir=temp_root))
        fixed_pak_path = temp_dir / "fixed.pak"
        unknown_files: List[str] = []
        try:
            kind = _sniff_archive_type(mod_file) or mod_file.suffix.lower().lstrip(".")
            kind = (kind or "").lower()
            found_any = False

            if kind == "pak":
                _ = read_scripts_from_single_pak_for_fixing(mod_file, needs_fixing, structure, unknown_files)
                found_any = True
            elif kind in ("zip", "rar", "7z", "7zip"):
                for _, mem_stream in _yield_pak_streams_from_archive(mod_file, log=log):
                    found_any = True
                    _ = read_scripts_from_single_pak_for_fixing(mem_stream, needs_fixing, structure, unknown_files)
            else:
                log(f"Unsupported mod archive type: {mod_file.name}")
                continue

            if not found_any:
                log(f"No .pak inside '{mod_name}'. Skipping.")
                continue

            if unknown_files:
                msg = (
                    f"Mod '{mod_name}' has files not found in data0.pak:\n"
                    + "\n".join(f" - {x}" for x in unknown_files)
                    + "\n\nUse original structure anyway?\nYes = keep, No = exclude this mod."
                )
                keep = ui_sync.call(messagebox.askyesno, "Unknown Files", msg, parent=parent)  # type: ignore
                if not keep:
                    log(f"Excluded '{mod_name}' due to unknown files.")
                    continue
                else:
                    log(f"Keeping '{mod_name}' with its original structure.")
                    valid.append(mod_file)
                    continue

            if needs_fixing[0]:
                with zipfile.ZipFile(fixed_pak_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    if kind == "pak":
                        scripts = read_scripts_from_single_pak_for_fixing(mod_file, needs_fixing, structure, [])
                        for _, correct_path, content_stream in scripts:
                            content_stream.seek(0)
                            zf.writestr(correct_path.replace("\\", "/"), content_stream.read())
                            content_stream.close()
                    else:
                        for _, mem_stream in _yield_pak_streams_from_archive(mod_file, log=log):
                            scripts = read_scripts_from_single_pak_for_fixing(mem_stream, needs_fixing, structure, [])
                            for _, correct_path, content_stream in scripts:
                                content_stream.seek(0)
                                zf.writestr(correct_path.replace("\\", "/"), content_stream.read())
                                content_stream.close()
                fixed_zip_path = temp_dir / f"{mod_name}_fixed.zip"
                with zipfile.ZipFile(fixed_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(fixed_pak_path, "mod.pak")
                log(f"Fixed '{mod_name}' → '{fixed_zip_path.name}'.")
                valid.append(fixed_zip_path)
            else:
                log(f"'{mod_name}' structure OK.")
                valid.append(mod_file)
        except Exception as e:
            log(f"ERROR processing '{mod_name}': {e}")
    return valid

def run_merge(
    game_folder: Path,
    mod_paths: List[Path],
    ui_sync: UiSync,
    parent: tk.Tk,
    log: Callable[[str], None],
) -> Optional[Path]:
    data0 = game_folder / "data0.pak"
    if not data0.exists():
        messagebox.showerror("Missing data0.pak", "data0.pak not found in the selected game source folder.", parent=parent)
        return None

    staging_root = Path(tempfile.mkdtemp(prefix="merge_staging_"))
    temp_root = Path(tempfile.mkdtemp(prefix="merge_temp_"))
    try:
        log("Fixing mod folder structures (if needed)...")
        valid_mods = fix_mod_structures_ui(data0, mod_paths, temp_root, ui_sync, parent, log)

        log("Loading original scripts and mods...")
        original_scripts = load_all_scripts_from_all_pak_files([data0])
        mod_scripts = load_all_scripts_from_mods_folder(valid_mods)

        by_rel: Dict[str, List[ScriptFile]] = defaultdict(list)
        originals: Dict[str, ScriptFile] = {}

        for s in original_scripts:
            originals[s.rel_path] = s
        for s in mod_scripts:
            by_rel[s.rel_path].append(s)

        merged_zip_path = staging_root / "merged.zip"
        merged_count = 0
        with zipfile.ZipFile(merged_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for rel_path, mods in by_rel.items():
                if rel_path in originals:
                    content = merge_scripts(
                        originals[rel_path],
                        mods,
                        lambda opts, root=parent: ui_sync.call(ask_user_to_resolve_conflict, opts, root),
                        parent,
                    )
                    zf.writestr(rel_path, content)
                    merged_count += 1
                else:
                    zf.writestr(rel_path, mods[-1].content)
                    merged_count += 1
        log(f"Merged {merged_count} files.")

        if merged_count == 0:
            messagebox.showinfo("Nothing to merge", "No mergeable scripts were found.\nNothing was installed.", parent=parent)
            return None

        name = next_data_name(str(game_folder))
        final_path = game_folder / name
        shutil.copy2(merged_zip_path, final_path)
        log(f"Installed merged pak as {final_path.name}")
        return final_path
    finally:
        try:
            shutil.rmtree(staging_root, ignore_errors=True)
        except Exception:
            pass
        try:
            shutil.rmtree(temp_root, ignore_errors=True)
        except Exception:
            pass

# ---- UI

class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        root.title(APP_NAME)
        root.geometry("1024x640")

        self.ui_sync = UiSync(root)
        self.settings = load_settings()

        self.palette = {
            "light": {
                "bg": "#f7f7fb",
                "bg2": "#ffffff",
                "fg": "#1f2937",
                "acc": "#2563eb",
                "entry_bg": "#ffffff",
                "entry_fg": "#111827",
                "entry_insert": "#111827",
                "label_bg": "#f7f7fb",
                "label_fg": "#1f2937",
                "listbox_bg": "#ffffff",
                "listbox_fg": "#111827",
                "listbox_sel_bg": "#c7d2fe",
                "listbox_sel_fg": "#111827",
                "text_bg": "#ffffff",
                "text_fg": "#111827",
                "text_insert": "#111827",
            },
            "dark": {
                "bg": "#0b1221",
                "bg2": "#111827",
                "fg": "#e5e7eb",
                "acc": "#60a5fa",
                "entry_bg": "#111827",
                "entry_fg": "#e5e7eb",
                "entry_insert": "#e5e7eb",
                "label_bg": "#0b1221",
                "label_fg": "#e5e7eb",
                "listbox_bg": "#111827",
                "listbox_fg": "#e5e7eb",
                "listbox_sel_bg": "#374151",
                "listbox_sel_fg": "#e5e7eb",
                "text_bg": "#111827",
                "text_fg": "#e5e7eb",
                "text_insert": "#e5e7eb",
            },
        }

        self._build_ui()
        self._apply_palette()
        self._restyle_existing_widgets()

        # Init game folder (saved/default/scan), then fill UI lists
        self._init_game_folder()
        self.refresh_file_list()
        self.refresh_merge_queue()

        # F5 refresh
        self.root.bind("<F5>", lambda e: self.refresh_file_list())

    # --- Default directory handling

    def _init_game_folder(self) -> None:
        """Choose saved folder, else default, else scan A:..Z:, else prompt."""
        # 1) Saved setting
        saved = str(self.settings.get("game_folder", "")).strip()
        if saved and folder_has_data0(saved):
            self.game_entry.delete(0, "end")
            self.game_entry.insert(0, saved)
            return

        # 2) Hard-coded default
        if folder_has_data0(DEFAULT_GAME_DIR):
            self.settings["game_folder"] = DEFAULT_GAME_DIR
            save_settings(self.settings)
            self.game_entry.delete(0, "end")
            self.game_entry.insert(0, DEFAULT_GAME_DIR)
            return

        # 3) Scan drives A–Z for the Steam install path you specified
        found_path: Optional[str] = None
        possible_drives = [chr(x) for x in range(ord('A'), ord('Z') + 1)]
        for drive in possible_drives:
            candidate = Path(f"{drive}:\\Program Files (x86)\\Steam\\steamapps\\common\\Dying Light The Beast\\ph_ft\\source")
            print(f"Trying: {candidate}")
            if candidate.exists() and (candidate / "data0.pak").exists():
                found_path = str(candidate)
                break
        if found_path:
            self.settings["game_folder"] = found_path
            save_settings(self.settings)
            self.game_entry.delete(0, "end")
            self.game_entry.insert(0, found_path)
            return

        # 4) Prompt user
        messagebox.showinfo(
            "Game folder not set",
            "Select the Dying Light (The Beast) source folder that contains 'data0.pak'.",
            parent=self.root,
        )
        self.on_browse_game_folder()

    def ensure_valid_game_folder(self, interactive: bool = True) -> bool:
        """Validate current folder; try default/scan; optionally prompt."""
        gf = self.game_entry.get().strip() or str(self.settings.get("game_folder", "")).strip()
        ok, why = validate_game_folder(gf) if gf else (False, "No folder set.")
        if ok:
            if gf != self.settings.get("game_folder", ""):
                self.settings["game_folder"] = gf
                save_settings(self.settings)
            return True

        if not interactive:
            return False

        # Try default
        if folder_has_data0(DEFAULT_GAME_DIR):
            use_default = messagebox.askyesno(
                "Use default directory?",
                f"Current folder is invalid ({why}).\n\nUse default?\n\n{DEFAULT_GAME_DIR}",
                parent=self.root,
            )
            if use_default:
                self.settings["game_folder"] = DEFAULT_GAME_DIR
                save_settings(self.settings)
                self.game_entry.delete(0, "end")
                self.game_entry.insert(0, DEFAULT_GAME_DIR)
                return True

        # Try scan
        found_path: Optional[str] = None
        for drive in [chr(x) for x in range(ord('A'), ord('Z') + 1)]:
            candidate = Path(f"{drive}:\\Program Files (x86)\\Steam\\steamapps\\common\\Dying Light The Beast\\ph_ft\\source")
            print(f"Trying: {candidate}")
            if candidate.exists() and (candidate / "data0.pak").exists():
                found_path = str(candidate)
                break
        if found_path:
            self.settings["game_folder"] = found_path
            save_settings(self.settings)
            self.game_entry.delete(0, "end")
            self.game_entry.insert(0, found_path)
            return True

        # Prompt browse
        messagebox.showwarning("Select game folder", f"Invalid folder ({why}). Please pick the folder with 'data0.pak'.", parent=self.root)
        self.on_browse_game_folder()
        gf2 = self.game_entry.get().strip()
        ok2, _ = validate_game_folder(gf2) if gf2 else (False, "")
        return ok2

    # --- UI construction
    def _build_ui(self):
        root = self.root
        self.root.minsize(960, 520)

        main = tk.Frame(root)
        main.pack(fill="both", expand=True, padx=10, pady=10)

        left = tk.Frame(main)
        left.pack(side="left", fill="both", expand=True, padx=(0, 8))

        right = tk.Frame(main)
        right.pack(side="left", fill="both", expand=True)

        # Game folder selection
        tk.Label(left, text="Game Source Folder (contains data0.pak):").pack(anchor="w")
        top_row = tk.Frame(left)
        top_row.pack(fill="x")

        self.game_entry = tk.Entry(top_row)
        self.game_entry.pack(side="left", fill="x", expand=True)
        self.game_entry.insert(0, self.settings.get("game_folder", ""))

        tk.Button(top_row, text="Browse...", command=self.on_browse_game_folder).pack(side="left", padx=5)

        btns = tk.Frame(left)
        btns.pack(fill="x", pady=6)
        tk.Button(btns, text="Open Folder", command=self.on_open_game_folder).pack(side="left")
        tk.Button(btns, text="Refresh", command=self.refresh_file_list).pack(side="left", padx=8)  # refresh with validation
        tk.Button(btns, text="Toggle Dark Mode", command=self.on_toggle_dark_mode).pack(side="left", padx=8)

        # Drop area to copy .pak into game folder
        self.install_drop = tk.Label(
            left, text="➕ Drop .pak/.zip/.rar/.7z here to INSTALL into game folder", relief="ridge", padx=8, pady=18
        )
        self.install_drop.pack(fill="x", pady=4)

        # List of installed files
        tk.Label(left, text="Installed Pak Files:").pack(anchor="w")
        self.file_list = tk.Listbox(left, height=10, selectmode="extended")
        self.file_list.pack(fill="both", expand=True)

        file_btns = tk.Frame(left)
        file_btns.pack(fill="x", pady=6)
        tk.Button(file_btns, text="Remove", command=self.on_remove_selected).pack(side="left")
        tk.Button(file_btns, text="Rename", command=self.on_rename_selected).pack(side="left", padx=6)
        tk.Button(file_btns, text="Disable/Enable", command=self.on_toggle_active).pack(side="left", padx=6)

        # Merge queue (right panel)
        tk.Label(right, text="Merge Queue (.pak/.zip/.rar/.7z; archives may contain .pak):").pack(anchor="w")

        merge_btns = tk.Frame(right)
        merge_btns.pack(fill="x")

        tk.Button(merge_btns, text="Add...", command=self.on_add_merge_files).pack(side="left")
        tk.Button(merge_btns, text="Remove", command=self.on_remove_merge_files).pack(side="left", padx=6)
        tk.Button(merge_btns, text="Clear", command=self.on_clear_merge_files).pack(side="left", padx=6)

        self.merge_list = tk.Listbox(right, height=10, selectmode="extended")
        self.merge_list.pack(fill="both", expand=True)

        # Drag & drop areas if tkinterdnd2 available
        if TKDND_OK and isinstance(root, TkinterDnD.Tk):  # type: ignore
            self.install_drop.drop_target_register(DND_FILES)  # type: ignore
            self.install_drop.dnd_bind("<<Drop>>", self.on_drop_install)  # type: ignore

            self.merge_drop = tk.Label(
                right, text="➕ Or drop .pak/.zip/.rar/.7z here to add to MERGE", relief="ridge", padx=8, pady=12
            )
            self.merge_drop.pack(fill="x", pady=4)
            self.merge_drop.drop_target_register(DND_FILES)  # type: ignore
            self.merge_drop.dnd_bind("<<Drop>>", self.on_drop_merge)  # type: ignore
        else:
            self.merge_drop = None
            # keep your custom footer text
            self.install_drop.config(text="Created by UnknownGamer and Pacmanninja998.")

        # Merge action
        action = tk.Frame(right)
        action.pack(fill="x", pady=8)
        tk.Button(action, text="Run MERGE", command=self.on_run_merge).pack(side="left")
        
        footer = tk.Label(self.root, text="Created by UnknownGamer and Pacmanninja998", font=("Segoe UI", 8), name="footerlabel")
        footer.pack(side="bottom", pady=1)

    # --- Styling / palette

    def _apply_palette(self) -> None:
        dm = bool(self.settings.get("dark_mode", False))
        pal = self.palette["dark" if dm else "light"]
        self.palette_active = pal

        def o(opt, val):
            try:
                self.root.option_add(opt, val)
            except Exception:
                pass

        bg = pal["bg"]
        bg2 = pal["bg2"]
        fg = pal["fg"]
        acc = pal["acc"]

        FONT_UI = ("Segoe UI", 10)   # tuple → avoids Tcl font tokenization issues
        FONT_MONO = ("Consolas", 10)

        o("*Font",           FONT_UI)
        o("*Button.font",    FONT_UI)
        o("*TButton.font",   FONT_UI)
        o("*Entry.font",     FONT_UI)
        o("*Label.font",     FONT_UI)
        o("*Listbox.font",   FONT_UI)
        o("*Text.font",      FONT_MONO)

        o("*Button.background", bg2)
        o("*Button.foreground", fg)
        o("*Label.background", bg)
        o("*Label.foreground", fg)
        o("*Frame.background", bg)
        o("*Entry.background", pal["entry_bg"])
        o("*Entry.foreground", pal["entry_fg"])
        o("*Listbox.background", pal["listbox_bg"])
        o("*Listbox.foreground", pal["listbox_fg"])
        o("*Text.background", pal["text_bg"])
        o("*Text.foreground", pal["text_fg"])
        o("*highlightBackground", bg)
        o("*highlightColor", bg)
        o("*selectBackground", pal["listbox_sel_bg"])
        o("*selectForeground", pal["listbox_sel_fg"])
        o("*activeForeground", fg)
        o("*activeBackground", acc)
        o("*troughColor", bg2)
        o("*borderColor", bg)
        o("*background", bg)

        self.root.configure(bg=bg)

    def _restyle_existing_widgets(self) -> None:
        pal = self.palette_active

        def style_entry(w: tk.Entry):
            w.config(bg=pal["entry_bg"], fg=pal["entry_fg"], insertbackground=pal["entry_insert"])

        def style_label(w: tk.Label):
            w.config(bg=pal["label_bg"], fg=pal["label_fg"])

        def style_listbox(w: tk.Listbox):
            w.config(
                bg=pal["listbox_bg"],
                fg=pal["listbox_fg"],
                selectbackground=pal["listbox_sel_bg"],
                selectforeground=pal["listbox_sel_fg"],
            )

        def style_text(w: tk.Text):
            w.config(bg=pal["text_bg"], fg=pal["text_fg"], insertbackground=pal["text_insert"])

        style_entry(self.game_entry)
        style_label(self.install_drop)
        style_listbox(self.file_list)
        style_listbox(self.merge_list)
        if isinstance(getattr(self, "merge_drop", None), tk.Label):
            style_label(self.merge_drop)  # type: ignore

    # --- Commands

    def on_browse_game_folder(self):
        p = filedialog.askdirectory(title="Select game source folder (contains data0.pak)")
        if p:
            ok, why = validate_game_folder(p)
            if not ok:
                messagebox.showerror("Invalid folder", f"{why}\n\nPick the folder with 'data0.pak'.")
                return
            self.game_entry.delete(0, "end")
            self.game_entry.insert(0, p)
            self.settings["game_folder"] = p
            save_settings(self.settings)
            self.refresh_file_list()

    def on_open_game_folder(self):
        gf = self.settings.get("game_folder", "")
        if not gf:
            messagebox.showerror("No game folder set", "Set the game folder above first.")
            return
        try:
            if sys.platform.startswith("win"):
                os.startfile(gf)  # type: ignore
            elif sys.platform.startswith("darwin"):
                subprocess.check_call(["open", gf])
            else:
                subprocess.check_call(["xdg-open", gf])
        except Exception as e:
            messagebox.showerror("Error", f"Failed to open folder:\n{e}")

    def on_toggle_dark_mode(self):
        self.settings["dark_mode"] = not bool(self.settings.get("dark_mode", False))
        save_settings(self.settings)
        self._apply_palette()
        self._restyle_existing_widgets()

    def refresh_file_list(self):
        """Rescan the game folder and refresh the installed pak list (validates first)."""
        if not self.ensure_valid_game_folder(interactive=True):
            return
        gf = self.game_entry.get().strip() or self.settings.get("game_folder", "")
        if gf and gf != self.settings.get("game_folder", ""):
            self.settings["game_folder"] = gf
            save_settings(self.settings)
        self.file_list.delete(0, "end")
        if not gf:
            return
        try:
            names = sorted([n for n in os.listdir(gf) if n.lower().endswith(".pak")])
        except Exception:
            names = []
        for n in names:
            meta = self.settings.setdefault("files", {}).get(n, {})
            nick = meta.get("nickname", "")
            active = meta.get("active", True)
            label = n
            if nick:
                label += f" — {nick}"
            if not active:
                label += " (disabled)"
            self.file_list.insert("end", label)
        save_settings(self.settings)

    def on_remove_selected(self):
        gf = self.settings.get("game_folder", "")
        if not gf:
            messagebox.showerror("No game folder set", "Set the game folder above first.")
            return
        sel = list(self.file_list.curselection())
        if not sel:
            return
        names = sorted([n for n in os.listdir(gf) if n.lower().endswith(".pak")])
        sel_names = []
        for i in sel:
            if i < len(names):
                sel_names.append(names[i])
        if not sel_names:
            return
        if not messagebox.askyesno("Confirm Delete", f"Delete selected files?\n\n" + "\n".join(sel_names)):
            return
        for n in sel_names:
            try:
                os.remove(os.path.join(gf, n))
            except Exception as e:
                messagebox.showerror("Error", f"Failed to delete {n}:\n{e}")
            self.settings.setdefault("files", {}).pop(n, None)
        save_settings(self.settings)
        self.refresh_file_list()

    def on_rename_selected(self):
        gf = self.settings.get("game_folder", "")
        if not gf:
            messagebox.showerror("No game folder set", "Set the game folder above first.")
            return
        sel = list(self.file_list.curselection())
        if len(sel) != 1:
            messagebox.showwarning("Select One", "Select exactly one file to rename.")
            return
        names = sorted([n for n in os.listdir(gf) if n.lower().endswith(".pak")])
        idx = sel[0]
        if idx >= len(names):
            return
        old = names[idx]
        new = simpledialog.askstring("Rename", "New filename (with .pak):", initialvalue=old)
        if not new:
            return
        new = new.strip()
        if not new.lower().endswith(".pak"):
            messagebox.showerror("Invalid name", "Filename must end with .pak")
            return
        try:
            os.rename(os.path.join(gf, old), os.path.join(gf, new))
        except Exception as e:
            messagebox.showerror("Error", f"Failed to rename:\n{e}")
            return
        meta = self.settings.setdefault("files", {}).pop(old, {})
        self.settings["files"][new] = meta
        save_settings(self.settings)
        self.refresh_file_list()

    def on_toggle_active(self):
        gf = self.settings.get("game_folder", "")
        if not gf:
            messagebox.showerror("No game folder set", "Set the game folder above first.")
            return
        sel = list(self.file_list.curselection())
        if not sel:
            return
        names = sorted([n for n in os.listdir(gf) if n.lower().endswith(".pak")])
        for i in sel:
            if i < len(names):
                n = names[i]
                meta = self.settings.setdefault("files", {}).setdefault(n, {"nickname": "", "link": "", "active": True})
                meta["active"] = not bool(meta.get("active", True))
        save_settings(self.settings)
        self.refresh_file_list()

    # ---- Drag/drop install
    def on_drop_install(self, event):
        gf = self.settings.get("game_folder", "")
        if not gf:
            messagebox.showerror("No game folder set", "Set the game folder above first.")
            return
        files = parse_drop_files(event.data)
        for f in files:
            if not os.path.isfile(f):
                messagebox.showwarning("Warning", f"Skipping non-file: {f}")
                continue
            suf = (_sniff_archive_type(Path(f)) or os.path.splitext(f)[1].lower().lstrip(".")).lower()
            if suf == "pak":
                new_name = next_data_name(gf)
                shutil.copy2(f, os.path.join(gf, new_name))
                self.settings.setdefault("files", {})[new_name] = {"nickname": "", "link": "", "active": True}
            elif suf in ("zip", "rar", "7z", "7zip"):
                inner = list(_yield_pak_streams_from_archive(Path(f)))
                if len(inner) == 0:
                    messagebox.showwarning("Warning", f"No .pak found inside: {os.path.basename(f)}")
                    continue
                if len(inner) > 1:
                    messagebox.showwarning(
                        "Warning",
                        f"Found {len(inner)} .pak files inside {os.path.basename(f)}. Use the MERGE queue instead.",
                    )
                    continue
                _, mem_stream = inner[0]
                mem_stream.seek(0)
                new_name = next_data_name(gf)
                with open(os.path.join(gf, new_name), "wb") as out:
                    out.write(mem_stream.read())
                self.settings.setdefault("files", {})[new_name] = {"nickname": "", "link": "", "active": True}
            else:
                messagebox.showwarning("Warning", f"Unsupported file type: {f}")
                continue
        self.refresh_file_list()

    # ---- Merge queue
    def on_add_merge_files(self):
        paths = filedialog.askopenfilenames(
            title="Select Mod Archives (.pak/.zip/.rar/.7z)",
            filetypes=[("Mod archives", "*.pak *.zip *.rar *.7z"), ("All files", "*.*")],
        )
        if not paths:
            return
        for p in paths:
            self.merge_list.insert(tk.END, p)
        self.persist_merge_queue()

    def on_remove_merge_files(self):
        sel = list(self.merge_list.curselection())
        if not sel:
            return
        sel.reverse()
        for i in sel:
            self.merge_list.delete(i)
        self.persist_merge_queue()

    def on_clear_merge_files(self):
        self.merge_list.delete(0, tk.END)
        self.persist_merge_queue()

    def refresh_merge_queue(self):
        self.merge_list.delete(0, "end")
        for p in self.settings.get("merge_queue", []):
            self.merge_list.insert("end", p)

    def on_drop_merge(self, event):
        files = parse_drop_files(event.data)
        for f in files:
            if os.path.isfile(f) and f.lower().endswith((".pak", ".zip", ".rar", ".7z", ".7zip")):
                self.merge_list.insert(tk.END, f)
        self.persist_merge_queue()

    def persist_merge_queue(self):
        self.settings["merge_queue"] = list(self.merge_list.get(0, tk.END))
        save_settings(self.settings)

    # ---- Merge action
    def on_run_merge(self):
        if not self.ensure_valid_game_folder(interactive=True):
            return
        gf = self.game_entry.get().strip()
        self.settings["game_folder"] = gf
        save_settings(self.settings)

        queue_paths = list(self.merge_list.get(0, tk.END))
        if not queue_paths:
            messagebox.showwarning("Empty queue", "Add at least one mod archive (.pak/.zip/.rar/.7z).")
            return

        for w in (self.game_entry, self.file_list, self.merge_list):
            w.config(state="disabled")

        def worker():
            try:
                res = run_merge(
                    Path(gf),
                    [Path(p) for p in queue_paths],
                    self.ui_sync,
                    self.root,
                    self.log,
                )
                if res:
                    self.log(f"Done: {res}")
                    self.root.after(500, self.refresh_file_list)
            except Exception as e:
                self.log(f"ERROR: {e}")
                messagebox.showerror("Merge failed", f"{e}")
            finally:
                def reenable():
                    for w in (self.game_entry, self.file_list, self.merge_list):
                        w.config(state="normal")
                self.root.after(0, reenable)

        threading.Thread(target=worker, daemon=True).start()

    # ---- Logging
    def log(self, msg: str):
        print(msg)

def main():
    try:
        root = TkinterDnD.Tk() if TKDND_OK else tk.Tk()  # type: ignore
        app = App(root)
        root.mainloop()
    except Exception:
        import traceback
        tb = traceback.format_exc()
        try:
            r = tk.Tk()
            r.withdraw()
            messagebox.showerror("Startup error", tb)
        except Exception:
            pass
        try:
            input("Press Enter to exit...")
        except Exception:
            pass

if __name__ == "__main__":
    main()
