# file: tool.py
"""
Dying Light: The Beast — Mod Manager + Merger (GUI)
- Fixed theme error: avoid Tk option_get() returning "", use our own palette.
- Dark Mode toggle (persisted).
- Settings saved to a user-writable path with atomic writes.
"""

from __future__ import annotations

import json
import os
import queue
import re
import shutil
import stat
import sys
import tempfile
import threading
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog

# PyInstaller bundle TKDND init (why: allow drag/drop in a frozen build)
if getattr(sys, "frozen", False):
    tkdnd_lib = os.path.join(sys._MEIPASS, "tkinterdnd2", "tkdnd")  # type: ignore[attr-defined]
    os.environ["TKDND_LIBRARY"] = tkdnd_lib

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
DEFAULT_GAME_DIR = r"C:\Program Files (x86)\Steam\steamapps\common\Dying Light The Beast\ph_ft\source"


# ---- Settings I/O (safe path + atomic write)
def _default_settings_dir(app_name: str = APP_NAME) -> Path:
    # Why: avoid protected folders (Program Files / CWD)
    if os.name == "nt":
        base = os.getenv("APPDATA") or str(Path.home() / "AppData" / "Roaming")
        return Path(base) / app_name
    xdg = os.getenv("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / app_name


def get_settings_path(filename: str = SETTINGS_FILENAME) -> Path:
    return _default_settings_dir() / filename


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _ensure_writable(path: Path) -> None:
    # Why: some extracted files are read-only on Windows
    if path.exists():
        try:
            mode = path.stat().st_mode
            path.chmod(mode | stat.S_IWRITE)
        except Exception:
            pass


def _atomic_write_text(target: Path, text: str, encoding: str = "utf-8") -> None:
    tmp = target.with_suffix(target.suffix + ".tmp")
    with open(tmp, "w", encoding=encoding) as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, target)


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
    try:
        _atomic_write_text(path, payload)
        print(f"Saving settings to {path}")
        return path
    except PermissionError:
        # Last-resort fallback if AppData blocked
        fb = Path.home() / f".{APP_NAME.replace(' ', '_').lower()}" / SETTINGS_FILENAME
        _ensure_parent_dir(fb)
        _ensure_writable(fb)
        _atomic_write_text(fb, payload)
        return fb


# ---- Data & helpers
@dataclass
class ScriptFile:
    full_path_in_pak: str
    content: str
    source_pak: str


def parse_drop_files(event_data: str) -> List[str]:
    # Handles Windows paths with spaces/braces
    pat = re.compile(r"\{([^}]+)\}|(\S+)")
    return [a or b for a, b in pat.findall(event_data)]


def safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def next_data_name(game_folder: str) -> str:
    # Reserve >= data3.pak to avoid base game packages
    existing = [
        f for f in os.listdir(game_folder)
        if f.startswith("data") and (f.endswith(".pak") or f.endswith(".pak.disabled"))
    ]
    used = set()
    for f in existing:
        num_part = f[4:].split(".")[0]
        if num_part.isdigit():
            used.add(int(num_part))
    n = 3
    while n in used:
        n += 1
    return f"data{n}.pak"


def get_game_file_structure(game_pak_path: Path) -> Dict[str, str]:
    structure: Dict[str, str] = {}
    with zipfile.ZipFile(game_pak_path, "r") as archive:
        for entry in archive.namelist():
            if entry.endswith(".scr") and not entry.endswith("/"):
                file_name = os.path.basename(entry)
                full_path = entry.replace("/", "\\")
                if file_name.lower() not in (k.lower() for k in structure.keys()):
                    structure[file_name] = full_path
    return structure


def read_scripts_from_single_pak_for_fixing(
    pak_path_or_stream, needs_fixing_flag: List[bool], file_structure: Dict[str, str], unknown_files: List[str]
) -> List[Tuple[str, str, "BytesIO"]]:
    if isinstance(pak_path_or_stream, (str, Path)):
        archive = zipfile.ZipFile(pak_path_or_stream, "r")
    else:
        pak_path_or_stream.seek(0)
        archive = zipfile.ZipFile(pak_path_or_stream, "r")

    mod_scripts = []
    try:
        for entry in archive.namelist():
            if entry.endswith(".scr") and not entry.endswith("/"):
                file_name = os.path.basename(entry)
                mod_path = entry.replace("/", "\\")
                correct_path = file_structure.get(file_name)
                if correct_path:
                    if mod_path.lower() != correct_path.lower():
                        needs_fixing_flag[0] = True
                    content = archive.read(entry)
                    from io import BytesIO

                    memory_stream = BytesIO(content)
                    mod_scripts.append((file_name, correct_path, memory_stream))
                else:
                    unknown_files.append(mod_path)
    finally:
        archive.close()
    return mod_scripts


def read_scripts_from_single_pak(pak_path_or_stream, source_name: str) -> List[ScriptFile]:
    if isinstance(pak_path_or_stream, (str, Path)):
        archive = zipfile.ZipFile(pak_path_or_stream, "r")
    else:
        pak_path_or_stream.seek(0)
        archive = zipfile.ZipFile(pak_path_or_stream, "r")

    scripts: List[ScriptFile] = []
    try:
        for entry in archive.namelist():
            if entry.endswith(".scr"):
                with archive.open(entry) as scr_stream:
                    data = scr_stream.read()
                    try:
                        content = data.decode("utf-8")
                    except UnicodeDecodeError:
                        try:
                            content = data.decode("latin1")
                        except Exception:
                            content = data.decode("utf-8", errors="replace")
                    scripts.append(ScriptFile(entry.replace("\\", "/"), content, source_name))
    finally:
        archive.close()
    return scripts


def load_scripts_from_pak_files(pak_file_paths: Iterable[Path]) -> List[ScriptFile]:
    all_scripts: List[ScriptFile] = []
    for path in pak_file_paths:
        all_scripts.extend(read_scripts_from_single_pak(path, os.path.basename(path)))
    return all_scripts


def load_all_scripts_from_mods_folder(paths: Iterable[Path]) -> List[ScriptFile]:
    scripts: List[ScriptFile] = []
    for mod_file_path in paths:
        try:
            source_name = os.path.basename(mod_file_path)
            suf = mod_file_path.suffix.lower()
            if suf == ".pak":
                scripts.extend(read_scripts_from_single_pak(mod_file_path, source_name))
            elif suf == ".zip":
                with zipfile.ZipFile(mod_file_path, "r") as archive:
                    for entry in archive.namelist():
                        if entry.endswith(".pak"):
                            with archive.open(entry) as pak_entry_stream:
                                from io import BytesIO

                                mem_stream = BytesIO(pak_entry_stream.read())
                                scripts.extend(read_scripts_from_single_pak(mem_stream, source_name))
            else:
                print(f"Unsupported mod archive type: {mod_file_path}")
        except Exception as e:
            print(f"ERROR: Could not read '{os.path.basename(mod_file_path)}'. Reason: {e}")
    return scripts


_key_regex = re.compile(r'^(\w+)\s*\(\s*"([^"]+)"')


def try_parse_key(line: str) -> Optional[str]:
    line = line.strip()
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
        self.root.after(50, self._pump)

    def _pump(self):
        try:
            while True:
                func, evt, args, kwargs = self.q.get_nowait()
                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    result = e
                kwargs["__result__"] = result
                evt.set()
        except queue.Empty:
            pass
        self.root.after(50, self._pump)

    def call(self, func: Callable, *args, **kwargs):
        evt = threading.Event()
        kwargs2 = dict(kwargs)
        self.q.put((func, evt, list(args), kwargs2))
        evt.wait()
        res = kwargs2.get("__result__")
        if isinstance(res, Exception):
            raise res
        return res


def ask_conflict_choice_dialog(
    parent: tk.Tk, file_path: str, key: str, options: List[Tuple[str, List[str]]]
) -> Tuple[str, Optional[str]]:
    dlg = tk.Toplevel(parent)
    dlg.title(f"Conflict: {Path(file_path).name} - {key}")
    dlg.grab_set()
    dlg.resizable(True, True)

    tk.Label(dlg, text=f"Conflict in {file_path}\nKey: {key}", justify="left").pack(anchor="w", padx=10, pady=6)

    prefer_var = tk.BooleanVar(value=False)
    frame = tk.Frame(dlg)
    frame.pack(fill="both", expand=True, padx=10, pady=6)
    lb = tk.Listbox(frame)
    lb.pack(side="left", fill="both", expand=True)
    sb = tk.Scrollbar(frame, command=lb.yview)
    sb.pack(side="right", fill="y")
    lb.config(yscrollcommand=sb.set)

    source_map: Dict[int, Optional[str]] = {}
    for idx, (line_text, sources) in enumerate(options):
        lb.insert(tk.END, f"{idx+1}. ({', '.join(sources)}): {line_text.strip()}")
        source_map[idx] = sources[0] if sources else None
    lb.selection_set(0)

    def on_ok():
        sel = lb.curselection()
        if not sel:
            return
        idx = sel[0]
        chosen_line = options[idx][0]
        chosen_source = source_map[idx] if prefer_var.get() else None
        dlg.__result__ = (chosen_line, chosen_source)
        dlg.destroy()

    tk.Checkbutton(dlg, text="Prefer this mod for the rest of this FILE", variable=prefer_var).pack(
        anchor="w", padx=10
    )

    btns = tk.Frame(dlg)
    btns.pack(fill="x", padx=10, pady=10)
    tk.Button(btns, text="OK", command=on_ok).pack(side="right")
    tk.Button(btns, text="Cancel", command=lambda: dlg.destroy()).pack(side="right", padx=6)

    parent.wait_window(dlg)
    return getattr(dlg, "__result__", (options[0][0], None))


def generate_merged_file_content(
    original: ScriptFile,
    mods: List[ScriptFile],
    ui_sync: UiSync,
    parent: tk.Tk,
) -> str:
    original_map: Dict[str, str] = {}
    for line in original.content.replace("\r\n", "\n").split("\n"):
        key = try_parse_key(line)
        if key and key not in original_map:
            original_map[key] = line

    mod_maps: List[Dict] = []
    for mod in mods:
        mod_map: Dict[str, str] = {}
        for line in mod.content.replace("\r\n", "\n").split("\n"):
            key = try_parse_key(line)
            if key and key not in mod_map:
                mod_map[key] = line
        mod_maps.append({"source_pak": mod.source_pak, "map": mod_map})

    final_content: List[str] = []
    resolutions: Dict[str, str] = {}
    preferred_mod_source: Optional[str] = None
    auto_resolved_count = 0
    original_lines = original.content.replace("\r\n", "\n").split("\n")

    for original_line in original_lines:
        key = try_parse_key(original_line)
        if key is None:
            final_content.append(original_line)
            continue

        if key in resolutions:
            final_content.append(resolutions[key])
            continue

        actual_changes: List[Tuple[str, str]] = []
        base_line = original_map.get(key)
        for mod in mod_maps:
            mod_line = mod["map"].get(key)
            if mod_line and mod_line != base_line:
                actual_changes.append((mod_line, mod["source_pak"]))

        if len(actual_changes) == 0:
            final_content.append(original_line)
        elif len(actual_changes) == 1:
            final_content.append(actual_changes[0][0])
            resolutions[key] = actual_changes[0][0]
        else:
            distinct_changes: Dict[str, List[str]] = {}
            for line, src in actual_changes:
                distinct_changes.setdefault(line, []).append(src)

            if len(distinct_changes) == 1:
                line = next(iter(distinct_changes))
                final_content.append(line)
                resolutions[key] = line
            else:
                if preferred_mod_source:
                    chosen_line = next(
                        (line for line, sources in distinct_changes.items() if preferred_mod_source in sources),
                        next(iter(distinct_changes)),
                    )
                    auto_resolved_count += 1
                else:
                    opts = list(distinct_changes.items())
                    chosen_line, chosen_source = ui_sync.call(
                        ask_conflict_choice_dialog,
                        parent,
                        original.full_path_in_pak,
                        key,
                        opts,
                    )
                    if chosen_source:
                        preferred_mod_source = chosen_source
                final_content.append(chosen_line)
                resolutions[key] = chosen_line

    if auto_resolved_count > 0:
        print(f"Auto-resolved {auto_resolved_count} conflicts using preference: '{preferred_mod_source}'.")
    return "\n".join(final_content)


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
            if mod_file.suffix.lower() == ".pak":
                _ = read_scripts_from_single_pak_for_fixing(mod_file, needs_fixing, structure, unknown_files)
            else:
                with zipfile.ZipFile(mod_file, "r") as archive:
                    for entry in archive.namelist():
                        if entry.endswith(".pak"):
                            with archive.open(entry) as pak_entry_stream:
                                from io import BytesIO

                                mem_stream = BytesIO(pak_entry_stream.read())
                                _ = read_scripts_from_single_pak_for_fixing(mem_stream, needs_fixing, structure, unknown_files)
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
                    scripts = read_scripts_from_single_pak_for_fixing(mod_file, needs_fixing, structure, [])
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

        log("Loading original scripts from game packages...")
        source_paks = list(game_folder.glob("*.pak"))
        originals = load_scripts_from_pak_files(source_paks)
        log(f"✓ {len(originals)} original scripts loaded.")

        log("Loading scripts from mods...")
        modded = load_all_scripts_from_mods_folder(valid_mods)
        log(f"✓ {len(modded)} modded scripts loaded.")

        log("Merging...")
        final_contents: Dict[str, str] = {}
        mod_file_groups: Dict[str, List[ScriptFile]] = defaultdict(list)
        for scr in modded:
            mod_file_groups[scr.full_path_in_pak].append(scr)

        originals_by_path: Dict[str, ScriptFile] = {o.full_path_in_pak.lower(): o for o in originals}

        for file_path, mods_touching in mod_file_groups.items():
            if len(mods_touching) == 1:
                final_contents[file_path] = mods_touching[0].content
                continue
            orig = originals_by_path.get(file_path.lower())
            if orig is None:
                final_contents[file_path] = mods_touching[0].content
                continue
            merged = generate_merged_file_content(orig, mods_touching, ui_sync, parent)
            final_contents[file_path] = merged

        log(f"Merged files: {len(final_contents)}")
        log("Creating package...")

        staging_dir = staging_root / "staging_area"
        if staging_dir.exists():
            shutil.rmtree(staging_dir)
        safe_mkdir(staging_dir)

        for file_entry, content in final_contents.items():
            full_path = staging_dir / Path(file_entry)
            safe_mkdir(full_path.parent)
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(content)

        name = next_data_name(str(game_folder))
        final_pak_path = game_folder / name
        if final_pak_path.exists():
            final_pak_path.unlink()

        zip_base = final_pak_path.with_suffix("")
        shutil.make_archive(str(zip_base), "zip", staging_dir)
        zip_path = zip_base.with_suffix(".zip")
        if zip_path.exists():
            zip_path.rename(final_pak_path)

        log(f"SUCCESS: Created {final_pak_path.name}")
        return final_pak_path
    finally:
        try:
            shutil.rmtree(staging_root, ignore_errors=True)
            shutil.rmtree(temp_root, ignore_errors=True)
        except Exception:
            pass


# ---- GUI
class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.settings = load_settings()
        if not self.settings.get("game_folder") and os.path.exists(DEFAULT_GAME_DIR):
            self.settings["game_folder"] = DEFAULT_GAME_DIR
            save_settings(self.settings)
        # Theme before building widgets
        self.dark_mode = bool(self.settings.get("dark_mode", False))
        self.palette: Dict[str, str] = {}
        self.apply_theme(self.dark_mode)

        self.ui_sync = UiSync(root)

        root.title("Dying Light: The Beast")
        root.geometry("980x640")

        # Menu: View ▸ Dark Mode
        menubar = tk.Menu(root)
        view_menu = tk.Menu(menubar, tearoff=False)
        self.dark_var = tk.BooleanVar(value=self.dark_mode)
        view_menu.add_checkbutton(label="Dark Mode", variable=self.dark_var, command=self.on_toggle_dark_mode)
        menubar.add_cascade(label="View", menu=view_menu)
        root.config(menu=menubar)

        # Top: game folder selector
        top = tk.Frame(root)
        top.pack(fill="x", padx=10, pady=8)

        tk.Label(top, text="Game Source Folder (contains data0.pak):").pack(side="left")
        self.game_folder_var = tk.StringVar(value=self.settings.get("game_folder", ""))
        self.game_entry = tk.Entry(top, textvariable=self.game_folder_var, width=80)
        self.game_entry.pack(side="left", padx=8, fill="x", expand=True)
        tk.Button(top, text="Browse...", command=self.on_browse_game_folder).pack(side="left")

        # Main split
        main = tk.PanedWindow(root, sashrelief="raised")
        main.pack(fill="both", expand=True, padx=10, pady=6)

        # Left: installed .pak manager
        left = tk.Frame(main)
        main.add(left, minsize=360)

        tk.Label(left, text="Installed data*.pak in Game Folder:").pack(anchor="w")
        self.file_list = tk.Listbox(left)
        self.file_list.pack(fill="both", expand=True, pady=4)

        btns = tk.Frame(left)
        btns.pack(fill="x", pady=4)
        tk.Button(btns, text="Set Nickname", command=self.on_set_nickname).pack(side="left", padx=2)
        tk.Button(btns, text="Set Link", command=self.on_set_link).pack(side="left", padx=2)
        tk.Button(btns, text="Open Link", command=self.on_open_link).pack(side="left", padx=2)
        tk.Button(btns, text="Activate/Deactivate", command=self.on_toggle_active).pack(side="left", padx=2)
        tk.Button(btns, text="Delete", command=self.on_delete).pack(side="left", padx=2)

        # Drop area to copy .pak into game folder
        self.install_drop = tk.Label(
            left, text="➕ Drop .pak files here to INSTALL into game folder", relief="ridge", padx=8, pady=18
        )
        self.install_drop.pack(fill="x", pady=4)

        # Right: merge queue
        right = tk.Frame(main)
        main.add(right)

        tk.Label(right, text="Merge Queue (.pak or .zip, each may contain .pak):").pack(anchor="w")
        self.merge_list = tk.Listbox(right)
        self.merge_list.pack(fill="both", expand=True, pady=4)

        mbtns = tk.Frame(right)
        mbtns.pack(fill="x", pady=4)
        tk.Button(mbtns, text="Add Files…", command=self.on_add_merge_files).pack(side="left", padx=2)
        tk.Button(mbtns, text="Remove Selected", command=self.on_remove_merge_files).pack(side="left", padx=2)
        tk.Button(mbtns, text="Clear", command=self.on_clear_merge_files).pack(side="left", padx=2)
        tk.Button(mbtns, text="Run Merge ▶", command=self.on_run_merge).pack(side="right", padx=2)

        # Log console
        tk.Label(root, text="Log:").pack(anchor="w", padx=10)
        self.log_txt = tk.Text(root, height=10, state="disabled")
        self.log_txt.pack(fill="both", expand=False, padx=10, pady=(0, 10))

        # DnD init
        if TKDND_OK and isinstance(root, TkinterDnD.Tk):  # type: ignore
            self.install_drop.drop_target_register(DND_FILES)  # type: ignore
            self.install_drop.dnd_bind("<<Drop>>", self.on_drop_install)  # type: ignore

            self.merge_drop = tk.Label(
                right, text="➕ Or drop .pak/.zip here to add to MERGE", relief="ridge", padx=8, pady=12
            )
            self.merge_drop.pack(fill="x", pady=4)
            self.merge_drop.drop_target_register(DND_FILES)  # type: ignore
            self.merge_drop.dnd_bind("<<Drop>>", self.on_drop_merge)  # type: ignore
        else:
            self.merge_drop = None
            self.install_drop.config(text="Drag & drop requires tkinterdnd2. Use buttons instead.")

        # Load persisted merge queue
        for p in self.settings.get("merge_queue", []):
            if os.path.exists(p):
                self.merge_list.insert(tk.END, p)

        # Apply palette to already-created widgets
        self._restyle_existing_widgets()
        self.refresh_file_list()

    # ---- Theme handling
    def apply_theme(self, dark: bool) -> None:
        # Keep our own palette to avoid empty values from option DB
        if dark:
            bg, bg2, fg, acc, sel_bg, sel_fg = "#1f2124", "#2b2d31", "#e6e6e6", "#3a3d41", "#3b82f6", "#ffffff"
        else:
            bg, bg2, fg, acc, sel_bg, sel_fg = "#f3f3f3", "#ffffff", "#202020", "#e0e0e0", "#316ac5", "#ffffff"

        self.palette = {
            # containers & labels
            "label_bg": bg,
            "label_fg": fg,
            # entries
            "entry_bg": bg2,
            "entry_fg": fg,
            "entry_insert": fg,
            # listboxes
            "listbox_bg": bg2,
            "listbox_fg": fg,
            "listbox_sel_bg": sel_bg,
            "listbox_sel_fg": sel_fg,
            # text
            "text_bg": bg2,
            "text_fg": fg,
            "text_insert": fg,
        }

        # Option DB is still set for future widgets (ok if ignored)
        o = self.root.option_add
        o("*Frame.background", bg)
        o("*Label.background", bg)
        o("*Label.foreground", fg)
        o("*Entry.background", bg2)
        o("*Entry.foreground", fg)
        o("*Entry.insertBackground", fg)
        o("*Listbox.background", bg2)
        o("*Listbox.foreground", fg)
        o("*Listbox.selectBackground", sel_bg)
        o("*Listbox.selectForeground", sel_fg)
        o("*Text.background", bg2)
        o("*Text.foreground", fg)
        o("*Text.insertBackground", fg)
        o("*Button.background", acc)
        o("*Button.activeBackground", sel_bg)
        o("*Button.foreground", fg)
        o("*Checkbutton.background", bg)
        o("*Checkbutton.foreground", fg)
        o("*Menubutton.background", acc)
        o("*Menubutton.foreground", fg)
        o("*Menu.background", bg2)
        o("*Menu.foreground", fg)
        o("*highlightBackground", bg)
        o("*highlightColor", bg)

        self.root.configure(bg=bg)

    def _restyle_existing_widgets(self) -> None:
        # Use explicit colors from our palette (why: avoid empty strings from option_get)
        def style_entry(w: tk.Entry):
            w.config(bg=self.palette["entry_bg"], fg=self.palette["entry_fg"], insertbackground=self.palette["entry_insert"])

        def style_label(w: tk.Label):
            w.config(bg=self.palette["label_bg"], fg=self.palette["label_fg"])

        def style_listbox(w: tk.Listbox):
            w.config(
                bg=self.palette["listbox_bg"],
                fg=self.palette["listbox_fg"],
                selectbackground=self.palette["listbox_sel_bg"],
                selectforeground=self.palette["listbox_sel_fg"],
            )

        def style_text(w: tk.Text):
            w.config(bg=self.palette["text_bg"], fg=self.palette["text_fg"], insertbackground=self.palette["text_insert"])

        # Specific widgets
        style_entry(self.game_entry)
        style_label(self.install_drop)
        if isinstance(getattr(self, "merge_drop", None), tk.Label):
            style_label(self.merge_drop)  # type: ignore
        style_listbox(self.file_list)
        style_listbox(self.merge_list)
        style_text(self.log_txt)

    def on_toggle_dark_mode(self) -> None:
        self.dark_mode = bool(self.dark_var.get())
        self.apply_theme(self.dark_mode)
        self._restyle_existing_widgets()
        self.settings["dark_mode"] = self.dark_mode
        save_settings(self.settings)

    # ---- Logging
    def log(self, msg: str) -> None:
        self.log_txt.config(state="normal")
        self.log_txt.insert(tk.END, msg + "\n")
        self.log_txt.see(tk.END)
        self.log_txt.config(state="disabled")
        self.log_txt.update_idletasks()

    # ---- Game folder selection
    def on_browse_game_folder(self):
        folder = filedialog.askdirectory(title="Select Game Source Folder")
        if not folder:
            return
        self.game_folder_var.set(folder)
        self.settings["game_folder"] = folder
        save_settings(self.settings)
        self.refresh_file_list()

    # ---- File list helpers & actions
    def _selected_filename(self) -> Optional[str]:
        sel = self.file_list.curselection()
        if not sel:
            return None
        val = self.file_list.get(sel[0])
        return val.split(" | ")[0].split(" [")[0]

    def refresh_file_list(self):
        self.file_list.delete(0, tk.END)
        gf = self.settings.get("game_folder", "")
        if not os.path.isdir(gf):
            return
        files = os.listdir(gf)
        self.settings["files"] = {k: v for k, v in self.settings.get("files", {}).items() if k in files}
        idx = 0
        for fname in sorted(files):
            if fname.startswith("data") and (fname.endswith(".pak") or fname.endswith(".pak.disabled")):
                meta = self.settings["files"].setdefault(
                    fname, {"nickname": "", "link": "", "active": fname.endswith(".pak")}
                )
                display = fname
                if not meta["active"]:
                    display += " [deactivated]"
                if meta.get("nickname"):
                    display += f" | {meta['nickname']}"
                self.file_list.insert(tk.END, display)
                color = "green" if meta["active"] else "red"
                self.file_list.itemconfig(idx, {"fg": color})
                idx += 1
        save_settings(self.settings)

    def on_set_nickname(self):
        fname = self._selected_filename()
        if not fname:
            return
        initial = self.settings["files"].get(fname, {}).get("nickname", "")
        nick = simpledialog.askstring("Set Nickname", f"Enter nickname for {fname}:", initialvalue=initial)
        if nick is not None:
            self.settings["files"].setdefault(fname, {}).update({"nickname": nick})
            save_settings(self.settings)
            self.refresh_file_list()

    def on_set_link(self):
        fname = self._selected_filename()
        if not fname:
            return
        initial = self.settings["files"].get(fname, {}).get("link", "")
        link = simpledialog.askstring("Set Link", f"Enter clickable URL for {fname}:", initialvalue=initial)
        if link is not None:
            self.settings["files"].setdefault(fname, {}).update({"link": link})
            save_settings(self.settings)
            self.refresh_file_list()

    def on_open_link(self):
        import webbrowser
        fname = self._selected_filename()
        if not fname:
            return
        link = self.settings["files"].get(fname, {}).get("link")
        if link:
            webbrowser.open(link)

    def on_delete(self):
        fname = self._selected_filename()
        if not fname:
            return
        gf = self.settings.get("game_folder", "")
        path = os.path.join(gf, fname)
        if messagebox.askyesno("Delete File", f"Delete file {fname}? This cannot be undone."):
            try:
                os.remove(path)
                self.settings["files"].pop(fname, None)
                save_settings(self.settings)
                self.refresh_file_list()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to delete file:\n{e}")

    def on_toggle_active(self):
        fname = self._selected_filename()
        if not fname:
            return
        gf = self.settings.get("game_folder", "")
        path = os.path.join(gf, fname)
        meta = self.settings["files"].get(fname)
        if not meta:
            return
        try:
            if meta["active"]:
                new_path = path + ".disabled"
                os.rename(path, new_path)
                meta["active"] = False
                self.settings["files"][fname + ".disabled"] = self.settings["files"].pop(fname)
            else:
                if fname.endswith(".disabled"):
                    new_name = fname[:-9]
                    new_path = os.path.join(gf, new_name)
                else:
                    new_name = fname
                    new_path = path
                os.rename(path, new_path)
                meta["active"] = True
                self.settings["files"][new_name] = self.settings["files"].pop(fname)
            save_settings(self.settings)
            self.refresh_file_list()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to toggle activation:\n{e}")

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
            if not f.lower().endswith(".pak"):
                messagebox.showwarning("Warning", f"Skipping non-.pak file: {f}")
                continue
            new_name = next_data_name(gf)
            shutil.copy2(f, os.path.join(gf, new_name))
            self.settings["files"][new_name] = {"nickname": "", "link": "", "active": True}
        self.refresh_file_list()

    # ---- Merge queue
    def on_add_merge_files(self):
        paths = filedialog.askopenfilenames(
            title="Select Mod Archives (.pak/.zip)", filetypes=[("Mod archives", "*.pak *.zip"), ("All files", "*.*")]
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

    def on_drop_merge(self, event):
        files = parse_drop_files(event.data)
        for f in files:
            if os.path.isfile(f) and f.lower().endswith((".pak", ".zip")):
                self.merge_list.insert(tk.END, f)
        self.persist_merge_queue()

    def persist_merge_queue(self):
        self.settings["merge_queue"] = list(self.merge_list.get(0, tk.END))
        save_settings(self.settings)

    # ---- Merge action
    def on_run_merge(self):
        gf = self.settings.get("game_folder", "")
        if not gf or not os.path.isdir(gf):
            messagebox.showerror("No game folder set", "Set the game folder above first.")
            return
        queue_paths = list(self.merge_list.get(0, tk.END))
        if not queue_paths:
            messagebox.showinfo("Empty Queue", "Add .pak/.zip files to the merge queue first.")
            return

        self.root.config(cursor="watch")
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
            finally:
                def reenable():
                    self.root.config(cursor="")
                    for w in (self.game_entry, self.file_list, self.merge_list):
                        w.config(state="normal")
                    self.refresh_file_list()

                self.root.after(0, reenable)

        threading.Thread(target=worker, daemon=True).start()


def main():
    root = TkinterDnD.Tk() if TKDND_OK else tk.Tk()  # type: ignore
    app = App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
