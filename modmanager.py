import os
import sys

if getattr(sys, 'frozen', False):
    # If running as a PyInstaller bundle
    tkdnd_lib = os.path.join(sys._MEIPASS, 'tkinterdnd2', 'tkdnd')
    os.environ['TKDND_LIBRARY'] = tkdnd_lib

import json
import shutil
import webbrowser
import re
import tkinter as tk
from tkinter import filedialog, simpledialog, messagebox
from tkinterdnd2 import TkinterDnD, DND_FILES

SETTINGS_FILE = "pak_manager_settings.json"
DEFAULT_MOD_DIR = r"C:\Program Files (x86)\Steam\steamapps\common\Dying Light The Beast\ph_ft\source"

def load_settings():
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r") as f:
            return json.load(f)
    return {"mod_folder": "", "files": {}}

def save_settings():
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)

def get_next_data_name(mod_folder):
    existing = [f for f in os.listdir(mod_folder) if f.startswith("data") and (f.endswith(".pak") or f.endswith(".pak.disabled"))]
    used_numbers = set()
    for f in existing:
        num_part = f[4:].split('.')[0]
        if num_part.isdigit():
            used_numbers.add(int(num_part))
    x = 1
    while x in used_numbers:
        x += 1
    return f"data{x}.pak"

def parse_drop_files(event_data):
    pattern = re.compile(r'\{([^}]+)\}|(\S+)')
    matches = pattern.findall(event_data)
    files = []
    for brace_path, simple_path in matches:
        if brace_path:
            files.append(brace_path)
        else:
            files.append(simple_path)
    return files

def refresh_file_list():
    file_listbox.delete(0, tk.END)
    if not os.path.isdir(settings["mod_folder"]):
        return
    files = os.listdir(settings["mod_folder"])
    settings["files"] = {k: v for k, v in settings["files"].items() if k in files}
    inserted_idx = 0
    for fname in sorted(files):
        if fname.startswith("data") and (fname.endswith(".pak") or fname.endswith(".pak.disabled")):
            meta = settings["files"].setdefault(fname, {"nickname": "", "link": "", "active": fname.endswith(".pak")})
            display = fname
            if not meta["active"]:
                display += " [deactivated]"
            if meta["nickname"]:
                display += f" | {meta['nickname']}"
            file_listbox.insert(tk.END, display)
            color = "green" if meta["active"] else "red"
            file_listbox.itemconfig(inserted_idx, {'fg': color})
            inserted_idx += 1
    save_settings()

def get_selected_file():
    sel = file_listbox.curselection()
    if not sel:
        return None
    val = file_listbox.get(sel[0])
    return val.split(" | ")[0].split(" [")[0]

def drop_handler(event):
    mod_folder = settings["mod_folder"]
    if not mod_folder:
        messagebox.showerror("No mod folder set", "Please set the mod folder before adding files.")
        return
    files = parse_drop_files(event.data)
    for f in files:
        # Only accept real .pak file paths; skip archives and others
        if not os.path.isfile(f):
            messagebox.showwarning("Warning", f"Skipping non-file: {f}")
            continue
        if not f.lower().endswith(".pak"):
            messagebox.showwarning("Warning", f"Skipping non-.pak file: {f}")
            continue
        new_name = get_next_data_name(mod_folder)
        shutil.copy2(f, os.path.join(mod_folder, new_name))
        settings["files"][new_name] = {"nickname": "", "link": "", "active": True}
    refresh_file_list()

def select_mod_folder():
    folder = filedialog.askdirectory()
    if folder:
        settings["mod_folder"] = folder
        save_settings()
        refresh_file_list()

def assign_nickname():
    fname = get_selected_file()
    if not fname:
        return
    nick = simpledialog.askstring("Set Nickname", f"Enter nickname for {fname}:", initialvalue=settings["files"][fname].get("nickname", ""))
    if nick is not None:
        settings["files"][fname]["nickname"] = nick
        save_settings()
        refresh_file_list()

def assign_link():
    fname = get_selected_file()
    if not fname:
        return
    link = simpledialog.askstring("Set Link", f"Enter clickable URL for {fname}:", initialvalue=settings["files"][fname].get("link", ""))
    if link is not None:
        settings["files"][fname]["link"] = link
        save_settings()
        refresh_file_list()

def open_link():
    fname = get_selected_file()
    if not fname:
        return
    link = settings["files"].get(fname, {}).get("link")
    if link:
        webbrowser.open(link)

def delete_file():
    fname = get_selected_file()
    if not fname:
        return
    path = os.path.join(settings["mod_folder"], fname)
    if messagebox.askyesno("Delete File", f"Delete file {fname}? This cannot be undone."):
        try:
            os.remove(path)
            settings["files"].pop(fname, None)
            save_settings()
            refresh_file_list()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to delete file:\n{str(e)}")

def toggle_activation():
    fname = get_selected_file()
    if not fname:
        return
    path = os.path.join(settings["mod_folder"], fname)
    meta = settings["files"].get(fname)
    if not meta:
        return
    try:
        if meta["active"]:
            new_path = path + ".disabled"
            os.rename(path, new_path)
            meta["active"] = False
            settings["files"][fname + ".disabled"] = settings["files"].pop(fname)
        else:
            if fname.endswith(".disabled"):
                new_name = fname[:-9]
                new_path = os.path.join(settings["mod_folder"], new_name)
            else:
                new_name = fname
                new_path = path
            os.rename(path, new_path)
            meta["active"] = True
            settings["files"][new_name] = settings["files"].pop(fname)
        save_settings()
        refresh_file_list()
    except Exception as e:
        messagebox.showerror("Error", f"Failed to toggle activation:\n{str(e)}")

# Main
settings = load_settings()
if not settings.get("mod_folder") and os.path.exists(DEFAULT_MOD_DIR):
    settings["mod_folder"] = DEFAULT_MOD_DIR

root = TkinterDnD.Tk()
root.title("Dying Light PAK Mod Manager")
root.geometry("650x450")

tk.Label(root, text="Drag & drop .pak files onto the area below:").pack(pady=8)
drop_area = tk.Label(root, text="➕ Drop .pak files here", relief="ridge", bg="lightcyan", padx=20, pady=30)
drop_area.pack(fill="x", padx=12)
drop_area.drop_target_register(DND_FILES)
drop_area.dnd_bind('<<Drop>>', drop_handler)

tk.Button(root, text="Select Mod Folder", command=select_mod_folder).pack(pady=10)

file_listbox = tk.Listbox(root, width=90)
file_listbox.pack(expand=True, fill="both", padx=12, pady=5)

btn_frame = tk.Frame(root)
btn_frame.pack(pady=6)

tk.Button(btn_frame, text="Set Nickname", command=assign_nickname).pack(side="left", padx=5)
tk.Button(btn_frame, text="Set Link", command=assign_link).pack(side="left", padx=5)
tk.Button(btn_frame, text="Open Link", command=open_link).pack(side="left", padx=5)
tk.Button(btn_frame, text="Activate / Deactivate", command=toggle_activation).pack(side="left", padx=5)
tk.Button(btn_frame, text="Delete File", fg="red", command=delete_file).pack(side="left", padx=5)

refresh_file_list()
root.mainloop()
