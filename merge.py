import os
import sys
import shutil
import tempfile
import zipfile
import re
from collections import defaultdict
from pathlib import Path

class ScriptFile:
    def __init__(self, full_path_in_pak: str, content: str, source_pak: str):
        self.full_path_in_pak = full_path_in_pak
        self.content = content
        self.source_pak = source_pak

final_file_contents = {}

def main():
    print("Unleash The Mods - Mod Merge Utility")
    print("By MetalHeadbang a.k.a @unsc.odst\n")

    base_directory = Path(__file__).parent.resolve()
    source_directory = base_directory / "source"
    mods_directory = base_directory / "mods"
    staging_directory = base_directory / "staging_area"
    fixed_mods_directory = base_directory / "fixed_mods"

    if not source_directory.is_dir() or not mods_directory.is_dir():
        print("\nERROR: 'source' and 'mods' folders not found!")
        input("Press Enter to exit...")
        return

    print("'source' and 'mods' folders found.\n")

    game_pak_path = source_directory / "data0.pak"
    if not game_pak_path.is_file():
        print("\nERROR: 'data0.pak' not found in source folder!")
        input("Press Enter to exit...")
        return

    fixed_mods_directory.mkdir(exist_ok=True)

    valid_mods = fix_mod_structures(game_pak_path, mods_directory, fixed_mods_directory)

    source_paks = list(source_directory.glob("*.pak"))
    original_scripts = load_scripts_from_pak_files(source_paks)
    print(f"{len(original_scripts)} scripts loaded from original game packages.")

    modded_scripts = load_all_scripts_from_mods_folder(valid_mods)
    print(f"{len(modded_scripts)} scripts loaded from the mods folder.\n")

    print("--- Merging Initializing ---")
    mod_file_groups = defaultdict(list)
    for scr in modded_scripts:
        mod_file_groups[scr.full_path_in_pak].append(scr)

    for file_path, mods_touching_this_file in mod_file_groups.items():
        if len(mods_touching_this_file) == 1:
            final_file_contents[file_path] = mods_touching_this_file[0].content
            continue

        original_file = next((f for f in original_scripts if f.full_path_in_pak.lower() == file_path.lower()), None)
        if original_file is None:
            final_file_contents[file_path] = mods_touching_this_file[0].content
            continue

        merged_content = generate_merged_file_content(original_file, mods_touching_this_file)
        final_file_contents[file_path] = merged_content

    print("\n\n--- Merge Completed ---")
    print(f"{len(final_file_contents)} modded files are ready to be packaged.")
    print("\n--- Creating .pak File ---")

    if staging_directory.exists():
        shutil.rmtree(staging_directory)
    staging_directory.mkdir()

    for file_entry, content in final_file_contents.items():
        full_path = staging_directory / Path(file_entry)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(content)

    print(f"{len(final_file_contents)} files packing")

    existing_paks = list(source_directory.glob("data*.pak"))
    next_pak_num = 0
    for pak in existing_paks:
        name = pak.stem  # e.g. data3
        if name.startswith("data"):
            try:
                num = int(name[4:])
                if num >= next_pak_num:
                    next_pak_num = num + 1
            except ValueError:
                pass

    if next_pak_num < 3:
        next_pak_num = 3

    final_pak_path = source_directory / f"data{next_pak_num}.pak"
    if final_pak_path.exists():
        final_pak_path.unlink()

    shutil.make_archive(str(final_pak_path.with_suffix('')), 'zip', staging_directory)
    # Rename to .pak
    final_pak_path_zip = final_pak_path.with_suffix('.zip')
    if final_pak_path_zip.exists():
        final_pak_path_zip.rename(final_pak_path)

    print(f"\nSUCCESS! All mods have been merged and saved as '{final_pak_path.name}' in the game's source folder!")

    shutil.rmtree(staging_directory)

    input("\nPress Enter to exit...")

def fix_mod_structures(game_pak_path, mods_directory, fixed_mods_directory):
    file_structure = get_game_file_structure(game_pak_path)
    valid_mods = []
    supported_extensions = {".pak", ".zip"}

    mod_files = [f for f in mods_directory.iterdir() if f.suffix.lower() in supported_extensions]

    for mod_file in mod_files:
        needs_fixing = False
        mod_name = mod_file.stem
        temp_dir = Path(tempfile.mkdtemp())
        fixed_pak_path = temp_dir / "fixed.pak"

        try:
            mod_scripts = []
            unknown_files = []

            if mod_file.suffix.lower() == ".pak":
                mod_scripts = read_scripts_from_single_pak_for_fixing(mod_file, needs_fixing, file_structure, unknown_files)
            else:
                with zipfile.ZipFile(mod_file, 'r') as archive:
                    for entry in archive.namelist():
                        if entry.endswith(".pak"):
                            with archive.open(entry) as pak_entry_stream:
                                from io import BytesIO
                                mem_stream = BytesIO(pak_entry_stream.read())
                                mod_scripts.extend(read_scripts_from_single_pak_for_fixing(mem_stream, needs_fixing, file_structure, unknown_files))

            if unknown_files:
                print(f"\nWARNING: Mod '{mod_name}' contains files not found in data0.pak:")
                for uf in unknown_files:
                    print(f" - {uf}")
                choice = input("Options: (1) Keep original structure, (2) Exclude this mod.\nPlease select an option (1 or 2): ").strip().lower()
                if choice != "1":
                    print(f"Mod '{mod_name}' excluded due to unknown files.")
                    continue
                else:
                    print(f"Mod '{mod_name}' will be used with its original structure.")
                    valid_mods.append(mod_file)
                    continue

            if needs_fixing:
                with zipfile.ZipFile(fixed_pak_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                    for file_name, correct_path, content_stream in mod_scripts:
                        content_stream.seek(0)
                        zf.writestr(correct_path.replace("\\", "/"), content_stream.read())
                        content_stream.close()

                fixed_zip_path = fixed_mods_directory / f"{mod_name}_fixed.zip"
                with zipfile.ZipFile(fixed_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(fixed_pak_path, "mod.pak")
                valid_mods.append(fixed_zip_path)
                print(f"Mod '{mod_name}' fixed and saved as '{fixed_zip_path.name}'.")
            else:
                valid_mods.append(mod_file)
                print(f"Mod '{mod_name}' already has correct folder structure.")
        except Exception as e:
            print(f"ERROR: Could not process mod '{mod_name}'. Reason: {e}")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    return valid_mods

def get_game_file_structure(game_pak_path):
    structure = {}
    with zipfile.ZipFile(game_pak_path, 'r') as archive:
        for entry in archive.namelist():
            if entry.endswith(".scr") and not entry.endswith("/"):
                file_name = os.path.basename(entry)
                full_path = entry.replace("/", "\\")
                if file_name.lower() not in (k.lower() for k in structure.keys()):
                    structure[file_name] = full_path
                else:
                    print(f"Warning: Duplicate file '{file_name}' found at '{full_path}' in data0.pak.")
    return structure

def read_scripts_from_single_pak_for_fixing(pak_path_or_stream, needs_fixing_flag, file_structure, unknown_files):
    if isinstance(pak_path_or_stream, (str, Path)):
        archive = zipfile.ZipFile(pak_path_or_stream, 'r')
    else:
        pak_path_or_stream.seek(0)
        archive = zipfile.ZipFile(pak_path_or_stream, 'r')

    mod_scripts = []
    try:
        for entry in archive.namelist():
            if entry.endswith(".scr") and not entry.endswith("/"):
                file_name = os.path.basename(entry)
                mod_path = entry.replace("/", "\\")
                correct_path = file_structure.get(file_name)
                if correct_path:
                    if mod_path.lower() != correct_path.lower():
                        needs_fixing_flag = True
                    content = archive.read(entry)
                    from io import BytesIO
                    memory_stream = BytesIO(content)
                    mod_scripts.append((file_name, correct_path, memory_stream))
                else:
                    unknown_files.append(mod_path)
    finally:
        archive.close()
    return mod_scripts

def try_parse_key(line: str):
    line = line.strip()
    match = re.match(r'^(\w+)\s*\(\s*"([^"]+)"', line)
    if match:
        function_name = match.group(1)
        first_param = match.group(2)
        return f"{function_name}_{first_param}"
    return None

def generate_merged_file_content(original: ScriptFile, mods: list):
    original_map = {}
    for line in original.content.replace("\r\n", "\n").split("\n"):
        key = try_parse_key(line)
        if key and key not in original_map:
            original_map[key] = line

    mod_maps = []
    for mod in mods:
        mod_map = {}
        for line in mod.content.replace("\r\n", "\n").split("\n"):
            key = try_parse_key(line)
            if key and key not in mod_map:
                mod_map[key] = line
        mod_maps.append({"source_pak": mod.source_pak, "map": mod_map})

    final_content = []
    resolutions = {}
    preferred_mod_source = None
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

        actual_changes = []
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
            distinct_changes = {}
            for line, src in actual_changes:
                distinct_changes.setdefault(line, []).append(src)

            if len(distinct_changes) == 1:
                line = next(iter(distinct_changes))
                final_content.append(line)
                resolutions[key] = line
            else:
                if preferred_mod_source:
                    chosen_line = next((line for line, sources in distinct_changes.items() if preferred_mod_source in sources), next(iter(distinct_changes)))
                    auto_resolved_count += 1
                else:
                    print(f"\n[CHOICE REQUIRED] Conflict in '{original.full_path_in_pak}'!")
                    print(f" -> Conflict for key '{key}':")
                    lines_list = list(distinct_changes.items())
                    for idx, (line, sources) in enumerate(lines_list):
                        print(f" {idx+1}. ({', '.join(sources)}): {line.strip()}")
                    print("To prefer a mod for all conflicts in this file, add 'y' to your choice (e.g., '1y') (CAREFUL! THIS IS FOR ADVANCED USERS).")

                    choice = -1
                    chosen_source = None
                    while choice < 1 or choice > len(lines_list):
                        raw_input = input(f"Please select the version to use (1-{len(lines_list)}): ").lower()
                        if raw_input.endswith("y") and raw_input[:-1].isdigit():
                            choice = int(raw_input[:-1])
                            if 1 <= choice <= len(lines_list):
                                chosen_source = lines_list[choice-1][1][0]
                        elif raw_input.isdigit():
                            choice = int(raw_input)

                    chosen_line = lines_list[choice-1][0]
                    if chosen_source:
                        preferred_mod_source = chosen_source
                    print(" -> Choice applied.")

                final_content.append(chosen_line)
                resolutions[key] = chosen_line

    if auto_resolved_count > 0:
        print(f"-> {auto_resolved_count} other conflicts in this file merged using your preference: '{preferred_mod_source}'.\n")

    return "\n".join(final_content)

def load_all_scripts_from_mods_folder(mod_files):
    all_scripts = []
    for mod_file_path in mod_files:
        try:
            source_name = os.path.basename(mod_file_path)
            if mod_file_path.suffix.lower() == ".pak":
                all_scripts.extend(read_scripts_from_single_pak(mod_file_path, source_name))
            elif mod_file_path.suffix.lower() == ".zip":
                with zipfile.ZipFile(mod_file_path, 'r') as archive:
                    for entry in archive.namelist():
                        if entry.endswith(".pak"):
                            with archive.open(entry) as pak_entry_stream:
                                from io import BytesIO
                                mem_stream = BytesIO(pak_entry_stream.read())
                                all_scripts.extend(read_scripts_from_single_pak(mem_stream, source_name))
            else:
                print(f"Unsupported mod archive type: {mod_file_path}")
        except Exception as e:
            print(f"ERROR: Could not read '{os.path.basename(mod_file_path)}'. Reason: {e}")
    return all_scripts

def read_scripts_from_single_pak(pak_path_or_stream, source_name):
    if isinstance(pak_path_or_stream, (str, Path)):
        archive = zipfile.ZipFile(pak_path_or_stream, 'r')
    else:
        pak_path_or_stream.seek(0)
        archive = zipfile.ZipFile(pak_path_or_stream, 'r')

    scripts = []
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
                            content = data.decode("utf-8", errors='replace')
                    scripts.append(ScriptFile(entry.replace("\\", "/"), content, source_name))
    finally:
        archive.close()
    return scripts

def load_scripts_from_pak_files(pak_file_paths):
    all_scripts = []
    for path in pak_file_paths:
        all_scripts.extend(read_scripts_from_single_pak(path, os.path.basename(path)))
    return all_scripts

if __name__ == "__main__":
    main()
