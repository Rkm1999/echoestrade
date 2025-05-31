import os
import json
import csv
import re

# Ensure this is at the top-level of the script for generate_item_json.py
def sanitize_for_path(name_str):
    if not name_str:
        name_str = "unknown"
    # Ensure name_str is a string before replace, in case of non-string types like None
    name_str = str(name_str).replace(' ', '_')
    name_str = re.sub(r'[^\w\-_.]', '', name_str) # Allow dots for filenames
    return name_str[:100]

def generate_json_from_directory(root_dir, output_file, item_lists_csv_path="item_lists.csv"):
    """
    Generates a JSON file representing the directory structure and item files
    within root_dir, including paths to icons.
    """
    data_structure = {}
    item_details_map = {}
    root_dir_abs = os.path.abspath(root_dir)
    # output_base_dir is the directory containing root_dir (e.g., project root)
    output_base_dir = os.path.dirname(root_dir_abs)

    try:
        with open(item_lists_csv_path, mode='r', encoding='utf-8', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                s_category = sanitize_for_path(row.get('category_name', ''))
                s_group = sanitize_for_path(row.get('group_name', ''))
                s_name = sanitize_for_path(row.get('name', ''))
                map_key = (s_category, s_group, s_name)
                item_details_map[map_key] = {
                    'icon_id': row.get('icon_id', ''),
                    'icon_downloaded': row.get('icon_downloaded', 'False')
                }
        print(f"Successfully loaded item details from {item_lists_csv_path}")
    except FileNotFoundError:
        print(f"Error: '{item_lists_csv_path}' not found. Icon paths will be missing.")
    except Exception as e:
        print(f"Error reading '{item_lists_csv_path}': {e}. Icon paths will be missing.")


    for dirpath, dirnames, filenames in os.walk(root_dir_abs):
        # Filter out hidden directories and files if necessary (e.g., .git)
        dirnames[:] = [d for d in dirnames if not d.startswith('.')]
        filenames[:] = [f for f in filenames if not f.startswith('.')]

        for filename in filenames:
            if filename.endswith("_history.csv"):
                history_file_disk_path = os.path.join(dirpath, filename)

                # Path relative to the directory containing root_dir (e.g., project root)
                history_path_in_json = os.path.relpath(history_file_disk_path, output_base_dir).replace(os.sep, '/')

                icon_path_in_json = "" # Default

                # Construct key to look up icon_id
                # Relative path from root_dir_abs to current dirpath (e.g., Category/Group/ItemName)
                relative_item_dir_to_root_dir = os.path.relpath(dirpath, root_dir_abs)
                
                # These parts should match how map_key was created (sanitized category, group, name)
                structure_parts_for_lookup = tuple(relative_item_dir_to_root_dir.split(os.sep))
                
                item_detail = item_details_map.get(structure_parts_for_lookup)

                if item_detail and item_detail.get('icon_id') and item_detail.get('icon_downloaded') == 'True':
                    icon_id_str = str(item_detail['icon_id'])
                    icon_filename_sanitized = sanitize_for_path(icon_id_str) + ".png" # Sanitizing icon_id just in case
                    icon_file_disk_path = os.path.join(dirpath, icon_filename_sanitized)

                    if os.path.exists(icon_file_disk_path):
                        icon_path_in_json = os.path.relpath(icon_file_disk_path, output_base_dir).replace(os.sep, '/')
                    else:
                        print(f"Icon file not found at {icon_file_disk_path} though CSV indicated downloaded.")
                
                item_data_entry = {
                    "history_path": history_path_in_json,
                    "icon_path": icon_path_in_json
                }

                # Place item_data_entry into the main data_structure
                # structure_parts_for_lookup is (sanitized_category, sanitized_group, sanitized_name)
                current_level_dict = data_structure
                for part in structure_parts_for_lookup[:-1]: # Navigate to parent dict
                    current_level_dict = current_level_dict.setdefault(part, {})

                item_name_key = structure_parts_for_lookup[-1] # Sanitized item name
                current_level_dict[item_name_key] = item_data_entry

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data_structure, f, indent=4)

if __name__ == "__main__":
    root_directory = "item_histories"  # This is the directory to scan
    output_json_file = "item_data.json"
    # Assuming item_lists.csv is in the same directory as this script,
    # or one level up if this script is in a 'scripts' folder relative to item_lists.csv
    # For now, direct reference means it's expected to be in the CWD when script is run.
    # If generate_item_json.py is at project root with item_lists.csv, then "item_lists.csv" is fine.
    csv_path = "item_lists.csv"
    generate_json_from_directory(root_directory, output_json_file, item_lists_csv_path=csv_path)
<<<<<<< HEAD
    print(f"'{output_json_file}' generated successfully.")
=======
    print(f"'{output_json_file}' generated successfully.")
>>>>>>> 2cffec4941aa9c908b826424919f181950243de0
