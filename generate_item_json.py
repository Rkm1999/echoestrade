import os
import json

def generate_json_from_directory(root_dir, output_file):
    """
    Generates a JSON file representing the directory structure and item files
    within root_dir.
    """
    data_structure = {}
    root_dir_abs = os.path.abspath(root_dir)

    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Skip the root directory itself if it's the initial call,
        # or process appropriately if nested.
        if os.path.abspath(dirpath) == root_dir_abs:
            current_level_dict = data_structure
        else:
            # Determine path relative to root_dir for dictionary keys
            relative_path = os.path.relpath(dirpath, root_dir_abs)
            path_parts = relative_path.split(os.sep)
            
            current_level_dict = data_structure
            for part in path_parts[:-1]: # Navigate to the parent dictionary
                current_level_dict = current_level_dict.setdefault(part, {})
            
            # The last part of path_parts is the current directory name,
            # which might be an item or a category.
            # We handle item creation when a relevant file is found.
            # Ensure the current directory exists in the structure
            current_level_dict = current_level_dict.setdefault(path_parts[-1], {})


        for filename in filenames:
            if filename.endswith("_history.csv"):
                # The directory containing the history file is the item name
                item_name = os.path.basename(dirpath)
                # The path to be stored is relative to the original root_dir parameter
                file_relative_path = os.path.join(os.path.relpath(dirpath, os.path.dirname(root_dir_abs)), filename)
                
                # Need to correctly place the item path in the structure.
                # If dirpath is 'item_histories/Category/SubCategory/ItemName',
                # path_parts would be ['Category', 'SubCategory', 'ItemName']
                # The item itself should be a key in its parent's dictionary.
                
                temp_dict = data_structure
                relative_to_item_histories = os.path.relpath(dirpath, root_dir_abs)
                structure_parts = relative_to_item_histories.split(os.sep)

                # Navigate/create path down to the parent of the item
                for part in structure_parts[:-1]:
                    temp_dict = temp_dict.setdefault(part, {})
                
                # Set the item name and its path
                temp_dict[structure_parts[-1]] = file_relative_path
                # Since an item is found, we don't want it to also be a container for other dicts
                # unless other subdirectories exist (which os.walk will handle later if they do)

    with open(output_file, 'w') as f:
        json.dump(data_structure, f, indent=4)

if __name__ == "__main__":
    root_directory = "item_histories"
    output_json_file = "item_data.json"
    generate_json_from_directory(root_directory, output_json_file)
    print(f"'{output_json_file}' generated successfully.")
