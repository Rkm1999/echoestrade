import requests
import time
import os
import csv
import re

# Configuration
API_BASE_URL_ITEMS = "https://echoes.mobi/api/items"
API_BASE_URL_HISTORY = "https://echoes.mobi/api/item_weekly_average_prices?page=1&itemId="
ITEMS_OUTPUT_CSV_FILE = "item_lists.csv"
HISTORIES_BASE_DIR = "item_histories"
REQUEST_DELAY_SECONDS = 0.5
FINAL_CSV_HEADERS = ['id', 'name', 'category_name', 'group_name', 'weekly_average_price', 'icon_id', 'date_created', 'date_updated', 'icon_url', 'icon_downloaded', 'needs_history_update']

def sanitize_for_path(name_str):
    """
    Sanitizes a string to be safe for directory/file names.
    """
    if not name_str:
        name_str = "unknown"
    name_str = name_str.replace(' ', '_')
    name_str = re.sub(r'[^\w\-_]', '', name_str)
    return name_str[:100]

def fetch_and_save_items():
    """
    Fetches item data from the paginated API, merges with existing data, and saves to a CSV file.
    Returns a list of item IDs that need their history updated.
    """
    all_items_data = {} # Keyed by item ID
    items_needing_history_update = []

    # Load existing data from item_lists.csv if it exists
    if os.path.exists(ITEMS_OUTPUT_CSV_FILE):
        print(f"Loading existing data from {ITEMS_OUTPUT_CSV_FILE}...")
        try:
            with open(ITEMS_OUTPUT_CSV_FILE, mode='r', encoding='utf-8', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    item_id = row.get('id')
                    if item_id:
                        # Ensure icon_downloaded exists, default to 'False' if not
                        if 'icon_downloaded' not in row:
                            row['icon_downloaded'] = 'False'
                        # Initialize needs_history_update to 'False' for existing items
                        row['needs_history_update'] = 'False'
                        all_items_data[item_id] = row
                print(f"Loaded {len(all_items_data)} items from CSV.")
        except Exception as e:
            print(f"Error reading {ITEMS_OUTPUT_CSV_FILE}: {e}. Starting with an empty dataset.")
            all_items_data = {} # Reset if error

    current_page = 1
    headers = {'accept': 'text/csv'}
    print("Starting to fetch item data from API...")

    api_data_processed = False

    while True:
        params = {
            'page': current_page,
            'order[name]': 'asc',
            'order[categoryName]': 'asc',
            'order[groupName]': 'asc',
            'exists[weekly_average_price]': 'true' # This filter might be too restrictive if we want all items
        }
        
        try:
            print(f"Fetching API item list page {current_page}...")
            response = requests.get(API_BASE_URL_ITEMS, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                response_text_stripped = response.text.strip()

                if not response_text_stripped:
                    print(f"API Page {current_page} is effectively empty. Assuming no more item data.")
                    break

                reader = csv.DictReader(response_text_stripped.splitlines())
                api_items_on_page = list(reader)

                if not api_items_on_page: # Handles header-only response or empty after parsing
                    print(f"No data items on API page {current_page} (only header or empty). Ending pagination.")
                    break
                
                api_data_processed = True # Mark that we have processed at least one page with data

                for api_item in api_items_on_page:
                    item_id = api_item.get('id')
                    if not item_id:
                        print(f"Skipping API item due to missing ID: {api_item.get('name', 'Unknown Name')}")
                        continue

                    date_updated_api = api_item.get('date_updated')

                    if item_id in all_items_data:
                        # Item exists, check for updates
                        existing_item = all_items_data[item_id]
                        date_updated_local = existing_item.get('date_updated')
                        icon_downloaded_status = existing_item.get('icon_downloaded', 'False') # Preserve existing

                        if date_updated_api and date_updated_local and date_updated_api > date_updated_local:
                            print(f"Updating item {item_id} ('{api_item.get('name')}') as API data is newer.")
                            # Update all fields from API, preserve icon_downloaded
                            for key, value in api_item.items():
                                existing_item[key] = value
                            existing_item['icon_downloaded'] = icon_downloaded_status
                            existing_item['needs_history_update'] = 'True'
                        else:
                            # API data not newer or local/API date missing, keep existing, set history update to False
                            existing_item['needs_history_update'] = 'False'
                        all_items_data[item_id] = existing_item # Ensure reference is updated if dict was copied
                    else:
                        # New item
                        print(f"Adding new item {item_id} ('{api_item.get('name')}').")
                        new_item_entry = {header: '' for header in FINAL_CSV_HEADERS} # Initialize with all headers
                        new_item_entry.update(api_item) # Populate with API data
                        new_item_entry['icon_downloaded'] = 'False'
                        new_item_entry['needs_history_update'] = 'True'
                        all_items_data[item_id] = new_item_entry
            else:
                print(f"Error fetching API item list page {current_page}: Status code {response.status_code}")
                print(f"Response content: {response.text[:200]}")
                break # Stop on error
        
        except requests.exceptions.RequestException as e:
            print(f"Request for API item list failed on page {current_page}: {e}")
            break # Stop on error
        except csv.Error as e:
            print(f"CSV parsing error on API page {current_page}: {e}. Response text: {response.text[:500]}")
            break

        current_page += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    # After loop, ensure items loaded from CSV but not in API have needs_history_update='False'
    # This is implicitly handled by the logic: initial load is 'False', and only API interaction changes it.
    # If an item from CSV was never found in API, its 'needs_history_update' remains 'False'.

    # Write all_items_data to CSV
    print(f"\nWriting {len(all_items_data)} items to {ITEMS_OUTPUT_CSV_FILE}...")
    try:
        with open(ITEMS_OUTPUT_CSV_FILE, mode='w', encoding='utf-8', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FINAL_CSV_HEADERS)
            writer.writeheader()
            for item_id, item_data in all_items_data.items():
                # Ensure all keys in FINAL_CSV_HEADERS are present, default to empty string if missing
                row_to_write = {header: item_data.get(header, '') for header in FINAL_CSV_HEADERS}
                writer.writerow(row_to_write)
        print(f"Successfully wrote items to {ITEMS_OUTPUT_CSV_FILE}.")
    except Exception as e:
        print(f"Error writing to {ITEMS_OUTPUT_CSV_FILE}: {e}")
        # Decide if we should return empty or raise, based on requirements for atomicity
        return [] # Return empty list on write failure

    # Collect IDs for history update
    for item_id, data in all_items_data.items():
        if data.get('needs_history_update') == 'True':
            items_needing_history_update.append(item_id)

    print(f"Found {len(items_needing_history_update)} items needing history update.")
    return items_needing_history_update, list(all_items_data.values())


def download_item_icons(all_items_data: list[dict]) -> list[dict]:
    """
    Downloads icons for items in the all_items_data list.
    Updates the 'icon_downloaded' status in each item's dictionary.
    """
    print(f"\nStarting to download icons for {len(all_items_data)} items...")
    downloaded_count = 0
    skipped_url_id_missing_count = 0
    skipped_already_exists_count = 0
    failed_count = 0

    for item_data in all_items_data:
        item_id = item_data.get('id')
        icon_url = item_data.get('icon_url')
        icon_id_val = item_data.get('icon_id') # Renamed to avoid clash with outer scope id
        category_name = item_data.get('category_name', 'UnknownCategory')
        group_name = item_data.get('group_name', 'UnknownGroup')
        name = item_data.get('name', f'UnknownItem_{item_id}')
        icon_downloaded_status = item_data.get('icon_downloaded', 'False')

        if not icon_url or not icon_id_val:
            # print(f"Skipping icon for item {item_id} ('{name}') due to missing icon_url or icon_id.")
            skipped_url_id_missing_count += 1
            item_data['icon_downloaded'] = 'False' # Ensure it's marked false
            continue

        item_dir = os.path.join(HISTORIES_BASE_DIR, sanitize_for_path(category_name), sanitize_for_path(group_name), sanitize_for_path(name))
        os.makedirs(item_dir, exist_ok=True)

        sanitized_icon_id = sanitize_for_path(icon_id_val)
        icon_filename = f"{sanitized_icon_id}.png"
        icon_path = os.path.join(item_dir, icon_filename)

        if icon_downloaded_status == 'True' and os.path.exists(icon_path):
            # print(f"Icon for item {item_id} ('{name}') already downloaded and exists. Skipping.")
            skipped_already_exists_count += 1
            continue

        print(f"Downloading icon for item {item_id} ('{name}') from {icon_url} to {icon_path}")
        try:
            response = requests.get(icon_url, stream=True, timeout=30)
            if response.status_code == 200:
                with open(icon_path, 'wb') as f:
                    f.write(response.content)
                # print(f"Successfully downloaded icon for item {item_id} ('{name}').")
                item_data['icon_downloaded'] = 'True'
                downloaded_count += 1
            else:
                print(f"Error downloading icon for item {item_id} ('{name}'): Status code {response.status_code}")
                item_data['icon_downloaded'] = 'False'
                failed_count += 1

        except requests.exceptions.RequestException as e:
            print(f"Request failed for icon download {item_id} ('{name}'): {e}")
            item_data['icon_downloaded'] = 'False'
            failed_count += 1
        except IOError as e:
            print(f"IOError saving icon for item {item_id} ('{name}'): {e}")
            item_data['icon_downloaded'] = 'False'
            failed_count += 1

        time.sleep(REQUEST_DELAY_SECONDS)

    print("\n--- Icon Download Summary ---")
    print(f"Successfully downloaded: {downloaded_count}")
    print(f"Skipped (missing URL/ID): {skipped_url_id_missing_count}")
    print(f"Skipped (already exists and marked True): {skipped_already_exists_count}")
    print(f"Failed to download/save: {failed_count}")
    print("-----------------------------")
    return all_items_data


def fetch_and_save_histories(item_ids_to_update: list[str]):
    """
    Fetches item history for specified item IDs and saves it.
    """
    print(f"\nStarting to fetch item histories for {len(item_ids_to_update)} items.")
    if not item_ids_to_update:
        print("No items require history updates.")
        return

    all_items_details = {}
    if os.path.exists(ITEMS_OUTPUT_CSV_FILE):
        try:
            with open(ITEMS_OUTPUT_CSV_FILE, mode='r', encoding='utf-8', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                # Ensure all required columns for path creation are present in the CSV header
                required_path_cols = ['id', 'category_name', 'group_name', 'name']
                if not all(col in reader.fieldnames for col in required_path_cols):
                    missing_cols = [col for col in required_path_cols if col not in reader.fieldnames]
                    print(f"Error: {ITEMS_OUTPUT_CSV_FILE} is missing required columns for path creation: {', '.join(missing_cols)}.")
                    return

                for row in reader:
                    # Only load details for items that are actually in the item_ids_to_update list
                    if row.get('id') in item_ids_to_update:
                        all_items_details[row['id']] = row
            if not all_items_details and item_ids_to_update: # Check if any relevant details were loaded
                print(f"Warning: No details found in {ITEMS_OUTPUT_CSV_FILE} for the provided item IDs to update.")
                # This might happen if ITEMS_OUTPUT_CSV_FILE is somehow empty or IDs don't match.
                # Depending on strictness, could return here.
        except Exception as e:
            print(f"Error reading {ITEMS_OUTPUT_CSV_FILE} for history details: {e}")
            return

    os.makedirs(HISTORIES_BASE_DIR, exist_ok=True)
    headers = {'accept': 'text/csv'}
    histories_fetched_count = 0
    histories_skipped_no_detail_count = 0 # Items in list but details not found in CSV
    histories_skipped_no_data_from_api = 0 # API returned no actual history data
    histories_failed_count = 0 # API request failed

    for item_id in item_ids_to_update:
        item_detail = all_items_details.get(item_id)

        if not item_detail:
            print(f"Skipping history for item ID {item_id}: Details not found in {ITEMS_OUTPUT_CSV_FILE}.")
            histories_skipped_no_detail_count += 1
            continue

        category = item_detail.get('category_name', 'UnknownCategory')
        group = item_detail.get('group_name', 'UnknownGroup')
        name = item_detail.get('name', f'UnknownItem_{item_id}')

        # Sanitize components for path creation
        sanitized_item_id_for_filename = sanitize_for_path(item_id)
        sanitized_category = sanitize_for_path(category)
        sanitized_group = sanitize_for_path(group)
        sanitized_name = sanitize_for_path(name)

        item_dir = os.path.join(HISTORIES_BASE_DIR, sanitized_category, sanitized_group, sanitized_name)
        os.makedirs(item_dir, exist_ok=True)

        history_file_path = os.path.join(item_dir, f"{sanitized_item_id_for_filename}_history.csv")

        history_api_url = f"{API_BASE_URL_HISTORY}{item_id}" # Use original item_id for API call
        print(f"Fetching history for item ID {item_id} ('{name}')...")

        try:
            response = requests.get(history_api_url, headers=headers, timeout=30)
            if response.status_code == 200:
                response_text_stripped = response.text.strip()
                history_lines = response_text_stripped.splitlines()
                if response_text_stripped and len(history_lines) > 1: # Check if there's more than just a header
                    with open(history_file_path, 'w', encoding='utf-8', newline='') as hf:
                        hf.write(response_text_stripped + '\n')
                    print(f"Successfully saved history for item {item_id} to '{history_file_path}'")
                    histories_fetched_count += 1
                else:
                    print(f"No actual history data (or only header) returned for item {item_id} ('{name}'). Skipping file write.")
                    histories_skipped_no_data_from_api += 1
            else:
                print(f"Error fetching history for item {item_id} ('{name}'): Status {response.status_code}")
                histories_failed_count += 1
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed for item history {item_id} ('{name}'): {e}")
            histories_failed_count += 1

        time.sleep(REQUEST_DELAY_SECONDS)

    print("\n--- Item History Fetch Summary ---")
    print(f"Successfully fetched and saved: {histories_fetched_count}")
    print(f"Skipped (details not found in CSV for ID): {histories_skipped_no_detail_count}")
    print(f"Skipped (no actual data from API): {histories_skipped_no_data_from_api}")
    print(f"Failed to fetch (API error or request exception): {histories_failed_count}")
    print("---------------------------------")

if __name__ == "__main__":
    items_to_update_history_for, all_items_data_list = fetch_and_save_items()

    if all_items_data_list: # Check if there's any data to process
        all_items_data_list = download_item_icons(all_items_data_list) # Update icon_downloaded flags

        # Write the potentially updated all_items_data (with new icon_downloaded flags) to CSV
        # FINAL_CSV_HEADERS is defined globally
        print(f"\nWriting final item data for {len(all_items_data_list)} items to {ITEMS_OUTPUT_CSV_FILE}...")
        try:
            with open(ITEMS_OUTPUT_CSV_FILE, 'w', encoding='utf-8', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=FINAL_CSV_HEADERS)
                writer.writeheader()
                for item_dict in all_items_data_list:
                    row_to_write = {header: item_dict.get(header, '') for header in FINAL_CSV_HEADERS}
                    writer.writerow(row_to_write)
            print(f"Final item data including icon status written to {ITEMS_OUTPUT_CSV_FILE}")
        except IOError as e:
            print(f"Error writing final item data to CSV: {e}")
        except Exception as e: # Catch any other unexpected error during write
             print(f"An unexpected error occurred while writing final CSV: {e}")


        if items_to_update_history_for:
            fetch_and_save_histories(items_to_update_history_for)
        else:
            print("No items require history updates based on initial fetch.")
    else:
        print("\nSkipping icon downloading and history fetching because item list was not created or is empty.")
