import requests
import time
import os
import csv
import re
from datetime import datetime, timezone # Ensure timezone is imported

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
    print(f"\nStarting to process icons for {len(all_items_data)} items...")
    icons_found_locally = 0
    icons_downloaded_successfully = 0
    icons_skipped_no_info = 0
    icons_failed_download = 0

    for item_data in all_items_data:
        item_id_str = item_data.get('id', 'Unknown ID') # For logging
        item_name_str = item_data.get('name', 'Unknown Name') # For logging

        icon_url = item_data.get('icon_url')
        icon_id_val = item_data.get('icon_id')
        category_name = item_data.get('category_name', 'UnknownCategory')
        group_name = item_data.get('group_name', 'UnknownGroup')
        name = item_data.get('name', f'UnknownItem_{item_id_str}') # For path

        # Construct paths first
        item_dir = os.path.join(HISTORIES_BASE_DIR, sanitize_for_path(category_name), sanitize_for_path(group_name), sanitize_for_path(name))
        # Icon ID must be valid for filename
        local_icon_path = ""
        if icon_id_val: # Ensure icon_id_val is not empty before sanitizing for path
            sanitized_icon_id = sanitize_for_path(str(icon_id_val))
            if sanitized_icon_id: # Ensure sanitized_icon_id is not empty
                 local_icon_path = os.path.join(item_dir, f"{sanitized_icon_id}.png")

        # Primary Check: If local icon path is valid and file exists
        if local_icon_path and os.path.exists(local_icon_path):
            if item_data.get('icon_downloaded') != 'True':
                print(f"Icon for {item_id_str} ('{item_name_str}') found locally at {local_icon_path}. Updating flag.")
            item_data['icon_downloaded'] = 'True'
            icons_found_locally += 1
            continue

        # If local icon does not exist, proceed to download attempt
        os.makedirs(item_dir, exist_ok=True) # Ensure directory exists before download attempt if icon wasn't local

        if not icon_url or not icon_id_val or not local_icon_path: # Check all necessary components for download
            # print(f"Skipping icon download for item {item_id_str} ('{item_name_str}') due to missing icon_url, icon_id, or invalid path.")
            item_data['icon_downloaded'] = 'False'
            icons_skipped_no_info += 1
            continue

        print(f"Downloading icon for item {item_id_str} ('{item_name_str}') from {icon_url} to {local_icon_path}")
        download_attempted = False
        try:
            download_attempted = True
            response = requests.get(icon_url, stream=True, timeout=30)
            if response.status_code == 200 and response.content: # Ensure content is not empty
                with open(local_icon_path, 'wb') as f:
                    f.write(response.content)
                item_data['icon_downloaded'] = 'True'
                icons_downloaded_successfully += 1
                # print(f"Successfully downloaded icon for item {item_id_str} ('{item_name_str}').")
            else:
                print(f"Error downloading icon for {item_id_str} ('{item_name_str}'): Status {response.status_code}, Content-Length {response.headers.get('Content-Length', 'N/A')}")
                item_data['icon_downloaded'] = 'False'
                icons_failed_download += 1

        except requests.exceptions.RequestException as e:
            print(f"Request failed for icon download {item_id_str} ('{item_name_str}'): {e}")
            item_data['icon_downloaded'] = 'False'
            icons_failed_download += 1
        except IOError as e:
            print(f"IOError saving icon for item {item_id_str} ('{item_name_str}'): {e}")
            item_data['icon_downloaded'] = 'False'
            icons_failed_download += 1

        if download_attempted: # Only sleep if an actual download attempt was made
            time.sleep(REQUEST_DELAY_SECONDS)

    print("\n--- Icon Download Summary ---")
    print(f"Icons found locally (flag updated if needed): {icons_found_locally}")
    print(f"Icons downloaded successfully: {icons_downloaded_successfully}")
    print(f"Skipped (missing URL/ID or invalid path): {icons_skipped_no_info}")
    print(f"Failed to download/save: {icons_failed_download}")
    print("-----------------------------")
    return all_items_data

API_V2_ITEM_PRICES_URL = "https://echoes.mobi/api/v2/item_prices"

def load_all_current_prices() -> dict:
    """
    Fetches all current item prices from the v2 API endpoint.
    Returns a dictionary mapping item_id to its price data.
    """
    print(f"\nFetching all current item prices from {API_V2_ITEM_PRICES_URL}...")
    current_prices_map = {}
    headers = {'accept': 'text/csv'}

    try:
        response = requests.get(API_V2_ITEM_PRICES_URL, headers=headers, timeout=30)
        if response.status_code == 200:
            response_text_stripped = response.text.strip()
            if not response_text_stripped:
                print("API response for current prices was empty.")
                return current_prices_map

            reader = csv.DictReader(response_text_stripped.splitlines())
            # Expected headers: id,name,estimated_price,date_updated,category_name,group_name,icon_id
            for row in reader:
                item_id = row.get('id')
                if item_id:
                    current_prices_map[item_id] = {
                        'estimated_price': row.get('estimated_price'),
                        'date_updated': row.get('date_updated')
                    }
            print(f"Successfully loaded {len(current_prices_map)} current item prices.")
        else:
            print(f"Error fetching current prices: Status code {response.status_code}")
            print(f"Response content: {response.text[:200]}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed for current prices: {e}")
    except csv.Error as e:
        print(f"CSV parsing error for current prices: {e}. Response text: {response.text[:500]}")
    except Exception as e:
        print(f"An unexpected error occurred while loading current prices: {e}")

    return current_prices_map

def get_week_year_from_isodate(iso_date_string: str) -> tuple[str, str]:
    """
    Parses an ISO 8601 date string and returns its ISO week number and ISO year.
    Handles timezone information like +00:00.
    """
    try:
        if not isinstance(iso_date_string, str):
            # Handle cases where date might be None or not a string
            if iso_date_string is None:
                raise ValueError("Input date string is None")
            raise ValueError(f"Input must be a string, got {type(iso_date_string)}")

        # datetime.fromisoformat handles "YYYY-MM-DDTHH:MM:SS+00:00" directly
        # and correctly interprets the +00:00 as UTC.
        dt_object = datetime.fromisoformat(iso_date_string)

        # isocalendar() returns (ISO year, ISO week number, ISO weekday)
        iso_year, iso_week, _ = dt_object.isocalendar()

        # Format week number with leading zero if needed (e.g., "01", "02", ..., "52")
        return (f"{iso_week:02d}", str(iso_year))
    except ValueError as e:
        print(f"Error parsing date string '{iso_date_string}': {e}. Returning default week/year ('00', '0000').")
        return ("00", "0000") # Fallback values
    except Exception as e: # Catch any other unexpected errors
        print(f"An unexpected error occurred while parsing date '{iso_date_string}': {e}. Returning default week/year ('00', '0000').")
        return ("00", "0000")

HISTORY_CSV_HEADERS = ['id', 'item_id', 'price', 'week', 'year', 'date_created', 'date_updated']

def fetch_and_save_histories(item_ids_to_update: list[str], all_current_prices_map: dict):
    """
    Fetches full item history or appends latest price for specified item IDs.
    - item_ids_to_update: List of item IDs whose history needs to be processed.
    - all_current_prices_map: Dictionary with current price data from /v2/item_prices.
    """
    print(f"\nStarting to process histories for {len(item_ids_to_update)} items...")
    if not item_ids_to_update:
        print("No items require history updates.")
        return

    all_items_details_for_paths = {} # To store category/group/name for path creation
    if os.path.exists(ITEMS_OUTPUT_CSV_FILE):
        try:
            with open(ITEMS_OUTPUT_CSV_FILE, mode='r', encoding='utf-8', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                required_path_cols = ['id', 'category_name', 'group_name', 'name']
                if not all(col in reader.fieldnames for col in required_path_cols):
                    missing_cols = [col for col in required_path_cols if col not in reader.fieldnames]
                    print(f"Error: {ITEMS_OUTPUT_CSV_FILE} is missing required columns for path creation: {', '.join(missing_cols)}.")
                    return
                for row in reader:
                    if row.get('id') in item_ids_to_update:
                        all_items_details_for_paths[row['id']] = row
            if not all_items_details_for_paths and item_ids_to_update:
                print(f"Warning: No details found in {ITEMS_OUTPUT_CSV_FILE} for the item IDs to update.")
        except Exception as e:
            print(f"Error reading {ITEMS_OUTPUT_CSV_FILE} for path details: {e}")
            return # Cannot proceed without path details

    os.makedirs(HISTORIES_BASE_DIR, exist_ok=True)
    headers = {'accept': 'text/csv'}

    new_histories_fetched_count = 0
    histories_appended_count = 0
    histories_failed_append_count = 0
    histories_skipped_no_data_from_api = 0 # For full fetch
    histories_failed_fetch_count = 0 # For full fetch
    histories_skipped_no_detail_for_path = 0
    histories_skipped_already_latest = 0

    for item_id in item_ids_to_update:
        item_detail_for_path = all_items_details_for_paths.get(item_id)

        if not item_detail_for_path:
            print(f"Skipping history for item ID {item_id}: Path details not found in {ITEMS_OUTPUT_CSV_FILE}.")
            histories_skipped_no_detail_for_path += 1
            continue

        category = item_detail_for_path.get('category_name', 'UnknownCategory')
        group = item_detail_for_path.get('group_name', 'UnknownGroup')
        name = item_detail_for_path.get('name', f'UnknownItem_{item_id}')

        sanitized_item_id_for_filename = sanitize_for_path(item_id)
        sanitized_category = sanitize_for_path(category)
        sanitized_group = sanitize_for_path(group)
        sanitized_name = sanitize_for_path(name)

        item_dir = os.path.join(HISTORIES_BASE_DIR, sanitized_category, sanitized_group, sanitized_name)
        os.makedirs(item_dir, exist_ok=True)
        history_file_path = os.path.join(item_dir, f"{sanitized_item_id_for_filename}_history.csv")

        if os.path.exists(history_file_path):
            current_price_data = all_current_prices_map.get(item_id)
            if current_price_data and current_price_data.get('estimated_price') is not None and current_price_data.get('estimated_price') != '':
                estimated_price = current_price_data['estimated_price']
                api_date_updated = current_price_data['date_updated']

                last_row_id_int = 0
                last_date_updated_in_file = None
                try:
                    with open(history_file_path, mode='r', encoding='utf-8', newline='') as hf_read:
                        reader = csv.DictReader(hf_read)
                        history_rows = list(reader)
                        if history_rows:
                            last_row = history_rows[-1]
                            last_date_updated_in_file = last_row.get('date_updated')
                            try:
                                last_row_id_int = int(last_row.get('id', 0))
                            except ValueError:
                                print(f"Warning: Could not parse last row ID for {item_id} in {history_file_path}. Defaulting to 0.")
                                last_row_id_int = 0 # Or len(history_rows) if IDs are sequential and 1-based
                except Exception as e:
                    print(f"Error reading existing history for {item_id} from {history_file_path}: {e}. Attempting full fetch as fallback.")
                    # Fallback to full fetch logic below this if-block by clearing history_file_path or similar
                    # For now, we'll let it try to fetch full history if append preparation fails badly.
                    # This path makes it try a full fetch:
                    history_file_path = None # Mark as non-existent to trigger full fetch logic

                if history_file_path and api_date_updated == last_date_updated_in_file:
                    print(f"Latest price for item {item_id} (date: {api_date_updated}) already in history. Skipping append.")
                    histories_skipped_already_latest +=1
                    continue

                if history_file_path: # Proceed with append if not skipped
                    new_unique_row_id = last_row_id_int + 1
                    derived_week, derived_year = get_week_year_from_isodate(api_date_updated)
                    new_row_dict = {
                        'id': str(new_unique_row_id),
                        'item_id': str(item_id),
                        'price': str(estimated_price),
                        'week': str(derived_week),
                        'year': str(derived_year),
                        'date_created': api_date_updated,
                        'date_updated': api_date_updated
                    }
                    try:
                        with open(history_file_path, mode='a', encoding='utf-8', newline='') as hf_append:
                            writer = csv.DictWriter(hf_append, fieldnames=HISTORY_CSV_HEADERS)
                            # Header is not written when appending
                            writer.writerow(new_row_dict)
                        print(f"Appended latest price for item {item_id} to {history_file_path}")
                        histories_appended_count += 1
                    except Exception as e:
                        print(f"Error appending to history for {item_id} at {history_file_path}: {e}")
                        histories_failed_append_count += 1
                    continue # Move to next item_id after append attempt
            else: # Item not in v2 prices or price is empty
                print(f"Could not find current price in v2 API for item {item_id} to append. Skipping append for this item.")
                histories_failed_append_count += 1
                continue # Skip to next item_id
        
        # This block executes if history_file_path does not exist OR was set to None due to read error
        if not os.path.exists(history_file_path) or history_file_path is None :
            history_api_url = f"{API_BASE_URL_HISTORY}{item_id}"
            print(f"Fetching full history for new item ID {item_id} ('{name}')...")
            try:
                response = requests.get(history_api_url, headers=headers, timeout=30)
                if response.status_code == 200:
                    response_text_stripped = response.text.strip()
                    history_lines = response_text_stripped.splitlines()
                    if response_text_stripped and len(history_lines) > 1:
                        # Reconstruct full path if it was None-d due to read error
                        current_history_file_path = os.path.join(item_dir, f"{sanitized_item_id_for_filename}_history.csv")
                        with open(current_history_file_path, 'w', encoding='utf-8', newline='') as hf:
                            hf.write(response_text_stripped + '\n')
                        print(f"Successfully saved new history for item {item_id} to {current_history_file_path}")
                        new_histories_fetched_count += 1
                    else:
                        print(f"No actual history data (or only header) for new item {item_id} ('{name}'). Skipping file write.")
                        histories_skipped_no_data_from_api += 1
                else:
                    print(f"Error fetching full history for {item_id} ('{name}'): Status {response.status_code}")
                    histories_failed_fetch_count += 1
            except requests.exceptions.RequestException as e:
                print(f"Request failed for full history {item_id} ('{name}'): {e}")
                histories_failed_fetch_count += 1
            time.sleep(REQUEST_DELAY_SECONDS) # Delay only for full fetches

    print("\n--- Item History Processing Summary ---")
    print(f"New full histories fetched: {new_histories_fetched_count}")
    print(f"Appended latest price to existing histories: {histories_appended_count}")
    print(f"Skipped (already latest price in history): {histories_skipped_already_latest}")
    print(f"Skipped (path details not found): {histories_skipped_no_detail_for_path}")
    print(f"Skipped (no actual data from API for full fetch): {histories_skipped_no_data_from_api}")
    print(f"Failed (full fetch API error or request exception): {histories_failed_fetch_count}")
    print(f"Failed (append operation due to missing v2 price or file write error): {histories_failed_append_count}")
    print("---------------------------------------")

if __name__ == "__main__":
    items_to_update_history_for, all_items_data_list = fetch_and_save_items()

    if all_items_data_list: # Check if there's any data to process
        all_items_data_list = download_item_icons(all_items_data_list) # Update icon_downloaded flags

        current_prices = load_all_current_prices()

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
            fetch_and_save_histories(items_to_update_history_for, current_prices)
        else:
            print("No items require history updates based on initial fetch.")
    else:
        print("\nSkipping icon downloading and history fetching because item list was not created or is empty.")
