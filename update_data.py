import requests
import time
import os
import csv
import re
from datetime import datetime, timezone # Ensure timezone is imported
import sys # Added for safe_print
import cloudflare
import boto3
from botocore.exceptions import ClientError
import json # Already present via csv but good to ensure

# Configuration
CLOUDFLARE_API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN")
CLOUDFLARE_ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
CLOUDFLARE_D1_DATABASE_ID = os.environ.get("CLOUDFLARE_D1_DATABASE_ID")
CLOUDFLARE_R2_BUCKET_NAME = os.environ.get("CLOUDFLARE_R2_BUCKET_NAME")
CLOUDFLARE_R2_S3_ACCESS_KEY_ID = os.environ.get("CLOUDFLARE_R2_S3_ACCESS_KEY_ID")
CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY = os.environ.get("CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY")
CLOUDFLARE_R2_S3_ENDPOINT_URL = os.environ.get("CLOUDFLARE_R2_S3_ENDPOINT_URL")

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

def safe_print(text_to_print):
    try:
        print(text_to_print)
    except UnicodeEncodeError:
        if sys.stdout.encoding:
            try:
                encoded_text = text_to_print.encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding)
                print(encoded_text)
            except Exception: # Fallback if even replacement fails with stdout.encoding
                print(text_to_print.encode('ascii', errors='replace').decode('ascii'))
        else: # If sys.stdout.encoding is None
            print(text_to_print.encode('ascii', errors='replace').decode('ascii'))
    except Exception as e:
            print(f"<print error: {e}>") # Catch other potential print errors

def fetch_and_save_items(d1_client_instance=None): # Added d1_client_instance
    """
    Fetches item data from API, merges with D1 data (or local CSV as fallback), saves to D1, and then to CSV.
    Returns a list of item IDs that need their history updated and the full list of item data.
    """
    all_items_data = {} # Keyed by item ID
    items_needing_history_update = []
    initial_d1_item_ids = set()
    d1_new_inserts_count = 0
    d1_updated_items_count = 0
    # items_failed_upsert_count is initialized later, just before the D1 upsert loop

    # Prioritize loading from D1
    if d1_client_instance:
        safe_print("Attempting to load existing item data from D1...")
        try:
            select_all_sql = "SELECT item_id, name, category_name, group_name, weekly_average_price, icon_id, icon_r2_key, date_created, date_updated FROM items;"
            response = d1_client_instance.d1.database.query(
                database_id=CLOUDFLARE_D1_DATABASE_ID,
                account_id=CLOUDFLARE_ACCOUNT_ID,
                sql=select_all_sql
            )
            if response.success and response.result and response.result[0].results:
                for row in response.result[0].results:
                    item_id = row.get('item_id')
                    if item_id:
                        # Convert D1 row (dict) to the structure expected by all_items_data
                        # The D1 row keys match the desired dictionary keys closely
                        item_entry = dict(row) # Make a mutable copy
                        item_entry['id'] = item_entry.pop('item_id') # Rename to 'id'

                        # Infer 'icon_downloaded' status from 'icon_r2_key'
                        item_entry['icon_downloaded'] = 'True' if row.get('icon_r2_key') else 'False'
                        item_entry['needs_history_update'] = 'False' # Default, will be updated

                        # Ensure all FINAL_CSV_HEADERS are present, even if null from D1
                        for header in FINAL_CSV_HEADERS:
                            if header not in item_entry:
                                item_entry[header] = None # Or appropriate default like ''

                        item_id_from_d1 = item_entry.get('id') # Use 'id' as it's now the key
                        item_name_for_log = item_entry.get('name', 'Unknown Name')
                        safe_print(f"D1 Load: Loaded item {item_id_from_d1} ('{item_name_for_log}') from D1.")
                        all_items_data[item_id] = item_entry

                if all_items_data: # Check if any items were actually loaded from D1
                    initial_d1_item_ids = set(all_items_data.keys()) # Populate AFTER D1 load loop
                    safe_print(f"Successfully loaded a total of {len(initial_d1_item_ids)} items from D1. Their IDs are now tracked for insert/update distinction.")
                else: # D1 query succeeded but no items were in the DB
                    safe_print("No existing items found in D1 database via initial query.") # This is correct if all_items_data is empty after loop
            else: # D1 query failed or returned no results structure
                if not response.success:
                    safe_print(f"D1 query to load items failed. Errors: {response.errors}")
                else: # response.success is True, but no response.result or no response.result[0].results
                    safe_print("D1 query successful but returned no parseable results. Assuming D1 is empty or structure is unexpected.")

                safe_print(f"D1 query failed or returned no data. Falling back to loading from {ITEMS_OUTPUT_CSV_FILE}...")
                if os.path.exists(ITEMS_OUTPUT_CSV_FILE):
                    try:
                        with open(ITEMS_OUTPUT_CSV_FILE, mode='r', encoding='utf-8', newline='') as csvfile:
                            reader = csv.DictReader(csvfile)
                            csv_items_loaded_count = 0
                            for row_csv in reader:
                                item_id_csv = row_csv.get('id')
                                if item_id_csv: # Ensure item_id_csv is not None or empty
                                    row_csv.setdefault('icon_downloaded', 'False')
                                    row_csv['needs_history_update'] = 'False'
                                    all_items_data[item_id_csv] = row_csv # This will form the baseline if D1 failed
                                    csv_items_loaded_count +=1
                            safe_print(f"Loaded {csv_items_loaded_count} items from CSV as fallback.")
                    except Exception as e_csv:
                        safe_print(f"Error reading {ITEMS_OUTPUT_CSV_FILE} during fallback: {e_csv}. Starting with an empty dataset.")
                        all_items_data = {} # Ensure it's empty if CSV also fails
                else:
                    safe_print(f"Fallback CSV {ITEMS_OUTPUT_CSV_FILE} not found. Starting with an empty dataset as D1 was also unavailable/empty.")
                    all_items_data = {}
        except cloudflare.APIError as e_d1_api:
            safe_print(f"D1 APIError during initial load: {e_d1_api}. Attempting CSV fallback.")
            # CSV Fallback logic (similar to above, can be refactored into a helper if too repetitive)
            if os.path.exists(ITEMS_OUTPUT_CSV_FILE):
                try:
                    # ... (CSV loading logic) ...
                    safe_print(f"Loaded items from CSV after D1 APIError.")
                except Exception as e_csv:
                    safe_print(f"Error reading {ITEMS_OUTPUT_CSV_FILE} during D1 APIError fallback: {e_csv}. Starting empty.")
                    all_items_data = {}
            else:
                safe_print(f"Fallback CSV {ITEMS_OUTPUT_CSV_FILE} not found after D1 APIError. Starting empty.")
                all_items_data = {}
        except Exception as e_generic:
            safe_print(f"Generic error during D1 initial load: {e_generic}. Attempting CSV fallback.")
            # CSV Fallback logic (similar to above)
            if os.path.exists(ITEMS_OUTPUT_CSV_FILE):
                try:
                    # ... (CSV loading logic) ...
                    safe_print(f"Loaded items from CSV after generic D1 load error.")
                except Exception as e_csv:
                    safe_print(f"Error reading {ITEMS_OUTPUT_CSV_FILE} during generic D1 error fallback: {e_csv}. Starting empty.")
                    all_items_data = {}
            else:
                safe_print(f"Fallback CSV {ITEMS_OUTPUT_CSV_FILE} not found after generic D1 load error. Starting empty.")
                all_items_data = {}
    else:
        # Fallback to CSV if D1 client is not available
        safe_print(f"D1 client not available. Loading from {ITEMS_OUTPUT_CSV_FILE}...")
        if os.path.exists(ITEMS_OUTPUT_CSV_FILE):
            try:
                with open(ITEMS_OUTPUT_CSV_FILE, mode='r', encoding='utf-8', newline='') as csvfile:
                    reader = csv.DictReader(csvfile)
                    csv_items_loaded_count = 0
                    for row in reader:
                        item_id = row.get('id')
                        if item_id:
                            row.setdefault('icon_downloaded', 'False')
                            row['needs_history_update'] = 'False'
                            all_items_data[item_id] = row
                            csv_items_loaded_count +=1
                    safe_print(f"Loaded {csv_items_loaded_count} items from CSV.")
            except Exception as e:
                safe_print(f"Error reading {ITEMS_OUTPUT_CSV_FILE}: {e}. Starting with an empty dataset.")
                all_items_data = {}
        else:
            safe_print(f"CSV file {ITEMS_OUTPUT_CSV_FILE} not found. Starting with an empty dataset.")
            all_items_data = {}


    current_page = 1
    headers = {'accept': 'text/csv'}
    safe_print("Starting to fetch item data from API...")

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
            safe_print(f"Fetching API item list page {current_page}...")
            response = requests.get(API_BASE_URL_ITEMS, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                response_text_stripped = response.text.strip()

                if not response_text_stripped:
                    safe_print(f"API Page {current_page} is effectively empty. Assuming no more item data.")
                    break

                reader = csv.DictReader(response_text_stripped.splitlines())
                api_items_on_page = list(reader)
                safe_print(f"Successfully fetched and parsed {len(api_items_on_page)} items from API page {current_page}.")

                if not api_items_on_page: # Handles header-only response or empty after parsing
                    safe_print(f"No data items on API page {current_page} (only header or empty). Ending pagination.")
                    break
                
                api_data_processed = True # Mark that we have processed at least one page with data

                for api_item in api_items_on_page:
                    item_id = api_item.get('id')
                    if not item_id:
                        safe_print(f"Skipping API item due to missing ID: {api_item.get('name', 'Unknown Name')}")
                        continue

                    date_updated_api = api_item.get('date_updated')

                    if item_id in all_items_data:
                        # Item exists, check for updates
                        existing_item = all_items_data[item_id]
                        date_updated_local = existing_item.get('date_updated')
                        icon_downloaded_status = existing_item.get('icon_downloaded', 'False') # Preserve existing

                        if date_updated_api and date_updated_local and date_updated_api > date_updated_local:
                            safe_print(f"Updating item {item_id} ('{api_item.get('name')}') as API data is newer.")
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
                        safe_print(f"API Fetch: New item {item_id} ('{api_item.get('name', 'Unknown Name')}') identified, adding to internal data list.")
                        new_item_entry = {header: '' for header in FINAL_CSV_HEADERS} # Initialize with all headers
                        new_item_entry.update(api_item) # Populate with API data
                        new_item_entry['icon_downloaded'] = 'False'
                        new_item_entry['needs_history_update'] = 'True'
                        all_items_data[item_id] = new_item_entry
            else:
                safe_print(f"Error fetching API item list page {current_page}: Status code {response.status_code}")
                safe_print(f"Response content: {response.text[:200]}")
                break # Stop on error
        
        except requests.exceptions.RequestException as e:
            safe_print(f"Request for API item list failed on page {current_page}: {e}")
            break # Stop on error
        except csv.Error as e:
            safe_print(f"CSV parsing error on API page {current_page}: {e}. Response text: {response.text[:500]}")
            break

        current_page += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    safe_print(f"Finished fetching from item API. Total pages processed: {current_page -1}. API data was processed: {'Yes' if api_data_processed else 'No'}.")

    # After loop, ensure items loaded from CSV but not in API have needs_history_update='False'
    # This is implicitly handled by the logic: initial load is 'False', and only API interaction changes it.
    # If an item from CSV was never found in API, its 'needs_history_update' remains 'False'.

    # The CSV writing is now handled at the end of the main script execution block,
    # ensuring it contains data processed by download_item_icons (e.g., icon_r2_key).

    # Collect IDs for history update
    for item_id, data in all_items_data.items():
        if data.get('needs_history_update') == 'True':
            items_needing_history_update.append(item_id)

    safe_print(f"Found {len(items_needing_history_update)} items needing history update.")

    # Save/Update items in D1
    if d1_client_instance and all_items_data:
        safe_print(f"\nUpserting {len(all_items_data)} items into D1 database...")
        items_failed_upsert_count = 0 # Initialize here, as it's specific to this loop
        # d1_new_inserts_count and d1_updated_items_count were initialized at the start of the function

        for item_id_key, item_data_dict in all_items_data.items():
            item_id_to_upsert = item_data_dict.get('id')
            item_name_for_log = item_data_dict.get('name', 'Unknown Name')

            if item_id_to_upsert in initial_d1_item_ids:
                safe_print(f"D1: Updating existing item {item_id_to_upsert} ('{item_name_for_log}')...")
            else:
                safe_print(f"D1: Inserting new item {item_id_to_upsert} ('{item_name_for_log}')...")

            try:
                # Ensure weekly_average_price is float or None
                wap = item_data_dict.get('weekly_average_price')
                if wap is not None and wap != '':
                    try:
                        wap_float = float(wap)
                    except ValueError:
                        safe_print(f"Warning: Could not convert weekly_average_price '{wap}' to float for item {item_id_key}. Setting to NULL.")
                        wap_float = None
                else:
                    wap_float = None

                params = [
                    item_data_dict.get('id'),
                    item_data_dict.get('name'),
                    item_data_dict.get('category_name'),
                    item_data_dict.get('group_name'),
                    wap_float,
                    item_data_dict.get('icon_id'),
                    item_data_dict.get('icon_r2_key'),
                    item_data_dict.get('date_created'),
                    item_data_dict.get('date_updated')
                ]

                upsert_sql = """
                INSERT INTO items (item_id, name, category_name, group_name, weekly_average_price, icon_id, icon_r2_key, date_created, date_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_id) DO UPDATE SET
                    name = excluded.name,
                    category_name = excluded.category_name,
                    group_name = excluded.group_name,
                    weekly_average_price = excluded.weekly_average_price,
                    icon_id = excluded.icon_id,
                    icon_r2_key = COALESCE(excluded.icon_r2_key, items.icon_r2_key),
                    date_created = COALESCE(items.date_created, excluded.date_created),
                    date_updated = excluded.date_updated;
                """

                response = d1_client_instance.d1.database.query(
                    database_id=CLOUDFLARE_D1_DATABASE_ID,
                    account_id=CLOUDFLARE_ACCOUNT_ID,
                    sql=upsert_sql,
                    params=params
                )

                if response.success:
                    if item_id_to_upsert in initial_d1_item_ids:
                        d1_updated_items_count += 1
                    else:
                        d1_new_inserts_count += 1
                else:
                    safe_print(f"Failed to upsert item {item_id_key} into D1. Errors: {response.errors}") # item_id_key is correct here as it's the dict key
                    items_failed_upsert_count +=1
            except cloudflare.APIError as e:
                safe_print(f"D1 APIError during upsert for item {item_id_key}: {e}") # item_id_key is correct here
                items_failed_upsert_count +=1
            except Exception as e:
                safe_print(f"Generic error during D1 upsert for item {item_id_key}: {e}") # item_id_key is correct here
                items_failed_upsert_count +=1

        total_successful_d1_ops = d1_new_inserts_count + d1_updated_items_count
        safe_print(f"D1 Sync for 'items' table Summary: {total_successful_d1_ops} items successfully processed.")
        safe_print(f"  - New items inserted into D1: {d1_new_inserts_count}")
        safe_print(f"  - Existing items updated/refreshed in D1: {d1_updated_items_count}")
        if items_failed_upsert_count > 0:
            safe_print(f"  - Failed D1 operations: {items_failed_upsert_count}")

    return items_needing_history_update, list(all_items_data.values())


def download_item_icons(all_items_data: list[dict], r2_client_instance, d1_client_instance) -> list[dict]:
    """
    Downloads icons for items, uploads them to R2, and updates D1 with the R2 key.
    Updates 'icon_downloaded' and 'icon_r2_key' status in each item's dictionary.
    """
    safe_print(f"\nStarting to process icons for {len(all_items_data)} items (download, R2 upload, D1 update)...")
    icons_found_locally = 0
    icons_downloaded_successfully = 0
    icons_skipped_no_info = 0
    icons_failed_download = 0
    icons_uploaded_to_r2 = 0
    icons_failed_r2_upload = 0
    d1_updates_successful = 0
    d1_updates_failed = 0

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
        icon_is_present_locally = False
        if local_icon_path and os.path.exists(local_icon_path):
            if item_data.get('icon_downloaded') != 'True': # Log if flag was false but file exists
                safe_print(f"Icon for {item_id_str} ('{item_name_str}') found locally at {local_icon_path}, flag was '{item_data.get('icon_downloaded')}'. Updating flag.")
            item_data['icon_downloaded'] = 'True' # Ensure flag is true
            icon_is_present_locally = True
            icons_found_locally += 1
            # Do not continue here; proceed to R2 upload check even for existing local icons if not yet uploaded.

        # If local icon does not exist, proceed to download attempt
        if not icon_is_present_locally:
            os.makedirs(item_dir, exist_ok=True) # Ensure directory exists before download attempt

            if not icon_url or not icon_id_val or not local_icon_path:
                item_data['icon_downloaded'] = 'False'
                icons_skipped_no_info += 1
                continue # Skip to next item if essential info for download is missing

            safe_print(f"Downloading icon for item {item_id_str} ('{item_name_str}') from {icon_url} to {local_icon_path}")
            download_attempted = True # Moved here
            try:
                response = requests.get(icon_url, stream=True, timeout=30)
                if response.status_code == 200 and response.content:
                    with open(local_icon_path, 'wb') as f:
                        f.write(response.content)
                    item_data['icon_downloaded'] = 'True'
                    icon_is_present_locally = True # Mark as present for R2 upload
                    icons_downloaded_successfully += 1
                else:
                    safe_print(f"Error downloading icon for {item_id_str} ('{item_name_str}'): Status {response.status_code}, Content-Length {response.headers.get('Content-Length', 'N/A')}")
                    item_data['icon_downloaded'] = 'False'
                    icons_failed_download += 1
                    continue # Skip R2 upload if download failed
            except requests.exceptions.RequestException as e:
                safe_print(f"Request failed for icon download {item_id_str} ('{item_name_str}'): {e}")
                item_data['icon_downloaded'] = 'False'
                icons_failed_download += 1
                continue # Skip R2 upload
            except IOError as e:
                safe_print(f"IOError saving icon for item {item_id_str} ('{item_name_str}'): {e}")
                item_data['icon_downloaded'] = 'False'
                icons_failed_download += 1
                continue # Skip R2 upload
            finally:
                if download_attempted:
                    time.sleep(REQUEST_DELAY_SECONDS)

        # R2 Upload and D1 Update Logic
        if icon_is_present_locally and r2_client_instance and d1_client_instance and icon_id_val:
            # Check if R2 key already exists and is valid (e.g. not empty string)
            # For simplicity, we re-upload if icon_r2_key is not already set in item_data from a previous successful run.
            # A more robust check would query D1 or compare with a known state.
            if not item_data.get('icon_r2_key'):
                r2_object_key = f"icons/{sanitize_for_path(str(icon_id_val))}.png"
                safe_print(f"Attempting to upload icon {local_icon_path} to R2 as {r2_object_key} for item {item_id_str}...")
                try:
                    with open(local_icon_path, 'rb') as icon_file_to_upload:
                        r2_client_instance.put_object(
                            Bucket=CLOUDFLARE_R2_BUCKET_NAME,
                            Key=r2_object_key,
                            Body=icon_file_to_upload,
                            ContentType='image/png'
                        )
                    safe_print(f"Successfully uploaded icon to R2: {r2_object_key}")
                    icons_uploaded_to_r2 += 1
                    item_data['icon_r2_key'] = r2_object_key # Update local dict

                    # Update D1 with the R2 key
                    safe_print(f"Updating D1 item {item_id_str} with R2 key: {r2_object_key}...")
                    update_d1_sql = "UPDATE items SET icon_r2_key = ? WHERE item_id = ?;"
                    d1_response = d1_client_instance.d1.database.query(
                        database_id=CLOUDFLARE_D1_DATABASE_ID,
                        account_id=CLOUDFLARE_ACCOUNT_ID,
                        sql=update_d1_sql,
                        params=[r2_object_key, item_id_str]
                    )
                    if d1_response.success:
                        safe_print(f"D1 updated successfully for item {item_id_str}.")
                        d1_updates_successful += 1
                        item_data['icon_downloaded'] = 'True' # Mark as fully processed
                        # Attempt to delete local icon file
                        try:
                            if local_icon_path and os.path.exists(local_icon_path):
                                os.remove(local_icon_path)
                                safe_print(f"Successfully deleted local icon: {local_icon_path}")
                        except OSError as e_os:
                            safe_print(f"Error deleting local icon {local_icon_path}: {e_os}")
                    else:
                        safe_print(f"Failed to update D1 for item {item_id_str}. Errors: {d1_response.errors}")
                        d1_updates_failed += 1
                        item_data['icon_downloaded'] = 'False' # Explicitly mark as not fully processed if D1 fails
                except ClientError as e:
                    safe_print(f"R2 ClientError uploading icon {r2_object_key} for item {item_id_str}: {e}")
                    icons_failed_r2_upload += 1
                    item_data['icon_downloaded'] = 'False'
                except FileNotFoundError:
                    safe_print(f"Error: Local icon file {local_icon_path} not found for R2 upload (item {item_id_str}). Should have been downloaded or found.")
                    icons_failed_r2_upload += 1
                    item_data['icon_downloaded'] = 'False'
                except cloudflare.APIError as e:
                    safe_print(f"D1 APIError updating icon_r2_key for item {item_id_str}: {e}")
                    d1_updates_failed += 1
                    item_data['icon_downloaded'] = 'False'
                except Exception as e:
                    safe_print(f"Generic error during R2 upload or D1 update for item {item_id_str}: {e}")
                    icons_failed_r2_upload += 1
                    item_data['icon_downloaded'] = 'False'
            elif item_data.get('icon_r2_key'): # If key already exists
                 item_data['icon_downloaded'] = 'True' # Ensure flag is true if R2 key is present
                 # safe_print(f"Skipping R2 upload for item {item_id_str}, icon_r2_key '{item_data.get('icon_r2_key')}' already set.")

        elif icon_is_present_locally and (not r2_client_instance or not d1_client_instance):
            safe_print(f"Skipping R2/D1 update for item {item_id_str} because R2 or D1 client is not available.")
            # Keep icon_downloaded as True if local file exists, but R2 key won't be set.
            # Or set to False if 'downloaded' means fully processed to cloud. For now, let it reflect local state if cloud fails.


        # Original download logic if icon_is_present_locally was false and download was attempted earlier
        # This part is now integrated above to ensure R2 upload happens after successful download.
        # The following lines are effectively replaced by the logic block above.
        # if not icon_url or not icon_id_val or not local_icon_path: # Check all necessary components for download
            # item_data['icon_downloaded'] = 'False'
            # icons_skipped_no_info += 1
            # continue # This was original logic, removed to integrate R2 upload

    safe_print("\n--- Icon Processing Summary ---")
    safe_print(f"Icons found locally (and flag potentially updated): {icons_found_locally}")
    safe_print(f"Icons newly downloaded successfully: {icons_downloaded_successfully}")
    safe_print(f"Skipped local download (missing URL/ID or invalid path): {icons_skipped_no_info}")
    safe_print(f"Failed local download/save: {icons_failed_download}")
    safe_print(f"Icons uploaded to R2: {icons_uploaded_to_r2}")
    safe_print(f"Failed R2 uploads: {icons_failed_r2_upload}")
    safe_print(f"D1 'icon_r2_key' updates successful: {d1_updates_successful}")
    safe_print(f"D1 'icon_r2_key' updates failed: {d1_updates_failed}")
    safe_print("-----------------------------")
    return all_items_data

API_V2_ITEM_PRICES_URL = "https://echoes.mobi/api/v2/item_prices"

def load_all_current_prices() -> dict:
    """
    Fetches all current item prices from the v2 API endpoint.
    Returns a dictionary mapping item_id to its price data.
    """
    safe_print(f"\nFetching all current item prices from {API_V2_ITEM_PRICES_URL}...")
    current_prices_map = {}
    headers = {'accept': 'text/csv'}

    try:
        response = requests.get(API_V2_ITEM_PRICES_URL, headers=headers, timeout=30)
        if response.status_code == 200:
            response_text_stripped = response.text.strip()
            if not response_text_stripped:
                safe_print("API response for current prices was empty.")
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
            safe_print(f"Successfully loaded {len(current_prices_map)} current item prices.")
        else:
            safe_print(f"Error fetching current prices: Status code {response.status_code}")
            safe_print(f"Response content: {response.text[:200]}")
    except requests.exceptions.RequestException as e:
        safe_print(f"Request failed for current prices: {e}")
    except csv.Error as e:
        safe_print(f"CSV parsing error for current prices: {e}. Response text: {response.text[:500]}")
    except Exception as e:
        safe_print(f"An unexpected error occurred while loading current prices: {e}")

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
        safe_print(f"Error parsing date string '{iso_date_string}': {e}. Returning default week/year ('00', '0000').")
        return ("00", "0000") # Fallback values
    except Exception as e: # Catch any other unexpected errors
        safe_print(f"An unexpected error occurred while parsing date '{iso_date_string}': {e}. Returning default week/year ('00', '0000').")
        return ("00", "0000")

HISTORY_CSV_HEADERS = ['id', 'item_id', 'price', 'week', 'year', 'date_created', 'date_updated']

def fetch_and_save_histories(item_ids_to_process: list[str], all_current_prices_map: dict, d1_client_instance):
    """
    Fetches full history for new items or appends latest price for existing items in D1.
    - item_ids_to_process: List of all item IDs to process.
    - all_current_prices_map: Dictionary with current price data from /v2/item_prices.
    """
    safe_print(f"\nStarting to process D1 histories for {len(item_ids_to_process)} items...")
    if not d1_client_instance:
        safe_print("D1 client is not available. Skipping D1 history operations.")
        return
    if not item_ids_to_process:
        safe_print("No item IDs provided for history processing.")
        return

    # Statistics
    full_history_items_processed = 0
    full_history_records_inserted_total = 0 # Renamed for clarity
    full_history_api_failures = 0
    full_history_d1_failures_total = 0 # Renamed for clarity
    appended_latest_price_count = 0
    append_failures_missing_data = 0
    append_failures_d1 = 0
    items_already_up_to_date = 0

    api_headers = {'accept': 'text/csv'}

    for item_id in item_ids_to_process:
        latest_d1_date_updated_for_item = None
        # max_date_from_full_history = None # This was for an intermediate step, not strictly needed here

        try:
            query_latest_sql = "SELECT MAX(date_updated) as latest_date FROM item_history WHERE item_id = ?;"
            response_latest = d1_client_instance.d1.database.query(
                database_id=CLOUDFLARE_D1_DATABASE_ID, account_id=CLOUDFLARE_ACCOUNT_ID,
                sql=query_latest_sql, params=[item_id]
            )
            if response_latest.success and response_latest.result and response_latest.result[0].results:
                latest_d1_date_updated_for_item = response_latest.result[0].results[0].get('latest_date')
        except Exception as e:
            safe_print(f"Error querying latest D1 history date for item {item_id}: {e}. Will attempt full backfill/append.")

        if latest_d1_date_updated_for_item is None:
            safe_print(f"No history found in D1 for item {item_id}. Attempting full history fetch...")
            full_history_items_processed += 1
            history_api_url = f"{API_BASE_URL_HISTORY}{item_id}"

            try:
                response = requests.get(history_api_url, headers=api_headers, timeout=30)
                if response.status_code == 200:
                    response_text_stripped = response.text.strip()
                    if response_text_stripped:
                        history_reader = csv.DictReader(response_text_stripped.splitlines())
                        records_inserted_this_item = 0
                        current_max_date_in_batch = None
                        for row in history_reader:
                            try:
                                price_val = row.get('price')
                                if price_val is None or price_val == '':
                                    safe_print(f"Skipping history row for item {item_id} due to missing price: {row}")
                                    continue
                                price = float(price_val)

                                week = row.get('week')
                                year = row.get('year')
                                date_created = row.get('date_created') # API's date_created for this history point
                                date_updated = row.get('date_updated') # API's date_updated for this history point

                                if not all([week, year, date_created, date_updated]):
                                    safe_print(f"Skipping history row for item {item_id} due to missing week, year, or date fields: {row}")
                                    continue

                                insert_hist_sql = "INSERT INTO item_history (item_id, price, week, year, date_created, date_updated) VALUES (?, ?, ?, ?, ?, ?);"
                                hist_params = [item_id, price, week, year, date_created, date_updated]

                                hist_resp = d1_client_instance.d1.database.query(
                                    database_id=CLOUDFLARE_D1_DATABASE_ID, account_id=CLOUDFLARE_ACCOUNT_ID,
                                    sql=insert_hist_sql, params=hist_params
                                )
                                if hist_resp.success:
                                    full_history_records_inserted_total += 1
                                    records_inserted_this_item += 1
                                    if current_max_date_in_batch is None or date_updated > current_max_date_in_batch:
                                        current_max_date_in_batch = date_updated
                                else:
                                    safe_print(f"Failed to insert full history row for item {item_id}: {hist_resp.errors}")
                                    full_history_d1_failures_total +=1
                            except ValueError:
                                safe_print(f"Skipping history row for item {item_id} due to invalid price: {row.get('price')}")
                            except Exception as e_row:
                                safe_print(f"Error processing/inserting history row for item {item_id}: {e_row} - Row: {row}")
                                full_history_d1_failures_total +=1

                        if records_inserted_this_item > 0:
                            safe_print(f"Successfully inserted {records_inserted_this_item} full history records for item {item_id}.")
                            if current_max_date_in_batch:
                                latest_d1_date_updated_for_item = current_max_date_in_batch # Update with the latest from the batch
                        else:
                            safe_print(f"No valid history records found or inserted from API response for item {item_id}.")
                    else:
                        safe_print(f"Full history API response for item {item_id} was empty.")
                else:
                    safe_print(f"Error fetching full history for item {item_id}: Status {response.status_code}")
                    full_history_api_failures += 1
            except requests.exceptions.RequestException as e_req:
                safe_print(f"Request failed for full history for item {item_id}: {e_req}")
                full_history_api_failures += 1
            except Exception as e_outer: # Catch other errors like CSV parsing issues
                safe_print(f"Outer error processing full history for item {item_id}: {e_outer}")
                full_history_api_failures +=1 # Count as API/processing failure

            time.sleep(REQUEST_DELAY_SECONDS) # Delay after each full history API call, even if it failed

        # This block now runs regardless of whether a full history backfill was attempted or if history already existed.
        # It ensures the very latest price from all_current_prices_map is considered.
        current_price_point = all_current_prices_map.get(item_id)
        if current_price_point and current_price_point.get('estimated_price') is not None and current_price_point.get('estimated_price') != '':
            api_price_str = current_price_point['estimated_price']
            api_date_updated_from_v2 = current_price_point['date_updated'] # Date from /v2/item_prices

            try:
                api_price_float = float(api_price_str)

                # Compare with latest_d1_date_updated_for_item (which might have been updated by full backfill)
                if latest_d1_date_updated_for_item and api_date_updated_from_v2 <= latest_d1_date_updated_for_item:
                    safe_print(f"Latest price for item {item_id} (date: {api_date_updated_from_v2}) from v2 API already reflected or older than D1 history. Skipping append.")
                    if latest_d1_date_updated_for_item is not None: # Only count if there was a D1 record to compare against
                        items_already_up_to_date +=1
                else: # D1 history is older, or non-existent, or v2 price is newer
                    derived_week, derived_year = get_week_year_from_isodate(api_date_updated_from_v2)
                    insert_latest_sql = "INSERT INTO item_history (item_id, price, week, year, date_created, date_updated) VALUES (?, ?, ?, ?, ?, ?);"
                    latest_params = [item_id, api_price_float, derived_week, derived_year, api_date_updated_from_v2, api_date_updated_from_v2]

                    safe_print(f"Appending latest price (from v2 map) to D1 for item {item_id}, price: {api_price_float}, date: {api_date_updated_from_v2}")
                    latest_resp = d1_client_instance.d1.database.query(
                        database_id=CLOUDFLARE_D1_DATABASE_ID, account_id=CLOUDFLARE_ACCOUNT_ID,
                        sql=insert_latest_sql, params=latest_params
                    )
                    if latest_resp.success:
                        appended_latest_price_count += 1
                    else:
                        safe_print(f"Failed to append latest price (from v2 map) for item {item_id} to D1: {latest_resp.errors}")
                        append_failures_d1 += 1
            except ValueError:
                safe_print(f"Error converting current price '{api_price_str}' (from v2 map) to float for item {item_id} during append. Skipping.")
                append_failures_missing_data +=1
            except Exception as e_append:
                safe_print(f"Generic error during append logic (from v2 map) for item {item_id}: {e_append}")
                append_failures_d1 +=1
        else:
            safe_print(f"No current price data in all_current_prices_map for item {item_id} to consider for append.")
            # append_failures_missing_data +=1 # Not necessarily a failure if it was backfilled and this map is just for more recent

        time.sleep(REQUEST_DELAY_SECONDS / 5) # Shorter delay between processing each item

    safe_print("\n--- D1 Item History Processing Summary ---")
    safe_print(f"Items processed for full history backfill (attempted): {full_history_items_processed}")
    safe_print(f"Total records inserted from full history fetches: {full_history_records_inserted_total}")
    safe_print(f"Full history API fetch failures: {full_history_api_failures}")
    safe_print(f"Full history D1 insert failures (individual records): {full_history_d1_failures_total}")
    safe_print(f"Latest price entries successfully appended to D1 (from v2 map): {appended_latest_price_count}")
    safe_print(f"Items skipped (latest v2 price already up-to-date in D1): {items_already_up_to_date}")
    safe_print(f"Append failures (missing data or D1 error for v2 price): {append_failures_d1 + append_failures_missing_data}")
    safe_print("---------------------------------------------")

def check_cloudflare_config():
    global CLOUDFLARE_R2_S3_ENDPOINT_URL # Allow modification of global
    missing_vars = []
    if not CLOUDFLARE_API_TOKEN:
        missing_vars.append("CLOUDFLARE_API_TOKEN")
    if not CLOUDFLARE_ACCOUNT_ID:
        missing_vars.append("CLOUDFLARE_ACCOUNT_ID")
    if not CLOUDFLARE_D1_DATABASE_ID:
        missing_vars.append("CLOUDFLARE_D1_DATABASE_ID")
    if not CLOUDFLARE_R2_BUCKET_NAME:
        missing_vars.append("CLOUDFLARE_R2_BUCKET_NAME")
    if not CLOUDFLARE_R2_S3_ACCESS_KEY_ID:
        missing_vars.append("CLOUDFLARE_R2_S3_ACCESS_KEY_ID")
    if not CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY:
        missing_vars.append("CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY")

    if not CLOUDFLARE_R2_S3_ENDPOINT_URL:
        if CLOUDFLARE_ACCOUNT_ID:
            CLOUDFLARE_R2_S3_ENDPOINT_URL = f"https://{CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com"
            safe_print(f"NOTE: CLOUDFLARE_R2_S3_ENDPOINT_URL was not set, constructed: {CLOUDFLARE_R2_S3_ENDPOINT_URL}")
        else:
            missing_vars.append("CLOUDFLARE_R2_S3_ENDPOINT_URL (and CLOUDFLARE_ACCOUNT_ID to construct it)")

    if missing_vars:
        safe_print("Error: The following Cloudflare environment variables are not set or derivable:")
        for var in missing_vars:
            safe_print(f" - {var}")
        return False
    safe_print("Cloudflare configuration variables are present.")
    return True

def create_d1_tables(d1_client_instance):
    """
    Creates the 'items' and 'item_history' tables in the D1 database if they don't already exist.
    """
    if not d1_client_instance:
        safe_print("D1 client is not available. Skipping table creation.")
        return

    safe_print("\nAttempting to create D1 tables if they don't exist...")

    items_table_sql = """
    CREATE TABLE IF NOT EXISTS items (
        item_id TEXT PRIMARY KEY,
        name TEXT,
        category_name TEXT,
        group_name TEXT,
        weekly_average_price REAL,
        icon_id TEXT,
        icon_r2_key TEXT,
        date_created TEXT,
        date_updated TEXT
    );
    """
    item_history_table_sql = """
    CREATE TABLE IF NOT EXISTS item_history (
        history_id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id TEXT,
        price REAL,
        week TEXT,
        year TEXT,
        date_created TEXT,
        date_updated TEXT,
        FOREIGN KEY(item_id) REFERENCES items(item_id)
    );
    """
    tables_to_create = {
        "items": items_table_sql,
        "item_history": item_history_table_sql
    }

    success_all = True
    for table_name, sql_statement in tables_to_create.items():
        safe_print(f"Creating table '{table_name}'...")
        try:
            response = d1_client_instance.d1.database.query(
                database_id=CLOUDFLARE_D1_DATABASE_ID,
                account_id=CLOUDFLARE_ACCOUNT_ID,
                sql=sql_statement
            )
            if response.success:
                safe_print(f"Table '{table_name}' creation command executed successfully (or table already exists).")
            else:
                safe_print(f"Failed to create table '{table_name}'. Errors: {response.errors}")
                success_all = False
        except cloudflare.APIError as e:
            safe_print(f"D1 APIError during table '{table_name}' creation: {e}")
            success_all = False
        except Exception as e:
            safe_print(f"Generic error during table '{table_name}' creation: {e}")
            success_all = False

    if success_all:
        safe_print("D1 table creation process completed successfully for all tables.")
    else:
        safe_print("D1 table creation process encountered errors.")


def get_d1_client():
    safe_print("\nInitializing Cloudflare D1 client...")
    if not CLOUDFLARE_API_TOKEN:
        safe_print("Error: CLOUDFLARE_API_TOKEN is not set. Cannot initialize D1 client.")
        return None
    try:
        client = cloudflare.Cloudflare(api_token=CLOUDFLARE_API_TOKEN)
        safe_print("Cloudflare D1 client initialized successfully.")
        return client
    except Exception as e:
        safe_print(f"Error initializing Cloudflare D1 client: {e}")
        return None

def get_r2_client():
    safe_print("\nInitializing Cloudflare R2 client (Boto3 S3)...")
    if not all([CLOUDFLARE_R2_S3_ENDPOINT_URL, CLOUDFLARE_R2_S3_ACCESS_KEY_ID, CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY]):
        safe_print("Error: Missing one or more R2 S3 configuration variables. Cannot initialize R2 client.")
        safe_print(f"  Endpoint URL set: {'Yes' if CLOUDFLARE_R2_S3_ENDPOINT_URL else 'No'}")
        safe_print(f"  Access Key ID set: {'Yes' if CLOUDFLARE_R2_S3_ACCESS_KEY_ID else 'No'}")
        safe_print(f"  Secret Access Key set: {'Yes' if CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY else 'No'}")
        return None
    try:
        client = boto3.client(
            's3',
            endpoint_url=CLOUDFLARE_R2_S3_ENDPOINT_URL,
            aws_access_key_id=CLOUDFLARE_R2_S3_ACCESS_KEY_ID,
            aws_secret_access_key=CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY,
        )
        safe_print("Cloudflare R2 S3 client initialized successfully.")
        return client
    except ClientError as e:
        safe_print(f"Boto3 ClientError initializing R2 S3 client: {e}")
        return None
    except Exception as e:
        safe_print(f"Generic error initializing R2 S3 client: {e}")
        return None

# Global clients, to be initialized in main
d1_client = None
r2_client = None

if __name__ == "__main__":
    safe_print("Starting data update script...")

    if not check_cloudflare_config():
        safe_print("Cloudflare configuration check failed. Please set the required environment variables.")
        exit(1)

    d1_client = get_d1_client()
    r2_client = get_r2_client()

    if not d1_client:
        safe_print("D1 client initialization failed. D1 related operations will be skipped.")
    if not r2_client:
        safe_print("R2 client initialization failed. R2 related operations will be skipped.")

    create_d1_tables(d1_client) # Ensure tables are created before fetching items

    # Proceed with existing logic, clients can be checked before use in respective functions
    items_to_update_history_for, all_items_data_list = fetch_and_save_items(d1_client_instance=d1_client)

    if all_items_data_list: # Check if there's any data to process
        all_items_data_list = download_item_icons(all_items_data_list, r2_client_instance=r2_client, d1_client_instance=d1_client)

        current_prices = load_all_current_prices()

        # Write the potentially updated all_items_data (with new icon_downloaded flags) to CSV
        # FINAL_CSV_HEADERS is defined globally
        safe_print(f"\nWriting final item data for {len(all_items_data_list)} items to {ITEMS_OUTPUT_CSV_FILE}...")
        try:
            with open(ITEMS_OUTPUT_CSV_FILE, 'w', encoding='utf-8', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=FINAL_CSV_HEADERS)
                writer.writeheader()
                for item_dict in all_items_data_list:
                    row_to_write = {header: item_dict.get(header, '') for header in FINAL_CSV_HEADERS}
                    writer.writerow(row_to_write)
            safe_print(f"Final item data including icon status written to {ITEMS_OUTPUT_CSV_FILE}")
        except IOError as e:
            safe_print(f"Error writing final item data to CSV: {e}")
        except Exception as e: # Catch any other unexpected error during write
             safe_print(f"An unexpected error occurred while writing final CSV: {e}")


        all_item_ids_for_history = [item['id'] for item in all_items_data_list if item.get('id')]
        if all_item_ids_for_history: # Check if there are any IDs to process
            fetch_and_save_histories(all_item_ids_for_history, current_prices, d1_client_instance=d1_client)
        else:
            safe_print("No item IDs available to process for history updates.")
    else:
        safe_print("\nSkipping icon downloading and history fetching because item list was not created or is empty.")

    safe_print("\nData update script finished.")
