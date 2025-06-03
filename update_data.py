import requests
import time
import os
import csv
import re
from datetime import datetime, timezone # Ensure timezone is imported
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

def fetch_and_save_items(d1_client_instance=None): # Added d1_client_instance
    """
    Fetches item data from API, merges with D1 data (or local CSV as fallback), saves to D1, and then to CSV.
    Returns a list of item IDs that need their history updated and the full list of item data.
    """
    all_items_data = {} # Keyed by item ID
    items_needing_history_update = []

    # Prioritize loading from D1
    if d1_client_instance:
        print("Attempting to load existing item data from D1...")
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

                        all_items_data[item_id] = item_entry
                print(f"Loaded {len(all_items_data)} items from D1.")
            else:
                if not response.success:
                    print(f"D1 query to load items failed. Errors: {response.errors}")
                else:
                    print("No items found in D1 database or query returned no results.")
                # Fallback to CSV if D1 load was not successful or empty
                print(f"Falling back to loading from {ITEMS_OUTPUT_CSV_FILE}...")
                # Re-add CSV loading logic here if desired as a fallback
                if os.path.exists(ITEMS_OUTPUT_CSV_FILE):
                    try:
                        with open(ITEMS_OUTPUT_CSV_FILE, mode='r', encoding='utf-8', newline='') as csvfile:
                            reader = csv.DictReader(csvfile)
                            for row_csv in reader:
                                item_id_csv = row_csv.get('id')
                                if item_id_csv and item_id_csv not in all_items_data: # Avoid overwriting D1 data if some was loaded
                                    row_csv.setdefault('icon_downloaded', 'False')
                                    row_csv['needs_history_update'] = 'False'
                                    all_items_data[item_id_csv] = row_csv
                            print(f"Loaded additional {len(all_items_data)} items from CSV as fallback/supplement.")
                    except Exception as e_csv:
                        print(f"Error reading {ITEMS_OUTPUT_CSV_FILE} during fallback: {e_csv}")
        except cloudflare.APIError as e_d1_api:
            print(f"D1 APIError during initial load: {e_d1_api}. Falling back to CSV if possible.")
            # Fallback to CSV (similar to above)
        except Exception as e_generic:
            print(f"Generic error during D1 initial load: {e_generic}. Falling back to CSV if possible.")
            # Fallback to CSV (similar to above)
    else:
        # Fallback to CSV if D1 client is not available
        print(f"D1 client not available. Loading from {ITEMS_OUTPUT_CSV_FILE}...")
        if os.path.exists(ITEMS_OUTPUT_CSV_FILE):
            try:
                with open(ITEMS_OUTPUT_CSV_FILE, mode='r', encoding='utf-8', newline='') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        item_id = row.get('id')
                        if item_id:
                            row.setdefault('icon_downloaded', 'False')
                            row['needs_history_update'] = 'False'
                            all_items_data[item_id] = row
                    print(f"Loaded {len(all_items_data)} items from CSV.")
            except Exception as e:
                print(f"Error reading {ITEMS_OUTPUT_CSV_FILE}: {e}. Starting with an empty dataset.")
                all_items_data = {}

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

    # The CSV writing is now handled at the end of the main script execution block,
    # ensuring it contains data processed by download_item_icons (e.g., icon_r2_key).

    # Collect IDs for history update
    for item_id, data in all_items_data.items():
        if data.get('needs_history_update') == 'True':
            items_needing_history_update.append(item_id)

    print(f"Found {len(items_needing_history_update)} items needing history update.")

    # Save/Update items in D1
    if d1_client_instance and all_items_data:
        print(f"\nUpserting {len(all_items_data)} items into D1 database...")
        items_upserted_count = 0
        items_failed_upsert_count = 0
        for item_id_key, item_data_dict in all_items_data.items():
            try:
                # Ensure weekly_average_price is float or None
                wap = item_data_dict.get('weekly_average_price')
                if wap is not None and wap != '':
                    try:
                        wap_float = float(wap)
                    except ValueError:
                        print(f"Warning: Could not convert weekly_average_price '{wap}' to float for item {item_id_key}. Setting to NULL.")
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
                    items_upserted_count +=1
                else:
                    print(f"Failed to upsert item {item_id_key} into D1. Errors: {response.errors}")
                    items_failed_upsert_count +=1
            except cloudflare.APIError as e:
                print(f"D1 APIError during upsert for item {item_id_key}: {e}")
                items_failed_upsert_count +=1
            except Exception as e:
                print(f"Generic error during D1 upsert for item {item_id_key}: {e}")
                items_failed_upsert_count +=1
        print(f"D1 Upsert Summary: {items_upserted_count} succeeded, {items_failed_upsert_count} failed.")

    return items_needing_history_update, list(all_items_data.values())


def download_item_icons(all_items_data: list[dict], r2_client_instance, d1_client_instance) -> list[dict]:
    """
    Downloads icons for items, uploads them to R2, and updates D1 with the R2 key.
    Updates 'icon_downloaded' and 'icon_r2_key' status in each item's dictionary.
    """
    print(f"\nStarting to process icons for {len(all_items_data)} items (download, R2 upload, D1 update)...")
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
                print(f"Icon for {item_id_str} ('{item_name_str}') found locally at {local_icon_path}, flag was '{item_data.get('icon_downloaded')}'. Updating flag.")
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

            print(f"Downloading icon for item {item_id_str} ('{item_name_str}') from {icon_url} to {local_icon_path}")
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
                    print(f"Error downloading icon for {item_id_str} ('{item_name_str}'): Status {response.status_code}, Content-Length {response.headers.get('Content-Length', 'N/A')}")
                    item_data['icon_downloaded'] = 'False'
                    icons_failed_download += 1
                    continue # Skip R2 upload if download failed
            except requests.exceptions.RequestException as e:
                print(f"Request failed for icon download {item_id_str} ('{item_name_str}'): {e}")
                item_data['icon_downloaded'] = 'False'
                icons_failed_download += 1
                continue # Skip R2 upload
            except IOError as e:
                print(f"IOError saving icon for item {item_id_str} ('{item_name_str}'): {e}")
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
                print(f"Attempting to upload icon {local_icon_path} to R2 as {r2_object_key} for item {item_id_str}...")
                try:
                    with open(local_icon_path, 'rb') as icon_file_to_upload:
                        r2_client_instance.put_object(
                            Bucket=CLOUDFLARE_R2_BUCKET_NAME,
                            Key=r2_object_key,
                            Body=icon_file_to_upload,
                            ContentType='image/png'
                        )
                    print(f"Successfully uploaded icon to R2: {r2_object_key}")
                    icons_uploaded_to_r2 += 1
                    item_data['icon_r2_key'] = r2_object_key # Update local dict

                    # Update D1 with the R2 key
                    print(f"Updating D1 item {item_id_str} with R2 key: {r2_object_key}...")
                    update_d1_sql = "UPDATE items SET icon_r2_key = ? WHERE item_id = ?;"
                    d1_response = d1_client_instance.d1.database.query(
                        database_id=CLOUDFLARE_D1_DATABASE_ID,
                        account_id=CLOUDFLARE_ACCOUNT_ID,
                        sql=update_d1_sql,
                        params=[r2_object_key, item_id_str]
                    )
                    if d1_response.success:
                        print(f"D1 updated successfully for item {item_id_str}.")
                        d1_updates_successful += 1
                        item_data['icon_downloaded'] = 'True' # Mark as fully processed
                        # Attempt to delete local icon file
                        try:
                            if local_icon_path and os.path.exists(local_icon_path):
                                os.remove(local_icon_path)
                                print(f"Successfully deleted local icon: {local_icon_path}")
                        except OSError as e_os:
                            print(f"Error deleting local icon {local_icon_path}: {e_os}")
                    else:
                        print(f"Failed to update D1 for item {item_id_str}. Errors: {d1_response.errors}")
                        d1_updates_failed += 1
                        item_data['icon_downloaded'] = 'False' # Explicitly mark as not fully processed if D1 fails
                except ClientError as e:
                    print(f"R2 ClientError uploading icon {r2_object_key} for item {item_id_str}: {e}")
                    icons_failed_r2_upload += 1
                    item_data['icon_downloaded'] = 'False'
                except FileNotFoundError:
                    print(f"Error: Local icon file {local_icon_path} not found for R2 upload (item {item_id_str}). Should have been downloaded or found.")
                    icons_failed_r2_upload += 1
                    item_data['icon_downloaded'] = 'False'
                except cloudflare.APIError as e:
                    print(f"D1 APIError updating icon_r2_key for item {item_id_str}: {e}")
                    d1_updates_failed += 1
                    item_data['icon_downloaded'] = 'False'
                except Exception as e:
                    print(f"Generic error during R2 upload or D1 update for item {item_id_str}: {e}")
                    icons_failed_r2_upload += 1
                    item_data['icon_downloaded'] = 'False'
            elif item_data.get('icon_r2_key'): # If key already exists
                 item_data['icon_downloaded'] = 'True' # Ensure flag is true if R2 key is present
                 # print(f"Skipping R2 upload for item {item_id_str}, icon_r2_key '{item_data.get('icon_r2_key')}' already set.")

        elif icon_is_present_locally and (not r2_client_instance or not d1_client_instance):
            print(f"Skipping R2/D1 update for item {item_id_str} because R2 or D1 client is not available.")
            # Keep icon_downloaded as True if local file exists, but R2 key won't be set.
            # Or set to False if 'downloaded' means fully processed to cloud. For now, let it reflect local state if cloud fails.


        # Original download logic if icon_is_present_locally was false and download was attempted earlier
        # This part is now integrated above to ensure R2 upload happens after successful download.
        # The following lines are effectively replaced by the logic block above.
        # if not icon_url or not icon_id_val or not local_icon_path: # Check all necessary components for download
            # item_data['icon_downloaded'] = 'False'
            # icons_skipped_no_info += 1
            # continue # This was original logic, removed to integrate R2 upload

    print("\n--- Icon Processing Summary ---")
    print(f"Icons found locally (and flag potentially updated): {icons_found_locally}")
    print(f"Icons newly downloaded successfully: {icons_downloaded_successfully}")
    print(f"Skipped local download (missing URL/ID or invalid path): {icons_skipped_no_info}")
    print(f"Failed local download/save: {icons_failed_download}")
    print(f"Icons uploaded to R2: {icons_uploaded_to_r2}")
    print(f"Failed R2 uploads: {icons_failed_r2_upload}")
    print(f"D1 'icon_r2_key' updates successful: {d1_updates_successful}")
    print(f"D1 'icon_r2_key' updates failed: {d1_updates_failed}")
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

HISTORY_CSV_HEADERS = ['id', 'item_id', 'price', 'week', 'year', 'date_created', 'date_updated'] # Kept for reference if CSVs are ever manually checked or as a schema reminder

def fetch_and_save_histories(item_ids_to_update: list[str], all_current_prices_map: dict, d1_client_instance):
    """
    Appends the latest price to item_history in D1 if it's newer than the existing latest.
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

    # os.makedirs(HISTORIES_BASE_DIR, exist_ok=True) # Local CSV folder no longer primary
    # headers = {'accept': 'text/csv'} # For API calls, still relevant if full fetch is implemented

    d1_inserts_success = 0
    d1_inserts_failed = 0
    d1_skipped_already_latest = 0
    # For a more detailed full history fetch, these would be used:
    # new_full_histories_to_d1_count = 0
    # failed_full_histories_to_d1_count = 0

    if not d1_client_instance:
        print("D1 client is not available. Skipping D1 history operations.")
        return

    for item_id in item_ids_to_update:
        current_price_data = all_current_prices_map.get(item_id)

        if not (current_price_data and current_price_data.get('estimated_price') is not None and current_price_data.get('estimated_price') != ''):
            print(f"Skipping D1 history update for item {item_id}: No current price data available in map.")
            d1_inserts_failed +=1 # Count as failed if data is missing for an update attempt
            continue

        api_price_str = current_price_data['estimated_price']
        api_date_updated = current_price_data['date_updated']

        try:
            api_price_float = float(api_price_str)
        except ValueError:
            print(f"Error converting price '{api_price_str}' to float for item {item_id}. Skipping D1 history update.")
            d1_inserts_failed += 1
            continue

        # Check latest date in D1 for this item_id
        latest_d1_date_updated = None
        try:
            query_latest_sql = "SELECT MAX(date_updated) as latest_date FROM item_history WHERE item_id = ?;"
            response_latest = d1_client_instance.d1.database.query(
                database_id=CLOUDFLARE_D1_DATABASE_ID,
                account_id=CLOUDFLARE_ACCOUNT_ID,
                sql=query_latest_sql,
                params=[item_id]
            )
            if response_latest.success and response_latest.result and response_latest.result[0].results:
                latest_d1_date_updated = response_latest.result[0].results[0].get('latest_date')
        except cloudflare.APIError as e:
            print(f"D1 APIError querying latest date for item {item_id}: {e}. Proceeding to insert.")
        except Exception as e:
            print(f"Generic error querying latest date for item {item_id}: {e}. Proceeding to insert.")

        if latest_d1_date_updated and api_date_updated <= latest_d1_date_updated:
            print(f"D1 history for item {item_id} (date: {api_date_updated}) is current or newer than API. Skipping insert.")
            d1_skipped_already_latest += 1
            continue

        # Proceed to insert if no D1 history or if API data is newer
        derived_week, derived_year = get_week_year_from_isodate(api_date_updated)

        insert_params = [
            item_id,
            api_price_float,
            derived_week,
            derived_year,
            api_date_updated, # date_created for this new history entry
            api_date_updated  # date_updated for this new history entry
        ]

        insert_sql = """
        INSERT INTO item_history (item_id, price, week, year, date_created, date_updated)
        VALUES (?, ?, ?, ?, ?, ?);
        """

        try:
            print(f"Inserting/Updating D1 history for item {item_id}, price: {api_price_float}, date: {api_date_updated}")
            d1_response = d1_client_instance.d1.database.query(
                database_id=CLOUDFLARE_D1_DATABASE_ID,
                account_id=CLOUDFLARE_ACCOUNT_ID,
                sql=insert_sql,
                params=insert_params
            )
            if d1_response.success:
                d1_inserts_success += 1
            else:
                print(f"Failed to insert D1 history for item {item_id}. Errors: {d1_response.errors}")
                d1_inserts_failed += 1
        except cloudflare.APIError as e:
            print(f"D1 APIError during history insert for item {item_id}: {e}")
            d1_inserts_failed += 1
        except Exception as e:
            print(f"Generic error during D1 history insert for item {item_id}: {e}")
            d1_inserts_failed += 1

        time.sleep(REQUEST_DELAY_SECONDS / 2) # Shorter delay for D1 history inserts if needed

    print("\n--- D1 Item History Update Summary ---")
    print(f"New history entries successfully inserted into D1: {d1_inserts_success}")
    print(f"Skipped (already latest in D1 or API data not newer): {d1_skipped_already_latest}")
    print(f"Failed D1 history inserts (includes missing price data or DB errors): {d1_inserts_failed}")
    print("------------------------------------")


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
            print(f"NOTE: CLOUDFLARE_R2_S3_ENDPOINT_URL was not set, constructed: {CLOUDFLARE_R2_S3_ENDPOINT_URL}")
        else:
            missing_vars.append("CLOUDFLARE_R2_S3_ENDPOINT_URL (and CLOUDFLARE_ACCOUNT_ID to construct it)")

    if missing_vars:
        print("Error: The following Cloudflare environment variables are not set or derivable:")
        for var in missing_vars:
            print(f" - {var}")
        return False
    print("Cloudflare configuration variables are present.")
    return True

def create_d1_tables(d1_client_instance):
    """
    Creates the 'items' and 'item_history' tables in the D1 database if they don't already exist.
    """
    if not d1_client_instance:
        print("D1 client is not available. Skipping table creation.")
        return

    print("\nAttempting to create D1 tables if they don't exist...")

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
        print(f"Creating table '{table_name}'...")
        try:
            response = d1_client_instance.d1.database.query(
                database_id=CLOUDFLARE_D1_DATABASE_ID,
                account_id=CLOUDFLARE_ACCOUNT_ID,
                sql=sql_statement
            )
            if response.success:
                print(f"Table '{table_name}' creation command executed successfully (or table already exists).")
            else:
                print(f"Failed to create table '{table_name}'. Errors: {response.errors}")
                success_all = False
        except cloudflare.APIError as e:
            print(f"D1 APIError during table '{table_name}' creation: {e}")
            success_all = False
        except Exception as e:
            print(f"Generic error during table '{table_name}' creation: {e}")
            success_all = False

    if success_all:
        print("D1 table creation process completed successfully for all tables.")
    else:
        print("D1 table creation process encountered errors.")


def get_d1_client():
    print("\nInitializing Cloudflare D1 client...")
    if not CLOUDFLARE_API_TOKEN:
        print("Error: CLOUDFLARE_API_TOKEN is not set. Cannot initialize D1 client.")
        return None
    try:
        client = cloudflare.Cloudflare(api_token=CLOUDFLARE_API_TOKEN)
        print("Cloudflare D1 client initialized successfully.")
        return client
    except Exception as e:
        print(f"Error initializing Cloudflare D1 client: {e}")
        return None

def get_r2_client():
    print("\nInitializing Cloudflare R2 client (Boto3 S3)...")
    if not all([CLOUDFLARE_R2_S3_ENDPOINT_URL, CLOUDFLARE_R2_S3_ACCESS_KEY_ID, CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY]):
        print("Error: Missing one or more R2 S3 configuration variables. Cannot initialize R2 client.")
        print(f"  Endpoint URL set: {'Yes' if CLOUDFLARE_R2_S3_ENDPOINT_URL else 'No'}")
        print(f"  Access Key ID set: {'Yes' if CLOUDFLARE_R2_S3_ACCESS_KEY_ID else 'No'}")
        print(f"  Secret Access Key set: {'Yes' if CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY else 'No'}")
        return None
    try:
        client = boto3.client(
            's3',
            endpoint_url=CLOUDFLARE_R2_S3_ENDPOINT_URL,
            aws_access_key_id=CLOUDFLARE_R2_S3_ACCESS_KEY_ID,
            aws_secret_access_key=CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY,
        )
        print("Cloudflare R2 S3 client initialized successfully.")
        return client
    except ClientError as e:
        print(f"Boto3 ClientError initializing R2 S3 client: {e}")
        return None
    except Exception as e:
        print(f"Generic error initializing R2 S3 client: {e}")
        return None

# Global clients, to be initialized in main
d1_client = None
r2_client = None

if __name__ == "__main__":
    print("Starting data update script...")

    if not check_cloudflare_config():
        print("Cloudflare configuration check failed. Please set the required environment variables.")
        exit(1)

    d1_client = get_d1_client()
    r2_client = get_r2_client()

    if not d1_client:
        print("D1 client initialization failed. D1 related operations will be skipped.")
    if not r2_client:
        print("R2 client initialization failed. R2 related operations will be skipped.")

    create_d1_tables(d1_client) # Ensure tables are created before fetching items

    # Proceed with existing logic, clients can be checked before use in respective functions
    items_to_update_history_for, all_items_data_list = fetch_and_save_items(d1_client_instance=d1_client)

    if all_items_data_list: # Check if there's any data to process
        all_items_data_list = download_item_icons(all_items_data_list, r2_client_instance=r2_client, d1_client_instance=d1_client) # Update icon_downloaded flags

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
            fetch_and_save_histories(items_to_update_history_for, current_prices, d1_client_instance=d1_client)
        else:
            print("No items require history updates based on initial fetch.")
    else:
        print("\nSkipping icon downloading and history fetching because item list was not created or is empty.")

    print("\nData update script finished.")
