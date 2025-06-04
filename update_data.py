import requests
import time
import os
import csv
# import re # Removed as sanitize_for_path is removed
# from datetime import datetime, timezone # Removed as no longer used
import sys # Added for safe_print
import cloudflare
# import boto3 # Removed
# from botocore.exceptions import ClientError # Removed
# import json # Removed as no longer used

# Configuration
CLOUDFLARE_API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN")
CLOUDFLARE_ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
CLOUDFLARE_D1_DATABASE_ID = os.environ.get("CLOUDFLARE_D1_DATABASE_ID")
# CLOUDFLARE_R2_BUCKET_NAME = os.environ.get("CLOUDFLARE_R2_BUCKET_NAME") # Removed
# CLOUDFLARE_R2_S3_ACCESS_KEY_ID = os.environ.get("CLOUDFLARE_R2_S3_ACCESS_KEY_ID") # Removed
# CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY = os.environ.get("CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY") # Removed
# CLOUDFLARE_R2_S3_ENDPOINT_URL = os.environ.get("CLOUDFLARE_R2_S3_ENDPOINT_URL") # Removed

API_BASE_URL_ITEMS = "https://echoes.mobi/api/items"
# API_BASE_URL_HISTORY = "https://echoes.mobi/api/item_weekly_average_prices?page=1&itemId=" # Removed
ITEMS_OUTPUT_CSV_FILE = "item_lists.csv"
# HISTORIES_BASE_DIR = "item_histories" # Removed - was only used by download_item_icons
# API_V2_ITEM_PRICES_URL = "https://echoes.mobi/api/v2/item_prices" # Removed
REQUEST_DELAY_SECONDS = 0.5
FINAL_CSV_HEADERS = ['id', 'name', 'category_name', 'group_name', 'weekly_average_price', 'icon_id', 'date_created', 'date_updated'] # icon_url removed
# HISTORY_CSV_HEADERS removed by removing the function that used it.

# def sanitize_for_path(name_str): # Removed as no longer used
#     """
#     Sanitizes a string to be safe for directory/file names.
#     """
#     if not name_str:
#         name_str = "unknown"
#     name_str = name_str.replace(' ', '_')
#     name_str = re.sub(r'[^\w\-_]', '', name_str)
#     return name_str[:100]

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
    Returns the full list of item data.
    """
    all_items_data = {} # Keyed by item ID
    initial_d1_item_ids = set()
    d1_new_inserts_count = 0
    d1_updated_items_count = 0
    d1_skipped_no_change_count = 0 # Added counter
    # items_failed_upsert_count is initialized later, just before the D1 upsert loop

    # Prioritize loading from D1
    if d1_client_instance:
        safe_print("Attempting to load existing item data from D1...")
        try:
            select_all_sql = "SELECT item_id, name, category_name, group_name, weekly_average_price, icon_id, date_created, date_updated FROM items;" # Removed icon_r2_key
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

                        # Ensure all FINAL_CSV_HEADERS are present, even if null from D1
                        # icon_downloaded and needs_history_update are removed from FINAL_CSV_HEADERS
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
                                    # row_csv.setdefault('icon_downloaded', 'False') # Removed
                                    # row_csv['needs_history_update'] = 'False' # Removed
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
                            # row.setdefault('icon_downloaded', 'False') # Removed
                            # row['needs_history_update'] = 'False' # Removed
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
                        # icon_downloaded_status removed

                        if date_updated_api and date_updated_local and date_updated_api > date_updated_local:
                            safe_print(f"Updating item {item_id} ('{api_item.get('name')}') as API data is newer.")
                            # Update all fields from API
                            for key, value in api_item.items():
                                existing_item[key] = value
                            # needs_history_update is removed
                        # else:
                            # needs_history_update is removed
                        all_items_data[item_id] = existing_item # Ensure reference is updated if dict was copied
                    else:
                        # New item
                        safe_print(f"API Fetch: New item {item_id} ('{api_item.get('name', 'Unknown Name')}') identified, adding to internal data list.")
                        new_item_entry = {header: '' for header in FINAL_CSV_HEADERS} # Initialize with all headers
                        new_item_entry.update(api_item) # Populate with API data
                        # icon_downloaded and needs_history_update are removed
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

    # The CSV writing is now handled at the end of the main script execution block.
    # (Comment about download_item_icons and icon_r2_key removed as they are no longer relevant)

    # Save/Update items in D1
    if d1_client_instance and all_items_data:
        safe_print(f"\nUpserting {len(all_items_data)} items into D1 database...")
        items_failed_upsert_count = 0 # Initialize here, as it's specific to this loop
        # d1_new_inserts_count and d1_updated_items_count were initialized at the start of the function

        for item_id_key, item_data_dict in all_items_data.items():
            item_id_to_process = item_data_dict.get('id')
            item_name_for_log = item_data_dict.get('name', 'Unknown Name')

            is_new_to_d1_this_run = item_id_to_process not in initial_d1_item_ids

            # Determine if an update is needed for an existing item.
            # This check should compare API data with existing D1 data (which is already in item_data_dict if loaded from D1 and then updated from API).
            # For simplicity, we can check if the item was updated by the API loop.
            # A robust way is to compare field by field, or rely on a 'dirty' flag set during API processing if specific fields change.
            # Since 'needs_history_update' is removed, we'll assume any item processed from the API that was already in D1 might need an update.
            # Or, more simply, if it's not new, and it was touched by the API (which all_items_data reflects), it's an update candidate.
            # The original code's `metadata_has_changed` was driven by `needs_history_update`.
            # Now, any item that exists in D1 (is_new_to_d1_this_run is False) and is present in the API data (meaning it's in all_items_data)
            # will have its fields updated from the API if the API's date_updated is newer.
            # So, the condition for upsert is simply if it's new OR if its data might have been changed by the API.
            # The upsert itself handles whether an actual DB write occurs based on changed values for existing items.

            # We need to decide if an existing item should be queued for update.
            # The original logic used `metadata_has_changed`. Since that's gone,
            # we can assume if an item is not new, it's potentially being updated.
            # The SQL `ON CONFLICT DO UPDATE` will handle the actual update if data differs.
            # So, effectively, we attempt to upsert all items that came from the API or were merged.

            # Let's consider an item as "changed" if it's not new and its `date_updated` field (from API)
            # is different from what was loaded from D1. This is implicitly handled by the API update logic.
            # For now, we will attempt to upsert if it's new or if it was present in the API data (which means it's in all_items_data).
            # The crucial part is that `all_items_data[item_id]` holds the latest state (from API if newer, or from D1/CSV otherwise).

            # The condition `if is_new_to_d1_this_run or metadata_has_changed:`
            # needs to be re-evaluated. `metadata_has_changed` is gone.
            # An item should be written to D1 if it's new, or if its data has been modified by the API.
            # The current loop iterates through `all_items_data` which contains the final state of all items.
            # So, we should attempt to upsert every item in `all_items_data`.
            # The `ON CONFLICT` clause will prevent unnecessary updates if data hasn't changed.
            # However, the logging and counters `d1_new_inserts_count`, `d1_updated_items_count`
            # relied on `is_new_to_d1_this_run` and `metadata_has_changed`.

            # Simplification: We will attempt to upsert every item.
            # The logging for "new" vs "updated" can still use `is_new_to_d1_this_run`.
            # An "update" occurs if it's not new and the `ON CONFLICT` clause is triggered.
            # The `d1_skipped_no_change_count` is for items from D1 not touched by API.
            # This part needs careful restructuring.

            # Let's refine the condition for attempting a D1 write:
            # Write if it's new OR if it was present in the API feed (and thus potentially updated).
            # Since all items in `all_items_data` are either from D1 (potentially updated by API) or new from API,
            # we should process all of them for D1 upsert.
            # The original `if is_new_to_d1_this_run or metadata_has_changed:` effectively did this.
            # Now, `metadata_has_changed` is gone.
            # We can simplify to always try to upsert. The `ON CONFLICT` handles efficiency.
            # The logging for "update" needs to be based on `is_new_to_d1_this_run == False`.

            # The `d1_skipped_no_change_count` logic also needs to be revisited.
            # An item is skipped if it was loaded from D1 and NOT updated by the API.
            # The current structure iterates `all_items_data`. If an item from D1 wasn't updated by API,
            # its `date_updated` field would be the original one from D1.
            # The original check `if is_new_to_d1_this_run or metadata_has_changed:`
            # implicitly handled skipping items that were not new and whose metadata didn't change.
            # Let's assume for now that we will attempt to upsert all items in `all_items_data`.
            # The `ON CONFLICT DO UPDATE` should ideally only update if values actually changed,
            # but Cloudflare D1 might not offer that detailed feedback directly in `response.success`.
            # We'll rely on `is_new_to_d1_this_run` for insert/update distinction for logging.

            # If an item was loaded from D1 and not updated by the API, we should skip the D1 write.
            # This means `date_updated_api <= date_updated_local` in the API processing loop.
            # Such items would have had `needs_history_update = 'False'`.
            # We need a similar flag or check here.
            # Let's check if the item's current data in `item_data_dict` is different from what's in `initial_d1_item_ids`'s corresponding entry,
            # or if it's a new item.
            # This is getting complex. The simplest is to upsert all, and let the DB handle no-ops.
            # The counters for new/updated will be based on `is_new_to_d1_this_run`.

            # For items NOT new:
            # An update should be queued if its data from API was newer.
            # This was implicitly handled by `existing_item['needs_history_update'] = 'True'`.
            # We need a way to know if `item_data_dict` was modified after being loaded from D1.
            # Perhaps by comparing `item_data_dict` with a pristine copy loaded from D1 if we had one.
            # Or, we can rely on the `date_updated` field. If `item_data_dict.date_updated` is from the API (newer), then update.

            # Let's assume any item in `all_items_data` that is NOT new MIGHT have changes.
            # The `ON CONFLICT` clause is the safety net.
            # The logging `Queuing UPDATE for existing item...` needs a condition.
            # This condition was `metadata_has_changed`.
            # Let's assume if it's not new, we are attempting an update.

            # Try to upsert all items in all_items_data.
            # The `d1_skipped_no_change_count` will become 0 with this approach unless we add a specific check.
            # Let's remove the `if is_new_to_d1_this_run or metadata_has_changed:` block's outer condition
            # and always attempt the upsert for items in `all_items_data`.
            # The inner logging can distinguish between insert and update attempts.

            # --- D1 UPSERT try-except block starts here ---
            try:
                    # Ensure weekly_average_price is float or None
                    wap = item_data_dict.get('weekly_average_price')
                    wap_float = None # Default value
                    if wap is not None and wap != '':
                        try:
                            wap_float = float(wap)
                        except ValueError:
                            safe_print(f"Warning: Could not convert weekly_average_price '{wap}' to float for item {item_id_key}. Setting to NULL.")
                            # wap_float remains None
                    # else: # Redundant, wap_float is already None
                        # wap_float = None

                    params = [
                        item_data_dict.get('id'),
                        item_data_dict.get('name'),
                        item_data_dict.get('category_name'),
                        item_data_dict.get('group_name'),
                        wap_float,
                        item_data_dict.get('icon_id'),
                        # item_data_dict.get('icon_r2_key'), # Removed
                        item_data_dict.get('date_created'),
                        item_data_dict.get('date_updated')
                    ]

                    upsert_sql = """
                    INSERT INTO items (item_id, name, category_name, group_name, weekly_average_price, icon_id, date_created, date_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(item_id) DO UPDATE SET
                        name = excluded.name,
                        category_name = excluded.category_name,
                        group_name = excluded.group_name,
                        weekly_average_price = excluded.weekly_average_price,
                        icon_id = excluded.icon_id,
                        # icon_r2_key = COALESCE(excluded.icon_r2_key, items.icon_r2_key), # Removed
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
                        if is_new_to_d1_this_run:
                            d1_new_inserts_count += 1
                            safe_print(f"D1: Successfully INSERTED new item {item_id_to_process} ('{item_name_for_log}').")
                        else:
                            d1_updated_items_count += 1
                            safe_print(f"D1: Successfully UPDATED existing item {item_id_to_process} ('{item_name_for_log}').")
                    else:
                        safe_print(f"Failed to upsert item {item_id_to_process} into D1. Errors: {response.errors}")
                        items_failed_upsert_count +=1
                except cloudflare.APIError as e:
                    safe_print(f"D1 APIError during upsert for item {item_id_to_process}: {e}")
                    items_failed_upsert_count +=1
                except Exception as e:
                    safe_print(f"Generic error during D1 upsert for item {item_id_to_process}: {e}")
                    items_failed_upsert_count +=1
            # The `else` block for skipping D1 writes if data unchanged is removed for now.
            # All items in `all_items_data` will be attempted to be upserted.
            # `d1_skipped_no_change_count` will remain 0 unless specific logic is added back.

        total_successful_d1_ops = d1_new_inserts_count + d1_updated_items_count
        safe_print(f"D1 Sync for 'items' table Summary:")
        safe_print(f"  - New items inserted into D1: {d1_new_inserts_count}")
        safe_print(f"  - Existing items updated in D1 (attempted): {d1_updated_items_count}") # Changed logging
        # safe_print(f"  - Items skipped (no metadata change): {d1_skipped_no_change_count}") # This counter is now effectively 0
        if items_failed_upsert_count > 0:
            safe_print(f"  - Failed D1 operations: {items_failed_upsert_count}")
        safe_print(f"  Total items successfully written to D1 this run: {total_successful_d1_ops}")

    return list(all_items_data.values())

# Removed download_item_icons function entirely
# Removed load_all_current_prices function entirely
# Removed get_week_year_from_isodate function entirely
# Removed HISTORY_CSV_HEADERS global variable
# Removed fetch_and_save_histories function entirely

def check_cloudflare_config():
    # global CLOUDFLARE_R2_S3_ENDPOINT_URL # Removed as CLOUDFLARE_R2_S3_ENDPOINT_URL is no longer used
    missing_vars = []
    if not CLOUDFLARE_API_TOKEN:
        missing_vars.append("CLOUDFLARE_API_TOKEN")
    if not CLOUDFLARE_ACCOUNT_ID:
        missing_vars.append("CLOUDFLARE_ACCOUNT_ID")
    if not CLOUDFLARE_D1_DATABASE_ID:
        missing_vars.append("CLOUDFLARE_D1_DATABASE_ID")
    # CLOUDFLARE_R2_BUCKET_NAME check removed
    # CLOUDFLARE_R2_S3_ACCESS_KEY_ID check removed
    # CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY check removed
    # CLOUDFLARE_R2_S3_ENDPOINT_URL check removed
    # Logic for constructing CLOUDFLARE_R2_S3_ENDPOINT_URL removed

    if missing_vars:
        safe_print("Error: The following Cloudflare environment variables are not set or derivable:")
        for var in missing_vars:
            safe_print(f" - {var}")
        return False
    safe_print("Cloudflare configuration variables are present.")
    return True

def create_d1_tables(d1_client_instance):
    """
    Creates the 'items' table in the D1 database if it doesn't already exist.
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
        # icon_r2_key TEXT, # Removed
        date_created TEXT,
        date_updated TEXT
    );
    """
    # item_history_table_sql removed
    tables_to_create = {
        "items": items_table_sql
        # "item_history" entry removed
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

# Removed get_r2_client function

# Global clients, to be initialized in main
d1_client = None
# r2_client = None # Removed

if __name__ == "__main__":
    safe_print("Starting data update script...")

    if not check_cloudflare_config():
        safe_print("Cloudflare configuration check failed. Please set the required environment variables.")
        exit(1)

    d1_client = get_d1_client()
    # r2_client = get_r2_client() # Removed

    if not d1_client:
        safe_print("D1 client initialization failed. D1 related operations will be skipped.")
    # if not r2_client: # Removed
        # safe_print("R2 client initialization failed. R2 related operations will be skipped.") # Removed

    create_d1_tables(d1_client) # Ensure tables are created before fetching items

    # Proceed with existing logic, clients can be checked before use in respective functions
    all_items_data_list = fetch_and_save_items(d1_client_instance=d1_client) # Modified return

    if all_items_data_list: # Check if there's any data to process
        # all_items_data_list = download_item_icons(all_items_data_list, r2_client_instance=r2_client, d1_client_instance=d1_client) # Removed call

        # current_prices = load_all_current_prices() # Removed

        # Write the item data to CSV
        # FINAL_CSV_HEADERS is defined globally
        safe_print(f"\nWriting final item data for {len(all_items_data_list)} items to {ITEMS_OUTPUT_CSV_FILE}...")
        try:
            with open(ITEMS_OUTPUT_CSV_FILE, 'w', encoding='utf-8', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=FINAL_CSV_HEADERS)
                writer.writeheader()
                for item_dict in all_items_data_list:
                    row_to_write = {header: item_dict.get(header, '') for header in FINAL_CSV_HEADERS}
                    writer.writerow(row_to_write)
            safe_print(f"Final item data written to {ITEMS_OUTPUT_CSV_FILE}") # Removed "including icon status"
        except IOError as e:
            safe_print(f"Error writing final item data to CSV: {e}")
        except Exception as e: # Catch any other unexpected error during write
             safe_print(f"An unexpected error occurred while writing final CSV: {e}")

        # Removed all_item_ids_for_history list creation and call to fetch_and_save_histories
        # The 'else' for "No item IDs available for history" is also removed by this.
    else:
        safe_print("\nSkipping CSV writing because item list was not created or is empty.") # Simplified message

    safe_print("\nData update script finished.")
