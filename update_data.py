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
    Fetches item data from the paginated API and saves it to a CSV file.
    Uses specific sorting and existence filters.
    Returns True if the file was created and has data, False otherwise.
    """
    current_page = 1
    headers = {'accept': 'text/csv'}
    first_write_done = False

    print("Starting to fetch item data with refined parameters...")

    if os.path.exists(ITEMS_OUTPUT_CSV_FILE):
        print(f"Removing existing {ITEMS_OUTPUT_CSV_FILE} for a fresh fetch.")
        os.remove(ITEMS_OUTPUT_CSV_FILE)
        
    while True:
        params = {
            'page': current_page,
            'order[name]': 'asc',
            'order[categoryName]': 'asc',
            'order[groupName]': 'asc',
            'exists[weekly_average_price]': 'true'
        }
        
        try:
            print(f"Fetching item list page {current_page}...")
            response = requests.get(API_BASE_URL_ITEMS, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                response_text_stripped = response.text.strip()
                lines = response_text_stripped.splitlines()

                if not response_text_stripped: # Handles completely empty response after stripping
                    print(f"Page {current_page} is effectively empty. Assuming no more item data.")
                    break
                
                if current_page == 1:
                    # Write whatever is received for the first page (includes header)
                    print(f"Writing initial data (page {current_page}) to {ITEMS_OUTPUT_CSV_FILE}...")
                    with open(ITEMS_OUTPUT_CSV_FILE, 'w', encoding='utf-8', newline='') as f:
                        f.write(response_text_stripped + '\n')
                    first_write_done = True
                    # Check if only header was written (or page was empty after all)
                    if len(lines) <= 1:
                        print("First page contained only a header or was empty. No data rows to process.")
                        # Keep the file with header, but signal no actual data for history processing
                        return False # Or True if a header-only file is considered "created"
                else:
                    # For subsequent pages, if only header or empty, it's the end
                    if len(lines) <= 1:
                        print(f"No new data rows on page {current_page} (only header or empty). Ending pagination.")
                        break
                    
                    # Skip header line (lines[0]) and append the rest
                    data_to_append = "\n".join(lines[1:])
                    if data_to_append: # Ensure there's actual data to append
                        print(f"Appending data from page {current_page} to {ITEMS_OUTPUT_CSV_FILE}...")
                        with open(ITEMS_OUTPUT_CSV_FILE, 'a', encoding='utf-8', newline='') as f:
                            f.write(data_to_append + '\n')
                    else:
                        # This case should ideally be caught by len(lines) <= 1, but as a safeguard:
                        print(f"No actual data rows on page {current_page} after removing header. Ending pagination.")
                        break
                
                # Optional: A more dynamic way to check for the last page could be if the number of items
                # returned is less than a known 'per_page' limit from the API, if such a limit is consistent.
                # For now, the len(lines) <= 1 check for page > 1 is the primary stop condition.

            else:
                print(f"Error fetching item list page {current_page}: Status code {response.status_code}")
                print(f"Response content: {response.text[:200]}")
                return False # Fetch failed
        
        except requests.exceptions.RequestException as e:
            print(f"Request for item list failed on page {current_page}: {e}")
            return False # Fetch failed
        
        current_page += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    if first_write_done:
        # Check if the file actually has content beyond a potential header
        with open(ITEMS_OUTPUT_CSV_FILE, 'r', encoding='utf-8') as f:
            final_lines = f.readlines()
            if len(final_lines) > 1:
                print(f"Finished writing item data to {ITEMS_OUTPUT_CSV_FILE}")
                return True
            else:
                print(f"{ITEMS_OUTPUT_CSV_FILE} was created but contains no data rows (only header).")
                return False # No actual item data for history processing
    else:
        print("No item data was fetched or written.")
        return False

def fetch_and_save_histories(item_list_csv_path):
    """
    Fetches item history for each item in the provided CSV file and saves it.
    """
    print(f"\nStarting to fetch item histories using list from: {item_list_csv_path}")
    if not os.path.exists(item_list_csv_path):
        print(f"Error: Item list file '{item_list_csv_path}' not found.")
        return

    os.makedirs(HISTORIES_BASE_DIR, exist_ok=True) 
    
    headers = {'accept': 'text/csv'}
    items_processed_count = 0
    items_skipped_count = 0
    items_failed_count = 0

    try:
        with open(item_list_csv_path, mode='r', encoding='utf-8', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            required_columns = ['id', 'category_name', 'group_name', 'name']
            # Check if all required columns are in fieldnames
            if not all(col in reader.fieldnames for col in required_columns):
                missing_cols = [col for col in required_columns if col not in reader.fieldnames]
                print(f"Error: CSV file is missing required columns: {', '.join(missing_cols)}.")
                return

            for row_num, row in enumerate(reader, 1):
                item_id = row.get('id')
                # Provide default values if specific keys might be missing, though DictReader handles missing keys as None
                category = row.get('category_name', 'UnknownCategory')
                group = row.get('group_name', 'UnknownGroup')
                name = row.get('name', f'UnknownItem_{item_id if item_id else "UnknownID"}')


                if not item_id:
                    print(f"Skipping row {row_num} due to missing item ID.")
                    items_skipped_count +=1
                    continue

                sanitized_item_id = sanitize_for_path(item_id) # Sanitize ID for filename too
                sanitized_category = sanitize_for_path(category)
                sanitized_group = sanitize_for_path(group)
                sanitized_name = sanitize_for_path(name)

                item_dir = os.path.join(HISTORIES_BASE_DIR, sanitized_category, sanitized_group, sanitized_name)
                os.makedirs(item_dir, exist_ok=True)
                
                history_file_path = os.path.join(item_dir, f"{sanitized_item_id}_history.csv")

                if os.path.exists(history_file_path):
                    print(f"History for item {item_id} ('{name}') already exists at '{history_file_path}'. Skipping.")
                    items_skipped_count +=1
                    continue
                
                history_api_url = f"{API_BASE_URL_HISTORY}{item_id}" # item_id here should be the original, not sanitized
                print(f"Fetching history for item {item_id} ('{name}')...")
                
                try:
                    response = requests.get(history_api_url, headers=headers, timeout=30)
                    if response.status_code == 200:
                        response_text_stripped = response.text.strip()
                        history_lines = response_text_stripped.splitlines()
                        if response_text_stripped and len(history_lines) > 1: 
                            with open(history_file_path, 'w', encoding='utf-8', newline='') as hf:
                                hf.write(response_text_stripped + '\n') 
                            print(f"Successfully saved history for item {item_id} to '{history_file_path}'")
                            items_processed_count += 1
                        else:
                            print(f"No history data (or only header) returned for item {item_id} ('{name}'). Skipping file write.")
                            items_skipped_count += 1 
                    else:
                        print(f"Error fetching history for item {item_id} ('{name}'): Status {response.status_code}")
                        items_failed_count += 1
                
                except requests.exceptions.RequestException as e:
                    print(f"Request failed for item history {item_id} ('{name}'): {e}")
                    items_failed_count += 1
                
                time.sleep(REQUEST_DELAY_SECONDS) 

    except FileNotFoundError:
        print(f"Error: Could not find the item list file at '{item_list_csv_path}'.")
        return
    except Exception as e:
        print(f"An unexpected error occurred while processing histories: {e}")
        return
        
    print("\n--- Item History Fetch Summary ---")
    print(f"Successfully processed and saved: {items_processed_count}")
    print(f"Skipped (already exist or no data/header only): {items_skipped_count}")
    print(f"Failed to fetch: {items_failed_count}")
    print("---------------------------------")

if __name__ == "__main__":
    if fetch_and_save_items(): 
        fetch_and_save_histories(ITEMS_OUTPUT_CSV_FILE)
    else:
        print("\nSkipping history fetching because item list was not created, is empty, or contains only a header.")
