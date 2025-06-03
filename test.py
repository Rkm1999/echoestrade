import os
import cloudflare # For D1
import boto3 # For R2 S3 Compatible API
from botocore.exceptions import ClientError # For boto3 error handling
import datetime

# --- Configuration - Read from Environment Variables ---
API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN")
ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
D1_DATABASE_ID = os.environ.get("CLOUDFLARE_D1_DATABASE_ID")
R2_BUCKET_NAME = os.environ.get("CLOUDFLARE_R2_BUCKET_NAME")

# New variables for R2 S3 Compatible API
R2_S3_ACCESS_KEY_ID = os.environ.get("CLOUDFLARE_R2_S3_ACCESS_KEY_ID")
R2_S3_SECRET_ACCESS_KEY = os.environ.get("CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY")
R2_S3_ENDPOINT_URL = os.environ.get("CLOUDFLARE_R2_S3_ENDPOINT_URL") # Loaded globally here

# --- Helper to Check Configuration ---
def check_config():
    global R2_S3_ENDPOINT_URL # Declare upfront that we might modify the global R2_S3_ENDPOINT_URL

    missing_vars = []
    # Cloudflare Native Client Vars
    if not API_TOKEN:
        missing_vars.append("CLOUDFLARE_API_TOKEN")
    if not ACCOUNT_ID:
        missing_vars.append("CLOUDFLARE_ACCOUNT_ID")
    
    # D1 Vars
    if not D1_DATABASE_ID:
        missing_vars.append("CLOUDFLARE_D1_DATABASE_ID")
    
    # R2 Vars (Bucket name is always needed)
    if not R2_BUCKET_NAME:
        missing_vars.append("CLOUDFLARE_R2_BUCKET_NAME")

    # R2 S3 Specific Vars
    if not R2_S3_ACCESS_KEY_ID:
        missing_vars.append("CLOUDFLARE_R2_S3_ACCESS_KEY_ID")
    if not R2_S3_SECRET_ACCESS_KEY:
        missing_vars.append("CLOUDFLARE_R2_S3_SECRET_ACCESS_KEY")
    
    # Check and potentially construct R2_S3_ENDPOINT_URL (now refers to global)
    if not R2_S3_ENDPOINT_URL:
        if ACCOUNT_ID:
            # This assignment now modifies the global R2_S3_ENDPOINT_URL
            R2_S3_ENDPOINT_URL = f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com"
            print(f"NOTE: CLOUDFLARE_R2_S3_ENDPOINT_URL was not set, constructed and updated globally to: {R2_S3_ENDPOINT_URL}")
        else:
            missing_vars.append("CLOUDFLARE_R2_S3_ENDPOINT_URL (and CLOUDFLARE_ACCOUNT_ID to construct it is also missing)")

    if missing_vars:
        print("Error: The following environment variables are not set or derivable:")
        for var in missing_vars:
            print(f" - {var}")
        print("Please set them correctly in your environment before running the script.")
        return False
    return True

# --- Initialize Cloudflare Client (for D1) ---
def get_cf_client():
    print("\nInitializing Cloudflare client (for D1)...")
    try:
        client = cloudflare.Cloudflare(api_token=API_TOKEN)
        print("Cloudflare client initialized successfully.")
        return client
    except Exception as e:
        print(f"Error initializing Cloudflare client: {e}")
        return None

# --- D1 Test Function (Should be working from previous version) ---
def test_d1(client):
    if not client:
        return

    print("\n--- Starting D1 Test ---")
    test_table_name = "sdk_smoke_test_d1"
    current_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    test_message = f"D1 SDK test message at {current_time}"

    # 1. Create table
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {test_table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message TEXT,
        created_at TEXT
    );
    """
    print(f"Attempting to create table '{test_table_name}' if it doesn't exist...")
    try:
        response_create = client.d1.database.query(
            D1_DATABASE_ID,
            account_id=ACCOUNT_ID,
            sql=create_table_sql
        )
        if hasattr(response_create, 'errors') and response_create.errors and len(response_create.errors) > 0:
            print(f"Failed to create table. Errors: {response_create.errors}")
            return
        elif hasattr(response_create, 'success') and not response_create.success and response_create.success is not None :
             print(f"Failed to create table. Success: False. Response: {response_create}")
             return
        else:
            print(f"Table '{test_table_name}' creation command executed successfully (or table already exists).")
    except cloudflare.APIError as e:
        print(f"D1 APIError during table creation: {e}")
        return
    except Exception as e:
        print(f"Generic error during table creation: {e}")
        return

    # 2. Insert a row
    insert_sql = f"INSERT INTO {test_table_name} (message, created_at) VALUES (?, ?);"
    print(f"Attempting to insert a row into '{test_table_name}' with message: '{test_message}'...")
    try:
        response_insert = client.d1.database.query(
            D1_DATABASE_ID,
            account_id=ACCOUNT_ID,
            sql=insert_sql,
            params=[test_message, current_time]
        )
        if hasattr(response_insert, 'errors') and response_insert.errors and len(response_insert.errors) > 0:
            print(f"Failed to insert row. Errors: {response_insert.errors}")
            return
        elif hasattr(response_insert, 'success') and not response_insert.success and response_insert.success is not None:
             print(f"Failed to insert row. Success: False. Response: {response_insert}")
             return
        else:
            print(f"Row inserted successfully.")
            if hasattr(response_insert, 'meta'):
                print(f"Insert meta: {response_insert.meta}")
    except cloudflare.APIError as e:
        print(f"D1 APIError during insert: {e}")
        return
    except Exception as e:
        print(f"Generic error during insert: {e}")
        return

    # 3. Query the inserted row
    select_sql = f"SELECT id, message, created_at FROM {test_table_name} ORDER BY id DESC LIMIT 1;"
    print(f"Attempting to query the latest row from '{test_table_name}'...")
    try:
        response_select = client.d1.database.query(
            D1_DATABASE_ID,
            account_id=ACCOUNT_ID,
            sql=select_sql
        )
        print(f"DEBUG: Raw D1 SELECT response object: {response_select}")

        query_errors = []
        actual_results_list = []
        if response_select and hasattr(response_select, 'errors') and response_select.errors:
            query_errors = response_select.errors
        if response_select and hasattr(response_select, 'result') and response_select.result and \
           isinstance(response_select.result, list) and len(response_select.result) > 0 and \
           hasattr(response_select.result[0], 'results') and response_select.result[0].results is not None:
            actual_results_list = response_select.result[0].results
        elif response_select and hasattr(response_select, 'success') and response_select.success is False:
             if not query_errors:
                query_errors.append({"code": "Unknown", "message": "Query reported overall success as False."})

        if not query_errors and actual_results_list:
            retrieved_row = actual_results_list[0]
            print(f"Successfully queried latest row: {retrieved_row}")
            retrieved_message = retrieved_row.get('message')
            print(f"  Retrieved message from DB: '{retrieved_message}'")
            print(f"  Expected message for this run: '{test_message}'")
            if retrieved_message == test_message:
                print("  SUCCESS: Retrieved message matches the expected message for this specific test run!")
            else:
                print("  NOTE: Retrieved message does NOT exactly match the message inserted in THIS specific test run.")
            print("D1 Test: Basic SELECT operation is working!")
        elif not query_errors:
            print(f"Query was successful (no errors from API) but no results were found in the parsed 'actual_results_list'.")
        else:
            print(f"Failed to query row. Errors: {query_errors}. Full response: {response_select}")
    except cloudflare.APIError as e:
        print(f"D1 APIError during select: {e}")
    except Exception as e:
        print(f"Generic error during select: {e}")
    print("--- D1 Test Finished ---")
    print(f"(Note: The table '{test_table_name}' and its data remain in your D1 database for verification.)")


# --- R2 Test Function (Using Boto3 for S3 Compatible API) ---
def test_r2():
    print("\n--- Starting R2 Test (using S3 Compatible API via Boto3) ---")
    
    if not all([R2_S3_ACCESS_KEY_ID, R2_S3_SECRET_ACCESS_KEY, R2_S3_ENDPOINT_URL, R2_BUCKET_NAME]):
        print("R2 S3 Test Skipped: Missing one or more required R2 S3 environment variables or R2_BUCKET_NAME.")
        # Print status of each R2 S3 var for clarity
        print(f"  R2_S3_ACCESS_KEY_ID: {'Set' if R2_S3_ACCESS_KEY_ID else 'Missing'}")
        print(f"  R2_S3_SECRET_ACCESS_KEY: {'Set' if R2_S3_SECRET_ACCESS_KEY else 'Missing'}")
        print(f"  R2_S3_ENDPOINT_URL: {R2_S3_ENDPOINT_URL if R2_S3_ENDPOINT_URL else 'Missing/Not Constructed'}") # Check if it got constructed
        print(f"  R2_BUCKET_NAME: {R2_BUCKET_NAME if R2_BUCKET_NAME else 'Missing'}")
        return

    object_key = f"s3_sdk_smoke_test_r2_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}.txt"
    object_data = b"R2 S3 Compatible API test: Hello from Python Boto3! This is a test file."
    content_type = "text/plain"

    print(f"Attempting to upload '{object_key}' to R2 bucket '{R2_BUCKET_NAME}' via S3 API (Endpoint: {R2_S3_ENDPOINT_URL})...")
    
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=R2_S3_ENDPOINT_URL,
            aws_access_key_id=R2_S3_ACCESS_KEY_ID,
            aws_secret_access_key=R2_S3_SECRET_ACCESS_KEY,
        )
        
        s3_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=object_key,
            Body=object_data,
            ContentType=content_type
        )
        
        print(f"Successfully uploaded '{object_key}' to R2 bucket '{R2_BUCKET_NAME}' using S3 compatible API.")
        print("R2 S3 Test: Basic upload seems to be working!")

        r2_public_url_base = os.environ.get("CLOUDFLARE_R2_PUBLIC_URL_BASE")
        if r2_public_url_base:
            print(f"Potential public URL (if bucket is public and URL base is correct): {r2_public_url_base.rstrip('/')}/{R2_BUCKET_NAME}/{object_key}")
            print(f"  Alternatively, using S3 endpoint: {R2_S3_ENDPOINT_URL}/{R2_BUCKET_NAME}/{object_key} (if bucket is public)")
        else:
            print(f"You can verify the object '{object_key}' in the Cloudflare R2 dashboard for bucket '{R2_BUCKET_NAME}'.")
            print(f"  It might be accessible via: {R2_S3_ENDPOINT_URL}/{R2_BUCKET_NAME}/{object_key} (if bucket is public)")

    except ClientError as e:
        print(f"R2 S3 Boto3 ClientError during upload: {e}")
    except Exception as e:
        print(f"Generic error during R2 S3 upload: {e}")
    print("--- R2 S3 Test Finished ---")
    print(f"(Note: If successful, the object '{object_key}' remains in your R2 bucket for verification.)")

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting Cloudflare Setup Test Script...")

    if not check_config():
        exit(1)

    cf_native_client = get_cf_client()

    if cf_native_client:
        test_d1(cf_native_client)
    else:
        print("D1 Test Skipped as Cloudflare native client could not be initialized.")
    
    test_r2() 

    print("\nTest script completed. Check the output above for success or error messages.")