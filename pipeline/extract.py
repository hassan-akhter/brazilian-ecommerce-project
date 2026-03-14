"""
=============================================================
  BRAZIL E-COMMERCE (OLIST) -- EXTRACT
  Data Engineering Layer
=============================================================

Step 1 of 3 in the ETL pipeline.

What this script does:
  - Loads all 9 raw CSV files from data/e_dataset/
  - Validates that all files exist
  - Checks basic quality (row counts, column names)
  - Reports any issues before cleaning starts
"""

import pandas as pd
import os
import sys

# Always run from the project root folder
os.chdir(os.path.dirname(os.path.abspath(__file__)) + "/..")

# CONFIG
RAW_DIR     = "data/e_dataset"
CLEANED_DIR = "data/e_dataset_cleaned"

# Expected files and their minimum required columns
EXPECTED_FILES = {
    "customers.csv": [
        "customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"
    ],
    "orders.csv": [
        "order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"
    ],
    "order_items.csv": [
        "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"
    ],
    "order_payments.csv": [
        "order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"
    ],
    "order_reviews.csv": [
        "review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp"
    ],
    "geolocation.csv": [
        "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"
    ],
    "product_category_name_translation.csv": [
        "product_category_name", "product_category_name_english"
    ],
    "products.csv": [
        "product_id", "product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    ],
    "sellers.csv": [
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
    ],
}


# Functions
def check_file_exists(filename):
    """Check if a file exists in the raw data folder."""
    path = os.path.join(RAW_DIR, filename)
    return os.path.exists(path)


def load_file(filename):
    """Load a CSV file into a dataframe."""
    path = os.path.join(RAW_DIR, filename)
    df = pd.read_csv(path)
    return df


def check_columns(df, filename, required_cols):
    """Check that all required columns exist in the file."""
    missing = [col for col in required_cols if col not in df.columns]
    return missing


def extract():
    """
    Main extract function.
    Loads all raw files, validates them, and returns a dictionary
    of dataframes ready for the transform step.
    """
    print("=" * 55)
    print("  STEP 1 — EXTRACT")
    print("  Loading raw CSV files from:", RAW_DIR)
    print("=" * 55)

    # Check the raw data folder exists
    if not os.path.exists(RAW_DIR):
        print(f"\n  ERROR: Folder '{RAW_DIR}' not found!")
        print(f"  Make sure your raw CSV files are in '{RAW_DIR}/'")
        sys.exit(1)

    dataframes = {}  # store all loaded dataframes here
    all_ok = True    # track if everything passed validation

    for filename, required_cols in EXPECTED_FILES.items():

        print(f"\n  Loading: {filename}")

        # 1. Check file exists
        if not check_file_exists(filename):
            print(f"    ERROR: File not found!")
            all_ok = False
            continue

        # 2. Load the file
        df = load_file(filename)
        print(f"    Rows    : {len(df):,}")
        print(f"    Columns : {len(df.columns)}")

        # 3. Check required columns exist
        missing_cols = check_columns(df, filename, required_cols)
        if missing_cols:
            print(f"    ERROR: Missing columns: {missing_cols}")
            all_ok = False
            continue

        # 4. Check for completely empty files
        if len(df) == 0:
            print(f"    ERROR: File is empty!")
            all_ok = False
            continue

        # 5. Report null counts per column
        null_counts = df.isnull().sum()
        cols_with_nulls = null_counts[null_counts > 0]
        if len(cols_with_nulls) > 0:
            print(f"    Nulls   : {cols_with_nulls.sum():,} total")
        else:
            print(f"    Nulls   : None")

        # 6. Report duplicate rows
        dupes = df.duplicated().sum()
        print(f"    Dupes   : {dupes:,}")

        # All good — store the dataframe
        print(f"    Status  : OK")
        name = filename.replace(".csv", "")
        dataframes[name] = df

    # Final summary
    print(f"\n{'=' * 55}")
    if all_ok:
        print(f"  Extract complete! {len(dataframes)}/9 files loaded.")
    else:
        print(f"  Extract failed! Fix the errors above and try again.")
        sys.exit(1)
    print(f"{'=' * 55}\n")

    return dataframes


# RUN STANDALONE
if __name__ == "__main__":
    dataframes = extract()
