"""
=============================================================
  BRAZIL E-COMMERCE (OLIST) -- TRANSFORM
  Data Engineering Layer
=============================================================
Step 2 of 3 in the ETL pipeline.

What this script does:
  - Cleans all 9 raw CSV files
  - Fixes data types, dates, invalid values
  - Removes duplicates and orphan rows
  - Saves cleaned files to e_dataset_cleaned/
=============================================================
"""

import pandas as pd
import numpy as np
import os
import sys
# Always run from the project root folder
os.chdir(os.path.dirname(os.path.abspath(__file__)) + "/..")

# CONFIG
INPUT_DIR     = "data/e_dataset"
OUTPUT_DIR = "data/e_dataset_cleaned"

# ── HELPER FUNCTIONS ──────────────────────────────────────────
def clean_strings(df, cols):
    """Lowercase and strip whitespace from text columns."""
    for col in cols:
        if col in df.columns:
            df[col] = df[col].str.strip().str.lower()
    return df


def parse_dates(df, cols):
    """Convert text columns to real dates. Invalid values become NaT."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def summarise(name, before, after):
    """Print a short cleaning summary."""
    print(f"\n  {name}")
    print(f"    Rows   : {len(before):,} → {len(after):,}  (dropped {len(before)-len(after):,})")
    print(f"    Dupes  : {before.duplicated().sum():,} removed")
    print(f"    Nulls  : {before.isnull().sum().sum():,} → {after.isnull().sum().sum():,}")


def save(df, filename):
    """Save cleaned dataframe to CSV."""
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"    Saved  : {path}")


# ── CLEANING FUNCTIONS ────────────────────────────────────────
def clean_customers(raw):
    df = raw.copy()
    df = clean_strings(df, ["customer_city", "customer_state"])
    df["customer_zip_code_prefix"] = pd.to_numeric(
        df["customer_zip_code_prefix"], errors="coerce"
    )
    df.drop_duplicates(inplace=True)
    df.dropna(subset=["customer_id", "customer_unique_id"], inplace=True)
    return df


def clean_orders(raw):
    df = raw.copy()
    df = clean_strings(df, ["order_status"])
    df = parse_dates(df, [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ])

    # Keep only valid statuses
    valid = ["delivered","shipped","canceled","unavailable",
             "invoiced","processing","created","approved"]
    df = df[df["order_status"].isin(valid)]

    # Fix illogical dates
    bad_approval = (
        df["order_approved_at"].notna() &
        (df["order_approved_at"] < df["order_purchase_timestamp"])
    )
    df.loc[bad_approval, "order_approved_at"] = pd.NaT
    print(f"    Bad approval dates fixed  : {bad_approval.sum():,}")

    bad_delivery = (
        df["order_delivered_customer_date"].notna() &
        df["order_delivered_carrier_date"].notna() &
        (df["order_delivered_customer_date"] < df["order_delivered_carrier_date"])
    )
    df.loc[bad_delivery, "order_delivered_customer_date"] = pd.NaT
    print(f"    Bad delivery dates fixed  : {bad_delivery.sum():,}")

    df.dropna(subset=["order_id", "customer_id"], inplace=True)
    return df


def clean_order_items(raw):
    df = raw.copy()
    df = parse_dates(df, ["shipping_limit_date"])
    df = df[(df["price"] > 0) & (df["freight_value"] >= 0)]

    # Flag price outliers (keep them, just mark)
    q99   = df["price"].quantile(0.99)
    fence = df["price"].quantile(0.75) + 3 * (
            df["price"].quantile(0.75) - df["price"].quantile(0.25))
    df["price_outlier_flag"] = (
        (df["price"] > fence) & (df["price"] > q99)
    ).astype(int)
    print(f"    Price outliers flagged    : {df['price_outlier_flag'].sum():,}")

    df.drop_duplicates(inplace=True)
    df.dropna(subset=["order_id", "product_id", "seller_id"], inplace=True)
    return df


def clean_order_payments(raw):
    df = raw.copy()
    df = clean_strings(df, ["payment_type"])
    valid_types = ["credit_card","boleto","voucher","debit_card","not_defined"]
    df = df[df["payment_type"].isin(valid_types)]
    df["payment_value"]        = pd.to_numeric(df["payment_value"],        errors="coerce")
    df["payment_installments"] = pd.to_numeric(df["payment_installments"], errors="coerce")
    df = df[(df["payment_value"] > 0) & (df["payment_installments"] >= 1)]
    df.drop_duplicates(inplace=True)
    df.dropna(subset=["order_id", "payment_type", "payment_value"], inplace=True)
    return df


def clean_order_reviews(raw):
    df = raw.copy()
    df = parse_dates(df, ["review_creation_date", "review_answer_timestamp"])
    df = df[df["review_score"].between(1, 5)]
    df = df.sort_values("review_creation_date", ascending=False)
    df = df.drop_duplicates(subset="order_id", keep="first")
    for col in ["review_comment_title", "review_comment_message"]:
        if col in df.columns:
            df[col] = df[col].str.strip().replace("", np.nan)
    df.dropna(subset=["review_id", "order_id"], inplace=True)
    return df


def clean_geolocation(raw):
    df = raw.copy()
    df = clean_strings(df, ["geolocation_city", "geolocation_state"])
    df = df[
        df["geolocation_lat"].between(-33.75,  5.27) &
        df["geolocation_lng"].between(-73.99, -28.85)
    ]
    df["geolocation_zip_code_prefix"] = pd.to_numeric(
        df["geolocation_zip_code_prefix"], errors="coerce"
    )
    df.drop_duplicates(inplace=True)
    df.dropna(subset=["geolocation_zip_code_prefix",
                      "geolocation_lat", "geolocation_lng"], inplace=True)
    return df


def clean_products(raw, translation_df):
    df = raw.copy()
    df = clean_strings(df, ["product_category_name"])
    df = df.merge(translation_df, on="product_category_name", how="left")
    for col in ["product_weight_g","product_length_cm",
                "product_height_cm","product_width_cm"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[df[col] <= 0, col] = np.nan
    df["product_photos_qty"] = pd.to_numeric(df["product_photos_qty"], errors="coerce")
    df.loc[df["product_photos_qty"] < 1, "product_photos_qty"] = np.nan
    df.drop_duplicates(subset="product_id", keep="first", inplace=True)
    df.dropna(subset=["product_id"], inplace=True)
    return df


def clean_sellers(raw):
    df = raw.copy()
    df = clean_strings(df, ["seller_city", "seller_state"])
    df["seller_zip_code_prefix"] = pd.to_numeric(
        df["seller_zip_code_prefix"], errors="coerce"
    )
    df.drop_duplicates(subset="seller_id", keep="first", inplace=True)
    df.dropna(subset=["seller_id"], inplace=True)
    return df


def check_integrity(cleaned):
    """Check referential integrity across all cleaned tables."""
    print("\n  Referential Integrity Checks")
    print("  " + "-" * 40)
    checks = [
        ("orders → customers",      cleaned["orders"],        "customer_id", cleaned["customers"]["customer_id"]),
        ("order_items → orders",    cleaned["order_items"],   "order_id",    cleaned["orders"]["order_id"]),
        ("order_payments → orders", cleaned["order_payments"],"order_id",    cleaned["orders"]["order_id"]),
        ("order_reviews → orders",  cleaned["order_reviews"], "order_id",    cleaned["orders"]["order_id"]),
        ("order_items → products",  cleaned["order_items"],   "product_id",  cleaned["products"]["product_id"]),
        ("order_items → sellers",   cleaned["order_items"],   "seller_id",   cleaned["sellers"]["seller_id"]),
    ]
    all_ok = True
    for label, left, col, right in checks:
        orphans = (~left[col].isin(right)).sum()
        status  = "OK" if orphans == 0 else "!!"
        if orphans > 0: all_ok = False
        print(f"    {status}  {label:<35} orphans: {orphans:,}")
    return all_ok


# ── MAIN TRANSFORM FUNCTION ───────────────────────────────────
def transform():
    """
    Main transform function.
    Cleans all 9 files and saves them to e_dataset_cleaned/.
    """
    print("=" * 55)
    print("  STEP 2 — TRANSFORM")
    print("  Cleaning raw data from:", INPUT_DIR)
    print("=" * 55)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load translation file first (needed for products)
    translation = pd.read_csv(f"{INPUT_DIR}/product_category_name_translation.csv")
    translation = clean_strings(translation, ["product_category_name",
                                               "product_category_name_english"])

    # Clean each table
    tables = {
        "customers":      (pd.read_csv(f"{INPUT_DIR}/customers.csv"),      clean_customers),
        "orders":         (pd.read_csv(f"{INPUT_DIR}/orders.csv"),         clean_orders),
        "order_items":    (pd.read_csv(f"{INPUT_DIR}/order_items.csv"),    clean_order_items),
        "order_payments": (pd.read_csv(f"{INPUT_DIR}/order_payments.csv"), clean_order_payments),
        "order_reviews":  (pd.read_csv(f"{INPUT_DIR}/order_reviews.csv"),  clean_order_reviews),
        "geolocation":    (pd.read_csv(f"{INPUT_DIR}/geolocation.csv"),    clean_geolocation),
        "sellers":        (pd.read_csv(f"{INPUT_DIR}/sellers.csv"),        clean_sellers),
    }

    cleaned = {}
    for name, (raw, clean_fn) in tables.items():
        cleaned[name] = clean_fn(raw)
        summarise(name, raw, cleaned[name])
        save(cleaned[name], f"{name}.csv")

    # Products needs the translation dataframe
    raw_products = pd.read_csv(f"{INPUT_DIR}/products.csv")
    cleaned["products"] = clean_products(raw_products, translation)
    summarise("products", raw_products, cleaned["products"])
    save(cleaned["products"], "products.csv")

    # Referential integrity check
    all_ok = check_integrity(cleaned)

    # Final summary
    print(f"\n{'=' * 55}")
    if all_ok:
        print("  Transform complete! All integrity checks passed.")
    else:
        print("  Transform done but some integrity checks failed!")
        print("  Check the orphan counts above.")
    print(f"{'=' * 55}\n")

    return cleaned


# ── RUN STANDALONE ────────────────────────────────────────────
if __name__ == "__main__":
    transform()
