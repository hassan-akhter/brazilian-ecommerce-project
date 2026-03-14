"""
=============================================================
  BRAZIL E-COMMERCE (OLIST) -- LOAD
  Data Engineering Layer
=============================================================
Step 3 of 3 in the ETL pipeline.

What this script does:
  - Connects to PostgreSQL using credentials from pass.env
  - Creates all tables using schema.sql
  - Loads all 8 cleaned CSV files into the database
  - Verifies row counts after loading
=============================================================
"""

import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
import os
import sys

# Always run from the project root folder
os.chdir(os.path.dirname(os.path.abspath(__file__)) + "/..")

# ── CONFIG ────────────────────────────────────────────────────
CLEANED_DIR = "data/e_dataset_cleaned"
SCHEMA_FILE = "pipeline/schema.sql"

# Load database credentials from pass.env
load_dotenv("pass.env")

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME",     "olist_db"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD"),
}

# Load tables in this order (respects foreign key constraints)
LOAD_ORDER = [
    "customers",
    "sellers",
    "products",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "geolocation",
]


# ── FUNCTIONS ─────────────────────────────────────────────────
def get_engine():
    """Create and return a database connection."""
    url = sa.URL.create(
        drivername = "postgresql+psycopg2",
        username   = DB_CONFIG["user"],
        password   = DB_CONFIG["password"],
        host       = DB_CONFIG["host"],
        port       = DB_CONFIG["port"],
        database   = DB_CONFIG["database"],
    )
    return sa.create_engine(url)


def test_connection(engine):
    """Test that the database connection works."""
    with engine.connect() as conn:
        result = conn.execute(sa.text("SELECT version();"))
        version = result.fetchone()[0]
        print(f"  Connected to: {version[:50]}...")
    return True


def run_schema(engine):
    """Create all tables by running schema.sql."""
    if not os.path.exists(SCHEMA_FILE):
        print(f"  WARNING: {SCHEMA_FILE} not found. Skipping schema creation.")
        print(f"  Make sure tables already exist in the database.")
        return

    with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
        sql = f.read()

    with engine.connect() as conn:
        # Run each SQL statement one by one
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for stmt in statements:
            try:
                conn.execute(sa.text(stmt))
            except Exception:
                pass  # ignore errors like "table already exists"
        conn.commit()
    print("  Schema created successfully.")


def load_table(engine, table_name, csv_path):
    """Load a single CSV file into a database table."""
    df = pd.read_csv(csv_path)

    # Drop the table first with CASCADE
    # CASCADE also removes any foreign keys that depend on this table
    with engine.connect() as conn:
        conn.execute(sa.text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
        conn.commit()

    # Load the data into PostgreSQL
    df.to_sql(
        name      = table_name,
        con       = engine,
        if_exists = "replace",  # replace = drop and recreate
        index     = False,      # don't save row numbers as a column
        chunksize = 10000,      # load 10,000 rows at a time
        method    = "multi",    # faster loading method
    )
    return len(df)


def verify_counts(engine):
    """Check row counts in all loaded tables."""
    print("\n  Row Count Verification")
    print("  " + "-" * 40)

    expected = {
        "customers":      99441,
        "sellers":         3095,
        "products":       32951,
        "orders":         99441,
        "order_items":   112650,
        "order_payments":103875,
        "order_reviews":  98673,
        "geolocation":   738305,
    }

    all_ok = True
    with engine.connect() as conn:
        for table, expected_count in expected.items():
            try:
                result = conn.execute(sa.text(f"SELECT COUNT(*) FROM {table}"))
                actual = result.fetchone()[0]
                status = "OK" if actual == expected_count else "??"
                if actual != expected_count:
                    all_ok = False
                print(f"    {status}  {table:<20} {actual:>10,} rows")
            except Exception as e:
                print(f"    !!  {table:<20} ERROR: {e}")
                all_ok = False

    return all_ok


# ── MAIN LOAD FUNCTION ────────────────────────────────────────
def load():
    """
    Main load function.
    Connects to PostgreSQL and loads all cleaned CSV files.
    """
    print("=" * 55)
    print("  STEP 3 — LOAD")
    print(f"  Loading data into PostgreSQL: {DB_CONFIG['database']}")
    print("=" * 55)

    # Check pass.env has a password
    if not DB_CONFIG["password"]:
        print("\n  ERROR: DB_PASSWORD not found in pass.env file!")
        print("  Make sure pass.env exists with your DB credentials.")
        sys.exit(1)

    # Check cleaned folder exists
    if not os.path.exists(CLEANED_DIR):
        print(f"\n  ERROR: '{CLEANED_DIR}' folder not found!")
        print("  Run transform.py first to generate cleaned files.")
        sys.exit(1)

    # Connect to database
    print("\n  Connecting to database...")
    try:
        engine = get_engine()
        test_connection(engine)
    except Exception as e:
        print(f"\n  ERROR: Could not connect to database!")
        print(f"  {e}")
        sys.exit(1)

    # Create tables from schema.sql
    print("\n  Creating tables from schema.sql...")
    run_schema(engine)

    # Load each table
    print("\n  Loading tables:")
    for table_name in LOAD_ORDER:
        csv_path = os.path.join(CLEANED_DIR, f"{table_name}.csv")

        if not os.path.exists(csv_path):
            print(f"    !!  {table_name:<20} File not found: {csv_path}")
            continue

        print(f"    Loading {table_name}...", end=" ", flush=True)
        rows = load_table(engine, table_name, csv_path)
        print(f"{rows:,} rows loaded")

    # Verify row counts
    all_ok = verify_counts(engine)

    # Final summary
    print(f"\n{'=' * 55}")
    if all_ok:
        print("  Load complete! All tables match expected row counts.")
    else:
        print("  Load done but some counts differ from expected.")
        print("  Check the table counts above.")
    print(f"{'=' * 55}\n")


# ── RUN STANDALONE ────────────────────────────────────────────
if __name__ == "__main__":
    load()