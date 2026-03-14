"""
=============================================================
  BRAZIL E-COMMERCE (OLIST) -- PIPELINE RUNNER
  Data Engineering Layer
=============================================================
Runs the full ETL pipeline in one command:
  
  python pipeline.py

Steps:
  1. EXTRACT  — validate raw CSV files
  2. TRANSFORM — clean and fix data
  3. LOAD      — insert into PostgreSQL

Usage:
  python pipeline.py            # run all 3 steps
  python pipeline.py --extract  # run extract only
  python pipeline.py --transform # run transform only
  python pipeline.py --load     # run load only
=============================================================
"""

import sys
import time

from extract   import extract
from transform import transform
from load      import load


def run_step(name, fn):
    """Run a single pipeline step and track time."""
    print(f"\n{'#' * 55}")
    print(f"#  RUNNING: {name}")
    print(f"{'#' * 55}")
    start = time.time()
    fn()
    elapsed = time.time() - start
    print(f"  Completed in {elapsed:.1f} seconds")


def run_pipeline():
    """Run the full ETL pipeline."""
    print("\n")
    print("=" * 55)
    print("  BRAZIL E-COMMERCE — ETL PIPELINE")
    print("  Extract → Transform → Load")
    print("=" * 55)

    total_start = time.time()

    run_step("EXTRACT",   extract)
    run_step("TRANSFORM", transform)
    run_step("LOAD",      load)

    total_elapsed = time.time() - total_start

    print("\n")
    print("=" * 55)
    print("  PIPELINE COMPLETE!")
    print(f"  Total time: {total_elapsed:.1f} seconds")
    print("=" * 55)
    print("\n  What was done:")
    print("    1. Extracted  : 9 raw CSV files validated")
    print("    2. Transformed: 8 tables cleaned and saved")
    print("    3. Loaded     : 8 tables inserted into PostgreSQL")
    print("\n  Next step: run the EDA queries in 2_analysis/")
    print("=" * 55)


# ── ENTRY POINT ───────────────────────────────────────────────
if __name__ == "__main__":

    # Check for optional arguments to run individual steps
    args = sys.argv[1:]

    if "--extract" in args:
        run_step("EXTRACT", extract)

    elif "--transform" in args:
        run_step("TRANSFORM", transform)

    elif "--load" in args:
        run_step("LOAD", load)

    else:
        # Default: run the full pipeline
        run_pipeline()
