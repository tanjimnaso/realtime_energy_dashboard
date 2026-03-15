"""
migrate_to_monthly.py — One-time migration script.

Splits the existing monolithic dispatch_scada.csv into:
  - data/dispatch_scada_YYYY-MM.csv  (one file per calendar month)
  - data/dispatch_scada_today.csv    (current day only)

Run once locally before deploying the new importdata.py, then delete this file.
The original dispatch_scada.csv is NOT deleted — remove it manually after verifying.
"""

import glob
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data"
SOURCE_CSV = DATA_DIR / "dispatch_scada.csv"
TODAY_CSV = DATA_DIR / "dispatch_scada_today.csv"


def main():
    if not SOURCE_CSV.exists():
        print(f"Source file not found: {SOURCE_CSV}")
        return

    print(f"Reading {SOURCE_CSV} ...")
    df = pd.read_csv(SOURCE_CSV, parse_dates=["SETTLEMENTDATE"])
    print(f"  {len(df):,} rows, date range: {df['SETTLEMENTDATE'].min()} to {df['SETTLEMENTDATE'].max()}")

    df.sort_values(["SETTLEMENTDATE", "DUID"], inplace=True)

    # Write monthly archives
    months_written = 0
    for period, month_df in df.groupby(df["SETTLEMENTDATE"].dt.to_period("M")):
        archive_path = DATA_DIR / f"dispatch_scada_{period}.csv"
        month_df.to_csv(archive_path, index=False)
        size_mb = archive_path.stat().st_size / 1_048_576
        print(f"  Wrote {archive_path.name}: {len(month_df):,} rows, {size_mb:.1f} MB")
        months_written += 1

    # Write today's file (latest calendar date in the data)
    today_date = df["SETTLEMENTDATE"].max().normalize()
    today_df = df[df["SETTLEMENTDATE"].dt.normalize() == today_date]
    today_df.to_csv(TODAY_CSV, index=False)
    size_kb = TODAY_CSV.stat().st_size / 1024
    print(f"  Wrote dispatch_scada_today.csv: {len(today_df):,} rows, {size_kb:.1f} KB")

    print()
    print(f"Done. {months_written} monthly archive(s) created.")
    print()

    # Verification summary
    print("Verification:")
    total_archive_rows = 0
    for f in sorted(glob.glob(str(DATA_DIR / "dispatch_scada_????-??.csv"))):
        check = pd.read_csv(f)
        dups = check.duplicated(subset=["SETTLEMENTDATE", "DUID"]).sum()
        print(f"  {Path(f).name}: {len(check):,} rows, {dups} duplicates")
        total_archive_rows += len(check)

    print(f"  Total archive rows: {total_archive_rows:,}  (original: {len(df):,})")
    if total_archive_rows != len(df):
        print("  WARNING: Row count mismatch — check for gaps or duplicates in the source CSV.")
    else:
        print("  Row counts match. Migration successful.")


if __name__ == "__main__":
    main()
