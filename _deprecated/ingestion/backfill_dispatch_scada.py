"""
Backfill AEMO Dispatch SCADA archives for a date range into the local monthly archive layout.

This script is separate from the rolling 5-minute importer. It is intended for one-time or
manual backfills, for example:

    python ingestion/backfill_dispatch_scada.py --start-date 2025-07-01 --end-date 2026-03-17

It downloads archive ZIPs from NEMWEB, appends them into monthly CSV archives, rebuilds the
today snapshot, and can optionally refresh the Bronze Parquet layer afterwards.
"""

from __future__ import annotations

import argparse
import io
import sys
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from importdata import (  # noqa: E402
    BASE_URL,
    DATA_DIR,
    KEEP_COLUMNS,
    REQUEST_TIMEOUT_SECONDS,
    append_to_monthly_archive,
    rebuild_today_snapshot_from_archives,
)
from ingestion.bronze_writer import main as refresh_bronze  # noqa: E402

ARCHIVE_URL = f"{BASE_URL}/Reports/Archive/Dispatch_SCADA/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Dispatch SCADA archive files into local monthly archives.")
    parser.add_argument("--start-date", required=True, help="Inclusive start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=True, help="Inclusive end date in YYYY-MM-DD format")
    parser.add_argument(
        "--refresh-bronze",
        action="store_true",
        help="Also rebuild data/bronze/*.parquet after the CSV archives are updated",
    )
    return parser.parse_args()


def iter_archive_dates(start_date: datetime.date, end_date: datetime.date) -> list[pd.Timestamp]:
    return list(pd.date_range(start=start_date, end=end_date, freq="D"))


def archive_zip_name(day: pd.Timestamp) -> str:
    return f"PUBLIC_DISPATCHSCADA_{day.strftime('%Y%m%d')}.zip"


def download_archive_zip(day: pd.Timestamp) -> bytes:
    zip_name = archive_zip_name(day)
    url = urljoin(ARCHIVE_URL, zip_name)
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.content


def extract_zip_to_frame(payload: bytes) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for csv_name in archive.namelist():
            df = pd.read_csv(archive.open(csv_name), skiprows=1)
            df = df[df.iloc[:, 0] != "C"]
            df = df[KEEP_COLUMNS]
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=KEEP_COLUMNS)

    frame = pd.concat(frames, ignore_index=True)
    frame.drop_duplicates(subset=["SETTLEMENTDATE", "DUID"], inplace=True)
    frame.sort_values(["SETTLEMENTDATE", "DUID"], inplace=True)
    return frame


def main() -> None:
    args = parse_args()
    start_date = pd.Timestamp(args.start_date).date()
    end_date = pd.Timestamp(args.end_date).date()

    if end_date < start_date:
        raise ValueError("end-date must be on or after start-date")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    loaded_days = 0
    missing_days: list[str] = []

    for day in iter_archive_dates(start_date, end_date):
        try:
            payload = download_archive_zip(day)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            print(f"Skipping {day.date()} — archive not available (HTTP {status})")
            missing_days.append(str(day.date()))
            continue

        frame = extract_zip_to_frame(payload)
        if frame.empty:
            print(f"Skipping {day.date()} — archive contained no SCADA rows")
            missing_days.append(str(day.date()))
            continue

        append_to_monthly_archive(frame)
        total_rows += len(frame)
        loaded_days += 1
        print(f"Loaded {day.date()}: {len(frame):,} rows")

    rebuild_today_snapshot_from_archives()

    if args.refresh_bronze:
        refresh_bronze()

    print("=" * 60)
    print("Dispatch SCADA backfill complete")
    print(f"Date range      : {start_date} to {end_date}")
    print(f"Days loaded     : {loaded_days}")
    print(f"Rows ingested   : {total_rows:,}")
    print(f"Days unavailable: {len(missing_days)}")
    if missing_days:
        print("Missing dates   : " + ", ".join(missing_days[:15]) + (" ..." if len(missing_days) > 15 else ""))
    print("=" * 60)


if __name__ == "__main__":
    import datetime

    main()
