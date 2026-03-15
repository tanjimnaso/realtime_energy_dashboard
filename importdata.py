"""
AEMO Dispatch SCADA Data Importer
Scrapes NEMWEB for 5-minute dispatch SCADA ZIP files,
extracts CSVs, and appends new data to a local CSV.
"""

import io
import os
import re
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────
BASE_URL = "https://nemweb.com.au"
SCADA_URL = f"{BASE_URL}/Reports/Current/Dispatch_SCADA/"
OUTPUT_CSV = Path(__file__).resolve().parent / "data" / "dispatch_scada.csv"
KEEP_COLUMNS = ["SETTLEMENTDATE", "DUID", "SCADAVALUE"]
REQUEST_TIMEOUT_SECONDS = int(os.getenv("AEMO_REQUEST_TIMEOUT_SECONDS", "30"))
LOOKBACK_ARCHIVES = int(os.getenv("AEMO_ZIP_LOOKBACK", "288"))
OVERLAP_MINUTES = int(os.getenv("AEMO_OVERLAP_MINUTES", "15"))
ZIP_TIMESTAMP_RE = re.compile(r"(\d{12}|\d{14})")


def parse_zip_timestamp(link):
    """Extract an AEMO timestamp from a zip filename when present."""
    match = ZIP_TIMESTAMP_RE.search(link)
    if not match:
        return None

    stamp = match.group(1)
    fmt = "%Y%m%d%H%M%S" if len(stamp) == 14 else "%Y%m%d%H%M"
    return pd.to_datetime(stamp, format=fmt, utc=False, errors="coerce")


def get_zip_links(latest_settlement=None):
    """Scrape the NEMWEB directory and keep only a recent overlap window."""
    response = requests.get(SCADA_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    links = sorted(
        {
            a["href"]
            for a in soup.find_all("a", href=True)
            if a["href"].lower().endswith(".zip")
        }
    )
    print(f"Found {len(links)} ZIP files on NEMWEB")

    if not links:
        return []

    if latest_settlement is None:
        selected = links[-LOOKBACK_ARCHIVES:]
        print(f"No existing CSV found. Using latest {len(selected)} archives.")
        return selected

    cutoff = latest_settlement - pd.Timedelta(minutes=OVERLAP_MINUTES)
    selected = [
        link for link in links
        if (link_ts := parse_zip_timestamp(link)) is not None and link_ts >= cutoff
    ]
    if not selected:
        selected = links[-min(LOOKBACK_ARCHIVES, len(links)):]

    print(
        "Selected "
        f"{len(selected)} archive(s) newer than {cutoff.strftime('%Y-%m-%d %H:%M:%S')} "
        f"with a {OVERLAP_MINUTES}-minute overlap."
    )
    return selected


def download_and_extract(links):
    """Download each ZIP, extract CSV, return combined DataFrame."""
    all_frames = []

    for i, link in enumerate(links):
        url = urljoin(BASE_URL, link)
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
            r.raise_for_status()
            z = zipfile.ZipFile(io.BytesIO(r.content))
            for csv_name in z.namelist():
                df = pd.read_csv(z.open(csv_name), skiprows=1)
                df = df[df.iloc[:, 0] != "C"]
                df = df[KEEP_COLUMNS]
                all_frames.append(df)
        except Exception as e:
            print(f"  Error processing {link}: {e}")
            continue

        if (i + 1) % 50 == 0:
            print(f"  Downloaded {i + 1}/{len(links)}")

    if not all_frames:
        print("No new data extracted.")
        return pd.DataFrame(columns=KEEP_COLUMNS)

    new_data = pd.concat(all_frames, ignore_index=True)
    print(f"Extracted {len(new_data)} rows from {len(all_frames)} files")
    return new_data


def load_existing():
    """Load the existing CSV if it exists, otherwise return empty DataFrame."""
    if OUTPUT_CSV.exists():
        existing = pd.read_csv(OUTPUT_CSV)
        print(f"Loaded {len(existing)} existing rows")
        return existing
    print("No existing CSV found — starting fresh")
    return pd.DataFrame(columns=KEEP_COLUMNS)


def save(df):
    """Write DataFrame to CSV, creating the data/ directory if needed."""
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(df)} rows to {OUTPUT_CSV}")


def main():
    print("=" * 60)
    print("AEMO Dispatch SCADA Importer")
    print("=" * 60)

    existing = load_existing()
    latest_settlement = None
    if not existing.empty:
        latest_settlement = pd.to_datetime(existing["SETTLEMENTDATE"]).max()
        print(f"Latest saved interval: {latest_settlement}")

    links = get_zip_links(latest_settlement)
    if not links:
        print("No ZIP links were selected.")
        return

    new_data = download_and_extract(links)

    if new_data.empty:
        print("Nothing new to save.")
        return

    combined = pd.concat([existing, new_data], ignore_index=True)

    before = len(combined)
    combined.drop_duplicates(subset=["SETTLEMENTDATE", "DUID"], inplace=True)
    print(f"Dropped {before - len(combined)} duplicate rows")

    combined.sort_values(["SETTLEMENTDATE", "DUID"], inplace=True)
    save(combined)

    print("=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
