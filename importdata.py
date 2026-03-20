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

try:
    from google.cloud import storage
except ImportError:  # pragma: no cover - optional outside cloud/runtime envs
    storage = None

# ── Config ────────────────────────────────────────────────────────
BASE_URL = "https://nemweb.com.au"
SCADA_URL = f"{BASE_URL}/Reports/Current/Dispatch_SCADA/"
DATA_DIR = Path(__file__).resolve().parent / "data"
TODAY_CSV = DATA_DIR / "dispatch_scada_today.csv"
KEEP_COLUMNS = ["SETTLEMENTDATE", "DUID", "SCADAVALUE"]
REQUEST_TIMEOUT_SECONDS = int(os.getenv("AEMO_REQUEST_TIMEOUT_SECONDS", "30"))
LOOKBACK_ARCHIVES = int(os.getenv("AEMO_ZIP_LOOKBACK", "288"))
OVERLAP_MINUTES = int(os.getenv("AEMO_OVERLAP_MINUTES", "15"))
ZIP_TIMESTAMP_RE = re.compile(r"(\d{12}|\d{14})")
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()
_storage_client = None


def gcs_enabled() -> bool:
    return bool(GCS_BUCKET)


def get_storage_client():
    global _storage_client
    if _storage_client is None:
        if storage is None:
            raise RuntimeError("google-cloud-storage is required when GCS_BUCKET is set.")
        _storage_client = storage.Client()
    return _storage_client


def get_bucket():
    return get_storage_client().bucket(GCS_BUCKET)


def list_monthly_archive_names() -> list[str]:
    if gcs_enabled():
        blobs = get_storage_client().list_blobs(GCS_BUCKET, prefix="dispatch_scada_")
        return sorted(
            blob.name for blob in blobs
            if re.fullmatch(r"dispatch_scada_\d{4}-\d{2}\.csv", blob.name)
        )
    return sorted(path.name for path in DATA_DIR.glob("dispatch_scada_????-??.csv"))


def read_csv_from_storage(filename: str, **kwargs) -> pd.DataFrame:
    if gcs_enabled():
        blob = get_bucket().blob(filename)
        if not blob.exists():
            raise FileNotFoundError(f"{filename} not found in gs://{GCS_BUCKET}")
        text = blob.download_as_text()
        return pd.read_csv(io.StringIO(text), **kwargs)
    return pd.read_csv(DATA_DIR / filename, **kwargs)


def write_df_to_storage(df: pd.DataFrame, filename: str) -> None:
    csv_text = df.to_csv(index=False)
    if gcs_enabled():
        blob = get_bucket().blob(filename)
        blob.upload_from_string(csv_text, content_type="text/csv")
        print(f"Written CSV {filename} to gs://{GCS_BUCKET}/{filename}")
    else:
        full_path = DATA_DIR / filename
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(csv_text)
        print(f"Written CSV {filename} to {full_path}")


def write_parquet_to_storage(df: pd.DataFrame, filename: str) -> None:
    """Write DataFrame as Parquet to storage (GCS or Local)."""
    if gcs_enabled():
        # Using a temporary buffer for Parquet conversion
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        blob = get_bucket().blob(filename)
        blob.upload_from_string(buffer.getvalue(), content_type="application/octet-stream")
        print(f"Written Parquet {filename} to gs://{GCS_BUCKET}/{filename}")
    else:
        full_path = DATA_DIR / filename
        full_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(full_path, index=False, engine="pyarrow")
        print(f"Written Parquet {filename} to {full_path}")


def parse_settlement_series(series: pd.Series) -> pd.Series:
    """Parse mixed SETTLEMENTDATE formats safely.

    Some archive files contain timestamps with `-` separators and others with `/`.
    Pandas can choke if it infers a single strict format from the first values, so
    force mixed parsing when supported and fall back to generic coercion otherwise.
    """
    try:
        return pd.to_datetime(series, errors="coerce", format="mixed")
    except TypeError:
        return pd.to_datetime(series, errors="coerce")


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


def get_archive_path(period: pd.Period) -> Path:
    """Return the monthly archive path for a given period (e.g. 2026-03)."""
    return DATA_DIR / f"dispatch_scada_{period}.csv"


def rebuild_today_snapshot_from_archives() -> None:
    """Rebuild dispatch_scada_today.csv from the latest calendar day in the current month archive.

    This avoids truncating the today snapshot to only the most recently fetched ZIP window.
    At midnight, the latest calendar day automatically rolls forward and the file resets.
    """
    archive_files = list_monthly_archive_names()
    if not archive_files:
        print("No monthly archives available yet; skipping today snapshot rebuild.")
        return

    latest_file = archive_files[-1]
    latest_df = read_csv_from_storage(latest_file)
    latest_df["SETTLEMENTDATE"] = parse_settlement_series(latest_df["SETTLEMENTDATE"])
    latest_df = latest_df.dropna(subset=["SETTLEMENTDATE"])
    if latest_df.empty:
        print(f"{latest_file} contains no valid settlement timestamps; skipping today snapshot rebuild.")
        return

    latest_date = latest_df["SETTLEMENTDATE"].dt.normalize().max()
    today_df = latest_df[latest_df["SETTLEMENTDATE"].dt.normalize() == latest_date].copy()
    today_df.drop_duplicates(subset=["SETTLEMENTDATE", "DUID"], inplace=True)
    today_df.sort_values(["SETTLEMENTDATE", "DUID"], inplace=True)

    write_df_to_storage(today_df, TODAY_CSV.name)
    print(f"Rebuilt {TODAY_CSV.name} with {len(today_df)} rows for {latest_date.date()} from {latest_file}")


def append_to_monthly_archive(df: pd.DataFrame) -> None:
    """Upsert df rows into per-month archive files, deduplicating on SETTLEMENTDATE+DUID."""
    dates = pd.to_datetime(df["SETTLEMENTDATE"])
    for period, month_df in df.groupby(dates.dt.to_period("M")):
        archive_path = get_archive_path(period)
        archive_name = archive_path.name
        archive_exists = get_bucket().blob(archive_name).exists() if gcs_enabled() else archive_path.exists()
        if archive_exists:
            existing = read_csv_from_storage(archive_name)
            existing["SETTLEMENTDATE"] = parse_settlement_series(existing["SETTLEMENTDATE"])
            existing = existing.dropna(subset=["SETTLEMENTDATE"])
            month_df = pd.concat([existing, month_df], ignore_index=True)
            month_df.drop_duplicates(subset=["SETTLEMENTDATE", "DUID"], inplace=True)
        month_df.sort_values(["SETTLEMENTDATE", "DUID"], inplace=True)
        write_df_to_storage(month_df, archive_name)
        size_mb = len(month_df.to_csv(index=False).encode("utf-8")) / 1_048_576
        print(f"  Archive {archive_name}: {len(month_df)} rows ({size_mb:.1f} MB)")


def latest_settlement_from_archives() -> pd.Timestamp | None:
    """Derive the most recent SETTLEMENTDATE from existing monthly archive files."""
    archive_files = list_monthly_archive_names()
    if not archive_files:
        return None
    latest_file = archive_files[-1]
    df = read_csv_from_storage(latest_file, usecols=["SETTLEMENTDATE"])
    ts = parse_settlement_series(df["SETTLEMENTDATE"]).max()
    print(f"Latest saved interval (from {latest_file}): {ts}")
    return ts


def main():
    print("=" * 60)
    print("AEMO Dispatch SCADA Importer")
    print("=" * 60)

    latest_settlement = latest_settlement_from_archives()

    links = get_zip_links(latest_settlement)
    if not links:
        print("No ZIP links were selected.")
        return

    new_data = download_and_extract(links)

    if new_data.empty:
        print("Nothing new to save.")
        return

    # Ensure consistent date parsing for all new data
    new_data["SETTLEMENTDATE"] = parse_settlement_series(new_data["SETTLEMENTDATE"])
    new_data = new_data.dropna(subset=["SETTLEMENTDATE"])

    before = len(new_data)
    new_data.drop_duplicates(subset=["SETTLEMENTDATE", "DUID"], inplace=True)
    print(f"Dropped {before - len(new_data)} duplicate rows")

    new_data.sort_values(["SETTLEMENTDATE", "DUID"], inplace=True)
    append_to_monthly_archive(new_data)
    rebuild_today_snapshot_from_archives()

    # --- Medallion Sync ---
    # Refresh the Bronze Parquet layer used by dbt
    print("Refreshing Bronze Parquet layer...")
    archive_names = list_monthly_archive_names()
    if archive_names:
        # For simplicity in this job, we'll just write the latest month + today to bronze.
        # If full history is needed in bronze, we'd concat all archives.
        latest_archive = read_csv_from_storage(archive_names[-1])
        latest_archive["SETTLEMENTDATE"] = parse_settlement_series(latest_archive["SETTLEMENTDATE"])
        
        # We also need generator metadata and emissions factors in bronze for dbt
        # But for now let's just ensure the main SCADA table is updated.
        write_parquet_to_storage(latest_archive, "bronze/dispatch_scada.parquet")
    # ----------------------

    print("=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
