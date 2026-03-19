"""
AEMO Aggregated Price & Demand — Historic Batch Downloader
==========================================================
Iterates all NEM states × calendar months from START_YYYYMM to END_YYYYMM,
downloads each CSV from AEMO, and streams it into a GCS bucket.

Designed to run as a Cloud Run Job (no HTTP server needed).

Environment variables
---------------------
GCS_BUCKET      : GCS bucket name (required)
GCS_PREFIX      : Object key prefix, e.g. "aemo/price_demand/"  (default: "aemo/price_demand/")
START_YYYYMM    : First month to fetch, e.g. "199901"           (default: "199901")
END_YYYYMM      : Last month to fetch, inclusive, e.g. "202502" (default: current month)
STATES          : Comma-separated NEM regions                   (default: all five)
SKIP_EXISTING   : "true" to skip blobs already in GCS           (default: "true")
MAX_WORKERS     : Download concurrency                          (default: 5)
REQUEST_TIMEOUT : Per-request timeout in seconds                (default: 60)

Usage (local)
-------------
    GCS_BUCKET=my-bucket python aemo_historic_downloader.py

Usage (Cloud Run Job)
---------------------
    gcloud run jobs create aemo-historic \
        --image gcr.io/PROJECT/aemo-downloader \
        --set-env-vars GCS_BUCKET=my-bucket,START_YYYYMM=200301 \
        --execute-now
"""

import io
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from itertools import product

import requests
from google.cloud import storage

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

AEMO_BASE_URL = "https://aemo.com.au/aemo/data/nem/priceanddemand"
ALL_STATES = ["NSW1", "VIC1", "QLD1", "SA1", "TAS1"]


def _env(key: str, default: str | None = None) -> str:
    val = os.environ.get(key, default)
    if val is None:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return val


def _parse_yyyymm(s: str) -> date:
    return datetime.strptime(s, "%Y%m").date().replace(day=1)


def _current_yyyymm() -> str:
    today = date.today()
    return today.strftime("%Y%m")


def _month_range(start: date, end: date):
    """Yield (year, month) tuples from start to end inclusive."""
    current = start
    while current <= end:
        yield current.year, current.month
        # advance one month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


# ---------------------------------------------------------------------------
# Download + upload
# ---------------------------------------------------------------------------

def build_url(year: int, month: int, state: str) -> str:
    yyyymm = f"{year:04d}{month:02d}"
    filename = f"PRICE_AND_DEMAND_{yyyymm}_{state}.csv"
    return f"{AEMO_BASE_URL}/{filename}", filename


def gcs_object_name(prefix: str, filename: str) -> str:
    return f"{prefix.rstrip('/')}/{filename}"


def blob_exists(bucket: storage.Bucket, name: str) -> bool:
    return bucket.blob(name).exists()


def download_and_upload(
    bucket: storage.Bucket,
    year: int,
    month: int,
    state: str,
    prefix: str,
    skip_existing: bool,
    timeout: int,
) -> tuple[str, str]:
    """
    Download one AEMO CSV and stream it to GCS.
    Returns (filename, status) where status is one of:
        'uploaded', 'skipped', '404', 'error:<msg>'
    """
    url, filename = build_url(year, month, state)
    obj_name = gcs_object_name(prefix, filename)

    if skip_existing and blob_exists(bucket, obj_name):
        return filename, "skipped"

    try:
        resp = requests.get(url, timeout=timeout, stream=True)
    except requests.RequestException as exc:
        return filename, f"error:{exc}"

    if resp.status_code == 404:
        return filename, "404"

    if resp.status_code != 200:
        return filename, f"error:HTTP {resp.status_code}"

    # Stream response content into GCS without touching disk
    blob = bucket.blob(obj_name)
    buf = io.BytesIO(resp.content)
    blob.upload_from_file(buf, content_type="text/csv", rewind=True)

    return filename, "uploaded"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    bucket_name = _env("GCS_BUCKET")
    prefix      = os.environ.get("GCS_PREFIX", "aemo/price_demand/")
    start_ym    = os.environ.get("START_YYYYMM", "199901")
    end_ym      = os.environ.get("END_YYYYMM", _current_yyyymm())
    states_raw  = os.environ.get("STATES", ",".join(ALL_STATES))
    skip_str    = os.environ.get("SKIP_EXISTING", "true").lower()
    max_workers = int(os.environ.get("MAX_WORKERS", "5"))
    timeout     = int(os.environ.get("REQUEST_TIMEOUT", "60"))

    states       = [s.strip().upper() for s in states_raw.split(",")]
    skip_existing = skip_str == "true"
    start_date   = _parse_yyyymm(start_ym)
    end_date     = _parse_yyyymm(end_ym)

    log.info(
        "Config: bucket=%s prefix=%s range=%s→%s states=%s "
        "skip_existing=%s workers=%d timeout=%ds",
        bucket_name, prefix, start_ym, end_ym,
        states, skip_existing, max_workers, timeout,
    )

    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)

    # Build the full work list
    tasks = list(product(_month_range(start_date, end_date), states))
    total = len(tasks)
    log.info("Total files to process: %d", total)

    counts = {"uploaded": 0, "skipped": 0, "404": 0, "error": 0}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                download_and_upload,
                bucket, year, month, state,
                prefix, skip_existing, timeout,
            ): (year, month, state)
            for (year, month), state in tasks
        }

        for i, future in enumerate(as_completed(futures), 1):
            year, month, state = futures[future]
            try:
                filename, status = future.result()
            except Exception as exc:
                filename = f"PRICE_AND_DEMAND_{year:04d}{month:02d}_{state}.csv"
                status = f"error:{exc}"

            category = status if status in counts else "error"
            counts[category] += 1

            if status == "uploaded":
                log.info("[%d/%d] ✓ %s", i, total, filename)
            elif status == "skipped":
                log.debug("[%d/%d] — skipped %s (already in GCS)", i, total, filename)
            elif status == "404":
                log.debug("[%d/%d] ✗ %s (404 — file not published by AEMO)", i, total, filename)
            else:
                log.warning("[%d/%d] ✗ %s → %s", i, total, filename, status)

    log.info(
        "Done. uploaded=%d skipped=%d not_found=%d errors=%d",
        counts["uploaded"], counts["skipped"], counts["404"], counts["error"],
    )

    if counts["error"] > 0:
        sys.exit(1)  # non-zero exit signals Cloud Run Job failure


if __name__ == "__main__":
    main()
