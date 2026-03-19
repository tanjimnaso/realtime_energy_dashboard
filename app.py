"""
app.py — Real-Time Energy Dashboard
by Tanjim Islam
======================================================
Primary app contract:
  Bronze Parquet  → dbt Silver/Gold in nem.duckdb → Streamlit views
Fallback path:
  Monthly CSV archives + reference CSVs when DuckDB Gold is unavailable
"""

import datetime
import glob
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import duckdb

try:
    from data_bootstrap import ensure_required_data
except ImportError:
    ensure_required_data = None
import os
import io
import datetime

def get_aemo_date() -> datetime.date:
    return pd.Timestamp.utcnow().tz_convert("Australia/Brisbane").date()

def get_aemo_now() -> pd.Timestamp:
    return pd.Timestamp.utcnow().tz_convert("Australia/Brisbane")

try:
    from google.cloud import storage
except ImportError:
    storage = None

# GCS Config
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()
_gcs_client = None
ZERO_EMISSION = {"Wind", "Solar PV", "Hydro", "Battery Storage"}

def get_gcs_client():
    global _gcs_client
    if _gcs_client is None:
        if storage is None:
            raise RuntimeError("google-cloud-storage not installed but GCS_BUCKET set.")
        _gcs_client = storage.Client()
    return _gcs_client

def get_bucket():
    return get_gcs_client().bucket(GCS_BUCKET)

def read_csv_from_any(path_or_name: str | Path, **kwargs) -> pd.DataFrame:
    if GCS_BUCKET:
        filename = Path(path_or_name).name
        # Do NOT catch exceptions here — if GCS is configured as the data source
        # we must never silently fall back to the local (stale) baked-in file.
        blob = get_bucket().blob(filename)
        blob.reload()  # Force fresh metadata; avoid stale client-side cache
        text = blob.download_as_text()
        return pd.read_csv(io.StringIO(text), **kwargs)
    return pd.read_csv(path_or_name, **kwargs)


def get_gcs_file_hash(filename: str) -> str:
    """Return the CRC32C hash of a GCS file to use as a cache key part.
    
    Calls reload() to force a fresh metadata fetch from GCS, bypassing any
    client-side caching of blob attributes (which would return a stale CRC
    and prevent Streamlit from detecting that the file has been updated).
    """
    if not GCS_BUCKET:
        return "local"
    try:
        blob = get_bucket().blob(filename)
        blob.reload()  # Force fresh metadata from GCS — do NOT use get_blob() here
        return blob.crc32c if blob.exists() else "missing"
    except Exception:
        return "error"


def get_latest_archive_cache_key(data_dir: Path) -> str:
    """Return a freshness token for the newest monthly SCADA archive."""
    monthly_archives = list_monthly_archives_from_any(data_dir)
    if not monthly_archives:
        return "missing"

    latest_archive = monthly_archives[-1]
    if GCS_BUCKET:
        blob = get_bucket().blob(latest_archive)
        try:
            blob.reload()
            crc = blob.crc32c or "nocrc"
            generation = getattr(blob, "generation", "nogeneration")
            updated = getattr(blob, "updated", None)
            updated_token = updated.isoformat() if updated else "noupdated"
            return f"{latest_archive}:{crc}:{generation}:{updated_token}"
        except Exception:
            return f"{latest_archive}:error"

    latest_path = Path(latest_archive)
    if not latest_path.exists():
        latest_path = data_dir / Path(latest_archive).name
    if not latest_path.exists():
        return f"{latest_archive}:missing"
    return f"{latest_path.name}:{latest_path.stat().st_mtime_ns}"


def get_duckdb_cache_key(db_path: str = "nem.duckdb") -> str:
    """Return a freshness token for the DuckDB file."""
    path = Path(db_path)
    if not path.exists():
        return "missing"
    stat = path.stat()
    return f"{stat.st_mtime_ns}:{stat.st_size}"

def list_monthly_archives_from_any(data_dir: Path) -> list[str]:
    if GCS_BUCKET:
        blobs = get_gcs_client().list_blobs(GCS_BUCKET, prefix="dispatch_scada_")
        import re
        return sorted(
            blob.name for blob in blobs
            if re.fullmatch(r"dispatch_scada_\d{4}-\d{2}\.csv", blob.name)
        )
    return sorted([str(p) for p in data_dir.glob("dispatch_scada_????-??.csv")])

# ─────────────────────────────────────────────────────────────
# DuckDB Connection
# ─────────────────────────────────────────────────────────────
def get_db_connection():
    """Connect to nem.duckdb and return connection object."""
    return duckdb.connect("nem.duckdb", read_only=True)

# ─────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Real-Time Energy Dashboard, NEM Emissions",
    layout="wide",
)

# ─────────────────────────────────────────────────────────────
# Background Polling for Live Data
# ─────────────────────────────────────────────────────────────
import time

# Initialize polling state
if "last_refresh_time" not in st.session_state:
    st.session_state.last_refresh_time = time.time()

# Background polling: rerun every 300 seconds (5 minutes, matches data update cadence)
current_time = time.time()
if current_time - st.session_state.last_refresh_time > 300:
    st.session_state.last_refresh_time = current_time
    st.rerun()

# Timestamp display function
def format_refresh_time():
    """Return formatted last refresh timestamp for display."""
    from datetime import datetime
    return datetime.fromtimestamp(st.session_state.last_refresh_time).strftime("%H:%M:%S")

# ─────────────────────────────────────────────────────────────
# CSS Design System
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&family=Source+Serif+4:opsz,wght@8..60,400;8..60,600;8..60,700&display=swap');

  :root {
    /* Typography */
    --font-display: 'Source Serif 4', Georgia, serif;
    --font-body: 'IBM Plex Sans', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
    --font-mono: 'IBM Plex Mono', 'SFMono-Regular', Menlo, monospace;
    --text-base: 0.9rem;
    --text-lg: 1.04rem;
    --text-xl: 1.4rem;
    --text-2xl: 2.35rem;

    /* Colors — base */
    --background: #f4f1ea;
    --foreground: #223a42;
    --card: #ffffff;
    --muted: #e8e1d4;
    --muted-foreground: #64777b;
    --accent: #0b7f94;
    --accent-light: #edf6f7;
    --accent-warm: #d97b2d;
    --border: rgba(34, 58, 66, 0.16);
    --radius: 0;

    /* Semantic — timing */
    --bg-green: #eef5e6;
    --bg-green-border: #9db36a;
    --bg-red: #f7e7e0;
    --bg-red-border: #d69086;
    --bg-neutral: #ffffff;

    /* Warm bands */
    --header-bg: #0b7f94;
    --header-border: rgba(11, 127, 148, 0.18);
    --footer-bg: #e6ece9;
    --footer-border: rgba(34, 58, 66, 0.12);
  }

  /* ── Base ── */
  html { font-size: 16px; }
  .stApp { background-color: var(--background) !important; }
  html, body, [class*="css"] {
    font-family: var(--font-body);
    background-color: var(--background);
    color: var(--foreground);
    font-size: var(--text-base);
    font-weight: 400;
    line-height: 1.5;
  }
  h1 {
    font-family: var(--font-display) !important;
    color: var(--foreground) !important;
    font-size: var(--text-2xl) !important;
    font-weight: 700 !important;
    letter-spacing: -0.02em !important;
    line-height: 1.2 !important;
  }
  h2 {
    font-family: var(--font-display) !important;
    color: var(--foreground) !important;
    font-size: var(--text-xl) !important;
    font-weight: 600 !important;
    letter-spacing: -0.01em !important;
    line-height: 1.3 !important;
  }
  h3, h4, h5, h6 {
    font-family: var(--font-display) !important;
    color: var(--foreground) !important;
    font-size: var(--text-lg) !important;
    font-weight: 600 !important;
    line-height: 1.4 !important;
  }

  /* ── Container ── */
  .block-container {
    max-width: 1080px !important;
    padding-top: 2rem !important;
    padding-left: 2.5rem !important;
    padding-right: 2.5rem !important;
  }

  /* Hide sidebar */
  section[data-testid="stSidebar"] { display: none !important; }
  div[data-testid="collapsedControl"] { display: none !important; }

  /* ── Header band ── */
  .header-band {
    background: var(--header-bg);
    border-top: 1px solid var(--header-border);
    border-bottom: 0;
    padding: 1rem 0 0.9rem 0;
    margin: 0 0 2rem 0;
    width: 100vw;
    margin-left: calc(-50vw + 50%);
    margin-right: calc(-50vw + 50%);
    box-shadow: inset 0 -4px 0 0 rgba(217, 123, 45, 0.95);
  }
  .header-band .page-header {
    font-family: var(--font-display);
    font-size: var(--text-2xl);
    font-weight: 500;
    color: #ffffff;
    margin: 0;
    text-align: left;
    letter-spacing: 0.01em;
    max-width: 1080px;
    margin-left: auto;
    margin-right: auto;
    padding: 0 2.5rem;
  }
  .header-band .page-deck {
    text-align: left;
    margin: 0.15rem auto 0 auto;
    padding: 0 2.5rem;
    max-width: 1080px;
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: rgba(255, 255, 255, 0.88);
    line-height: 1.45;
  }
  .meta-line {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-bottom: 0.8rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  /* ── Hero cards ── */
  .hero-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
    margin-bottom: 1.5rem;
  }
  .hero-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    border-top: 4px solid var(--accent);
    padding: 1rem 1rem 0.9rem 1rem;
    text-align: left;
    box-shadow: 0 0 0 1px rgba(255,255,255,0.6) inset;
  }
  .hero-card--accent {
    background: #f7fbfb;
    border-color: rgba(11, 127, 148, 0.24);
    color: var(--accent);
  }
  .hero-card--accent .hero-label { color: rgba(11, 127, 148, 0.75); }
  .hero-card--accent .hero-value { color: var(--accent); }
  .hero-card--accent .hero-sub { color: rgba(11, 127, 148, 0.75); }
  .hero-label {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-bottom: 0.15rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }
  .hero-value {
    font-family: var(--font-body);
    font-size: calc(var(--text-2xl) * 0.88);
    font-weight: 600;
    color: var(--foreground);
    letter-spacing: -0.02em;
    line-height: 0.95;
    font-variant-numeric: tabular-nums lining-nums;
  }
  .hero-sub {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-top: 0.35rem;
  }

  /* ── Insight callout ── */
  .insight-callout {
    background: #ffffff;
    border-top: 0;
    border-right: 0;
    border-bottom: 2px solid var(--accent-warm);
    border-left: 0;
    border-radius: var(--radius);
    padding: 0.85rem 0;
    margin-bottom: 1.35rem;
    font-family: var(--font-display);
    font-size: var(--text-lg);
    font-weight: 500;
    color: var(--accent);
    text-align: left;
    line-height: 1.5;
  }

  /* ── Section headings ── */
  .section-heading {
    font-family: var(--font-display);
    font-size: calc(var(--text-2xl) * 0.7);
    color: var(--foreground);
    font-weight: 600;
    line-height: 1.15;
    margin: 0 0 0.55rem 0;
    letter-spacing: -0.01em;
  }
  .section-sub {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin: 0 0 1rem 0;
    max-width: 54rem;
  }
  .eyebrow {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin: 0 0 0.5rem 0;
  }

  /* ── Timing cards ── */
  .timing-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 0.5rem;
    margin-bottom: 1rem;
  }
  .timing-card {
    border-radius: var(--radius);
    padding: 0.9rem 1rem;
    text-align: left;
    display: flex;
    flex-direction: column;
    justify-content: center;
  }
  .timing-card--green {
    background: #e7f1d5;
    border: 1px solid var(--bg-green-border);
    color: #274c12;
  }
  .timing-card--red {
    background: #f3dfdd;
    border: 1px solid var(--bg-red-border);
    color: #7b2424;
  }
  .timing-card--neutral {
    background: #ffffff;
    border: 1px solid var(--border);
  }
  .timing-label {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-bottom: 0.2rem;
    font-weight: 600;
  }
  .timing-value {
    font-family: var(--font-display);
    font-size: var(--text-lg);
    font-weight: 700;
    color: var(--foreground);
    line-height: 1.1;
  }
  .timing-hours {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-top: 0.25rem;
  }
  .timing-intensity {
    font-family: var(--font-body);
    font-size: var(--text-base);
    font-weight: 600;
    color: var(--foreground);
    margin-top: 0.35rem;
  }
  .timing-card--green .timing-label,
  .timing-card--green .timing-hours,
  .timing-card--green .timing-intensity,
  .timing-card--green .timing-value {
    color: #2e5e11;
  }
  .timing-card--red .timing-label,
  .timing-card--red .timing-hours,
  .timing-card--red .timing-intensity,
  .timing-card--red .timing-value {
    color: #8c2d2d;
  }
  .timing-card--neutral .timing-label,
  .timing-card--neutral .timing-hours {
    color: var(--muted-foreground);
  }
  .timing-card--neutral .timing-intensity,
  .timing-card--neutral .timing-value {
    color: var(--foreground);
  }

  /* ── Sector cards ── */
  .sector-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 0.75rem;
    margin-bottom: 1.5rem;
  }
  .sector-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.2rem 1.3rem;
    border-top: 3px solid var(--accent);
  }
  .sector-name {
    font-family: var(--font-display);
    font-size: var(--text-base);
    font-weight: 600;
    color: var(--foreground);
    margin-bottom: 0.15rem;
  }
  .sector-profile {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-bottom: 0.75rem;
    line-height: 1.4;
  }
  .sector-metric {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    padding: 0.3rem 0;
    border-top: 1px solid var(--border);
  }
  .sector-metric-label {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
  }
  .sector-metric-value {
    font-family: var(--font-display);
    font-size: var(--text-base);
    font-weight: 600;
    color: var(--foreground);
  }
  .sector-saving {
    color: var(--accent);
    font-weight: 600;
  }

  /* ── Estimator ── */
  .estimator-results {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 0.75rem;
    margin-top: 1rem;
    margin-bottom: 1.5rem;
  }
  .estimator-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.5rem 0.65rem;
    text-align: center;
    border-top: 3px solid var(--accent);
  }
  .estimator-card--saving {
    background: #f7fbfb;
    border-color: rgba(11, 127, 148, 0.24);
  }
  .estimator-label {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-bottom: 0.1rem;
  }
  .estimator-value {
    font-family: var(--font-display);
    font-size: var(--text-base);
    font-weight: 600;
    color: var(--foreground);
  }
  .estimator-sub {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-top: 0.1rem;
  }

  /* ── Metric cards (KPI) ── */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(5, minmax(0, 1fr));
    gap: 0.75rem;
  }
  .metric-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 16px 20px;
    text-align: center;
    min-height: 120px;
    display: flex;
    flex-direction: column;
    justify-content: center;
  }
  .metric-card.positive {
    background: var(--accent-light);
    border-color: var(--bg-green-border);
  }
  .metric-label {
    font-size: var(--text-base);
    color: var(--muted-foreground);
    font-weight: 400;
    font-family: var(--font-body);
  }
  .metric-value {
    font-size: var(--text-xl);
    font-weight: 700;
    font-family: var(--font-display);
    color: var(--foreground);
    letter-spacing: -0.02em;
  }
  .metric-sub {
    font-size: var(--text-base);
    color: var(--muted-foreground);
    font-family: var(--font-body);
    min-height: 2em;
  }

  /* ── ASRS cards ── */
  .asrs-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.75rem;
    max-width: 1000px;
    margin: 0 auto;
  }
  .asrs-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.3rem 1.5rem;
    height: 100%;
  }
  .asrs-card .asrs-tag {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 0.4rem;
  }
  .asrs-card .asrs-group {
    font-family: var(--font-display);
    font-size: var(--text-xl);
    font-weight: 600;
    color: var(--foreground);
    margin-bottom: 0.4rem;
  }
  .asrs-card .asrs-date {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-bottom: 0.7rem;
  }
  .asrs-card .asrs-threshold {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--foreground);
    line-height: 1.5;
  }

  /* ── Comparison panel ── */
  .comparison-panel {
    background: #35342f;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1rem 1.15rem;
    height: 100%;
  }
  .comparison-label {
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-bottom: 0.4rem;
    font-family: var(--font-body);
  }
  .comparison-value {
    font-size: var(--text-lg);
    font-weight: 600;
    color: var(--foreground);
    margin-bottom: 0.35rem;
    font-family: var(--font-display);
  }
  .comparison-sub {
    font-size: var(--text-base);
    color: var(--muted-foreground);
    line-height: 1.5;
    font-family: var(--font-body);
  }
  .comparison-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.9rem 1.2rem;
    margin-top: 0.9rem;
  }
  .comparison-item-label {
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-bottom: 0.25rem;
    font-family: var(--font-body);
  }
  .comparison-item-value {
    font-size: 1.125rem;
    font-weight: 600;
    color: var(--foreground);
    margin-bottom: 0.2rem;
    font-family: var(--font-display);
  }
  .comparison-item-copy {
    font-size: var(--text-base);
    color: var(--muted-foreground);
    line-height: 1.5;
    font-family: var(--font-body);
  }

  /* ── Chart elements ── */
  .chart-title {
    font-family: var(--font-display);
    font-size: calc(var(--text-xl) * 1.02);
    font-weight: 600;
    color: var(--foreground);
    line-height: 1.25;
    margin: 0 0 0.55rem 0;
    letter-spacing: -0.01em;
  }
  .chart-axis-notes {
    display: flex;
    justify-content: space-between;
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin: 0 0 0.4rem 0;
    font-family: var(--font-body);
  }
  .chart-insight {
    background: #ffffff;
    border-left: 4px solid var(--accent-warm);
    padding: 0.85rem 1rem;
    margin-top: 0.4rem;
    margin-bottom: 1.25rem;
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--foreground);
    line-height: 1.55;
  }
  .chart-insight strong { color: var(--accent); }

  /* ── Section text ── */
  .section-text {
    line-height: 1.5;
    font-size: var(--text-base);
    color: var(--foreground);
    margin: 0 auto 24px auto;
    max-width: 960px;
    font-family: var(--font-body);
  }
  .section-text a { color: var(--accent); text-decoration: none; border-bottom: 1px solid var(--border); }
  .section-text a:hover { border-bottom-color: var(--accent); }
  .info-panel {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.95rem 1rem;
    margin-top: 0.75rem;
    margin-bottom: 0.6rem;
    border-left: 4px solid var(--accent);
  }
  .info-panel .section-heading {
    margin-bottom: 0.6rem;
  }
  code, pre, .stCode {
    font-family: var(--font-mono) !important;
    font-variant-numeric: tabular-nums lining-nums;
  }
  .info-panel p,
  .info-panel div,
  .info-panel li {
    font-size: var(--text-base);
    line-height: 1.6;
  }

  /* ── Methodology / intro blocks ── */
  .intro-hero {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.5rem 1.6rem;
    margin-bottom: 1.5rem;
    max-width: 1000px;
    margin-left: auto;
    margin-right: auto;
  }
  .intro-hero h2 {
    font-family: var(--font-display) !important;
    font-size: var(--text-xl) !important;
    color: var(--foreground) !important;
    margin-bottom: 0.9rem !important;
    font-weight: 600 !important;
  }
  .intro-hero p {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--foreground);
    line-height: 1.5;
    margin: 0;
  }
  .intro-hero strong, .intro-hero em {
    color: var(--foreground); font-style: normal; font-weight: 600;
  }
  .methodology-note {
    margin-top: 2rem;
    max-width: 1000px;
    margin-left: auto;
    margin-right: auto;
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--foreground);
    line-height: 1.5;
  }
  .methodology-note h2 {
    font-family: var(--font-display) !important;
    font-size: var(--text-xl) !important;
    font-weight: 600 !important;
  }
  .methodology-note strong { color: var(--foreground); }
  .methodology-note a { color: var(--accent); text-decoration: none; border-bottom: 1px solid var(--border); }

  /* ── Controls ── */
  .controls-note {
    font-size: var(--text-base);
    color: var(--muted-foreground);
    line-height: 1.5;
    margin-top: 0.65rem;
    font-family: var(--font-body);
  }
  .controls-note a { color: var(--accent); }
  label, .stRadio label p, .stSelectbox label, .stMultiSelect label, .stDateInput label, .stNumberInput label {
    color: var(--foreground) !important;
    font-family: var(--font-body) !important;
    font-size: var(--text-base) !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  div[data-baseweb="select"] > div,
  div[data-testid="stDateInput"] input,
  div[data-testid="stNumberInput"] input {
    background: #ffffff !important;
    border: 1px solid var(--border) !important;
    border-radius: 0 !important;
    color: var(--foreground) !important;
    box-shadow: none !important;
  }
  div[data-baseweb="select"] span,
  div[data-baseweb="select"] input,
  div[data-testid="stDateInput"] input,
  div[data-testid="stNumberInput"] input {
    color: var(--foreground) !important;
    font-family: var(--font-body) !important;
  }
  div[data-baseweb="tag"] {
    background: var(--accent-light) !important;
    border-radius: 0 !important;
    border: 1px solid rgba(11, 127, 148, 0.18) !important;
  }
  div[data-baseweb="radio"] label {
    background: transparent !important;
  }
  div[data-testid="stCaptionContainer"] {
    color: var(--muted-foreground) !important;
    font-family: var(--font-body) !important;
  }

  /* ── Footer ── */
  .page-footer {
    margin: 2.2rem 0 0 0;
    width: 100vw;
    margin-left: calc(-50vw + 50%);
    margin-right: calc(-50vw + 50%);
    border-top: 1px solid var(--footer-border);
    background: var(--footer-bg);
    padding: 0.9rem 2rem;
    color: var(--muted-foreground);
    font-size: var(--text-base);
    font-family: var(--font-body);
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .footer-inner {
    max-width: 1100px;
    margin: 0 auto;
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    flex-wrap: wrap;
  }
  .linkedin-link {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    color: var(--accent);
    text-decoration: none;
    font-weight: 600;
  }
  .linkedin-link:hover { color: var(--accent-warm); }
  .linkedin-icon { width: 18px; height: 18px; fill: #374151; }

  details[data-testid="stExpander"] {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    border-left: 4px solid var(--accent);
  }
  details[data-testid="stExpander"] summary {
    color: var(--foreground);
  }
  div[data-testid="stDataFrame"] {
    background: var(--card);
    border-radius: var(--radius);
    border: 1px solid var(--border);
  }

  /* ── Responsive ── */
  @media (max-width: 1200px) {
    .hero-grid, .sector-grid, .estimator-results {
      grid-template-columns: repeat(2, 1fr);
    }
  }
  @media (max-width: 900px) {
    .block-container {
      padding-left: 0.65rem !important;
      padding-right: 0.65rem !important;
      padding-top: 1.25rem !important;
      max-width: 100% !important;
    }
    .header-band { padding: 1rem 0 0.9rem 0; }
    .header-band .page-header { font-size: var(--text-xl); padding: 0 0.9rem; }
    .header-band .page-deck { padding: 0 0.9rem; }
    .hero-grid, .sector-grid, .estimator-results {
      grid-template-columns: repeat(2, 1fr);
    }
    .timing-grid {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .kpi-grid {
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 0.65rem;
    }
    .kpi-grid .metric-card.full-span { grid-column: 1 / -1; }
    .asrs-grid {
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 0.45rem;
    }
    .asrs-card { padding: 0.7rem 0.55rem !important; }
    .asrs-card .asrs-group { font-size: 1rem !important; }
    .asrs-card .asrs-tag, .asrs-card .asrs-date, .asrs-card .asrs-threshold {
      font-size: 0.72rem !important; line-height: 1.35 !important;
    }
    .comparison-grid { grid-template-columns: 1fr !important; }
    .section-text { max-width: 100% !important; }
    .chart-title { font-size: 1.05rem; }
    .metric-card { min-height: 100px; padding: 14px 16px; }
    .metric-sub { min-height: 0; }
    div[data-testid="stHorizontalBlock"] { flex-direction: column !important; gap: 0.75rem !important; }
    div[data-testid="column"] { width: 100% !important; flex: 1 1 100% !important; }
    .js-plotly-plot .modebar, .js-plotly-plot .legend { display: none !important; }
  }
  @media (max-width: 600px) {
    .hero-grid, .timing-grid, .sector-grid, .estimator-results {
      grid-template-columns: 1fr;
    }
    .asrs-grid { grid-template-columns: 1fr; }
    .insight-callout { font-size: var(--text-base); padding: 0.85rem 1rem; }
  }
  @media (min-width: 1800px) {
    :root { --text-base: 0.9rem; --text-lg: 1.04rem; --text-xl: 1.4rem; --text-2xl: 2.35rem; }
    .block-container { max-width: 1080px !important; }
  }

  footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# Data loading — split by source
# ─────────────────────────────────────────────────────────────
def _enrich(scada: pd.DataFrame, data_dir: Path) -> pd.DataFrame:
    """Join SCADA data with DUID lookup and emissions factors, add computed columns."""
    lookup = (
        read_csv_from_any(data_dir / "duid_lookup.csv")
        [["DUID", "Unit Name", "Technology Type", "Region"]]
        .drop_duplicates("DUID")
    )
    ef_raw = read_csv_from_any(data_dir / "emissions_factors.csv")
    ef_s1 = (
        ef_raw[ef_raw["scope"] == "scope_1"]
        [["technology_type", "emission_factor_tCO2e_MWh"]]
        .rename(columns={"technology_type": "Technology Type",
                         "emission_factor_tCO2e_MWh": "ef_scope1"})
    )
    ef_s3 = (
        ef_raw[ef_raw["scope"] == "scope_3"]
        [["technology_type", "emission_factor_tCO2e_MWh"]]
        .rename(columns={"technology_type": "Technology Type",
                         "emission_factor_tCO2e_MWh": "ef_scope3"})
    )
    df = (
        scada
        .merge(lookup, on="DUID", how="left")
        .merge(ef_s1,  on="Technology Type", how="left")
        .merge(ef_s3,  on="Technology Type", how="left")
    )
    df["Technology Type"] = df["Technology Type"].fillna("Unknown")
    df["ef_scope1"] = df["ef_scope1"].fillna(0.1855)
    df["ef_scope3"] = df["ef_scope3"].fillna(0.0)
    df["mwh"]          = df["SCADAVALUE"] * (5 / 60)
    df["tco2e_scope1"] = df["mwh"] * df["ef_scope1"]
    df["tco2e_scope3"] = df["mwh"] * df["ef_scope3"]
    df["tco2e_total"]  = df["tco2e_scope1"] + df["tco2e_scope3"]
    return df


def _read_scada_csv(csv_path: Path) -> pd.DataFrame:
    scada = read_csv_from_any(csv_path, parse_dates=["SETTLEMENTDATE"])
    scada["SETTLEMENTDATE"] = pd.to_datetime(scada["SETTLEMENTDATE"], errors="coerce", format="mixed")
    scada = scada.dropna(subset=["SETTLEMENTDATE"])
    return scada[scada["SCADAVALUE"] > 0].copy()


def _aggregate_to_generation_mix(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate enriched DUID-level rows into the app's interval generation-mix shape."""
    if df.empty:
        return pd.DataFrame(
            columns=[
                "SETTLEMENTDATE",
                "Region",
                "Technology Type",
                "mwh",
                "tco2e_scope1",
                "tco2e_scope3",
                "tco2e_total",
            ]
        )

    grouped = (
        df.groupby(["SETTLEMENTDATE", "Region", "Technology Type"], as_index=False)
        .agg(
            mwh=("mwh", "sum"),
            tco2e_scope1=("tco2e_scope1", "sum"),
            tco2e_scope3=("tco2e_scope3", "sum"),
            tco2e_total=("tco2e_total", "sum"),
        )
        .sort_values(["SETTLEMENTDATE", "Region", "Technology Type"])
    )
    return grouped


@st.cache_data(ttl=270)  # 270s so cache expires before the 5-min rerun fires
def load_live_today_generation_mix(data_dir_str: str, file_mtime: float = 0.0, gcs_hash: str = "") -> pd.DataFrame:
    """Load the current day's CSV snapshot and aggregate it to region × technology × interval."""
    # gcs_hash is passed to ensure cache invalidation when the bucket file changes.
    data_dir = Path(data_dir_str)
    today_csv = data_dir / "dispatch_scada_today.csv"
    
    scada = _read_scada_csv(today_csv)
    if scada.empty:
        return pd.DataFrame()

    return _aggregate_to_generation_mix(_enrich(scada, data_dir))


@st.cache_data(ttl=300)
def load_generation_mix_from_duckdb(duckdb_cache_key: str = "") -> pd.DataFrame:
    """Load Gold interval generation mix from DuckDB for dashboard use."""
    try:
        conn = get_db_connection()
        query = """
        SELECT
            settlement_date AS SETTLEMENTDATE,
            region AS Region,
            technology_type AS "Technology Type",
            generation_mwh AS mwh,
            scope1_tco2e,
            scope3_tco2e,
            total_tco2e AS tco2e_total
        FROM main_gold.fct_generation_mix_interval
        ORDER BY settlement_date, region, technology_type
        """
        df = conn.execute(query).df()
        conn.close()
        if df.empty:
            return df
        df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], errors="coerce", format="mixed")
        df = df.dropna(subset=["SETTLEMENTDATE"])
        return df.rename(columns={"scope1_tco2e": "tco2e_scope1", "scope3_tco2e": "tco2e_scope3"})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=270)  # 270s so cache expires before the 5-min rerun fires
def load_full_history(
    data_dir_str: str,
    today_file_mtime: float = 0.0,
    gcs_hash: str = "",
    latest_archive_cache_key: str = "",
    duckdb_cache_key: str = "",
) -> pd.DataFrame:
    """Load the full available history, preferring DuckDB Gold and falling back to raw CSV enrichment.
    The gcs_hash argument ensures that Streamlit refreshes the cache when the bucket updates.
    """
    gold_history = load_generation_mix_from_duckdb(duckdb_cache_key=duckdb_cache_key)
    live_today = load_live_today_generation_mix(data_dir_str, file_mtime=today_file_mtime, gcs_hash=gcs_hash)
    if not gold_history.empty:
        gold_max = gold_history["SETTLEMENTDATE"].max()
        
        # Fill gap from the most recent monthly archive if DuckDB is lagging
        data_dir = Path(data_dir_str)
        monthly_paths = list_monthly_archives_from_any(data_dir)
        gap_data = pd.DataFrame()
        
        if monthly_paths and not pd.isna(gold_max):
            latest_archive = monthly_paths[-1]
            raw_arc = _read_scada_csv(Path(latest_archive))
            if not raw_arc.empty:
                raw_arc = raw_arc[raw_arc["SETTLEMENTDATE"] > gold_max]
                if not live_today.empty:
                    # Exclude the current live day from the gap archive since live_today covers it
                    live_date = live_today["SETTLEMENTDATE"].dt.date.max()
                    raw_arc = raw_arc[raw_arc["SETTLEMENTDATE"].dt.date != live_date]
                if not raw_arc.empty:
                    gap_data = _aggregate_to_generation_mix(_enrich(raw_arc, data_dir))

        frames = [gold_history]
        if not gap_data.empty:
            frames.append(gap_data)
        if not live_today.empty:
            # We also drop live_today's date from gold_history just in case of overlap
            live_date = live_today["SETTLEMENTDATE"].dt.date.max()
            gold_history = gold_history[gold_history["SETTLEMENTDATE"].dt.date != live_date]
            frames = [gold_history] # Re-seat the frames block
            if not gap_data.empty:
                frames.append(gap_data)
            frames.append(live_today)

        merged = pd.concat(frames, ignore_index=True)
        merged.drop_duplicates(subset=["SETTLEMENTDATE", "Region", "Technology Type"], keep="last", inplace=True)
        return merged.sort_values(["SETTLEMENTDATE", "Region", "Technology Type"]).reset_index(drop=True)

    data_dir = Path(data_dir_str)
    full_csv = data_dir / "dispatch_scada.csv"
    monthly_paths = list_monthly_archives_from_any(data_dir)

    frames = []
    if GCS_BUCKET:
        # Check for full_csv in GCS if needed, but usually we use monthly
        if get_bucket().blob("dispatch_scada.csv").exists():
            frames.append(_read_scada_csv(Path("dispatch_scada.csv")))
        for path in monthly_paths:
            frames.append(_read_scada_csv(Path(path)))
    else:
        if full_csv.exists():
            frames.append(_read_scada_csv(full_csv))
        for path in monthly_paths:
            frames.append(_read_scada_csv(Path(path)))

    if not frames:
        raise FileNotFoundError(
            "No SCADA history found. Expected DuckDB Gold outputs or dispatch_scada_YYYY-MM.csv archives."
        )

    scada = pd.concat(frames, ignore_index=True)
    scada.drop_duplicates(subset=["SETTLEMENTDATE", "DUID"], inplace=True)
    scada.sort_values(["SETTLEMENTDATE", "DUID"], inplace=True)
    return _enrich(scada, data_dir)


def load_scada_for_date(data_dir_str: str, date: datetime.date) -> pd.DataFrame:
    """Load and filter data for a specific calendar date by slicing the full history."""
    data_dir = Path(data_dir_str)
    today_csv = data_dir / "dispatch_scada_today.csv"
    mtime = today_csv.stat().st_mtime if today_csv.exists() else 0.0
    g_hash = get_gcs_file_hash("dispatch_scada_today.csv")
    latest_archive_cache_key = get_latest_archive_cache_key(data_dir)
    duckdb_cache_key = get_duckdb_cache_key()
    df = load_full_history(
        data_dir_str,
        today_file_mtime=mtime,
        gcs_hash=g_hash,
        latest_archive_cache_key=latest_archive_cache_key,
        duckdb_cache_key=duckdb_cache_key,
    )
    return df[df["SETTLEMENTDATE"].dt.date == date].copy()



def aggregate_daily_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate interval data into daily metrics by region."""
    if df.empty:
        return pd.DataFrame(
            columns=[
                "date", "Region", "total_mwh", "scope1_tco2e", "total_tco2e",
                "renewable_mwh", "intensity_scope1", "intensity_total", "re_share",
            ]
        )

    work = df.copy()
    work["SETTLEMENTDATE"] = pd.to_datetime(work["SETTLEMENTDATE"], errors="coerce", format="mixed")
    work = work.dropna(subset=["SETTLEMENTDATE"])
    if work.empty:
        return pd.DataFrame(
            columns=[
                "date", "Region", "total_mwh", "scope1_tco2e", "total_tco2e",
                "renewable_mwh", "intensity_scope1", "intensity_total", "re_share",
            ]
        )
    work["date"] = work["SETTLEMENTDATE"].dt.date
    work["renewable_mwh"] = work["mwh"].where(work["Technology Type"].isin(ZERO_EMISSION), 0.0)
    daily = (
        work.groupby(["date", "Region"])
        .agg(
            total_mwh=("mwh", "sum"),
            scope1_tco2e=("tco2e_scope1", "sum"),
            total_tco2e=("tco2e_total", "sum"),
            renewable_mwh=("renewable_mwh", "sum"),
        )
        .reset_index()
    )
    daily["intensity_scope1"] = (daily["scope1_tco2e"] / daily["total_mwh"]).where(daily["total_mwh"] > 0)
    daily["intensity_total"] = (daily["total_tco2e"] / daily["total_mwh"]).where(daily["total_mwh"] > 0)
    daily["re_share"] = (100 * daily["renewable_mwh"] / daily["total_mwh"]).where(daily["total_mwh"] > 0)
    return daily.sort_values(["date", "Region"]).reset_index(drop=True)


@st.cache_data(ttl=300)
def load_historical_daily_metrics(data_dir_str: str) -> pd.DataFrame:
    """Aggregate all daily metrics continuously using the full history."""
    data_dir = Path(data_dir_str)
    today_csv = data_dir / "dispatch_scada_today.csv"
    mtime = today_csv.stat().st_mtime if today_csv.exists() else 0.0
    g_hash = get_gcs_file_hash("dispatch_scada_today.csv")
    latest_archive_cache_key = get_latest_archive_cache_key(data_dir)
    duckdb_cache_key = get_duckdb_cache_key()
    history = load_full_history(
        data_dir_str,
        today_file_mtime=mtime,
        gcs_hash=g_hash,
        latest_archive_cache_key=latest_archive_cache_key,
        duckdb_cache_key=duckdb_cache_key,
    )
    return aggregate_daily_metrics(history)


@st.cache_data(ttl=300)
def get_latest_available_data_date(data_dir_str: str) -> datetime.date:
    """Return the newest calendar date actually present in the loaded history."""
    daily_metrics = load_historical_daily_metrics(data_dir_str)
    if daily_metrics.empty:
        return get_aemo_date()
    return pd.Timestamp(daily_metrics["date"].max()).date()


@st.cache_data(ttl=300)
def load_today_daily_metrics(data_dir_str: str) -> pd.DataFrame:
    """Aggregate today's live file into daily metrics."""
    today_df = load_today(data_dir_str)
    return aggregate_daily_metrics(today_df)


@st.cache_data(ttl=300)
def load_emissions_intensity_from_duckdb() -> pd.DataFrame:
    """Load regional emissions intensity from Gold layer (main_gold.fct_regional_emissions_intensity)."""
    try:
        conn = get_db_connection()
        query = """
        SELECT 
            settlement_date,
            region,
            total_generation_mwh,
            scope1_tco2e,
            scope3_tco2e,
            total_tco2e,
            emissions_intensity_scope1_gco2eq_per_kwh,
            emissions_intensity_total_gco2eq_per_kwh
        FROM main_gold.fct_regional_emissions_intensity
        ORDER BY settlement_date DESC, region
        """
        df = conn.execute(query).df()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to load emissions data from Gold layer: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_dashboard_metadata(data_dir_str: str) -> dict:
    """Return available date bounds, regions, and archive status for the dashboard."""
    data_dir = Path(data_dir_str)
    monthly_files = list_monthly_archives_from_any(data_dir)
    has_monthly_archives = bool(monthly_files)

    try:
        conn = get_db_connection()
        bounds = conn.execute(
            """
            SELECT
                CAST(min(settlement_date) AS DATE) AS min_date,
                CAST(max(settlement_date) AS DATE) AS max_date
            FROM main_gold.fct_regional_emissions_intensity
            """
        ).df()
        region_df = conn.execute(
            """
            SELECT DISTINCT region
            FROM main_gold.dim_generator
            WHERE region IS NOT NULL
            ORDER BY region
            """
        ).df()
        conn.close()

        if not bounds.empty and pd.notna(bounds.loc[0, "max_date"]):
            return {
                "date_min": pd.Timestamp(bounds.loc[0, "min_date"]).date(),
                "date_max": pd.Timestamp(bounds.loc[0, "max_date"]).date(),
                "regions": region_df["region"].dropna().tolist(),
                "has_monthly_archives": has_monthly_archives,
            }
    except Exception:
        pass

    if monthly_files:
        earliest_month = Path(monthly_files[0]).stem[-7:]
        date_min = pd.Period(earliest_month, freq="M").start_time.date()
    elif GCS_BUCKET and get_bucket().blob("dispatch_scada.csv").exists():
        min_df = read_csv_from_any("dispatch_scada.csv", usecols=["SETTLEMENTDATE"], parse_dates=["SETTLEMENTDATE"])
        date_min = min_df["SETTLEMENTDATE"].min().date()
    elif not GCS_BUCKET and (data_dir / "dispatch_scada.csv").exists():
        min_df = pd.read_csv(data_dir / "dispatch_scada.csv", usecols=["SETTLEMENTDATE"], parse_dates=["SETTLEMENTDATE"])
        date_min = min_df["SETTLEMENTDATE"].min().date()
    else:
        date_min = get_aemo_date()

    lookup_df = read_csv_from_any(data_dir / "duid_lookup.csv")
    regions = sorted(lookup_df["Region"].dropna().unique().tolist())

    return {
        "date_min": date_min,
        "date_max": get_aemo_date(),
        "regions": regions,
        "has_monthly_archives": has_monthly_archives,
    }


@st.cache_data(ttl=300)
def load_emissions_factor_reference(data_dir_str: str) -> pd.DataFrame:
    """Load emissions factors for reference tables, preferring Bronze/duckdb over raw CSV."""
    try:
        conn = get_db_connection()
        df = conn.execute(
            """
            SELECT technology_type, scope, emission_factor_tCO2e_MWh
            FROM bronze.emissions_factors
            ORDER BY technology_type, scope
            """
        ).df()
        conn.close()
        if not df.empty:
            return df
    except Exception:
        pass

    return read_csv_from_any(Path(data_dir_str) / "emissions_factors.csv")


def combine_daily_metrics_for_regions(
    daily_metrics: pd.DataFrame,
    selected_regions: list[str],
    scope: str,
) -> pd.DataFrame:
    """Collapse region-level daily metrics into a single daily series for the selected regions."""
    if daily_metrics.empty:
        return pd.DataFrame(columns=["date", "total_mwh", "intensity", "re_share"])

    filtered = daily_metrics.copy()
    if selected_regions:
        filtered = filtered[filtered["Region"].isin(selected_regions)]
    if filtered.empty:
        return pd.DataFrame(columns=["date", "total_mwh", "intensity", "re_share"])

    grouped = (
        filtered.groupby("date")
        .agg(
            total_mwh=("total_mwh", "sum"),
            scope1_tco2e=("scope1_tco2e", "sum"),
            total_tco2e=("total_tco2e", "sum"),
            renewable_mwh=("renewable_mwh", "sum"),
        )
        .reset_index()
    )
    emissions_col = "scope1_tco2e" if scope == "Scope 1 only" else "total_tco2e"
    grouped["intensity"] = (grouped[emissions_col] / grouped["total_mwh"]).where(grouped["total_mwh"] > 0)
    grouped["re_share"] = (100 * grouped["renewable_mwh"] / grouped["total_mwh"]).where(grouped["total_mwh"] > 0)
    return grouped.sort_values("date").reset_index(drop=True)


def shift_years_safe(date_value: datetime.date, years_back: int) -> datetime.date:
    """Shift a date backwards by whole years, falling back to 28 Feb for leap-day collisions."""
    try:
        return date_value.replace(year=max(date_value.year - years_back, 1))
    except ValueError:
        return date_value.replace(month=2, day=28, year=max(date_value.year - years_back, 1))


def get_previous_financial_year(selected_date: datetime.date) -> tuple[datetime.date, datetime.date, str]:
    """Return the previous Australian financial year range and required label."""
    if selected_date >= datetime.date(selected_date.year, 7, 1):
        fy_end_year = selected_date.year
    else:
        fy_end_year = selected_date.year - 1
    fy_start = datetime.date(fy_end_year - 1, 7, 1)
    fy_end = datetime.date(fy_end_year, 6, 30)
    label = f"Previous FY {fy_start.strftime('%d/%B/%y')} - {fy_end.strftime('%d/%B/%y')}"
    return fy_start, fy_end, label


def get_current_financial_year_ytd(selected_date: datetime.date) -> tuple[datetime.date, datetime.date, str]:
    """Return the current Australian financial year YTD range and compact label."""
    if selected_date >= datetime.date(selected_date.year, 7, 1):
        fy_start = datetime.date(selected_date.year, 7, 1)
        fy_start_year = selected_date.year
        fy_end_year = selected_date.year + 1
    else:
        fy_start = datetime.date(selected_date.year - 1, 7, 1)
        fy_start_year = selected_date.year - 1
        fy_end_year = selected_date.year
    label = f"YTD {str(fy_start_year)[-2:]}-{str(fy_end_year)[-2:]}"
    return fy_start, selected_date, label


def resolve_trend_window(
    daily_series: pd.DataFrame,
    range_label: str,
    anchor_date: datetime.date,
) -> tuple[pd.DataFrame, str]:
    """Filter the daily trend series to the requested range, clamping to available data."""
    if daily_series.empty:
        return daily_series, "No history available"

    daily_series = daily_series.copy()
    daily_series["date"] = pd.to_datetime(daily_series["date"])
    min_date = daily_series["date"].min().date()
    max_date = daily_series["date"].max().date()

    if range_label == "Today":
        filtered = daily_series[daily_series["date"].dt.date == anchor_date].copy()
        if filtered.empty:
            filtered = daily_series[daily_series["date"].dt.date == max_date].copy()
            if filtered.empty:
                filtered = daily_series.tail(1).copy()
        return filtered, f"{filtered['date'].min().date().strftime('%d %b %Y')}"

    if range_label == "Past week":
        start = max(anchor_date - datetime.timedelta(days=6), min_date)
        filtered = daily_series[daily_series["date"].dt.date >= start].copy()
        if filtered.empty:
            filtered = daily_series.copy()
        return filtered, f"{filtered['date'].min().date().strftime('%d %b %Y')} - {filtered['date'].max().date().strftime('%d %b %Y')}"

    current_fy_start, current_fy_end, current_fy_label = get_current_financial_year_ytd(anchor_date)
    if range_label == current_fy_label:
        window_start = max(current_fy_start, min_date)
        filtered = daily_series[
            daily_series["date"].dt.date.between(window_start, current_fy_end)
        ].copy()
        if filtered.empty:
            filtered = daily_series.copy()
            return filtered, f"{current_fy_label} (showing available history)"
        return filtered, f"{current_fy_label} · {filtered['date'].min().date().strftime('%d %b %Y')} - {filtered['date'].max().date().strftime('%d %b %Y')}"

    if range_label == "MAX (28 years)":
        return daily_series, f"{min_date.strftime('%d %b %Y')} - {max_date.strftime('%d %b %Y')}"

    if range_label in {"10Y", "5Y"}:
        years = int(range_label[:-1])
        start = shift_years_safe(anchor_date, years)
        start = max(start, min_date)
        filtered = daily_series[daily_series["date"].dt.date >= start].copy()
        if filtered.empty:
            filtered = daily_series.copy()
        return filtered, f"{filtered['date'].min().date().strftime('%d %b %Y')} - {filtered['date'].max().date().strftime('%d %b %Y')}"

    fy_start, fy_end, fy_label = get_previous_financial_year(anchor_date)
    window_start = max(fy_start, min_date)
    window_end = min(fy_end, max_date)
    filtered = daily_series[
        daily_series["date"].dt.date.between(window_start, window_end)
    ].copy()
    if filtered.empty:
        filtered = daily_series.copy()
        return filtered, f"{fy_label} (showing available history)"
    return filtered, fy_label


def get_weather_features_for_date(
    target_date: datetime.date,
    selected_regions: list[str],
) -> dict | None:
    """Weather adapter boundary for future integration. V1 intentionally falls back to analog-only weights."""
    _ = (target_date, tuple(selected_regions))
    return None


def compute_weather_similarity_weight(
    target_weather: dict | None,
    candidate_date: datetime.date,
    selected_regions: list[str],
) -> float:
    """Return a multiplicative weather similarity weight. V1 fallback is neutral when no provider is configured."""
    _ = (candidate_date, tuple(selected_regions))
    return 1.0 if target_weather is None else 1.0


def build_analog_forecast(
    history_df: pd.DataFrame,
    selected_regions: list[str],
    scope: str,
    resolution: str,
    target_date: datetime.date,
    observed_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Forecast the remaining intraday intensity profile using weighted analog days."""
    if history_df.empty or observed_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    filtered_history = history_df.copy()
    if selected_regions:
        filtered_history = filtered_history[filtered_history["Region"].isin(selected_regions)]
    if filtered_history.empty:
        return pd.DataFrame(), pd.DataFrame()

    emission_col = "tco2e_scope1" if scope == "Scope 1 only" else "tco2e_total"
    interval_label = observed_df["SETTLEMENTDATE"].max().floor(resolution)
    observed_profile = (
        observed_df.groupby(observed_df["SETTLEMENTDATE"].dt.floor(resolution))
        .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
        .reset_index()
        .rename(columns={"SETTLEMENTDATE": "period"})
    )
    observed_profile["intensity"] = (observed_profile["tco2e"] / observed_profile["mwh"]).where(observed_profile["mwh"] > 0)
    observed_profile["slot"] = (
        observed_profile["period"].dt.hour * 60 + observed_profile["period"].dt.minute
    ) // (5 if resolution == "5min" else 60)

    today_weather = get_weather_features_for_date(target_date, selected_regions)
    analog_rows = []

    for candidate_date, candidate_df in filtered_history.groupby(filtered_history["SETTLEMENTDATE"].dt.date):
        if candidate_date == target_date:
            continue

        candidate_profile = (
            candidate_df.groupby(candidate_df["SETTLEMENTDATE"].dt.floor(resolution))
            .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
            .reset_index()
            .rename(columns={"SETTLEMENTDATE": "period"})
        )
        candidate_profile["intensity"] = (candidate_profile["tco2e"] / candidate_profile["mwh"]).where(candidate_profile["mwh"] > 0)
        candidate_profile["slot"] = (
            candidate_profile["period"].dt.hour * 60 + candidate_profile["period"].dt.minute
        ) // (5 if resolution == "5min" else 60)

        compare = observed_profile[["slot", "intensity"]].merge(
            candidate_profile[["slot", "intensity"]],
            on="slot",
            suffixes=("_obs", "_cand"),
        )
        compare = compare[compare["slot"] <= observed_profile["slot"].max()]
        if compare.empty:
            continue

        mae = (compare["intensity_obs"] - compare["intensity_cand"]).abs().mean()
        weekday_weight = 1.35 if candidate_date.weekday() == target_date.weekday() else 0.9
        weekend_weight = 1.15 if (candidate_date.weekday() >= 5) == (target_date.weekday() >= 5) else 0.9
        month_distance = abs(candidate_date.month - target_date.month)
        month_distance = min(month_distance, 12 - month_distance)
        season_weight = max(0.45, 1 - (month_distance / 12))
        last_year_weight = 1.3 if (
            candidate_date.month == target_date.month and candidate_date.day == target_date.day
        ) else 1.0
        weather_weight = compute_weather_similarity_weight(today_weather, candidate_date, selected_regions)
        similarity_weight = 1 / max(mae, 0.015)
        score = weekday_weight * weekend_weight * season_weight * last_year_weight * weather_weight * similarity_weight

        analog_rows.append({
            "candidate_date": candidate_date,
            "score": score,
            "mae": mae,
        })

    analog_df = pd.DataFrame(analog_rows).sort_values("score", ascending=False)
    if analog_df.empty:
        return observed_profile, pd.DataFrame()

    top_analogs = analog_df.head(12)
    future_slot_threshold = observed_profile["slot"].max()
    future_profiles = []

    for _, analog in top_analogs.iterrows():
        analog_date = analog["candidate_date"]
        analog_df_day = filtered_history[filtered_history["SETTLEMENTDATE"].dt.date == analog_date]
        candidate_profile = (
            analog_df_day.groupby(analog_df_day["SETTLEMENTDATE"].dt.floor(resolution))
            .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
            .reset_index()
            .rename(columns={"SETTLEMENTDATE": "period"})
        )
        candidate_profile["intensity"] = (candidate_profile["tco2e"] / candidate_profile["mwh"]).where(candidate_profile["mwh"] > 0)
        candidate_profile["slot"] = (
            candidate_profile["period"].dt.hour * 60 + candidate_profile["period"].dt.minute
        ) // (5 if resolution == "5min" else 60)
        candidate_profile = candidate_profile[candidate_profile["slot"] > future_slot_threshold].copy()
        if candidate_profile.empty:
            continue
        candidate_profile["candidate_date"] = analog_date
        candidate_profile["score"] = analog["score"]
        future_profiles.append(candidate_profile[["slot", "intensity", "candidate_date", "score"]])

    if not future_profiles:
        return observed_profile, pd.DataFrame()

    future_df = pd.concat(future_profiles, ignore_index=True)
    summary_rows = []
    base_period = observed_profile["period"].max().normalize()
    slot_minutes = 5 if resolution == "5min" else 60

    for slot, slot_df in future_df.groupby("slot"):
        weights = slot_df["score"].to_numpy()
        values = slot_df["intensity"].to_numpy()
        forecast_intensity = (values * weights).sum() / weights.sum()
        summary_rows.append({
            "slot": slot,
            "period": base_period + pd.Timedelta(minutes=int(slot * slot_minutes)),
            "forecast_intensity": forecast_intensity,
            "band_low": float(slot_df["intensity"].quantile(0.25)) if len(slot_df) >= 3 else None,
            "band_high": float(slot_df["intensity"].quantile(0.75)) if len(slot_df) >= 3 else None,
        })

    forecast_df = pd.DataFrame(summary_rows).sort_values("slot")
    return observed_profile, forecast_df


def build_reference_intraday_profile(
    history_df: pd.DataFrame,
    selected_regions: list[str],
    scope: str,
    resolution: str,
    dates: list[datetime.date],
) -> pd.DataFrame:
    """Aggregate one or more reference days into an average intraday intensity profile."""
    if history_df.empty or not dates:
        return pd.DataFrame(columns=["slot", "period", "intensity"])

    filtered = history_df[history_df["SETTLEMENTDATE"].dt.date.isin(dates)].copy()
    if selected_regions:
        filtered = filtered[filtered["Region"].isin(selected_regions)]
    if filtered.empty:
        return pd.DataFrame(columns=["slot", "period", "intensity"])

    emission_col = "tco2e_scope1" if scope == "Scope 1 only" else "tco2e_total"
    profile = (
        filtered.groupby(filtered["SETTLEMENTDATE"].dt.floor(resolution))
        .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
        .reset_index()
        .rename(columns={"SETTLEMENTDATE": "period"})
    )
    profile["slot"] = (
        profile["period"].dt.hour * 60 + profile["period"].dt.minute
    ) // (5 if resolution == "5min" else 60)
    profile["intensity"] = (profile["tco2e"] / profile["mwh"]).where(profile["mwh"] > 0)
    profile = (
        profile.groupby("slot")
        .agg(intensity=("intensity", "mean"))
        .reset_index()
    )
    base_day = pd.Timestamp(get_aemo_date())
    slot_minutes = 5 if resolution == "5min" else 60
    profile["period"] = base_day + pd.to_timedelta(profile["slot"] * slot_minutes, unit="m")
    return profile


def interpolate_yearly_series(years: list[int], anchors: dict[int, float]) -> list[float]:
    """Linearly interpolate a yearly series from sparse anchor points."""
    sorted_points = sorted(anchors.items())
    values = []
    for year in years:
        if year <= sorted_points[0][0]:
            values.append(sorted_points[0][1])
            continue
        if year >= sorted_points[-1][0]:
            values.append(sorted_points[-1][1])
            continue

        for (y0, v0), (y1, v1) in zip(sorted_points[:-1], sorted_points[1:]):
            if y0 <= year <= y1:
                ratio = (year - y0) / (y1 - y0)
                values.append(v0 + ratio * (v1 - v0))
                break
    return values


def build_placeholder_outlook_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create placeholder medium- and long-term outlook frames for UI scaffolding."""
    years = list(range(2025, 2051))

    scenario_generation_anchors = {
        "Official transition": {
            "Coal": {2025: 135, 2030: 88, 2035: 48, 2040: 20, 2050: 4},
            "Gas": {2025: 42, 2030: 38, 2035: 36, 2040: 32, 2050: 26},
            "Wind": {2025: 32, 2030: 58, 2035: 84, 2040: 108, 2050: 132},
            "Solar": {2025: 28, 2030: 66, 2035: 101, 2040: 136, 2050: 170},
            "Hydro + storage": {2025: 18, 2030: 24, 2035: 31, 2040: 37, 2050: 46},
        },
        "Delayed buildout": {
            "Coal": {2025: 135, 2030: 105, 2035: 79, 2040: 52, 2050: 18},
            "Gas": {2025: 42, 2030: 44, 2035: 45, 2040: 43, 2050: 39},
            "Wind": {2025: 32, 2030: 46, 2035: 61, 2040: 76, 2050: 95},
            "Solar": {2025: 28, 2030: 51, 2035: 72, 2040: 94, 2050: 121},
            "Hydro + storage": {2025: 18, 2030: 21, 2035: 25, 2040: 29, 2050: 34},
        },
        "Accelerated clean grid": {
            "Coal": {2025: 135, 2030: 72, 2035: 26, 2040: 6, 2050: 0},
            "Gas": {2025: 42, 2030: 34, 2035: 28, 2040: 22, 2050: 15},
            "Wind": {2025: 32, 2030: 69, 2035: 104, 2040: 134, 2050: 165},
            "Solar": {2025: 28, 2030: 82, 2035: 125, 2040: 164, 2050: 202},
            "Hydro + storage": {2025: 18, 2030: 28, 2035: 39, 2040: 50, 2050: 61},
        },
    }

    generation_rows = []
    for scenario, tech_map in scenario_generation_anchors.items():
        for technology, anchors in tech_map.items():
            for year, twh in zip(years, interpolate_yearly_series(years, anchors)):
                generation_rows.append(
                    {"year": year, "scenario": scenario, "technology": technology, "generation_twh": twh}
                )

    price_anchors = {
        "Official transition": {2025: 188, 2030: 171, 2035: 156, 2040: 148, 2050: 141},
        "Delayed buildout": {2025: 188, 2030: 196, 2035: 207, 2040: 214, 2050: 221},
        "Accelerated clean grid": {2025: 188, 2030: 165, 2035: 145, 2040: 136, 2050: 128},
    }
    price_rows = []
    for scenario, anchors in price_anchors.items():
        for year, aud_mwh in zip(years, interpolate_yearly_series(years, anchors)):
            price_rows.append({"year": year, "scenario": scenario, "aud_mwh": aud_mwh})

    return pd.DataFrame(generation_rows), pd.DataFrame(price_rows)


try:
    APP_DIR = Path(__file__).resolve().parent
    if GCS_BUCKET:
        DATA_DIR = APP_DIR / "data"
    else:
        if ensure_required_data:
            DATA_DIR = ensure_required_data(APP_DIR)
        else:
            DATA_DIR = APP_DIR / "data"
except FileNotFoundError as e:
    st.error("### Data files not found")
    st.markdown(str(e))
    st.stop()



# ─────────────────────────────────────────────────────────────
# Sidebar configuration
# ─────────────────────────────────────────────────────────────
st.sidebar.markdown("<h2 class='sidebar-header'>Configuration</h2>", unsafe_allow_html=True)
# ── Derive date range and regions, preferring Gold metadata ──
dashboard_metadata = load_dashboard_metadata(str(DATA_DIR))
date_min = dashboard_metadata["date_min"]
latest_available_date = get_latest_available_data_date(str(DATA_DIR))
date_max = min(get_aemo_date(), latest_available_date)
regions = dashboard_metadata["regions"]
has_monthly_archives = dashboard_metadata["has_monthly_archives"]


# ─────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────
TECH_COLORS = {
    "Coal":            "#8B3A3A",
    "Brown coal":      "#B87333",
    "Gas Turbine":     "#D4A855",
    "Other":           "#9CA3AF",
    "Wind":            "#4A8C6F",
    "Solar PV":        "#C9A935",
    "Hydro":           "#4A7FB5",
    "Battery Storage": "#7B5EA7",
    "Unknown":         "#6B7280",
}

RESOLUTIONS = {
    "5 minutes":  "5min",
    "1 hour": "1h",
}
ILLUSTRATIVE_CARBON_RATE = 75

plotly_config = {
    "displayModeBar": False,
    "displaylogo": False,
    "scrollZoom": False,
    "doubleClick": False,
    "responsive": True,
}

PLOT_BG = "#ffffff"
PLOT_TEXT = "#000000"
PLOT_MUTED = "#000000"
PLOT_GRID = "#e1e1e1"
PLOT_BORDER = "#cfdbd7"


# ─────────────────────────────────────────────────────────────
# State & filtering
# ─────────────────────────────────────────────────────────────
if "selected_date" not in st.session_state:
    st.session_state.selected_date = date_max
elif st.session_state.selected_date != date_max:
    # Keep the hidden selected day aligned with the newest available data.
    st.session_state.selected_date = date_max
if "scope_choice" not in st.session_state:
    st.session_state.scope_choice = "Scope 1 only"
if "sel_regions" not in st.session_state:
    st.session_state.sel_regions = regions.copy()
_, _, previous_fy_label_default = get_previous_financial_year(get_aemo_date())
if "trend_range" not in st.session_state:
    st.session_state.trend_range = previous_fy_label_default
_, _, current_fy_ytd_label_default = get_current_financial_year_ytd(get_aemo_date())
if "top_chart_range" not in st.session_state:
    st.session_state.top_chart_range = "Today"

selected_date = st.session_state.selected_date
is_live_today = selected_date == get_aemo_date() and latest_available_date == get_aemo_date()
if selected_date < date_min:
    selected_date = date_min
    st.session_state.selected_date = date_min
if selected_date > date_max:
    selected_date = date_max
    st.session_state.selected_date = date_max
resolution_label = "5 minutes"
resolution = RESOLUTIONS[resolution_label]
scope_choice = st.session_state.scope_choice
sel_regions = [region for region in st.session_state.sel_regions if region in regions]
st.session_state.sel_regions = sel_regions
_, _, current_fy_ytd_label = get_current_financial_year_ytd(selected_date)
_, _, previous_fy_label = get_previous_financial_year(selected_date)
top_chart_range = st.session_state.top_chart_range

# Load data for the selected date and filter to chosen regions
_day_df = load_scada_for_date(str(DATA_DIR), selected_date)
dff = _day_df[_day_df["Region"].isin(sel_regions)].copy() if sel_regions else _day_df.iloc[0:0].copy()

# Combine daily metrics used for long-range trends
daily_history = load_historical_daily_metrics(str(DATA_DIR))
daily_history = combine_daily_metrics_for_regions(daily_history, sel_regions, scope_choice)

with st.sidebar:
    st.markdown("---")
    st.markdown("### Data Status")
    mode = "🌩️ Cloud Bucket (GCS)" if GCS_BUCKET else "📁 Local Filesystem"
    st.info(f"Source: **{mode}**")
    if not daily_history.empty:
        # Get the max date from the actual interval data for max accuracy
        max_dt = daily_history["date"].max()
        st.success(f"Latest Data: **{max_dt}**")
        if pd.Timestamp(max_dt).date() < get_aemo_date():
            st.warning(
                f"Latest available data is for **{max_dt}**. "
                "The current AEMO day has not landed yet, so the dashboard is showing the newest available day."
            )
    st.markdown("---")

# ── Auto-refresh every 5 minutes when viewing today's live data ──
import time as _time
_now = _time.monotonic()
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = _now
elif is_live_today and (_now - st.session_state.last_refresh) >= 300:
    st.session_state.last_refresh = _now
    st.rerun()

emission_col = "tco2e_scope1" if scope_choice == "Scope 1 only" else "tco2e_total"


# ─────────────────────────────────────────────────────────────
# Core calculations
# ─────────────────────────────────────────────────────────────
business_hours_mask = (
    (dff["SETTLEMENTDATE"].dt.hour >= 9) &
    (dff["SETTLEMENTDATE"].dt.hour < 17)
)
after_hours_mask = ~business_hours_mask

business_mwh = dff.loc[business_hours_mask, "mwh"].sum()
business_tco2e = dff.loc[business_hours_mask, emission_col].sum()
business_avg_intensity = business_tco2e / business_mwh if business_mwh > 0 else 0

after_hours_mwh = dff.loc[after_hours_mask, "mwh"].sum()
after_hours_tco2e = dff.loc[after_hours_mask, emission_col].sum()
after_hours_avg_intensity = after_hours_tco2e / after_hours_mwh if after_hours_mwh > 0 else 0

# Five-minute benchmark for clean window calculation
five_min_benchmark = (
    dff.groupby(dff["SETTLEMENTDATE"].dt.floor("5min"))
    .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
    .sort_index()
)
five_min_benchmark["intensity"] = (
    five_min_benchmark["tco2e"] / five_min_benchmark["mwh"]
).where(five_min_benchmark["mwh"] > 0)

# Rolling 4-hour cleanest window
clean_window_start = None
clean_window_end = None
if len(five_min_benchmark) >= 48:
    rolling_window = five_min_benchmark[["mwh", "tco2e"]].rolling(window=48, min_periods=48).sum()
    rolling_window["intensity"] = rolling_window["tco2e"] / rolling_window["mwh"]
    if rolling_window["intensity"].notna().any():
        clean_window_end = rolling_window["intensity"].idxmin()
        clean_window_start = clean_window_end - pd.Timedelta(minutes=5 * 47)


def to_decimal_hour(ts):
    return ts.hour + ts.minute / 60


def decimal_hour_to_label(hour_value):
    total_minutes = int(round(hour_value * 60))
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f"{hours:02d}:{minutes:02d}"


def time_window_mask(start_hour, end_hour):
    start_minutes = int(round(start_hour * 60))
    end_minutes = int(round(end_hour * 60))
    minutes_of_day = dff["SETTLEMENTDATE"].dt.hour * 60 + dff["SETTLEMENTDATE"].dt.minute
    return (minutes_of_day >= start_minutes) & (minutes_of_day < end_minutes)


clean_window_start_hour = to_decimal_hour(clean_window_start) if clean_window_start is not None else 12
clean_window_end_hour = (
    to_decimal_hour(clean_window_end) + (5 / 60) if clean_window_end is not None else 16
)
empty_mask = pd.Series(False, index=dff.index, dtype=bool)

business_windows = [
    {"window": "Night shift", "display_label": "Night shift", "start": 0, "end": 6,
     "mask": time_window_mask(0, 6)},
    {"window": "Standard hours", "display_label": "Standard hours", "start": 9, "end": 17,
     "mask": time_window_mask(9, 17)},
    {"window": "Food operations", "display_label": "Food operations", "start": 6, "end": 15,
     "mask": time_window_mask(6, 15)},
    {"window": "Small site operations", "display_label": "Small site operations", "start": 8, "end": 16,
     "mask": time_window_mask(8, 16)},
    {"window": "Cheapest 4 hours", "display_label": "Cheapest 4 hours",
     "start": clean_window_start_hour, "end": clean_window_end_hour,
     "mask": (
         (dff["SETTLEMENTDATE"] >= clean_window_start) &
         (dff["SETTLEMENTDATE"] <= clean_window_end)
     ) if clean_window_start is not None and clean_window_end is not None else empty_mask},
    {"window": "Late hours", "display_label": "Late hours", "start": 17, "end": 22,
     "mask": time_window_mask(17, 22)},
]

business_window_df = pd.DataFrame(business_windows)
business_window_df["duration"] = business_window_df["end"] - business_window_df["start"]
business_window_df["start_label"] = business_window_df["start"].map(decimal_hour_to_label)
business_window_df["end_label"] = business_window_df["end"].map(decimal_hour_to_label)
business_window_df["mwh"] = [dff.loc[mask, "mwh"].sum() for mask in business_window_df["mask"]]
business_window_df["emissions"] = [dff.loc[mask, emission_col].sum() for mask in business_window_df["mask"]]
business_window_df["avg_intensity"] = (
    business_window_df["emissions"] / business_window_df["mwh"]
).where(business_window_df["mwh"] > 0, 0)
window_metrics = business_window_df.set_index("window").to_dict("index")

# KPI calculations
total_mwh     = dff["mwh"].sum()
total_tco2e   = dff[emission_col].sum()
avg_intensity = total_tco2e / total_mwh if total_mwh > 0 else 0
renewable_mwh = dff[dff["Technology Type"].isin(ZERO_EMISSION)]["mwh"].sum()
re_share      = 100 * renewable_mwh / total_mwh if total_mwh > 0 else 0

interval_agg = (
    dff.groupby(dff["SETTLEMENTDATE"].dt.floor(resolution))
    .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
    .assign(intensity=lambda x: x["tco2e"] / x["mwh"])
)
period_low  = interval_agg["intensity"].min() if not interval_agg.empty else 0
period_high = interval_agg["intensity"].max() if not interval_agg.empty else 0

# Hourly aggregation for duck curve
hourly_agg = (
    dff.groupby(dff["SETTLEMENTDATE"].dt.hour)
    .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
    .reset_index()
)
hourly_agg.columns = ["hour", "mwh", "tco2e"]
hourly_agg["intensity"] = hourly_agg["tco2e"] / hourly_agg["mwh"]


# ─────────────────────────────────────────────────────────────
# ███  DASHBOARD LAYOUT  ███
# ─────────────────────────────────────────────────────────────

# ── Header ──────────────────────────────────────────────────
st.markdown("""
<div class="header-band">
  <div class="page-header">AEMO Grid Emissions Outlook</div>
  <div class='page-deck'>Near-real-time NEM emissions intensity, historical context, and rest-of-day outlook from AEMO dispatch data.</div>
</div>
""", unsafe_allow_html=True)

# ── D.2  Hero Row ───────────────────────────────────────────
clean_window_label = (
    f"{clean_window_start.strftime('%H:%M')}, {(clean_window_end + pd.Timedelta(minutes=5)).strftime('%H:%M')}"
    if clean_window_start is not None and clean_window_end is not None
    else "Calculating..."
)

st.markdown(f"""
<div class="hero-grid">
  <div class="hero-card hero-card--accent">
    <div class="hero-label">Grid Intensity</div>
    <div class="hero-value">{avg_intensity:.3f}</div>
    <div class="hero-sub">t CO&#8322;-e / MWh</div>
  </div>
  <div class="hero-card">
    <div class="hero-label">Zero-Emission Share</div>
    <div class="hero-value">{re_share:.1f}%</div>
    <div class="hero-sub">of total generation</div>
  </div>
  <div class="hero-card">
    <div class="hero-label">Total Generation</div>
    <div class="hero-value">{total_mwh/1e3:.1f}k</div>
    <div class="hero-sub">MWh</div>
  </div>
  <div class="hero-card">
    <div class="hero-label">Cleanest Window</div>
    <div class="hero-value" style="font-size: var(--text-lg);">{clean_window_label}</div>
    <div class="hero-sub">lowest 4-hour avg intensity</div>
  </div>
</div>
""", unsafe_allow_html=True)


st.markdown("<h2 class='section-heading'>Today's dispatch shows when the grid gets cleaner</h2>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Stacked generation and total emissions reveal how coal, gas, wind, hydro, and solar shape the selected day</p>",
            unsafe_allow_html=True)

# (Radio control moved down to grouped controls row for better hierarchy)
top_chart_options = ["Today", "Past week", current_fy_ytd_label]
if top_chart_range not in top_chart_options:
    top_chart_range = "Today"

if top_chart_range == "Today":
    combo_source_df = dff.copy()
    combo_period_label = resolution
    combo_tickvals = [pd.Timestamp(selected_date), pd.Timestamp(selected_date) + pd.Timedelta(hours=12), pd.Timestamp(selected_date) + pd.Timedelta(hours=24)]
    combo_ticktext = ["00:00", "12:00", "24:00"]
    combo_range = [pd.Timestamp(selected_date), pd.Timestamp(selected_date) + pd.Timedelta(hours=24)]
    chart_subtitle = "Stacked generation and total emissions reveal how coal, gas, wind, hydro, and solar shape the selected day"
elif top_chart_range == "Past week":
    past_week_start = selected_date - datetime.timedelta(days=6)
    combo_source_df = load_full_history(str(DATA_DIR))
    combo_source_df = combo_source_df[
        combo_source_df["Region"].isin(sel_regions)
    ].copy() if sel_regions else combo_source_df.iloc[0:0].copy()
    combo_source_df = combo_source_df[
        combo_source_df["SETTLEMENTDATE"].dt.date.between(past_week_start, selected_date)
    ].copy()
    combo_period_label = "D"
    combo_tickvals = pd.to_datetime(sorted(combo_source_df["SETTLEMENTDATE"].dt.normalize().unique())) if not combo_source_df.empty else []
    combo_ticktext = [pd.Timestamp(ts).strftime("%d %b") for ts in combo_tickvals]
    combo_range = None
    chart_subtitle = "Daily generation mix and total emissions show how the last seven days have shifted across the selected regions"
else:
    fy_start, fy_end, _ = get_current_financial_year_ytd(selected_date)
    combo_source_df = load_full_history(str(DATA_DIR))
    combo_source_df = combo_source_df[
        combo_source_df["Region"].isin(sel_regions)
    ].copy() if sel_regions else combo_source_df.iloc[0:0].copy()
    combo_source_df = combo_source_df[
        combo_source_df["SETTLEMENTDATE"].dt.date.between(fy_start, fy_end)
    ].copy()
    combo_period_label = "MS"
    combo_tickvals = pd.to_datetime(sorted(combo_source_df["SETTLEMENTDATE"].dt.to_period("M").dt.to_timestamp().unique())) if not combo_source_df.empty else []
    combo_ticktext = [pd.Timestamp(ts).strftime("%b %y") for ts in combo_tickvals]
    combo_range = None
    chart_subtitle = "Monthly generation mix and total emissions place the current financial year in strategic context"

st.markdown(f"<p class='section-sub'>{chart_subtitle}</p>", unsafe_allow_html=True)

if top_chart_range == "Today":
    combo_source_df["period"] = combo_source_df["SETTLEMENTDATE"].dt.floor(combo_period_label)
elif top_chart_range == "Past week":
    combo_source_df["period"] = combo_source_df["SETTLEMENTDATE"].dt.normalize()
else:
    combo_source_df["period"] = combo_source_df["SETTLEMENTDATE"].dt.to_period("M").dt.to_timestamp()
agg = (
    combo_source_df.groupby("period")
    .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
    .reset_index()
)
mix = combo_source_df.groupby(["period", "Technology Type"]).agg(mwh=("mwh", "sum")).reset_index()
tech_order = [t for t in TECH_COLORS if t in mix["Technology Type"].unique()]

if top_chart_range == "Today" and not interval_agg.empty and period_low > 0:
    ratio = period_high / period_low
    best_t = interval_agg["intensity"].idxmin()
    worst_t = interval_agg["intensity"].idxmax()
    chart_title = f"Grid ran {ratio:.1f}x cleaner at {best_t.strftime('%H:%M')} than at {worst_t.strftime('%H:%M')} today"
elif top_chart_range == "Past week" and not agg.empty:
    chart_title = "The past week shows how emissions moved with the generation mix"
elif top_chart_range == current_fy_ytd_label and not agg.empty:
    chart_title = f"{current_fy_ytd_label} shows how the financial year is building so far"
else:
    chart_title = "Generation mix and total emissions"

combo_fig = make_subplots(specs=[[{"secondary_y": True}]])
for tech in tech_order:
    subset = mix[mix["Technology Type"] == tech]
    combo_fig.add_trace(go.Bar(
        x=subset["period"],
        y=subset["mwh"],
        name=tech,
        marker=dict(color=TECH_COLORS.get(tech, "#555"), line=dict(color="#f4f1ea", width=0.35)),
        hovertemplate=f"{tech}<br>%{{x|%H:%M}}<br><b>%{{y:,.0f}}</b> MWh<extra></extra>",
    ), secondary_y=False)

combo_fig.add_trace(go.Scatter(
    x=agg["period"],
    y=agg["tco2e"],
    name="Emissions (t CO\u2082-e)",
    mode="lines",
    line=dict(color="#000000", width=3),
    hovertemplate="%{x|%H:%M}<br><b>%{y:,.0f}</b> t CO\u2082-e<extra></extra>",
), secondary_y=True)

if top_chart_range == "Today" and clean_window_start is not None and clean_window_end is not None:
    combo_fig.add_vrect(
        x0=clean_window_start,
        x1=clean_window_end + pd.Timedelta(minutes=5),
        fillcolor="#e9f2ef",
        opacity=0.55,
        layer="below",
        line_width=0,
        annotation_text="Cleanest 4 hours",
        annotation_position="top left",
        annotation_font=dict(color="#0b7f94", family="IBM Plex Sans, sans-serif", size=11),
    )

combo_fig.update_layout(
    barmode="stack",
    bargap=0,
    bargroupgap=0,
    xaxis=dict(
        showgrid=False,
        color=PLOT_MUTED,
        tickmode="array",
        tickvals=combo_tickvals,
        ticktext=combo_ticktext,
        range=combo_range,
        tickangle=0,
        tickfont=dict(color=PLOT_MUTED, size=13),
    ),
    plot_bgcolor=PLOT_BG,
    paper_bgcolor=PLOT_BG,
    font=dict(color=PLOT_TEXT, family="IBM Plex Sans, sans-serif", size=13),
    legend=dict(
        bgcolor="rgba(255,255,255,0.82)",
        bordercolor=PLOT_BORDER,
        orientation="h",
        yanchor="top",
        y=-0.08,
        xanchor="center",
        x=0.5,
        font=dict(color=PLOT_TEXT, size=13),
    ),
    margin=dict(l=0, r=0, t=8, b=42),
    hovermode="x unified",
    height=520,
)
combo_fig.update_yaxes(
    title_text="",
    showgrid=True,
    gridcolor=PLOT_GRID,
    zeroline=False,
    color=PLOT_MUTED,
    tickfont=dict(color=PLOT_MUTED, size=13),
    automargin=True,
    secondary_y=False,
)
combo_fig.update_yaxes(
    title_text="",
    showgrid=False,
    zeroline=False,
    range=[0, max(100, float(agg["tco2e"].max()) * 1.15 if not agg.empty else 100)],
    color=PLOT_MUTED,
    tickfont=dict(color=PLOT_MUTED, size=13),
    automargin=True,
    secondary_y=True,
)
combo_fig.update_xaxes(fixedrange=True)
combo_fig.update_yaxes(fixedrange=True)

st.markdown(f"<div class='chart-title' style='margin-bottom: 0.5rem;'>{chart_title}</div>", unsafe_allow_html=True)

# Grouped controls on one horizontal row
c1, c2, c3 = st.columns([1.0, 1.2, 2.5], gap="small")
with c1:
    st.radio("Range", top_chart_options, key="top_chart_range")
with c2:
    st.radio("Emissions scope", ["Scope 1 only", "Scope 1 + 3 (combined)"],
             key="scope_choice",
             help="Scope 1 = direct combustion. Scope 3 = upstream fuel extraction (coal only in NGA 2025).")
with c3:
    st.multiselect("Regions", regions, key="sel_regions")

st.markdown(
    "<div class='chart-axis-notes'><span>Left, MWh</span><span>Right, t CO&#8322;-e</span></div>",
    unsafe_allow_html=True,
)
st.plotly_chart(combo_fig, use_container_width=True, config=plotly_config, theme=None)


# ── Controls ────────────────────────────────────────────────
st.markdown(
    f"<div class='meta-line'>AEMO NEM  &middot;  {selected_date.strftime('%d %B %Y')}  &middot;  "
    f"{', '.join(sel_regions) if sel_regions else 'No region selected'}  &middot;  "
    f"{scope_choice}</div>",
    unsafe_allow_html=True
)

source_label = "Live today" if is_live_today else "Latest available archive"
st.caption(f"Source: {source_label}  |  Note: AEMO NEMWEB dispatch SCADA is typically published with a ~1 hour delay.")
if not has_monthly_archives:
    st.caption(
        "Historical archive windows are currently falling back to the available raw CSV history. "
        "Run migrate_to_monthly.py once to materialize monthly archive files cleanly."
    )

st.markdown("<h2 class='section-heading'>Recent history shows whether today's grid is unusual</h2>", unsafe_allow_html=True)
st.markdown(
    "<p class='section-sub'>Daily emissions intensity provides context for the current day, the recent run-rate, and the previous financial year</p>",
    unsafe_allow_html=True,
)

trend_options = [
    previous_fy_label,
    "5Y",
    "10Y",
    "MAX (28 years)",
]
stored_trend_range = st.session_state.get("trend_range", previous_fy_label)
if stored_trend_range not in trend_options:
    stored_trend_range = previous_fy_label
trend_range = st.radio(
    "Historical range",
    trend_options,
    horizontal=True,
    index=trend_options.index(stored_trend_range),
    label_visibility="collapsed",
)
st.session_state.trend_range = trend_range

trend_df, trend_range_label = resolve_trend_window(daily_history, trend_range, selected_date)
if not trend_df.empty:
    trend_df = trend_df.sort_values("date").copy()
    trend_df["rolling_7d"] = trend_df["intensity"].rolling(7, min_periods=3).mean()

    previous_day = selected_date - datetime.timedelta(days=1)
    previous_year_same_day = shift_years_safe(selected_date, 1)
    previous_day_value = daily_history.loc[pd.to_datetime(daily_history["date"]) == pd.Timestamp(previous_day), "intensity"]
    previous_year_value = daily_history.loc[pd.to_datetime(daily_history["date"]) == pd.Timestamp(previous_year_same_day), "intensity"]

    trend_fig = go.Figure()
    trend_fig.add_trace(go.Scatter(
        x=pd.to_datetime(trend_df["date"]),
        y=trend_df["intensity"],
        mode="lines",
        name="Daily intensity",
        line=dict(color="#9ED26A", width=2.5),
        hovertemplate="%{x|%d %b %Y}<br><b>%{y:.3f}</b> t CO₂-e/MWh<extra></extra>",
    ))
    trend_fig.add_trace(go.Scatter(
        x=pd.to_datetime(trend_df["date"]),
        y=trend_df["rolling_7d"],
        mode="lines",
        name="7-day average",
        line=dict(color="#7CA7FF", width=1.8, dash="dot"),
        hovertemplate="%{x|%d %b %Y}<br><b>%{y:.3f}</b> t CO₂-e/MWh<extra></extra>",
    ))

    if not previous_day_value.empty:
        trend_fig.add_hline(
            y=float(previous_day_value.iloc[0]),
            line_color="#d97b2d",
            line_dash="dot",
            annotation_text="Previous day",
            annotation_position="top left",
        )
    if not previous_year_value.empty:
        trend_fig.add_hline(
            y=float(previous_year_value.iloc[0]),
            line_color="#6d6ba8",
            line_dash="dash",
            annotation_text="Same date last year",
            annotation_position="bottom left",
        )

    trend_fig.update_layout(
        plot_bgcolor=PLOT_BG,
        paper_bgcolor=PLOT_BG,
        font=dict(color=PLOT_TEXT, family="IBM Plex Sans, sans-serif", size=13),
        margin=dict(l=0, r=0, t=8, b=8),
        height=340,
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            bgcolor="rgba(0,0,0,0)",
        ),
        xaxis=dict(showgrid=False, color=PLOT_MUTED),
        yaxis=dict(showgrid=True, gridcolor=PLOT_GRID, color=PLOT_MUTED, title_text="t CO₂-e / MWh"),
    )
    trend_fig.update_xaxes(fixedrange=True)
    trend_fig.update_yaxes(fixedrange=True)

    st.plotly_chart(trend_fig, use_container_width=True, config=plotly_config, theme=None)
    st.caption(f"Showing: {trend_range_label}")
else:
    st.info("No historical daily trend data is available for the current selection.")

if is_live_today:
    st.markdown("<h2 class='section-heading'>The remainder of today is projected from analog grid days</h2>", unsafe_allow_html=True)
    st.markdown(
        "<p class='section-sub'>Observed intensity so far is extended with recent analog days, prior-day structure, and same-date-last-year context</p>",
        unsafe_allow_html=True,
    )

    full_history = load_full_history(str(DATA_DIR))
    observed_profile, forecast_profile = build_analog_forecast(
        full_history,
        sel_regions,
        scope_choice,
        resolution,
        selected_date,
        dff,
    )

    if not observed_profile.empty and not forecast_profile.empty:
        prev_day_profile = build_reference_intraday_profile(
            full_history,
            sel_regions,
            scope_choice,
            resolution,
            [selected_date - datetime.timedelta(days=1)],
        )
        prev_week_profile = build_reference_intraday_profile(
            full_history,
            sel_regions,
            scope_choice,
            resolution,
            [selected_date - datetime.timedelta(days=offset) for offset in range(1, 8)],
        )
        same_day_last_year = shift_years_safe(selected_date, 1)
        prev_year_profile = build_reference_intraday_profile(
            full_history,
            sel_regions,
            scope_choice,
            resolution,
            [same_day_last_year],
        )

        forecast_fig = go.Figure()
        for ref_profile, name, color in [
            (prev_day_profile, "Previous day", "#d97b2d"),
            (prev_week_profile, "Previous 7-day avg", "#6a93c4"),
            (prev_year_profile, "Same date last year", "#6d6ba8"),
        ]:
            if not ref_profile.empty:
                forecast_fig.add_trace(go.Scatter(
                    x=ref_profile["period"],
                    y=ref_profile["intensity"],
                    mode="lines",
                    name=name,
                    line=dict(color=color, width=1.4, dash="dot"),
                    opacity=0.45,
                ))

        if forecast_profile["band_low"].notna().any():
            forecast_fig.add_trace(go.Scatter(
                x=forecast_profile["period"],
                y=forecast_profile["band_high"],
                mode="lines",
                line=dict(color="rgba(158,210,106,0)"),
                hoverinfo="skip",
                showlegend=False,
            ))
            forecast_fig.add_trace(go.Scatter(
                x=forecast_profile["period"],
                y=forecast_profile["band_low"],
                mode="lines",
                line=dict(color="rgba(158,210,106,0)"),
                fill="tonexty",
                fillcolor="rgba(11,127,148,0.12)",
                hoverinfo="skip",
                name="Analog range",
            ))

        forecast_fig.add_trace(go.Scatter(
            x=observed_profile["period"],
            y=observed_profile["intensity"],
            mode="lines",
            name="Observed today",
            line=dict(color="#223a42", width=2.8),
            hovertemplate="%{x|%H:%M}<br><b>%{y:.3f}</b> t CO₂-e/MWh<extra></extra>",
        ))
        forecast_fig.add_trace(go.Scatter(
            x=forecast_profile["period"],
            y=forecast_profile["forecast_intensity"],
            mode="lines",
            name="Forecast remainder",
            line=dict(color="#0b7f94", width=2.6, dash="dash"),
            hovertemplate="%{x|%H:%M}<br><b>%{y:.3f}</b> t CO₂-e/MWh<extra></extra>",
        ))
        forecast_fig.add_vline(
            x=observed_profile["period"].max(),
            line_color="#223a42",
            line_dash="dot",
            opacity=0.45,
        )

        forecast_fig.update_layout(
            plot_bgcolor=PLOT_BG,
            paper_bgcolor=PLOT_BG,
            font=dict(color=PLOT_TEXT, family="IBM Plex Sans, sans-serif", size=13),
            margin=dict(l=0, r=0, t=8, b=8),
            height=360,
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="left",
                x=0,
                bgcolor="rgba(0,0,0,0)",
            ),
            xaxis=dict(
                showgrid=False,
                color=PLOT_MUTED,
                tickmode="array",
                tickvals=pd.date_range(
                    start=pd.Timestamp(selected_date),
                    end=pd.Timestamp(selected_date) + pd.Timedelta(hours=23),
                    freq="2h",
                ),
                ticktext=[f"{h:02d}:00" for h in range(0, 24, 2)],
            ),
            yaxis=dict(showgrid=True, gridcolor=PLOT_GRID, color=PLOT_MUTED, title_text="t CO₂-e / MWh"),
        )
        forecast_fig.update_xaxes(fixedrange=True)
        forecast_fig.update_yaxes(fixedrange=True)
        st.plotly_chart(forecast_fig, use_container_width=True, config=plotly_config, theme=None)
        st.caption(
            "Forecast uses weighted analog days based on observed shape so far, weekday/weekend pattern, "
            "seasonality, same-date-last-year similarity, and a pluggable weather adapter with neutral fallback."
        )
    else:
        st.info("Not enough historical analog data is available yet to forecast the remainder of today.")


st.markdown("<h2 class='section-heading'>Long-horizon pathways belong in scenarios, not single-point forecasts</h2>", unsafe_allow_html=True)
st.markdown(
    "<p class='section-sub'>These placeholder scenario views separate operational monitoring from medium- and long-term transition pathways</p>",
    unsafe_allow_html=True,
)

st.markdown("""
<div class="info-panel section-text">
This section is intentionally <strong>placeholder scenario data</strong>. It is here to define the product shape before the long-horizon modelling is built properly in Databricks/Prefect. 
The goal is to separate <strong>live operations</strong> from <strong>strategic outlook</strong>: managers can use the historical chart for context and curiosity, while the scenario charts below show how a later pipeline could present the federal transition pathway, delayed buildout risk, and faster clean-grid delivery.
</div>
""", unsafe_allow_html=True)

generation_placeholder_df, price_placeholder_df = build_placeholder_outlook_frames()
scenario_order = ["Official transition", "Delayed buildout", "Accelerated clean grid"]
scenario_choice = st.radio(
    "Scenario outlook",
    scenario_order,
    horizontal=True,
    key="scenario_outlook_choice",
)

scenario_chart_col, price_chart_col = st.columns(2, gap="large")

with scenario_chart_col:
    scenario_generation_titles = {
        "Official transition": "Official transition retires coal steadily and lifts wind and solar",
        "Delayed buildout": "Delayed buildout keeps fossil generation in the mix for longer",
        "Accelerated clean grid": "Accelerated buildout shifts the system decisively toward clean generation",
    }
    st.markdown(f"<div class='chart-title'>{scenario_generation_titles[scenario_choice]}</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='chart-axis-notes'><span>Placeholder TWh trajectory</span><span>2025 to 2050</span></div>",
        unsafe_allow_html=True,
    )
    scenario_gen = generation_placeholder_df[generation_placeholder_df["scenario"] == scenario_choice]
    gen_fig = go.Figure()
    technology_colors = {
        "Coal": "#7f3b3b",
        "Gas": "#d97b2d",
        "Wind": "#0b7f94",
        "Solar": "#f0b24b",
        "Hydro + storage": "#5d8db9",
    }
    for technology in ["Coal", "Gas", "Wind", "Solar", "Hydro + storage"]:
        tech_df = scenario_gen[scenario_gen["technology"] == technology]
        gen_fig.add_trace(go.Scatter(
            x=tech_df["year"],
            y=tech_df["generation_twh"],
            mode="lines",
            name=technology,
            line=dict(color=technology_colors[technology], width=2.4),
            hovertemplate=f"{technology}<br>%{{x}}<br><b>%{{y:.0f}}</b> TWh<extra></extra>",
        ))
    gen_fig.update_layout(
        plot_bgcolor=PLOT_BG,
        paper_bgcolor=PLOT_BG,
        font=dict(color=PLOT_TEXT, family="IBM Plex Sans, sans-serif", size=13),
        margin=dict(l=0, r=0, t=8, b=8),
        height=340,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(showgrid=False, color=PLOT_MUTED, dtick=5),
        yaxis=dict(showgrid=True, gridcolor=PLOT_GRID, color=PLOT_MUTED, title_text="TWh"),
    )
    gen_fig.update_xaxes(fixedrange=True)
    gen_fig.update_yaxes(fixedrange=True)
    st.plotly_chart(gen_fig, use_container_width=True, config=plotly_config, theme=None)
    st.markdown(f"""
    <div class="section-text" style="margin-top: 0.6rem;">
    <strong>{scenario_choice}</strong> is shown as a simple placeholder pathway. In the eventual data-engineered version, this chart would be sourced from official planning artefacts and scenario assumptions, then reconciled into expected generation by technology. 
    For now it is only defining the information architecture: a manager should be able to see whether coal exits quickly, whether gas remains a balancing technology, and how much wind, solar, hydro, and storage are expected to carry the system by 2050.
    </div>
    """, unsafe_allow_html=True)

with price_chart_col:
    scenario_price_titles = {
        "Official transition": "Official transition keeps delivered-cost pressure elevated before easing",
        "Delayed buildout": "Delayed buildout leaves businesses exposed to a higher cost path",
        "Accelerated clean grid": "Faster clean-grid delivery lowers the long-run business cost proxy",
    }
    st.markdown(f"<div class='chart-title'>{scenario_price_titles[scenario_choice]}</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='chart-axis-notes'><span>Placeholder delivered-cost proxy</span><span>A$/MWh</span></div>",
        unsafe_allow_html=True,
    )
    price_fig = go.Figure()
    scenario_colors = {
        "Official transition": "#0b7f94",
        "Delayed buildout": "#d97b2d",
        "Accelerated clean grid": "#6aa84f",
    }
    for scenario_name in scenario_order:
        scenario_df = price_placeholder_df[price_placeholder_df["scenario"] == scenario_name]
        price_fig.add_trace(go.Scatter(
            x=scenario_df["year"],
            y=scenario_df["aud_mwh"],
            mode="lines",
            name=scenario_name,
            line=dict(color=scenario_colors[scenario_name], width=2.6),
            hovertemplate=f"{scenario_name}<br>%{{x}}<br><b>A$%{{y:.0f}}</b>/MWh<extra></extra>",
        ))
    price_fig.update_layout(
        plot_bgcolor=PLOT_BG,
        paper_bgcolor=PLOT_BG,
        font=dict(color=PLOT_TEXT, family="IBM Plex Sans, sans-serif", size=13),
        margin=dict(l=0, r=0, t=8, b=8),
        height=340,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(showgrid=False, color=PLOT_MUTED, dtick=5),
        yaxis=dict(showgrid=True, gridcolor=PLOT_GRID, color=PLOT_MUTED, title_text="A$/MWh"),
    )
    price_fig.update_xaxes(fixedrange=True)
    price_fig.update_yaxes(fixedrange=True)
    st.plotly_chart(price_fig, use_container_width=True, config=plotly_config, theme=None)
    st.markdown("""
    <div class="section-text" style="margin-top: 0.6rem;">
    This second chart is not a tariff quote. It is a placeholder for a later <strong>business cost benchmark</strong> layer that could combine wholesale expectations, network pressure, and transition-delivery assumptions into an illustrative range. 
    In the eventual version, each line should be source-linked to public planning and industry commentary, so the user can distinguish official pathway assumptions from delayed-buildout risk or faster clean-grid delivery.
    </div>
    """, unsafe_allow_html=True)

st.markdown("""
<div class="chart-insight">
  <strong>Placeholder design note.</strong> These long-horizon visuals are intentionally not presented as hard predictions. 
  They are scaffolding for a later scenario engine, where assumptions from DCCEEW, AEMO, the Clean Energy Council, and other market sources can be versioned, compared, and cited directly.
</div>
""", unsafe_allow_html=True)


# ── D.3  Insight Callout ────────────────────────────────────
if clean_window_start is not None and avg_intensity > 0:
    clean_mask = (
        (five_min_benchmark.index >= clean_window_start) &
        (five_min_benchmark.index <= clean_window_end)
    )
    clean_mwh = five_min_benchmark.loc[clean_mask, "mwh"].sum()
    clean_tco2e = five_min_benchmark.loc[clean_mask, "tco2e"].sum()
    clean_intensity = clean_tco2e / clean_mwh if clean_mwh > 0 else avg_intensity
    pct_cleaner = (1 - clean_intensity / avg_intensity) * 100
    if pct_cleaner > 0:
        insight_text = (
            f"The cleanest 4-hour window today is <strong>{pct_cleaner:.0f}% less carbon-intensive</strong> "
            f"than the daily average, shift flexible load to {clean_window_label} to reduce Scope 2."
        )
    else:
        insight_text = (
            f"Grid intensity is relatively flat today. "
            f"Cleanest window ({clean_window_label}) is close to the daily average."
        )
else:
    insight_text = "Insufficient data to calculate the cleanest window for this day."

st.markdown(f'<div class="insight-callout">{insight_text}</div>', unsafe_allow_html=True)


charts_left, charts_right = st.columns(2, gap="large")

with charts_left:
    st.markdown("<h2 class='section-heading'>Regional fuel mix shows why the NEM does not clean uniformly</h2>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>The same time of day can look very different across regions because each grid relies on a different mix of coal, gas, hydro, wind, and solar</p>",
                unsafe_allow_html=True)

    fuel_region = st.radio("Region", ["All"] + [r.replace("1", "") for r in regions],
                           horizontal=True, key="fuel_region_tab", label_visibility="collapsed")

    if fuel_region == "All":
        fuel_df = dff.copy()
    else:
        fuel_df = dff[dff["Region"] == f"{fuel_region}1"].copy()

    fuel_mix = (
        fuel_df.groupby("Technology Type")
        .agg(total_mwh=("mwh", "sum"))
        .reset_index()
        .sort_values("total_mwh", ascending=True)
    )
    fuel_mix["pct"] = 100 * fuel_mix["total_mwh"] / fuel_mix["total_mwh"].sum() if fuel_mix["total_mwh"].sum() > 0 else 0

    fuel_fig = go.Figure(go.Bar(
        y=fuel_mix["Technology Type"],
        x=fuel_mix["total_mwh"],
        orientation="h",
        marker=dict(
            color=[TECH_COLORS.get(t, "#6B7280") for t in fuel_mix["Technology Type"]],
            line=dict(color="#f4f1ea", width=0.5),
        ),
        customdata=fuel_mix[["pct"]],
        hovertemplate="<b>%{y}</b><br>%{x:,.0f} MWh (%{customdata[0]:.1f}%)<extra></extra>",
    ))
    fuel_fig.update_layout(
        plot_bgcolor=PLOT_BG, paper_bgcolor=PLOT_BG,
        font=dict(color=PLOT_TEXT, family="IBM Plex Sans, sans-serif"),
        margin=dict(l=0, r=20, t=8, b=8),
        height=320,
        showlegend=False,
        xaxis=dict(showgrid=True, gridcolor=PLOT_GRID, title_text="MWh", color=PLOT_MUTED),
        yaxis=dict(showgrid=False, color=PLOT_TEXT, automargin=True),
    )
    fuel_fig.update_xaxes(fixedrange=True)
    fuel_fig.update_yaxes(fixedrange=True)
    st.plotly_chart(fuel_fig, use_container_width=True, config=plotly_config, theme=None)

with charts_right:
    st.markdown("<h2 class='section-heading'>Midday usually runs cleaner; the evening ramp lifts intensity</h2>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>The duck-curve view shows when the grid is cleaner, when thermal generation returns, and how fast intensity rises into the evening</p>",
                unsafe_allow_html=True)

    if not hourly_agg.empty and hourly_agg["intensity"].notna().any():
        median_i = hourly_agg["intensity"].median()
        p75_i = hourly_agg["intensity"].quantile(0.75)

        # Use absolute thresholds as fallback for extreme days
        green_threshold = min(median_i, 0.3) if median_i < 0.15 else median_i
        red_threshold = max(p75_i, 0.6) if p75_i > 0.8 else p75_i

        def duck_color(val):
            if pd.isna(val):
                return "#9CA3AF"
            if val <= green_threshold:
                return "#4A8C6F"
            elif val >= red_threshold:
                return "#8B3A3A"
            else:
                return "#D4A855"

        duck_colors = hourly_agg["intensity"].apply(duck_color).tolist()

        duck_fig = go.Figure(go.Bar(
            x=hourly_agg["hour"],
            y=hourly_agg["intensity"],
            marker=dict(color=duck_colors, line=dict(color="#f4f1ea", width=0.5)),
            hovertemplate="Hour %{x}:00<br><b>%{y:.3f}</b> t CO₂-e/MWh<extra></extra>",
        ))
        duck_fig.update_layout(
            plot_bgcolor=PLOT_BG, paper_bgcolor=PLOT_BG,
            font=dict(color=PLOT_TEXT, family="IBM Plex Sans, sans-serif"),
            margin=dict(l=10, r=10, t=8, b=8),
            height=320,
            showlegend=False,
            xaxis=dict(
                showgrid=False, color=PLOT_MUTED,
                tickmode="array",
                tickvals=[0, 12, 24],
                ticktext=["00:00", "12:00", "24:00"],
                range=[0, 24],
                title_text="Hour of Day",
            ),
            yaxis=dict(
                showgrid=True, gridcolor=PLOT_GRID, color=PLOT_MUTED,
                title_text="t CO₂-e / MWh", rangemode="tozero",
            ),
        )
        duck_fig.update_xaxes(fixedrange=True)
        duck_fig.update_yaxes(fixedrange=True)

        if len(hourly_agg) > 12:
            solar_hours = hourly_agg[(hourly_agg["hour"] >= 10) & (hourly_agg["hour"] <= 14)]
            evening_hours = hourly_agg[(hourly_agg["hour"] >= 17) & (hourly_agg["hour"] <= 20)]
            if not solar_hours.empty and not evening_hours.empty:
                solar_min = solar_hours.loc[solar_hours["intensity"].idxmin()]
                evening_max = evening_hours.loc[evening_hours["intensity"].idxmax()]
                duck_fig.add_annotation(
                    x=solar_min["hour"], y=solar_min["intensity"],
                    text="Solar peak", showarrow=True, arrowhead=0,
                    font=dict(size=11, color="#4A8C6F"), ax=0, ay=-30,
                )
                duck_fig.add_annotation(
                    x=evening_max["hour"], y=evening_max["intensity"],
                    text="Evening ramp", showarrow=True, arrowhead=0,
                    font=dict(size=11, color="#8B3A3A"), ax=0, ay=-30,
                )

        st.plotly_chart(duck_fig, use_container_width=True, config=plotly_config, theme=None)

        st.markdown("""
        <div style="display: flex; gap: 1.5rem; justify-content: center; margin-bottom: 1rem;">
          <span style="font-size: var(--text-base); color: #b7b1a6;">
            <span style="display:inline-block;width:12px;height:12px;background:#4A8C6F;border-radius:2px;margin-right:4px;vertical-align:middle;"></span> Clean
          </span>
          <span style="font-size: var(--text-base); color: #b7b1a6;">
            <span style="display:inline-block;width:12px;height:12px;background:#D4A855;border-radius:2px;margin-right:4px;vertical-align:middle;"></span> Moderate
          </span>
          <span style="font-size: var(--text-base); color: #b7b1a6;">
            <span style="display:inline-block;width:12px;height:12px;background:#8B3A3A;border-radius:2px;margin-right:4px;vertical-align:middle;"></span> Dirty
          </span>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("No hourly data available for this date/region selection.")


# ── D.6  Timing Cards ──────────────────────────────────────
st.markdown("<h2 class='section-heading'>Flexible load should sit away from the evening ramp</h2>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>These windows translate the day profile into simple operational timing guidance</p>",
            unsafe_allow_html=True)

if not hourly_agg.empty and len(hourly_agg) > 6:
    morning = hourly_agg[(hourly_agg["hour"] >= 5) & (hourly_agg["hour"] <= 9)]
    midday  = hourly_agg[(hourly_agg["hour"] >= 10) & (hourly_agg["hour"] <= 15)]
    evening = hourly_agg[(hourly_agg["hour"] >= 16) & (hourly_agg["hour"] <= 21)]
    overnight = hourly_agg[(hourly_agg["hour"] >= 22) | (hourly_agg["hour"] <= 4)]

    def best_hour(subset):
        if subset.empty:
            return 0, 0.0
        row = subset.loc[subset["intensity"].idxmin()]
        return int(row["hour"]), float(row["intensity"])

    def worst_hour(subset):
        if subset.empty:
            return 0, 0.0
        row = subset.loc[subset["intensity"].idxmax()]
        return int(row["hour"]), float(row["intensity"])

    bm_hour, bm_int = best_hour(morning)
    bd_hour, bd_int = best_hour(midday)
    we_hour, we_int = worst_hour(evening)
    on_int = overnight["intensity"].mean() if not overnight.empty else 0

    st.markdown(f"""
    <div class="timing-grid">
      <div class="timing-card timing-card--green">
        <div class="timing-label">Best Morning</div>
        <div class="timing-value">{bm_hour:02d}:00</div>
        <div class="timing-hours">05:00, 09:00 range</div>
        <div class="timing-intensity">{bm_int:.3f} t CO&#8322;-e/MWh</div>
      </div>
      <div class="timing-card timing-card--green">
        <div class="timing-label">Best Midday</div>
        <div class="timing-value">{bd_hour:02d}:00</div>
        <div class="timing-hours">10:00, 15:00 range</div>
        <div class="timing-intensity">{bd_int:.3f} t CO&#8322;-e/MWh</div>
      </div>
      <div class="timing-card timing-card--red">
        <div class="timing-label">Avoid: Evening Peak</div>
        <div class="timing-value">{we_hour:02d}:00</div>
        <div class="timing-hours">16:00, 21:00 range</div>
        <div class="timing-intensity">{we_int:.3f} t CO&#8322;-e/MWh</div>
      </div>
      <div class="timing-card timing-card--neutral">
        <div class="timing-label">Overnight Avg</div>
        <div class="timing-value">22, 04</div>
        <div class="timing-hours">baseload period</div>
        <div class="timing-intensity">{on_int:.3f} t CO&#8322;-e/MWh</div>
      </div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.info("Insufficient data to show timing cards.")


# ── D.7  Sector Cards ──────────────────────────────────────
st.markdown("<h2 class='section-heading'>The same electricity use can produce very different emissions outcomes</h2>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Illustrative operating profiles show how timing changes reported emissions even when total consumption stays constant</p>",
            unsafe_allow_html=True)

clean_4h_intensity = window_metrics.get("Cheapest 4 hours", {}).get("avg_intensity", avg_intensity)

sector_profiles = [
    {"name": "Aluminium Smelter", "hours": "24/7 baseload", "mwh": 500,
     "window": "Standard hours", "note": "Potlines run continuously, limited flexibility"},
    {"name": "Food Manufacturing", "hours": "06:00, 15:00", "mwh": 20,
     "window": "Food operations", "note": "Batch cooking, CIP cleaning, pasteurisation"},
    {"name": "Data Centre", "hours": "24/7, flex batch", "mwh": 50,
     "window": "Standard hours", "note": "Batch compute and cooling can shift to clean windows"},
    {"name": "Commercial Office", "hours": "08:00, 18:00", "mwh": 5,
     "window": "Small site operations", "note": "HVAC pre-cooling, EV charging flexibility"},
]

sector_html = '<div class="sector-grid">'
for sp in sector_profiles:
    sp_intensity = window_metrics.get(sp["window"], {}).get("avg_intensity", avg_intensity)
    sp_emissions = sp["mwh"] * sp_intensity
    sp_optimized = sp["mwh"] * clean_4h_intensity
    sp_saving = sp_emissions - sp_optimized
    sp_saving_pct = (sp_saving / sp_emissions * 100) if sp_emissions > 0 else 0

    sector_html += f"""
    <div class="sector-card">
      <div class="sector-name">{sp["name"]}</div>
      <div class="sector-profile">{sp["hours"]} &middot; ~{sp["mwh"]} MWh/day<br>{sp["note"]}</div>
      <div class="sector-metric">
        <span class="sector-metric-label">Current emissions</span>
        <span class="sector-metric-value">{sp_emissions:.1f} t</span>
      </div>
      <div class="sector-metric">
        <span class="sector-metric-label">Optimized</span>
        <span class="sector-metric-value">{sp_optimized:.1f} t</span>
      </div>
      <div class="sector-metric">
        <span class="sector-metric-label">Potential saving</span>
        <span class="sector-metric-value sector-saving">{sp_saving:.1f} t ({sp_saving_pct:.0f}%)</span>
      </div>
    </div>"""
sector_html += "</div>"
st.markdown(sector_html, unsafe_allow_html=True)


# ── D.8  Scope 2 Estimator ─────────────────────────────────
st.markdown("<h2 class='section-heading'>A single operating window can materially change Scope 2</h2>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>This illustrative estimator applies the current day profile to a simple operating window and compares it with the cleanest period</p>",
            unsafe_allow_html=True)

est_c1, est_c2 = st.columns([1, 1.5])
with est_c1:
    est_mwh = st.number_input("Daily MWh", value=100.0, step=10.0, min_value=0.1, key="estimator_mwh")
with est_c2:
    est_window = st.selectbox("Operating window",
                              [w["window"] for w in business_windows],
                              key="estimator_window")

est_intensity = window_metrics.get(est_window, {}).get("avg_intensity", avg_intensity)
est_current = est_mwh * est_intensity
est_optimized = est_mwh * clean_4h_intensity
est_saving = est_current - est_optimized
est_saving_pct = (est_saving / est_current * 100) if est_current > 0 else 0
est_annual = est_saving * 365
est_annual_cost = est_annual * ILLUSTRATIVE_CARBON_RATE

st.markdown(f"""
<div class="estimator-results">
  <div class="estimator-card">
    <div class="estimator-label">Current Emissions</div>
    <div class="estimator-value">{est_current:.1f}</div>
    <div class="estimator-sub">t CO&#8322;-e / day</div>
  </div>
  <div class="estimator-card">
    <div class="estimator-label">Optimized Emissions</div>
    <div class="estimator-value">{est_optimized:.1f}</div>
    <div class="estimator-sub">t CO&#8322;-e / day (cleanest 4h)</div>
  </div>
  <div class="estimator-card estimator-card--saving">
    <div class="estimator-label">Daily Saving</div>
    <div class="estimator-value" style="color: var(--accent);">{est_saving:.1f} t ({est_saving_pct:.0f}%)</div>
    <div class="estimator-sub">by shifting to cleanest window</div>
  </div>
  <div class="estimator-card estimator-card--saving">
    <div class="estimator-label">Annual Projection</div>
    <div class="estimator-value" style="color: var(--accent);">{est_annual:,.0f} t</div>
    <div class="estimator-sub">~A${est_annual_cost:,.0f} at $75/t CO&#8322;-e</div>
  </div>
</div>
""", unsafe_allow_html=True)

st.caption(
    "Illustrative only. Uses the current page filters and a single day's grid intensity profile. "
    "Actual disclosure-grade calculations require metered interval consumption data."
)

st.markdown("""
<div class="chart-insight">
  <strong>Operational caveat.</strong> The cleanest window is not automatically the best business choice.
  Staffing, delivery cut-offs, product quality, thermal inertia, and customer demand can outweigh emissions benefits.
  This app models emissions timing, not your tariff or full compliance workflow.
</div>
""", unsafe_allow_html=True)

with st.expander("Raw interval data"):
    raw_interval_df = (
        dff.groupby(dff["SETTLEMENTDATE"].dt.floor(resolution))
        .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
        .reset_index()
        .rename(columns={
            "SETTLEMENTDATE": f"Period ({resolution_label})",
            "mwh": "MWh",
            "tco2e": "t CO\u2082-e",
        })
    )
    raw_interval_df["Intensity (t CO\u2082-e/MWh)"] = (
        raw_interval_df["t CO\u2082-e"] / raw_interval_df["MWh"]
    ).where(raw_interval_df["MWh"] > 0)
    st.dataframe(raw_interval_df.sort_values(f"Period ({resolution_label})", ascending=False),
                 use_container_width=True, hide_index=True)

with st.expander("Emissions factors reference (NGA 2025)"):
    ef_display = load_emissions_factor_reference(str(DATA_DIR))
    st.dataframe(ef_display, use_container_width=True, hide_index=True)
    st.caption(
        "Source: National Greenhouse Accounts Factors 2025, DCCEEW. "
        "Converted with kg CO\u2082-e/GJ \u00d7 3.6 GJ/MWh \u00f7 1000."
    )

st.markdown("""
<div class="section-text" style="margin-top: 1.1rem;">
  <h3 class="section-heading">How to read this dashboard</h3>
  <p>
    This dashboard is a <strong>5-minute near-real-time reference layer</strong> for NEM grid emissions intensity.
    It shows how clean or dirty the grid is by time of day and region, using AEMO dispatch data joined to emissions factors.
    It does <strong>not</strong> calculate a company's official disclosure by itself. Disclosure-grade Scope 2 reporting still requires the company's own interval consumption data.
  </p>

  <h3 class="section-heading" style="margin-top: 1.5rem;">Methodology, lineage, and sources</h3>
  <p>
    <strong>Coverage.</strong> NEM regions only: QLD, NSW, VIC, SA, and TAS. This excludes WEM, NT grids, and rooftop solar.
  </p>
  <p>
    <strong>Lineage.</strong> Bronze Parquet lands raw Dispatch SCADA, generator metadata, and emissions factors. dbt then materialises typed Silver tables and Gold analytical tables in <code>nem.duckdb</code>, which this dashboard reads first before falling back to raw CSV history.
  </p>
  <p>
    <strong>Transform.</strong> Dispatch MW is converted into interval MWh using <code>mwh = SCADAVALUE * (5 / 60)</code>, then aggregated by region, interval, and technology. Gold outputs expose both generation mix and regional emissions intensity for Scope 1 and combined Scope 1 + 3 views.
  </p>
  <p>
    <strong>Sources.</strong> AEMO Dispatch SCADA:
    <a href="https://nemweb.com.au/Reports/Current/Dispatch_SCADA/">nemweb.com.au</a>.
    AEMO Generation Information:
    <a href="https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information">aemo.com.au</a>.
    National Greenhouse Accounts Factors 2025:
    <a href="https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors">dcceew.gov.au</a>.
  </p>
</div>
""", unsafe_allow_html=True)

# ── Live data status ──────────────────────────────────────────
st.divider()
col1, col2 = st.columns([3, 1])
with col2:
    st.caption(f"🔄 Last refreshed: {format_refresh_time()}")

# ── Footer ───────────────────────────────────────────────────
st.markdown("""
<div class="page-footer">
  <div class="footer-inner">
    <div>This is a personal project by Tanjim Islam, for demonstration purposes only, and does not constitute professional advice.</div>
    <a class="linkedin-link" href="https://www.linkedin.com/in/tanjimislam/" target="_blank" rel="noopener noreferrer">
      <svg class="linkedin-icon" viewBox="0 0 24 24" aria-hidden="true">
        <path d="M19 3A2 2 0 0 1 21 5V19A2 2 0 0 1 19 21H5A2 2 0 0 1 3 19V5A2 2 0 0 1 5 3H19ZM8.34 18V9.66H5.66V18H8.34ZM7 8.54C7.86 8.54 8.54 7.85 8.54 7S7.86 5.46 7 5.46 5.46 6.14 5.46 7 6.14 8.54 7 8.54ZM18.54 18V13.43C18.54 10.98 17.23 9.43 14.9 9.43 13.78 9.43 12.96 10.05 12.66 10.63V9.66H10V18H12.68V13.88C12.68 12.79 12.88 11.73 14.22 11.73 15.54 11.73 15.56 12.98 15.56 13.95V18H18.54Z"/>
      </svg>
      Contact on LinkedIn
    </a>
  </div>
</div>
""", unsafe_allow_html=True)
