"""
app.py — Real-Time Energy Dashboard
by Tanjim Islam
======================================================
Data flow:
  dispatch_scada.csv    → 5-min SCADA generation (MW) per DUID
  duid_lookup.csv       → DUID → Technology Type, Region
  emissions_factors.csv → Technology Type → t CO₂-e/MWh (NGA Factors 2025)
"""

import datetime
import glob
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

from data_bootstrap import ensure_required_data

# ─────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Real-Time Energy Dashboard, NEM Emissions",
    layout="wide",
)

# ─────────────────────────────────────────────────────────────
# CSS Design System
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Inter:wght@400;500;600&display=swap');

  :root {
    /* Typography */
    --font-display: 'DM Sans', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
    --font-body: 'Inter', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
    --text-base: 0.82rem;
    --text-lg: 0.94rem;
    --text-xl: 1.15rem;
    --text-2xl: 1.5rem;

    /* Colors — base */
    --background: #25241f;
    --foreground: #f2efe8;
    --card: #302f2a;
    --muted: #383730;
    --muted-foreground: #b7b1a6;
    --accent: #87b61f;
    --accent-light: #e4efd2;
    --border: rgba(243, 239, 232, 0.10);
    --radius: 0.625rem;

    /* Semantic — timing */
    --bg-green: #dfeacc;
    --bg-green-border: #95b36b;
    --bg-red: #f0d8d6;
    --bg-red-border: #d18f88;
    --bg-neutral: #35342f;

    /* Warm bands */
    --header-bg: #25241f;
    --header-border: rgba(243, 239, 232, 0.12);
    --footer-bg: #25241f;
    --footer-border: rgba(243, 239, 232, 0.12);
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
    border-bottom: 1px solid var(--header-border);
    padding: 1.55rem 0 1.45rem 0;
    margin: 0 0 1.2rem 0;
    width: 100vw;
    margin-left: calc(-50vw + 50%);
    margin-right: calc(-50vw + 50%);
  }
  .header-band .page-header {
    font-family: var(--font-display);
    font-size: var(--text-2xl);
    font-weight: 700;
    color: var(--foreground);
    margin: 0;
    text-align: center;
    letter-spacing: -0.02em;
  }
  .header-band .page-deck {
    text-align: center;
    margin: 0.35rem auto 0 auto;
    padding: 0 1rem;
    max-width: 900px;
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    line-height: 1.5;
  }
  .meta-line {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-bottom: 1rem;
  }

  /* ── Hero cards ── */
  .hero-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 0.75rem;
    margin-bottom: 1.25rem;
  }
  .hero-card {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.875rem 0.945rem;
    text-align: center;
  }
  .hero-card--accent {
    background: var(--accent-light);
    border-color: var(--bg-green-border);
    color: #2e5e11;
  }
  .hero-card--accent .hero-label { color: rgba(46, 94, 17, 0.72); }
  .hero-card--accent .hero-value { color: #2e5e11; }
  .hero-card--accent .hero-sub { color: rgba(46, 94, 17, 0.72); }
  .hero-label {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-bottom: 0.25rem;
  }
  .hero-value {
    font-family: var(--font-display);
    font-size: var(--text-xl);
    font-weight: 700;
    color: var(--foreground);
    letter-spacing: -0.02em;
  }
  .hero-sub {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin-top: 0.15rem;
  }

  /* ── Insight callout ── */
  .insight-callout {
    background: var(--accent-light);
    border: 1px solid var(--bg-green-border);
    border-radius: var(--radius);
    padding: 0.7rem 1.05rem;
    margin-bottom: 1.05rem;
    font-family: var(--font-display);
    font-size: var(--text-base);
    font-weight: 500;
    color: #2e5e11;
    text-align: center;
    line-height: 1.5;
  }

  /* ── Section headings ── */
  .section-heading {
    font-family: var(--font-display);
    font-size: var(--text-xl);
    color: var(--foreground);
    font-weight: 600;
    line-height: 1.3;
    margin: 0 0 0.9rem 0;
    letter-spacing: -0.01em;
  }
  .section-sub {
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: var(--muted-foreground);
    margin: -0.5rem 0 1rem 0;
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
    padding: 0.7rem 0.8rem;
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
    background: #302f2a;
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
    color: #d2ccbf;
  }
  .timing-card--neutral .timing-intensity,
  .timing-card--neutral .timing-value {
    color: #f2efe8;
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
  }
  .estimator-card--saving {
    background: var(--accent-light);
    border-color: var(--bg-green-border);
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
    font-size: var(--text-lg);
    font-weight: 600;
    color: var(--foreground);
    line-height: 1.35;
    margin: 0 0 0.55rem 0;
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
    background: #e7f1d5;
    border-left: 3px solid var(--accent);
    padding: 0.6rem 0.9rem;
    margin-top: 0.4rem;
    margin-bottom: 1rem;
    font-family: var(--font-body);
    font-size: var(--text-base);
    color: #2f431d;
    line-height: 1.55;
  }
  .chart-insight strong { color: #203015; }

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
    padding: 0.75rem 0.9rem;
    margin-top: 0.75rem;
    margin-bottom: 0.6rem;
  }
  .info-panel .section-heading {
    margin-bottom: 0.6rem;
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
    color: var(--foreground);
    text-decoration: none;
    font-weight: 600;
  }
  .linkedin-link:hover { color: var(--accent); }
  .linkedin-icon { width: 18px; height: 18px; fill: #374151; }

  details[data-testid="stExpander"] {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
  }
  details[data-testid="stExpander"] summary {
    color: var(--foreground);
  }
  div[data-testid="stDataFrame"] {
    background: var(--card);
    border-radius: var(--radius);
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
    .header-band { padding: 1.1rem 0 1rem 0; }
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
    :root { --text-base: 0.82rem; --text-lg: 0.94rem; --text-xl: 1.15rem; --text-2xl: 1.5rem; }
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
        pd.read_csv(data_dir / "duid_lookup.csv")
        [["DUID", "Unit Name", "Technology Type", "Region"]]
        .drop_duplicates("DUID")
    )
    ef_raw = pd.read_csv(data_dir / "emissions_factors.csv")
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


@st.cache_data(ttl=300)
def load_today(data_dir_str: str) -> pd.DataFrame:
    """Load today's SCADA file (refreshed every 5 minutes)."""
    data_dir = Path(data_dir_str)
    scada = pd.read_csv(data_dir / "dispatch_scada_today.csv", parse_dates=["SETTLEMENTDATE"])
    scada = scada[scada["SCADAVALUE"] > 0].copy()
    return _enrich(scada, data_dir)


@st.cache_data(ttl=3600)
def load_month(data_dir_str: str, year_month: str) -> pd.DataFrame:
    """Load a monthly archive file (cached for 1 hour — historical data is stable)."""
    data_dir = Path(data_dir_str)
    scada = pd.read_csv(
        data_dir / f"dispatch_scada_{year_month}.csv",
        parse_dates=["SETTLEMENTDATE"],
    )
    scada = scada[scada["SCADAVALUE"] > 0].copy()
    return _enrich(scada, data_dir)


def load_scada_for_date(data_dir_str: str, date: datetime.date) -> pd.DataFrame:
    """Load and filter data for a specific calendar date."""
    today = datetime.date.today()
    if date == today:
        df = load_today(data_dir_str)
    else:
        df = load_month(data_dir_str, date.strftime("%Y-%m"))
    return df[df["SETTLEMENTDATE"].dt.date == date].copy()


try:
    APP_DIR = Path(__file__).resolve().parent
    DATA_DIR = ensure_required_data(APP_DIR)
except FileNotFoundError as e:
    st.error("### Data files not found")
    st.markdown(str(e))
    st.stop()

# ── Derive date range and regions without loading all SCADA data ──
_monthly_files = sorted(glob.glob(str(DATA_DIR / "dispatch_scada_????-??.csv")))
if _monthly_files:
    _earliest_month = Path(_monthly_files[0]).stem[-7:]  # e.g. "2026-02"
    date_min = pd.Period(_earliest_month, freq="M").start_time.date()
else:
    date_min = datetime.date.today()
date_max = datetime.date.today()

_lookup_df = pd.read_csv(DATA_DIR / "duid_lookup.csv")
regions = sorted(_lookup_df["Region"].dropna().unique().tolist())


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
ZERO_EMISSION = {"Wind", "Solar PV", "Hydro", "Battery Storage"}

RESOLUTIONS = {
    "5 minutes":  "5min",
    "1 hour": "1h",
}
INTERVAL_MINUTES = {
    "5 minutes": 5,
    "1 hour": 60,
}
ILLUSTRATIVE_CARBON_RATE = 75

plotly_config = {
    "displayModeBar": False,
    "displaylogo": False,
    "scrollZoom": False,
    "doubleClick": False,
    "responsive": True,
}


# ─────────────────────────────────────────────────────────────
# State & filtering
# ─────────────────────────────────────────────────────────────
date_min = df["SETTLEMENTDATE"].dt.date.min()
date_max = df["SETTLEMENTDATE"].dt.date.max()
regions = sorted(df["Region"].dropna().unique().tolist())
default_selected_date = date_max

if "selected_date" not in st.session_state:
    st.session_state.selected_date = default_selected_date
if "resolution_label" not in st.session_state:
    st.session_state.resolution_label = "1 hour"
if "scope_choice" not in st.session_state:
    st.session_state.scope_choice = "Scope 1 only"
if "sel_regions" not in st.session_state:
    st.session_state.sel_regions = regions.copy()

selected_date = st.session_state.selected_date
resolution_label = st.session_state.resolution_label
resolution = RESOLUTIONS[resolution_label]
interval_minutes = INTERVAL_MINUTES[resolution_label]
scope_choice = st.session_state.scope_choice
sel_regions = st.session_state.sel_regions

# Filter to selected day and regions
mask_day = (
    (df["SETTLEMENTDATE"].dt.date == selected_date) &
    (df["Region"].isin(sel_regions))
)
dff = df[mask_day].copy()

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
  <div class="page-header">Real-Time Energy Dashboard</div>
  <div class='page-deck'>NEM grid emissions intensity, updated from AEMO 5-minute dispatch data.
  Personal project by Tanjim Islam, for demonstration purposes only.</div>
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


st.markdown("<h2 class='section-heading'>Generation Mix and Emissions Profile</h2>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Stacked generation by technology with total emissions over the selected day</p>",
            unsafe_allow_html=True)

dff["period"] = dff["SETTLEMENTDATE"].dt.floor(resolution)
agg = (
    dff.groupby("period")
    .agg(mwh=("mwh", "sum"), tco2e=(emission_col, "sum"))
    .reset_index()
)
mix = dff.groupby(["period", "Technology Type"]).agg(mwh=("mwh", "sum")).reset_index()
tech_order = [t for t in TECH_COLORS if t in mix["Technology Type"].unique()]

if not interval_agg.empty and period_low > 0:
    ratio = period_high / period_low
    best_t = interval_agg["intensity"].idxmin()
    worst_t = interval_agg["intensity"].idxmax()
    chart_title = f"Grid ran {ratio:.1f}x cleaner at {best_t.strftime('%H:%M')} than at {worst_t.strftime('%H:%M')} today"
else:
    chart_title = "Generation mix and total emissions"

chart_tickvals = pd.date_range(
    start=pd.Timestamp(selected_date),
    end=pd.Timestamp(selected_date) + pd.Timedelta(hours=22),
    freq="2h",
)

combo_fig = make_subplots(specs=[[{"secondary_y": True}]])
for tech in tech_order:
    subset = mix[mix["Technology Type"] == tech]
    combo_fig.add_trace(go.Bar(
        x=subset["period"],
        y=subset["mwh"],
        name=tech,
        marker=dict(color=TECH_COLORS.get(tech, "#555"), line=dict(color="#25241f", width=0.35)),
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

if clean_window_start is not None and clean_window_end is not None:
    combo_fig.add_vrect(
        x0=clean_window_start,
        x1=clean_window_end + pd.Timedelta(minutes=5),
        fillcolor="#bbf7d0",
        opacity=0.22,
        layer="below",
        line_width=0,
        annotation_text="Cleanest 4 hours",
        annotation_position="top left",
        annotation_font=dict(color="#166534", family="Inter, sans-serif", size=11),
    )

combo_fig.update_layout(
    barmode="stack",
    bargap=0,
    bargroupgap=0,
    xaxis=dict(
        showgrid=False,
        color="#b7b1a6",
        tickmode="array",
        tickvals=chart_tickvals,
        ticktext=[str(ts.hour) for ts in chart_tickvals],
        tickangle=0,
        tickfont=dict(size=13),
    ),
    plot_bgcolor="#302f2a",
    paper_bgcolor="#302f2a",
    font=dict(color="#f2efe8", family="Inter, sans-serif", size=13),
    legend=dict(
        bgcolor="#35342f",
        bordercolor="#47453d",
        orientation="h",
        yanchor="top",
        y=-0.08,
        xanchor="center",
        x=0.5,
        font=dict(size=13),
    ),
    margin=dict(l=0, r=0, t=8, b=42),
    hovermode="x unified",
    height=520,
)
combo_fig.update_yaxes(
    title_text="",
    showgrid=True,
    gridcolor="#47453d",
    zeroline=False,
    color="#b7b1a6",
    tickfont=dict(size=13),
    automargin=True,
    secondary_y=False,
)
combo_fig.update_yaxes(
    title_text="",
    showgrid=False,
    zeroline=False,
    range=[0, int(((1600 * interval_minutes / 15) + 99) // 100) * 100],
    color="#b7b1a6",
    tickfont=dict(size=13),
    automargin=True,
    secondary_y=True,
)
combo_fig.update_xaxes(fixedrange=True)
combo_fig.update_yaxes(fixedrange=True)

st.markdown(f"<div class='chart-title'>{chart_title}</div>", unsafe_allow_html=True)
st.markdown(
    "<div class='chart-axis-notes'><span>Left, MWh</span><span>Right, t CO&#8322;-e</span></div>",
    unsafe_allow_html=True,
)
st.plotly_chart(combo_fig, use_container_width=True, config=plotly_config)


# ── Controls ────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns([1.15, 1.1, 1.35, 2.4], gap="medium")
with c1:
    st.date_input("Date", min_value=date_min, max_value=date_max,
                  key="selected_date", format="DD/MM/YYYY")
with c2:
    st.selectbox("Interval", list(RESOLUTIONS.keys()), key="resolution_label")
with c3:
    st.radio("Emissions scope", ["Scope 1 only", "Scope 1 + 3 (combined)"],
             key="scope_choice",
             help="Scope 1 = direct combustion. Scope 3 = upstream fuel extraction (coal only in NGA 2025).",
             horizontal=True)
with c4:
    st.multiselect("Regions", regions, key="sel_regions")

st.markdown(
    f"<div class='meta-line'>AEMO NEM  &middot;  {selected_date.strftime('%d %B %Y')}  &middot;  "
    f"{', '.join(sel_regions) if sel_regions else 'No region selected'}  &middot;  "
    f"{scope_choice}  &middot;  {resolution_label} intervals</div>",
    unsafe_allow_html=True
)


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
    st.markdown("<h2 class='section-heading'>Fuel Mix by Region</h2>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Current-day generation breakdown by technology type</p>",
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
            line=dict(color="#25241f", width=0.5),
        ),
        customdata=fuel_mix[["pct"]],
        hovertemplate="<b>%{y}</b><br>%{x:,.0f} MWh (%{customdata[0]:.1f}%)<extra></extra>",
    ))
    fuel_fig.update_layout(
        plot_bgcolor="#302f2a", paper_bgcolor="#302f2a",
        font=dict(color="#f2efe8", family="Inter, sans-serif"),
        margin=dict(l=0, r=20, t=8, b=8),
        height=max(280, len(fuel_mix) * 40),
        showlegend=False,
        xaxis=dict(showgrid=True, gridcolor="#47453d", title_text="MWh", color="#b7b1a6"),
        yaxis=dict(showgrid=False, color="#f2efe8", automargin=True),
    )
    fuel_fig.update_xaxes(fixedrange=True)
    fuel_fig.update_yaxes(fixedrange=True)
    st.plotly_chart(fuel_fig, use_container_width=True, config=plotly_config)

with charts_right:
    st.markdown("<h2 class='section-heading'>Emissions Intensity Across the Day</h2>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>The duck curve, when the grid is clean vs dirty, by hour</p>",
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
            marker=dict(color=duck_colors, line=dict(color="#25241f", width=0.5)),
            hovertemplate="Hour %{x}:00<br><b>%{y:.3f}</b> t CO₂-e/MWh<extra></extra>",
        ))
        duck_fig.update_layout(
            plot_bgcolor="#302f2a", paper_bgcolor="#302f2a",
            font=dict(color="#f2efe8", family="Inter, sans-serif"),
            margin=dict(l=10, r=10, t=8, b=8),
            height=320,
            showlegend=False,
            xaxis=dict(
                showgrid=False, color="#b7b1a6",
                tickmode="array",
                tickvals=list(range(0, 24, 2)),
                ticktext=[f"{h}:00" for h in range(0, 24, 2)],
                title_text="Hour of Day",
            ),
            yaxis=dict(
                showgrid=True, gridcolor="#47453d", color="#b7b1a6",
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

        st.plotly_chart(duck_fig, use_container_width=True, config=plotly_config)

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
st.markdown("<h2 class='section-heading'>Operational Windows</h2>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Best and worst times to run flexible load today</p>",
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
st.markdown("<h2 class='section-heading'>Industry Impact Profiles</h2>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Illustrative daily emissions for typical operations, and what shifting to the cleanest window could save</p>",
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
st.markdown("<h2 class='section-heading'>Scope 2 Estimator</h2>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Type your daily consumption to see illustrative emissions and savings</p>",
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
    ef_display = pd.read_csv(DATA_DIR / "emissions_factors.csv")
    st.dataframe(ef_display, use_container_width=True, hide_index=True)
    st.caption(
        "Source: National Greenhouse Accounts Factors 2025, DCCEEW. "
        "Converted with kg CO\u2082-e/GJ \u00d7 3.6 GJ/MWh \u00f7 1000."
    )

st.markdown("""
<div class="info-panel section-text">
<h3 class="section-heading">How this dashboard should be used</h3>
This dashboard is a <strong>5-minute near-real-time reference layer</strong> for NEM grid emissions intensity.
It shows how clean or dirty the grid is by time of day and region, using AEMO dispatch data joined to emissions factors.
It does <strong>not</strong> calculate a company's official disclosure by itself. Disclosure-grade Scope 2 reporting still requires the company's own interval consumption data.
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="info-panel section-text">
<h3 class="section-heading">Data lineage, methodology, and sources</h3>
<b>Coverage</b>: NEM regions only (QLD, NSW, VIC, SA, TAS). Excludes WEM, NT grids, and rooftop solar.<br><br>
<b>Lineage</b>: <code>dispatch_scada.csv</code> provides 5-minute generator dispatch by DUID, <code>duid_lookup.csv</code> maps DUIDs to technology and region, and <code>emissions_factors.csv</code> provides technology-level emissions factors.<br><br>
<b>Transform</b>: Python joins those three datasets, filters to positive dispatch, converts dispatch MW into interval MWh with <code>mwh = SCADAVALUE * (5 / 60)</code>, and aggregates by time window and technology.<br><br>
<b>Sources</b><br>
&bull; AEMO Dispatch SCADA:
<a href="https://nemweb.com.au/Reports/Current/Dispatch_SCADA/">nemweb.com.au</a><br>
&bull; AEMO Generation Information:
<a href="https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information">aemo.com.au</a><br>
&bull; National Greenhouse Accounts Factors 2025:
<a href="https://www.dcceew.gov.au/climate-change/publications/national-greenhouse-accounts-factors">dcceew.gov.au</a>
</div>
""", unsafe_allow_html=True)


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
