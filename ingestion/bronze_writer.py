"""
Bronze writer: converts raw data/ CSVs to typed Parquet files in data/bronze/.

Run this before dbt build to refresh the Bronze layer.
"""
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
BRONZE_DIR = DATA_DIR / "bronze"


def write_dispatch_scada() -> None:
    """Concatenate all monthly SCADA CSV archives into one Bronze Parquet file."""
    archive_files = sorted(DATA_DIR.glob("dispatch_scada_????-??.csv"))
    if not archive_files:
        raise FileNotFoundError(f"No monthly SCADA archives found in {DATA_DIR}")

    frames = []
    for f in archive_files:
        df = pd.read_csv(f, dtype={"DUID": str, "SCADAVALUE": float})
        df["SETTLEMENTDATE"] = pd.to_datetime(df["SETTLEMENTDATE"], errors="coerce")
        frames.append(df)
        print(f"  Read {f.name}: {len(df):,} rows")

    combined = pd.concat(frames, ignore_index=True)
    out = BRONZE_DIR / "dispatch_scada.parquet"
    combined.to_parquet(out, index=False)
    print(f"Wrote {out.name}: {len(combined):,} rows")


def write_generator_metadata() -> None:
    """Copy duid_lookup.csv to Bronze Parquet."""
    src = DATA_DIR / "duid_lookup.csv"
    df = pd.read_csv(src, dtype=str)
    out = BRONZE_DIR / "generator_metadata.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out.name}: {len(df):,} rows")


def write_emissions_factors() -> None:
    """Copy emissions_factors.csv to Bronze Parquet."""
    src = DATA_DIR / "emissions_factors.csv"
    df = pd.read_csv(src, dtype={"emission_factor_tCO2e_MWh": float})
    out = BRONZE_DIR / "emissions_factors.parquet"
    df.to_parquet(out, index=False)
    print(f"Wrote {out.name}: {len(df):,} rows")


def main() -> None:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    print("=== Bronze writer ===")
    write_dispatch_scada()
    write_generator_metadata()
    write_emissions_factors()
    print("=== Done ===")


if __name__ == "__main__":
    main()
