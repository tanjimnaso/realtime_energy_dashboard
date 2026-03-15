import io
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


AEMO_BASE_URL = "https://www.aemo.com.au"
AEMO_GENERATION_INFO_PAGE = (
    "https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/"
    "nem-forecasting-and-planning/forecasting-and-planning-data/generation-information"
)
NEMWEB_BASE_URL = "https://nemweb.com.au"
NEMWEB_DISPATCH_SCADA_URL = f"{NEMWEB_BASE_URL}/Reports/Current/Dispatch_SCADA/"
REQUEST_TIMEOUT_SECONDS = 30
SCADA_ARCHIVE_LIMIT = 576
SCADA_COLUMNS = ["SETTLEMENTDATE", "DUID", "SCADAVALUE"]

EMISSIONS_FACTORS_ROWS = [
    {
        "technology_type": "Coal",
        "scope": "scope_1",
        "emission_factor_tCO2e_MWh": 0.3249,
        "nga_fuel_name": "Bituminous coal",
        "nga_factor_kgCO2e_GJ": 90.24,
        "conversion_note": "Scope 1, Table 4. NEM black coal generators. 90.24 kg CO2-e/GJ x 3.6 GJ/MWh / 1000",
    },
    {
        "technology_type": "Brown coal",
        "scope": "scope_1",
        "emission_factor_tCO2e_MWh": 0.3378,
        "nga_fuel_name": "Brown coal (lignite)",
        "nga_factor_kgCO2e_GJ": 93.82,
        "conversion_note": "Scope 1, Table 4. Latrobe Valley brown coal generators. 93.82 x 3.6 / 1000",
    },
    {
        "technology_type": "Gas Turbine",
        "scope": "scope_1",
        "emission_factor_tCO2e_MWh": 0.1855,
        "nga_fuel_name": "Natural gas distributed in a pipeline",
        "nga_factor_kgCO2e_GJ": 51.53,
        "conversion_note": "Scope 1, Table 5. Natural gas CCGT/OCGT. 51.53 x 3.6 / 1000",
    },
    {
        "technology_type": "Other",
        "scope": "scope_1",
        "emission_factor_tCO2e_MWh": 0.2527,
        "nga_fuel_name": "Diesel oil",
        "nga_factor_kgCO2e_GJ": 70.2,
        "conversion_note": "Scope 1, Table 8. Proxy for diesel/other fossil. 70.20 x 3.6 / 1000",
    },
    {
        "technology_type": "Wind",
        "scope": "scope_1",
        "emission_factor_tCO2e_MWh": 0.0,
        "nga_fuel_name": "Renewable",
        "nga_factor_kgCO2e_GJ": 0.0,
        "conversion_note": "Zero direct emissions - no combustion",
    },
    {
        "technology_type": "Solar PV",
        "scope": "scope_1",
        "emission_factor_tCO2e_MWh": 0.0,
        "nga_fuel_name": "Renewable",
        "nga_factor_kgCO2e_GJ": 0.0,
        "conversion_note": "Zero direct emissions - no combustion",
    },
    {
        "technology_type": "Hydro",
        "scope": "scope_1",
        "emission_factor_tCO2e_MWh": 0.0,
        "nga_fuel_name": "Renewable",
        "nga_factor_kgCO2e_GJ": 0.0,
        "conversion_note": "Zero direct emissions - no combustion",
    },
    {
        "technology_type": "Battery Storage",
        "scope": "scope_1",
        "emission_factor_tCO2e_MWh": 0.0,
        "nga_fuel_name": "Storage",
        "nga_factor_kgCO2e_GJ": 0.0,
        "conversion_note": "Zero direct emissions - downstream from generation source",
    },
    {
        "technology_type": "Coal",
        "scope": "scope_3",
        "emission_factor_tCO2e_MWh": 0.0108,
        "nga_fuel_name": "Bituminous coal",
        "nga_factor_kgCO2e_GJ": 3.0,
        "conversion_note": "Scope 3, Table 4. Upstream extraction/transport. 3.0 x 3.6 / 1000",
    },
    {
        "technology_type": "Brown coal",
        "scope": "scope_3",
        "emission_factor_tCO2e_MWh": 0.00144,
        "nga_fuel_name": "Brown coal (lignite)",
        "nga_factor_kgCO2e_GJ": 0.4,
        "conversion_note": "Scope 3, Table 4. 0.4 x 3.6 / 1000",
    },
    {
        "technology_type": "Gas Turbine",
        "scope": "scope_3",
        "emission_factor_tCO2e_MWh": 0.0,
        "nga_fuel_name": "Natural gas",
        "nga_factor_kgCO2e_GJ": 0.0,
        "conversion_note": "NGA does not specify Scope 3 for pipeline gas combustion in electricity",
    },
    {
        "technology_type": "Other",
        "scope": "scope_3",
        "emission_factor_tCO2e_MWh": 0.0,
        "nga_fuel_name": "Diesel oil",
        "nga_factor_kgCO2e_GJ": 0.0,
        "conversion_note": "NGA Scope 3 for diesel not separately specified for stationary energy",
    },
    {
        "technology_type": "Wind",
        "scope": "scope_3",
        "emission_factor_tCO2e_MWh": 0.0,
        "nga_fuel_name": "Renewable",
        "nga_factor_kgCO2e_GJ": 0.0,
        "conversion_note": "Not applicable",
    },
    {
        "technology_type": "Solar PV",
        "scope": "scope_3",
        "emission_factor_tCO2e_MWh": 0.0,
        "nga_fuel_name": "Renewable",
        "nga_factor_kgCO2e_GJ": 0.0,
        "conversion_note": "Not applicable",
    },
    {
        "technology_type": "Hydro",
        "scope": "scope_3",
        "emission_factor_tCO2e_MWh": 0.0,
        "nga_fuel_name": "Renewable",
        "nga_factor_kgCO2e_GJ": 0.0,
        "conversion_note": "Not applicable",
    },
    {
        "technology_type": "Battery Storage",
        "scope": "scope_3",
        "emission_factor_tCO2e_MWh": 0.0,
        "nga_fuel_name": "Storage",
        "nga_factor_kgCO2e_GJ": 0.0,
        "conversion_note": "Not applicable",
    },
]

FALLBACK_DUID_ROWS = [
    ("DG_NSW1", "Distrib. Gen NSW", "Other", "NSW1", "Non-scheduled"),
    ("DG_QLD1", "Distrib. Gen QLD", "Other", "QLD1", "Non-scheduled"),
    ("DG_SA1", "Distrib. Gen SA", "Other", "SA1", "Non-scheduled"),
    ("DG_TAS1", "Distrib. Gen TAS", "Other", "TAS1", "Non-scheduled"),
    ("DG_VIC1", "Distrib. Gen VIC", "Other", "VIC1", "Non-scheduled"),
    ("RT_NSW1", "RT Solar NSW 1", "Solar PV", "NSW1", "Non-scheduled"),
    ("RT_NSW2", "RT Solar NSW 2", "Solar PV", "NSW1", "Non-scheduled"),
    ("RT_NSW3", "RT Solar NSW 3", "Solar PV", "NSW1", "Non-scheduled"),
    ("RT_NSW4", "RT Solar NSW 4", "Solar PV", "NSW1", "Non-scheduled"),
    ("RT_NSW5", "RT Solar NSW 5", "Solar PV", "NSW1", "Non-scheduled"),
    ("RT_NSW6", "RT Solar NSW 6", "Solar PV", "NSW1", "Non-scheduled"),
    ("RT_QLD1", "RT Solar QLD", "Solar PV", "QLD1", "Non-scheduled"),
    ("RT_SA1", "RT Solar SA 1", "Solar PV", "SA1", "Non-scheduled"),
    ("RT_SA2", "RT Solar SA 2", "Solar PV", "SA1", "Non-scheduled"),
    ("RT_SA3", "RT Solar SA 3", "Solar PV", "SA1", "Non-scheduled"),
    ("RT_SA4", "RT Solar SA 4", "Solar PV", "SA1", "Non-scheduled"),
    ("RT_SA5", "RT Solar SA 5", "Solar PV", "SA1", "Non-scheduled"),
    ("RT_SA6", "RT Solar SA 6", "Solar PV", "SA1", "Non-scheduled"),
    ("RT_TAS1", "RT Solar TAS", "Solar PV", "TAS1", "Non-scheduled"),
    ("RT_VIC1", "RT Solar VIC 1", "Solar PV", "VIC1", "Non-scheduled"),
    ("RT_VIC2", "RT Solar VIC 2", "Solar PV", "VIC1", "Non-scheduled"),
    ("RT_VIC3", "RT Solar VIC 3", "Solar PV", "VIC1", "Non-scheduled"),
    ("RT_VIC4", "RT Solar VIC 4", "Solar PV", "VIC1", "Non-scheduled"),
    ("RT_VIC5", "RT Solar VIC 5", "Solar PV", "VIC1", "Non-scheduled"),
    ("RT_VIC6", "RT Solar VIC 6", "Solar PV", "VIC1", "Non-scheduled"),
    ("RT_VIC7", "RT Solar VIC 7", "Solar PV", "VIC1", "Non-scheduled"),
    ("RT_VIC8", "RT Solar VIC 8", "Solar PV", "VIC1", "Non-scheduled"),
    ("RT_VIC9", "RT Solar VIC 9", "Solar PV", "VIC1", "Non-scheduled"),
    ("RT_VIC10", "RT Solar VIC 10", "Solar PV", "VIC1", "Non-scheduled"),
    ("RT_VIC11", "RT Solar VIC 11", "Solar PV", "VIC1", "Non-scheduled"),
    ("RT_VIC12", "RT Solar VIC 12", "Solar PV", "VIC1", "Non-scheduled"),
    ("PUMP1", "Pumped Hydro Load 1", "Hydro", "NSW1", "Scheduled"),
    ("PUMP2", "Pumped Hydro Load 2", "Hydro", "NSW1", "Scheduled"),
    ("SNOWYP", "Snowy Pump Load", "Hydro", "NSW1", "Scheduled"),
    ("SHPUMP", "Shoalhaven Pump", "Hydro", "NSW1", "Scheduled"),
    ("SHOAL1", "Shoalhaven", "Hydro", "NSW1", "Scheduled"),
    ("MLSP1", "Mt Lyell Smelter", "Other", "TAS1", "Non-scheduled"),
    ("PIONEER", "Pioneer Sugar Mill", "Other", "QLD1", "Non-scheduled"),
    ("BUSF1", "Bulli Creek Solar Farm", "Solar PV", "QLD1", "Semi-scheduled"),
    ("CRWARP1", "Crown Point Solar", "Solar PV", "QLD1", "Semi-scheduled"),
    ("GPWFWST1", "Golden Plains Wind W1", "Wind", "VIC1", "Semi-scheduled"),
    ("GPWFWST2", "Golden Plains Wind W2", "Wind", "VIC1", "Semi-scheduled"),
    ("KIATAWF1", "Kiata Wind Farm", "Wind", "VIC1", "Semi-scheduled"),
    ("KIDSPHL1", "Kidston Pump Hydro 1", "Hydro", "QLD1", "Scheduled"),
    ("KIDSPHL2", "Kidston Pump Hydro 2", "Hydro", "QLD1", "Scheduled"),
    ("LANCSF1", "Lancaster Solar Farm", "Solar PV", "VIC1", "Semi-scheduled"),
    ("NESBESS1", "Neoen BESS 1", "Battery Storage", "SA1", "Scheduled"),
    ("NESBESS2", "Neoen BESS 2", "Battery Storage", "SA1", "Scheduled"),
    ("TRGBESS1", "Torrens Island BESS", "Battery Storage", "SA1", "Scheduled"),
    ("WNSF1", "Walla Walla North Solar", "Solar PV", "NSW1", "Semi-scheduled"),
]


def resolve_data_dir(base_dir: Path) -> Path:
    data_dir = base_dir / "data"

    if data_dir.is_symlink():
        try:
            resolved_dir = data_dir.resolve(strict=True)
        except FileNotFoundError:
            resolved_dir = None
        if resolved_dir and resolved_dir.is_dir():
            return resolved_dir

    if data_dir.is_dir():
        return data_dir

    runtime_data_dir = base_dir / ".runtime_data"
    runtime_data_dir.mkdir(parents=True, exist_ok=True)
    return runtime_data_dir


def ensure_required_data(base_dir: Path) -> Path:
    data_dir = resolve_data_dir(base_dir)
    required = required_data_paths(base_dir)
    bootstrap_errors = []

    if file_missing_or_empty(required["Emissions factors"]):
        write_emissions_factors(required["Emissions factors"])

    if file_missing_or_empty(required["DUID lookup"]):
        try:
            build_duid_lookup(required["DUID lookup"])
        except Exception as exc:
            bootstrap_errors.append(f"- Could not build `duid_lookup.csv`: {exc}")

    if file_missing_or_empty(required["SCADA data"]):
        try:
            download_recent_dispatch_scada(required["SCADA data"])
        except Exception as exc:
            bootstrap_errors.append(f"- Could not download `dispatch_scada_today.csv`: {exc}")

    missing = []
    empty = []
    for label, path in required.items():
        if not path.exists():
            missing.append(f"`{display_path(path, base_dir)}` ({label})")
        elif path.stat().st_size == 0:
            empty.append(f"`{display_path(path, base_dir)}` ({label})")

    if missing or empty:
        details = [
            "The app attempted to bootstrap runtime data automatically but could not prepare every required file.",
            f"Resolved data directory: `{display_path(data_dir, base_dir)}`",
        ]
        if bootstrap_errors:
            details.append("**Bootstrap errors:**\n" + "\n".join(bootstrap_errors))
        if missing:
            details.append("**Missing files:**\n" + "\n".join(f"- {item}" for item in missing))
        if empty:
            details.append("**Empty files:**\n" + "\n".join(f"- {item}" for item in empty))
        raise FileNotFoundError("\n\n".join(details))

    return data_dir


def required_data_paths(base_dir: Path) -> dict[str, Path]:
    data_dir = resolve_data_dir(base_dir)
    return {
        "SCADA data": data_dir / "dispatch_scada_today.csv",
        "DUID lookup": data_dir / "duid_lookup.csv",
        "Emissions factors": data_dir / "emissions_factors.csv",
    }


def file_missing_or_empty(path: Path) -> bool:
    return not path.exists() or path.stat().st_size == 0


def display_path(path: Path, base_dir: Path) -> str:
    try:
        return str(path.relative_to(base_dir))
    except ValueError:
        return str(path)


def write_emissions_factors(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(EMISSIONS_FACTORS_ROWS).to_csv(output_path, index=False)


def build_duid_lookup(output_path: Path) -> None:
    download_url = find_generation_information_download_url()
    response = requests.get(download_url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    workbook = io.BytesIO(response.content)
    generator_info = pd.read_excel(
        workbook,
        sheet_name="Generator Information",
        header=3,
    )

    generator_info = (
        generator_info[["DUID", "Unit Name", "Technology Type", "Region", "Dispatch Type"]]
        .dropna(subset=["DUID"])
        .assign(source="AEMO Gen Info")
    )

    fallback = pd.DataFrame(
        FALLBACK_DUID_ROWS,
        columns=["DUID", "Unit Name", "Technology Type", "Region", "Dispatch Type"],
    ).assign(source="manual_fallback")

    lookup = pd.concat([generator_info, fallback], ignore_index=True)
    lookup = lookup.drop_duplicates(subset=["DUID"], keep="first")
    lookup = lookup.sort_values("DUID").reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    lookup.to_csv(output_path, index=False)


def find_generation_information_download_url() -> str:
    response = requests.get(AEMO_GENERATION_INFO_PAGE, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    ranked_links = []
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        href_lower = href.lower()
        text = " ".join(anchor.get_text(" ", strip=True).lower().split())
        if ".xlsx" not in href_lower:
            continue

        score = 0
        if "generation" in href_lower or "generation" in text:
            score += 2
        if "info" in href_lower or "information" in text:
            score += 2
        if "nem" in href_lower or "nem" in text:
            score += 1

        ranked_links.append((score, urljoin(AEMO_BASE_URL, href)))

    if not ranked_links:
        raise RuntimeError("No Excel download link was found on the AEMO Generation Information page.")

    ranked_links.sort(key=lambda item: item[0], reverse=True)
    return ranked_links[0][1]


def download_recent_dispatch_scada(output_path: Path, max_archives: int = SCADA_ARCHIVE_LIMIT) -> None:
    response = requests.get(NEMWEB_DISPATCH_SCADA_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")

    zip_links = sorted(
        {
            urljoin(NEMWEB_BASE_URL, anchor["href"])
            for anchor in soup.find_all("a", href=True)
            if anchor["href"].lower().endswith(".zip")
        }
    )
    if not zip_links:
        raise RuntimeError("No SCADA archives were listed on the NEMWEB Dispatch_SCADA page.")

    selected_links = zip_links[-max_archives:]
    frames = []
    for link in selected_links:
        archive_response = requests.get(link, timeout=REQUEST_TIMEOUT_SECONDS)
        archive_response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(archive_response.content)) as archive:
            for member_name in archive.namelist():
                if not member_name.lower().endswith(".csv"):
                    continue
                frame = pd.read_csv(archive.open(member_name), skiprows=1)
                frame = frame[frame.iloc[:, 0] != "C"]
                frames.append(frame[SCADA_COLUMNS])

    if not frames:
        raise RuntimeError("Downloaded SCADA archives did not contain readable CSV payloads.")

    dispatch_scada = pd.concat(frames, ignore_index=True)
    dispatch_scada = dispatch_scada.drop_duplicates(subset=["SETTLEMENTDATE", "DUID"])
    dispatch_scada = dispatch_scada.sort_values(["SETTLEMENTDATE", "DUID"]).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    dispatch_scada.to_csv(output_path, index=False)
