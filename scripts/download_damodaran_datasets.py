"""
scripts/download_damodaran_datasets.py

Phase: 3
Specs: SPEC-VAL-009
Owner: Platform / Valuation
Consumers: systems/damodaran_valuation/dcf/wacc.py (manual refresh path)

Downloads Damodaran's annual datasets from NYU Stern and caches them locally.

Run once per year (January, after Damodaran updates his data files):
    python scripts/download_damodaran_datasets.py

Datasets from pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/:
  betas.xls   — unlevered betas by sector
  ctryprem.xls — country risk premiums
  wacc.xls    — WACC by industry
  mgnpe.xls   — margins and PE by sector

Cached at: datastore/raw/damodaran/

If the network is unavailable the script falls back to the hardcoded
constants in SECTOR_UNLEVERED_BETAS (July 2025 Damodaran data — see
systems/damodaran_valuation/dcf/wacc.py).  The constants are the
authoritative fallback; this script is a best-effort update mechanism.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Dict
from urllib.request import urlretrieve

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Hardcoded fallback constants — Damodaran July 2025, India emerging market
# These mirror SECTOR_UNLEVERED_BETAS in dcf/wacc.py and are the source of
# truth used when downloaded files are absent or malformed.
# ---------------------------------------------------------------------------
SECTOR_UNLEVERED_BETAS: Dict[str, float] = {
    "Banking": 0.35,
    "NBFC": 0.55,
    "Insurance": 0.60,
    "IT Services": 0.85,
    "Pharma": 0.75,
    "FMCG": 0.55,
    "Auto": 0.90,
    "Metals": 1.10,
    "Chemicals": 0.85,
    "Real Estate": 0.95,
    "Power": 0.70,
    "Infrastructure": 0.85,
    "Telecom": 0.75,
    "Default": 0.90,
}

COUNTRY_RISK_PREMIUMS: Dict[str, float] = {
    "India": 0.023,       # 2.3 % — Damodaran Jan 2025
    "US": 0.0,
    "China": 0.016,
    "Brazil": 0.029,
}

# Damodaran NYU data file base URL
_BASE_URL = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/"

_DATASETS = {
    "betas.xls": "beta.xls",        # sector unlevered betas
    "ctryprem.xls": "ctryprem.xls",  # country risk premiums
    "wacc.xls": "wacc.xls",         # WACC by industry
    "mgnpe.xls": "mgnpe.xls",       # margins & PE by sector
}

# Local cache directory (relative to project root)
_PROJECT_ROOT = Path(__file__).parent.parent
_CACHE_DIR = _PROJECT_ROOT / "datastore" / "raw" / "damodaran"


def download_datasets(cache_dir: Path = _CACHE_DIR, force: bool = False) -> Dict[str, Path]:
    """
    Download latest Damodaran annual datasets and cache locally.

    Parameters
    ----------
    cache_dir : Path
        Directory to cache downloaded files (default: datastore/raw/damodaran/).
    force : bool
        If True, re-download even if cached files exist.

    Returns
    -------
    dict
        Mapping of dataset name to local file path for files that were
        successfully downloaded.

    Notes
    -----
    Network failures are caught and logged; the function always returns
    without raising so callers can fall back to hardcoded constants.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    downloaded: Dict[str, Path] = {}

    for local_name, remote_name in _DATASETS.items():
        dest = cache_dir / local_name
        if dest.exists() and not force:
            logger.info(f"Already cached: {dest}")
            downloaded[local_name] = dest
            continue

        url = _BASE_URL + remote_name
        try:
            logger.info(f"Downloading {url} → {dest}")
            urlretrieve(url, str(dest))
            downloaded[local_name] = dest
            logger.info(f"Saved {dest} ({dest.stat().st_size:,} bytes)")
        except Exception as exc:
            logger.warning(
                f"Failed to download {url}: {exc}  "
                "Falling back to hardcoded SECTOR_UNLEVERED_BETAS constants."
            )

    return downloaded


def parse_betas(betas_path: Path) -> Dict[str, float]:
    """
    Parse the Damodaran betas.xls file into a sector → unlevered beta dict.

    Parameters
    ----------
    betas_path : Path
        Path to the locally cached betas.xls file.

    Returns
    -------
    dict
        {sector_name: unlevered_beta}.  Returns SECTOR_UNLEVERED_BETAS fallback
        if parsing fails.

    Notes
    -----
    The Damodaran betas.xls format has changed over the years; this parser
    handles the most common layout (sector in column A, unlevered corrected
    beta in column H).  Callers should treat the result as best-effort.
    """
    try:
        import openpyxl  # noqa: F401 — optional dependency
        import pandas as pd

        df = pd.read_excel(betas_path, sheet_name=0, header=0)
        # Damodaran format: first col = Industry Name, somewhere around col 7-8 = Unlevered beta
        # Column names vary by year — fuzzy-match
        beta_col = None
        for col in df.columns:
            col_lower = str(col).lower()
            if "unlevered" in col_lower and ("corrected" in col_lower or "beta" in col_lower):
                beta_col = col
                break

        if beta_col is None:
            logger.warning("Could not identify unlevered beta column in betas.xls; using defaults.")
            return SECTOR_UNLEVERED_BETAS.copy()

        name_col = df.columns[0]
        parsed: Dict[str, float] = {}
        for _, row in df.iterrows():
            try:
                name = str(row[name_col]).strip()
                beta = float(row[beta_col])
                if name and name.lower() not in ("industry name", "nan", "total market"):
                    parsed[name] = beta
            except (ValueError, TypeError):
                continue

        logger.info(f"Parsed {len(parsed)} sector betas from {betas_path}")
        return parsed or SECTOR_UNLEVERED_BETAS.copy()

    except ImportError:
        logger.warning("openpyxl not installed; cannot parse betas.xls.  Run: pip install openpyxl")
        return SECTOR_UNLEVERED_BETAS.copy()
    except Exception as exc:
        logger.warning(f"Failed to parse {betas_path}: {exc}")
        return SECTOR_UNLEVERED_BETAS.copy()


def main() -> None:
    """Entry point for CLI usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Download Damodaran annual datasets for AlphaLens valuation."
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-download even if files are already cached."
    )
    parser.add_argument(
        "--cache-dir", type=Path, default=_CACHE_DIR,
        help=f"Local cache directory (default: {_CACHE_DIR})"
    )
    args = parser.parse_args()

    downloaded = download_datasets(cache_dir=args.cache_dir, force=args.force)

    if "betas.xls" in downloaded:
        betas = parse_betas(downloaded["betas.xls"])
        logger.info(f"Sample betas: {dict(list(betas.items())[:5])}")

    if downloaded:
        logger.info(f"Successfully cached {len(downloaded)} datasets to {args.cache_dir}")
    else:
        logger.warning(
            "No datasets were downloaded.  "
            "SECTOR_UNLEVERED_BETAS hardcoded constants will be used."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
