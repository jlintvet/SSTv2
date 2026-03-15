"""
DailySSTRetrieval.py
====================
Retrieves the last three days of MUR SST 1-km sea surface temperature data
from the NOAA CoastWatch ERDDAP server (dataset: jplMURSST41).

MUR SST v4.1 is a daily, global, 1-km L4 analysis product.  One granule is
produced per UTC day (timestamp 09:00:00 Z).  This script therefore downloads
one .nc file per day for the three most-recent days that are present in the
dataset, overwriting any previously fetched files on each run.

Output layout (relative to this script's directory):
  DailySST/
    MUR_SST_YYYYMMDD.nc       – NetCDF4 data file for each day
    manifest.json              – catalogue of currently-stored files

Behaviour
---------
* On every run the DailySST directory is scanned.  Any file whose date is
  older than three calendar days (relative to today UTC) is deleted.
* The three most-recent available days are fetched fresh and written,
  overwriting any file that already exists for those dates.
* A manifest.json is written/overwritten with metadata about every file
  that is present in DailySST after the run completes.

ERDDAP endpoint
---------------
  Base URL : https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41.nc
  Variables: analysed_sst, analysis_error, sea_ice_fraction, mask
  The full global grid at 0.01° is extremely large (~17 GB per day).
  By default this script downloads a regional subset defined by LAT/LON
  constants below.  Edit them or remove the spatial slice to fetch globally
  (not recommended on routine runs).

Dependencies
------------
  pip install requests tqdm
  (Standard-library only otherwise; xarray/netCDF4 are NOT required to run
   this retrieval script, though they are useful for downstream analysis.)
"""

import os
import json
import hashlib
import logging
import datetime
import pathlib
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# ERDDAP server and dataset
ERDDAP_BASE = "https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41.nc"

# Spatial subset  (degrees)
# Region: southern New Jersey (N) → Myrtle Beach SC (S),
#         Charlotte NC (W)        → ~200 miles offshore Virginia Beach (E)
# East boundary calculated at ~37°N where 1° longitude ≈ 53 statute miles;
# 200 mi / 53 mi·deg⁻¹ ≈ 3.77° east of Virginia Beach shore (−75.98°W).
LAT_MIN = 33.70
LAT_MAX = 39.00
LON_MIN = -80.85
LON_MAX = -72.21

# Stride (every Nth grid point).  1 = full 1-km resolution.
# Increase to e.g. 10 for a ~10-km subsampled quick download.
LAT_STRIDE = 1
LON_STRIDE = 1

# Variables to retrieve
VARIABLES = ["analysed_sst", "analysis_error", "sea_ice_fraction", "mask"]

# Output directory (relative to this file)
OUTPUT_DIR = pathlib.Path(__file__).parent / "DailySST"

# Number of days to retain
RETENTION_DAYS = 3

# How many days back to look for available data.
# MUR SST NRT is typically available with ~1-day latency; the retrospective
# reanalysis has ~4-day latency.  We search up to SEARCH_WINDOW days back.
SEARCH_WINDOW = 7

# HTTP request settings
TIMEOUT_SECONDS = 600          # 10-minute timeout per download
MAX_RETRIES = 3
BACKOFF_FACTOR = 2

# MUR SST daily timestamp (always 09:00:00 Z)
DAILY_HOUR = "09:00:00Z"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    """Return a requests Session with automatic retries."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _build_url(date: datetime.date) -> str:
    """
    Construct the ERDDAP griddap URL for a single day.

    Griddap constraint syntax:
      variable[(time_start):(time_end)][(lat_min):(lat_max)][(lon_min):(lon_max)]
    For a single daily timestamp, time_start == time_end.
    """
    ts = f"{date.isoformat()}T{DAILY_HOUR}"

    if LAT_MIN is not None:
        lat_part = f"[({LAT_MIN}):{LAT_STRIDE}:({LAT_MAX})]"
        lon_part = f"[({LON_MIN}):{LON_STRIDE}:({LON_MAX})]"
    else:
        lat_part = "[(-89.99):1:(89.99)]"
        lon_part = "[(-179.99):1:(180.0)]"

    time_part = f"[({ts}):1:({ts})]"

    var_queries = ",".join(
        f"{v}{time_part}{lat_part}{lon_part}" for v in VARIABLES
    )
    return f"{ERDDAP_BASE}?{var_queries}"


def _sha256(path: pathlib.Path, chunk: int = 1 << 20) -> str:
    """Return hex SHA-256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _check_availability(session: requests.Session, date: datetime.date) -> bool:
    """
    HEAD-check whether data for *date* exists on the server.
    Returns True if the server responds 200, False otherwise.
    """
    url = _build_url(date)
    try:
        r = session.head(url, timeout=30)
        return r.status_code == 200
    except requests.RequestException:
        return False


def _download_day(session: requests.Session, date: datetime.date,
                  dest: pathlib.Path) -> bool:
    """
    Download the NetCDF file for *date* to *dest*.
    Returns True on success, False on failure.
    """
    url = _build_url(date)
    log.info("Downloading  %s  ->  %s", date.isoformat(), dest.name)
    log.debug("URL: %s", url)

    try:
        with session.get(url, stream=True, timeout=TIMEOUT_SECONDS) as r:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length", 0))
            written = 0
            tmp = dest.with_suffix(".tmp")
            with open(tmp, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 17):  # 128 KB
                    fh.write(chunk)
                    written += len(chunk)
            tmp.rename(dest)
            log.info(
                "  Saved %s  (%.1f MB)",
                dest.name,
                written / 1e6,
            )
        return True
    except requests.HTTPError as exc:
        log.warning("  HTTP error for %s: %s", date.isoformat(), exc)
        if dest.with_suffix(".tmp").exists():
            dest.with_suffix(".tmp").unlink()
        return False
    except requests.RequestException as exc:
        log.warning("  Request error for %s: %s", date.isoformat(), exc)
        if dest.with_suffix(".tmp").exists():
            dest.with_suffix(".tmp").unlink()
        return False


def _purge_old_files(output_dir: pathlib.Path, cutoff: datetime.date) -> list:
    """
    Delete any MUR_SST_YYYYMMDD.nc files whose date is older than *cutoff*.
    Returns list of deleted filenames.
    """
    deleted = []
    for nc_file in sorted(output_dir.glob("MUR_SST_????????.nc")):
        stem = nc_file.stem          # e.g. MUR_SST_20240312
        date_str = stem.split("_")[-1]
        try:
            file_date = datetime.date.fromisoformat(
                f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            )
        except ValueError:
            continue
        if file_date < cutoff:
            log.info("Purging old file: %s", nc_file.name)
            nc_file.unlink()
            deleted.append(nc_file.name)
    return deleted


def _write_manifest(output_dir: pathlib.Path, fetched: list[dict]) -> pathlib.Path:
    """
    Write manifest.json to *output_dir*.
    The manifest lists every .nc file currently in the directory.
    """
    files_on_disk = []
    for nc_file in sorted(output_dir.glob("MUR_SST_????????.nc")):
        stem = nc_file.stem
        date_str = stem.split("_")[-1]
        iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

        # Find matching entry from this run (has sha256); else compute it.
        entry = next(
            (f for f in fetched if f.get("filename") == nc_file.name), None
        )
        if entry:
            sha = entry.get("sha256", _sha256(nc_file))
        else:
            sha = _sha256(nc_file)

        files_on_disk.append(
            {
                "filename": nc_file.name,
                "date": iso_date,
                "size_bytes": nc_file.stat().st_size,
                "sha256": sha,
                "erddap_dataset": "jplMURSST41",
                "variables": VARIABLES,
                "lat_range": [LAT_MIN, LAT_MAX],
                "lon_range": [LON_MIN, LON_MAX],
                "stride": [LAT_STRIDE, LON_STRIDE],
                "source": "https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41",
            }
        )

    manifest = {
        "generated_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "retention_days": RETENTION_DAYS,
        "file_count": len(files_on_disk),
        "files": files_on_disk,
    }
    manifest_path = output_dir / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    log.info("Manifest written: %s  (%d files)", manifest_path, len(files_on_disk))
    return manifest_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    today_utc = datetime.datetime.utcnow().date()
    cutoff_date = today_utc - datetime.timedelta(days=RETENTION_DAYS)

    log.info("=== MUR SST Daily Retrieval ===")
    log.info("Today (UTC)  : %s", today_utc)
    log.info("Cutoff date  : %s  (files older than this will be deleted)", cutoff_date)

    # 1. Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 2. Purge files outside the retention window
    deleted = _purge_old_files(OUTPUT_DIR, cutoff_date)
    if deleted:
        log.info("Purged %d old file(s): %s", len(deleted), deleted)
    else:
        log.info("No files to purge.")

    # 3. Identify the three most-recent available days
    session = _make_session()

    # MUR SST has a ~1-day NRT latency (sometimes up to 4 days for the
    # retrospective product).  We probe backwards from yesterday.
    candidate_dates = [
        today_utc - datetime.timedelta(days=d) for d in range(1, SEARCH_WINDOW + 1)
    ]

    target_dates: list[datetime.date] = []
    log.info("Probing server for available dates …")
    for d in candidate_dates:
        if len(target_dates) == RETENTION_DAYS:
            break
        log.info("  Checking %s …", d.isoformat())
        if _check_availability(session, d):
            log.info("    Available ✓")
            target_dates.append(d)
        else:
            log.info("    Not yet available, skipping.")
        time.sleep(0.5)   # be polite to the server

    if not target_dates:
        log.error("Could not find any available dates within the search window.")
        return

    log.info("Will fetch %d day(s): %s", len(target_dates),
             [d.isoformat() for d in target_dates])

    # 4. Download each target day (overwrite if file already exists)
    fetched = []
    for date in target_dates:
        filename = f"MUR_SST_{date.strftime('%Y%m%d')}.nc"
        dest = OUTPUT_DIR / filename

        success = _download_day(session, date, dest)
        if success:
            fetched.append(
                {
                    "filename": filename,
                    "date": date.isoformat(),
                    "sha256": _sha256(dest),
                }
            )
        else:
            log.warning("Skipping manifest entry for %s (download failed).", date)

    # 5. Write manifest
    _write_manifest(OUTPUT_DIR, fetched)

    log.info("=== Done. %d/%d day(s) successfully retrieved. ===",
             len(fetched), len(target_dates))


if __name__ == "__main__":
    main()
