"""
VIIRSHourlyBundler.py
=====================
Fetches NOAA VIIRS hourly SST passes from the CoastWatch THREDDS catalog and
writes one compact JSON bundle per day into DailySST/.

Output files
------------
  DailySST/viirs_YYYY-MM-DD.json   — one file per day, all hourly passes
  DailySST/viirs_index.json        — list of available dates (React reads this first)

Bundle format  (viirs_YYYY-MM-DD.json)
--------------------------------------
{
  "date": "2026-05-18",
  "generated": "2026-05-18T14:22:00Z",
  "latSet": [33.70, 33.72, ...],        // ascending
  "lonSet": [-78.89, -78.87, ...],      // ascending
  "available_hours": [6, 9, 12, 15],   // UTC hours present in this file
  "hours": {
    "12": {
      "sst": [75.2, null, 74.1, ...],   // flat array, len = len(latSet)*len(lonSet)
      "min": 68.1,                       // Fahrenheit
      "max": 79.3
    },
    ...
  }
}

The flat sst array is indexed by  latIdx * len(lonSet) + lonIdx.
null = cloud gap or satellite didn't cover that point this pass.
All SST values are in degrees Fahrenheit.

Run schedule
------------
  Run hourly (or every 3 hours) via cron / GitHub Actions.
  Each run regenerates today's bundle to pick up newly arriving passes.
  Bundles older than KEEP_DAYS are deleted to keep repo size under control.

Usage
-----
  python VIIRSHourlyBundler.py                   # today
  TARGET_DATE_OVERRIDE=2026-05-17 python VIIRSHourlyBundler.py
  DAYS_BACK=3 python VIIRSHourlyBundler.py       # today + 2 prior days
"""

import datetime
import io
import json
import logging
import math
import os
import re
import warnings
from pathlib import Path

import numpy as np
import requests
import xarray as xr
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
BBOX = {
    "lat_min": 33.70,
    "lat_max": 39.00,
    "lon_min": -78.89,
    "lon_max": -72.21,
}

# How many calendar days to bundle (today + N-1 prior days)
DAYS_BACK = int(os.environ.get("DAYS_BACK", "5"))

# Keep this many days of bundle files; older ones are deleted
KEEP_DAYS = 7

# Target date override for back-filling
_date_override = os.environ.get("TARGET_DATE_OVERRIDE", "").strip()
TARGET_DATE = (
    datetime.date.fromisoformat(_date_override)
    if _date_override
    else datetime.date.today()
)

# Output directory — same folder as everything else
OUTPUT_DIR = Path(__file__).resolve().parent / "DailySST"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TIMEOUT = 60   # seconds per THREDDS/OPeNDAP request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# HTTP session with retry
# ─────────────────────────────────────────────────────────────────────────────
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

SESSION = _make_session()

# ─────────────────────────────────────────────────────────────────────────────
# THREDDS catalog fetch — one day at a time
# ─────────────────────────────────────────────────────────────────────────────
THREDDS_CATALOG = (
    "https://coastwatch.noaa.gov/thredds/catalog"
    "/gridN20VIIRSNRTL3UWW00/{year}/{doy:03d}/catalog.xml"
)
THREDDS_OPENDAP = (
    "https://coastwatch.noaa.gov/thredds/dodsC"
    "/gridN20VIIRSNRTL3UWW00/{year}/{doy:03d}/{nc_name}"
)


def _fetch_passes_for_date(date: datetime.date) -> list[tuple[int, np.ndarray, list, list]]:
    """
    Fetch all available VIIRS hourly passes for *date* from THREDDS.

    Returns a list of (hour_utc, sst_fahrenheit_2d, lats, lons) tuples.
    sst_fahrenheit_2d is a 2-D numpy array (lats × lons) with NaN for gaps.
    """
    doy  = date.timetuple().tm_yday
    year = date.year

    catalog_url = THREDDS_CATALOG.format(year=year, doy=doy)
    try:
        resp = SESSION.get(catalog_url, timeout=30)
        resp.raise_for_status()
        matches = re.findall(
            r"gridN20VIIRSSCIENCEL3UWW00/[^\"]+\.nc", resp.text
        )
        if not matches:
            log.info("  No .nc files in THREDDS catalog for %s (DOY %03d)", date, doy)
            return []
        log.info("  %s: found %d pass(es) in THREDDS catalog", date, len(matches))
    except Exception as exc:
        log.warning("  THREDDS catalog unavailable for %s: %s", date, exc)
        return []

    results = []
    for nc_path_match in sorted(matches):
        nc_name    = nc_path_match.split("/")[-1]
        opendap    = THREDDS_OPENDAP.format(year=year, doy=doy, nc_name=nc_name)
        hour_match = re.search(r"(\d{8})(\d{2})\d{4}", nc_name)
        hour       = int(hour_match.group(2)) if hour_match else 0

        try:
            ds = xr.open_dataset(opendap, engine="netcdf4")

            # Find lat / lon coordinate names
            lat_name = next((c for c in ds.coords if "lat" in c.lower()), None)
            lon_name = next((c for c in ds.coords if "lon" in c.lower()), None)
            if not lat_name or not lon_name:
                log.warning("    %02d:00Z — no lat/lon coords, skipping", hour)
                ds.close()
                continue

            # Subset to bbox
            ds = ds.sel({
                lat_name: slice(BBOX["lat_min"], BBOX["lat_max"]),
                lon_name: slice(BBOX["lon_min"], BBOX["lon_max"]),
            })

            # Find SST variable
            sst_var = next(
                (v for v in ds.data_vars if "sst" in v.lower()), None
            )
            if sst_var is None:
                log.warning("    %02d:00Z — no SST variable, skipping", hour)
                ds.close()
                continue

            da = ds[sst_var].squeeze()
            # Drop any size-1 dims that aren't lat/lon
            for dim in list(da.dims):
                if dim not in (lat_name, lon_name) and da.sizes[dim] == 1:
                    da = da.isel({dim: 0})

            lats = da[lat_name].values.tolist()
            lons = da[lon_name].values.tolist()
            vals = da.values.astype(float)  # (n_lats, n_lons)

            # Convert Kelvin → Celsius → Fahrenheit if needed
            finite = vals[np.isfinite(vals)]
            if len(finite) and finite.mean() > 200:
                vals = vals - 273.15  # K → C
            vals_f = vals * 9.0 / 5.0 + 32.0  # C → F

            valid = np.sum(np.isfinite(vals_f))
            if valid == 0:
                log.info("    %02d:00Z — 0 valid SST pixels (full cloud cover), skipping", hour)
                ds.close()
                continue

            log.info(
                "    %02d:00Z — %d valid pixels  %.1f–%.1f °F",
                hour, valid,
                float(np.nanmin(vals_f)), float(np.nanmax(vals_f)),
            )
            results.append((hour, vals_f, lats, lons))
            ds.close()

        except Exception as exc:
            log.warning("    %02d:00Z — error opening %s: %s", hour, nc_name, exc)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Bundle builder
# ─────────────────────────────────────────────────────────────────────────────
def _build_bundle(date: datetime.date,
                  passes: list[tuple[int, np.ndarray, list, list]]) -> dict:
    """
    Given a list of (hour, sst_2d, lats, lons) from one day, build the compact
    bundle dict.  latSet / lonSet are the union of all pass grids, rounded to
    4 decimal places.  Each hour's sst is a flat array aligned to that shared
    grid.
    """
    # Collect unique lats and lons across all passes (rounded for key consistency)
    all_lats: set[float] = set()
    all_lons: set[float] = set()
    for _, _, lats, lons in passes:
        all_lats.update(round(v, 4) for v in lats)
        all_lons.update(round(v, 4) for v in lons)

    lat_set = sorted(all_lats)          # ascending
    lon_set = sorted(all_lons)          # ascending
    lat_idx = {v: i for i, v in enumerate(lat_set)}
    lon_idx = {v: i for i, v in enumerate(lon_set)}
    n_lats  = len(lat_set)
    n_lons  = len(lon_set)

    hours_dict: dict[str, dict] = {}
    available_hours: list[int] = []

    for hour, vals_f, lats, lons in passes:
        flat = [None] * (n_lats * n_lons)
        for ri, la in enumerate(lats):
            la_r = round(la, 4)
            gi = lat_idx.get(la_r)
            if gi is None:
                continue
            for ci, lo in enumerate(lons):
                lo_r = round(lo, 4)
                gj = lon_idx.get(lo_r)
                if gj is None:
                    continue
                v = vals_f[ri, ci]
                if math.isfinite(v):
                    flat[gi * n_lons + gj] = round(float(v), 2)

        valid_vals = [v for v in flat if v is not None]
        if not valid_vals:
            continue

        hours_dict[str(hour)] = {
            "sst": flat,
            "min": round(min(valid_vals), 1),
            "max": round(max(valid_vals), 1),
        }
        available_hours.append(hour)

    available_hours.sort()

    return {
        "date":            str(date),
        "generated":       datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latSet":          [round(v, 4) for v in lat_set],
        "lonSet":          [round(v, 4) for v in lon_set],
        "available_hours": available_hours,
        "hours":           hours_dict,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Write helpers
# ─────────────────────────────────────────────────────────────────────────────
def _write_bundle(date: datetime.date, bundle: dict) -> Path:
    dest = OUTPUT_DIR / f"viirs_{date}.json"
    tmp  = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(bundle, fh, separators=(",", ":"))
    tmp.rename(dest)
    size_kb = dest.stat().st_size / 1024
    log.info(
        "Wrote %s  (%d hours, %d×%d grid, %.0f KB)",
        dest.name,
        len(bundle["available_hours"]),
        len(bundle["latSet"]),
        len(bundle["lonSet"]),
        size_kb,
    )
    return dest


def _write_index(available_dates: list[str]) -> None:
    dest = OUTPUT_DIR / "viirs_index.json"
    tmp  = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "dates":     sorted(available_dates),
                "generated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            fh,
            separators=(",", ":"),
        )
    tmp.rename(dest)
    log.info("Wrote viirs_index.json  (%d dates)", len(available_dates))


def _purge_old_bundles(keep_days: int) -> None:
    cutoff = datetime.date.today() - datetime.timedelta(days=keep_days)
    for path in OUTPUT_DIR.glob("viirs_????-??-??.json"):
        try:
            file_date = datetime.date.fromisoformat(path.stem.replace("viirs_", ""))
        except ValueError:
            continue
        if file_date < cutoff:
            path.unlink()
            log.info("Purged old bundle: %s", path.name)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    log.info("=== VIIRSHourlyBundler  target=%s  days_back=%d ===", TARGET_DATE, DAYS_BACK)

    dates_to_process = [
        TARGET_DATE - datetime.timedelta(days=i)
        for i in range(DAYS_BACK)
    ]

    written_dates: list[str] = []

    for date in dates_to_process:
        log.info("--- Processing %s ---", date)
        passes = _fetch_passes_for_date(date)

        if not passes:
            log.warning("  No passes retrieved for %s — skipping bundle", date)
            # Keep existing bundle if it exists (from a prior run)
            if (OUTPUT_DIR / f"viirs_{date}.json").exists():
                written_dates.append(str(date))
            continue

        bundle = _build_bundle(date, passes)

        if not bundle["available_hours"]:
            log.warning("  Bundle for %s has no valid hours — skipping", date)
            continue

        _write_bundle(date, bundle)
        written_dates.append(str(date))

    # Collect ALL existing bundles (including ones from prior runs not re-fetched today)
    all_dates = sorted({
        path.stem.replace("viirs_", "")
        for path in OUTPUT_DIR.glob("viirs_????-??-??.json")
    })
    _write_index(all_dates)

    _purge_old_bundles(KEEP_DAYS)

    log.info("=== Done.  %d bundle(s) written ===", len(written_dates))


if __name__ == "__main__":
    main()
