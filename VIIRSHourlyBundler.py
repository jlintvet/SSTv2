"""
VIIRSHourlyBundler.py
=====================
Fetches NOAA VIIRS hourly SST passes from the CoastWatch THREDDS catalog and
writes one compact JSON bundle per day into DailySSTData/VIIRS/Bundled/.

Output files
------------
  viirs_YYYY-MM-DD.json   — one file per day, clean passes only
  viirs_index.json        — list of available dates (React reads this first)
  viirs_composite.json    — gap-fill composite: freshest pass wins per pixel

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
  MIN_PASS_DENSITY=0.20 python VIIRSHourlyBundler.py  # loosen quality filter
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

# Composite: look back this many hours across daily bundles
COMPOSITE_WINDOW_HOURS = int(os.environ.get("COMPOSITE_WINDOW_HOURS", "36"))

# Spatial coherence filter: minimum fraction of valid pixels within their own
# bounding box.  Clean passes score 0.45–0.75; fragmented edge-of-swath passes
# score 0.10–0.25.  Set to 0.0 to disable.
MIN_PASS_DENSITY = float(os.environ.get("MIN_PASS_DENSITY", "0.30"))

# Target date override for back-filling
_date_override = os.environ.get("TARGET_DATE_OVERRIDE", "").strip()
TARGET_DATE = (
    datetime.date.fromisoformat(_date_override)
    if _date_override
    else datetime.date.today()
)

# Output directory — matches GitHub repo path DailySSTData/VIIRS/Bundled/
OUTPUT_DIR = Path(__file__).resolve().parent / "DailySSTData" / "VIIRS" / "Bundled"
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
    Fragmented / edge-of-swath passes are rejected by the spatial coherence
    filter before being returned.

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
            r"gridN20VIIRSNRTL3UWW00/[^\"]+\.nc", resp.text
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

            # Subset to bbox — detect coordinate direction so slice order is correct.
            # ACSPO L3U stores lat descending (90 → -90); ascending is also possible.
            lat_vals = ds[lat_name].values
            lat_asc  = float(lat_vals[0]) < float(lat_vals[-1])
            lat_sl   = (slice(BBOX["lat_min"], BBOX["lat_max"]) if lat_asc
                        else slice(BBOX["lat_max"], BBOX["lat_min"]))
            lon_vals = ds[lon_name].values
            lon_asc  = float(lon_vals[0]) < float(lon_vals[-1])
            lon_sl   = (slice(BBOX["lon_min"], BBOX["lon_max"]) if lon_asc
                        else slice(BBOX["lon_max"], BBOX["lon_min"]))
            ds = ds.sel({lat_name: lat_sl, lon_name: lon_sl})

            # Find SST variable.
            # GHRSST L3U standard name is 'sea_surface_temperature'; fall back
            # to other known names.  Explicitly exclude 'sst_dtime' (time offset).
            SST_NAMES = (
                "sea_surface_temperature", "analysed_sst",
                "sst_subskin", "sst_skin", "sst",
            )
            sst_var = next((v for v in SST_NAMES if v in ds.data_vars), None)
            if sst_var is None:
                sst_var = next(
                    (v for v in ds.data_vars
                     if "sst" in v.lower()
                     and "dtime" not in v.lower()
                     and "flag"  not in v.lower()),
                    None,
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

            # Apply quality-level mask if present (ACSPO QL 0–5; keep ≥ 4)
            if "quality_level" in ds.data_vars:
                ql = ds["quality_level"].squeeze()
                for dim in list(ql.dims):
                    if dim not in (lat_name, lon_name) and ql.sizes[dim] == 1:
                        ql = ql.isel({dim: 0})
                da = da.where(ql >= 4)

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

            # ── Spatial coherence filter ──────────────────────────────────────
            # Compute local density: valid pixels ÷ bounding-box area of valid
            # data.  Clean passes are dense within their swath (0.45–0.75).
            # Fragmented edge-of-swath passes scatter isolated islands across the
            # full bbox → low density (0.10–0.25) → skip.
            valid_mask = np.isfinite(vals_f)
            rows_with_data = np.any(valid_mask, axis=1)
            cols_with_data = np.any(valid_mask, axis=0)
            if rows_with_data.any() and cols_with_data.any():
                r0, r1 = int(np.where(rows_with_data)[0][0]),  int(np.where(rows_with_data)[0][-1])
                c0, c1 = int(np.where(cols_with_data)[0][0]),  int(np.where(cols_with_data)[0][-1])
                bbox_pixels = (r1 - r0 + 1) * (c1 - c0 + 1)
                local_density = valid / bbox_pixels if bbox_pixels > 0 else 0.0
            else:
                local_density = 0.0

            if local_density < MIN_PASS_DENSITY:
                log.info(
                    "    %02d:00Z — fragmented pass (%.1f%% local density < %.0f%% threshold), skipping",
                    hour, local_density * 100, MIN_PASS_DENSITY * 100,
                )
                ds.close()
                continue
            # ─────────────────────────────────────────────────────────────────

            log.info(
                "    %02d:00Z — %d valid pixels  %.1f–%.1f °F  (density %.0f%%)",
                hour, valid,
                float(np.nanmin(vals_f)), float(np.nanmax(vals_f)),
                local_density * 100,
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
# Temporal compositor — gap-fill strategy
# ─────────────────────────────────────────────────────────────────────────────
def build_composite(window_hours: int = COMPOSITE_WINDOW_HOURS) -> dict | None:
    """
    Read all existing daily bundle files whose passes fall within the last
    *window_hours* hours and produce a single composite grid.

    Strategy: gap-fill only.
    Passes are processed newest-first.  Each pass claims any pixel it covers;
    older passes only fill pixels not yet claimed.  This preserves the sharp
    thermal front boundaries of the most recent clean swath while using older
    data only to fill genuine cloud gaps.

    Composite JSON format
    ---------------------
    {
      "generated":        "2026-05-20T14:00:00Z",
      "window_hours":     36,
      "latSet":           [...],
      "lonSet":           [...],
      "sst":              [...],   // flat, null where no coverage in window
      "age":              [...],   // parallel: hours since observation (null = no data)
      "min":              68.1,
      "max":              79.3,
      "coverage_pct":     74.2,    // % of bbox grid points with any data
      "oldest_obs_hours": 31.4,    // age of the oldest gap-fill pixel kept
      "pass_count":       7        // number of passes merged
    }
    """
    now_utc = datetime.datetime.utcnow()
    cutoff  = now_utc - datetime.timedelta(hours=window_hours)

    # Gather all bundle files, sorted newest → oldest so fresher passes fill first.
    bundle_paths = sorted(OUTPUT_DIR.glob("viirs_????-??-??.json"), reverse=True)
    if not bundle_paths:
        log.warning("[Compositor] No daily bundle files found in %s", OUTPUT_DIR)
        return None

    # Gap-fill composite: the freshest pass claims its pixels first; older passes
    # only fill cells that are still empty (cloud gaps in newer observations).
    # This preserves sharp thermal front boundaries from the most recent swath
    # instead of blurring them by mixing data from different times.
    composite_grid: dict[tuple[float, float], list] = {}
    pass_count = 0

    for bp in bundle_paths:
        try:
            date_str = bp.stem.replace("viirs_", "")
            bundle_date = datetime.date.fromisoformat(date_str)
        except ValueError:
            continue

        try:
            with open(bp, encoding="utf-8") as fh:
                bundle = json.load(fh)
        except Exception as exc:
            log.warning("[Compositor] Could not read %s: %s", bp.name, exc)
            continue

        lat_set = bundle.get("latSet", [])
        lon_set = bundle.get("lonSet", [])
        n_lons  = len(lon_set)
        hours   = bundle.get("hours", {})

        # Process hours newest-first within each day bundle
        for hr_str, hr_data in sorted(hours.items(), key=lambda x: int(x[0]), reverse=True):
            try:
                hr_int = int(hr_str)
            except ValueError:
                continue

            # Reconstruct UTC datetime for this pass
            pass_dt = datetime.datetime(
                bundle_date.year, bundle_date.month, bundle_date.day, hr_int,
                tzinfo=None  # all UTC
            )
            if pass_dt < cutoff:
                continue  # outside the compositing window

            age_hours = (now_utc - pass_dt).total_seconds() / 3600.0
            sst_flat  = hr_data.get("sst", [])

            for idx, val in enumerate(sst_flat):
                if val is None:
                    continue
                lat_i = idx // n_lons
                lon_i = idx  % n_lons
                if lat_i >= len(lat_set) or lon_i >= len(lon_set):
                    continue
                key = (round(lat_set[lat_i], 4), round(lon_set[lon_i], 4))
                # Gap-fill only: never overwrite a pixel already claimed by a
                # fresher pass — older data only fills genuinely empty cells.
                if key not in composite_grid:
                    composite_grid[key] = [round(float(val), 2), age_hours]

            pass_count += 1

    if not composite_grid:
        log.warning("[Compositor] No valid pixels found within %d-hour window", window_hours)
        return None

    # Build union lat/lon sets from composite keys
    all_lats = sorted({k[0] for k in composite_grid})
    all_lons = sorted({k[1] for k in composite_grid})
    lat_idx  = {v: i for i, v in enumerate(all_lats)}
    lon_idx  = {v: i for i, v in enumerate(all_lons)}
    n_lats   = len(all_lats)
    n_lons_c = len(all_lons)
    total    = n_lats * n_lons_c

    sst_out = [None] * total
    age_out = [None] * total

    for (lat_r, lon_r), (sst_v, age_v) in composite_grid.items():
        gi = lat_idx[lat_r]
        gj = lon_idx[lon_r]
        flat_i = gi * n_lons_c + gj
        sst_out[flat_i] = sst_v
        age_out[flat_i] = round(age_v, 1)

    valid_sst  = [v for v in sst_out if v is not None]
    valid_ages = [v for v in age_out if v is not None]

    coverage_pct     = round(len(valid_sst) / total * 100, 1) if total else 0.0
    oldest_obs_hours = round(max(valid_ages), 1) if valid_ages else None

    log.info(
        "[Compositor] %d passes merged | %d×%d grid | %.1f%% coverage | oldest %.1f h",
        pass_count, n_lats, n_lons_c, coverage_pct, oldest_obs_hours or 0,
    )

    return {
        "generated":        now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_hours":     window_hours,
        "latSet":           [round(v, 4) for v in all_lats],
        "lonSet":           [round(v, 4) for v in all_lons],
        "sst":              sst_out,
        "age":              age_out,
        "min":              round(min(valid_sst), 1) if valid_sst else None,
        "max":              round(max(valid_sst), 1) if valid_sst else None,
        "coverage_pct":     coverage_pct,
        "oldest_obs_hours": oldest_obs_hours,
        "pass_count":       pass_count,
    }


def _write_composite(composite: dict) -> None:
    dest = OUTPUT_DIR / "viirs_composite.json"
    tmp  = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(composite, fh, separators=(",", ":"))
    tmp.rename(dest)
    size_kb = dest.stat().st_size / 1024
    log.info(
        "Wrote viirs_composite.json  (%d passes | %.1f%% coverage | %.0f KB)",
        composite["pass_count"], composite["coverage_pct"], size_kb,
    )


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

    # Build and write the temporal composite
    composite = build_composite(COMPOSITE_WINDOW_HOURS)
    if composite:
        _write_composite(composite)
    else:
        log.warning("Composite could not be built — no data in window")

    log.info("=== Done.  %d bundle(s) written ===", len(written_dates))


if __name__ == "__main__":
    main()
