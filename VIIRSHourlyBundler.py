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
  "latSet": [33.70, 33.72, ...],        // ascending, fixed 0.02° grid
  "lonSet": [-78.89, -78.87, ...],      // ascending, fixed 0.02° grid
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

# Fixed regular output grid — all bundles and the composite use exactly this
# grid so React's bilinear interpolation always gets a uniform lat/lon set.
GRID_STEP = 0.02   # degrees

def _make_fixed_grid():
    """Build the canonical lat/lon arrays for the full bbox at GRID_STEP."""
    lats, lons = [], []
    lat = BBOX["lat_min"]
    while lat <= BBOX["lat_max"] + 1e-9:
        lats.append(round(lat, 4))
        lat = round(lat + GRID_STEP, 4)
    lon = BBOX["lon_min"]
    while lon <= BBOX["lon_max"] + 1e-9:
        lons.append(round(lon, 4))
        lon = round(lon + GRID_STEP, 4)
    return lats, lons

FIXED_LATS, FIXED_LONS = _make_fixed_grid()
FIXED_LAT_IDX = {v: i for i, v in enumerate(FIXED_LATS)}
FIXED_LON_IDX = {v: i for i, v in enumerate(FIXED_LONS)}
N_LATS = len(FIXED_LATS)
N_LONS = len(FIXED_LONS)

# How many calendar days to bundle (today + N-1 prior days)
DAYS_BACK = int(os.environ.get("DAYS_BACK", "5"))

# Keep this many days of daily bundle files; older ones are deleted
KEEP_DAYS = 5

# Composite: look back this many hours across daily bundles
COMPOSITE_WINDOW_HOURS = int(os.environ.get("COMPOSITE_WINDOW_HOURS", "36"))

# Spatial coherence filter: minimum fraction of valid pixels within their own
# bounding box.  Clean passes score 0.45–0.75; fragmented edge-of-swath passes
# score 0.10–0.25.  Set to 0.0 to disable.
MIN_PASS_DENSITY = float(os.environ.get("MIN_PASS_DENSITY", "0.30"))

# Hard minimum valid-pixel count.  Passes with fewer pixels than this are
# discarded even if they pass the density check (e.g. 9-pixel edge-of-swath
# fragments that happen to be tightly clustered).
MIN_PASS_PIXELS = int(os.environ.get("MIN_PASS_PIXELS", "500"))

# Composite quality gates — if either threshold is not met the new composite is
# discarded and the existing viirs_composite.json is left untouched.
COMPOSITE_MIN_PASSES   = int(float(os.environ.get("COMPOSITE_MIN_PASSES",   "2")))
COMPOSITE_MIN_COVERAGE = float(os.environ.get("COMPOSITE_MIN_COVERAGE", "35.0"))

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
# Grid snapping helpers
# ─────────────────────────────────────────────────────────────────────────────
def _snap_to_fixed(raw_val: float, step: float, origin: float) -> float:
    """Snap a raw coordinate to the nearest fixed grid point."""
    return round(origin + round((raw_val - origin) / step) * step, 4)


def _fill_row_gaps(flat: list, n_lats: int, n_lons: int, max_gap: int = 2) -> list:
    """
    Vertically interpolate over small null gaps in each longitude column.

    VIIRS scan-line geometry leaves systematic 1–2 row gaps when raw pixels are
    snapped to a regular grid, producing visible horizontal stripes in the
    rendered composite.  This pass fills only short null runs (≤ max_gap rows)
    that are sandwiched between valid values, leaving real cloud holes intact.
    """
    result = flat[:]
    for lon_i in range(n_lons):
        i = 0
        while i < n_lats:
            if result[i * n_lons + lon_i] is None:
                # find end of null run
                j = i
                while j < n_lats and result[j * n_lons + lon_i] is None:
                    j += 1
                gap = j - i
                if gap <= max_gap and i > 0 and j < n_lats:
                    v0 = result[(i - 1) * n_lons + lon_i]
                    v1 = result[j       * n_lons + lon_i]
                    if v0 is not None and v1 is not None:
                        for k in range(gap):
                            t = (k + 1) / (gap + 1)
                            result[(i + k) * n_lons + lon_i] = round(v0 + t * (v1 - v0), 2)
                i = j
            else:
                i += 1
    return result


def _pass_to_fixed_grid(vals_f: np.ndarray,
                        raw_lats: list, raw_lons: list) -> list:
    """
    Resample a raw pass (irregular or fine-grained lat/lon) onto FIXED_LATS/FIXED_LONS
    using nearest-neighbour assignment.  Each raw pixel votes for the nearest
    fixed grid cell; the last writer wins (fine for ~0.02° source data).

    A vertical gap-fill pass is applied afterward to eliminate horizontal stripe
    artifacts caused by VIIRS scan-line geometry.

    Returns a flat list of length N_LATS * N_LONS with float or None values.
    """
    flat = [None] * (N_LATS * N_LONS)
    for ri, raw_lat in enumerate(raw_lats):
        snapped_lat = _snap_to_fixed(raw_lat, GRID_STEP, FIXED_LATS[0])
        gi = FIXED_LAT_IDX.get(snapped_lat)
        if gi is None:
            continue
        for ci, raw_lon in enumerate(raw_lons):
            snapped_lon = _snap_to_fixed(raw_lon, GRID_STEP, FIXED_LONS[0])
            gj = FIXED_LON_IDX.get(snapped_lon)
            if gj is None:
                continue
            v = vals_f[ri, ci]
            if math.isfinite(v):
                flat[gi * N_LONS + gj] = round(float(v), 2)

    return _fill_row_gaps(flat, N_LATS, N_LONS)


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
    sst_fahrenheit_2d is a 2-D numpy array (lats x lons) with NaN for gaps.
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

            lat_name = next((c for c in ds.coords if "lat" in c.lower()), None)
            lon_name = next((c for c in ds.coords if "lon" in c.lower()), None)
            if not lat_name or not lon_name:
                log.warning("    %02d:00Z — no lat/lon coords, skipping", hour)
                ds.close()
                continue

            lat_vals = ds[lat_name].values
            lat_asc  = float(lat_vals[0]) < float(lat_vals[-1])
            lat_sl   = (slice(BBOX["lat_min"], BBOX["lat_max"]) if lat_asc
                        else slice(BBOX["lat_max"], BBOX["lat_min"]))
            lon_vals = ds[lon_name].values
            lon_asc  = float(lon_vals[0]) < float(lon_vals[-1])
            lon_sl   = (slice(BBOX["lon_min"], BBOX["lon_max"]) if lon_asc
                        else slice(BBOX["lon_max"], BBOX["lon_min"]))
            ds = ds.sel({lat_name: lat_sl, lon_name: lon_sl})

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
            for dim in list(da.dims):
                if dim not in (lat_name, lon_name) and da.sizes[dim] == 1:
                    da = da.isel({dim: 0})

            if "quality_level" in ds.data_vars:
                ql = ds["quality_level"].squeeze()
                for dim in list(ql.dims):
                    if dim not in (lat_name, lon_name) and ql.sizes[dim] == 1:
                        ql = ql.isel({dim: 0})
                da = da.where(ql >= 4)

            lats = da[lat_name].values.tolist()
            lons = da[lon_name].values.tolist()
            vals = da.values.astype(float)

            finite = vals[np.isfinite(vals)]
            if len(finite) and finite.mean() > 200:
                vals = vals - 273.15
            vals_f = vals * 9.0 / 5.0 + 32.0

            valid = np.sum(np.isfinite(vals_f))
            if valid == 0:
                log.info("    %02d:00Z — 0 valid SST pixels (full cloud cover), skipping", hour)
                ds.close()
                continue

            # Spatial coherence filter
            valid_mask = np.isfinite(vals_f)
            rows_with_data = np.any(valid_mask, axis=1)
            cols_with_data = np.any(valid_mask, axis=0)
            if rows_with_data.any() and cols_with_data.any():
                r0 = int(np.where(rows_with_data)[0][0])
                r1 = int(np.where(rows_with_data)[0][-1])
                c0 = int(np.where(cols_with_data)[0][0])
                c1 = int(np.where(cols_with_data)[0][-1])
                bbox_pixels = (r1 - r0 + 1) * (c1 - c0 + 1)
                local_density = valid / bbox_pixels if bbox_pixels > 0 else 0.0
            else:
                local_density = 0.0

            if valid < MIN_PASS_PIXELS:
                log.info(
                    "    %02d:00Z — too few pixels (%d < %d minimum), skipping",
                    hour, valid, MIN_PASS_PIXELS,
                )
                ds.close()
                continue

            if local_density < MIN_PASS_DENSITY:
                log.info(
                    "    %02d:00Z — fragmented pass (%.1f%% local density < %.0f%% threshold), skipping",
                    hour, local_density * 100, MIN_PASS_DENSITY * 100,
                )
                ds.close()
                continue

            log.info(
                "    %02d:00Z — %d valid pixels  %.1f-%.1f F  (density %.0f%%)",
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
# Bundle builder — always outputs on the fixed canonical grid
# ─────────────────────────────────────────────────────────────────────────────
def _build_bundle(date: datetime.date,
                  passes: list[tuple[int, np.ndarray, list, list]]) -> dict:
    """
    Build the daily bundle using the fixed canonical grid (FIXED_LATS/FIXED_LONS).
    Every pass is resampled onto that grid via nearest-neighbour snapping (with
    stripe gap-fill) so the output latSet/lonSet is always uniform.
    """
    hours_dict: dict[str, dict] = {}
    available_hours: list[int] = []

    for hour, vals_f, lats, lons in passes:
        flat = _pass_to_fixed_grid(vals_f, lats, lons)
        valid_vals = [v for v in flat if v is not None]
        if not valid_vals:
            log.info("    %02d:00Z — no pixels landed on fixed grid, skipping", hour)
            continue

        hours_dict[str(hour)] = {
            "sst": flat,
            "min": round(min(valid_vals), 1),
            "max": round(max(valid_vals), 1),
        }
        available_hours.append(hour)
        log.info(
            "    %02d:00Z — %d/%d fixed grid cells filled  %.1f-%.1f F",
            hour, len(valid_vals), N_LATS * N_LONS,
            min(valid_vals), max(valid_vals),
        )

    available_hours.sort()

    return {
        "date":            str(date),
        "generated":       datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latSet":          FIXED_LATS,
        "lonSet":          FIXED_LONS,
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
        "Wrote %s  (%d hours, %d x %d grid, %.0f KB)",
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
    """
    Delete daily bundle files older than keep_days.
    viirs_index.json and viirs_composite.json are never deleted here.
    """
    cutoff = datetime.date.today() - datetime.timedelta(days=keep_days)
    purged = 0
    for path in OUTPUT_DIR.glob("viirs_????-??-??.json"):
        try:
            file_date = datetime.date.fromisoformat(path.stem.replace("viirs_", ""))
        except ValueError:
            continue
        if file_date < cutoff:
            path.unlink()
            log.info("Purged old bundle: %s", path.name)
            purged += 1
    if purged == 0:
        log.info("No old bundles to purge (keeping last %d days)", keep_days)


# ─────────────────────────────────────────────────────────────────────────────
# Temporal compositor — always outputs on the fixed canonical grid
# ─────────────────────────────────────────────────────────────────────────────
def build_composite(window_hours: int = COMPOSITE_WINDOW_HOURS) -> dict | None:
    """
    Read all existing daily bundle files whose passes fall within the last
    window_hours hours and produce a single composite on the fixed canonical grid.

    Strategy: gap-fill only (newest pass wins, older only fills empty cells).
    A vertical stripe gap-fill is applied to the final composite to eliminate
    any residual scan-line banding before writing.
    """
    now_utc = datetime.datetime.utcnow()
    cutoff  = now_utc - datetime.timedelta(hours=window_hours)

    bundle_paths = sorted(OUTPUT_DIR.glob("viirs_????-??-??.json"), reverse=True)
    if not bundle_paths:
        log.warning("[Compositor] No daily bundle files found in %s", OUTPUT_DIR)
        return None

    # flat composite arrays on the fixed grid
    sst_out = [None] * (N_LATS * N_LONS)
    age_out = [None] * (N_LATS * N_LONS)
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

        bundle_lat_set = bundle.get("latSet", [])
        bundle_lon_set = bundle.get("lonSet", [])
        bundle_n_lons  = len(bundle_lon_set)
        hours          = bundle.get("hours", {})

        for hr_str, hr_data in sorted(hours.items(), key=lambda x: int(x[0]), reverse=True):
            try:
                hr_int = int(hr_str)
            except ValueError:
                continue

            pass_dt = datetime.datetime(
                bundle_date.year, bundle_date.month, bundle_date.day, hr_int,
            )
            if pass_dt < cutoff:
                continue

            age_hours = (now_utc - pass_dt).total_seconds() / 3600.0
            sst_flat  = hr_data.get("sst", [])

            for idx, val in enumerate(sst_flat):
                if val is None:
                    continue
                lat_i = idx // bundle_n_lons
                lon_i = idx  % bundle_n_lons
                if lat_i >= len(bundle_lat_set) or lon_i >= len(bundle_lon_set):
                    continue
                raw_lat = bundle_lat_set[lat_i]
                raw_lon = bundle_lon_set[lon_i]
                snapped_lat = _snap_to_fixed(raw_lat, GRID_STEP, FIXED_LATS[0])
                snapped_lon = _snap_to_fixed(raw_lon, GRID_STEP, FIXED_LONS[0])
                gi = FIXED_LAT_IDX.get(snapped_lat)
                gj = FIXED_LON_IDX.get(snapped_lon)
                if gi is None or gj is None:
                    continue
                flat_i = gi * N_LONS + gj
                if sst_out[flat_i] is None:   # gap-fill only
                    sst_out[flat_i] = round(float(val), 2)
                    age_out[flat_i] = round(age_hours, 1)

            pass_count += 1

    # Fill residual scan-line stripe gaps in the composite
    sst_out = _fill_row_gaps(sst_out, N_LATS, N_LONS)

    valid_sst  = [v for v in sst_out if v is not None]
    valid_ages = [v for v in age_out if v is not None]

    if not valid_sst:
        log.warning("[Compositor] No valid pixels found within %d-hour window", window_hours)
        return None

    total        = N_LATS * N_LONS
    coverage_pct = round(len(valid_sst) / total * 100, 1)
    oldest_hours = round(max(valid_ages), 1) if valid_ages else None

    log.info(
        "[Compositor] %d passes merged | %d x %d fixed grid | %.1f%% coverage | oldest %.1f h",
        pass_count, N_LATS, N_LONS, coverage_pct, oldest_hours or 0,
    )

    return {
        "generated":        now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_hours":     window_hours,
        "latSet":           FIXED_LATS,
        "lonSet":           FIXED_LONS,
        "sst":              sst_out,
        "age":              age_out,
        "min":              round(min(valid_sst), 1),
        "max":              round(max(valid_sst), 1),
        "coverage_pct":     coverage_pct,
        "oldest_obs_hours": oldest_hours,
        "pass_count":       pass_count,
    }


def _write_composite_if_sufficient(composite: dict) -> bool:
    """
    Write viirs_composite.json only if the composite meets both quality gates:
      - pass_count   >= COMPOSITE_MIN_PASSES
      - coverage_pct >= COMPOSITE_MIN_COVERAGE
    If either gate fails the existing file is left untouched.
    Returns True if the file was written, False if it was skipped.
    """
    passes   = composite["pass_count"]
    coverage = composite["coverage_pct"]

    if passes < COMPOSITE_MIN_PASSES:
        log.warning(
            "[Compositor] Skipping write — only %d pass(es) merged, need >= %d. "
            "Keeping existing viirs_composite.json.",
            passes, COMPOSITE_MIN_PASSES,
        )
        return False

    if coverage < COMPOSITE_MIN_COVERAGE:
        log.warning(
            "[Compositor] Skipping write — coverage %.1f%% below %.1f%% threshold. "
            "Keeping existing viirs_composite.json.",
            coverage, COMPOSITE_MIN_COVERAGE,
        )
        return False

    dest = OUTPUT_DIR / "viirs_composite.json"
    tmp  = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(composite, fh, separators=(",", ":"))
    tmp.rename(dest)
    size_kb = dest.stat().st_size / 1024
    log.info(
        "Wrote viirs_composite.json  (%d passes | %.1f%% coverage | %d x %d grid | %.0f KB)",
        passes, coverage,
        len(composite["latSet"]), len(composite["lonSet"]),
        size_kb,
    )
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    log.info(
        "=== VIIRSHourlyBundler  target=%s  days_back=%d  keep=%d  grid=%dx%d ===",
        TARGET_DATE, DAYS_BACK, KEEP_DAYS, N_LATS, N_LONS,
    )

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

    # Collect ALL existing bundles (including ones from prior runs)
    all_dates = sorted({
        path.stem.replace("viirs_", "")
        for path in OUTPUT_DIR.glob("viirs_????-??-??.json")
    })
    _write_index(all_dates)

    # Purge daily bundles older than KEEP_DAYS
    _purge_old_bundles(KEEP_DAYS)

    # Build the temporal composite and write it only if quality gates pass.
    composite = build_composite(COMPOSITE_WINDOW_HOURS)
    if composite:
        written = _write_composite_if_sufficient(composite)
        if not written:
            existing = OUTPUT_DIR / "viirs_composite.json"
            if existing.exists():
                log.info(
                    "Composite quality gates not met — existing file retained (%s).",
                    existing.name,
                )
            else:
                log.warning(
                    "Composite quality gates not met and no existing file found. "
                    "UI will show no composite data until a sufficient composite is built."
                )
    else:
        log.warning("Composite could not be built — no data in window")

    log.info("=== Done.  %d bundle(s) written ===", len(written_dates))


if __name__ == "__main__":
    main()
