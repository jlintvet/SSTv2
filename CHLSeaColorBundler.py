"""
CHLSeaColorBundler.py
=====================
Reads existing daily CHL and SeaColor JSON files produced by
DailyChlorophyllandSeaColorRetrieval.py, bins them onto a fixed 0.02°
canonical grid (same as the SST VIIRS grid), and produces compact
flat-array bundle files.  Also builds multi-day composites using a
newest-pixel-wins strategy across a configurable window.

Run immediately after DailyChlorophyllandSeaColorRetrieval.py in the
same GitHub Actions job.

Output
------
SSTv2/Chlorophyll/Bundled/
  chl_bundle_index.json             — available dates + composite info
  chl_bundle_YYYY-MM-DD.json        — daily compact flat-array
  chl_composite.json                — multi-day gap-filled composite (latest)

SSTv2/SeaColor/Bundled/
  seacolor_bundle_index.json
  seacolor_bundle_YYYY-MM-DD.json
  seacolor_composite.json

Bundle format (chl_bundle_YYYY-MM-DD.json)
------------------------------------------
{
  "date":         "2026-06-15",
  "generated":    "2026-06-15T18:30:00Z",
  "latSet":       [33.70, 33.72, ...],   // ascending, 0.02° step
  "lonSet":       [-78.89, -78.87, ...], // ascending, 0.02° step
  "chl":          [null, 0.12, ...],     // flat, len = len(latSet)*len(lonSet)
  "min":          0.08,
  "max":          0.45,
  "coverage_pct": 10.5
}

Index: chl[i * len(lonSet) + j]  →  lat=latSet[i], lon=lonSet[j]
null = cloud gap.

Composite adds: "age" flat array (days since observation), "pass_count",
"window_days".  SeaColor uses "kd490" instead of "chl".
"""

import datetime
import json
import logging
import os
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
_REGION_CONFIGS = {
    "mid_atlantic": {"lat_min": 33.70, "lat_max": 39.00, "lon_min": -78.89, "lon_max": -72.21, "subdir": ""},
    "ga_sc":        {"lat_min": 29.80, "lat_max": 35.20, "lon_min": -82.00, "lon_max": -75.20, "subdir": "ga_sc"},
}
_REGION     = os.environ.get("REGION", "mid_atlantic")
_region_cfg = _REGION_CONFIGS[_REGION]
LAT_MIN, LAT_MAX = _region_cfg["lat_min"], _region_cfg["lat_max"]
LON_MIN, LON_MAX = _region_cfg["lon_min"], _region_cfg["lon_max"]
_SUBDIR = _region_cfg["subdir"]
GRID_STEP = 0.02  # degrees — matches SST VIIRS canonical grid

WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "5"))   # composite lookback
KEEP_DAYS   = int(os.environ.get("KEEP_DAYS",   "5"))   # bundle file retention

BASE_DIR = Path(__file__).resolve().parent
CHL_SRC  = BASE_DIR / "SSTv2" / "Chlorophyll" / _SUBDIR
SC_SRC   = BASE_DIR / "SSTv2" / "SeaColor" / _SUBDIR
CHL_OUT  = CHL_SRC / "Bundled"
SC_OUT   = SC_SRC  / "Bundled"
CHL_OUT.mkdir(parents=True, exist_ok=True)
SC_OUT.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Fixed canonical grid
# ─────────────────────────────────────────────────────────────────────────────
def _make_fixed_grid():
    lats, lons = [], []
    v = LAT_MIN
    while v <= LAT_MAX + 1e-9:
        lats.append(round(v, 4))
        v = round(v + GRID_STEP, 4)
    v = LON_MIN
    while v <= LON_MAX + 1e-9:
        lons.append(round(v, 4))
        v = round(v + GRID_STEP, 4)
    return lats, lons


FIXED_LATS, FIXED_LONS = _make_fixed_grid()
N_LATS = len(FIXED_LATS)
N_LONS = len(FIXED_LONS)
FIXED_LAT_IDX = {v: i for i, v in enumerate(FIXED_LATS)}
FIXED_LON_IDX = {v: i for i, v in enumerate(FIXED_LONS)}

log.info("Fixed grid: %d lats x %d lons = %d cells", N_LATS, N_LONS, N_LATS * N_LONS)


def _snap(val: float, origin: float) -> float:
    return round(origin + round((val - origin) / GRID_STEP) * GRID_STEP, 4)


# ─────────────────────────────────────────────────────────────────────────────
# CHL bundler
# ─────────────────────────────────────────────────────────────────────────────
def _bin_chl_rows(rows):
    buckets = {}
    for r in rows:
        raw_lat = r.get("lat")
        raw_lon = r.get("lon")
        chl     = r.get("chlorophyll")
        if raw_lat is None or raw_lon is None or chl is None:
            continue
        if not (LAT_MIN - 0.05 <= raw_lat <= LAT_MAX + 0.05):
            continue
        if not (LON_MIN - 0.05 <= raw_lon <= LON_MAX + 0.05):
            continue
        s_lat = _snap(raw_lat, LAT_MIN)
        s_lon = _snap(raw_lon, LON_MIN)
        gi = FIXED_LAT_IDX.get(s_lat)
        gj = FIXED_LON_IDX.get(s_lon)
        if gi is None or gj is None:
            continue
        idx = gi * N_LONS + gj
        buckets.setdefault(idx, []).append(float(chl))

    flat = [None] * (N_LATS * N_LONS)
    vals = []
    for idx, bucket in buckets.items():
        avg = sum(bucket) / len(bucket)
        flat[idx] = round(avg, 4)
        vals.append(avg)

    total        = N_LATS * N_LONS
    coverage_pct = round(len(vals) / total * 100, 2) if total else 0.0
    min_val      = round(min(vals), 4) if vals else None
    max_val      = round(max(vals), 4) if vals else None
    return flat, min_val, max_val, coverage_pct


def bundle_chl_day(src_path, date_str):
    log.info("  [CHL] Bundling %s from %s", date_str, src_path.name)
    try:
        with open(src_path, encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as exc:
        log.warning("  [CHL] Could not read %s: %s", src_path.name, exc)
        return None

    rows = data.get("rows", [])
    flat, min_val, max_val, coverage_pct = _bin_chl_rows(rows)
    valid = sum(1 for v in flat if v is not None)
    log.info("  [CHL] %s -> %d/%d cells (%.1f%%)  %.3f-%.3f",
             date_str, valid, N_LATS * N_LONS, coverage_pct, min_val or 0, max_val or 0)
    return {
        "date":         date_str,
        "generated":    datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latSet":       FIXED_LATS,
        "lonSet":       FIXED_LONS,
        "chl":          flat,
        "min":          min_val,
        "max":          max_val,
        "coverage_pct": coverage_pct,
    }


def build_chl_composite(bundle_dates):
    today    = datetime.date.today()
    cutoff   = today - datetime.timedelta(days=WINDOW_DAYS)
    eligible = sorted((d for d in bundle_dates if d >= str(cutoff)), reverse=True)
    if not eligible:
        log.warning("[CHL composite] No eligible dates in %d-day window", WINDOW_DAYS)
        return None

    flat_chl = [None] * (N_LATS * N_LONS)
    flat_age = [None] * (N_LATS * N_LONS)
    pass_count = 0

    for date_str in eligible:
        path = CHL_OUT / f"chl_bundle_{date_str}.json"
        if not path.exists():
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                bundle = json.load(fh)
        except Exception as exc:
            log.warning("[CHL composite] Could not read %s: %s", path.name, exc)
            continue

        chl_arr  = bundle.get("chl", [])
        age_days = (today - datetime.date.fromisoformat(date_str)).days

        filled = 0
        for i, val in enumerate(chl_arr):
            if val is not None and flat_chl[i] is None:
                flat_chl[i] = val
                flat_age[i] = float(age_days)
                filled += 1

        pass_count += 1
        log.info("[CHL composite] Merged %s (age %d days, +%d new cells)", date_str, age_days, filled)

    valid_vals = [v for v in flat_chl if v is not None]
    if not valid_vals:
        log.warning("[CHL composite] No valid pixels")
        return None

    total        = N_LATS * N_LONS
    coverage_pct = round(len(valid_vals) / total * 100, 2)
    log.info("[CHL composite] %d passes | %d/%d cells (%.1f%%)  %.3f-%.3f",
             pass_count, len(valid_vals), total, coverage_pct, min(valid_vals), max(valid_vals))
    return {
        "generated":    datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days":  WINDOW_DAYS,
        "latSet":       FIXED_LATS,
        "lonSet":       FIXED_LONS,
        "chl":          flat_chl,
        "age":          flat_age,
        "min":          round(min(valid_vals), 4),
        "max":          round(max(valid_vals), 4),
        "coverage_pct": coverage_pct,
        "pass_count":   pass_count,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SeaColor bundler
# ─────────────────────────────────────────────────────────────────────────────
def _bin_sc_rows(rows):
    # SeaColor native grid is ~0.1667 deg — bin each row to nearest 0.02 deg cell.
    # expandCoarseGrid in the frontend handles visual gap-filling for the coarse native grid.
    buckets = {}
    for r in rows:
        raw_lat = r.get("lat")
        raw_lon = r.get("lon")
        kd490   = r.get("kd490")
        if raw_lat is None or raw_lon is None or kd490 is None:
            continue
        if not (LAT_MIN - 0.25 <= raw_lat <= LAT_MAX + 0.25):
            continue
        if not (LON_MIN - 0.25 <= raw_lon <= LON_MAX + 0.25):
            continue
        s_lat = _snap(raw_lat, LAT_MIN)
        s_lon = _snap(raw_lon, LON_MIN)
        gi = FIXED_LAT_IDX.get(s_lat)
        gj = FIXED_LON_IDX.get(s_lon)
        if gi is None or gj is None:
            continue
        buckets.setdefault(gi * N_LONS + gj, []).append(float(kd490))

    flat = [None] * (N_LATS * N_LONS)
    vals = []
    for idx, bucket in buckets.items():
        avg = sum(bucket) / len(bucket)
        flat[idx] = round(avg, 4)
        vals.append(avg)

    total        = N_LATS * N_LONS
    coverage_pct = round(len(vals) / total * 100, 2) if total else 0.0
    min_val      = round(min(vals), 4) if vals else None
    max_val      = round(max(vals), 4) if vals else None
    return flat, min_val, max_val, coverage_pct


def bundle_sc_day(src_path, date_str):
    log.info("  [SC] Bundling %s from %s", date_str, src_path.name)
    try:
        with open(src_path, encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as exc:
        log.warning("  [SC] Could not read %s: %s", src_path.name, exc)
        return None

    rows = data.get("rows", [])
    flat, min_val, max_val, coverage_pct = _bin_sc_rows(rows)
    valid = sum(1 for v in flat if v is not None)
    log.info("  [SC] %s -> %d/%d cells (%.1f%%)  %.4f-%.4f",
             date_str, valid, N_LATS * N_LONS, coverage_pct, min_val or 0, max_val or 0)
    return {
        "date":         date_str,
        "generated":    datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latSet":       FIXED_LATS,
        "lonSet":       FIXED_LONS,
        "kd490":        flat,
        "min":          min_val,
        "max":          max_val,
        "coverage_pct": coverage_pct,
    }


def build_sc_composite(bundle_dates):
    today    = datetime.date.today()
    cutoff   = today - datetime.timedelta(days=WINDOW_DAYS)
    eligible = sorted((d for d in bundle_dates if d >= str(cutoff)), reverse=True)
    if not eligible:
        log.warning("[SC composite] No eligible dates in %d-day window", WINDOW_DAYS)
        return None

    flat_kd  = [None] * (N_LATS * N_LONS)
    flat_age = [None] * (N_LATS * N_LONS)
    pass_count = 0

    for date_str in eligible:
        path = SC_OUT / f"seacolor_bundle_{date_str}.json"
        if not path.exists():
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                bundle = json.load(fh)
        except Exception as exc:
            log.warning("[SC composite] Could not read %s: %s", path.name, exc)
            continue

        kd_arr   = bundle.get("kd490", [])
        age_days = (today - datetime.date.fromisoformat(date_str)).days

        filled = 0
        for i, val in enumerate(kd_arr):
            if val is not None and flat_kd[i] is None:
                flat_kd[i] = val
                flat_age[i] = float(age_days)
                filled += 1

        pass_count += 1
        log.info("[SC composite] Merged %s (age %d days, +%d new cells)", date_str, age_days, filled)

    valid_vals = [v for v in flat_kd if v is not None]
    if not valid_vals:
        log.warning("[SC composite] No valid pixels")
        return None

    total        = N_LATS * N_LONS
    coverage_pct = round(len(valid_vals) / total * 100, 2)
    log.info("[SC composite] %d passes | %d/%d cells (%.1f%%)  %.4f-%.4f",
             pass_count, len(valid_vals), total, coverage_pct, min(valid_vals), max(valid_vals))
    return {
        "generated":    datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days":  WINDOW_DAYS,
        "latSet":       FIXED_LATS,
        "lonSet":       FIXED_LONS,
        "kd490":        flat_kd,
        "age":          flat_age,
        "min":          round(min(valid_vals), 4),
        "max":          round(max(valid_vals), 4),
        "coverage_pct": coverage_pct,
        "pass_count":   pass_count,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Write helpers
# ─────────────────────────────────────────────────────────────────────────────
def _write_json(path, payload):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    tmp.rename(path)
    log.info("Wrote %s  (%.0f KB)", path.name, path.stat().st_size / 1024)


def _purge_old_bundles(output_dir, prefix, keep_days):
    cutoff = datetime.date.today() - datetime.timedelta(days=keep_days)
    for path in output_dir.glob(f"{prefix}_????-??-??.json"):
        stem = path.stem.replace(prefix + "_", "")
        try:
            if datetime.date.fromisoformat(stem) < cutoff:
                path.unlink()
                log.info("Purged old bundle: %s", path.name)
        except ValueError:
            pass


COMPOSITE_KEEP_DAYS = int(os.environ.get("COMPOSITE_KEEP_DAYS", "7"))


def _purge_old_composites(output_dir, prefix, keep_days):
    """Delete dated composite snapshots older than keep_days; canonical latest is never deleted."""
    cutoff = datetime.date.today() - datetime.timedelta(days=keep_days)
    for path in output_dir.glob(f"{prefix}_????-??-??.json"):
        stem = path.stem.replace(prefix + "_", "")
        try:
            if datetime.date.fromisoformat(stem) < cutoff:
                path.unlink()
                log.info("Purged old composite snapshot: %s", path.name)
        except ValueError:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info("=== CHLSeaColorBundler  grid=%dx%d  window=%d days ===",
             N_LATS, N_LONS, WINDOW_DAYS)

    # ── CHL ──────────────────────────────────────────────────────────────────
    log.info("--- CHL ---")
    chl_manifest_path = CHL_SRC / "chl_manifest.json"
    chl_bundle_dates = []

    if chl_manifest_path.exists():
        with open(chl_manifest_path, encoding="utf-8") as fh:
            chl_manifest = json.load(fh)
        for entry in chl_manifest.get("files", []):
            date_str  = entry["date"]
            src_file  = CHL_SRC / entry["filename"]
            dest_file = CHL_OUT / f"chl_bundle_{date_str}.json"
            if not src_file.exists():
                log.warning("  [CHL] Source file missing: %s", src_file.name)
                continue
            bundle = bundle_chl_day(src_file, date_str)
            if bundle is None:
                continue
            _write_json(dest_file, bundle)
            chl_bundle_dates.append(date_str)
    else:
        log.warning("[CHL] chl_manifest.json not found — skipping CHL bundling")

    _purge_old_bundles(CHL_OUT, "chl_bundle", KEEP_DAYS)

    chl_composite = build_chl_composite(chl_bundle_dates)
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    if chl_composite:
        _write_json(CHL_OUT / "chl_composite.json", chl_composite)
        _write_json(CHL_OUT / f"chl_composite_{today_str}.json", chl_composite)

    _purge_old_composites(CHL_OUT, "chl_composite", COMPOSITE_KEEP_DAYS)

    chl_composite_dates = sorted(
        path.stem.replace("chl_composite_", "")
        for path in CHL_OUT.glob("chl_composite_????-??-??.json")
    )

    _write_json(CHL_OUT / "chl_bundle_index.json", {
        "generated":              datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dates":                  sorted(chl_bundle_dates),
        "composite_dates":        chl_composite_dates,
        "has_composite":          chl_composite is not None,
        "composite_coverage_pct": chl_composite["coverage_pct"] if chl_composite else None,
    })

    # ── SeaColor ─────────────────────────────────────────────────────────────
    log.info("--- SeaColor ---")
    sc_manifest_path = SC_SRC / "seacolor_manifest.json"
    sc_bundle_dates = []

    if sc_manifest_path.exists():
        with open(sc_manifest_path, encoding="utf-8") as fh:
            sc_manifest = json.load(fh)
        for entry in sc_manifest.get("files", []):
            date_str  = entry["date"]
            src_file  = SC_SRC / entry["filename"]
            dest_file = SC_OUT / f"seacolor_bundle_{date_str}.json"
            if not src_file.exists():
                log.warning("  [SC] Source file missing: %s", src_file.name)
                continue
            bundle = bundle_sc_day(src_file, date_str)
            if bundle is None:
                continue
            _write_json(dest_file, bundle)
            sc_bundle_dates.append(date_str)
    else:
        log.warning("[SC] seacolor_manifest.json not found — skipping SeaColor bundling")

    _purge_old_bundles(SC_OUT, "seacolor_bundle", KEEP_DAYS)

    sc_composite = build_sc_composite(sc_bundle_dates)
    if sc_composite:
        _write_json(SC_OUT / "seacolor_composite.json", sc_composite)
        _write_json(SC_OUT / f"seacolor_composite_{today_str}.json", sc_composite)

    _purge_old_composites(SC_OUT, "seacolor_composite", COMPOSITE_KEEP_DAYS)

    sc_composite_dates = sorted(
        path.stem.replace("seacolor_composite_", "")
        for path in SC_OUT.glob("seacolor_composite_????-??-??.json")
    )

    _write_json(SC_OUT / "seacolor_bundle_index.json", {
        "generated":              datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dates":                  sorted(sc_bundle_dates),
        "composite_dates":        sc_composite_dates,
        "has_composite":          sc_composite is not None,
        "composite_coverage_pct": sc_composite["coverage_pct"] if sc_composite else None,
    })

    log.info("=== Done.  CHL: %d bundles | SC: %d bundles ===",
             len(chl_bundle_dates), len(sc_bundle_dates))


if __name__ == "__main__":
    main()
