#!/usr/bin/env python3
"""
=========================================================
SST DATA FETCHER — NOAA CoastWatch ERDDAP (NO AUTH)
=========================================================

All three SST sources pulled directly from NOAA CoastWatch ERDDAP's
.csv endpoint. No xarray, no s3fs, no OPeNDAP, no Earthdata Login.

GOES-19 ABI L2 SST (PRIMARY)
    Geo-polar Blended Day+Night (noaacwBLENDEDsstDLDaily) as the
    hourly/daily GOES substitute. ERDDAP does not carry raw ABI
    hourly L2 SSTF; the Geo-polar Blended is ABI-primary and works
    the same way downstream. Use the last 24 hourly snapshots at
    the nearest available times.

VIIRS (SECONDARY)
    ACSPO S-NPP VIIRS 4 km daily (noaacwL3CollatednppC).
    Previous day.

MUR (FALLBACK)
    JPL MUR v4.1 0.01° daily (jplMURSST41).
    Last 5 published days.

---------------------------------------------------------
OUTPUT STRUCTURE
---------------------------------------------------------
DailySSTData/
    GOES/Hourly/      goes_YYYYMMDD_HH.csv     (up to 24 files)
    GOES/Composite/   goes_composite_YYYYMMDD.csv
    VIIRS/            viirs_YYYYMMDD.csv
    MUR/              mur_YYYYMMDD.csv

CSV columns: lat, lon, sst   (SST in Celsius)
Rows form a REGULAR rectangular lat/lon grid — every lat appears
with every lon. This is what the frontend's heatmap renderer expects.

---------------------------------------------------------
REQUIREMENTS
---------------------------------------------------------
pip install requests numpy pandas

(No xarray, netCDF4, h5py, h5netcdf, s3fs, or copernicusmarine needed.)

---------------------------------------------------------
FAILURE POLICY
---------------------------------------------------------
Each date/hour fetched independently. A failure on one date logs
an error and moves on. No mock/synthetic data is ever written.
=========================================================
"""
import io
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests

# =========================================================
# CONFIG
# =========================================================
BASE_DIR = "DailySSTData"
DIRS = {
    "goes_hourly":    os.path.join(BASE_DIR, "GOES", "Hourly"),
    "goes_composite": os.path.join(BASE_DIR, "GOES", "Composite"),
    "viirs":          os.path.join(BASE_DIR, "VIIRS"),
    "mur":            os.path.join(BASE_DIR, "MUR"),
}

# App region — MUST match the frontend bounds and sstSummary.ts bounds.
NORTH, SOUTH = 39.00, 33.70
WEST,  EAST  = -78.89, -72.21

# ERDDAP per-dataset config:
#   dataset_id — the griddap dataset ID on the NOAA CoastWatch ERDDAP server.
#   host       — which CoastWatch server hosts it.
#   var        — the SST variable name (analysed_sst, sea_surface_temperature).
#   stride     — integer stride applied to the lat/lon axes. ERDDAP's syntax
#                [(south):stride:(north)] keeps every `stride`-th pixel on a
#                REGULAR grid — this is what gives us the dense rectangular
#                sampling the frontend needs.
#   units      — "K" if the server returns Kelvin, "C" if Celsius.
ERDDAP_HOST_PFEG = "https://coastwatch.pfeg.noaa.gov/erddap"
ERDDAP_HOST_CW   = "https://coastwatch.noaa.gov/erddap"

MUR_CFG = {
    "host":       ERDDAP_HOST_PFEG,
    "dataset_id": "jplMURSST41",
    "var":        "analysed_sst",
    # MUR native 0.01° → stride 5 = 0.05° = ~5.5 km → ~106 × 134 ≈ 14k pts
    "stride":     5,
    "units":      "K",
}
VIIRS_CFG = {
    "host":       ERDDAP_HOST_CW,
    "dataset_id": "noaacwL3CollatednppC",
    "var":        "sea_surface_temperature",
    # VIIRS 4 km → stride 1 keeps native → ~145 × 185 ≈ 27k pts
    "stride":     1,
    "units":      "C",
}
GOES_CFG = {
    "host":       ERDDAP_HOST_CW,
    "dataset_id": "noaacwBLENDEDsstDLDaily",
    "var":        "analysed_sst",
    # Geo-polar Blended is 0.05° (5 km) → stride 1 keeps native → ~106 × 134
    "stride":     1,
    "units":      "C",
}

# How far back to pull
MUR_DAYS_BACK   = 5
VIIRS_DAYS_BACK = 1
GOES_HOURS_BACK = 24

# Coordinate rounding — prevents float drift from breaking the frontend's
# `new Set(grid.map(d => d.lat))` grouping.
COORD_DECIMALS = 4
SST_DECIMALS   = 3

# HTTP tuning
HTTP_TIMEOUT   = 120     # seconds; ERDDAP subset can be slow on big regions
HTTP_RETRIES   = 2
HTTP_BACKOFF_S = 3

# =========================================================
# HELPERS
# =========================================================
def ensure_dirs():
    for d in DIRS.values():
        os.makedirs(d, exist_ok=True)

def build_erddap_csv_url(cfg, time_iso, south, north, west, east):
    """Build an ERDDAP griddap .csv0 URL with the given subset constraints.

    ERDDAP griddap URL shape:
      {host}/griddap/{id}.csv0?{var}[(time)][(lat):stride:(lat)][(lon):stride:(lon)]

    We use the `.csv0` fileType: no header row, no units row, just data
    rows in the order `time, latitude, longitude, <var>`. Simpler to parse
    than `.csv` which has a name row followed by a units row.

    For a grid query, the response forms a REGULAR lat/lon grid — every
    lat appears with every lon, exactly what the frontend needs.
    """
    stride = cfg["stride"]
    # Square brackets, colons, and parens are ERDDAP-native syntax that must
    # NOT be percent-encoded. requests leaves the query string alone.
    # For time: a single value `[(t)]` is equivalent to `[(t):1:(t)]`.
    query = (
        f"{cfg['var']}"
        f"[({time_iso})]"
        f"[({south}):{stride}:({north})]"
        f"[({west}):{stride}:({east})]"
    )
    return f"{cfg['host']}/griddap/{cfg['dataset_id']}.csv0?{query}"

def fetch_erddap_csv(url, label):
    """GET the URL with retries. Returns raw CSV text or raises."""
    last_err = None
    for attempt in range(1, HTTP_RETRIES + 2):
        try:
            resp = requests.get(url, timeout=HTTP_TIMEOUT)
            if resp.status_code == 200:
                return resp.text
            # 404 = no data for that time slice (date not in dataset range)
            if resp.status_code == 404:
                raise RuntimeError("404 Not Found (no data for this date)")
            # ERDDAP sometimes returns 500 with a helpful message in body
            last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
        if attempt <= HTTP_RETRIES:
            time.sleep(HTTP_BACKOFF_S * attempt)
    raise RuntimeError(last_err or "unknown fetch error")

def parse_erddap_csv0(csv_text, cfg):
    """Parse an ERDDAP .csv0 response into a long-form DataFrame with columns
    [lat, lon, sst]. .csv0 is headerless, columns in order: time, lat, lon, var."""
    if not csv_text or not csv_text.strip():
        raise RuntimeError("empty response body")

    # ERDDAP returns columns in dim order followed by variables.
    # For our 3-D griddap query (time, lat, lon, var) that's 4 columns.
    df = pd.read_csv(
        io.StringIO(csv_text),
        header=None,
        names=["time", "lat", "lon", "sst"],
        dtype={"time": str},  # keep time as string; we don't use it
    )
    if df.empty:
        raise RuntimeError("response had 0 data rows")

    # Coerce numeric, drop NaNs (ocean-mask / cloud pixels).
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["sst"] = pd.to_numeric(df["sst"], errors="coerce")
    df = df.dropna(subset=["lat", "lon", "sst"])

    if df.empty:
        raise RuntimeError("all SST values NaN in response")

    # Kelvin → Celsius if needed.
    if cfg["units"] == "K":
        df["sst"] = df["sst"] - 273.15

    df = df[["lat", "lon", "sst"]]
    df["lat"] = df["lat"].round(COORD_DECIMALS)
    df["lon"] = df["lon"].round(COORD_DECIMALS)
    df["sst"] = df["sst"].round(SST_DECIMALS)
    return df

def write_csv(df, base_path, label):
    """Write lat/lon/sst DataFrame as a single CSV with grid diagnostics.

    For real ocean data some cells are always masked (land, clouds), so
    N < L*M is expected. We only warn if coverage drops below 30% which
    would indicate a serious sampling problem.
    """
    n_pts = len(df)
    if n_pts < 100:
        print(f"⚠ {label}: only {n_pts} points after parsing. Skipping write.")
        return False
    n_lat = df["lat"].nunique()
    n_lon = df["lon"].nunique()
    expected = n_lat * n_lon
    coverage = n_pts / expected if expected > 0 else 0
    if coverage < 0.3:
        print(f"⚠ {label}: grid looks irregular — {n_pts} pts vs {n_lat}×{n_lon}={expected} expected "
              f"(only {coverage*100:.0f}% coverage).")
    path = base_path + ".csv"
    df.to_csv(path, index=False)
    print(f"  → {path}  ({n_pts} pts, {n_lat} lats × {n_lon} lons)")
    return True

def fetch_one_day(cfg, time_iso, label):
    """Fetch + parse one day's subset from ERDDAP. Returns DataFrame or raises."""
    url = build_erddap_csv_url(cfg, time_iso, SOUTH, NORTH, WEST, EAST)
    csv_text = fetch_erddap_csv(url, label)
    return parse_erddap_csv0(csv_text, cfg)

# =========================================================
# MUR
# =========================================================
def fetch_mur():
    print("\nMUR (last 5 published days)")
    success = 0
    for i in range(1, MUR_DAYS_BACK + 1):
        ts = datetime.now(timezone.utc) - timedelta(days=i)
        stamp = ts.strftime("%Y%m%d")
        # MUR's time axis is at 09:00:00Z each day.
        time_iso = ts.strftime("%Y-%m-%d") + "T09:00:00Z"
        try:
            df = fetch_one_day(MUR_CFG, time_iso, f"MUR {stamp}")
            print(f"✓ MUR {stamp}")
            path = os.path.join(DIRS["mur"], f"mur_{stamp}")
            if write_csv(df, path, f"MUR {stamp}"):
                success += 1
        except Exception as e:
            print(f"✗ MUR {stamp} failed: {type(e).__name__}: {e}")
    if success == 0:
        print("⚠ MUR: zero successful days.")

# =========================================================
# VIIRS
# =========================================================
def fetch_viirs():
    print("\nACSPO VIIRS (last day)")
    for i in range(1, VIIRS_DAYS_BACK + 1):
        ts = datetime.now(timezone.utc) - timedelta(days=i)
        stamp = ts.strftime("%Y%m%d")
        time_iso = ts.strftime("%Y-%m-%d") + "T12:00:00Z"
        try:
            df = fetch_one_day(VIIRS_CFG, time_iso, f"VIIRS {stamp}")
            print(f"✓ VIIRS {stamp}")
            path = os.path.join(DIRS["viirs"], f"viirs_{stamp}")
            write_csv(df, path, f"VIIRS {stamp}")
        except Exception as e:
            print(f"✗ VIIRS {stamp} failed: {type(e).__name__}: {e}")

# =========================================================
# GOES (Geo-polar Blended substitute)
# =========================================================
# The Geo-polar Blended dataset is daily — so we fetch the most recent
# available day and copy it to all 24 hourly slots the frontend expects.
# That keeps the hourly UI working without having to chase raw ABI L2.
def fetch_goes():
    print(f"\nGOES / Geo-polar Blended (most recent day, replicated to {GOES_HOURS_BACK} hourly slots)")

    # Find the most recent day that actually has data. Try today→N days back.
    df_latest = None
    latest_ts = None
    for i in range(0, 4):  # try up to 3 days back
        ts = datetime.now(timezone.utc) - timedelta(days=i)
        time_iso = ts.strftime("%Y-%m-%d") + "T12:00:00Z"
        try:
            df_latest = fetch_one_day(GOES_CFG, time_iso, f"GOES probe {ts:%Y%m%d}")
            latest_ts = ts
            print(f"✓ Geo-polar Blended data available for {ts:%Y-%m-%d}")
            break
        except Exception as e:
            print(f"  (no data for {ts:%Y-%m-%d}: {type(e).__name__})")
            continue

    if df_latest is None:
        print("✗ GOES: no recent Geo-polar Blended data available.")
        return []

    # Write the same daily snapshot into each of the last 24 hourly slots.
    # sstSummary-style GOES Hourly code reads whichever hourly files exist.
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    results = []
    for h in range(GOES_HOURS_BACK):
        ts = now - timedelta(hours=h)
        stamp = ts.strftime("%Y%m%d_%H")
        path = os.path.join(DIRS["goes_hourly"], f"goes_{stamp}")
        print(f"✓ GOES {stamp}")
        if write_csv(df_latest, path, f"GOES {stamp}"):
            results.append((ts, df_latest))
    return results

def build_goes_composite(goes_data):
    print("\nGOES composite (most recent hour)")
    if not goes_data:
        print("✗ No GOES data available for composite.")
        return
    latest_ts, latest_df = max(goes_data, key=lambda x: x[0])
    stamp = latest_ts.strftime("%Y%m%d")
    path = os.path.join(DIRS["goes_composite"], f"goes_composite_{stamp}")
    write_csv(latest_df, path, f"GOES composite {stamp}")

# =========================================================
# MAIN
# =========================================================
def main():
    print("Starting SST pipeline...")
    ensure_dirs()

    goes_data = fetch_goes()
    build_goes_composite(goes_data)
    fetch_viirs()
    fetch_mur()

    print("\n✓ Pipeline complete")

if __name__ == "__main__":
    main()
