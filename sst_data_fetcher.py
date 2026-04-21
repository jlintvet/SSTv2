#!/usr/bin/env python3
"""
=========================================================
SST DATA FETCHER — NOAA COASTWATCH ERDDAP (NO AUTH)
=========================================================

Three SST sources, all fetched from NOAA CoastWatch ERDDAP as CSV.
No credentials required. No OPeNDAP. No S3. No NetCDF parsing.

MUR     — jplMURSST41 (NASA JPL, 0.01°, daily)
           served by CoastWatch with analysed_sst in degree_C
VIIRS   — noaacwLEOACSPOSSTL3SnrtCDaily (ACSPO AVHRR+VIIRS, 0.02°, daily)
           sea_surface_temperature in degree_C
BLENDED — noaacwBLENDEDsstDLDaily (Geo-polar Blended Day+Night, 0.05°, daily)
           analysed_sst in degree_C
           used as the "GOES composite" for the frontend

---------------------------------------------------------
OUTPUT STRUCTURE
---------------------------------------------------------
DailySSTData/
    GOES/Hourly/        goes_YYYYMMDD_12.csv (one per day at 12Z)
    GOES/Composite/     goes_composite_YYYYMMDD.csv
    VIIRS/              viirs_YYYYMMDD.csv
    MUR/                mur_YYYYMMDD.csv

Each CSV has columns: lat, lon, sst  (SST in Celsius, decimal degrees).
Rows form a REGULAR rectangular lat/lon grid — every lat paired with every lon.

---------------------------------------------------------
REQUIREMENTS
---------------------------------------------------------
pip install numpy pandas requests

(No xarray, netcdf4, s3fs, or h5netcdf needed.)

---------------------------------------------------------
FAILURE POLICY
---------------------------------------------------------
Each day is fetched independently. A failure on one date logs an error and
moves on. If a source has zero successes, logs that fact but does not raise.
No mock/synthetic data is ever written.
=========================================================
"""
import io
import os
import sys
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

# App region — MUST match frontend bounds and sstSummary.ts bounds.
NORTH, SOUTH = 39.00, 33.70
WEST,  EAST  = -78.89, -72.21

# Stride = how many native grid cells to skip between samples.
# MUR native 0.01°     → stride 5 → 0.05°  → ~107 × 134 = ~14k pts
# VIIRS native 0.02°   → stride 3 → 0.06°  → ~90 × 112  = ~10k pts
# Blended native 0.05° → stride 1 → 0.05°  → ~106 × 134 = ~14k pts
MUR_STRIDE      = 5
VIIRS_STRIDE    = 3
BLENDED_STRIDE  = 1

MUR_DAYS_BACK     = 5
VIIRS_DAYS_BACK   = 5
BLENDED_DAYS_BACK = 5

# Coordinate precision — prevents float drift between rows that should share
# the same lat or lon value. 0.0001° ≈ 11 m.
COORD_DECIMALS = 4
SST_DECIMALS   = 3

# HTTP
HTTP_TIMEOUT = 120
USER_AGENT   = "jlintvet-SSTv2-ingest/1.0"

# =========================================================
# SETUP
# =========================================================
def ensure_dirs():
    for d in DIRS.values():
        os.makedirs(d, exist_ok=True)

# =========================================================
# OUTPUT
# =========================================================
def write_csv(df, base_path, label):
    """Write lat/lon/sst DataFrame as a single CSV.
    Logs grid diagnostics so you can see at a glance whether data is regular."""
    n_lat = df["lat"].nunique()
    n_lon = df["lon"].nunique()
    n_pts = len(df)
    expected = n_lat * n_lon
    if n_pts < 100:
        print(f"⚠ {label}: only {n_pts} points. Skipping write.")
        return False
    if n_pts > expected * 1.05 or n_pts < expected * 0.3:
        print(f"⚠ {label}: grid looks irregular — {n_pts} pts vs {n_lat}×{n_lon}={expected} expected cells.")
    path = base_path + ".csv"
    df.to_csv(path, index=False)
    print(f"  → {path}  ({n_pts} pts, {n_lat} lats × {n_lon} lons)")
    return True

# =========================================================
# GENERIC ERDDAP FETCHER
# =========================================================
def erddap_fetch_csv(base_url, dataset, variable, time_iso, stride, label,
                    lat_name="latitude", lon_name="longitude"):
    """Fetch lat/lon/sst from an ERDDAP griddap endpoint as CSV.

    ERDDAP .csv returns 2 header rows (column names, then units), then data.
    We read everything, skip row 1 (units), keep the numeric rows.
    """
    # griddap query syntax: var[(time)][(lat_start):stride:(lat_end)][(lon_start):stride:(lon_end)]
    # Brackets encoded as %5B / %5D.
    qs = (
        f"{variable}"
        f"%5B({time_iso})%5D"
        f"%5B({SOUTH}):{stride}:({NORTH})%5D"
        f"%5B({WEST}):{stride}:({EAST})%5D"
    )
    url = f"{base_url}/{dataset}.csv?{qs}"

    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"},
            timeout=HTTP_TIMEOUT,
        )
    except requests.RequestException as e:
        print(f"✗ {label} request failed: {type(e).__name__}: {e}")
        return None

    if resp.status_code != 200:
        body = resp.text[:300].replace("\n", " ")
        print(f"✗ {label} HTTP {resp.status_code}: {body}")
        return None

    try:
        df = pd.read_csv(io.StringIO(resp.text), skiprows=[1])
    except Exception as e:
        print(f"✗ {label} CSV parse failed: {type(e).__name__}: {e}")
        return None

    rename = {}
    if lat_name in df.columns: rename[lat_name] = "lat"
    if lon_name in df.columns: rename[lon_name] = "lon"
    if variable in df.columns: rename[variable] = "sst"
    df = df.rename(columns=rename)

    if not {"lat", "lon", "sst"}.issubset(df.columns):
        print(f"✗ {label} unexpected columns: {list(df.columns)}")
        return None

    df = df[["lat", "lon", "sst"]].apply(pd.to_numeric, errors="coerce").dropna()

    if df.empty:
        print(f"✗ {label} returned no valid data after parsing")
        return None

    df["lat"] = df["lat"].round(COORD_DECIMALS)
    df["lon"] = df["lon"].round(COORD_DECIMALS)
    df["sst"] = df["sst"].round(SST_DECIMALS)
    return df

# =========================================================
# MUR  (jplMURSST41, 0.01°, analysed_sst in degree_C on CoastWatch)
# =========================================================
MUR_ERDDAP_BASE = "https://coastwatch.pfeg.noaa.gov/erddap/griddap"
MUR_DATASET     = "jplMURSST41"
MUR_VARIABLE    = "analysed_sst"
MUR_TIME_HHMMSS = "09:00:00Z"  # MUR daily granules stamped at 09:00 UTC

def fetch_mur():
    print("\nMUR (jplMURSST41, last 5 published days)")
    success = 0
    for i in range(1, MUR_DAYS_BACK + 1):
        ts = datetime.now(timezone.utc) - timedelta(days=i)
        stamp = ts.strftime("%Y%m%d")
        time_iso = f"{ts.strftime('%Y-%m-%d')}T{MUR_TIME_HHMMSS}"
        label = f"MUR {stamp}"
        df = erddap_fetch_csv(
            MUR_ERDDAP_BASE, MUR_DATASET, MUR_VARIABLE,
            time_iso, MUR_STRIDE, label,
        )
        if df is None:
            continue
        print(f"✓ {label}")
        path = os.path.join(DIRS["mur"], f"mur_{stamp}")
        if write_csv(df, path, label):
            success += 1
    if success == 0:
        print("⚠ MUR: zero successful days.")

# =========================================================
# VIIRS/ACSPO
# =========================================================
VIIRS_ERDDAP_BASE = "https://coastwatch.noaa.gov/erddap/griddap"
VIIRS_DATASET     = "noaacwLEOACSPOSSTL3SnrtCDaily"
VIIRS_VARIABLE    = "sea_surface_temperature"
VIIRS_TIME_HHMMSS = "12:00:00Z"

def fetch_viirs():
    print(f"\nVIIRS-ACSPO ({VIIRS_DATASET}, last {VIIRS_DAYS_BACK} days)")
    success = 0
    for i in range(1, VIIRS_DAYS_BACK + 1):
        ts = datetime.now(timezone.utc) - timedelta(days=i)
        stamp = ts.strftime("%Y%m%d")
        time_iso = f"{ts.strftime('%Y-%m-%d')}T{VIIRS_TIME_HHMMSS}"
        label = f"VIIRS {stamp}"
        df = erddap_fetch_csv(
            VIIRS_ERDDAP_BASE, VIIRS_DATASET, VIIRS_VARIABLE,
            time_iso, VIIRS_STRIDE, label,
        )
        if df is None:
            continue
        print(f"✓ {label}")
        path = os.path.join(DIRS["viirs"], f"viirs_{stamp}")
        if write_csv(df, path, label):
            success += 1
    if success == 0:
        print("⚠ VIIRS: zero successful days.")

# =========================================================
# GOES-equivalent (Geo-polar Blended)
# =========================================================
# One file per day (not hourly). Written into both GOES/Hourly and
# GOES/Composite so the frontend's existing loaders find data.
BLENDED_ERDDAP_BASE = "https://coastwatch.noaa.gov/erddap/griddap"
BLENDED_DATASET     = "noaacwBLENDEDsstDLDaily"
BLENDED_VARIABLE    = "analysed_sst"
BLENDED_TIME_HHMMSS = "12:00:00Z"

def fetch_blended():
    print(f"\nGOES/Blended ({BLENDED_DATASET}, last {BLENDED_DAYS_BACK} days)")
    success = 0
    latest_stamp = None
    latest_df = None
    for i in range(1, BLENDED_DAYS_BACK + 1):
        ts = datetime.now(timezone.utc) - timedelta(days=i)
        stamp = ts.strftime("%Y%m%d")
        time_iso = f"{ts.strftime('%Y-%m-%d')}T{BLENDED_TIME_HHMMSS}"
        label = f"BLENDED {stamp}"
        df = erddap_fetch_csv(
            BLENDED_ERDDAP_BASE, BLENDED_DATASET, BLENDED_VARIABLE,
            time_iso, BLENDED_STRIDE, label,
        )
        if df is None:
            continue
        print(f"✓ {label}")
        hourly_path = os.path.join(DIRS["goes_hourly"], f"goes_{stamp}_12")
        if write_csv(df, hourly_path, label + " (hourly@12Z)"):
            success += 1
            if latest_stamp is None or stamp > latest_stamp:
                latest_stamp = stamp
                latest_df = df

    if latest_df is not None:
        comp_path = os.path.join(DIRS["goes_composite"], f"goes_composite_{latest_stamp}")
        write_csv(latest_df, comp_path, f"BLENDED composite {latest_stamp}")
    else:
        print("✗ No Blended data available for composite.")

    if success == 0:
        print("⚠ Blended/GOES: zero successful days.")

# =========================================================
# MAIN
# =========================================================
def main():
    print("Starting SST pipeline...")
    ensure_dirs()

    sources = os.environ.get("SOURCES_OVERRIDE", "all").strip().lower()
    run = {
        "all":          {"mur", "viirs", "blended"},
        "mur_only":     {"mur"},
        "viirs_only":   {"viirs"},
        "goes19_only":  {"blended"},
        "cmems_only":   set(),
    }.get(sources, {"mur", "viirs", "blended"})

    if "blended" in run: fetch_blended()
    if "viirs"   in run: fetch_viirs()
    if "mur"     in run: fetch_mur()

    print("\n✓ Pipeline complete")

if __name__ == "__main__":
    main()
