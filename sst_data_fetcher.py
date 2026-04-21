#!/usr/bin/env python3
"""
=========================================================
SST DATA FETCHER — REAL-TIME PIPELINE
=========================================================

GOES-16 (PRIMARY)
    Hourly L2 Sea Surface Temperature from NOAA's public AWS bucket.
    Last 24 hours, one file per hour.
    A composite is built from the most recent hour.

VIIRS (SECONDARY)
    Daily L3 SST from NOAA CoastWatch ERDDAP.
    Previous day only (VIIRS has ~1-day latency).

MUR (FALLBACK)
    JPL MUR v4.1 global 0.01° daily analysis, via PO.DAAC OPeNDAP.
    Last 5 published days (MUR has ~1-day latency).
    Requires Earthdata Login credentials in environment variables
    EARTHDATA_USERNAME and EARTHDATA_PASSWORD.

---------------------------------------------------------
OUTPUT STRUCTURE
---------------------------------------------------------
DailySSTData/
    GOES/Hourly/      goes_YYYYMMDD_HH.csv
    GOES/Composite/   goes_composite_YYYYMMDD.csv
    VIIRS/            viirs_YYYYMMDD.csv
    MUR/              mur_YYYYMMDD.csv

Each CSV has columns: lat, lon, sst  (SST in Celsius)
Rows form a REGULAR rectangular lat/lon grid — every lat appears
with every lon. This is what the frontend's heatmap renderer expects.

---------------------------------------------------------
REQUIREMENTS
---------------------------------------------------------
pip install numpy pandas xarray netcdf4 requests s3fs

For MUR (Earthdata Login):
    export EARTHDATA_USERNAME=your_username
    export EARTHDATA_PASSWORD=your_password

---------------------------------------------------------
FAILURE POLICY
---------------------------------------------------------
Each day/hour is fetched independently. A failure on one date
logs an error and moves on — the pipeline does not abort.
If a source has zero successful fetches, it logs that fact but
does not raise.

No mock/synthetic data is ever written. If the real fetch fails,
no file is written for that date, and the frontend will simply
show the most recent available day.
=========================================================
"""
import os
import sys
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# xarray / s3fs are imported lazily inside each fetcher so that a missing
# optional dep for one source doesn't block the others.

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

# App region. These MUST match the frontend bounds and sstSummary.ts bounds.
NORTH, SOUTH = 39.00, 33.70
WEST,  EAST  = -78.89, -72.21

# Stride for downsampling from native resolution.
# MUR native 0.01°: stride 5 → 0.05° (~5.5 km) → ~106×134 = ~14k points.
# VIIRS native 0.75km: stride 5 → ~3.75 km.
# GOES native 2 km: stride 2 → ~4 km.
MUR_STRIDE   = 5
VIIRS_STRIDE = 5
GOES_STRIDE  = 2

# How far back to pull
MUR_DAYS_BACK    = 5   # MUR has ~1-day latency
VIIRS_DAYS_BACK  = 1   # VIIRS also ~1-day latency; we want yesterday
GOES_HOURS_BACK  = 24

# Coordinate rounding (prevents float drift from breaking the frontend's
# `new Set(grid.map(d => d.lat))` grouping into unique lat values).
COORD_DECIMALS = 4   # 0.0001° ≈ 11 m
SST_DECIMALS   = 3

# =========================================================
# SETUP
# =========================================================
def ensure_dirs():
    for d in DIRS.values():
        os.makedirs(d, exist_ok=True)

def setup_earthdata_auth():
    """Write ~/.netrc from env vars if not already present.
    Required for MUR via PO.DAAC OPeNDAP."""
    user = os.environ.get("EARTHDATA_USERNAME")
    pw   = os.environ.get("EARTHDATA_PASSWORD")
    if not user or not pw:
        print("⚠ EARTHDATA_USERNAME / EARTHDATA_PASSWORD not set — MUR fetch will fail.")
        return False
    netrc_path = Path.home() / ".netrc"
    entry = f"machine urs.earthdata.nasa.gov login {user} password {pw}\n"
    if netrc_path.exists() and "urs.earthdata.nasa.gov" in netrc_path.read_text():
        return True
    with open(netrc_path, "a") as f:
        f.write(entry)
    os.chmod(netrc_path, 0o600)
    print("✓ Wrote Earthdata credentials to ~/.netrc")
    return True

# =========================================================
# OUTPUT WRITER
# =========================================================
def write_csv(df, base_path, label):
    """Write lat/lon/sst DataFrame as a single CSV.
    Logs grid diagnostics: regular grids have L*M == N (modulo masked cells)."""
    n_lat = df["lat"].nunique()
    n_lon = df["lon"].nunique()
    n_pts = len(df)
    expected = n_lat * n_lon
    if n_pts < 100:
        # Not enough data to be useful — something went wrong upstream.
        print(f"⚠ {label}: only {n_pts} points after cropping. Skipping write.")
        return False
    if n_lat * n_lon < n_pts or n_pts < expected * 0.3:
        # Either duplicated points (lat*lon < N) or the grid is wildly
        # non-rectangular (points fill <30% of the bounding grid).
        print(f"⚠ {label}: grid looks irregular — {n_pts} pts vs {n_lat}×{n_lon}={expected} expected cells.")
    path = base_path + ".csv"
    df.to_csv(path, index=False)
    print(f"  → {path}  ({n_pts} pts, {n_lat} lats × {n_lon} lons)")
    return True

def flatten_to_df(arr_2d, lat_name="lat", lon_name="lon", value_name="sst"):
    """Flatten a 2-D xarray DataArray with lat/lon dims into a long DataFrame
    with repeating lat/lon values on a regular grid."""
    df = (
        arr_2d.to_dataframe(name=value_name)
              .reset_index()
              .rename(columns={lat_name: "lat", lon_name: "lon"})
              [["lat", "lon", value_name]]
              .dropna(subset=[value_name])
    )
    df["lat"] = df["lat"].round(COORD_DECIMALS)
    df["lon"] = df["lon"].round(COORD_DECIMALS)
    df[value_name] = df[value_name].round(SST_DECIMALS)
    return df

# =========================================================
# MUR  (PO.DAAC OPeNDAP, Earthdata auth required)
# =========================================================
def mur_opendap_url(date_iso):
    """MUR v4.1 OPeNDAP URL for a given YYYY-MM-DD."""
    fname = f"{date_iso.replace('-','')}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"
    return (
        "https://opendap.earthdata.nasa.gov/providers/POCLOUD/datasets/"
        f"MUR-JPL-L4-GLOB-v4.1/granules/{fname}"
    )

def fetch_mur():
    print("\nMUR (last 5 published days)")
    try:
        import xarray as xr
    except ImportError:
        print("✗ xarray not installed — skipping MUR")
        return

    success = 0
    for i in range(1, MUR_DAYS_BACK + 1):  # start at 1: today's file not yet published
        ts = datetime.now(timezone.utc) - timedelta(days=i)
        stamp = ts.strftime("%Y%m%d")
        date_iso = ts.strftime("%Y-%m-%d")
        url = mur_opendap_url(date_iso)

        try:
            with xr.open_dataset(url) as ds:
                # MUR lat axis is ascending (-90 → 90).
                sub = ds["analysed_sst"].sel(
                    lat=slice(SOUTH, NORTH),
                    lon=slice(WEST,  EAST),
                )
                if sub.sizes.get("time", 1) > 1:
                    sub = sub.isel(time=0)
                elif "time" in sub.dims:
                    sub = sub.squeeze("time", drop=True)

                # Stride-downsample onto a regular grid.
                sub = sub.isel(
                    lat=slice(None, None, MUR_STRIDE),
                    lon=slice(None, None, MUR_STRIDE),
                )
                # Kelvin → Celsius.
                sub = sub - 273.15

                df = flatten_to_df(sub, "lat", "lon", "sst")

            print(f"✓ MUR {stamp}")
            path = os.path.join(DIRS["mur"], f"mur_{stamp}")
            if write_csv(df, path, f"MUR {stamp}"):
                success += 1
        except Exception as e:
            print(f"✗ MUR {stamp} failed: {type(e).__name__}: {e}")

    if success == 0:
        print("⚠ MUR: zero successful days.")

# =========================================================
# VIIRS  (NOAA CoastWatch ERDDAP, no auth)
# =========================================================
# Using the VIIRS NPP / JPSS-1 daily SST product on CoastWatch ERDDAP.
# Dataset id: noaacwBLENDEDsstDaily (blended multi-sensor) works well here;
# alternative VIIRS-only: nesdisVHNSQchlaDaily is chl, not SST.
# We use noaacwBLENDEDsstDaily which is multi-sensor (includes VIIRS) at 5 km.
# If you strictly want VIIRS-only, swap in a VIIRS-specific dataset id here.
ERDDAP_BASE = "https://coastwatch.noaa.gov/erddap/griddap"
VIIRS_DATASET = "noaacwBLENDEDsstDaily"

def fetch_viirs():
    print("\nVIIRS-blended (previous day)")
    try:
        import xarray as xr
    except ImportError:
        print("✗ xarray not installed — skipping VIIRS")
        return

    for i in range(1, VIIRS_DAYS_BACK + 1):
        ts = datetime.now(timezone.utc) - timedelta(days=i)
        stamp = ts.strftime("%Y%m%d")
        date_iso = ts.strftime("%Y-%m-%d")

        # ERDDAP serves NetCDF directly when we request .nc — xarray can open it.
        # Query spec: [time][lat][lon] with ERDDAP's stride-in-URL syntax.
        url = (
            f"{ERDDAP_BASE}/{VIIRS_DATASET}.nc?"
            f"analysed_sst"
            f"[({date_iso}T12:00:00Z):1:({date_iso}T12:00:00Z)]"
            f"[({SOUTH}):1:({NORTH})]"
            f"[({WEST}):1:({EAST})]"
        )

        try:
            with xr.open_dataset(url) as ds:
                var = "analysed_sst" if "analysed_sst" in ds else list(ds.data_vars)[0]
                sub = ds[var]
                if "time" in sub.dims:
                    sub = sub.isel(time=0)
                # ERDDAP may name dims latitude/longitude — normalize.
                rename = {}
                if "latitude"  in sub.dims: rename["latitude"]  = "lat"
                if "longitude" in sub.dims: rename["longitude"] = "lon"
                if rename: sub = sub.rename(rename)
                # Stride-downsample.
                sub = sub.isel(
                    lat=slice(None, None, VIIRS_STRIDE),
                    lon=slice(None, None, VIIRS_STRIDE),
                )
                # Units: this dataset is already in Celsius; sanity-check.
                units = ds[var].attrs.get("units", "").lower()
                if "kelvin" in units or "k" == units.strip():
                    sub = sub - 273.15

                df = flatten_to_df(sub, "lat", "lon", "sst")

            print(f"✓ VIIRS {stamp}")
            path = os.path.join(DIRS["viirs"], f"viirs_{stamp}")
            write_csv(df, path, f"VIIRS {stamp}")
        except Exception as e:
            print(f"✗ VIIRS {stamp} failed: {type(e).__name__}: {e}")

# =========================================================
# GOES-16  (AWS S3, no auth)
# =========================================================
# L2 SST Full-Disk product: ABI-L2-SSTF. Published ~hourly.
# Bucket: s3://noaa-goes16/ABI-L2-SSTF/YYYY/DDD/HH/
# Each hour has ~1 granule, filename:
#   OR_ABI-L2-SSTF-M6_G16_sYYYYDDDHHMMSSs_eYYYY...c...nc
GOES_BUCKET = "noaa-goes16"
GOES_PRODUCT = "ABI-L2-SSTF"

def fetch_goes():
    print(f"\nGOES-16 (last {GOES_HOURS_BACK} hours)")
    try:
        import xarray as xr
        import s3fs
    except ImportError:
        print("✗ xarray/s3fs not installed — skipping GOES")
        return []

    fs = s3fs.S3FileSystem(anon=True)
    results = []

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    for i in range(GOES_HOURS_BACK):
        ts = now - timedelta(hours=i)
        stamp = ts.strftime("%Y%m%d_%H")
        doy = ts.strftime("%j")
        prefix = f"{GOES_BUCKET}/{GOES_PRODUCT}/{ts.year}/{doy}/{ts.strftime('%H')}/"

        try:
            keys = fs.ls(prefix)
        except Exception as e:
            print(f"✗ GOES {stamp} listing failed: {e}")
            continue
        if not keys:
            print(f"✗ GOES {stamp}: no granules in bucket yet")
            continue

        # Pick the latest granule in the hour (highest start-time suffix).
        key = sorted(keys)[-1]
        try:
            with fs.open(key, "rb") as f:
                with xr.open_dataset(f, engine="h5netcdf") as ds:
                    # GOES ABI L2 SSTF is on a 2 km ABI fixed grid (x,y radians).
                    # Converting x/y radians → lat/lon requires the projection.
                    # ds["SST"] has dims (y, x); attach lat/lon via the
                    # goes_imager_projection coordinate reference.
                    sst = ds["SST"]
                    # Many preprocessed copies of ABI also ship lat/lon 2-D arrays.
                    # If not present, derive them.
                    if "lat" in ds.coords and "lon" in ds.coords:
                        lat2d = ds["lat"].values
                        lon2d = ds["lon"].values
                    else:
                        lat2d, lon2d = _abi_xy_to_latlon(ds)

                    sst_vals = sst.values
                    # Build a mask of points inside our bounds
                    mask = (
                        (lat2d >= SOUTH) & (lat2d <= NORTH) &
                        (lon2d >= WEST)  & (lon2d <= EAST)  &
                        np.isfinite(sst_vals)
                    )
                    if not mask.any():
                        print(f"✗ GOES {stamp}: no pixels in region")
                        continue

                    # GOES native is on a curvilinear (non-rectangular in lat/lon)
                    # grid, so we resample onto a REGULAR lat/lon grid by binning.
                    df_raw = pd.DataFrame({
                        "lat": lat2d[mask],
                        "lon": lon2d[mask],
                        "sst": sst_vals[mask],
                    })
                    # ABI SST is in Kelvin.
                    units = ds["SST"].attrs.get("units", "").lower()
                    if "kelvin" in units or units.strip() == "k":
                        df_raw["sst"] = df_raw["sst"] - 273.15

                    # Snap onto a regular 0.04° (~4 km) lat/lon grid so downstream
                    # `new Set(grid.map(d=>d.lat))` produces a clean axis set.
                    GRID_STEP = 0.04 * GOES_STRIDE  # stride 2 → 0.08° (~9 km)
                    df_raw["lat"] = (df_raw["lat"] / GRID_STEP).round() * GRID_STEP
                    df_raw["lon"] = (df_raw["lon"] / GRID_STEP).round() * GRID_STEP
                    df = (df_raw.groupby(["lat", "lon"], as_index=False)["sst"]
                                 .mean())
                    df["lat"] = df["lat"].round(COORD_DECIMALS)
                    df["lon"] = df["lon"].round(COORD_DECIMALS)
                    df["sst"] = df["sst"].round(SST_DECIMALS)

            print(f"✓ GOES {stamp}")
            path = os.path.join(DIRS["goes_hourly"], f"goes_{stamp}")
            if write_csv(df, path, f"GOES {stamp}"):
                results.append((ts, df))
        except Exception as e:
            print(f"✗ GOES {stamp} failed: {type(e).__name__}: {e}")

    return results

def _abi_xy_to_latlon(ds):
    """Convert GOES ABI fixed-grid (x, y in radians) to (lat, lon) arrays.
    Uses the goes_imager_projection attributes in the NetCDF.
    """
    proj = ds["goes_imager_projection"].attrs
    req  = proj["semi_major_axis"]
    rpol = proj["semi_minor_axis"]
    H    = proj["perspective_point_height"] + req
    lon0 = np.deg2rad(proj["longitude_of_projection_origin"])

    x = ds["x"].values  # radians
    y = ds["y"].values
    X, Y = np.meshgrid(x, y)

    a = np.sin(X)**2 + (np.cos(X)**2)*(np.cos(Y)**2 + (req**2/rpol**2)*np.sin(Y)**2)
    b = -2.0 * H * np.cos(X) * np.cos(Y)
    c = H**2 - req**2
    disc = b**2 - 4*a*c
    disc = np.where(disc < 0, np.nan, disc)
    rs = (-b - np.sqrt(disc)) / (2*a)

    sx = rs * np.cos(X) * np.cos(Y)
    sy = -rs * np.sin(X)
    sz = rs * np.cos(X) * np.sin(Y)

    lat = np.rad2deg(np.arctan((req**2/rpol**2) * (sz / np.sqrt((H - sx)**2 + sy**2))))
    lon = np.rad2deg(lon0 - np.arctan(sy / (H - sx)))
    return lat, lon

def build_goes_composite(goes_data):
    print("\nGOES composite (latest hour)")
    if not goes_data:
        print("✗ No GOES data available for composite.")
        return
    latest_ts, latest_df = max(goes_data, key=lambda x: x[0])
    stamp = latest_ts.strftime("%Y%m%d")
    path = os.path.join(DIRS["goes_composite"], f"goes_composite_{stamp}")
    print(f"✓ GOES composite from {latest_ts.strftime('%Y%m%d_%H')}")
    write_csv(latest_df, path, f"GOES composite {stamp}")

# =========================================================
# MAIN
# =========================================================
def main():
    print("Starting SST pipeline...")
    ensure_dirs()
    setup_earthdata_auth()

    goes_data = fetch_goes()
    build_goes_composite(goes_data)
    fetch_viirs()
    fetch_mur()

    print("\n✓ Pipeline complete")

if __name__ == "__main__":
    main()
