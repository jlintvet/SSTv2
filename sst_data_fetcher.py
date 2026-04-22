#!/usr/bin/env python3
"""
=========================================================
SST DATA FETCHER — NOAA CoastWatch ERDDAP + VIIRS L3U
=========================================================

Three independent data sources serving three distinct UI views:

MUR (DAILY COMPOSITE)
    JPL MUR v4.1 0.01° daily (jplMURSST41) via ERDDAP.
    Smooth gap-filled L4 analysis. Best overall daily picture.
    Last 5 published days.

GOES-19 (BLENDED DAILY)
    Geo-polar Blended Day+Night (noaacwBLENDEDsstDLDaily).
    Multi-sensor blended analysis with ABI/GOES-19 as primary
    geostationary input. Different sensor weighting than MUR.
    Most recent available day written to GOES/Composite/.
    NOTE: This is a daily product — one file per day, not hourly.
    The previous "hourly" files were identical copies of one daily
    snapshot and have been removed. The composite is the honest form.

VIIRS (MULTI-PASS)
    ACSPO L3U 10-min granule NetCDF files from the STAR NESDIS
    NRT file server (no auth required). NPP, NOAA-20, and NOAA-21
    each make ~2 passes over the Mid-Atlantic per day, giving ~6
    genuinely distinct per-swath snapshots per 24 hours with 1-3 hr
    NRT latency. Cloud-covered pixels are masked (fill values) -- that
    is expected and correct for L3U swath data. This view shows the
    actual satellite passes as they happened, not a blended analysis.

---------------------------------------------------------
OUTPUT STRUCTURE
---------------------------------------------------------
DailySSTData/
    MUR/              mur_YYYYMMDD.csv
    GOES/Composite/   goes_composite_YYYYMMDD.csv
    VIIRS/Passes/     viirs_{platform}_{YYYYMMDD_HHMM}.csv

CSV columns: lat, lon, sst   (SST in Celsius)
MUR and GOES form regular rectangular lat/lon grids.
VIIRS pass CSVs are sparse (swath footprint, cloud gaps present).

---------------------------------------------------------
REQUIREMENTS
---------------------------------------------------------
pip install requests numpy pandas netCDF4

netCDF4 is required only for the VIIRS multi-pass fetch.
MUR and GOES continue to work without it via ERDDAP CSV.

---------------------------------------------------------
VERIFYING VIIRS FILE SERVER PATH
---------------------------------------------------------
Before first run confirm which NRT base URL is reachable:

  python3 -c "
  import requests
  for u in [
    'https://www.star.nesdis.noaa.gov/pub/socd2/coastwatch/sst/nrt/viirs/n20/',
    'https://coastwatch.noaa.gov/pub/socd/mecb/coastwatch/viirs/nrt/n20/',
  ]:
      r = requests.get(u, timeout=10)
      print(r.status_code, u)
  "

Set VIIRS_BASE_CANDIDATES below accordingly.

---------------------------------------------------------
FAILURE POLICY
---------------------------------------------------------
Each date/granule fetched independently. A failure on one
logs an error and moves on. No mock/synthetic data is written.
=========================================================
"""
import io
import os
import re
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests

try:
    import netCDF4 as nc
    _NETCDF4_AVAILABLE = True
except ImportError:
    _NETCDF4_AVAILABLE = False
    print("WARNING: netCDF4 not installed -- VIIRS multi-pass fetch disabled.")
    print("         pip install netCDF4")


# =========================================================
# LAND FILTER
# =========================================================
NE_LAND_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
    "master/geojson/ne_10m_land.geojson"
)
_LAND_POLYS_CACHE = None


def _point_in_ring(px, py, ring):
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > py) != (yj > py)) and (
            px < (xj - xi) * (py - yi) / (yj - yi + 1e-12) + xi
        ):
            inside = not inside
        j = i
    return inside


def _load_land_polys_for_bounds(north, south, east, west):
    global _LAND_POLYS_CACHE
    if _LAND_POLYS_CACHE is not None:
        return _LAND_POLYS_CACHE
    try:
        r = requests.get(NE_LAND_URL, timeout=60)
        r.raise_for_status()
        gj = r.json()
        polys = []
        for f in gj.get("features", []):
            g = f.get("geometry", {})
            t = g.get("type")
            if t == "Polygon":
                polys.append(g["coordinates"])
            elif t == "MultiPolygon":
                polys.extend(g["coordinates"])
        kept = []
        for poly in polys:
            ring = poly[0]
            mnl, mxl, mnla, mxla = 1e9, -1e9, 1e9, -1e9
            for lo, la in ring:
                if lo < mnl: mnl = lo
                if lo > mxl: mxl = lo
                if la < mnla: mnla = la
                if la > mxla: mxla = la
            if mxl >= west and mnl <= east and mxla >= south and mnla <= north:
                kept.append(poly)
        print(f"  (land filter: loaded {len(kept)} coastline polygons intersecting region)")
        _LAND_POLYS_CACHE = kept
        return kept
    except Exception as e:
        print(f"  ⚠ land filter: failed to load coastline ({e}); skipping.")
        _LAND_POLYS_CACHE = []
        return []


def _is_land(lat, lon, polys):
    for poly in polys:
        if _point_in_ring(lon, lat, poly[0]):
            in_hole = False
            for h in range(1, len(poly)):
                if _point_in_ring(lon, lat, poly[h]):
                    in_hole = True
                    break
            if not in_hole:
                return True
    return False


def filter_to_ocean(df, label=""):
    """Drop rows whose (lat, lon) fall on land per NE 1:10m coastline."""
    polys = _load_land_polys_for_bounds(NORTH, SOUTH, EAST, WEST)
    if not polys:
        return df
    before = len(df)
    mask = np.fromiter(
        (not _is_land(lat, lon, polys)
         for lat, lon in zip(df["lat"].to_numpy(), df["lon"].to_numpy())),
        dtype=bool,
        count=len(df),
    )
    df = df[mask].reset_index(drop=True)
    dropped = before - len(df)
    if dropped > 0:
        print(
            f"  (land filter: dropped {dropped} inland points,"
            f" kept {len(df)} ocean points)"
        )
    return df


# =========================================================
# CONFIG
# =========================================================
BASE_DIR = "DailySSTData"
DIRS = {
    "goes_composite": os.path.join(BASE_DIR, "GOES", "Composite"),
    "viirs_passes":   os.path.join(BASE_DIR, "VIIRS", "Passes"),
    "mur":            os.path.join(BASE_DIR, "MUR"),
}

# App region -- MUST match the frontend bounds and sstSummary.ts bounds.
NORTH, SOUTH = 39.00, 33.70
WEST,  EAST  = -78.89, -72.21


# =========================================================
# ERDDAP CONFIG
# =========================================================
ERDDAP_HOST_PFEG = "https://coastwatch.pfeg.noaa.gov/erddap"
ERDDAP_HOST_CW   = "https://coastwatch.noaa.gov/erddap"

# MUR: multiple mirrors in priority order.
# MUR native 0.01 deg -> stride 5 = 0.05 deg ~ 5.5 km -> ~106x134 ~ 14k pts
MUR_MIRRORS = [
    {
        "host":       ERDDAP_HOST_PFEG,
        "dataset_id": "jplMURSST41",
        "var":        "analysed_sst",
        "stride":     5,
        # MUR ERDDAP serves analysed_sst in Celsius despite the underlying
        # NetCDF variable. Observed empirically: setting "K" produces -444F
        # offsets downstream. "C" keeps the pipeline honest.
        "units":      "C",
    },
    {
        "host":       ERDDAP_HOST_CW,
        "dataset_id": "jplMURSST41",
        "var":        "analysed_sst",
        "stride":     5,
        "units":      "C",
    },
    {
        "host":       "https://upwell.pfeg.noaa.gov/erddap",
        "dataset_id": "jplMURSST41",
        "var":        "analysed_sst",
        "stride":     5,
        "units":      "C",
    },
]
MUR_CFG      = MUR_MIRRORS[0]
MUR_DAYS_BACK = 5

# GOES / Geo-polar Blended Day+Night.
# noaacwBLENDEDsstDLDaily = Diurnal-corrected Day+Night blend.
# Uses GOES-19 ABI as primary geostationary input alongside NOAA-20/21
# VIIRS, MetOp AVHRR, Himawari AHI, and Meteosat SEVIRI.
# 0.05 deg native resolution -> stride 1 keeps all points.
GOES_CFG = {
    "host":       ERDDAP_HOST_CW,
    "dataset_id": "noaacwBLENDEDsstDLDaily",
    "var":        "analysed_sst",
    "stride":     1,
    "units":      "C",
}


# =========================================================
# VIIRS L3U GRANULE CONFIG
# =========================================================
# ACSPO L3U 10-minute granule NetCDF files. No auth required.
# _probe_viirs_base() tests both at startup and uses the live one.
VIIRS_BASE_CANDIDATES = [
    "https://www.star.nesdis.noaa.gov/pub/socd2/coastwatch/sst/nrt/viirs",
    "https://coastwatch.noaa.gov/pub/socd/mecb/coastwatch/viirs/nrt",
]
VIIRS_PLATFORMS  = ["npp", "n20", "n21"]   # Suomi-NPP, NOAA-20, NOAA-21
VIIRS_HOURS_BACK = 24


# =========================================================
# HTTP TUNING
# =========================================================
HTTP_TIMEOUT       = 120
HTTP_RETRIES       = 2
HTTP_BACKOFF_S     = 3
REQUEST_SPACING_S  = 2.0
USER_AGENT         = "SSTv2-fetcher/1.0 (+https://github.com/jlintvet/SSTv2)"

COORD_DECIMALS = 4
SST_DECIMALS   = 3

_last_request_at  = {}
_host_blacklisted = set()
_host_conn_resets = {}
CONN_RESET_THRESHOLD = 2


# =========================================================
# SHARED HTTP / ERDDAP HELPERS
# =========================================================
def ensure_dirs():
    for d in DIRS.values():
        os.makedirs(d, exist_ok=True)


def _host_of(url):
    return url.split("/", 3)[2]


def _throttle(host):
    """Sleep if needed to enforce REQUEST_SPACING_S between calls to a host."""
    now  = time.monotonic()
    last = _last_request_at.get(host)
    if last is not None:
        wait = REQUEST_SPACING_S - (now - last)
        if wait > 0:
            time.sleep(wait)
    _last_request_at[host] = time.monotonic()


def build_erddap_csv_url(cfg, time_iso, south, north, west, east):
    """Build an ERDDAP griddap .csv0 URL for a bounding-box/time subset."""
    stride = cfg["stride"]
    query = (
        f"{cfg['var']}"
        f"[({time_iso})]"
        f"[({south}):{stride}:({north})]"
        f"[({west}):{stride}:({east})]"
    )
    return f"{cfg['host']}/griddap/{cfg['dataset_id']}.csv0?{query}"


def fetch_erddap_csv(url, label):
    """GET an ERDDAP CSV URL with throttling, retries, and blacklist logic."""
    host = _host_of(url)
    if host in _host_blacklisted:
        raise RuntimeError(f"host {host} is blacklisted this run -- skipping.")
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/csv,text/plain;q=0.9,*/*;q=0.5",
    }
    last_err = None
    for attempt in range(1, HTTP_RETRIES + 2):
        _throttle(host)
        try:
            resp = requests.get(url, timeout=HTTP_TIMEOUT, headers=headers)
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
            if isinstance(e, (requests.ConnectionError, requests.Timeout)):
                _host_conn_resets[host] = _host_conn_resets.get(host, 0) + 1
                if _host_conn_resets[host] >= CONN_RESET_THRESHOLD:
                    _host_blacklisted.add(host)
                    raise RuntimeError(
                        f"host {host} keeps resetting -- blacklisted."
                    )
                raise RuntimeError(last_err)
            if attempt <= HTTP_RETRIES:
                time.sleep(HTTP_BACKOFF_S * attempt)
            continue

        if resp.status_code == 200:
            return resp.text
        if resp.status_code == 404:
            raise RuntimeError("404 Not Found (no data for this date)")
        if resp.status_code == 403:
            _host_blacklisted.add(host)
            raise RuntimeError(f"403 Forbidden: {resp.text[:200]}")
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "30"))
            last_err = f"HTTP 429 (Retry-After {retry_after}s)"
            if attempt <= HTTP_RETRIES:
                time.sleep(retry_after)
            continue
        last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
        if attempt <= HTTP_RETRIES:
            time.sleep(HTTP_BACKOFF_S * attempt)

    raise RuntimeError(last_err or "unknown fetch error")


def parse_erddap_csv0(csv_text, cfg):
    """
    Parse ERDDAP .csv0 (headerless) response into DataFrame [lat, lon, sst].
    Column order from griddap: time, latitude, longitude, variable.
    """
    if not csv_text or not csv_text.strip():
        raise RuntimeError("empty response body")
    df = pd.read_csv(
        io.StringIO(csv_text),
        header=None,
        names=["time", "lat", "lon", "sst"],
        dtype={"time": str},
    )
    if df.empty:
        raise RuntimeError("response had 0 data rows")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["sst"] = pd.to_numeric(df["sst"], errors="coerce")
    df = df.dropna(subset=["lat", "lon", "sst"])
    if df.empty:
        raise RuntimeError("all SST values NaN in response")
    if cfg["units"] == "K":
        df["sst"] = df["sst"] - 273.15
    df = df[["lat", "lon", "sst"]]
    df["lat"] = df["lat"].round(COORD_DECIMALS)
    df["lon"] = df["lon"].round(COORD_DECIMALS)
    df["sst"] = df["sst"].round(SST_DECIMALS)
    df = filter_to_ocean(df, label="ingest")
    return df


def write_csv(df, base_path, label):
    """
    Write [lat, lon, sst] DataFrame as CSV with a grid coverage diagnostic.
    Gridded products (MUR, GOES) warn below 30% coverage.
    VIIRS pass files are inherently sparse -- warn only below 5%.
    """
    n_pts = len(df)
    if n_pts < 100:
        print(f"  ⚠ {label}: only {n_pts} points. Skipping write.")
        return False
    n_lat    = df["lat"].nunique()
    n_lon    = df["lon"].nunique()
    expected = n_lat * n_lon
    coverage = n_pts / expected if expected > 0 else 0
    threshold = 0.05 if "VIIRS" in label else 0.30
    if coverage < threshold:
        print(
            f"  ⚠ {label}: sparse -- {n_pts} pts vs "
            f"{n_lat}x{n_lon}={expected} ({coverage*100:.0f}% fill)."
        )
    path = base_path + ".csv"
    df.to_csv(path, index=False)
    print(f"  -> {path}  ({n_pts} pts, {n_lat} lats x {n_lon} lons)")
    return True


def fetch_one_day_erddap(cfg, time_iso, label):
    """Fetch + parse one ERDDAP day subset. Returns DataFrame or raises."""
    url      = build_erddap_csv_url(cfg, time_iso, SOUTH, NORTH, WEST, EAST)
    csv_text = fetch_erddap_csv(url, label)
    return parse_erddap_csv0(csv_text, cfg)


# =========================================================
# MUR -- DAILY COMPOSITE
# =========================================================
def fetch_mur():
    """
    Pull the last MUR_DAYS_BACK daily MUR L4 snapshots via ERDDAP.
    Tries each mirror in order; falls back silently if a mirror is
    blacklisted or returns errors.
    Output: DailySSTData/MUR/mur_YYYYMMDD.csv
    """
    print(f"\n── MUR daily composite (last {MUR_DAYS_BACK} days) ──")
    success = 0

    for i in range(1, MUR_DAYS_BACK + 1):
        ts       = datetime.now(timezone.utc) - timedelta(days=i)
        stamp    = ts.strftime("%Y%m%d")
        time_iso = ts.strftime("%Y-%m-%d") + "T09:00:00Z"

        df       = None
        last_err = None
        for cfg in MUR_MIRRORS:
            mirror_host = cfg["host"].split("/", 3)[2]
            if mirror_host in _host_blacklisted:
                continue
            try:
                df = fetch_one_day_erddap(
                    cfg, time_iso, f"MUR {stamp}@{mirror_host}"
                )
                break
            except Exception as e:
                last_err = f"{mirror_host}: {type(e).__name__}: {str(e)[:120]}"
                continue

        if df is None:
            print(f"  ✗ MUR {stamp} failed on all mirrors. Last: {last_err}")
            continue

        print(f"  ✓ MUR {stamp}")
        path = os.path.join(DIRS["mur"], f"mur_{stamp}")
        if write_csv(df, path, f"MUR {stamp}"):
            success += 1

    if success == 0:
        print("  ⚠ MUR: zero successful days (all mirrors failed).")


# =========================================================
# GOES -- BLENDED DAILY COMPOSITE
# =========================================================
def fetch_goes():
    """
    Pull the most recent available Geo-polar Blended Day+Night snapshot
    via ERDDAP and write it as a single dated composite file.

    This is a genuine daily product -- one analysis per day.
    GOES-19 ABI is the primary geostationary input; it is blended with
    NOAA-20/21 VIIRS, MetOp AVHRR, Himawari AHI, and Meteosat SEVIRI.
    The result is a different view of SST than MUR due to different sensor
    weighting and analysis methodology.

    Output: DailySSTData/GOES/Composite/goes_composite_YYYYMMDD.csv
    """
    print("\n── GOES-19 geo-polar blended daily composite ──")

    df_latest = None
    latest_ts = None

    for i in range(0, 4):   # try today back to 3 days ago
        ts       = datetime.now(timezone.utc) - timedelta(days=i)
        time_iso = ts.strftime("%Y-%m-%d") + "T12:00:00Z"
        try:
            df_latest = fetch_one_day_erddap(
                GOES_CFG, time_iso, f"GOES {ts:%Y%m%d}"
            )
            latest_ts = ts
            print(f"  ✓ Geo-polar Blended available for {ts:%Y-%m-%d}")
            break
        except Exception as e:
            print(f"  (no data for {ts:%Y-%m-%d}: {type(e).__name__})")
            continue

    if df_latest is None:
        print("  ✗ GOES: no recent Geo-polar Blended data available.")
        return

    stamp = latest_ts.strftime("%Y%m%d")
    path  = os.path.join(DIRS["goes_composite"], f"goes_composite_{stamp}")
    write_csv(df_latest, path, f"GOES composite {stamp}")


# =========================================================
# VIIRS -- MULTI-PASS (L3U GRANULE FILE SERVER)
# =========================================================

def _probe_viirs_base() -> str:
    """
    Test each VIIRS_BASE_CANDIDATES URL and return the first live one.
    Raises RuntimeError if none respond.
    """
    for base in VIIRS_BASE_CANDIDATES:
        test_url = f"{base}/n20/"
        try:
            r = requests.get(
                test_url,
                timeout=15,
                headers={"User-Agent": USER_AGENT},
                allow_redirects=True,
            )
            if r.status_code in (200, 403):
                print(f"  ✓ VIIRS NRT base confirmed: {base}")
                return base
        except requests.RequestException:
            pass
    raise RuntimeError(
        "Neither VIIRS NRT file server URL is reachable.\n"
        "Check VIIRS_BASE_CANDIDATES or network connectivity."
    )


def _list_viirs_granules(base: str, platform: str, year: int, doy: int) -> list:
    """
    Fetch the HTTP directory listing for one platform/year/doy and return
    a sorted list of ACSPO L3U granule filenames.

    ACSPO L3U filename pattern:
      20250422123456-STAR-L3U_GHRSST-SSTsubskin-VIIRS_N20-ACSPO_V2.80-v02.0-fv01.0.nc
    """
    url  = f"{base}/{platform}/l3u/{year}/{doy:03d}/"
    host = _host_of(url)
    try:
        _throttle(host)
        resp = requests.get(
            url,
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        )
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ⚠ dir listing failed {platform} {year}/{doy:03d}: {e}")
        return []

    found = re.findall(
        r"(\d{14}-STAR-L3U_GHRSST-SSTsubskin-VIIRS[^\s\"<>]+\.nc)",
        resp.text,
    )
    return sorted(set(found))


def _granule_time(filename: str) -> datetime:
    """Extract granule UTC start time from the leading 14-digit timestamp."""
    return datetime.strptime(filename[:14], "%Y%m%d%H%M%S").replace(
        tzinfo=timezone.utc
    )


def _fetch_viirs_granule(
    base: str, platform: str, year: int, doy: int, filename: str
) -> pd.DataFrame:
    """
    Download one ACSPO L3U NetCDF4 granule, subset to the bounding box,
    and return a long-form DataFrame [lat, lon, sst] in Celsius.
    Returns None on any failure (network, parse, empty bbox).

    L3U file characteristics:
      - Global 0.02 deg grid, ~25 MB per file
      - sea_surface_temperature: shape (1, nlat, nlon), units Kelvin
      - lat / lon: 1-D coordinate arrays
      - Only QL=5 (confidently clear) pixels populated -- NOAA pre-filters
        at L3U ingest, so no further quality masking needed here
      - Cloud-covered pixels are fill values -> NaN -> dropped from output
      - Swath edges outside the satellite view are also fill values
      This means the output CSV is sparse -- that is correct and expected.
    """
    if not _NETCDF4_AVAILABLE:
        return None

    url  = f"{base}/{platform}/l3u/{year}/{doy:03d}/{filename}"
    host = _host_of(url)
    if host in _host_blacklisted:
        return None

    try:
        _throttle(host)
        resp = requests.get(
            url,
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
            stream=False,   # must be fully in memory for nc.Dataset(memory=)
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ⚠ granule download failed {filename}: {e}")
        if isinstance(e, (requests.ConnectionError, requests.Timeout)):
            _host_conn_resets[host] = _host_conn_resets.get(host, 0) + 1
            if _host_conn_resets[host] >= CONN_RESET_THRESHOLD:
                _host_blacklisted.add(host)
        return None

    try:
        ds = nc.Dataset("inmemory.nc", memory=resp.content)

        lat_full = ds.variables["lat"][:]
        lon_full = ds.variables["lon"][:]
        sst_var  = ds.variables["sea_surface_temperature"]  # (1, nlat, nlon) K

        # Slice to bounding box
        lat_mask = (lat_full >= SOUTH) & (lat_full <= NORTH)
        lon_mask = (lon_full >= WEST)  & (lon_full <= EAST)
        lat_idx  = np.where(lat_mask)[0]
        lon_idx  = np.where(lon_mask)[0]

        if len(lat_idx) == 0 or len(lon_idx) == 0:
            ds.close()
            return None   # granule does not overlap our region

        lat_sl  = slice(int(lat_idx[0]), int(lat_idx[-1]) + 1)
        lon_sl  = slice(int(lon_idx[0]), int(lon_idx[-1]) + 1)

        sst_sub = sst_var[0, lat_sl, lon_sl]   # masked array, Kelvin
        lat_sub = lat_full[lat_sl]
        lon_sub = lon_full[lon_sl]
        ds.close()

    except Exception as e:
        print(f"  ⚠ NetCDF parse failed {filename}: {e}")
        return None

    # Meshgrid -> long-form, K -> C
    lon_grid, lat_grid = np.meshgrid(lon_sub, lat_sub)
    sst_c = np.ma.filled(sst_sub, np.nan).astype(np.float64) - 273.15

    df = pd.DataFrame({
        "lat": lat_grid.flatten(),
        "lon": lon_grid.flatten(),
        "sst": sst_c.flatten(),
    })
    df = df.dropna(subset=["sst"])
    df = df[(df["sst"] > -2.0) & (df["sst"] < 40.0)]   # physical sanity

    if df.empty:
        return None

    df["lat"] = df["lat"].round(COORD_DECIMALS)
    df["lon"] = df["lon"].round(COORD_DECIMALS)
    df["sst"] = df["sst"].round(SST_DECIMALS)
    df = filter_to_ocean(df, label=filename)
    return df if not df.empty else None


def fetch_viirs_passes():
    """
    Scan the STAR NESDIS NRT file server for ACSPO L3U granules from the
    last VIIRS_HOURS_BACK hours across Suomi-NPP, NOAA-20, and NOAA-21.
    Write one CSV per granule that has data within the bounding box.

    Typical yield for the Mid-Atlantic (33.7-39N, 78.9-72.2W):
        ~2 passes x 3 satellites = ~6 distinct files per 24-hour window.

    Output: DailySSTData/VIIRS/Passes/viirs_{platform}_{YYYYMMDD_HHMM}.csv
    """
    if not _NETCDF4_AVAILABLE:
        print("\n── VIIRS multi-pass ──")
        print("  skipped (netCDF4 not installed -- pip install netCDF4)")
        return

    print(f"\n── VIIRS multi-pass (last {VIIRS_HOURS_BACK}h -- NPP / NOAA-20 / NOAA-21) ──")

    try:
        live_base = _probe_viirs_base()
    except RuntimeError as e:
        print(f"  ✗ {e}")
        return

    now_utc = datetime.now(timezone.utc)
    cutoff  = now_utc - timedelta(hours=VIIRS_HOURS_BACK)

    # Collect (year, doy) pairs covered by the window (usually 1-2 days)
    day_pairs = set()
    for h in range(VIIRS_HOURS_BACK + 2):
        t = now_utc - timedelta(hours=h)
        day_pairs.add((t.year, t.timetuple().tm_yday))

    total_new     = 0
    total_skipped = 0

    for platform in VIIRS_PLATFORMS:
        for (year, doy) in sorted(day_pairs):
            filenames = _list_viirs_granules(live_base, platform, year, doy)
            if not filenames:
                continue

            for fname in filenames:
                try:
                    gran_time = _granule_time(fname)
                except ValueError:
                    continue

                if gran_time < cutoff or gran_time > now_utc:
                    continue

                stamp    = gran_time.strftime("%Y%m%d_%H%M")
                out_path = os.path.join(
                    DIRS["viirs_passes"],
                    f"viirs_{platform}_{stamp}",
                )

                # Skip granules already written in a previous run
                if os.path.exists(out_path + ".csv"):
                    total_skipped += 1
                    continue

                print(f"  {platform} {stamp} ... ", end="", flush=True)
                df = _fetch_viirs_granule(live_base, platform, year, doy, fname)

                if df is not None and write_csv(
                    df, out_path, f"VIIRS {platform} {stamp}"
                ):
                    total_new += 1
                else:
                    print("✗ no usable data in bounding box")

    if total_new == 0 and total_skipped == 0:
        print("  ⚠ VIIRS: zero granules found in time window.")
    else:
        print(
            f"  ✓ VIIRS: {total_new} new granule(s) written,"
            f" {total_skipped} already cached."
        )


# =========================================================
# MAIN
# =========================================================
def main():
    print("=" * 57)
    print("SST PIPELINE")
    print(f"  UTC  : {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}")
    print(f"  Bbox : {SOUTH}-{NORTH}N  {WEST}-{EAST}E")
    print("=" * 57)

    ensure_dirs()

    # View 1 -- MUR: smooth gap-filled L4 daily composite
    fetch_mur()

    # View 2 -- GOES-19: geo-polar blended daily composite
    fetch_goes()

    # View 3 -- VIIRS: per-swath multi-pass, genuinely sub-daily
    fetch_viirs_passes()

    print("\n" + "=" * 57)
    print("✓ Pipeline complete")
    print("=" * 57)


if __name__ == "__main__":
    main()
