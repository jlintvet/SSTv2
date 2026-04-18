"""
High-Fidelity SST Data Fetcher — App-Ready Output
===================================================
Fetches SST data from the top highest-resolution free sources and exports
structured data (GeoJSON, CSV, Parquet, Grid JSON) for rendering in a web/mobile app.
Sources (most→least real-time):
  1. GOES-19 ABI SST    — ~2 km, geostationary IR, ~3–6 hr lag (cloud gaps, no fill)
     Source:  NOAA/AOML ERDDAP (cwcgom.aoml.noaa.gov)
  2. NOAA VIIRS NRT     — ~1–4 km, near-real-time, hourly passes per day
     Primary:  CoastWatch THREDDS catalog (all hourly passes, up to 5 days)
     Fallback: ERDDAP blended products
  3. NASA MUR SST       — 0.01° (~1 km), L4 blended, gap-filled, daily ~1–2 day lag
     Primary:  NASA PO.DAAC OPeNDAP
     Fallback: ERDDAP mirrors (polarwatch, USF, upwell, ifremer, pfeg)
               + csvp format with mask-based land/lake/ice/tidal filtering
  4. Copernicus CMEMS   — 0.05° (~5 km), L4 blended, fully gap-filled, ~1 day lag
     Requires CMEMS_USER and CMEMS_PASSWORD env vars.
Output directory: SSTv2/DailySST/
Output formats (your choice via EXPORT dict):
  - GeoJSON FeatureCollection  → Mapbox / Leaflet / deck.gl
  - CSV                        → pandas / PostGIS / any DB ingest
  - Parquet                    → high-performance columnar
  - Raw numpy grid + metadata  → custom tile renderer / WebGL
Dependencies:
    pip install requests xarray netCDF4 numpy pandas pyarrow copernicusmarine
"""
import os
import csv
import io
import json
import datetime
import warnings
import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import xarray as xr
from pathlib import Path
warnings.filterwarnings("ignore")
# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
BBOX = {
    "lon_min": -78.89,
    "lon_max": -72.21,
    "lat_min": 33.70,
    "lat_max": 39.00,
}
# Defaults to yesterday — best data availability across all sources
_date_override = os.environ.get("TARGET_DATE_OVERRIDE", "").strip()
TARGET_DATE = (
    datetime.date.fromisoformat(_date_override)
    if _date_override
    else datetime.date.today() - datetime.timedelta(days=1)
)
# Sources to run — override via SOURCES_OVERRIDE env var
# Values: all | mur_only | viirs_only | cmems_only | goes19_only
SOURCES_OVERRIDE = os.environ.get("SOURCES_OVERRIDE", "all").strip()
# How many recent datasets to retrieve per source.
# For GOES-19 (hourly): up to MAX_DATASETS hourly snapshots (can be same day).
# For VIIRS: all hourly passes across up to MAX_DATASETS days.
# For daily sources (MUR, CMEMS): up to MAX_DATASETS days.
MAX_DATASETS = 5
OUTPUT_DIR = Path("SSTv2") / "DailySST"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
# Output formats
EXPORT = {
    "geojson": True,
    "csv":     True,
    "parquet": True,
    "grid":    True,
}
GEOJSON_MAX_POINTS = None
# Shared request settings
TIMEOUT_SECONDS = 180
MAX_RETRIES     = 2
BACKOFF_FACTOR  = 1
# ─────────────────────────────────────────────────────────────────────────────
# SESSION
# ─────────────────────────────────────────────────────────────────────────────
def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session
_SESSION = _make_session()
# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1: NASA MUR SST
#   Resolution : 0.01° × 0.01° (~1 km)
#   Latency    : ~1–2 days
#   Dataset ID : jplMURSST41
# ─────────────────────────────────────────────────────────────────────────────
# ERDDAP hosts tried for .nc format (NetCDF download)
MUR_ERDDAP_HOSTS_NC = [
    "https://polarwatch.noaa.gov/erddap/griddap",      # PolarWatch — CI-friendly
    "https://erddap.marine.usf.edu/erddap/griddap",    # USF mirror
    "https://upwell.pfeg.noaa.gov/erddap/griddap",     # upwell — same NOAA group, more open
    "https://erddap.ifremer.fr/erddap/griddap",        # French mirror — reliable from CI
    "https://coastwatch.pfeg.noaa.gov/erddap/griddap", # West Coast (may 403 from CI)
]
# ERDDAP hosts tried for csvp format (from DailySSTRetrieval pipeline)
# csvp is lighter than NC and supports the mask field for precise land filtering
MUR_ERDDAP_HOSTS_CSVP = [
    "https://upwell.pfeg.noaa.gov/erddap/griddap/jplMURSST41.csvp",
    "https://erddap.ifremer.fr/erddap/griddap/jplMURSST41.csvp",
    "https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41.csvp",
]
MUR_VARIABLES_CSVP = ["analysed_sst", "analysis_error", "sea_ice_fraction", "mask"]
MUR_DAILY_HOUR     = "09:00:00Z"
def _latest_available_dates(base_date: datetime.date, max_lookback: int = None) -> list:
    """Return list of dates from base_date back by max_lookback days (inclusive)."""
    lookback = max_lookback if max_lookback is not None else (MAX_DATASETS - 1)
    return [base_date - datetime.timedelta(days=i) for i in range(lookback + 1)]
def _fahrenheit(val) -> "float | None":
    try:
        c = float(val)
        if c != c or c < -3.0 or c > 40.0:
            return None
        return round(c * 9 / 5 + 32, 4)
    except (ValueError, TypeError):
        return None
def _float_val(val) -> "float | None":
    try:
        f = float(val)
        return None if f != f else round(f, 6)
    except (ValueError, TypeError):
        return None
def _parse_mur_csvp(text: str, bbox: dict) -> pd.DataFrame:
    """
    Parse ERDDAP csvp response for MUR SST.
    MUR mask values:
      1 = open ocean  ← keep SST
      2 = land        ← null SST
      3 = lake        ← null SST
      4 = ice         ← null SST
      5 = tidal/estuarine ← null SST (Pamlico Sound, Albemarle Sound, etc.)
    Returns a DataFrame with lat, lon, sst_c, source, date.
    sst is stored as Celsius (converted from Fahrenheit intermediate).
    """
    reader   = csv.reader(io.StringIO(text))
    rows_raw = list(reader)
    if len(rows_raw) < 3:
        return pd.DataFrame()
    headers = [h.split(" (")[0].strip() for h in rows_raw[0]]
    idx     = {name: i for i, name in enumerate(headers)}
    records = []
    for raw in rows_raw[2:]:
        if len(raw) < len(headers):
            continue
        lat = _float_val(raw[idx.get("latitude",  -1)]) if "latitude"  in idx else None
        lon = _float_val(raw[idx.get("longitude", -1)]) if "longitude" in idx else None
        if lat is None or lon is None:
            continue
        # Mask: only keep open-ocean cells (mask == 1)
        mask_val = None
        if "mask" in idx and raw[idx["mask"]] not in ("", "NaN"):
            try:
                mask_val = int(float(raw[idx["mask"]]))
            except (ValueError, TypeError):
                pass
        is_ocean = (mask_val == 1)
        sst_raw = raw[idx["analysed_sst"]] if "analysed_sst" in idx else None
        sst_f   = _fahrenheit(sst_raw) if (sst_raw is not None and is_ocean) else None
        sst_c   = round((sst_f - 32) * 5 / 9, 4) if sst_f is not None else None
        if sst_c is not None:
            records.append({"lat": lat, "lon": lon, "sst_c": sst_c,
                            "source": "MUR", "date": str(TARGET_DATE)})
    return pd.DataFrame(records)
def _build_mur_csvp_url(base: str, date: datetime.date, bbox: dict) -> str:
    ts        = f"{date.isoformat()}T{MUR_DAILY_HOUR}"
    time_part = f"[({ts}):1:({ts})]"
    lat_part  = f"[({bbox['lat_min']}):1:({bbox['lat_max']})]"
    lon_part  = f"[({bbox['lon_min']}):1:({bbox['lon_max']})]"
    var_q     = ",".join(
        f"{v}{time_part}{lat_part}{lon_part}" for v in MUR_VARIABLES_CSVP
    )
    return f"{base}?{var_q}"
def _fetch_mur_single(try_date: datetime.date, bbox: dict) -> "pd.DataFrame | None":
    """
    Fetch MUR SST for a single date. Returns DataFrame or None.
    Attempt order: csvp (upwell/ifremer) → NASA OPeNDAP → NC mirrors.
    """
    # ── csvp — upwell / ifremer ───────────────────────────────────────────────
    for base_url in MUR_ERDDAP_HOSTS_CSVP:
        url = _build_mur_csvp_url(base_url, try_date, bbox)
        try:
            host_label = base_url.split("/")[2]
            print(f"    {host_label} csvp {try_date} ...")
            r = _SESSION.get(url, timeout=TIMEOUT_SECONDS)
            r.raise_for_status()
            df = _parse_mur_csvp(r.text, bbox)
            if not df.empty:
                print(f"    ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  ({host_label} csvp)")
                return df
        except Exception as e:
            print(f"    ✗ {base_url.split('/')[2]} csvp: {e}")
    # ── NASA PO.DAAC OPeNDAP ─────────────────────────────────────────────────
    date_str    = try_date.strftime("%Y%m%d")
    opendap_url = (
        f"https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections"
        f"/MUR-JPL-L4-GLOB-v4.1/granules"
        f"/{date_str}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"
    )
    try:
        print(f"    NASA OPeNDAP {try_date} ...")
        ce = (
            f"?analysed_sst[0:1:0]"
            f"[{_lat_idx(bbox['lat_min'])}:{_lat_idx(bbox['lat_max'])}]"
            f"[{_lon_idx(bbox['lon_min'])}:{_lon_idx(bbox['lon_max'])}]"
        )
        ds = xr.open_dataset(opendap_url + ".nc4" + ce, engine="netcdf4")
        df = _ds_to_dataframe(ds, "analysed_sst", "MUR", try_date)
        print(f"    ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  (OPeNDAP)")
        return df
    except Exception as e:
        print(f"    ✗ NASA OPeNDAP: {e}")
    # ── ERDDAP mirrors — NetCDF (.nc) ─────────────────────────────────────────
    for host in MUR_ERDDAP_HOSTS_NC:
        url = (
            f"{host}/jplMURSST41.nc"
            f"?analysed_sst"
            f"[({try_date}T{MUR_DAILY_HOUR})]"
            f"[({bbox['lat_min']}):1:({bbox['lat_max']})]"
            f"[({bbox['lon_min']}):1:({bbox['lon_max']})]"
        )
        nc_path = OUTPUT_DIR / f"_mur_{try_date}.nc"
        try:
            host_label = host.split("/")[2]
            print(f"    {host_label} .nc {try_date} ...")
            r = _SESSION.get(url, timeout=TIMEOUT_SECONDS, stream=True)
            r.raise_for_status()
            with open(nc_path, "wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
            ds = xr.open_dataset(nc_path)
            df = _ds_to_dataframe(ds, "analysed_sst", "MUR", try_date)
            print(f"    ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  ({host_label})")
            return df
        except Exception as e:
            print(f"    ✗ {host.split('/')[2]} .nc: {e}")
    return None
def fetch_mur(date: datetime.date, bbox: dict) -> "list[tuple[pd.DataFrame, str]]":
    """
    Collect up to MAX_DATASETS daily MUR SST results.
    Returns list of (df, 'YYYYMMDD') tuples, most-recent first.
    """
    print(f"\n[4/4] NASA MUR SST  (target: {date}, collecting up to {MAX_DATASETS} days)  ~1 km")
    results = []
    for try_date in _latest_available_dates(date):
        if len(results) >= MAX_DATASETS:
            break
        print(f"  Date {try_date}:")
        df = _fetch_mur_single(try_date, bbox)
        if df is not None:
            results.append((df, try_date.strftime("%Y%m%d")))
        else:
            print(f"  ✗ No MUR data for {try_date}")
    print(f"  MUR: {len(results)}/{MAX_DATASETS} datasets retrieved")
    return results
def _lat_idx(lat: float) -> int:
    return int(round((lat + 89.99) / 0.01))
def _lon_idx(lon: float) -> int:
    return int(round((lon + 179.99) / 0.01))
# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2: GOES-19 ABI SST (NOAA/AOML)
#   Resolution : ~2 km (ABI full-disk, sub-sampled per stride)
#   Latency    : ~3–6 hours from observation — most real-time source
#   Dataset ID : goes19SSThourly  (hourly composites)
#   Host       : https://cwcgom.aoml.noaa.gov/erddap/griddap
#   Notes      : IR-only — cloud pixels are null (no gap-fill). Land pixels
#                are also null directly from the sensor.
# ─────────────────────────────────────────────────────────────────────────────
GOES19_ERDDAP_CSVP = "https://cwcgom.aoml.noaa.gov/erddap/griddap/goes19SSThourly.csvp"
GOES19_VARIABLE    = "sst"
GOES19_STRIDE      = 1
# Hours to probe (UTC), ordered by likelihood of having useful daytime data
# over the US East Coast study area. Morning-to-afternoon passes prioritized.
GOES19_HOURS_ORDERED = [14, 15, 13, 16, 12, 17, 11, 18, 10, 19, 9, 20, 8, 21, 7, 22, 6, 23, 5, 4, 3, 2, 1, 0]
def _build_goes19_url(date: datetime.date, hour: int, bbox: dict) -> str:
    ts        = f"{date.isoformat()}T{hour:02d}:00:00Z"
    time_part = f"[({ts}):1:({ts})]"
    lat_part  = f"[({bbox['lat_min']}):{GOES19_STRIDE}:({bbox['lat_max']})]"
    lon_part  = f"[({bbox['lon_min']}):{GOES19_STRIDE}:({bbox['lon_max']})]"
    return f"{GOES19_ERDDAP_CSVP}?{GOES19_VARIABLE}{time_part}{lat_part}{lon_part}"
def _parse_goes19_csvp(text: str, date: datetime.date) -> pd.DataFrame:
    """
    Parse GOES-19 hourly SST csvp response.
    GOES-19 ABI SST values are in Celsius. Land and cloud pixels are NaN
    directly from the sensor — no secondary masking required.
    """
    reader   = csv.reader(io.StringIO(text))
    rows_raw = list(reader)
    if len(rows_raw) < 3:
        return pd.DataFrame()
    headers = [h.split(" (")[0].strip() for h in rows_raw[0]]
    idx     = {name: i for i, name in enumerate(headers)}
    records = []
    for raw in rows_raw[2:]:
        if len(raw) < len(headers):
            continue
        lat = _float_val(raw[idx.get("latitude",  -1)]) if "latitude"  in idx else None
        lon = _float_val(raw[idx.get("longitude", -1)]) if "longitude" in idx else None
        if lat is None or lon is None:
            continue
        sst_col = idx.get(GOES19_VARIABLE)
        sst_c   = None
        if sst_col is not None and raw[sst_col] not in ("", "NaN"):
            try:
                c = float(raw[sst_col])
                if -3.0 <= c <= 40.0:   # valid SST range in Celsius
                    sst_c = round(c, 4)
            except (ValueError, TypeError):
                pass
        if sst_c is not None:
            records.append({"lat": lat, "lon": lon, "sst_c": sst_c,
                            "source": "GOES19", "date": str(date)})
    return pd.DataFrame(records)
def _probe_goes19_server() -> bool:
    """
    Quick single-point probe to check if cwcgom.aoml.noaa.gov is reachable.
    Returns True if the server responds (even with no-data), False if down/blocked.
    """
    url = (
        f"{GOES19_ERDDAP_CSVP}"
        f"?sst[(2026-01-01T14:00:00Z):1:(2026-01-01T14:00:00Z)]"
        f"[(35.0):1:(35.0)][(-75.0):1:(-75.0)]"
    )
    try:
        r = _SESSION.get(url, timeout=15)
        # 200 (data) or 404/400 (no data for date) both mean the server is up
        return r.status_code in (200, 400, 404)
    except Exception:
        return False
def fetch_goes19(date: datetime.date, bbox: dict) -> "list[tuple[pd.DataFrame, str]]":
    """
    Collect up to MAX_DATASETS hourly GOES-19 ABI SST snapshots.
    Returns list of (df, 'YYYYMMDD_HH') tuples, most-recent first.
    Searches hours in daytime-first order (14 UTC → outward) across the
    last 2 calendar days. Multiple snapshots from the same day are fine —
    each represents a different hourly observation.
    Strategy:
      1. Quick server probe (15s) — skip all if cwcgom is unreachable.
      2. Collect up to MAX_DATASETS successful hours, keeping unique timestamps.
    """
    print(f"\n[1/4] GOES-19 ABI SST  (target: {date}, collecting up to {MAX_DATASETS} hourly snapshots)  ~2 km")
    # Fast server probe — avoid 24+ slow timeouts if AOML is down
    print("  Probing cwcgom.aoml.noaa.gov ...")
    if not _probe_goes19_server():
        print("  ✗ GOES-19 server unreachable — skipping")
        return []
    # Search up to 2 calendar days (today + yesterday) — hourly data older
    # than 48 hours is less actionable than daily VIIRS/CMEMS.
    lookback_dates = _latest_available_dates(date, max_lookback=1)
    results = []
    for try_date in lookback_dates:
        for hour in GOES19_HOURS_ORDERED:
            if len(results) >= MAX_DATASETS:
                break
            url = _build_goes19_url(try_date, hour, bbox)
            try:
                print(f"  Trying {try_date} {hour:02d}:00Z ...")
                r = _SESSION.get(url, timeout=40)
                r.raise_for_status()
                df = _parse_goes19_csvp(r.text, try_date)
                if df.empty:
                    print(f"  ✗ {try_date} {hour:02d}:00Z: no ocean SST rows (cloud cover?)")
                    continue
                label = f"{try_date.strftime('%Y%m%d')}_{hour:02d}"
                print(f"  ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  "
                      f"({try_date} {hour:02d}:00Z)")
                results.append((df, label))
            except Exception as e:
                print(f"  ✗ GOES-19 {try_date} {hour:02d}:00Z: {e}")
        if len(results) >= MAX_DATASETS:
            break
    print(f"  GOES-19: {len(results)}/{MAX_DATASETS} hourly snapshots retrieved")
    return results
# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3: NOAA CoastWatch VIIRS SST (NOAA-20, hourly passes)
#   Resolution : ~1–4 km
#   Latency    : ~6–12 hours NRT
#   Strategy   : Collect ALL hourly passes from the THREDDS catalog for each
#                of the last MAX_DATASETS days. Each .nc file in the catalog
#                is a separate orbital overpass — grabbing all of them gives
#                maximum spatial coverage for compositing later.
# ─────────────────────────────────────────────────────────────────────────────
def _fetch_viirs_passes(try_date: datetime.date, bbox: dict) -> "list[tuple[pd.DataFrame, str]]":
    """
    Fetch ALL available VIIRS hourly passes for a single date from the
    THREDDS catalog. Returns list of (df, 'YYYYMMDD_HH') tuples.
    Each .nc file in the daily catalog is one orbital overpass; filenames
    encode the observation time (e.g. 20260416140000 → 14:00 UTC).
    """
    import re
    doy  = try_date.timetuple().tm_yday
    year = try_date.year
    catalog_url = (
        f"https://coastwatch.noaa.gov/thredds/catalog/gridN20VIIRSSCIENCEL3UWW00"
        f"/{year}/{doy:03d}/catalog.xml"
    )
    try:
        print(f"    THREDDS catalog DOY {doy:03d} ({try_date}) ...")
        resp = _SESSION.get(catalog_url, timeout=30)
        resp.raise_for_status()
        matches = re.findall(r'gridN20VIIRSSCIENCEL3UWW00/[^"]+\.nc', resp.text)
        if not matches:
            print(f"    ✗ No .nc files in catalog for {try_date}")
            return []
        print(f"    Found {len(matches)} pass(es) for {try_date}")
    except Exception as e:
        print(f"    ✗ THREDDS catalog {try_date}: {e}")
        return []

    results = []
    for nc_path_match in sorted(matches):
        nc_name     = nc_path_match.split("/")[-1]
        opendap_url = (
            f"https://coastwatch.noaa.gov/thredds/dodsC/gridN20VIIRSSCIENCEL3UWW00"
            f"/{year}/{doy:03d}/{nc_name}"
        )
        # Extract UTC hour from filename timestamp (e.g. 20260416140000 → 14)
        hour_match = re.search(r'(\d{8})(\d{2})\d{4}', nc_name)
        hour       = int(hour_match.group(2)) if hour_match else 0
        label      = f"{try_date.strftime('%Y%m%d')}_{hour:02d}"
        try:
            print(f"    Opening pass {hour:02d}:00Z: {nc_name} ...")
            ds       = xr.open_dataset(opendap_url, engine="netcdf4")
            lat_name = next(c for c in ds.coords if "lat" in c.lower())
            lon_name = next(c for c in ds.coords if "lon" in c.lower())
            ds = ds.sel({
                lat_name: slice(bbox["lat_min"], bbox["lat_max"]),
                lon_name: slice(bbox["lon_min"], bbox["lon_max"]),
            })
            var = next((v for v in ds.data_vars if "sst" in v.lower()), None)
            if var is None:
                print(f"    ✗ {nc_name}: no SST variable found in {list(ds.data_vars)}")
                continue
            df = _ds_to_dataframe(ds, var, "VIIRS", try_date)
            if df.empty:
                print(f"    ✗ {label}: no valid SST pixels in bbox (cloud cover?)")
                continue
            print(f"    ✓ {label}  {len(df):,} pts  "
                  f"{df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C")
            results.append((df, label))
        except Exception as e:
            print(f"    ✗ {label}: {e}")

    return results


def _fetch_viirs_erddap_fallback(try_date: datetime.date, bbox: dict) -> "pd.DataFrame | None":
    """
    ERDDAP blended fallback for a single date when THREDDS catalog has no data.
    Returns a single DataFrame (daily composite) or None.
    """
    erddap_fallbacks = [
        ("https://coastwatch.noaa.gov/erddap/griddap", "noaacrwsstDaily",         "analysed_sst"),
        ("https://coastwatch.noaa.gov/erddap/griddap", "noaacwBLENDEDsstDLDaily", "analysed_sst"),
    ]
    for base, dataset_id, var in erddap_fallbacks:
        url = (
            f"{base}/{dataset_id}.nc?{var}"
            f"[({try_date}T12:00:00Z)]"
            f"[({bbox['lat_min']}):1:({bbox['lat_max']})]"
            f"[({bbox['lon_min']}):1:({bbox['lon_max']})]"
        )
        nc_path = OUTPUT_DIR / f"_viirs_{try_date}.nc"
        try:
            print(f"    {dataset_id} ({try_date}) ...")
            r = _SESSION.get(url, timeout=TIMEOUT_SECONDS, stream=True)
            r.raise_for_status()
            with open(nc_path, "wb") as fh:
                for chunk in r.iter_content(1 << 20):
                    fh.write(chunk)
            ds = xr.open_dataset(nc_path)
            df = _ds_to_dataframe(ds, var, "VIIRS", try_date)
            print(f"    ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  ({dataset_id})")
            return df
        except Exception as e:
            print(f"    ✗ {dataset_id}: {e}")
    return None


def fetch_viirs(date: datetime.date, bbox: dict) -> "list[tuple[pd.DataFrame, str]]":
    """
    Collect all available VIIRS hourly passes across the last MAX_DATASETS days.
    Each orbital pass is returned as a separate (df, 'YYYYMMDD_HH') tuple so
    they can be written individually and used for post-processing composites.

    For days where the THREDDS catalog yields nothing (e.g. very recent dates
    not yet processed), falls back to ERDDAP blended daily products.

    Typical yield: ~3–6 passes/day × 5 days = 15–30 total passes.
    """
    print(f"\n[2/4] NOAA VIIRS SST  "
          f"(target: {date}, all hourly passes across last {MAX_DATASETS} days)  ~1–4 km")
    all_results = []
    for try_date in _latest_available_dates(date):
        print(f"  Date {try_date}:")
        passes = _fetch_viirs_passes(try_date, bbox)
        if passes:
            all_results.extend(passes)
        else:
            # THREDDS had nothing — try ERDDAP blended as a fallback
            print(f"    No THREDDS passes — trying ERDDAP fallback ...")
            df = _fetch_viirs_erddap_fallback(try_date, bbox)
            if df is not None:
                all_results.append((df, try_date.strftime("%Y%m%d")))
            else:
                print(f"  ✗ No VIIRS data for {try_date}")

    total_passes = len(all_results)
    total_pts    = sum(len(df) for df, _ in all_results)
    print(f"  VIIRS: {total_passes} passes retrieved across last {MAX_DATASETS} days  "
          f"({total_pts:,} total pts)")
    return all_results
# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 4: Copernicus CMEMS L4 Global SST
#   Resolution : 0.05° (~5 km) — fully gap-filled
#   Latency    : ~1 day
#   Auth       : CMEMS_USER + CMEMS_PASSWORD env vars
# ─────────────────────────────────────────────────────────────────────────────
def _fetch_cmems_single(try_date: datetime.date, bbox: dict,
                        user: str, pw: str, cm) -> "pd.DataFrame | None":
    """Fetch CMEMS L4 SST for a single date. Returns DataFrame or None."""
    nc_path = OUTPUT_DIR / f"_cmems_{try_date}.nc"
    try:
        print(f"    CMEMS downloading {try_date} ...")
        cm.subset(
            dataset_id="METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2",
            variables=["analysed_sst"],
            minimum_longitude=bbox["lon_min"],
            maximum_longitude=bbox["lon_max"],
            minimum_latitude=bbox["lat_min"],
            maximum_latitude=bbox["lat_max"],
            start_datetime=f"{try_date}T00:00:00",
            end_datetime=f"{try_date}T23:59:59",
            output_filename=str(nc_path),
            username=user,
            password=pw,
            overwrite=True,
        )
        ds = xr.open_dataset(nc_path)
        df = _ds_to_dataframe(ds, "analysed_sst", "CMEMS", try_date)
        print(f"    ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C")
        return df
    except Exception as e:
        err = str(e).lower()
        if "credential" in err or "password" in err or "username" in err:
            print(f"    ✗ CMEMS auth failed — check CMEMS_USER/CMEMS_PASSWORD: {e}")
        else:
            print(f"    ✗ CMEMS fetch failed: {e}")
        return None
def fetch_cmems(date: datetime.date, bbox: dict) -> "list[tuple[pd.DataFrame, str]]":
    """
    Collect up to MAX_DATASETS daily CMEMS L4 SST results.
    Returns list of (df, 'YYYYMMDD') tuples, most-recent first.
    """
    print(f"\n[3/4] Copernicus CMEMS L4 SST  (target: {date}, collecting up to {MAX_DATASETS} days)  ~5 km")
    user = os.environ.get("CMEMS_USER")
    pw   = os.environ.get("CMEMS_PASSWORD")
    if not user or not pw:
        print("  ✗ Skipped — set CMEMS_USER and CMEMS_PASSWORD env vars")
        return []
    try:
        import copernicusmarine as cm
    except ImportError:
        print("  ✗ Run: pip install copernicusmarine")
        return []
    results = []
    for try_date in _latest_available_dates(date):
        if len(results) >= MAX_DATASETS:
            break
        print(f"  Date {try_date}:")
        df = _fetch_cmems_single(try_date, bbox, user, pw, cm)
        if df is not None:
            results.append((df, try_date.strftime("%Y%m%d")))
        else:
            print(f"  ✗ No CMEMS data for {try_date}")
    print(f"  CMEMS: {len(results)}/{MAX_DATASETS} datasets retrieved")
    return results
# ─────────────────────────────────────────────────────────────────────────────
# BLENDING
# ─────────────────────────────────────────────────────────────────────────────
def blend_sources(frames: list) -> pd.DataFrame:
    """
    Merges multiple source DataFrames into a single best-available grid.
    Priority: GOES19 > VIIRS > MUR > CMEMS. All sources snapped to 0.05° grid.
    GOES-19 is most real-time (3-6hr) but has cloud gaps; CMEMS is fully
    gap-filled and fills any remaining nulls in the blended output.
    """
    if not frames:
        return pd.DataFrame()
    priority          = ["GOES19", "VIIRS", "MUR", "CMEMS"]
    frames_by_source  = {df["source"].iloc[0]: df for df in frames}
    grid_res = 0.05
    lons = np.arange(
        round(min(df["lon"].min() for df in frames)),
        round(max(df["lon"].max() for df in frames)) + grid_res,
        grid_res,
    )
    lats = np.arange(
        round(min(df["lat"].min() for df in frames)),
        round(max(df["lat"].max() for df in frames)) + grid_res,
        grid_res,
    )
    lon2d, lat2d = np.meshgrid(lons, lats)
    blended = np.full(lon2d.shape, np.nan)
    for source in reversed(priority):
        if source not in frames_by_source:
            continue
        df = frames_by_source[source].copy()
        df["lon_snap"] = (df["lon"] / grid_res).round() * grid_res
        df["lat_snap"] = (df["lat"] / grid_res).round() * grid_res
        pivot = df.pivot_table(index="lat_snap", columns="lon_snap",
                               values="sst_c", aggfunc="mean")
        for i, la in enumerate(lats):
            for j, lo in enumerate(lons):
                try:
                    val = pivot.at[round(la, 6), round(lo, 6)]
                    if not np.isnan(val):
                        blended[i, j] = val
                except KeyError:
                    pass
    mask = np.isfinite(blended)
    return pd.DataFrame({
        "lat":    lat2d[mask].round(5),
        "lon":    lon2d[mask].round(5),
        "sst_c":  blended[mask].round(3),
        "source": "blended",
        "date":   str(TARGET_DATE),
    })
# ─────────────────────────────────────────────────────────────────────────────
# EXPORTERS
# ─────────────────────────────────────────────────────────────────────────────
def export_geojson(df: pd.DataFrame, path: Path, max_points=None):
    if max_points and len(df) > max_points:
        df = df.sample(max_points, random_state=42)
    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point",
                         "coordinates": [round(float(r.lon), 5), round(float(r.lat), 5)]},
            "properties": {"sst_c":  round(float(r.sst_c), 3),
                           "sst_f":  round(float(r.sst_c) * 9/5 + 32, 2),
                           "source": r.source,
                           "date":   r.date},
        }
        for r in df.itertuples(index=False)
    ]
    fc = {
        "type": "FeatureCollection",
        "metadata": {
            "generated":   datetime.datetime.utcnow().isoformat() + "Z",
            "date":        str(TARGET_DATE),
            "bbox":        BBOX,
            "point_count": len(features),
            "source":      df["source"].iloc[0] if len(df) else "unknown",
        },
        "features": features,
    }
    with open(path, "w") as fh:
        json.dump(fc, fh, separators=(",", ":"))
    print(f"    GeoJSON  → {path}  ({len(features):,} pts, {path.stat().st_size/1024:.0f} KB)")
def export_csv(df: pd.DataFrame, path: Path):
    out = df.copy()
    out["sst_f"] = (out["sst_c"] * 9/5 + 32).round(2)
    out.to_csv(path, index=False)
    print(f"    CSV      → {path}  ({len(out):,} rows, {path.stat().st_size/1024:.0f} KB)")
def export_parquet(df: pd.DataFrame, path: Path):
    try:
        out = df.copy()
        out["sst_f"] = (out["sst_c"] * 9/5 + 32).round(2)
        out.to_parquet(path, index=False, compression="snappy")
        print(f"    Parquet  → {path}  ({len(out):,} rows, {path.stat().st_size/1024:.0f} KB)")
    except ImportError:
        print("    Parquet skipped — run: pip install pyarrow")
class _NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.floating, np.float32, np.float64)):
            return float(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)
def export_grid(df: pd.DataFrame, path: Path):
    lats = sorted(float(v) for v in df["lat"].unique())
    lons = sorted(float(v) for v in df["lon"].unique())
    lat_idx = {v: i for i, v in enumerate(lats)}
    lon_idx = {v: i for i, v in enumerate(lons)}
    grid = [[None] * len(lons) for _ in range(len(lats))]
    for r in df.itertuples(index=False):
        i = lat_idx.get(float(r.lat))
        j = lon_idx.get(float(r.lon))
        if i is not None and j is not None:
            grid[i][j] = round(float(r.sst_c), 3)
    out = {
        "meta": {
            "date":    str(TARGET_DATE),
            "bbox":    BBOX,
            "res_deg": round(lats[1] - lats[0], 5) if len(lats) > 1 else None,
            "n_lats":  len(lats),
            "n_lons":  len(lons),
            "source":  df["source"].iloc[0],
        },
        "lats": [round(v, 5) for v in lats],
        "lons": [round(v, 5) for v in lons],
        "sst":  grid,
    }
    with open(path, "w") as fh:
        json.dump(out, fh, separators=(",", ":"), cls=_NumpyEncoder)
    print(f"    Grid JSON → {path}  ({len(lats)}×{len(lons)}, {path.stat().st_size/1024:.0f} KB)")
# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _ds_to_dataframe(ds: xr.Dataset, var: str, source: str,
                     date: datetime.date) -> pd.DataFrame:
    da = ds[var].squeeze()
    for dim in list(da.dims):
        if da.sizes[dim] == 1:
            da = da.isel({dim: 0})
    lat_name = next((c for c in da.coords if "lat" in c.lower()), None)
    lon_name = next((c for c in da.coords if "lon" in c.lower()), None)
    if not lat_name or not lon_name:
        raise ValueError(f"Cannot find lat/lon coords in {list(da.coords)}")
    lats = da[lat_name].values
    lons = da[lon_name].values
    vals = da.values
    lon2d, lat2d = np.meshgrid(lons, lats)
    flat_lat = lat2d.flatten()
    flat_lon = lon2d.flatten()
    flat_sst = vals.flatten()
    mask = np.isfinite(flat_sst)
    sst_c = flat_sst[mask]
    if sst_c.mean() > 200:
        sst_c = sst_c - 273.15
    return pd.DataFrame({
        "lat":    flat_lat[mask].round(5),
        "lon":    flat_lon[mask].round(5),
        "sst_c":  sst_c.round(3),
        "source": source,
        "date":   str(date),
    })
def _write_all(df: pd.DataFrame, label: str, date_label: str = None):
    """
    Write all export formats for a single dataset.
    date_label: filename suffix, e.g. '20260417' (daily) or '20260417_14' (hourly pass).
    Defaults to TARGET_DATE if not provided.
    """
    dl   = date_label or TARGET_DATE.strftime("%Y%m%d")
    base = OUTPUT_DIR / f"{label}_{dl}"
    print(f"  Exporting {label} {dl} ({len(df):,} points) ...")
    if EXPORT["geojson"]:
        export_geojson(df, base.with_suffix(".geojson"), GEOJSON_MAX_POINTS)
    if EXPORT["csv"]:
        export_csv(df, base.with_suffix(".csv"))
    if EXPORT["parquet"]:
        export_parquet(df, base.with_suffix(".parquet"))
    if EXPORT["grid"]:
        export_grid(df, Path(str(base) + "_grid.json"))
# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  High-Fidelity SST Data Fetcher")
    print(f"  Date      : {TARGET_DATE}")
    print(f"  BBOX      : {BBOX}")
    print(f"  Output    : {OUTPUT_DIR.resolve()}")
    print(f"  Sources   : {SOURCES_OVERRIDE}")
    print(f"  Datasets  : up to {MAX_DATASETS} per source")
    print("=" * 60)
    run_goes19 = SOURCES_OVERRIDE in ("all", "goes19_only")
    run_viirs  = SOURCES_OVERRIDE in ("all", "viirs_only")
    run_mur    = SOURCES_OVERRIDE in ("all", "mur_only")
    run_cmems  = SOURCES_OVERRIDE in ("all", "cmems_only")
    # ── Fetch all sources (each returns list of (df, date_label) tuples) ──────
    goes19_results = fetch_goes19(TARGET_DATE, BBOX) if run_goes19 else []
    viirs_results  = fetch_viirs(TARGET_DATE, BBOX)  if run_viirs  else []
    mur_results    = fetch_mur(TARGET_DATE, BBOX)    if run_mur    else []
    cmems_results  = fetch_cmems(TARGET_DATE, BBOX)  if run_cmems  else []
    # ── Write individual source files ─────────────────────────────────────────
    print("\n[Writing individual source files ...]")
    for df, label in goes19_results:
        _write_all(df, "goes19", label)
    for df, label in viirs_results:
        _write_all(df, "viirs", label)
    for df, label in mur_results:
        _write_all(df, "mur", label)
    for df, label in cmems_results:
        _write_all(df, "cmems", label)
    # ── Blended composite — one per calendar date ─────────────────────────────
    # Group most-recent result per source by their calendar date (YYYYMMDD),
    # then blend whatever sources are available for each date.
    print("\n[Building blended composites per date ...]")
    # Map date_label → {source: df} — use only daily-resolution date (first 8 chars)
    # so GOES-19 and VIIRS hourly slots (YYYYMMDD_HH) align with daily sources (YYYYMMDD).
    date_map: "dict[str, dict[str, pd.DataFrame]]" = {}
    def _register(results, source_key):
        for df, label in results:
            day = label[:8]   # YYYYMMDD prefix
            date_map.setdefault(day, {})[source_key] = df
    _register(goes19_results, "GOES19")
    _register(viirs_results,  "VIIRS")
    _register(mur_results,    "MUR")
    _register(cmems_results,  "CMEMS")
    blended_count = 0
    for day_str in sorted(date_map.keys(), reverse=True):
        day_frames = list(date_map[day_str].values())
        sources_present = list(date_map[day_str].keys())
        if len(day_frames) < 2:
            print(f"  {day_str}: only {sources_present} — skipping blend (need ≥2 sources)")
            continue
        print(f"  {day_str}: blending {sources_present} ...")
        df_blend = blend_sources(day_frames)
        if not df_blend.empty:
            _write_all(df_blend, "blended", day_str)
            blended_count += 1
    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"  {'Source':<8}  {'Passes':>8}  {'Total pts':>10}")
    print(f"  {'-'*8}  {'-'*8}  {'-'*10}")
    summary = [
        ("GOES19", goes19_results),
        ("VIIRS",  viirs_results),
        ("MUR",    mur_results),
        ("CMEMS",  cmems_results),
    ]
    total_files = 0
    for name, res in summary:
        if res:
            pts = sum(len(df) for df, _ in res)
            print(f"  {name:<8}  {len(res):>8}  {pts:>10,}")
            total_files += len(res) * sum(EXPORT.values())
        else:
            print(f"  {name:<8}  {'—':>8}")
    print(f"  {'Blended':<8}  {blended_count:>8}")
    print("=" * 60)
    print(f"\n  Files written to: {OUTPUT_DIR.resolve()}")
if __name__ == "__main__":
    main()
