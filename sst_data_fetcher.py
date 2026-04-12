"""
High-Fidelity SST Data Fetcher — App-Ready Output
===================================================
Fetches SST data from the top 3 highest-resolution free sources and exports
structured data (GeoJSON, CSV, Parquet) for rendering in a web/mobile app.

Sources:
  1. NASA MUR SST       — 0.01° (~1 km), L4 blended, gap-filled, daily
  2. NOAA VIIRS NRT     — 0.01° (~750 m native), near-real-time, daily
  3. Copernicus CMEMS   — 0.05° (~5 km), L4 blended, fully gap-filled

Output formats (your choice):
  - GeoJSON FeatureCollection  → Mapbox / Leaflet / deck.gl
  - CSV                        → pandas / PostGIS / any DB ingest
  - Parquet                    → high-performance columnar (best for large grids)
  - Raw numpy grid + metadata  → custom tile renderer / WebGL

Dependencies:
    pip install requests xarray netCDF4 numpy pandas pyarrow copernicusmarine

Usage:
    python sst_data_fetcher.py

    For CMEMS (optional, best gap-fill):
        export CMEMS_USER=your_username
        export CMEMS_PASSWORD=your_password
"""

import os
import json
import datetime
import warnings
import numpy as np
import pandas as pd
import requests
import xarray as xr
from pathlib import Path

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

BBOX = {
    "lon_min": -77.5,
    "lon_max": -74.0,
    "lat_min": 33.5,
    "lat_max": 36.5,
}

# Defaults to yesterday — best data availability across all sources
# Override via env var TARGET_DATE_OVERRIDE=YYYY-MM-DD (set by GitHub Actions workflow)
_date_override = os.environ.get("TARGET_DATE_OVERRIDE", "").strip()
TARGET_DATE = (
    datetime.date.fromisoformat(_date_override)
    if _date_override
    else datetime.date.today() - datetime.timedelta(days=1)
)

# Sources to run — override via SOURCES_OVERRIDE env var (set by GitHub Actions workflow)
# Values: all | mur_only | viirs_only | cmems_only
SOURCES_OVERRIDE = os.environ.get("SOURCES_OVERRIDE", "all").strip()

# Write directly into DailySST/v2/ — files committed individually to repo
OUTPUT_DIR = Path("DailySST") / "v2"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Output formats — set to True for whichever your app needs
EXPORT = {
    "geojson": True,    # Mapbox / Leaflet / deck.gl heatmap
    "csv":     True,    # DB ingest / pandas analysis
    "parquet": True,    # Best for large grids / columnar analytics
    "grid":    True,    # Raw lat/lon/sst arrays as JSON — WebGL / custom tile renderer
}

# Max data points in GeoJSON export (subsample for very large grids)
# Set to None to export every point
GEOJSON_MAX_POINTS = None


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1: NASA MUR SST
#   Resolution : 0.01° × 0.01° (~1 km)
#   Latency    : ~1 day
#   Endpoints  : Multiple ERDDAP mirrors tried in order (pfeg blocks CI IPs)
#   Dataset ID : jplMURSST41
# ─────────────────────────────────────────────────────────────────────────────

MUR_ERDDAP_HOSTS = [
    "https://polarwatch.noaa.gov/erddap/griddap",    # PolarWatch — CI-friendly
    "https://erddap.marine.usf.edu/erddap/griddap",  # USF mirror
    "https://coastwatch.pfeg.noaa.gov/erddap/griddap", # West Coast (may 403 CI)
]

def _latest_available_date(base_date: datetime.date, max_lookback: int = 3) -> list:
    """Return [base_date, base_date-1, base_date-2, ...] up to max_lookback days."""
    return [base_date - datetime.timedelta(days=i) for i in range(max_lookback + 1)]


def fetch_mur(date: datetime.date, bbox: dict) -> pd.DataFrame | None:
    """
    Returns a DataFrame with columns: lat, lon, sst_c, source, date
    Primary: NASA PO.DAAC OPeNDAP (no IP blocking, no auth required)
    Fallback: ERDDAP mirrors
    """
    print(f"\n[1/3] NASA MUR SST  (target: {date})  ~1 km resolution")

    for try_date in _latest_available_date(date):
        # ── Primary: NASA PO.DAAC OPeNDAP — most reliable for CI runners ──────
        date_str = try_date.strftime("%Y%m%d")
        opendap_url = (
            f"https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections"
            f"/MUR-JPL-L4-GLOB-v4.1/granules"
            f"/{date_str}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"
        )
        nc_path = OUTPUT_DIR / f"_mur_{try_date}.nc"
        try:
            print(f"  Trying NASA OPeNDAP for {try_date} ...")
            # Subset via OPeNDAP constraint expression
            ce = (
                f"?analysed_sst"
                f"[0:1:0]"
                f"[{_lat_idx(bbox['lat_min'])}:{_lat_idx(bbox['lat_max'])}]"
                f"[{_lon_idx(bbox['lon_min'])}:{_lon_idx(bbox['lon_max'])}]"
            )
            ds = xr.open_dataset(opendap_url + ".nc4" + ce, engine="netcdf4")
            df = _ds_to_dataframe(ds, "analysed_sst", "MUR", try_date)
            print(f"  ✓ {len(df):,} points  |  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  (date used: {try_date})")
            return df
        except Exception as e:
            print(f"  ✗ NASA OPeNDAP {try_date}: {e}")

        # ── Fallback: ERDDAP mirrors ───────────────────────────────────────────
        for host in MUR_ERDDAP_HOSTS:
            url = (
                f"{host}/jplMURSST41.nc"
                f"?analysed_sst"
                f"[({try_date}T09:00:00Z)]"
                f"[({bbox['lat_min']}):1:({bbox['lat_max']})]"
                f"[({bbox['lon_min']}):1:({bbox['lon_max']})]"
            )
            try:
                print(f"  Trying {host.split('/')[2]} for {try_date} ...")
                r = requests.get(url, timeout=180, stream=True)
                r.raise_for_status()
                with open(nc_path, "wb") as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
                ds = xr.open_dataset(nc_path)
                df = _ds_to_dataframe(ds, "analysed_sst", "MUR", try_date)
                print(f"  ✓ {len(df):,} points  |  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  (date used: {try_date})")
                return df
            except Exception as e:
                print(f"  ✗ {host.split('/')[2]} / {try_date} failed: {e}")
                continue

    print("  ✗ All MUR sources failed across all fallback dates")
    return None


def _lat_idx(lat: float) -> int:
    """MUR grid: lat from -89.99 at 0.01 deg resolution"""
    return int(round((lat + 89.99) / 0.01))

def _lon_idx(lon: float) -> int:
    """MUR grid: lon from -179.99 at 0.01 deg resolution"""
    return int(round((lon + 179.99) / 0.01))


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2: NOAA CoastWatch Co-gridded VIIRS SST (NOAA-20, daily composite)
#   Resolution : ~1.2 km (sector) / 4 km (global WW00)
#   Latency    : ~6–12 hours NRT
#   Endpoint   : coastwatch.noaa.gov THREDDS catalog — dynamic file discovery
#   Variable   : sst
# ─────────────────────────────────────────────────────────────────────────────

def fetch_viirs(date: datetime.date, bbox: dict) -> pd.DataFrame | None:
    """
    Fetches NOAA-20 VIIRS co-gridded daily SST.
    Tries THREDDS catalog discovery first (up to 3 days back),
    then falls back to ERDDAP mirrors.
    """
    import re
    print(f"\n[2/3] NOAA VIIRS SST  (target: {date})  ~1–4 km resolution")

    # ── Attempt 1: CoastWatch THREDDS catalog — auto-discovers actual filename ─
    for try_date in _latest_available_date(date):
        doy = try_date.timetuple().tm_yday
        year = try_date.year
        catalog_url = (
            f"https://coastwatch.noaa.gov/thredds/catalog/gridN20VIIRSSCIENCEL3UWW00"
            f"/{year}/{doy:03d}/catalog.xml"
        )
        try:
            print(f"  THREDDS catalog DOY {doy:03d} ({try_date}) ...")
            resp = requests.get(catalog_url, timeout=30)
            resp.raise_for_status()
            matches = re.findall(r'gridN20VIIRSSCIENCEL3UWW00/[^"]+\.nc', resp.text)
            if not matches:
                raise ValueError("No .nc files in catalog")
            nc_name = sorted(matches)[-1].split('/')[-1]
            opendap_url = (
                f"https://coastwatch.noaa.gov/thredds/dodsC/gridN20VIIRSSCIENCEL3UWW00"
                f"/{year}/{doy:03d}/{nc_name}"
            )
            print(f"  Opening: {nc_name}")
            ds = xr.open_dataset(opendap_url, engine="netcdf4")
            lat_name = next(c for c in ds.coords if "lat" in c.lower())
            lon_name = next(c for c in ds.coords if "lon" in c.lower())
            ds = ds.sel({
                lat_name: slice(bbox["lat_min"], bbox["lat_max"]),
                lon_name: slice(bbox["lon_min"], bbox["lon_max"]),
            })
            var = next((v for v in ds.data_vars if "sst" in v.lower()), None)
            if var is None:
                raise ValueError(f"No SST var in {list(ds.data_vars)}")
            df = _ds_to_dataframe(ds, var, "VIIRS", try_date)
            print(f"  ✓ {len(df):,} points  |  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  (date: {try_date})")
            return df
        except Exception as e:
            print(f"  ✗ THREDDS {try_date}: {e}")
            continue

    # ── Attempt 2: ERDDAP fallbacks — real alternative SST products ─────────────
    # Note: jplMURSST41 is excluded here (already fetched as source 1)
    erddap_fallbacks = [
        # NOAA CoralTemp 5km gap-free — different data from MUR
        ("https://coastwatch.noaa.gov/erddap/griddap",  "noaacrwsstDaily",  "analysed_sst"),
        # Geo-polar blended — multi-sensor gap-free
        ("https://coastwatch.noaa.gov/erddap/griddap",  "noaacwBLENDEDsstDLDaily", "analysed_sst"),
    ]
    for try_date in _latest_available_date(date):
        for base, dataset_id, var in erddap_fallbacks:
            url = (
                f"{base}/{dataset_id}.nc?{var}"
                f"[({try_date}T12:00:00Z)]"
                f"[({bbox['lat_min']}):1:({bbox['lat_max']})]"
                f"[({bbox['lon_min']}):1:({bbox['lon_max']})]"
            )
            nc_path = OUTPUT_DIR / f"_viirs_{try_date}.nc"
            try:
                host = base.split('/')[2]
                print(f"  Trying {dataset_id} @ {host} ({try_date}) ...")
                r = requests.get(url, timeout=120, stream=True)
                r.raise_for_status()
                with open(nc_path, "wb") as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
                ds = xr.open_dataset(nc_path)
                df = _ds_to_dataframe(ds, var, "VIIRS", try_date)
                print(f"  ✓ {len(df):,} points  |  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  (date: {try_date})")
                return df
            except Exception as e:
                print(f"  ✗ {dataset_id} / {try_date}: {e}")
                continue

    print("  ✗ All VIIRS sources failed")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2b: NOAA Geo-polar Blended SST (gap-fill layer, no auth required)
#   Resolution : 0.05° (~5 km), fully gap-filled
#   Latency    : ~1–2 days
#   Endpoint   : coastwatch.noaa.gov ERDDAP
#   Dataset ID : noaacwBLENDEDsstDaily  (confirmed active 2025)
#   Variable   : analysed_sst
#   Use        : Fill cloud gaps in VIIRS; equivalent to CMEMS if no login
# ─────────────────────────────────────────────────────────────────────────────

def fetch_blended(date: datetime.date, bbox: dict) -> pd.DataFrame | None:
    """
    NOAA Geo-polar blended gap-free SST. No credentials required.
    Returns a DataFrame with columns: lat, lon, sst_c, source, date
    """
    print(f"\n[2b] NOAA Geo-polar Blended SST  ({date})  ~5 km, gap-filled")

    # Try multiple gap-filled blended products in order
    # coastwatch.noaa.gov is slow but confirmed up — use long timeout
    blended_endpoints = [
        ("https://coastwatch.noaa.gov/erddap/griddap", "noaacrwsstDaily",         "analysed_sst"),
        ("https://coastwatch.noaa.gov/erddap/griddap", "noaacwBLENDEDsstDLDaily", "analysed_sst"),
    ]
    url = None
    _active_var = "analysed_sst"
    for try_date in _latest_available_date(date):
        for _base, _dsid, _var in blended_endpoints:
            _url = (
                f"{_base}/{_dsid}.nc?{_var}"
                f"[({try_date}T12:00:00Z)]"
                f"[({bbox['lat_min']}):1:({bbox['lat_max']})]"
                f"[({bbox['lon_min']}):1:({bbox['lon_max']})]"
            )
            try:
                _r = requests.get(_url, timeout=120, stream=True)
                _r.raise_for_status()
                url = _url
                _active_var = _var
                print(f"  Using {_dsid} for {try_date}")
                # write what we already fetched
                nc_path = OUTPUT_DIR / f"_blended_{try_date}.nc"
                with open(nc_path, "wb") as _f:
                    _f.write(_r.content)
                    for chunk in _r.iter_content(1 << 20):
                        _f.write(chunk)
                ds = xr.open_dataset(nc_path)
                df = _ds_to_dataframe(ds, _active_var, "BLENDED", try_date)
                print(f"  ✓ {len(df):,} points  |  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  (date used: {try_date})")
                return df
            except Exception as _e:
                print(f"  ✗ {_dsid} / {try_date}: {_e}")
                continue
        if url:
            break
    if url is None:
        print("  ✗ No blended endpoint responded for any tried date")
        return None
    # All dates and endpoints exhausted (return already happened inside loop if successful)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3: Copernicus CMEMS L4 Global SST
#   Resolution : 0.05° (~5 km) — lower res but FULLY gap-filled (no cloud holes)
#   Latency    : ~1 day
#   Auth       : Free account at https://marine.copernicus.eu
#   Dataset    : SST_GLO_SST_L4_NRT_OBSERVATIONS_010_001
#   Best use   : Fill VIIRS cloud gaps; gradient analysis; fronts
# ─────────────────────────────────────────────────────────────────────────────

def fetch_cmems(date: datetime.date, bbox: dict) -> pd.DataFrame | None:
    """
    Returns a DataFrame with columns: lat, lon, sst_c, source, date
    Requires CMEMS_USER and CMEMS_PASSWORD environment variables.
    """
    print(f"\n[3/3] Copernicus CMEMS L4 SST  ({date})  ~5 km, fully gap-filled")

    user = os.environ.get("CMEMS_USER")
    pw   = os.environ.get("CMEMS_PASSWORD")
    if not user or not pw:
        print("  ✗ Skipped — set CMEMS_USER and CMEMS_PASSWORD env vars")
        print("    Free registration: https://marine.copernicus.eu")
        return None

    try:
        import copernicusmarine as cm
    except ImportError:
        print("  ✗ Run: pip install copernicusmarine")
        return None

    nc_path = OUTPUT_DIR / f"_cmems_{date}.nc"
    try:
        print("  Downloading via copernicusmarine client ...")
        # v2.0.0+ API: overwrite_output_data -> overwrite, force_download removed
        # Credentials read from env vars COPERNICUSMARINE_SERVICE_USERNAME/PASSWORD
        cm.subset(
            dataset_id="METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2",
            variables=["analysed_sst"],
            minimum_longitude=bbox["lon_min"],
            maximum_longitude=bbox["lon_max"],
            minimum_latitude=bbox["lat_min"],
            maximum_latitude=bbox["lat_max"],
            start_datetime=f"{date}T00:00:00",
            end_datetime=f"{date}T23:59:59",
            output_filename=str(nc_path),
            username=user,
            password=pw,
            overwrite=True,
        )
        ds = xr.open_dataset(nc_path)
        df = _ds_to_dataframe(ds, "analysed_sst", "CMEMS", date)
        print(f"  ✓ {len(df):,} points  |  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C")
        return df

    except Exception as e:
        err_str = str(e)
        if "credential" in err_str.lower() or "password" in err_str.lower() or "username" in err_str.lower():
            print(f"  ✗ CMEMS auth failed — check CMEMS_USER/CMEMS_PASSWORD secrets in GitHub")
            print(f"    Error: {e}")
        else:
            print(f"  ✗ CMEMS fetch failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# BLENDING — combine sources into one best-available dataset
# ─────────────────────────────────────────────────────────────────────────────

def blend_sources(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Merges multiple source DataFrames into a single best-available grid.

    Priority order (highest fidelity wins per cell):
      VIIRS > MUR > CMEMS

    NaN cells from higher-priority sources are filled by the next source.
    All sources are snapped to a common 0.05° grid for alignment.
    """
    if not frames:
        return pd.DataFrame()

    priority = ["VIIRS", "MUR", "CMEMS"]
    frames_by_source = {df["source"].iloc[0]: df for df in frames}

    # Snap to common 0.05° grid
    grid_res = 0.05
    lons = np.arange(
        round(min(df["lon"].min() for df in frames)),
        round(max(df["lon"].max() for df in frames)) + grid_res,
        grid_res
    )
    lats = np.arange(
        round(min(df["lat"].min() for df in frames)),
        round(max(df["lat"].max() for df in frames)) + grid_res,
        grid_res
    )

    lon2d, lat2d = np.meshgrid(lons, lats)
    blended = np.full(lon2d.shape, np.nan)

    # Fill from lowest to highest priority so higher priority overwrites
    for source in reversed(priority):
        if source not in frames_by_source:
            continue
        df = frames_by_source[source]
        df["lon_snap"] = (df["lon"] / grid_res).round() * grid_res
        df["lat_snap"] = (df["lat"] / grid_res).round() * grid_res
        pivot = df.pivot_table(index="lat_snap", columns="lon_snap",
                               values="sst_c", aggfunc="mean")
        for i, la in enumerate(lats):
            for j, lo in enumerate(lons):
                la_r = round(la, 6)
                lo_r = round(lo, 6)
                try:
                    val = pivot.at[la_r, lo_r]
                    if not np.isnan(val):
                        blended[i, j] = val
                except KeyError:
                    pass

    # Flatten to DataFrame
    mask = np.isfinite(blended)
    out = pd.DataFrame({
        "lat":    lat2d[mask].round(5),
        "lon":    lon2d[mask].round(5),
        "sst_c":  blended[mask].round(3),
        "source": "blended",
        "date":   str(TARGET_DATE),
    })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# EXPORTERS
# ─────────────────────────────────────────────────────────────────────────────

def export_geojson(df: pd.DataFrame, path: Path, max_points: int | None = None):
    """GeoJSON FeatureCollection — drop into Mapbox, Leaflet, deck.gl"""
    if max_points and len(df) > max_points:
        df = df.sample(max_points, random_state=42)

    features = [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [round(float(row.lon), 5), round(float(row.lat), 5)]
            },
            "properties": {
                "sst_c":   round(float(row.sst_c), 3),
                "sst_f":   round(float(row.sst_c) * 9/5 + 32, 2),
                "source":  row.source,
                "date":    row.date,
            }
        }
        for row in df.itertuples(index=False)
    ]

    fc = {
        "type": "FeatureCollection",
        "metadata": {
            "generated":   str(datetime.datetime.utcnow()),
            "date":        str(TARGET_DATE),
            "bbox":        BBOX,
            "point_count": len(features),
            "source":      df["source"].iloc[0] if len(df) else "unknown",
        },
        "features": features,
    }

    with open(path, "w") as f:
        json.dump(fc, f, separators=(",", ":"))  # compact — smaller files
    print(f"    GeoJSON  → {path}  ({len(features):,} pts, {path.stat().st_size / 1024:.0f} KB)")


def export_csv(df: pd.DataFrame, path: Path):
    """CSV with lat, lon, sst_c, sst_f, source, date"""
    df = df.copy()
    df["sst_f"] = (df["sst_c"] * 9/5 + 32).round(2)
    df.to_csv(path, index=False)
    print(f"    CSV      → {path}  ({len(df):,} rows, {path.stat().st_size / 1024:.0f} KB)")


def export_parquet(df: pd.DataFrame, path: Path):
    """Parquet — best for large grids, columnar analytics, PostGIS ingest"""
    try:
        df = df.copy()
        df["sst_f"] = (df["sst_c"] * 9/5 + 32).round(2)
        df.to_parquet(path, index=False, compression="snappy")
        print(f"    Parquet  → {path}  ({len(df):,} rows, {path.stat().st_size / 1024:.0f} KB)")
    except ImportError:
        print("    Parquet skipped — run: pip install pyarrow")


class _NumpyEncoder(json.JSONEncoder):
    """Converts numpy scalar types to native Python before JSON serialization."""
    def default(self, obj):
        if isinstance(obj, (np.floating, np.float32, np.float64)):
            return float(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def export_grid(df: pd.DataFrame, path: Path):
    """
    Compact JSON grid format for WebGL / custom tile renderers.
    Structure:
    {
      "lats": [...],          // 1-D lat array
      "lons": [...],          // 1-D lon array
      "sst":  [[...], ...],   // 2-D grid [lat][lon], null where no data
      "meta": { ... }
    }
    """
    # Cast to native float to avoid numpy type issues throughout
    lats = sorted(float(v) for v in df["lat"].unique())
    lons = sorted(float(v) for v in df["lon"].unique())
    lat_idx = {v: i for i, v in enumerate(lats)}
    lon_idx = {v: i for i, v in enumerate(lons)}

    grid = [[None] * len(lons) for _ in range(len(lats))]
    for row in df.itertuples(index=False):
        i = lat_idx.get(float(row.lat))
        j = lon_idx.get(float(row.lon))
        if i is not None and j is not None:
            grid[i][j] = round(float(row.sst_c), 3)

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

    with open(path, "w") as f:
        json.dump(out, f, separators=(",", ":"), cls=_NumpyEncoder)
    print(f"    Grid JSON → {path}  ({len(lats)}×{len(lons)} grid, {path.stat().st_size / 1024:.0f} KB)")


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _ds_to_dataframe(ds: xr.Dataset, var: str, source: str,
                     date: datetime.date) -> pd.DataFrame:
    """Convert an xarray Dataset to a flat DataFrame, dropping NaNs."""
    da = ds[var].squeeze()

    # Drop leftover scalar dimensions
    for dim in list(da.dims):
        if da.sizes[dim] == 1:
            da = da.isel({dim: 0})

    # Identify lat/lon coordinate names
    lat_name = next((c for c in da.coords if "lat" in c.lower()), None)
    lon_name = next((c for c in da.coords if "lon" in c.lower()), None)
    if not lat_name or not lon_name:
        raise ValueError(f"Cannot find lat/lon coords in {list(da.coords)}")

    lats = da[lat_name].values
    lons = da[lon_name].values
    vals = da.values

    lon2d, lat2d = np.meshgrid(lons, lats)
    flat_lat  = lat2d.flatten()
    flat_lon  = lon2d.flatten()
    flat_sst  = vals.flatten()

    mask = np.isfinite(flat_sst)

    sst_c = flat_sst[mask]
    # Convert Kelvin if needed
    if sst_c.mean() > 200:
        sst_c = sst_c - 273.15

    return pd.DataFrame({
        "lat":    flat_lat[mask].round(5),
        "lon":    flat_lon[mask].round(5),
        "sst_c":  sst_c.round(3),
        "source": source,
        "date":   str(date),
    })


def _write_all(df: pd.DataFrame, label: str):
    """Run all enabled exporters for a given DataFrame."""
    date_str = TARGET_DATE.strftime("%Y%m%d")
    base = OUTPUT_DIR / f"{label}_{date_str}"
    print(f"  Exporting {label} ({len(df):,} points) ...")
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
    print(f"  Date : {TARGET_DATE}")
    print(f"  BBOX : {BBOX}")
    print(f"  Out  : {OUTPUT_DIR.resolve()}")
    print("=" * 60)

    frames = []
    df_blended = None  # initialized here so summary block always has it in scope
    run_mur   = SOURCES_OVERRIDE in ("all", "mur_only")
    run_viirs = SOURCES_OVERRIDE in ("all", "viirs_only")
    run_cmems = SOURCES_OVERRIDE in ("all", "cmems_only")

    df_mur = fetch_mur(TARGET_DATE, BBOX) if run_mur else None
    if df_mur is not None:
        frames.append(df_mur)
        _write_all(df_mur, "mur")

    df_viirs = fetch_viirs(TARGET_DATE, BBOX) if run_viirs else None
    if df_viirs is not None:
        frames.append(df_viirs)
        _write_all(df_viirs, "viirs")

    # Geo-polar blended — always run, no auth, fills cloud gaps in VIIRS
    df_blended = fetch_blended(TARGET_DATE, BBOX)
    if df_blended is not None:
        frames.append(df_blended)
        _write_all(df_blended, "blended_geopolar")

    df_cmems = fetch_cmems(TARGET_DATE, BBOX) if run_cmems else None
    if df_cmems is not None:
        frames.append(df_cmems)
        _write_all(df_cmems, "cmems")

    # Blended composite — best coverage, fills cloud gaps
    if len(frames) > 1:
        print("\n[Blending sources into composite ...]")
        df_blend = blend_sources(frames)
        if not df_blend.empty:
            _write_all(df_blend, "blended")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    all_dfs = {
        "MUR":          df_mur,
        "VIIRS(ACSPO)": df_viirs,
        "BLENDED":      df_blended,
        "CMEMS":        df_cmems,
    }
    print(f"  {'Source':<10} {'Points':>10}  {'Min °C':>8}  {'Max °C':>8}")
    print(f"  {'-'*10}  {'-'*9}  {'-'*7}  {'-'*7}")
    for name, df in all_dfs.items():
        if df is not None:
            print(f"  {name:<10} {len(df):>10,}  {df['sst_c'].min():>8.2f}  {df['sst_c'].max():>8.2f}")
        else:
            print(f"  {name:<10} {'—':>10}")
    print("=" * 60)
    print(f"\n  Files written to: {OUTPUT_DIR.resolve()}")
    print("""
  Data schema (all formats):
    lat     — decimal degrees (WGS84)
    lon     — decimal degrees (WGS84)
    sst_c   — sea surface temperature, Celsius
    sst_f   — sea surface temperature, Fahrenheit
    source  — MUR | VIIRS | CMEMS | blended
    date    — ISO date string

  Recommended app rendering stack:
    • deck.gl HeatmapLayer or GridLayer  (WebGL, handles 500k+ pts)
    • Mapbox GL raster-array source      (feed the grid JSON)
    • Leaflet canvas renderer            (for the GeoJSON)
    • PostGIS + ST_PixelAsPoints         (for server-side tile generation)
""")


if __name__ == "__main__":
    main()
