"""
High-Fidelity SST Data Fetcher — App-Ready Output
===================================================
Fetches SST data from the top highest-resolution free sources and exports
structured data (GeoJSON, CSV, Parquet, Grid JSON) for rendering in a web/mobile app.

Sources:
  1. NASA MUR SST       — 0.01° (~1 km), L4 blended, gap-filled, daily
     Primary:  NASA PO.DAAC OPeNDAP
     Fallback: ERDDAP mirrors (polarwatch, USF, upwell, ifremer, pfeg)
               + csvp format with mask-based land/lake/ice/tidal filtering
  2. NOAA VIIRS NRT     — ~1–4 km, near-real-time, daily
     Primary:  CoastWatch THREDDS catalog
     Fallback: ERDDAP blended products
  3. Copernicus CMEMS   — 0.05° (~5 km), L4 blended, fully gap-filled
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
# Values: all | mur_only | viirs_only | cmems_only
SOURCES_OVERRIDE = os.environ.get("SOURCES_OVERRIDE", "all").strip()

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


def _latest_available_dates(base_date: datetime.date, max_lookback: int = 3) -> list:
    return [base_date - datetime.timedelta(days=i) for i in range(max_lookback + 1)]


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


def fetch_mur(date: datetime.date, bbox: dict) -> "pd.DataFrame | None":
    """
    Returns a DataFrame with columns: lat, lon, sst_c, source, date.

    Attempt order per date:
      1. ERDDAP csvp — upwell + ifremer (confirmed CI-friendly per DailySSTRetrieval)
         Uses mask field to precisely remove land/lake/ice/tidal cells.
      2. NASA PO.DAAC OPeNDAP (requires Earthdata login from CI — often 401)
      3. ERDDAP mirrors — .nc format (polarwatch, USF, upwell, ifremer, pfeg)
    """
    print(f"\n[1/3] NASA MUR SST  (target: {date})  ~1 km resolution")

    for try_date in _latest_available_dates(date):

        # ── 1a. ERDDAP csvp — upwell / ifremer (most reliable from CI) ───────
        for base_url in MUR_ERDDAP_HOSTS_CSVP:
            url = _build_mur_csvp_url(base_url, try_date, bbox)
            try:
                host_label = base_url.split("/")[2]
                print(f"  Trying {host_label} csvp for {try_date} ...")
                r = _SESSION.get(url, timeout=TIMEOUT_SECONDS)
                r.raise_for_status()
                df = _parse_mur_csvp(r.text, bbox)
                if df.empty:
                    print(f"  ✗ {host_label} csvp: no rows parsed")
                    continue
                print(f"  ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  ({host_label} csvp, {try_date})")
                return df
            except Exception as e:
                print(f"  ✗ {base_url.split('/')[2]} csvp / {try_date}: {e}")
                continue

        # ── 1b. NASA PO.DAAC OPeNDAP ─────────────────────────────────────────
        date_str    = try_date.strftime("%Y%m%d")
        opendap_url = (
            f"https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections"
            f"/MUR-JPL-L4-GLOB-v4.1/granules"
            f"/{date_str}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"
        )
        try:
            print(f"  Trying NASA OPeNDAP for {try_date} ...")
            ce = (
                f"?analysed_sst"
                f"[0:1:0]"
                f"[{_lat_idx(bbox['lat_min'])}:{_lat_idx(bbox['lat_max'])}]"
                f"[{_lon_idx(bbox['lon_min'])}:{_lon_idx(bbox['lon_max'])}]"
            )
            ds = xr.open_dataset(opendap_url + ".nc4" + ce, engine="netcdf4")
            df = _ds_to_dataframe(ds, "analysed_sst", "MUR", try_date)
            print(f"  ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  (OPeNDAP, {try_date})")
            return df
        except Exception as e:
            print(f"  ✗ NASA OPeNDAP {try_date}: {e}")

        # ── 1c. ERDDAP mirrors — NetCDF (.nc) ────────────────────────────────
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
                print(f"  Trying {host_label} .nc for {try_date} ...")
                r = _SESSION.get(url, timeout=TIMEOUT_SECONDS, stream=True)
                r.raise_for_status()
                with open(nc_path, "wb") as fh:
                    for chunk in r.iter_content(1 << 20):
                        fh.write(chunk)
                ds = xr.open_dataset(nc_path)
                df = _ds_to_dataframe(ds, "analysed_sst", "MUR", try_date)
                print(f"  ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  ({host_label}, {try_date})")
                return df
            except Exception as e:
                print(f"  ✗ {host.split('/')[2]} .nc / {try_date}: {e}")
                continue

        # ── 1c. ERDDAP mirrors — csvp with mask filtering ─────────────────────
        # Lighter than NC; mask field precisely removes land/lake/ice/tidal cells
        for base_url in MUR_ERDDAP_HOSTS_CSVP:
            url = _build_mur_csvp_url(base_url, try_date, bbox)
            try:
                host_label = base_url.split("/")[2]
                print(f"  Trying {host_label} csvp for {try_date} ...")
                r = _SESSION.get(url, timeout=TIMEOUT_SECONDS)
                r.raise_for_status()
                df = _parse_mur_csvp(r.text, bbox)
                if df.empty:
                    print(f"  ✗ {host_label} csvp: no rows parsed")
                    continue
                print(f"  ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  ({host_label} csvp, {try_date})")
                return df
            except Exception as e:
                print(f"  ✗ {base_url.split('/')[2]} csvp / {try_date}: {e}")
                continue

    print("  ✗ All MUR sources failed across all fallback dates")
    return None


def _lat_idx(lat: float) -> int:
    return int(round((lat + 89.99) / 0.01))


def _lon_idx(lon: float) -> int:
    return int(round((lon + 179.99) / 0.01))


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2: NOAA CoastWatch VIIRS SST (NOAA-20, daily composite)
#   Resolution : ~1–4 km
#   Latency    : ~6–12 hours NRT
# ─────────────────────────────────────────────────────────────────────────────

def fetch_viirs(date: datetime.date, bbox: dict) -> "pd.DataFrame | None":
    import re
    print(f"\n[2/3] NOAA VIIRS SST  (target: {date})  ~1–4 km resolution")

    # ── THREDDS catalog discovery ─────────────────────────────────────────────
    for try_date in _latest_available_dates(date):
        doy  = try_date.timetuple().tm_yday
        year = try_date.year
        catalog_url = (
            f"https://coastwatch.noaa.gov/thredds/catalog/gridN20VIIRSSCIENCEL3UWW00"
            f"/{year}/{doy:03d}/catalog.xml"
        )
        try:
            print(f"  THREDDS catalog DOY {doy:03d} ({try_date}) ...")
            resp = _SESSION.get(catalog_url, timeout=30)
            resp.raise_for_status()
            matches = re.findall(r'gridN20VIIRSSCIENCEL3UWW00/[^"]+\.nc', resp.text)
            if not matches:
                raise ValueError("No .nc files in catalog")
            nc_name     = sorted(matches)[-1].split("/")[-1]
            opendap_url = (
                f"https://coastwatch.noaa.gov/thredds/dodsC/gridN20VIIRSSCIENCEL3UWW00"
                f"/{year}/{doy:03d}/{nc_name}"
            )
            print(f"  Opening: {nc_name}")
            ds      = xr.open_dataset(opendap_url, engine="netcdf4")
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
            print(f"  ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  (THREDDS, {try_date})")
            return df
        except Exception as e:
            print(f"  ✗ THREDDS {try_date}: {e}")
            continue

    # ── ERDDAP blended fallbacks ──────────────────────────────────────────────
    erddap_fallbacks = [
        ("https://coastwatch.noaa.gov/erddap/griddap", "noaacrwsstDaily",         "analysed_sst"),
        ("https://coastwatch.noaa.gov/erddap/griddap", "noaacwBLENDEDsstDLDaily", "analysed_sst"),
    ]
    for try_date in _latest_available_dates(date):
        for base, dataset_id, var in erddap_fallbacks:
            url = (
                f"{base}/{dataset_id}.nc?{var}"
                f"[({try_date}T12:00:00Z)]"
                f"[({bbox['lat_min']}):1:({bbox['lat_max']})]"
                f"[({bbox['lon_min']}):1:({bbox['lon_max']})]"
            )
            nc_path = OUTPUT_DIR / f"_viirs_{try_date}.nc"
            try:
                print(f"  Trying {dataset_id} ({try_date}) ...")
                r = _SESSION.get(url, timeout=TIMEOUT_SECONDS, stream=True)
                r.raise_for_status()
                with open(nc_path, "wb") as fh:
                    for chunk in r.iter_content(1 << 20):
                        fh.write(chunk)
                ds = xr.open_dataset(nc_path)
                df = _ds_to_dataframe(ds, var, "VIIRS", try_date)
                print(f"  ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C  ({dataset_id}, {try_date})")
                return df
            except Exception as e:
                print(f"  ✗ {dataset_id} / {try_date}: {e}")
                continue

    print("  ✗ All VIIRS sources failed")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3: Copernicus CMEMS L4 Global SST
#   Resolution : 0.05° (~5 km) — fully gap-filled
#   Latency    : ~1 day
#   Auth       : CMEMS_USER + CMEMS_PASSWORD env vars
# ─────────────────────────────────────────────────────────────────────────────

def fetch_cmems(date: datetime.date, bbox: dict) -> "pd.DataFrame | None":
    print(f"\n[3/3] Copernicus CMEMS L4 SST  ({date})  ~5 km, fully gap-filled")
    user = os.environ.get("CMEMS_USER")
    pw   = os.environ.get("CMEMS_PASSWORD")
    if not user or not pw:
        print("  ✗ Skipped — set CMEMS_USER and CMEMS_PASSWORD env vars")
        return None
    try:
        import copernicusmarine as cm
    except ImportError:
        print("  ✗ Run: pip install copernicusmarine")
        return None

    nc_path = OUTPUT_DIR / f"_cmems_{date}.nc"
    try:
        print("  Downloading via copernicusmarine client ...")
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
        print(f"  ✓ {len(df):,} pts  {df['sst_c'].min():.2f}–{df['sst_c'].max():.2f} °C")
        return df
    except Exception as e:
        err = str(e).lower()
        if "credential" in err or "password" in err or "username" in err:
            print(f"  ✗ CMEMS auth failed — check CMEMS_USER/CMEMS_PASSWORD: {e}")
        else:
            print(f"  ✗ CMEMS fetch failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# BLENDING
# ─────────────────────────────────────────────────────────────────────────────

def blend_sources(frames: list) -> pd.DataFrame:
    """
    Merges multiple source DataFrames into a single best-available grid.
    Priority: VIIRS > MUR > CMEMS. All sources snapped to 0.05° grid.
    """
    if not frames:
        return pd.DataFrame()
    priority          = ["VIIRS", "MUR", "CMEMS"]
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


def _write_all(df: pd.DataFrame, label: str):
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
    print(f"  Date      : {TARGET_DATE}")
    print(f"  BBOX      : {BBOX}")
    print(f"  Output    : {OUTPUT_DIR.resolve()}")
    print(f"  Sources   : {SOURCES_OVERRIDE}")
    print("=" * 60)

    run_mur   = SOURCES_OVERRIDE in ("all", "mur_only")
    run_viirs = SOURCES_OVERRIDE in ("all", "viirs_only")
    run_cmems = SOURCES_OVERRIDE in ("all", "cmems_only")

    frames = []

    df_mur = fetch_mur(TARGET_DATE, BBOX) if run_mur else None
    if df_mur is not None:
        frames.append(df_mur)
        _write_all(df_mur, "mur")

    df_viirs = fetch_viirs(TARGET_DATE, BBOX) if run_viirs else None
    if df_viirs is not None:
        frames.append(df_viirs)
        _write_all(df_viirs, "viirs")

    df_cmems = fetch_cmems(TARGET_DATE, BBOX) if run_cmems else None
    if df_cmems is not None:
        frames.append(df_cmems)
        _write_all(df_cmems, "cmems")

    if len(frames) > 1:
        print("\n[Blending sources into composite ...]")
        df_blend = blend_sources(frames)
        if not df_blend.empty:
            _write_all(df_blend, "blended")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    all_dfs = {"MUR": df_mur, "VIIRS": df_viirs, "CMEMS": df_cmems}
    print(f"  {'Source':<8} {'Points':>10}  {'Min °C':>8}  {'Max °C':>8}")
    print(f"  {'-'*8}  {'-'*9}  {'-'*7}  {'-'*7}")
    for name, df in all_dfs.items():
        if df is not None:
            print(f"  {name:<8} {len(df):>10,}  {df['sst_c'].min():>8.2f}  {df['sst_c'].max():>8.2f}")
        else:
            print(f"  {name:<8} {'—':>10}")
    print("=" * 60)
    print(f"\n  Files written to: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
