#!/usr/bin/env python3
"""
Ocean Dynamics Fetcher — Currents + Altimetry
==============================================
Fetches sea current (u/v) and altimetry (SSH/SLA) data for the Mid-Atlantic
offshore region and exports in formats compatible with the SST app.

Output files (in OUTPUT_DIR):
  Currents/
    currents_{YYYYMMDD}.json   — leaflet-velocity animation format (u/v grid)
    currents_{YYYYMMDD}.csv    — tabular (lat, lon, u, v, speed_ms, dir_deg)
  Altimetry/
    altimetry_{YYYYMMDD}.csv   — tabular (lat, lon, ssh_m, sla_m, adt_m)
    altimetry_{YYYYMMDD}_grid.json — grid JSON for color overlay (like SST)

Sources (in priority order):
  1. HYCOM NCSS   — 1/12 deg (~9 km), daily, no auth
  2. OSCAR ERDDAP — 1/3 deg (~37 km), 5-day composite, no auth (fallback)
  3. CMEMS        — 0.083 deg currents + 0.125 deg altimetry (auth required)
     Set CMEMS_USER and CMEMS_PASSWORD env vars.
     CMEMS altimetry also provides geostrophic currents (ugos/vgos) which
     are merged with the model currents for a more complete picture.

Animation format (currents_{date}.json) is compatible with leaflet-velocity
and the existing wind particle layer. Load it the same way as wind data:
  { source, date, maxSpeed, hours: [{ time, velocityJSON, grid }] }

Dependencies:
    pip install requests xarray netCDF4 numpy pandas pyarrow copernicusmarine
"""
import os
import re
import csv
import io
import json
import datetime
import warnings
import math
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
    "lat_min": 33.70,
    "lat_max": 39.00,
    "lon_min": -78.89,
    "lon_max": -72.21,
}

_date_override = os.environ.get("TARGET_DATE_OVERRIDE", "").strip()
TARGET_DATE = (
    datetime.date.fromisoformat(_date_override)
    if _date_override
    else datetime.date.today() - datetime.timedelta(days=1)
)

OUTPUT_DIR  = Path("DailySST")
CURR_DIR    = OUTPUT_DIR / "Currents"
ALT_DIR     = OUTPUT_DIR / "Altimetry"
CURR_DIR.mkdir(parents=True, exist_ok=True)
ALT_DIR.mkdir(parents=True, exist_ok=True)

TIMEOUT    = 180
MAX_RETRY  = 2

# ─────────────────────────────────────────────────────────────────────────────
# HTTP SESSION
# ─────────────────────────────────────────────────────────────────────────────
def _make_session():
    s = requests.Session()
    retry = Retry(total=MAX_RETRY, backoff_factor=1,
                  status_forcelist=[429, 502, 503, 504],
                  allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s

SESSION = _make_session()

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _recent_dates(base, n=3):
    return [base - datetime.timedelta(days=i) for i in range(n + 1)]

def _speed(u, v):
    return round(math.sqrt(float(u)**2 + float(v)**2), 4)

def _direction(u, v):
    """Oceanographic convention: direction current is flowing TOWARD (degrees)."""
    return round((math.degrees(math.atan2(float(u), float(v))) + 360) % 360, 1)

def _ds_extract_uv(ds, u_var, v_var, depth_idx=0):
    """Extract surface u/v from an xarray Dataset, return DataFrame."""
    def _squeeze(da):
        for dim in list(da.dims):
            if dim.lower() in ("time", "depth", "lev", "level", "altitude") or da.sizes[dim] == 1:
                if da.sizes[dim] > 1:
                    da = da.isel({dim: depth_idx})
                else:
                    da = da.squeeze(dim)
        return da

    u_da = _squeeze(ds[u_var])
    v_da = _squeeze(ds[v_var])

    lat_name = next((c for c in u_da.coords if "lat" in c.lower()), None)
    lon_name = next((c for c in u_da.coords if "lon" in c.lower()), None)
    if not lat_name or not lon_name:
        raise ValueError(f"No lat/lon coords in {list(u_da.coords)}")

    lats = u_da[lat_name].values
    lons = u_da[lon_name].values
    u_vals = u_da.values
    v_vals = v_da.values

    lon2d, lat2d = np.meshgrid(lons, lats)
    u_flat = u_vals.flatten()
    v_flat = v_vals.flatten()
    mask   = np.isfinite(u_flat) & np.isfinite(v_flat)

    records = []
    for la, lo, u, v in zip(lat2d.flatten()[mask],
                              lon2d.flatten()[mask],
                              u_flat[mask], v_flat[mask]):
        records.append({
            "lat":      round(float(la), 5),
            "lon":      round(float(lo), 5),
            "u":        round(float(u), 5),
            "v":        round(float(v), 5),
            "speed_ms": _speed(u, v),
            "dir_deg":  _direction(u, v),
        })
    return pd.DataFrame(records)

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1: HYCOM (no auth, ~1/12 deg, daily)
#   Variables: water_u, water_v (m/s), surf_el (SSH, m)
#   Access via THREDDS NCSS (returns NetCDF subset on a regular lat/lon grid)
# ─────────────────────────────────────────────────────────────────────────────
HYCOM_CATALOG = "https://ncss.hycom.org/thredds/catalog/GLBy0.08/catalog.xml"
HYCOM_NCSS    = "https://ncss.hycom.org/thredds/ncss/GLBy0.08"

def _discover_hycom_experiment():
    """Scrape the HYCOM catalog to find the latest available experiment path."""
    try:
        r = SESSION.get(HYCOM_CATALOG, timeout=20)
        r.raise_for_status()
        # catalog references look like: GLBy0.08/expt_93.0/...
        expts = re.findall(r'name="(expt_[\d.]+)"', r.text)
        if not expts:
            expts = re.findall(r'expt_([\d.]+)', r.text)
            expts = [f"expt_{e}" for e in expts]
        if expts:
            # Sort by numeric value and take the highest
            expts_sorted = sorted(set(expts),
                                  key=lambda x: float(x.replace("expt_","")),
                                  reverse=True)
            print(f"  HYCOM experiments found: {expts_sorted[:3]}")
            return expts_sorted[0]
    except Exception as e:
        print(f"  HYCOM catalog lookup failed: {e}")
    return "expt_93.0"  # best-guess default

def fetch_hycom(date):
    """Fetch HYCOM surface currents + SSH. Returns (df_currents, df_ssh) or (None, None)."""
    print(f"\n[1/3] HYCOM currents + SSH  (target: {date})  ~1/12 deg")
    expt = _discover_hycom_experiment()

    b = BBOX
    curr_vars = "water_u,water_v,surf_el"

    for try_date in _recent_dates(date):
        # HYCOM NCSS uses 12Z files for daily
        date_str = try_date.strftime("%Y%m%d")
        for hour in ("12", "00"):
            url = (
                f"{HYCOM_NCSS}/{expt}/data/daily/{date_str}{hour}.nc"
                f"?var={curr_vars}"
                f"&north={b['lat_max']}&south={b['lat_min']}"
                f"&west={b['lon_min']}&east={b['lon_max']}"
                f"&horizStride=1&vertCoord=0"
                f"&disableLLSubset=on&disableProjSubset=on"
                f"&time_start={try_date}T{hour}:00:00Z"
                f"&time_end={try_date}T{hour}:00:00Z"
            )
            nc_path = OUTPUT_DIR / f"_hycom_{try_date}_{hour}.nc"
            try:
                print(f"  Trying HYCOM NCSS {try_date} {hour}Z ...")
                r = SESSION.get(url, timeout=TIMEOUT, stream=True)
                r.raise_for_status()
                with open(nc_path, "wb") as fh:
                    for chunk in r.iter_content(1 << 20):
                        fh.write(chunk)
                ds = xr.open_dataset(nc_path)
                print(f"  Dataset vars: {list(ds.data_vars)}")

                # Currents
                u_var = next((v for v in ds.data_vars if "water_u" in v or v == "u"), None)
                v_var = next((v for v in ds.data_vars if "water_v" in v or v == "v"), None)
                df_curr = None
                if u_var and v_var:
                    df_curr = _ds_extract_uv(ds, u_var, v_var)
                    df_curr["source"] = "HYCOM"
                    df_curr["date"]   = str(try_date)
                    spd = df_curr["speed_ms"]
                    print(f"  Currents: {len(df_curr):,} pts  speed {spd.min():.3f}–{spd.max():.3f} m/s")

                # SSH
                ssh_var = next((v for v in ds.data_vars
                                if "surf_el" in v or "ssh" in v.lower() or "zos" in v.lower()), None)
                df_ssh = None
                if ssh_var:
                    da = ds[ssh_var].squeeze()
                    for dim in list(da.dims):
                        if da.sizes[dim] == 1:
                            da = da.squeeze(dim)
                    lat_n = next((c for c in da.coords if "lat" in c.lower()), None)
                    lon_n = next((c for c in da.coords if "lon" in c.lower()), None)
                    if lat_n and lon_n:
                        lats = da[lat_n].values
                        lons = da[lon_n].values
                        vals = da.values
                        lo2d, la2d = np.meshgrid(lons, lats)
                        flat = vals.flatten()
                        mask = np.isfinite(flat)
                        df_ssh = pd.DataFrame({
                            "lat":    la2d.flatten()[mask].round(5),
                            "lon":    lo2d.flatten()[mask].round(5),
                            "ssh_m":  flat[mask].round(4),
                            "sla_m":  np.nan,
                            "adt_m":  np.nan,
                            "source": "HYCOM",
                            "date":   str(try_date),
                        })
                        print(f"  SSH: {len(df_ssh):,} pts  {df_ssh['ssh_m'].min():.3f}–{df_ssh['ssh_m'].max():.3f} m")

                if df_curr is not None or df_ssh is not None:
                    return df_curr, df_ssh

            except Exception as e:
                print(f"  HYCOM NCSS {try_date} {hour}Z failed: {e}")
                continue

    print("  HYCOM: all attempts failed")
    return None, None

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2: OSCAR via ERDDAP (no auth, 1/3 deg, 5-day composite)
#   OSCAR = Ocean Surface Current Analysis Real-time (NASA/JPL)
#   Variables: u, v (m/s at surface)
# ─────────────────────────────────────────────────────────────────────────────
OSCAR_ERDDAP_HOSTS = [
    "https://upwell.pfeg.noaa.gov/erddap/griddap/OSCAR_L4_OC_NRT_V2.0",
    "https://coastwatch.pfeg.noaa.gov/erddap/griddap/OSCAR_L4_OC_NRT_V2.0",
]

def fetch_oscar(date):
    """Fetch OSCAR surface currents. Returns df_currents or None."""
    print(f"\n[2/3] OSCAR currents  (target: {date})  1/3 deg, 5-day composite")
    b = BBOX
    for try_date in _recent_dates(date, n=5):
        ts = f"({try_date}T00:00:00Z)"
        for host in OSCAR_ERDDAP_HOSTS:
            # OSCAR depth dimension = 0.0 (surface)
            url = (
                f"{host}.nc"
                f"?u[{ts}][(0.0)]"
                f"[({b['lat_min']}):1:({b['lat_max']})]"
                f"[({b['lon_min']}):1:({b['lon_max']})]"
                f",v[{ts}][(0.0)]"
                f"[({b['lat_min']}):1:({b['lat_max']})]"
                f"[({b['lon_min']}):1:({b['lon_max']})]"
            )
            nc_path = OUTPUT_DIR / f"_oscar_{try_date}.nc"
            try:
                host_label = host.split("/")[2]
                print(f"  Trying {host_label} {try_date} ...")
                r = SESSION.get(url, timeout=TIMEOUT, stream=True)
                r.raise_for_status()
                with open(nc_path, "wb") as fh:
                    for chunk in r.iter_content(1 << 20):
                        fh.write(chunk)
                ds = xr.open_dataset(nc_path)
                u_var = next((v for v in ds.data_vars if v == "u" or "eastward" in v.lower()), None)
                v_var = next((v for v in ds.data_vars if v == "v" or "northward" in v.lower()), None)
                if not u_var or not v_var:
                    raise ValueError(f"No u/v in {list(ds.data_vars)}")
                df = _ds_extract_uv(ds, u_var, v_var)
                df["source"] = "OSCAR"
                df["date"]   = str(try_date)
                spd = df["speed_ms"]
                print(f"  OSCAR {try_date}: {len(df):,} pts  speed {spd.min():.3f}–{spd.max():.3f} m/s")
                return df
            except Exception as e:
                print(f"  OSCAR {host_label} {try_date}: {e}")
                continue

    print("  OSCAR: all attempts failed")
    return None

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3: CMEMS (auth required)
#   Currents:   cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m  (0.083 deg, daily)
#   Altimetry:  cmems_obs-sl_glo_phy-ssh_nrt_allsat-l4-duacs-0.125deg_P1D
#               → includes sla, adt, ugos (geostrophic u), vgos (geostrophic v)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_cmems_currents(date):
    print(f"\n[CMEMS] Currents  ({date})  0.083 deg, daily")
    user = os.environ.get("CMEMS_USER")
    pw   = os.environ.get("CMEMS_PASSWORD")
    if not user or not pw:
        print("  Skipped — set CMEMS_USER and CMEMS_PASSWORD")
        return None
    try:
        import copernicusmarine as cm
    except ImportError:
        print("  Run: pip install copernicusmarine")
        return None
    b = BBOX
    nc_path = OUTPUT_DIR / f"_cmems_curr_{date}.nc"
    try:
        cm.subset(
            dataset_id="cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
            variables=["uo", "vo"],
            minimum_longitude=b["lon_min"],
            maximum_longitude=b["lon_max"],
            minimum_latitude=b["lat_min"],
            maximum_latitude=b["lat_max"],
            minimum_depth=0.0,
            maximum_depth=1.0,
            start_datetime=f"{date}T00:00:00",
            end_datetime=f"{date}T23:59:59",
            output_filename=str(nc_path),
            username=user,
            password=pw,
            overwrite=True,
        )
        ds = xr.open_dataset(nc_path)
        df = _ds_extract_uv(ds, "uo", "vo")
        df["source"] = "CMEMS"
        df["date"]   = str(date)
        spd = df["speed_ms"]
        print(f"  CMEMS currents: {len(df):,} pts  {spd.min():.3f}–{spd.max():.3f} m/s")
        return df
    except Exception as e:
        print(f"  CMEMS currents failed: {e}")
        return None

def fetch_cmems_altimetry(date):
    """
    Fetch CMEMS L4 altimetry: SSH, SLA, ADT, plus geostrophic currents ugos/vgos.
    ugos/vgos represent the geostrophic component of surface currents derived
    from altimetry — useful as a complementary current source.
    """
    print(f"\n[CMEMS] Altimetry  ({date})  0.125 deg, daily")
    user = os.environ.get("CMEMS_USER")
    pw   = os.environ.get("CMEMS_PASSWORD")
    if not user or not pw:
        print("  Skipped — set CMEMS_USER and CMEMS_PASSWORD")
        return None, None
    try:
        import copernicusmarine as cm
    except ImportError:
        print("  Run: pip install copernicusmarine")
        return None, None
    b = BBOX
    nc_path = OUTPUT_DIR / f"_cmems_alt_{date}.nc"
    try:
        cm.subset(
            dataset_id="cmems_obs-sl_glo_phy-ssh_nrt_allsat-l4-duacs-0.125deg_P1D",
            variables=["sla", "adt", "ugos", "vgos"],
            minimum_longitude=b["lon_min"],
            maximum_longitude=b["lon_max"],
            minimum_latitude=b["lat_min"],
            maximum_latitude=b["lat_max"],
            start_datetime=f"{date}T00:00:00",
            end_datetime=f"{date}T23:59:59",
            output_filename=str(nc_path),
            username=user,
            password=pw,
            overwrite=True,
        )
        ds = xr.open_dataset(nc_path)
        lat_n = next((c for c in ds.coords if "lat" in c.lower()), None)
        lon_n = next((c for c in ds.coords if "lon" in c.lower()), None)
        lats = ds[lat_n].values
        lons = ds[lon_n].values
        lo2d, la2d = np.meshgrid(lons, lats)

        def _flat(var):
            da = ds[var].squeeze()
            for dim in list(da.dims):
                if da.sizes[dim] == 1:
                    da = da.squeeze(dim)
            return da.values.flatten()

        sla  = _flat("sla")
        adt  = _flat("adt")
        ugos = _flat("ugos")
        vgos = _flat("vgos")
        la_f = la2d.flatten()
        lo_f = lo2d.flatten()
        mask = np.isfinite(sla) & np.isfinite(adt)

        df_alt = pd.DataFrame({
            "lat":    la_f[mask].round(5),
            "lon":    lo_f[mask].round(5),
            "ssh_m":  adt[mask].round(4),
            "sla_m":  sla[mask].round(4),
            "adt_m":  adt[mask].round(4),
            "source": "CMEMS_ALT",
            "date":   str(date),
        })
        print(f"  Altimetry: {len(df_alt):,} pts  SLA {sla[mask].min():.3f}–{sla[mask].max():.3f} m")

        # Geostrophic currents from altimetry
        mask_geo = np.isfinite(ugos) & np.isfinite(vgos)
        df_geo = None
        if mask_geo.any():
            df_geo = pd.DataFrame({
                "lat":      la_f[mask_geo].round(5),
                "lon":      lo_f[mask_geo].round(5),
                "u":        ugos[mask_geo].round(5),
                "v":        vgos[mask_geo].round(5),
                "speed_ms": [_speed(u, v) for u, v in zip(ugos[mask_geo], vgos[mask_geo])],
                "dir_deg":  [_direction(u, v) for u, v in zip(ugos[mask_geo], vgos[mask_geo])],
                "source":   "CMEMS_GEO",
                "date":     str(date),
            })
            spd = df_geo["speed_ms"]
            print(f"  Geostrophic currents: {len(df_geo):,} pts  {spd.min():.3f}–{spd.max():.3f} m/s")

        return df_alt, df_geo

    except Exception as e:
        print(f"  CMEMS altimetry failed: {e}")
        return None, None

# ─────────────────────────────────────────────────────────────────────────────
# ANIMATION FORMAT (leaflet-velocity / wind-particle compatible)
# ─────────────────────────────────────────────────────────────────────────────
def _build_velocity_json(df, grid_res=0.083):
    """
    Convert a currents DataFrame to leaflet-velocity format.

    The format is a 2-element array:
      [ u_component_object, v_component_object ]
    where each object has:
      header: { la1 (top-left lat), lo1 (left lon), nx, ny, dx, dy }
      data:   flat array ordered north-to-south, west-to-east

    This matches the format consumed by velocityLayerRef.setData() in the app.
    grid_res: output grid resolution in degrees (snap input points to this grid)
    """
    # Snap to regular grid
    df = df.copy()
    df["lat_g"] = (df["lat"] / grid_res).round() * grid_res
    df["lon_g"] = (df["lon"] / grid_res).round() * grid_res

    # Average any duplicates on same cell
    df_u = df.pivot_table(index="lat_g", columns="lon_g", values="u", aggfunc="mean")
    df_v = df.pivot_table(index="lat_g", columns="lon_g", values="v", aggfunc="mean")

    lats = sorted(df_u.index.tolist(), reverse=True)  # north to south
    lons = sorted(df_u.columns.tolist())               # west to east

    u_flat = []
    v_flat = []
    for la in lats:
        for lo in lons:
            try:
                uv = df_u.at[la, lo]
                vv = df_v.at[la, lo]
                u_flat.append(None if (uv != uv) else round(float(uv), 5))
                v_flat.append(None if (vv != vv) else round(float(vv), 5))
            except KeyError:
                u_flat.append(None)
                v_flat.append(None)

    header = {
        "la1": round(lats[0],  5),   # top-left latitude (northernmost)
        "lo1": round(lons[0],  5),   # top-left longitude (westernmost)
        "la2": round(lats[-1], 5),
        "lo2": round(lons[-1], 5),
        "nx":  len(lons),
        "ny":  len(lats),
        "dx":  round(grid_res, 5),
        "dy":  round(grid_res, 5),
    }
    return [
        {"header": {**header, "parameterCategory": 2, "parameterNumber": 2,
                    "name": "U-component_of_current", "parameterUnit": "m.s-1"}, "data": u_flat},
        {"header": {**header, "parameterCategory": 2, "parameterNumber": 3,
                    "name": "V-component_of_current", "parameterUnit": "m.s-1"}, "data": v_flat},
    ]

def _build_grid_points(df):
    """Return a list of {lat, lon, u, v, speed_ms, dir_deg} for click-to-inspect."""
    out = []
    for r in df.itertuples(index=False):
        out.append({
            "lat":      round(float(r.lat), 4),
            "lon":      round(float(r.lon), 4),
            "u":        round(float(r.u), 4),
            "v":        round(float(r.v), 4),
            "speed_ms": round(float(r.speed_ms), 4),
            "dir_deg":  round(float(r.dir_deg), 1),
        })
    return out

# ─────────────────────────────────────────────────────────────────────────────
# EXPORTERS
# ─────────────────────────────────────────────────────────────────────────────
class _NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.floating, np.float32, np.float64)): return float(obj)
        if isinstance(obj, np.integer):  return int(obj)
        if isinstance(obj, np.ndarray):  return obj.tolist()
        return super().default(obj)

def export_currents(df, date):
    """Export currents in animation JSON + CSV formats."""
    date_str = date.strftime("%Y%m%d")
    source   = df["source"].iloc[0]
    max_spd  = float(df["speed_ms"].quantile(0.98).round(2))

    # Determine grid resolution based on source
    grid_res = 0.083 if source in ("CMEMS", "HYCOM", "CMEMS_GEO") else 0.25

    velocity_json = _build_velocity_json(df, grid_res=grid_res)
    grid_pts      = _build_grid_points(df)

    animation_data = {
        "source":        source,
        "date":          str(date),
        "generated_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "maxSpeed":      max_spd,
        "bbox":          BBOX,
        "hours": [
            {
                # Daily currents: represent as a single 12Z entry
                "time":        f"{date}T12:00:00Z",
                "velocityJSON": velocity_json,
                "grid":         grid_pts,
            }
        ],
    }

    # Animation JSON
    json_path = CURR_DIR / f"currents_{date_str}.json"
    with open(json_path, "w") as fh:
        json.dump(animation_data, fh, separators=(",", ":"), cls=_NpEncoder)
    print(f"    Currents JSON → {json_path}  ({json_path.stat().st_size/1024:.0f} KB)")

    # Also write a "latest" symlink/copy
    latest_path = CURR_DIR / "currents_latest.json"
    import shutil
    shutil.copy(json_path, latest_path)

    # CSV
    csv_path = CURR_DIR / f"currents_{date_str}.csv"
    df.to_csv(csv_path, index=False)
    print(f"    Currents CSV  → {csv_path}  ({len(df):,} rows, {csv_path.stat().st_size/1024:.0f} KB)")

def export_altimetry(df, date):
    """Export altimetry in CSV + grid JSON formats."""
    date_str = date.strftime("%Y%m%d")

    # CSV
    csv_path = ALT_DIR / f"altimetry_{date_str}.csv"
    df.to_csv(csv_path, index=False)
    print(f"    Altimetry CSV  → {csv_path}  ({len(df):,} rows, {csv_path.stat().st_size/1024:.0f} KB)")

    # Grid JSON for SSH (color overlay, same format as SST grid)
    lats   = sorted(df["lat"].unique().tolist())
    lons   = sorted(df["lon"].unique().tolist())
    lat_i  = {v: i for i, v in enumerate(lats)}
    lon_i  = {v: i for i, v in enumerate(lons)}
    ssh_g  = [[None] * len(lons) for _ in range(len(lats))]
    sla_g  = [[None] * len(lons) for _ in range(len(lats))]

    for r in df.itertuples(index=False):
        i = lat_i.get(float(r.lat))
        j = lon_i.get(float(r.lon))
        if i is not None and j is not None:
            ssh_g[i][j] = round(float(r.ssh_m), 4) if not math.isnan(float(r.ssh_m)) else None
            if hasattr(r, "sla_m") and not math.isnan(float(r.sla_m)):
                sla_g[i][j] = round(float(r.sla_m), 4)

    grid_data = {
        "meta": {
            "date":    str(date),
            "bbox":    BBOX,
            "source":  df["source"].iloc[0],
            "res_deg": round(lats[1] - lats[0], 5) if len(lats) > 1 else None,
            "n_lats":  len(lats),
            "n_lons":  len(lons),
            "generated_utc": datetime.datetime.utcnow().isoformat() + "Z",
        },
        "lats": [round(v, 5) for v in lats],
        "lons": [round(v, 5) for v in lons],
        "ssh":  ssh_g,   # absolute SSH (m)
        "sla":  sla_g,   # anomaly from mean sea level (m) — better for display
    }
    grid_path = ALT_DIR / f"altimetry_{date_str}_grid.json"
    with open(grid_path, "w") as fh:
        json.dump(grid_data, fh, separators=(",", ":"), cls=_NpEncoder)
    print(f"    Altimetry grid → {grid_path}  ({grid_path.stat().st_size/1024:.0f} KB)")

    # Latest copy
    import shutil
    shutil.copy(grid_path, ALT_DIR / "altimetry_latest_grid.json")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Ocean Dynamics Fetcher — Currents + Altimetry")
    print(f"  Date      : {TARGET_DATE}")
    print(f"  BBOX      : {BBOX}")
    print(f"  Output    : {OUTPUT_DIR.resolve()}")
    print("=" * 60)

    df_currents = None
    df_altimetry = None

    # ── Currents ──────────────────────────────────────────────────────────────
    # Try HYCOM first (best free resolution)
    df_hycom_curr, df_hycom_ssh = fetch_hycom(TARGET_DATE)
    if df_hycom_curr is not None:
        df_currents = df_hycom_curr

    # OSCAR fallback
    if df_currents is None:
        df_oscar = fetch_oscar(TARGET_DATE)
        if df_oscar is not None:
            df_currents = df_oscar

    # CMEMS (premium — overrides free sources if credentials available)
    df_cmems_curr = fetch_cmems_currents(TARGET_DATE)
    if df_cmems_curr is not None:
        df_currents = df_cmems_curr  # prefer CMEMS when available

    # ── Altimetry ─────────────────────────────────────────────────────────────
    if df_hycom_ssh is not None:
        df_altimetry = df_hycom_ssh

    # CMEMS altimetry (preferred — includes observed SLA, not model SSH)
    df_cmems_alt, df_cmems_geo = fetch_cmems_altimetry(TARGET_DATE)
    if df_cmems_alt is not None:
        df_altimetry = df_cmems_alt
        # If CMEMS geostrophic currents available and no better source, use them
        if df_currents is None and df_cmems_geo is not None:
            df_currents = df_cmems_geo
            print("  Using geostrophic currents from altimetry as current source")

    # ── Export ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  Exporting ...")

    if df_currents is not None:
        export_currents(df_currents, TARGET_DATE)
    else:
        print("  No current data to export")

    if df_altimetry is not None:
        export_altimetry(df_altimetry, TARGET_DATE)
    else:
        print("  No altimetry data to export")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    if df_currents is not None:
        spd = df_currents["speed_ms"]
        print(f"  Currents  ({df_currents['source'].iloc[0]}): "
              f"{len(df_currents):,} pts  "
              f"speed {spd.min():.3f}–{spd.max():.3f} m/s  "
              f"p98={spd.quantile(0.98):.3f} m/s")
    if df_altimetry is not None:
        print(f"  Altimetry ({df_altimetry['source'].iloc[0]}): "
              f"{len(df_altimetry):,} pts  "
              f"SSH {df_altimetry['ssh_m'].min():.3f}–{df_altimetry['ssh_m'].max():.3f} m")
    print("=" * 60)
    print(f"\n  Files → {CURR_DIR.resolve()}")
    print(f"         → {ALT_DIR.resolve()}")

if __name__ == "__main__":
    main()
