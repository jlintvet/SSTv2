#!/usr/bin/env python3
"""
=========================================================
SST DATA FETCHER — NOAA CoastWatch ERDDAP + VIIRS L3U
=========================================================

Three independent data sources serving three distinct UI views:

  MUR       — JPL MUR v4.1 daily L4 composite    (jplMURSST41)
  GOES-19   — Geo-polar Blended daily composite  (noaacwBLENDEDsstDLDaily)
  VIIRS     — ACSPO L3U per-swath multi-pass     (STAR NESDIS file server)

REQUIREMENTS:  pip install requests numpy pandas netCDF4

VERIFYING VIIRS FILE SERVER PATH — run before first deploy:
  python3 -c "
  import requests
  for u in [
    'https://www.star.nesdis.noaa.gov/pub/socd2/coastwatch/sst/nrt/viirs/n20/',
    'https://coastwatch.noaa.gov/pub/socd/mecb/coastwatch/viirs/nrt/n20/',
  ]:
      print(requests.get(u, timeout=10).status_code, u)
  "
=========================================================
"""
import io
import os
import re
import signal
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests

try:
    import netCDF4 as nc
    _NETCDF4_AVAILABLE = True
except ImportError:
    _NETCDF4_AVAILABLE = False
    print("WARNING: netCDF4 not installed — VIIRS multi-pass fetch disabled.")
    print("         pip install netCDF4")


# =========================================================
# HARD TIMEOUT CONTEXT MANAGER
# =========================================================
# requests timeout=(connect, read) caps individual socket reads but NOT
# the total wall-clock time if the server dribbles bytes slowly forever.
# This SIGALRM-based context manager enforces an absolute ceiling.
# Only works on Unix/Linux (GitHub Actions is Linux — fine for CI).

class _TimeoutError(Exception):
    pass

@contextmanager
def hard_timeout(seconds: int, label: str = ""):
    """Raise _TimeoutError if the block takes longer than `seconds`."""
    def _handler(signum, frame):
        raise _TimeoutError(
            f"hard timeout ({seconds}s) exceeded{': ' + label if label else ''}"
        )

    old = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


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
        r = requests.get(NE_LAND_URL, timeout=(10, 30))
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
        print(f"  (land filter: loaded {len(kept)} coastline polygons)")
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
        print(f"  (land filter: dropped {dropped} inland, kept {len(df)} ocean)")
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

# App region — must match frontend bounds and sstSummary.ts
NORTH, SOUTH =  39.00, 33.70
WEST,  EAST  = -78.89, -72.21


# =========================================================
# ERDDAP CONFIG
# =========================================================
ERDDAP_HOST_PFEG = "https://coastwatch.pfeg.noaa.gov/erddap"
ERDDAP_HOST_CW   = "https://coastwatch.noaa.gov/erddap"

# MUR mirrors in priority order.
# stride 5 on 0.01° native = 0.05° ~ 5.5 km, ~14 k pts over the bbox.
MUR_MIRRORS = [
    {
        "host":       ERDDAP_HOST_PFEG,
        "dataset_id": "jplMURSST41",
        "var":        "analysed_sst",
        "stride":     5,
        "units":      "C",   # ERDDAP serves this in °C despite the NetCDF var
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
MUR_DAYS_BACK = 5

# Geo-polar Blended Day+Night (GOES-19 ABI primary geostationary input).
# stride 2 on 0.05° native = 0.10° — still ~3.5 k pts, much smaller response.
# Raising stride reduces transfer size and hang risk without losing visual detail.
GOES_CFG = {
    "host":       ERDDAP_HOST_CW,
    "dataset_id": "noaacwBLENDEDsstDLDaily",
    "var":        "analysed_sst",
    "stride":     2,
    "units":      "C",
}


# =========================================================
# VIIRS L3U GRANULE CONFIG
# =========================================================
VIIRS_BASE_CANDIDATES = [
    "https://www.star.nesdis.noaa.gov/pub/socd2/coastwatch/sst/nrt/viirs",
    "https://coastwatch.noaa.gov/pub/socd/mecb/coastwatch/viirs/nrt",
]
VIIRS_PLATFORMS  = ["npp", "n20", "n21"]
VIIRS_HOURS_BACK = 24


# =========================================================
# HTTP TUNING
# =========================================================
# timeout=(connect_seconds, read_seconds)
# read_seconds caps the time between consecutive bytes from the server.
# A slow-but-alive ERDDAP that streams at 1 byte/read_seconds will still
# hang. The hard_timeout() context manager is the backstop for that case.

HTTP_CONNECT_TIMEOUT   = 15     # seconds to establish TCP connection
HTTP_READ_TIMEOUT      = 90     # seconds of silence before read gives up
HTTP_TIMEOUT           = (HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT)

# Absolute wall-clock ceiling per fetch call (connect + transfer).
# Covers the case where ERDDAP trickle-streams a huge response byte by byte.
ERDDAP_HARD_TIMEOUT_S  = 180    # 3 minutes max per ERDDAP request
VIIRS_HARD_TIMEOUT_S   = 120    # 2 minutes max per granule download

HTTP_RETRIES       = 2
HTTP_BACKOFF_S     = 5
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
    now  = time.monotonic()
    last = _last_request_at.get(host)
    if last is not None:
        wait = REQUEST_SPACING_S - (now - last)
        if wait > 0:
            time.sleep(wait)
    _last_request_at[host] = time.monotonic()


def build_erddap_csv_url(cfg, time_iso, south, north, west, east):
    stride = cfg["stride"]
    query = (
        f"{cfg['var']}"
        f"[({time_iso})]"
        f"[({south}):{stride}:({north})]"
        f"[({west}):{stride}:({east})]"
    )
    return f"{cfg['host']}/griddap/{cfg['dataset_id']}.csv0?{query}"


def fetch_erddap_csv(url, label):
    """
    GET an ERDDAP .csv0 URL.
    Uses (connect, read) timeout tuple AND a hard SIGALRM wall-clock ceiling.
    """
    host = _host_of(url)
    if host in _host_blacklisted:
        raise RuntimeError(f"host {host} blacklisted this run")

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/csv,text/plain;q=0.9,*/*;q=0.5",
    }
    last_err = None

    for attempt in range(1, HTTP_RETRIES + 2):
        _throttle(host)
        try:
            with hard_timeout(ERDDAP_HARD_TIMEOUT_S, label):
                resp = requests.get(
                    url,
                    timeout=HTTP_TIMEOUT,
                    headers=headers,
                )
        except _TimeoutError as e:
            last_err = str(e)
            print(f"\n  ⚠ {last_err}")
            # Don't retry a hard timeout — the server is clearly stuck
            raise RuntimeError(last_err)
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
            if isinstance(e, (requests.ConnectionError, requests.Timeout)):
                _host_conn_resets[host] = _host_conn_resets.get(host, 0) + 1
                if _host_conn_resets[host] >= CONN_RESET_THRESHOLD:
                    _host_blacklisted.add(host)
                    raise RuntimeError(f"host {host} keeps resetting — blacklisted")
                raise RuntimeError(last_err)
            if attempt <= HTTP_RETRIES:
                time.sleep(HTTP_BACKOFF_S * attempt)
            continue

        if resp.status_code == 200:
            return resp.text
        if resp.status_code == 404:
            raise RuntimeError("404 — no data for this date")
        if resp.status_code == 403:
            _host_blacklisted.add(host)
            raise RuntimeError(f"403 Forbidden — blacklisted: {resp.text[:200]}")
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
    if not csv_text or not csv_text.strip():
        raise RuntimeError("empty response body")
    df = pd.read_csv(
        io.StringIO(csv_text),
        header=None,
        names=["time", "lat", "lon", "sst"],
        dtype={"time": str},
    )
    if df.empty:
        raise RuntimeError("0 data rows")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["sst"] = pd.to_numeric(df["sst"], errors="coerce")
    df = df.dropna(subset=["lat", "lon", "sst"])
    if df.empty:
        raise RuntimeError("all SST values NaN")
    if cfg["units"] == "K":
        df["sst"] = df["sst"] - 273.15
    df = df[["lat", "lon", "sst"]]
    df["lat"] = df["lat"].round(COORD_DECIMALS)
    df["lon"] = df["lon"].round(COORD_DECIMALS)
    df["sst"] = df["sst"].round(SST_DECIMALS)
    df = filter_to_ocean(df, label="ingest")
    return df


def write_csv(df, base_path, label):
    n_pts = len(df)
    if n_pts < 100:
        print(f"  ⚠ {label}: only {n_pts} pts — skipping write.")
        return False
    n_lat    = df["lat"].nunique()
    n_lon    = df["lon"].nunique()
    expected = n_lat * n_lon
    coverage = n_pts / expected if expected > 0 else 0
    threshold = 0.05 if "VIIRS" in label else 0.30
    if coverage < threshold:
        print(
            f"  ⚠ {label}: sparse — {n_pts} pts / "
            f"{n_lat}×{n_lon}={expected} ({coverage*100:.0f}%)"
        )
    path = base_path + ".csv"
    df.to_csv(path, index=False)
    print(f"  → {path}  ({n_pts} pts, {n_lat} lats × {n_lon} lons)")
    return True


def fetch_one_day_erddap(cfg, time_iso, label):
    url      = build_erddap_csv_url(cfg, time_iso, SOUTH, NORTH, WEST, EAST)
    csv_text = fetch_erddap_csv(url, label)
    return parse_erddap_csv0(csv_text, cfg)


# =========================================================
# MUR — DAILY COMPOSITE
# =========================================================
def fetch_mur():
    print(f"\n── MUR daily composite (last {MUR_DAYS_BACK} days) ──")
    success = 0

    for i in range(1, MUR_DAYS_BACK + 1):
        ts       = datetime.now(timezone.utc) - timedelta(days=i)
        stamp    = ts.strftime("%Y%m%d")
        time_iso = ts.strftime("%Y-%m-%d") + "T09:00:00Z"

        df = None
        last_err = None
        for cfg in MUR_MIRRORS:
            mhost = cfg["host"].split("/", 3)[2]
            if mhost in _host_blacklisted:
                continue
            try:
                print(f"  MUR {stamp} ({mhost}) … ", end="", flush=True)
                df = fetch_one_day_erddap(cfg, time_iso, f"MUR {stamp}")
                break
            except Exception as e:
                last_err = f"{mhost}: {type(e).__name__}: {str(e)[:120]}"
                print(f"✗ {type(e).__name__}")
                continue

        if df is None:
            print(f"  ✗ MUR {stamp} — all mirrors failed. Last: {last_err}")
            continue

        print(f"  ✓ MUR {stamp}")
        path = os.path.join(DIRS["mur"], f"mur_{stamp}")
        if write_csv(df, path, f"MUR {stamp}"):
            success += 1

    if success == 0:
        print("  ⚠ MUR: zero successful days.")


# =========================================================
# GOES — BLENDED DAILY COMPOSITE
# =========================================================
def fetch_goes():
    """
    Pull the most recent Geo-polar Blended Day+Night snapshot via ERDDAP.
    One honest daily file — not replicated to fake hourly slots.
    """
    print("\n── GOES-19 geo-polar blended daily composite ──")

    df_latest = None
    latest_ts = None

    for i in range(0, 4):
        ts       = datetime.now(timezone.utc) - timedelta(days=i)
        time_iso = ts.strftime("%Y-%m-%d") + "T12:00:00Z"
        label    = f"GOES {ts:%Y%m%d}"
        try:
            print(f"  {label} … ", end="", flush=True)
            df_latest = fetch_one_day_erddap(GOES_CFG, time_iso, label)
            latest_ts = ts
            print(f"  ✓ Geo-polar Blended {ts:%Y-%m-%d}")
            break
        except Exception as e:
            print(f"✗ {type(e).__name__}: {str(e)[:80]}")
            continue

    if df_latest is None:
        print("  ✗ GOES: no recent data available.")
        return

    stamp = latest_ts.strftime("%Y%m%d")
    path  = os.path.join(DIRS["goes_composite"], f"goes_composite_{stamp}")
    write_csv(df_latest, path, f"GOES composite {stamp}")


# =========================================================
# VIIRS — MULTI-PASS (L3U GRANULE FILE SERVER)
# =========================================================

def _probe_viirs_base() -> str:
    for base in VIIRS_BASE_CANDIDATES:
        test_url = f"{base}/n20/"
        try:
            r = requests.get(
                test_url,
                timeout=(10, 20),
                headers={"User-Agent": USER_AGENT},
                allow_redirects=True,
            )
            if r.status_code in (200, 403):
                print(f"  ✓ VIIRS NRT base: {base}")
                return base
        except requests.RequestException:
            pass
    raise RuntimeError(
        "Neither VIIRS NRT file server URL is reachable. "
        "Check VIIRS_BASE_CANDIDATES."
    )


def _list_viirs_granules(base: str, platform: str, year: int, doy: int) -> list:
    url  = f"{base}/{platform}/l3u/{year}/{doy:03d}/"
    host = _host_of(url)
    try:
        _throttle(host)
        resp = requests.get(
            url,
            timeout=(10, 30),
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
    return datetime.strptime(filename[:14], "%Y%m%d%H%M%S").replace(
        tzinfo=timezone.utc
    )


def _fetch_viirs_granule(
    base: str, platform: str, year: int, doy: int, filename: str
) -> pd.DataFrame:
    """
    Download one ACSPO L3U NetCDF4 granule (~25 MB global 0.02° grid),
    subset to bbox, return DataFrame [lat, lon, sst] in °C.
    Returns None on any failure.
    Cloud/land pixels are fill values → NaN → dropped — expected for L3U.
    """
    if not _NETCDF4_AVAILABLE:
        return None

    url  = f"{base}/{platform}/l3u/{year}/{doy:03d}/{filename}"
    host = _host_of(url)
    if host in _host_blacklisted:
        return None

    try:
        _throttle(host)
        with hard_timeout(VIIRS_HARD_TIMEOUT_S, filename):
            resp = requests.get(
                url,
                timeout=HTTP_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
                stream=False,
            )
            resp.raise_for_status()
    except _TimeoutError as e:
        print(f"\n  ⚠ {e}")
        return None
    except requests.RequestException as e:
        print(f"  ⚠ download failed {filename}: {e}")
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

        lat_mask = (lat_full >= SOUTH) & (lat_full <= NORTH)
        lon_mask = (lon_full >= WEST)  & (lon_full <= EAST)
        lat_idx  = np.where(lat_mask)[0]
        lon_idx  = np.where(lon_mask)[0]

        if len(lat_idx) == 0 or len(lon_idx) == 0:
            ds.close()
            return None

        lat_sl  = slice(int(lat_idx[0]), int(lat_idx[-1]) + 1)
        lon_sl  = slice(int(lon_idx[0]), int(lon_idx[-1]) + 1)

        sst_sub = sst_var[0, lat_sl, lon_sl]
        lat_sub = lat_full[lat_sl]
        lon_sub = lon_full[lon_sl]
        ds.close()

    except Exception as e:
        print(f"  ⚠ NetCDF parse failed {filename}: {e}")
        return None

    lon_grid, lat_grid = np.meshgrid(lon_sub, lat_sub)
    sst_c = np.ma.filled(sst_sub, np.nan).astype(np.float64) - 273.15

    df = pd.DataFrame({
        "lat": lat_grid.flatten(),
        "lon": lon_grid.flatten(),
        "sst": sst_c.flatten(),
    })
    df = df.dropna(subset=["sst"])
    df = df[(df["sst"] > -2.0) & (df["sst"] < 40.0)]
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
    ~2 passes × 3 satellites = ~6 distinct files per 24-hour window.
    """
    if not _NETCDF4_AVAILABLE:
        print("\n── VIIRS multi-pass ──")
        print("  skipped (netCDF4 not installed — pip install netCDF4)")
        return

    print(f"\n── VIIRS multi-pass (last {VIIRS_HOURS_BACK}h — NPP/N20/N21) ──")

    try:
        live_base = _probe_viirs_base()
    except RuntimeError as e:
        print(f"  ✗ {e}")
        return

    now_utc = datetime.now(timezone.utc)
    cutoff  = now_utc - timedelta(hours=VIIRS_HOURS_BACK)

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

                if os.path.exists(out_path + ".csv"):
                    total_skipped += 1
                    continue

                print(f"  {platform} {stamp} … ", end="", flush=True)
                df = _fetch_viirs_granule(live_base, platform, year, doy, fname)

                if df is not None and write_csv(
                    df, out_path, f"VIIRS {platform} {stamp}"
                ):
                    total_new += 1
                else:
                    print("✗ no usable data in bbox")

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
    print(f"  Bbox : {SOUTH}–{NORTH}°N  {WEST}–{EAST}°E")
    print("=" * 57)

    ensure_dirs()

    fetch_mur()           # View 1: MUR smooth gap-filled L4 daily
    fetch_goes()          # View 2: GOES-19 geo-polar blended daily
    fetch_viirs_passes()  # View 3: VIIRS per-swath multi-pass

    print("\n" + "=" * 57)
    print("✓ Pipeline complete")
    print("=" * 57)


if __name__ == "__main__":
    main()
