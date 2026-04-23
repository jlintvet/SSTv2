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

=========================================================
VIIRS PASS TARGETING STRATEGY
=========================================================

NPP, NOAA-20, and NOAA-21 all fly ~1:30pm local solar time ascending node.
For the Mid-Atlantic center (~36N, 75.5W = UTC-5.03h):

  Ascending  (day)   1:30pm local -> 18:33 UTC
  Descending (night) 1:30am local -> 06:33 UTC

Observed from CI logs (2026-04-22, NPP):
  Night pass hit at 0610 UTC  (36,633 pts)
  Next hit at    0750 UTC     (28,052 pts) — adjacent orbit, ~100 min later

Each satellite makes one meaningful crossing of our bbox per pass.
The VIIRS swath is ~3000 km wide and the bbox is ~700 km wide, so the
crossing takes roughly one 10-minute granule to cover, sometimes two.

Strategy: for each satellite, for each pass (night/day), download only
the granules within VIIRS_PASS_CENTER_WINDOW_MIN minutes of the known
center time. This limits downloads to ~3-5 per satellite per pass
instead of 20+ from a broad hourly window scan.

Center times and the window width are tunable below. If a pass is
consistently missed, widen VIIRS_PASS_CENTER_WINDOW_MIN by 10 and re-run.
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
class _TimeoutError(Exception):
    pass


@contextmanager
def hard_timeout(seconds: int, label: str = ""):
    def _handler(signum, frame):
        raise _TimeoutError(
            f"hard timeout ({seconds}s) exceeded"
            + (f": {label}" if label else "")
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

VIIRS_BBOX_PAD = 1.0   # degrees padding for bbox slice


# =========================================================
# ERDDAP CONFIG
# =========================================================
ERDDAP_HOST_PFEG = "https://coastwatch.pfeg.noaa.gov/erddap"
ERDDAP_HOST_CW   = "https://coastwatch.noaa.gov/erddap"

# MUR mirrors — coastwatch.noaa.gov first (more tolerant of CI runner IPs).
MUR_MIRRORS = [
    {
        "host":       ERDDAP_HOST_CW,
        "dataset_id": "jplMURSST41",
        "var":        "analysed_sst",
        "stride":     5,
        "units":      "C",
    },
    {
        "host":       ERDDAP_HOST_PFEG,
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

# Geo-polar Blended Day+Night. stride 2 on 0.05 deg native = 0.10 deg.
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
VIIRS_HOURS_BACK = 30   # look back 30h so we always capture both passes

# Known pass center times (UTC hours + minutes) for the Mid-Atlantic.
# Derived from orbital mechanics for ~36N / 75.5W (UTC-5.03h) and
# confirmed from CI logs: NPP night pass hit at 0610 UTC on 2026-04-22.
#
# Format: list of (hour, minute) tuples, one per pass per day.
# N20 and N21 are on very similar orbits to NPP — their passes arrive
# within ~1-2 minutes of the same center times, so the same targets work
# for all three platforms.
#
# The VIIRS orbital period is ~101 minutes, so successive passes of the
# same satellite over the same region are ~101 min apart. For our bbox
# there are typically 2 crossings per satellite per day — one ascending
# (daytime) and one descending (nighttime).
VIIRS_PASS_CENTERS_UTC = [
    ( 6, 10),   # night/descending — confirmed 0610 UTC from logs
    (18, 33),   # day/ascending    — calculated, not yet confirmed
]

# Download granules within ±this many minutes of each pass center.
# At 10 min/granule, 30 min = 3 granules per side = 7 granules total per pass.
# Observed: the actual bbox-crossing is usually 1-2 granules at the center.
# 30 min gives comfortable margin for orbital drift across the season.
VIIRS_PASS_CENTER_WINDOW_MIN = 30

# If a pass consistently misses, increase this by 10 and re-run.
# Maximum meaningful value is ~50 min (half the orbital period / 2).


# =========================================================
# HTTP TUNING
# =========================================================
HTTP_CONNECT_TIMEOUT  = 15
HTTP_READ_TIMEOUT     = 90
HTTP_TIMEOUT          = (HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT)

ERDDAP_HARD_TIMEOUT_S = 180
VIIRS_HARD_TIMEOUT_S  = 120

HTTP_RETRIES      = 2
HTTP_BACKOFF_S    = 5
REQUEST_SPACING_S = 2.0
USER_AGENT        = "SSTv2-fetcher/1.0 (+https://github.com/jlintvet/SSTv2)"

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
    """GET an ERDDAP .csv0 URL with timeout tuple and SIGALRM backstop."""
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
                resp = requests.get(url, timeout=HTTP_TIMEOUT, headers=headers)
        except _TimeoutError as e:
            last_err = str(e)
            print(f"\n  ⚠ {last_err}")
            raise RuntimeError(last_err)
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
            if isinstance(e, (requests.ConnectionError, requests.Timeout)):
                _host_conn_resets[host] = _host_conn_resets.get(host, 0) + 1
                if _host_conn_resets[host] >= CONN_RESET_THRESHOLD:
                    _host_blacklisted.add(host)
                    raise RuntimeError(f"host {host} blacklisted after resets")
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

        out_path = os.path.join(DIRS["mur"], f"mur_{stamp}.csv")
        if os.path.exists(out_path):
            print(f"  ✓ MUR {stamp} (cached)")
            success += 1
            continue

        df = None
        last_err = None
        for cfg in MUR_MIRRORS:
            mhost = _host_of(cfg["host"])
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
    print("\n── GOES-19 geo-polar blended daily composite ──")

    for i in range(0, 4):
        ts       = datetime.now(timezone.utc) - timedelta(days=i)
        time_iso = ts.strftime("%Y-%m-%d") + "T12:00:00Z"
        stamp    = ts.strftime("%Y%m%d")
        label    = f"GOES {stamp}"

        out_path = os.path.join(DIRS["goes_composite"], f"goes_composite_{stamp}.csv")
        if os.path.exists(out_path):
            print(f"  ✓ GOES composite {stamp} (cached)")
            return

        try:
            print(f"  {label} … ", end="", flush=True)
            df = fetch_one_day_erddap(GOES_CFG, time_iso, label)
            print(f"  ✓ Geo-polar Blended {ts:%Y-%m-%d}")
            path = os.path.join(DIRS["goes_composite"], f"goes_composite_{stamp}")
            write_csv(df, path, f"GOES composite {stamp}")
            return
        except Exception as e:
            print(f"✗ {type(e).__name__}: {str(e)[:80]}")
            continue

    print("  ✗ GOES: no recent data available.")


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


def _build_target_windows(now_utc: datetime, lookback_hours: int) -> list:
    """
    Build the list of (window_start, window_end) datetime pairs to fetch,
    by projecting VIIRS_PASS_CENTERS_UTC backward over lookback_hours.

    For each pass center (h, m) we generate one target window per calendar
    day covered by the lookback period. Each window is:
        center - VIIRS_PASS_CENTER_WINDOW_MIN  to
        center + VIIRS_PASS_CENTER_WINDOW_MIN

    Only windows that overlap [now_utc - lookback_hours, now_utc] are kept.

    Returns a list of (window_start_utc, window_end_utc, center_label) tuples,
    sorted chronologically.
    """
    cutoff  = now_utc - timedelta(hours=lookback_hours)
    half    = timedelta(minutes=VIIRS_PASS_CENTER_WINDOW_MIN)
    windows = []

    # Check 2 days back to cover lookback period straddling midnight
    for day_offset in range(3):
        base_date = (now_utc - timedelta(days=day_offset)).date()
        for (h, m) in VIIRS_PASS_CENTERS_UTC:
            center = datetime(
                base_date.year, base_date.month, base_date.day,
                h, m, 0, tzinfo=timezone.utc
            )
            w_start = center - half
            w_end   = center + half
            # Keep window only if it overlaps the lookback period
            if w_end >= cutoff and w_start <= now_utc:
                windows.append((w_start, w_end, f"{h:02d}{m:02d}UTC"))

    # Deduplicate (same center can appear from multiple day offsets)
    seen = set()
    unique = []
    for w in sorted(windows):
        key = w[0]
        if key not in seen:
            seen.add(key)
            unique.append(w)

    return unique


def _fetch_viirs_granule(
    base: str, platform: str, year: int, doy: int, filename: str
) -> tuple:
    """
    Download one ACSPO L3U NetCDF4 granule, subset to bbox.
    Returns (DataFrame, reason_string). DataFrame is None on failure.
    """
    if not _NETCDF4_AVAILABLE:
        return None, "netCDF4 not installed"

    url  = f"{base}/{platform}/l3u/{year}/{doy:03d}/{filename}"
    host = _host_of(url)
    if host in _host_blacklisted:
        return None, "host blacklisted"

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
        return None, f"hard timeout: {e}"
    except requests.RequestException as e:
        if isinstance(e, (requests.ConnectionError, requests.Timeout)):
            _host_conn_resets[host] = _host_conn_resets.get(host, 0) + 1
            if _host_conn_resets[host] >= CONN_RESET_THRESHOLD:
                _host_blacklisted.add(host)
        return None, f"download error: {type(e).__name__}"

    try:
        ds = nc.Dataset("inmemory.nc", memory=resp.content)

        lat_full = ds.variables["lat"][:]
        lon_full = ds.variables["lon"][:]
        sst_var  = ds.variables["sea_surface_temperature"]

        pad      = VIIRS_BBOX_PAD
        lat_mask = (lat_full >= SOUTH - pad) & (lat_full <= NORTH + pad)
        lon_mask = (lon_full >= WEST  - pad) & (lon_full <= EAST  + pad)
        lat_idx  = np.where(lat_mask)[0]
        lon_idx  = np.where(lon_mask)[0]

        if len(lat_idx) == 0 or len(lon_idx) == 0:
            ds.close()
            return None, "swath does not overlap bbox"

        lat_sl  = slice(int(lat_idx[0]), int(lat_idx[-1]) + 1)
        lon_sl  = slice(int(lon_idx[0]), int(lon_idx[-1]) + 1)

        sst_sub = sst_var[0, lat_sl, lon_sl]
        lat_sub = lat_full[lat_sl]
        lon_sub = lon_full[lon_sl]
        ds.close()

    except Exception as e:
        return None, f"NetCDF parse error: {e}"

    lon_grid, lat_grid = np.meshgrid(lon_sub, lat_sub)
    sst_c = np.ma.filled(sst_sub, np.nan).astype(np.float64) - 273.15

    df = pd.DataFrame({
        "lat": lat_grid.flatten(),
        "lon": lon_grid.flatten(),
        "sst": sst_c.flatten(),
    })
    df = df.dropna(subset=["sst"])
    df = df[
        (df["lat"] >= SOUTH) & (df["lat"] <= NORTH) &
        (df["lon"] >= WEST)  & (df["lon"] <= EAST)
    ]
    df = df[(df["sst"] > -2.0) & (df["sst"] < 40.0)]

    if df.empty:
        return None, "swath overlaps bbox but all pixels cloud/land masked"

    df["lat"] = df["lat"].round(COORD_DECIMALS)
    df["lon"] = df["lon"].round(COORD_DECIMALS)
    df["sst"] = df["sst"].round(SST_DECIMALS)
    df = filter_to_ocean(df, label=filename)

    if df.empty:
        return None, "all bbox pixels filtered as inland water"

    return df, "ok"


def fetch_viirs_passes():
    """
    Fetch VIIRS ACSPO L3U granules near known pass center times.

    Instead of scanning a broad hourly window (20+ granules per pass),
    we target only granules within VIIRS_PASS_CENTER_WINDOW_MIN minutes
    of the known pass center time. This limits downloads to ~7 granules
    per pass per platform instead of 20+.

    Total worst-case downloads per run:
      2 passes × 3 platforms × ~7 granules = ~42
    With caching of already-written files this is typically much lower.
    """
    if not _NETCDF4_AVAILABLE:
        print("\n── VIIRS multi-pass ──")
        print("  skipped (netCDF4 not installed — pip install netCDF4)")
        return

    half_min = VIIRS_PASS_CENTER_WINDOW_MIN
    centers  = [f"{h:02d}:{m:02d} UTC" for h, m in VIIRS_PASS_CENTERS_UTC]
    print(
        f"\n── VIIRS multi-pass (last {VIIRS_HOURS_BACK}h — NPP/N20/N21) ──\n"
        f"  Pass centers: {', '.join(centers)}  ±{half_min} min"
    )

    try:
        live_base = _probe_viirs_base()
    except RuntimeError as e:
        print(f"  ✗ {e}")
        return

    now_utc  = datetime.now(timezone.utc)
    windows  = _build_target_windows(now_utc, VIIRS_HOURS_BACK)

    if not windows:
        print("  ⚠ No pass windows fall within the lookback period.")
        return

    print(f"  Target windows this run: {len(windows)}")
    for w_start, w_end, label in windows:
        print(f"    {w_start:%Y-%m-%d %H:%M} – {w_end:%H:%M} UTC  ({label})")

    # Collect all (year, doy) pairs covered by the windows
    day_pairs = set()
    for w_start, w_end, _ in windows:
        for dt in (w_start, w_end):
            day_pairs.add((dt.year, dt.timetuple().tm_yday))

    total_new      = 0
    total_skipped  = 0
    total_filtered = 0
    total_miss     = 0

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

                # Check if this granule falls inside any target window
                in_window = any(
                    w_start <= gran_time <= w_end
                    for w_start, w_end, _ in windows
                )
                if not in_window:
                    total_filtered += 1
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
                df, reason = _fetch_viirs_granule(
                    live_base, platform, year, doy, fname
                )

                if df is not None and write_csv(
                    df, out_path, f"VIIRS {platform} {stamp}"
                ):
                    total_new += 1
                else:
                    print(f"✗ {reason}")
                    total_miss += 1

    print(
        f"\n  VIIRS summary: {total_new} written, {total_skipped} cached,"
        f" {total_miss} miss/cloud, {total_filtered} outside windows."
    )
    if total_new == 0 and total_skipped == 0:
        print(
            "  ⚠ No VIIRS data produced.\n"
            "    If this persists across multiple runs, the pass center times\n"
            "    may need adjustment. Check logs for the nearest miss/hit and\n"
            "    update VIIRS_PASS_CENTERS_UTC or increase\n"
            "    VIIRS_PASS_CENTER_WINDOW_MIN."
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

    fetch_mur()
    fetch_goes()
    fetch_viirs_passes()

    print("\n" + "=" * 57)
    print("✓ Pipeline complete")
    print("=" * 57)


if __name__ == "__main__":
    main()
