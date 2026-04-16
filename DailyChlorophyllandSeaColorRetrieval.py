"""
DailyChlorophyllandSeaColorRetrieval.py
========================================
Retrieves the last three days of:

  1. Chlorophyll-a (CHL)
     ─────────────────────────────────────────────────────────────────────
     Daily L3 4 km composite — cloud pixels are null (visible-band sensor).
     Sources tried in order:
       1. Blended SNPP+NOAA-20 VIIRS (nesdisVHNnoaaSNPPnoaa20chlaDaily) — pfeg-hosted,
          may bypass the coastwatch.noaa.gov griddap backend
       2. NOAA-20 VIIRS NRT (noaacwNPPN20VIIRSchlociDaily) — separate satellite pipeline,
          may succeed when S-NPP pipeline is 500ing
       3. VIIRS S-NPP NRT  (noaacwNPPVIIRSchlaDaily)   — same-day, coastwatch.noaa.gov
       4. VIIRS S-NPP SQ   (noaacwNPPVIIRSSQchlaDaily) — science quality, ~3-day lag
       5. MODIS Aqua SQ    (erdMH1chla1day)             — archive fallback (MODIS Aqua
          hardware issues since 2022; may not have recent data)

     8-day composite — gap-filled, better spatial coverage:
       VIIRS SNPP+NOAA-20 DINEOF  (noaacwNPPN20VIIRSDINEOFDaily) — cloud gap-filled, 9km
       VIIRS+S3A DINEOF SQ        (nesdisNPPN20S3ASCIDINEOFDaily) — polarwatch, gap-filled
       MODIS Aqua 8day            (erdMH1chla8day)  — polarwatch.noaa.gov

     Per-cell color classification from chlorophyll-a (mg/m³):
       blue_water  : chl < 0.15   — oligotrophic / Gulf Stream
       mixed       : 0.15 – 0.50  — transitional
       green_water : chl > 0.50   — productive shelf water

     Output folder  : DailySST/Chlorophyll/
     Files          : CHL_YYYYMMDD.json
                      CHL_8day_YYYYMMDD.json
                      chl_latest.json
                      chl_8day_latest.json
                      chl_manifest.json

  2. Sea Color / Water Clarity (SEACOLOR)
     ─────────────────────────────────────────────────────────────────────
     Primary metric: Kd490 — diffuse attenuation coefficient at 490 nm (m⁻¹)
       Low Kd490  → clear blue water (Gulf Stream, oligotrophic)
       High Kd490 → turbid green water (shelf, productive, sediment-loaded)

     The blue/green boundary identified by Kd490 is the primary search cue
     for offshore pelagic species (mahi-mahi, tuna, billfish, wahoo).

     Sources tried in order:
       VIIRS NOAA-20 NRT (noaacwNPPN20VIIRSkd490Daily)  — separate satellite pipeline, tried first
       VIIRS SNPP NRT    (noaacwNPPVIIRSkd490Daily)     — NRT daily
       VIIRS SNPP SQ     (noaacwNPPVIIRSSQkd490Daily)   — science quality, ID confirmed from redirect logs

     Per-cell color classification from Kd490 (m⁻¹):
       blue_water  : kd490 < 0.06    — clear, oligotrophic
       mixed       : 0.06 – 0.12     — transitional zone
       green_water : kd490 > 0.12    — turbid, productive

     Output folder  : DailySST/v2/SeaColor/
     Files          : SEACOLOR_YYYYMMDD.json
                      seacolor_latest.json
                      seacolor_manifest.json

Cloud note
──────────
Chlorophyll and Kd490 are both derived from visible-band radiometry.
Cloud-covered and sun-glint pixels are null in all daily products.
The 8-day chlorophyll composite significantly reduces cloud gaps.
No gap-filled product is generated for Kd490 — cloud gaps are
preserved as null and excluded from rendering and feature detection.

Thresholds reference
────────────────────
Chlorophyll classification adapted from:
  NOAA CoastWatch OceanWatch product documentation
  Hooker et al. (1992) — SeaWiFS ocean color classification

Kd490 classification adapted from:
  Lee et al. (2005) — Remote sensing of inherent optical properties
  NOAA CoralTemp water clarity methodology
"""

import csv
import hashlib
import io
import json
import logging
import datetime
import os
import pathlib
import tempfile
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LAT_MIN = 33.70
LAT_MAX = 39.00
LON_MIN = -78.89
LON_MAX = -72.21
STRIDE  = 1

RETENTION_DAYS = 3
SEARCH_WINDOW  = 7      # days back to search if latest dates unavailable
TIMEOUT        = 60     # seconds — short; ERDDAP 500/404 errors are persistent, not transient
MAX_RETRIES    = 2      # retries only on network-level errors (429, 502, 503, 504)
BACKOFF_FACTOR = 1      # 1 s, 2 s between retries

# ---------------------------------------------------------------------------
# CMEMS (Copernicus Marine Service) — Sentinel-3 OLCI, 300m, primary source
# ---------------------------------------------------------------------------
# Set CMEMS_USERNAME and CMEMS_PASSWORD as GitHub repository secrets.
# When credentials are present, CMEMS is tried first (before ERDDAP).
# Sentinel-3 OLCI delivers chlorophyll and Kd490 at 300m native resolution —
# higher fidelity than 4km VIIRS L3 global products.
#
# CMEMS product: OCEANCOLOUR_GLO_BGC_L3_NRT_009_101
#   Chlorophyll : cmems_obs-oc_glo_bgc-plankton_nrt_l3-olci-300m_P1D  var: CHL
#   Kd490       : cmems_obs-oc_glo_bgc-optics_nrt_l3-olci-300m_P1D    var: KD490
#
# Latency: ~3–6 hours after overpass (NRT). Both Sentinel-3A and 3B contribute.
# ---------------------------------------------------------------------------
CMEMS_USERNAME = os.environ.get("CMEMS_USERNAME", "")
CMEMS_PASSWORD = os.environ.get("CMEMS_PASSWORD", "")
CMEMS_ENABLED  = bool(CMEMS_USERNAME and CMEMS_PASSWORD)

# Dataset IDs within OCEANCOLOUR_GLO_BGC_L3_NRT_009_101
CMEMS_CHL_DATASET_ID   = "cmems_obs-oc_glo_bgc-plankton_nrt_l3-olci-300m_P1D"
CMEMS_KD490_DATASET_ID = "cmems_obs-oc_glo_bgc-optics_nrt_l3-olci-300m_P1D"

# Stride applied when reading the 300m NetCDF to keep output file size
# manageable.  stride=4 → ~1.2 km effective resolution, ~250k cells in bbox.
CMEMS_STRIDE = 4

# Output directories (relative to script location)
_SCRIPT_DIR         = pathlib.Path(__file__).resolve().parent
CHL_OUTPUT_DIR      = _SCRIPT_DIR / "DailySST" / "Chlorophyll"
SEACOLOR_OUTPUT_DIR = _SCRIPT_DIR / "DailySST" / "v2" / "SeaColor"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sea color classification thresholds
# ---------------------------------------------------------------------------
# Kd490 (m⁻¹) — diffuse attenuation at 490 nm
KD490_BLUE_THRESHOLD  = 0.06    # < 0.06  → blue water
KD490_GREEN_THRESHOLD = 0.12    # > 0.12  → green water

# Chlorophyll-a (mg/m³)
CHL_BLUE_THRESHOLD  = 0.15      # < 0.15  → blue water
CHL_GREEN_THRESHOLD = 0.50      # > 0.50  → green water

# ---------------------------------------------------------------------------
# ERDDAP source lists
# Each entry: (base_url, erddap_variable_name, source_label)
# ---------------------------------------------------------------------------

# ── Background: server consolidation ─────────────────────────────────────────
# As of 2025-2026 both coastwatch.pfeg.noaa.gov AND polarwatch.noaa.gov issue
# HTTP 301/302 redirects for all VIIRS griddap requests, landing at
# coastwatch.noaa.gov with renamed dataset IDs (noaacwNPP* prefix).
# Querying the pfeg/polarwatch hostnames therefore doubles latency and still
# hits the same backend.  Sources below target coastwatch.noaa.gov directly.
#
# Naming convention at coastwatch.noaa.gov:
#   noaacwNPPVIIRS<product>Daily   — Near Real-Time  (NRT, same-day processing)
#   noaacwNPPVIIRSSQ<product>Daily — Science Quality (SQ, reprocessed, ~3-day lag)
# NRT is listed first: it is more likely to have yesterday's data ready in time
# for the morning cron run.  SQ is retained as a fallback for higher quality.
#
# coastwatch.pfeg.noaa.gov is kept only for MODIS Aqua products where no
# canonical coastwatch.noaa.gov equivalent has been confirmed.
# ─────────────────────────────────────────────────────────────────────────────

# ── Chlorophyll-a daily ───────────────────────────────────────────────────────
# Source priority rationale:
# 1. Blended SNPP+NOAA-20 on pfeg: uses old "nesdis" naming, may be locally
#    served on coastwatch.pfeg.noaa.gov without redirecting to coastwatch.noaa.gov.
#    If pfeg now proxies all griddap to coastwatch.noaa.gov this will also 500,
#    but it's worth trying first since pfeg has historically kept some datasets local.
# 2. NOAA-20 VIIRS (OCI algorithm): separate satellite, separate processing
#    pipeline on coastwatch.noaa.gov.  Variable name is chlor_a.
# 3+4. S-NPP NRT and SQ: both on coastwatch.noaa.gov; these are the canonical
#    sources but currently 500-ing during server outage.
# 5. MODIS Aqua: hardware issues since 2022; recent data may be absent (404).
CHL_DAILY_SOURCES = [
    ("https://coastwatch.pfeg.noaa.gov/erddap/griddap/nesdisVHNnoaaSNPPnoaa20chlaDaily.csvp", "chlorophyll", "VIIRS_SNPP_N20_Blend"),
    ("https://coastwatch.noaa.gov/erddap/griddap/noaacwNPPN20VIIRSchlociDaily.csvp",          "chlor_a",     "VIIRS_NOAA20_NRT"),
    ("https://coastwatch.noaa.gov/erddap/griddap/noaacwNPPVIIRSchlaDaily.csvp",               "chlorophyll", "VIIRS_SNPP_NRT"),
    ("https://coastwatch.noaa.gov/erddap/griddap/noaacwNPPVIIRSSQchlaDaily.csvp",             "chlorophyll", "VIIRS_SNPP_SQ"),
    ("https://coastwatch.pfeg.noaa.gov/erddap/griddap/erdMH1chla1day.csvp",                   "chlorophyll", "MODIS_Aqua"),
]

# ── Chlorophyll-a 8-day / gap-filled composite ────────────────────────────────
# DINEOF (Data Interpolating Empirical Orthogonal Functions) gap-fills cloud
# pixels using spatiotemporal interpolation — far better spatial coverage than
# a raw 8-day max composite.  9km resolution is coarser but cloud-free.
# Blended VIIRS+Sentinel-3A DINEOF on polarwatch is the best available gap-fill.
CHL_8DAY_SOURCES = [
    # DINEOF gap-filled — best cloud coverage, variable name is chlor_a
    ("https://coastwatch.noaa.gov/erddap/griddap/noaacwNPPN20VIIRSDINEOFDaily.csvp",     "chlor_a",     "VIIRS_N20_DINEOF_9km"),
    ("https://polarwatch.noaa.gov/erddap/griddap/nesdisNPPN20S3ASCIDINEOFDaily.csvp",    "chlor_a",     "VIIRS_S3A_DINEOF_SQ"),
    # Standard 8-day composites
    ("https://polarwatch.noaa.gov/erddap/griddap/erdMH1chla8day.csvp",                   "chlorophyll", "MODIS_Aqua_8day"),
    ("https://coastwatch.pfeg.noaa.gov/erddap/griddap/erdMH1chla8day.csvp",              "chlorophyll", "MODIS_Aqua_8day"),
    ("https://coastwatch.noaa.gov/erddap/griddap/noaacwNPPVIIRSSQchlaWeekly.csvp",      "chlorophyll", "VIIRS_SNPP_SQ_8day"),
]

# ── Kd490 daily L3 4 km ───────────────────────────────────────────────────────
# Same rationale as CHL: NOAA-20 tried first (separate satellite pipeline),
# then S-NPP NRT and SQ.  SQ dataset ID confirmed from redirect logs.
# If noaacwNPPN20VIIRSkd490Daily does not exist (404), script falls through
# to S-NPP sources gracefully.
KD490_DAILY_SOURCES = [
    ("https://coastwatch.noaa.gov/erddap/griddap/noaacwNPPN20VIIRSkd490Daily.csvp",   "kd490", "VIIRS_NOAA20_NRT"),
    ("https://coastwatch.noaa.gov/erddap/griddap/noaacwNPPVIIRSkd490Daily.csvp",      "kd490", "VIIRS_SNPP_NRT"),
    ("https://coastwatch.noaa.gov/erddap/griddap/noaacwNPPVIIRSSQkd490Daily.csvp",    "kd490", "VIIRS_SNPP_SQ"),
]

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    session = requests.Session()
    retry   = Retry(
        total             = MAX_RETRIES,
        backoff_factor    = BACKOFF_FACTOR,
        # 500 intentionally excluded: ERDDAP server errors are persistent (wrong ID,
        # server outage).  Retrying a 500 just wastes minutes per source.
        # Retry only on transient gateway/rate-limit errors.
        status_forcelist  = [429, 502, 503, 504],
        allowed_methods   = ["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def _now_utc() -> str:
    return (datetime.datetime.now(datetime.timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"))

def _sha256(path: pathlib.Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()

def _parse_float(val: str) -> "float | None":
    try:
        f = float(val)
        return None if f != f else f    # NaN → None
    except (ValueError, TypeError):
        return None

# ---------------------------------------------------------------------------
# Color classifiers
# ---------------------------------------------------------------------------
def _classify_kd490(kd490: "float | None") -> "str | None":
    if kd490 is None:
        return None
    if kd490 < KD490_BLUE_THRESHOLD:
        return "blue_water"
    if kd490 > KD490_GREEN_THRESHOLD:
        return "green_water"
    return "mixed"

def _classify_chl(chl: "float | None") -> "str | None":
    if chl is None:
        return None
    if chl < CHL_BLUE_THRESHOLD:
        return "blue_water"
    if chl > CHL_GREEN_THRESHOLD:
        return "green_water"
    return "mixed"

# ---------------------------------------------------------------------------
# ERDDAP csvp parser
# Handles any 2-variable griddap response: time, latitude, longitude, value.
# ---------------------------------------------------------------------------
def _parse_erddap_csvp(text: str, value_col: str) -> list[dict]:
    """
    Parse ERDDAP csvp response for a single value column.
    Returns list of {lat, lon, <value_col>} dicts.
    Cloud/invalid pixels → value_col = None (preserved, not dropped).
    """
    reader   = csv.reader(io.StringIO(text))
    rows_raw = list(reader)
    if len(rows_raw) < 3:
        return []

    # Strip units from headers: "chlorophyll (mg m-3)" → "chlorophyll"
    headers = [h.split(" (")[0].strip() for h in rows_raw[0]]
    idx     = {name: i for i, name in enumerate(headers)}

    missing = [c for c in ("latitude", "longitude", value_col) if c not in idx]
    if missing:
        log.warning("  CSV missing expected columns %s — found: %s", missing, headers)
        return []

    rows = []
    for raw in rows_raw[2:]:
        if len(raw) < len(headers):
            continue
        lat = _parse_float(raw[idx["latitude"]])
        lon = _parse_float(raw[idx["longitude"]])
        if lat is None or lon is None:
            continue
        val_str = raw[idx[value_col]]
        val     = _parse_float(val_str) if val_str not in ("", "NaN") else None
        rows.append({"lat": lat, "lon": lon, value_col: val})
    return rows

# ---------------------------------------------------------------------------
# ERDDAP URL builder
# ---------------------------------------------------------------------------
def _build_url(base_url: str, variable: str,
               date: datetime.date, time_str: str = "12:00:00Z") -> str:
    ts        = f"{date.isoformat()}T{time_str}"
    time_part = f"[({ts}):1:({ts})]"
    lat_part  = f"[({LAT_MIN}):{STRIDE}:({LAT_MAX})]"
    lon_part  = f"[({LON_MIN}):{STRIDE}:({LON_MAX})]"
    return f"{base_url}?{variable}{time_part}{lat_part}{lon_part}"

# ---------------------------------------------------------------------------
# Generic day fetcher — tries all sources for a given date
# ---------------------------------------------------------------------------
def _fetch_day(session: requests.Session, sources: list[tuple],
               date: datetime.date,
               time_str: str = "12:00:00Z") -> "tuple | None":
    """
    Try each (base_url, variable, label) source for `date`.
    Returns (rows, source_label, base_url, variable) on first success, or None.
    """
    for base_url, variable, label in sources:
        url  = _build_url(base_url, variable, date, time_str)
        host = base_url.split("/")[2]
        log.info("    [%s] %s @ %s", label, date.isoformat(), host)
        try:
            r = session.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            rows = _parse_erddap_csvp(r.text, variable)
            if rows:
                ocean = sum(1 for row in rows if row[variable] is not None)
                log.info("    ✓ %d rows, %d ocean cells", len(rows), ocean)
                return rows, label, base_url, variable
            log.warning("    ✗ Empty response from %s", label)
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "?"
            log.warning("    ✗ HTTP %s — %s", code, label)
        except requests.RequestException as exc:
            log.warning("    ✗ Request error — %s: %s", label, exc)
    return None

# ---------------------------------------------------------------------------
# CMEMS fetch — Sentinel-3 OLCI NetCDF subset via copernicusmarine client
# ---------------------------------------------------------------------------
def _fetch_cmems_subset(dataset_id: str, variable: str,
                        date: datetime.date) -> "list[dict] | None":
    """
    Download one day of data from CMEMS via the copernicusmarine Python client.

    Returns a list of {lat, lon, <variable>} dicts (same shape as ERDDAP rows),
    or None on any failure.  The variable name matches what the NetCDF file
    contains (e.g. 'CHL', 'KD490') — callers normalise it downstream.

    Soft dependencies: copernicusmarine, xarray.  If either is missing the
    function logs a warning and returns None so the ERDDAP fallback is used.
    """
    if not CMEMS_ENABLED:
        return None

    try:
        import copernicusmarine          # pip install copernicusmarine
    except ImportError:
        log.warning("  copernicusmarine not installed — skipping CMEMS source.")
        return None

    try:
        import xarray as xr              # pip install xarray
    except ImportError:
        log.warning("  xarray not installed — skipping CMEMS source.")
        return None

    log.info("    [CMEMS Sentinel-3 OLCI 300m] %s  var=%s", date.isoformat(), variable)

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = pathlib.Path(tmpdir) / "cmems_subset.nc"

            copernicusmarine.subset(
                dataset_id           = dataset_id,
                variables            = [variable],
                minimum_longitude    = LON_MIN,
                maximum_longitude    = LON_MAX,
                minimum_latitude     = LAT_MIN,
                maximum_latitude     = LAT_MAX,
                start_datetime       = f"{date.isoformat()}T00:00:00",
                end_datetime         = f"{date.isoformat()}T23:59:59",
                output_filename      = str(out_path),
                username             = CMEMS_USERNAME,
                password             = CMEMS_PASSWORD,
                overwrite            = True,
                disable_progress_bar = True,
            )

            if not out_path.exists() or out_path.stat().st_size == 0:
                log.warning("    CMEMS: empty output for %s", date.isoformat())
                return None

            ds = xr.open_dataset(str(out_path))

            # Sentinel-3 OLCI NetCDF uses 'latitude'/'longitude' coordinate names.
            # Some products use 'lat'/'lon' — fall back gracefully.
            lat_key = "latitude" if "latitude" in ds.coords else "lat"
            lon_key = "longitude" if "longitude" in ds.coords else "lon"
            lats = ds[lat_key].values
            lons = ds[lon_key].values

            # Shape may be (time, lat, lon) or (lat, lon); squeeze time dim.
            vals = ds[variable].values
            if vals.ndim == 3:
                vals = vals[0]

            rows = []
            s = CMEMS_STRIDE
            for i in range(0, len(lats), s):
                for j in range(0, len(lons), s):
                    v   = vals[i, j]
                    val = None if (v != v) else float(v)   # NaN → None
                    rows.append({
                        "lat": round(float(lats[i]), 5),
                        "lon": round(float(lons[j]), 5),
                        variable: val,
                    })

            ds.close()

            ocean = sum(1 for r in rows if r[variable] is not None)
            if ocean == 0:
                log.warning("    CMEMS: all cells cloud/null for %s", date.isoformat())
                return None

            log.info("    ✓ CMEMS %d rows, %d ocean cells (stride=%d, ~%.0fm res)",
                     len(rows), ocean, s, 300 * s)
            return rows

    except Exception as exc:
        log.warning("    ✗ CMEMS error (%s): %s", dataset_id, exc)
        return None


# ===========================================================================
# CHLOROPHYLL PIPELINE
# ===========================================================================

def _build_chl_payload(rows: list[dict], variable: str, date_key: str,
                       date_val: str, source_label: str, base_url: str,
                       product: str, cloud_note: str) -> dict:
    """Normalise rows, classify color, and build the JSON payload dict."""
    for r in rows:
        chl = r.get(variable)
        r["color_class"] = _classify_chl(chl)
        # Normalise key → always "chlorophyll"
        if variable != "chlorophyll":
            r["chlorophyll"] = r.pop(variable)
        if r["chlorophyll"] is not None:
            r["chlorophyll"] = round(r["chlorophyll"], 4)

    ocean = sum(1 for r in rows if r["chlorophyll"] is not None)
    cloud = len(rows) - ocean

    return {
        date_key:        date_val,
        "generated_utc": _now_utc(),
        "dataset":       source_label,
        "source_url":    base_url,
        "product":       product,
        "region": {
            "lat_min": LAT_MIN, "lat_max": LAT_MAX,
            "lon_min": LON_MIN, "lon_max": LON_MAX,
            "stride":  STRIDE,
        },
        "units": {
            "chlorophyll": "mg_m3",
            "color_class": "categorical: blue_water | mixed | green_water",
        },
        "thresholds": {
            "blue_water_max_chl_mg_m3":  CHL_BLUE_THRESHOLD,
            "green_water_min_chl_mg_m3": CHL_GREEN_THRESHOLD,
        },
        "cloud_note":   cloud_note,
        "row_count":    len(rows),
        "ocean_count":  ocean,
        "cloud_count":  cloud,
        "coverage_pct": round(ocean / len(rows) * 100, 1) if rows else 0.0,
        "rows":         rows,
    }


def _save_json(payload: dict, dest: pathlib.Path) -> None:
    tmp = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    tmp.rename(dest)


def fetch_chl_daily(session: requests.Session, date: datetime.date,
                    output_dir: pathlib.Path) -> "dict | None":
    """Fetch one day of chlorophyll-a. Returns manifest entry dict or None."""
    log.info("  CHL daily  %s", date.isoformat())

    # ── 1. Try CMEMS Sentinel-3 OLCI 300m (primary — highest resolution) ──
    cmems_rows = _fetch_cmems_subset(CMEMS_CHL_DATASET_ID, "CHL", date)
    if cmems_rows is not None:
        rows, label, base_url, variable = (
            cmems_rows,
            "CMEMS_Sentinel3_OLCI",
            "https://marine.copernicus.eu",
            "CHL",
        )
    else:
        # ── 2. Fall back to ERDDAP sources ────────────────────────────────
        result = _fetch_day(session, CHL_DAILY_SOURCES, date)
        if result is None:
            log.warning("  No CHL daily data for %s.", date.isoformat())
            return None
        rows, label, base_url, variable = result
    payload = _build_chl_payload(
        rows, variable,
        date_key    = "date",
        date_val    = date.isoformat(),
        source_label= label,
        base_url    = base_url,
        product     = "chlorophyll_a_daily_L3_4km",
        cloud_note  = (
            "chlorophyll=null means cloud-covered or sun-glint pixel. "
            "No gap fill applied to daily product. Use 8-day composite for gap-filled view."
        ),
    )

    date_str      = date.strftime("%Y%m%d")
    json_filename = f"CHL_{date_str}.json"
    dest          = output_dir / json_filename
    _save_json(payload, dest)
    log.info("  Saved %s  (%.0f%% coverage, %.1f KB)",
             json_filename, payload["coverage_pct"], dest.stat().st_size / 1024)

    return {
        "filename":     json_filename,
        "date":         date.isoformat(),
        "source":       label,
        "sha256":       _sha256(dest),
        "row_count":    payload["row_count"],
        "ocean_count":  payload["ocean_count"],
        "cloud_count":  payload["cloud_count"],
        "coverage_pct": payload["coverage_pct"],
    }


def fetch_chl_8day(session: requests.Session, date: datetime.date,
                   output_dir: pathlib.Path) -> "dict | None":
    """
    Fetch the 8-day chlorophyll composite that covers `date`.
    ERDDAP snaps the time constraint to the nearest valid composite period.
    Tries the target date first; falls back one 8-day step (~8 days prior)
    if the latest composite is not yet published.
    """
    log.info("  CHL 8-day composite near %s", date.isoformat())

    # Try target date, then one period back (8 days), then two periods back
    result = None
    actual_date = date
    for offset in [0, 8, 16]:
        try_date = date - datetime.timedelta(days=offset)
        result   = _fetch_day(session, CHL_8DAY_SOURCES, try_date)
        if result is not None:
            actual_date = try_date
            break

    if result is None:
        log.warning("  No CHL 8-day data near %s.", date.isoformat())
        return None

    rows, label, base_url, variable = result
    payload = _build_chl_payload(
        rows, variable,
        date_key    = "composite_center_date",
        date_val    = actual_date.isoformat(),
        source_label= label,
        base_url    = base_url,
        product     = "chlorophyll_a_8day_composite_L3_4km",
        cloud_note  = (
            "8-day composite significantly reduces cloud gaps. "
            "Residual nulls indicate persistent cloud cover across the full 8-day window."
        ),
    )
    payload["requested_date"] = date.isoformat()

    date_str      = date.strftime("%Y%m%d")
    json_filename = f"CHL_8day_{date_str}.json"
    dest          = output_dir / json_filename
    _save_json(payload, dest)
    log.info("  Saved %s  (%.0f%% coverage, %.1f KB)",
             json_filename, payload["coverage_pct"], dest.stat().st_size / 1024)

    return {
        "filename":     json_filename,
        "date":         date.isoformat(),
        "source":       label,
        "sha256":       _sha256(dest),
        "row_count":    payload["row_count"],
        "ocean_count":  payload["ocean_count"],
        "coverage_pct": payload["coverage_pct"],
    }


def _purge_chl(output_dir: pathlib.Path, cutoff: datetime.date) -> None:
    for pattern in ("CHL_????????.json", "CHL_8day_????????.json"):
        for f in sorted(output_dir.glob(pattern)):
            date_str = f.stem.split("_")[-1]
            try:
                file_date = datetime.date(
                    int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            except ValueError:
                continue
            if file_date < cutoff:
                log.info("Purging %s", f.name)
                f.unlink()


def _write_chl_latest(output_dir: pathlib.Path, newest_date: datetime.date) -> None:
    date_str = newest_date.strftime("%Y%m%d")
    for src_name, dst_name in [
        (f"CHL_{date_str}.json",      "chl_latest.json"),
        (f"CHL_8day_{date_str}.json", "chl_8day_latest.json"),
    ]:
        src = output_dir / src_name
        dst = output_dir / dst_name
        if src.exists():
            dst.write_bytes(src.read_bytes())
            log.info("  %s → %s", src_name, dst_name)


def _write_chl_manifest(output_dir: pathlib.Path, fetched: list) -> None:
    files = []
    for f in sorted(output_dir.glob("CHL_????????.json")):
        date_str = f.stem.split("_")[-1]
        iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        meta     = next((x for x in fetched if x.get("filename") == f.name), None)
        files.append({
            "filename":     f.name,
            "date":         iso_date,
            "size_bytes":   f.stat().st_size,
            "sha256":       meta["sha256"] if meta else _sha256(f),
            "row_count":    meta.get("row_count") if meta else None,
            "ocean_count":  meta.get("ocean_count") if meta else None,
            "coverage_pct": meta.get("coverage_pct") if meta else None,
            "source":       meta.get("source") if meta else None,
        })

    manifest = {
        "generated_utc":  _now_utc(),
        "product":        "chlorophyll_a_daily_L3_4km",
        "retention_days": RETENTION_DAYS,
        "region": {
            "lat_min": LAT_MIN, "lat_max": LAT_MAX,
            "lon_min": LON_MIN, "lon_max": LON_MAX,
        },
        "units":     {"chlorophyll": "mg_m3"},
        "thresholds": {
            "blue_water_max_mg_m3":  CHL_BLUE_THRESHOLD,
            "green_water_min_mg_m3": CHL_GREEN_THRESHOLD,
        },
        "file_count": len(files),
        "files":      files,
    }
    dest = output_dir / "chl_manifest.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    log.info("CHL manifest written: %d file(s)", len(files))

# ===========================================================================
# SEA COLOR (Kd490) PIPELINE
# ===========================================================================

def fetch_seacolor_day(session: requests.Session, date: datetime.date,
                       output_dir: pathlib.Path) -> "dict | None":
    """
    Fetch one day of Kd490 sea color / water clarity data.
    Normalises variable key to 'kd490' regardless of source (k490 or kd490).
    Returns manifest entry dict or None.
    """
    log.info("  SEACOLOR (Kd490)  %s", date.isoformat())

    # ── 1. Try CMEMS Sentinel-3 OLCI 300m (primary) ───────────────────────
    cmems_rows = _fetch_cmems_subset(CMEMS_KD490_DATASET_ID, "KD490", date)
    if cmems_rows is not None:
        rows, label, base_url, variable = (
            cmems_rows,
            "CMEMS_Sentinel3_OLCI",
            "https://marine.copernicus.eu",
            "KD490",
        )
    else:
        # ── 2. Fall back to ERDDAP sources ────────────────────────────────
        result = _fetch_day(session, KD490_DAILY_SOURCES, date)
        if result is None:
            log.warning("  No SEACOLOR data for %s.", date.isoformat())
            return None
        rows, label, base_url, variable = result

    # Classify and normalise key name (k490 or kd490 → kd490)
    for r in rows:
        raw_val = r.get(variable)
        r["color_class"] = _classify_kd490(raw_val)
        if variable != "kd490":
            r["kd490"] = r.pop(variable)
        if r["kd490"] is not None:
            r["kd490"] = round(r["kd490"], 5)

    ocean  = sum(1 for r in rows if r["kd490"] is not None)
    cloud  = len(rows) - ocean
    blue   = sum(1 for r in rows if r["color_class"] == "blue_water")
    mixed  = sum(1 for r in rows if r["color_class"] == "mixed")
    green  = sum(1 for r in rows if r["color_class"] == "green_water")

    date_str      = date.strftime("%Y%m%d")
    json_filename = f"SEACOLOR_{date_str}.json"
    dest          = output_dir / json_filename

    payload = {
        "date":          date.isoformat(),
        "generated_utc": _now_utc(),
        "dataset":       label,
        "source_url":    base_url,
        "product":       "kd490_daily_L3_4km",
        "metric":        "Kd490 — diffuse attenuation coefficient at 490 nm",
        "fishing_note": (
            "The blue_water / green_water boundary identified by Kd490 is a primary "
            "search cue for offshore pelagic species. Blue water (Gulf Stream) vs. "
            "green water (shelf) edges align with thermal breaks and bathymetric features."
        ),
        "region": {
            "lat_min": LAT_MIN, "lat_max": LAT_MAX,
            "lon_min": LON_MIN, "lon_max": LON_MAX,
            "stride":  STRIDE,
        },
        "units": {
            "kd490":      "m-1 (inverse metres)",
            "color_class": "categorical: blue_water | mixed | green_water",
        },
        "thresholds": {
            "blue_water":  f"kd490 < {KD490_BLUE_THRESHOLD} m-1",
            "mixed":       f"{KD490_BLUE_THRESHOLD} <= kd490 <= {KD490_GREEN_THRESHOLD} m-1",
            "green_water": f"kd490 > {KD490_GREEN_THRESHOLD} m-1",
        },
        "cloud_note": (
            "kd490=null means cloud-covered, sun-glint, or invalid pixel. "
            "No gap fill applied. Null cells are excluded from color boundary rendering."
        ),
        "row_count":    len(rows),
        "ocean_count":  ocean,
        "cloud_count":  cloud,
        "coverage_pct": round(ocean / len(rows) * 100, 1) if rows else 0.0,
        "color_distribution": {
            "blue_water":  blue,
            "mixed":       mixed,
            "green_water": green,
            "cloud_null":  cloud,
        },
        "rows": rows,
    }

    _save_json(payload, dest)
    log.info("  Saved %s  (%.0f%% coverage — B:%d M:%d G:%d, %.1f KB)",
             json_filename, payload["coverage_pct"],
             blue, mixed, green, dest.stat().st_size / 1024)

    return {
        "filename":           json_filename,
        "date":               date.isoformat(),
        "source":             label,
        "sha256":             _sha256(dest),
        "row_count":          payload["row_count"],
        "ocean_count":        ocean,
        "cloud_count":        cloud,
        "coverage_pct":       payload["coverage_pct"],
        "color_distribution": payload["color_distribution"],
    }


def _purge_seacolor(output_dir: pathlib.Path, cutoff: datetime.date) -> None:
    for f in sorted(output_dir.glob("SEACOLOR_????????.json")):
        date_str = f.stem.split("_")[-1]
        try:
            file_date = datetime.date(
                int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        except ValueError:
            continue
        if file_date < cutoff:
            log.info("Purging %s", f.name)
            f.unlink()


def _write_seacolor_latest(output_dir: pathlib.Path, newest_date: datetime.date) -> None:
    date_str = newest_date.strftime("%Y%m%d")
    src = output_dir / f"SEACOLOR_{date_str}.json"
    dst = output_dir / "seacolor_latest.json"
    if src.exists():
        dst.write_bytes(src.read_bytes())
        log.info("  seacolor_latest.json → %s", src.name)


def _write_seacolor_manifest(output_dir: pathlib.Path, fetched: list) -> None:
    files = []
    for f in sorted(output_dir.glob("SEACOLOR_????????.json")):
        date_str = f.stem.split("_")[-1]
        iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        meta     = next((x for x in fetched if x.get("filename") == f.name), None)

        # Load color_distribution from disk if not in fetched list
        color_dist = meta.get("color_distribution") if meta else None
        if color_dist is None and f.exists():
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    color_dist = json.load(fh).get("color_distribution")
            except Exception:
                pass

        files.append({
            "filename":           f.name,
            "date":               iso_date,
            "size_bytes":         f.stat().st_size,
            "sha256":             meta["sha256"] if meta else _sha256(f),
            "row_count":          meta.get("row_count") if meta else None,
            "ocean_count":        meta.get("ocean_count") if meta else None,
            "coverage_pct":       meta.get("coverage_pct") if meta else None,
            "source":             meta.get("source") if meta else None,
            "color_distribution": color_dist,
        })

    manifest = {
        "generated_utc":  _now_utc(),
        "product":        "sea_color_kd490_daily_L3_4km",
        "metric":         "Kd490 (diffuse attenuation at 490 nm) — water clarity / color proxy",
        "retention_days": RETENTION_DAYS,
        "region": {
            "lat_min": LAT_MIN, "lat_max": LAT_MAX,
            "lon_min": LON_MIN, "lon_max": LON_MAX,
        },
        "units": {"kd490": "m-1"},
        "thresholds": {
            "blue_water":  f"< {KD490_BLUE_THRESHOLD} m-1",
            "mixed":       f"{KD490_BLUE_THRESHOLD}–{KD490_GREEN_THRESHOLD} m-1",
            "green_water": f"> {KD490_GREEN_THRESHOLD} m-1",
        },
        "file_count": len(files),
        "files":      files,
    }
    dest = output_dir / "seacolor_manifest.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    log.info("SEACOLOR manifest written: %d file(s)", len(files))

# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    today_utc   = datetime.datetime.now(datetime.timezone.utc).date()
    cutoff_date = today_utc - datetime.timedelta(days=RETENTION_DAYS)

    log.info("=== Chlorophyll + Sea Color Daily Retrieval ===")
    log.info("Today (UTC): %s  |  Retention cutoff: %s", today_utc, cutoff_date)

    CHL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SEACOLOR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    session = _make_session()

    # Candidate dates — search backwards from yesterday
    candidate_dates = [
        today_utc - datetime.timedelta(days=d) for d in range(1, SEARCH_WINDOW + 1)
    ]

    # ── Chlorophyll daily ────────────────────────────────────────────────────
    log.info("--- Chlorophyll-a daily ---")
    _purge_chl(CHL_OUTPUT_DIR, cutoff_date)

    chl_fetched: list[dict] = []
    chl_dates:   list[datetime.date] = []

    for d in candidate_dates:
        if len(chl_dates) == RETENTION_DAYS:
            break
        meta = fetch_chl_daily(session, d, CHL_OUTPUT_DIR)
        if meta:
            chl_fetched.append(meta)
            chl_dates.append(d)
        time.sleep(0.5)

    if not chl_dates:
        log.error("CHL: No data found within %d-day search window.", SEARCH_WINDOW)
    else:
        _write_chl_latest(CHL_OUTPUT_DIR, max(chl_dates))

    # ── Chlorophyll 8-day composite ──────────────────────────────────────────
    log.info("--- Chlorophyll-a 8-day composite ---")
    if chl_dates:
        meta_8day = fetch_chl_8day(session, max(chl_dates), CHL_OUTPUT_DIR)
        if meta_8day:
            chl_fetched.append(meta_8day)

    _write_chl_manifest(CHL_OUTPUT_DIR, chl_fetched)

    # ── Sea Color (Kd490) ────────────────────────────────────────────────────
    log.info("--- Sea Color (Kd490) ---")
    _purge_seacolor(SEACOLOR_OUTPUT_DIR, cutoff_date)

    sc_fetched: list[dict] = []
    sc_dates:   list[datetime.date] = []

    for d in candidate_dates:
        if len(sc_dates) == RETENTION_DAYS:
            break
        meta = fetch_seacolor_day(session, d, SEACOLOR_OUTPUT_DIR)
        if meta:
            sc_fetched.append(meta)
            sc_dates.append(d)
        time.sleep(0.5)

    if not sc_dates:
        log.error("SEACOLOR: No data found within %d-day search window.", SEARCH_WINDOW)
    else:
        _write_seacolor_latest(SEACOLOR_OUTPUT_DIR, max(sc_dates))

    _write_seacolor_manifest(SEACOLOR_OUTPUT_DIR, sc_fetched)

    # ── Summary ──────────────────────────────────────────────────────────────
    chl_daily_count = len([f for f in chl_fetched if "8day" not in f["filename"]])
    log.info("=== Done. CHL daily %d/%d | SEACOLOR %d/%d day(s) retrieved. ===",
             chl_daily_count, RETENTION_DAYS,
             len(sc_fetched), RETENTION_DAYS)


if __name__ == "__main__":
    main()
