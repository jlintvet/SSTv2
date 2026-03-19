"""
DailySSTRetrieval.py
====================
Retrieves the last three days of SST data from two satellite sources:

  1. MUR SST (jplMURSST41) — NASA JPL 1-km daily blended analysis.
     Gap-filled (no cloud holes). ~2 day publication lag.

  2. GOES-19 ABI SST (goes19SSThourly) — NOAA/AOML hourly geostationary
     SST. IR only — cloud pixels are null. ~3-6 hour lag.

Output layout (relative to this script's directory):
  DailySST/
    MUR_SST_YYYYMMDD.json    – MUR full grid data for each day
    latest.json              – MUR most recent day (convenience alias)
    manifest.json            – MUR catalogue of all stored files

    GOES19_SST_YYYYMMDD.json – GOES-19 full grid data for each day
    goes19_latest.json       – GOES-19 most recent day (convenience alias)
    goes19_manifest.json     – GOES-19 catalogue of all stored files

Behaviour
---------
* Purges JSON files older than RETENTION_DAYS on every run.
* Downloads the 3 most-recent available days for each dataset.
* Writes latest alias and manifest for each dataset.

ERDDAP endpoint
---------------
  Uses the .csvp format — no binary parsing library required.

Dependencies
------------
  pip install requests
"""

import csv
import io
import json
import hashlib
import logging
import datetime
import pathlib
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ERDDAP_BASE = "https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41.csvp"

# GOES-19 Hourly SST (goes19SSThourly, cwcgom.aoml.noaa.gov)
# GOES-19 replaced GOES-16 as the operational East Coast geostationary
# satellite in late 2024. IR only — cloud pixels are NaN. ~3-6 hr lag.
ERDDAP_GOES19    = "https://cwcgom.aoml.noaa.gov/erddap/griddap/goes19SSThourly.csvp"
GOES19_VARIABLE  = "sst"  # Celsius; NaN where cloud-obscured
GOES19_STRIDE    = 1      # native resolution

# Region: southern New Jersey (N) → Myrtle Beach SC (S),
#         Myrtle Beach SC (W)     → ~200 miles offshore Virginia Beach (E)
LAT_MIN = 33.70
LAT_MAX = 39.00
LON_MIN = -78.89
LON_MAX = -72.21

# Stride — 10 = ~10 km resolution, keeps JSON files to a manageable size.
# Set to 1 for full 1-km resolution (large files, slow download).
LAT_STRIDE = 1
LON_STRIDE = 1

# Variables to retrieve
VARIABLES = ["analysed_sst", "analysis_error", "sea_ice_fraction", "mask"]

# Output directory (relative to this file)
OUTPUT_DIR = pathlib.Path(__file__).resolve().parent / "DailySST"

# Days to retain
RETENTION_DAYS = 3

# How many days back to search for available data
SEARCH_WINDOW = 7

# HTTP settings
TIMEOUT_SECONDS = 300
MAX_RETRIES = 3
BACKOFF_FACTOR = 2

# MUR SST daily timestamp
DAILY_HOUR = "09:00:00Z"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _build_url(date: datetime.date) -> str:
    """Construct the ERDDAP csvp URL for a single day and region."""
    ts = f"{date.isoformat()}T{DAILY_HOUR}"
    time_part = f"[({ts}):1:({ts})]"
    lat_part  = f"[({LAT_MIN}):{LAT_STRIDE}:({LAT_MAX})]"
    lon_part  = f"[({LON_MIN}):{LON_STRIDE}:({LON_MAX})]"
    var_queries = ",".join(
        f"{v}{time_part}{lat_part}{lon_part}" for v in VARIABLES
    )
    return f"{ERDDAP_BASE}?{var_queries}"


def _check_availability(session: requests.Session, date: datetime.date) -> bool:
    """
    Check whether MUR SST data for date exists on the server.
    Tries HEAD first; falls back to a small GET request if HEAD fails or
    returns a non-200 status, since some ERDDAP servers respond poorly to HEAD.
    """
    nc_url  = _build_url(date).replace(".csvp", ".nc")
    # HEAD check
    try:
        r = session.head(nc_url, timeout=15)
        if r.status_code == 200:
            return True
        if r.status_code == 404:
            return False
        # Any other status (405 Method Not Allowed, 5xx, etc.) — fall through to GET
        log.debug("  HEAD returned %d for %s — trying GET probe", r.status_code, date)
    except requests.RequestException:
        log.debug("  HEAD failed for %s — trying GET probe", date)

    # GET fallback: request just 1 row via csvp (fast, small response)
    probe_url = (
        f"https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41.csvp"
        f"?analysed_sst"
        f"[({date.isoformat()}T{DAILY_HOUR}):1:({date.isoformat()}T{DAILY_HOUR})]"
        f"[({LAT_MIN}):1:({LAT_MIN})]"
        f"[({LON_MIN}):1:({LON_MIN})]"
    )
    try:
        r = session.get(probe_url, timeout=20)
        return r.status_code == 200 and len(r.text.strip()) > 0
    except requests.RequestException:
        return False


def _fahrenheit(val: str) -> float | None:
    """
    Convert an ERDDAP csvp SST value to Fahrenheit.
    ERDDAP unpacks the NetCDF scale/offset automatically, so csvp delivers
    analysed_sst already in degrees Celsius (valid ocean range: ~-2 to 36 C).
    Fill/land pixels arrive as -327.67 (raw int16 fill scaled to float);
    we reject anything outside the physically plausible ocean range.
    """
    try:
        c = float(val)
        if c != c:               # NaN
            return None
        if c < -3.0 or c > 40.0:  # fill value or non-ocean pixel
            return None
        return round(c * 9/5 + 32, 4)   # Celsius -> Fahrenheit
    except (ValueError, TypeError):
        return None


def _float(val: str) -> float | None:
    try:
        f = float(val)
        return None if f != f else round(f, 6)
    except (ValueError, TypeError):
        return None


def _parse_csv(text: str) -> list[dict]:
    """
    Parse ERDDAP csvp response into a list of row dicts.

    csvp format:
      Row 0: column names  e.g. time (UTC), latitude (degrees_north), ...
      Row 1: units         e.g. UTC, degrees_north, degrees_east, degree_C, ...
      Row 2+: data
    """
    reader = csv.reader(io.StringIO(text))
    rows_raw = list(reader)

    if len(rows_raw) < 3:
        return []

    # Row 0 = headers, Row 1 = units (skip both for data)
    headers = [h.split(" (")[0].strip() for h in rows_raw[0]]

    # Build index map
    idx = {name: i for i, name in enumerate(headers)}

    rows = []
    for raw in rows_raw[2:]:
        if len(raw) < len(headers):
            continue
        try:
            lat = _float(raw[idx["latitude"]])
            lon = _float(raw[idx["longitude"]])
        except KeyError:
            continue
        if lat is None or lon is None:
            continue

        # analysed_sst is delivered in Celsius by ERDDAP (scale/offset already applied)
        sst_raw = raw[idx.get("analysed_sst", -1)] if "analysed_sst" in idx else None
        sst = _fahrenheit(sst_raw) if sst_raw is not None else None

        row = {
            "lat":   lat,
            "lon":   lon,
            "sst":   sst,
            # analysis_error is a delta in Celsius; multiply by 1.8 for Fahrenheit delta
            "error": round(float(raw[idx["analysis_error"]]) * 1.8, 4)
                     if "analysis_error" in idx and raw[idx["analysis_error"]] not in ("", "NaN")
                     else None,
            "ice":   _float(raw[idx["sea_ice_fraction"]]) if "sea_ice_fraction" in idx else None,
            "mask":  _float(raw[idx["mask"]]) if "mask" in idx else None,
        }
        rows.append(row)

    return rows


def _actual_extent(rows: list[dict]) -> dict:
    """Return the min/max lat/lon actually present in the parsed rows."""
    if not rows:
        return {}
    lats = [r["lat"] for r in rows]
    lons = [r["lon"] for r in rows]
    return {
        "lat_min": min(lats), "lat_max": max(lats),
        "lon_min": min(lons), "lon_max": max(lons),
    }


def _fetch_day_json(session: requests.Session,
                    date: datetime.date,
                    dest: pathlib.Path) -> bool:
    """Download CSV from ERDDAP, convert to JSON, write to dest."""
    url = _build_url(date)
    log.info("Downloading  %s  ->  %s", date.isoformat(), dest.name)

    try:
        r = session.get(url, timeout=TIMEOUT_SECONDS)
        r.raise_for_status()
    except requests.HTTPError as exc:
        log.warning("  HTTP error for %s: %s", date.isoformat(), exc)
        return False
    except requests.RequestException as exc:
        log.warning("  Request error for %s: %s", date.isoformat(), exc)
        return False

    rows = _parse_csv(r.text)
    if not rows:
        log.warning("  No rows parsed for %s — skipping.", date.isoformat())
        return False

    extent = _actual_extent(rows)
    payload = {
        "date":          date.isoformat(),
        "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z"),
        "dataset":       "jplMURSST41",
        "source":        "https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41",
        "region": {
            "lat_min": LAT_MIN,
            "lat_max": LAT_MAX,
            "lon_min": LON_MIN,
            "lon_max": LON_MAX,
            "stride":  LAT_STRIDE,
        },
        "actual_extent": extent,
        "units": {
            "sst":   "fahrenheit",
            "error": "fahrenheit",
            "ice":   "fraction_0_to_1",
            "mask":  "categorical",
            
        },
        "row_count": len(rows),
        "rows": rows,
    }

    tmp = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))  # compact — smaller file
    tmp.rename(dest)

    log.info("  Saved %s  (%d rows, %.1f KB)",
             dest.name, len(rows), dest.stat().st_size / 1024)
    return True


def _sha256(path: pathlib.Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _purge_old_files(output_dir: pathlib.Path, cutoff: datetime.date) -> list:
    """Delete MUR_SST_YYYYMMDD.json files older than cutoff."""
    deleted = []
    for f in sorted(output_dir.glob("MUR_SST_????????.json")):
        date_str = f.stem.split("_")[-1]
        try:
            file_date = datetime.date(
                int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
            )
        except ValueError:
            continue
        if file_date < cutoff:
            log.info("Purging old file: %s", f.name)
            f.unlink()
            deleted.append(f.name)
    return deleted


def _write_latest(output_dir: pathlib.Path, newest_date: datetime.date) -> None:
    """Write latest.json as a copy of the newest day's data file."""
    src = output_dir / f"MUR_SST_{newest_date.strftime('%Y%m%d')}.json"
    dst = output_dir / "latest.json"
    if src.exists():
        dst.write_bytes(src.read_bytes())
        log.info("latest.json updated  ->  %s", src.name)


def _write_manifest(output_dir: pathlib.Path, fetched: list[dict]) -> None:
    """Write manifest.json cataloguing all JSON files present."""
    files_on_disk = []
    for f in sorted(output_dir.glob("MUR_SST_????????.json")):
        date_str = f.stem.split("_")[-1]
        iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        entry = next((x for x in fetched if x.get("filename") == f.name), None)
        files_on_disk.append({
            "filename":   f.name,
            "date":       iso_date,
            "size_bytes": f.stat().st_size,
            "sha256":     entry["sha256"] if entry else _sha256(f),
            "row_count":  entry["row_count"] if entry else None,
        })

    manifest = {
        "generated_utc":  datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z"),
        "dataset":        "jplMURSST41",
        "source":         "https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41",
        "retention_days": RETENTION_DAYS,
        "region": {
            "lat_min": LAT_MIN, "lat_max": LAT_MAX,
            "lon_min": LON_MIN, "lon_max": LON_MAX,
            "stride":  LAT_STRIDE,
        },
        "units": {
            "sst":   "fahrenheit",
            "error": "fahrenheit",
            "ice":   "fraction_0_to_1",
            
        },
        "file_count": len(files_on_disk),
        "files":      files_on_disk,
    }

    manifest_path = output_dir / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    log.info("Manifest written: %d file(s)", len(files_on_disk))


# ---------------------------------------------------------------------------
# GOES-19 Hourly SST pipeline (cwcgom.aoml.noaa.gov)
# ---------------------------------------------------------------------------

def _build_goes19_url(date: datetime.date, hour: int) -> str:
    """Construct the ERDDAP csvp URL for a single GOES-19 hourly SST snapshot."""
    ts        = f"{date.isoformat()}T{hour:02d}:00:00Z"
    time_part = f"[({ts}):1:({ts})]"
    lat_part  = f"[({LAT_MIN}):{GOES19_STRIDE}:({LAT_MAX})]"
    lon_part  = f"[({LON_MIN}):{GOES19_STRIDE}:({LON_MAX})]"
    return f"{ERDDAP_GOES19}?{GOES19_VARIABLE}{time_part}{lat_part}{lon_part}"


def _find_latest_goes19_hour(session: requests.Session,
                              date: datetime.date) -> "int | None":
    """
    Probe GOES-19 hourly SST for the latest available hour on a given date.
    Walks backwards 23 → 0 and returns the first hour that gets HTTP 200.
    Short 10-second timeout per probe so unavailable dates fail quickly.
    """
    for hour in range(23, -1, -1):
        url = _build_goes19_url(date, hour).replace(".csvp", ".nc")
        try:
            r = session.head(url, timeout=10)
            if r.status_code == 200:
                log.info("    Latest available hour: %02d:00Z", hour)
                return hour
        except requests.RequestException:
            continue
    return None


def _parse_goes19_csv(text: str) -> list[dict]:
    """
    Parse ERDDAP csvp response for GOES-19 SST.
    Variable: sst in degrees Celsius. NaN = cloud-covered, stored as null.
    """
    reader   = csv.reader(io.StringIO(text))
    rows_raw = list(reader)
    if len(rows_raw) < 3:
        return []

    headers = [h.split(" (")[0].strip() for h in rows_raw[0]]
    idx     = {name: i for i, name in enumerate(headers)}

    rows = []
    for raw in rows_raw[2:]:
        if len(raw) < len(headers):
            continue
        lat = _float(raw[idx.get("latitude",  -1)]) if "latitude"  in idx else None
        lon = _float(raw[idx.get("longitude", -1)]) if "longitude" in idx else None
        if lat is None or lon is None:
            continue

        sst_col = idx.get("sst")
        sst = None
        if sst_col is not None and raw[sst_col] not in ("", "NaN"):
            sst = _fahrenheit(raw[sst_col])

        rows.append({"lat": lat, "lon": lon, "sst": sst})

    return rows


def _fetch_goes19_day_json(session: requests.Session,
                           date: datetime.date,
                           hour: int,
                           dest: pathlib.Path) -> bool:
    """Download GOES-19 hourly SST from AOML ERDDAP, convert to JSON, write to dest."""
    url = _build_goes19_url(date, hour)
    log.info("GOES-19 Downloading  %s %02d:00Z  ->  %s",
             date.isoformat(), hour, dest.name)

    try:
        r = session.get(url, timeout=TIMEOUT_SECONDS)
        r.raise_for_status()
    except requests.HTTPError as exc:
        log.warning("  GOES-19 HTTP error for %s: %s", date.isoformat(), exc)
        return False
    except requests.RequestException as exc:
        log.warning("  GOES-19 Request error for %s: %s", date.isoformat(), exc)
        return False

    rows = _parse_goes19_csv(r.text)
    if not rows:
        log.warning("  GOES-19: No rows parsed for %s — skipping.", date.isoformat())
        return False

    ocean = sum(1 for r in rows if r["sst"] is not None)
    cloud = len(rows) - ocean
    log.info("  GOES-19 %s %02d:00Z: %d rows (%d ocean, %d cloud/null)",
             date.isoformat(), hour, len(rows), ocean, cloud)

    extent = _actual_extent(rows)
    payload = {
        "date":          date.isoformat(),
        "hour_utc":      hour,
        "obs_time_utc":  f"{date.isoformat()}T{hour:02d}:00:00Z",
        "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "dataset":       "goes19SSThourly",
        "source":        "https://cwcgom.aoml.noaa.gov/erddap/griddap/goes19SSThourly",
        "sensor":        "GOES-19 ABI (NOAA/AOML hourly SST)",
        "cloud_note":    "sst=null means cloud-covered — no gap fill applied",
        "region": {
            "lat_min": LAT_MIN, "lat_max": LAT_MAX,
            "lon_min": LON_MIN, "lon_max": LON_MAX,
            "stride":  GOES19_STRIDE,
        },
        "actual_extent": extent,
        "units":         {"sst": "fahrenheit"},
        "row_count":     len(rows),
        "ocean_count":   ocean,
        "cloud_count":   cloud,
        "rows":          rows,
    }

    tmp = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    tmp.rename(dest)

    log.info("  Saved %s  (%.1f KB)", dest.name, dest.stat().st_size / 1024)
    return True


def _purge_goes19_files(output_dir: pathlib.Path, cutoff: datetime.date) -> list:
    """Delete GOES19_SST_YYYYMMDD.json files older than cutoff."""
    deleted = []
    for f in sorted(output_dir.glob("GOES19_SST_????????.json")):
        date_str = f.stem.split("_")[-1]
        try:
            file_date = datetime.date(
                int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
            )
        except ValueError:
            continue
        if file_date < cutoff:
            log.info("Purging old GOES-19 file: %s", f.name)
            f.unlink()
            deleted.append(f.name)
    return deleted


def _write_goes19_latest(output_dir: pathlib.Path, newest_date: datetime.date) -> None:
    """Write goes19_latest.json as a copy of the newest GOES-19 day file."""
    src = output_dir / f"GOES19_SST_{newest_date.strftime('%Y%m%d')}.json"
    dst = output_dir / "goes19_latest.json"
    if src.exists():
        dst.write_bytes(src.read_bytes())
        log.info("goes19_latest.json updated  ->  %s", src.name)


def _write_goes19_manifest(output_dir: pathlib.Path, fetched: list[dict]) -> None:
    """
    Write goes19_manifest.json cataloguing all GOES-19 JSON files present.
    Ranks files by coverage_pct (most ocean pixels = rank 1) so the UI
    can immediately identify the clearest-sky observation.
    """
    files_on_disk = []
    for f in sorted(output_dir.glob("GOES19_SST_????????.json")):
        date_str = f.stem.split("_")[-1]
        iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        entry = next((x for x in fetched if x.get("filename") == f.name), None)

        ocean = entry.get("ocean_count") if entry else None
        total = entry.get("row_count")   if entry else None
        hour  = entry.get("hour_utc")    if entry else None

        # Fall back to reading counts from the file itself if not in fetched
        if ocean is None or total is None:
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    meta = json.load(fh)
                ocean = meta.get("ocean_count")
                total = meta.get("row_count")
                hour  = meta.get("hour_utc")
            except Exception:
                pass

        cloud    = (total - ocean) if (ocean is not None and total is not None) else None
        pct_cov  = round(ocean / total * 100, 1) if (ocean and total) else None

        files_on_disk.append({
            "filename":     f.name,
            "date":         iso_date,
            "hour_utc":     hour,
            "size_bytes":   f.stat().st_size,
            "sha256":       entry["sha256"] if entry else _sha256(f),
            "row_count":    total,
            "ocean_count":  ocean,
            "cloud_count":  cloud,
            "coverage_pct": pct_cov,
        })

    # Rank by coverage (rank 1 = most ocean pixels = least cloud obstruction)
    sortable = [f for f in files_on_disk if f["ocean_count"] is not None]
    sortable.sort(key=lambda x: x["ocean_count"], reverse=True)
    rank_map = {f["filename"]: i + 1 for i, f in enumerate(sortable)}
    for f in files_on_disk:
        f["coverage_rank"] = rank_map.get(f["filename"])

    best = sortable[0] if sortable else None
    log.info("  GOES-19 coverage ranking:")
    for f in sortable:
        log.info("    Rank %d: %s  %.1f%% ocean  (%d cloud pixels)",
                 f["coverage_rank"], f["date"],
                 f["coverage_pct"] or 0, f["cloud_count"] or 0)

    manifest = {
        "generated_utc":  datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "dataset":        "goes19SSThourly",
        "source":         "https://cwcgom.aoml.noaa.gov/erddap/griddap/goes19SSThourly",
        "sensor":         "GOES-19 ABI (NOAA/AOML hourly SST)",
        "retention_days": RETENTION_DAYS,
        "region": {
            "lat_min": LAT_MIN, "lat_max": LAT_MAX,
            "lon_min": LON_MIN, "lon_max": LON_MAX,
            "stride":  GOES19_STRIDE,
        },
        "units":            {"sst": "fahrenheit"},
        "file_count":       len(files_on_disk),
        "best_coverage":    best["filename"] if best else None,
        "files":            files_on_disk,
    }

    manifest_path = output_dir / "goes19_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    log.info("GOES-19 manifest written: %d file(s)", len(files_on_disk))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    today_utc   = datetime.datetime.now(datetime.timezone.utc).date()
    cutoff_date = today_utc - datetime.timedelta(days=RETENTION_DAYS)

    log.info("=== MUR + GOES-19 SST Daily Retrieval ===")
    log.info("Today (UTC): %s  |  Cutoff: %s", today_utc, cutoff_date)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    session = _make_session()

    candidate_dates = [
        today_utc - datetime.timedelta(days=d) for d in range(1, SEARCH_WINDOW + 1)
    ]

    # -----------------------------------------------------------------------
    # MUR SST
    # -----------------------------------------------------------------------
    log.info("--- MUR SST ---")
    deleted = _purge_old_files(OUTPUT_DIR, cutoff_date)
    log.info("Purged %d old MUR file(s).", len(deleted))

    target_dates: list[datetime.date] = []
    log.info("Probing MUR server for available dates …")
    for d in candidate_dates:
        if len(target_dates) == RETENTION_DAYS:
            break
        log.info("  Checking %s …", d.isoformat())
        if _check_availability(session, d):
            log.info("    Available ✓")
            target_dates.append(d)
        else:
            log.info("    Not yet available.")
        time.sleep(0.5)

    fetched = []
    if not target_dates:
        log.error("MUR: No available dates found within search window.")
    else:
        log.info("Fetching %d MUR day(s): %s", len(target_dates),
                 [d.isoformat() for d in target_dates])
        for date in target_dates:
            filename = f"MUR_SST_{date.strftime('%Y%m%d')}.json"
            dest     = OUTPUT_DIR / filename
            success  = _fetch_day_json(session, date, dest)
            if success:
                with open(dest, "r", encoding="utf-8") as fh:
                    meta = json.load(fh)
                fetched.append({
                    "filename":  filename,
                    "date":      date.isoformat(),
                    "sha256":    _sha256(dest),
                    "row_count": meta.get("row_count"),
                })

    if fetched:
        _write_latest(OUTPUT_DIR, max(target_dates))
    _write_manifest(OUTPUT_DIR, fetched)

    # -----------------------------------------------------------------------
    # GOES-19 hourly SST (cwcgom.aoml.noaa.gov, ~3-6 hr lag, cloud=null)
    # -----------------------------------------------------------------------
    log.info("--- GOES-19 Hourly SST ---")
    deleted_goes = _purge_goes19_files(OUTPUT_DIR, cutoff_date)
    log.info("Purged %d old GOES-19 file(s).", len(deleted_goes))

    goes_dates: list[datetime.date] = []
    goes_hours: dict[datetime.date, int] = {}
    log.info("Probing GOES-19 server for available dates …")
    for d in candidate_dates:
        if len(goes_dates) == RETENTION_DAYS:
            break
        log.info("  Checking %s …", d.isoformat())
        hour = _find_latest_goes19_hour(session, d)
        if hour is not None:
            log.info("    Available ✓  (latest hour: %02d:00Z)", hour)
            goes_dates.append(d)
            goes_hours[d] = hour
        else:
            log.info("    Not yet available.")
        time.sleep(0.5)

    goes_fetched = []
    if not goes_dates:
        log.error("GOES-19: No available dates found within search window.")
    else:
        log.info("Fetching %d GOES-19 day(s): %s", len(goes_dates),
                 [d.isoformat() for d in goes_dates])
        for date in goes_dates:
            filename = f"GOES19_SST_{date.strftime('%Y%m%d')}.json"
            dest     = OUTPUT_DIR / filename
            success  = _fetch_goes19_day_json(session, date, goes_hours[date], dest)
            if success:
                with open(dest, "r", encoding="utf-8") as fh:
                    meta = json.load(fh)
                goes_fetched.append({
                    "filename":    filename,
                    "date":        date.isoformat(),
                    "hour_utc":    goes_hours[date],
                    "sha256":      _sha256(dest),
                    "row_count":   meta.get("row_count"),
                    "ocean_count": meta.get("ocean_count"),
                    "cloud_count": meta.get("cloud_count"),
                })

    if goes_fetched:
        _write_goes19_latest(OUTPUT_DIR, max(goes_dates))
    _write_goes19_manifest(OUTPUT_DIR, goes_fetched)

    log.info("=== Done. MUR %d/%d | GOES-19 %d/%d day(s) retrieved. ===",
             len(fetched),   len(target_dates),
             len(goes_fetched), len(goes_dates))


if __name__ == "__main__":
    main()
