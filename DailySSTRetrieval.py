"""
DailySSTRetrieval.py
====================
Retrieves the last three days of MUR SST 1-km sea surface temperature data
from the NOAA CoastWatch ERDDAP server (dataset: jplMURSST41) and saves them
as JSON files consumable by web or mobile apps.

Output layout (relative to this script's directory):
  DailySST/
    MUR_SST_YYYYMMDD.json   – full grid data for each day
    latest.json             – most recent day's data (convenience alias)
    manifest.json           – catalogue of all stored files + metadata

JSON structure per day file:
  {
    "date":        "2024-03-12",
    "generated_utc": "2024-03-13T00:01:22Z",
    "dataset":     "jplMURSST41",
    "region": {
      "lat_min": 33.7, "lat_max": 39.0,
      "lon_min": -80.85, "lon_max": -72.21,
      "stride": 10
    },
    "units": { "analysed_sst": "fahrenheit", "analysis_error": "fahrenheit",
               "sea_ice_fraction": "1", "mask": "1" },
    "rows": [
      { "lat": 33.70, "lon": -80.85, "sst": 18.42,
        "error": 0.31, "ice": 0.0, "mask": 1 },
      ...
    ]
  }

Behaviour
---------
* Purges JSON files older than RETENTION_DAYS on every run.
* Downloads the 3 most-recent available days, overwriting existing files.
* Writes latest.json pointing at the newest day's data.
* Writes manifest.json cataloguing all files present after the run.

ERDDAP endpoint
---------------
  Uses the .csvp (CSV with units header) format — no binary parsing library
  required. Only the standard library + requests is needed.

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

# Spatial subset (degrees)
# Region: southern New Jersey (N) → Myrtle Beach SC (S),
#         Charlotte NC (W)        → ~200 miles offshore Virginia Beach (E)
LAT_MIN = 33.70
LAT_MAX = 39.00
LON_MIN = -80.85
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
    """HEAD-check whether data for date exists on the server."""
    # Use .nc endpoint for the HEAD check — csvp doesn't support HEAD reliably
    url = _build_url(date).replace(".csvp", ".nc")
    try:
        r = session.head(url, timeout=30)
        return r.status_code == 200
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
        "generated_utc": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
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
        "generated_utc":  datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
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
            "mask":  "categorical",
        },
        "file_count": len(files_on_disk),
        "files":      files_on_disk,
    }

    manifest_path = output_dir / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    log.info("Manifest written: %d file(s)", len(files_on_disk))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    today_utc  = datetime.datetime.utcnow().date()
    cutoff_date = today_utc - datetime.timedelta(days=RETENTION_DAYS)

    log.info("=== MUR SST Daily Retrieval ===")
    log.info("Today (UTC): %s  |  Cutoff: %s", today_utc, cutoff_date)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Purge old files
    deleted = _purge_old_files(OUTPUT_DIR, cutoff_date)
    log.info("Purged %d old file(s).", len(deleted))

    # 2. Find the 3 most-recent available days
    session = _make_session()
    candidate_dates = [
        today_utc - datetime.timedelta(days=d) for d in range(1, SEARCH_WINDOW + 1)
    ]

    target_dates: list[datetime.date] = []
    log.info("Probing server for available dates …")
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

    if not target_dates:
        log.error("No available dates found within search window.")
        return

    log.info("Fetching %d day(s): %s", len(target_dates),
             [d.isoformat() for d in target_dates])

    # 3. Download and convert each day
    fetched = []
    for date in target_dates:
        filename = f"MUR_SST_{date.strftime('%Y%m%d')}.json"
        dest = OUTPUT_DIR / filename
        success = _fetch_day_json(session, date, dest)
        if success:
            # Peek at row_count from the written file
            with open(dest, "r", encoding="utf-8") as fh:
                meta = json.load(fh)
            fetched.append({
                "filename":  filename,
                "date":      date.isoformat(),
                "sha256":    _sha256(dest),
                "row_count": meta.get("row_count"),
            })

    # 4. Write latest.json and manifest
    if fetched:
        newest = max(target_dates)
        _write_latest(OUTPUT_DIR, newest)

    _write_manifest(OUTPUT_DIR, fetched)

    log.info("=== Done. %d/%d day(s) retrieved. ===",
             len(fetched), len(target_dates))


if __name__ == "__main__":
    main()
