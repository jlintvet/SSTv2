"""
DailySSTRetrieval.py
====================
Retrieves the last three days of SST data from two satellite sources:

  1. MUR SST (jplMURSST41) — NASA JPL 1-km daily blended analysis.
     Gap-filled (no cloud holes). ~2 day publication lag.

  2. GOES-19 ABI SST (goes19SSThourly) — NOAA/AOML hourly geostationary
     SST. IR only — cloud pixels are null. ~3-6 hour lag.

Land masking
------------
  MUR:    Uses the built-in `mask` field (1=ocean only). Non-ocean cells
          (land, lakes, sea ice, tidal) are set to sst=null at parse time.
  GOES-19: SST is already null on land (IR sensor). regionmask applied as
           a second-pass safety net using Natural Earth 110m land polygons.

Output layout (relative to this script's directory):
  DailySST/
    MUR_SST_YYYYMMDD.json
    MUR_SST_YYYYMMDD.jpg
    latest.json / latest.jpg
    manifest.json
    GOES19_SST_YYYYMMDD.json
    GOES19_SST_YYYYMMDD.jpg
    goes19_latest.json / goes19_latest.jpg
    goes19_manifest.json
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

ERDDAP_BASE    = "https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41.csvp"
ERDDAP_MUR_IMG = "https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41.largePng"

ERDDAP_GOES19     = "https://cwcgom.aoml.noaa.gov/erddap/griddap/goes19SSThourly.csvp"
ERDDAP_GOES19_IMG = "https://cwcgom.aoml.noaa.gov/erddap/griddap/goes19SSThourly.largePng"
GOES19_VARIABLE   = "sst"
GOES19_STRIDE     = 1

LAT_MIN = 33.70
LAT_MAX = 39.00
LON_MIN = -78.89
LON_MAX = -72.21

LAT_STRIDE = 1
LON_STRIDE = 1

VARIABLES = ["analysed_sst", "analysis_error", "sea_ice_fraction", "mask"]

IMG_WIDTH        = 1800
IMG_HEIGHT       = 1200
SST_COLORBAR_MIN = 4
SST_COLORBAR_MAX = 32

OUTPUT_DIR     = pathlib.Path(__file__).resolve().parent / "DailySST"
RETENTION_DAYS = 3
SEARCH_WINDOW  = 7

TIMEOUT_SECONDS     = 300
IMG_TIMEOUT_SECONDS = 120
MAX_RETRIES         = 3
BACKOFF_FACTOR      = 2

DAILY_HOUR = "09:00:00Z"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Land mask — regionmask (optional, graceful fallback if not installed)
# ---------------------------------------------------------------------------

def _build_land_mask(lats, lons):
    """
    Build a boolean 2-D array (lat x lon) that is True where a cell is land.
    Uses regionmask + Natural Earth 110m land polygons.
    Returns None if regionmask is not installed (non-fatal).
    """
    try:
        import regionmask
        import numpy as np
        land   = regionmask.defined_regions.natural_earth_v5_0_0.land_110
        lon2d, lat2d = np.meshgrid(lons, lats)
        mask   = land.mask(lon2d, lat2d)   # NaN = ocean, integer = land region
        is_land = ~np.isnan(mask)           # True where land
        return is_land
    except ImportError:
        log.warning("regionmask not installed — skipping secondary land mask. "
                    "Run: pip install regionmask")
        return None
    except Exception as exc:
        log.warning("regionmask error: %s — skipping secondary land mask.", exc)
        return None


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
    session.mount("http://",  adapter)
    return session


def _build_url(date: datetime.date) -> str:
    ts        = f"{date.isoformat()}T{DAILY_HOUR}"
    time_part = f"[({ts}):1:({ts})]"
    lat_part  = f"[({LAT_MIN}):{LAT_STRIDE}:({LAT_MAX})]"
    lon_part  = f"[({LON_MIN}):{LON_STRIDE}:({LON_MAX})]"
    var_queries = ",".join(
        f"{v}{time_part}{lat_part}{lon_part}" for v in VARIABLES
    )
    return f"{ERDDAP_BASE}?{var_queries}"


def _build_mur_image_url(date: datetime.date) -> str:
    ts        = f"{date.isoformat()}T{DAILY_HOUR}"
    time_part = f"[({ts}):1:({ts})]"
    lat_part  = f"[({LAT_MIN}):1:({LAT_MAX})]"
    lon_part  = f"[({LON_MIN}):1:({LON_MAX})]"
    query = (
        f"analysed_sst{time_part}{lat_part}{lon_part}"
        f"&.draw=surface&.trim=2"
        f"&.vars=longitude|latitude|analysed_sst"
        f"&.colorBar=KT_thermal|||{SST_COLORBAR_MIN}|{SST_COLORBAR_MAX}|"
        f"&.bgColor=0xffccccff"
        f"&.width={IMG_WIDTH}&.height={IMG_HEIGHT}"
    )
    return f"{ERDDAP_MUR_IMG}?{query}"


def _build_goes19_image_url(date: datetime.date, hour: int) -> str:
    ts        = f"{date.isoformat()}T{hour:02d}:00:00Z"
    time_part = f"[({ts}):1:({ts})]"
    lat_part  = f"[({LAT_MIN}):1:({LAT_MAX})]"
    lon_part  = f"[({LON_MIN}):1:({LON_MAX})]"
    query = (
        f"sst{time_part}{lat_part}{lon_part}"
        f"&.draw=surface&.trim=2"
        f"&.vars=longitude|latitude|sst"
        f"&.colorBar=KT_thermal|||{SST_COLORBAR_MIN}|{SST_COLORBAR_MAX}|"
        f"&.bgColor=0xffccccff"
        f"&.width={IMG_WIDTH}&.height={IMG_HEIGHT}"
    )
    return f"{ERDDAP_GOES19_IMG}?{query}"


def _fetch_image(session, url, dest, label) -> bool:
    log.info("  %s image  ->  %s", label, dest.name)
    try:
        r = session.get(url, timeout=IMG_TIMEOUT_SECONDS)
        r.raise_for_status()
    except requests.HTTPError as exc:
        log.warning("  Image HTTP error (%s): %s", label, exc)
        return False
    except requests.RequestException as exc:
        log.warning("  Image request error (%s): %s", label, exc)
        return False

    content_type = r.headers.get("Content-Type", "")
    if "image" not in content_type and len(r.content) < 1000:
        log.warning("  %s image response looks invalid — skipping.", label)
        return False

    tmp = dest.with_suffix(".tmp")
    tmp.write_bytes(r.content)
    tmp.rename(dest)
    log.info("  Saved %s  (%.1f KB)", dest.name, dest.stat().st_size / 1024)
    return True


def _check_availability(session, date) -> bool:
    nc_url = _build_url(date).replace(".csvp", ".nc")
    try:
        r = session.head(nc_url, timeout=15)
        if r.status_code == 200:
            return True
        if r.status_code == 404:
            return False
    except requests.RequestException:
        pass

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


def _fahrenheit(val: str) -> "float | None":
    try:
        c = float(val)
        if c != c:
            return None
        if c < -3.0 or c > 40.0:
            return None
        return round(c * 9/5 + 32, 4)
    except (ValueError, TypeError):
        return None


def _float(val: str) -> "float | None":
    try:
        f = float(val)
        return None if f != f else round(f, 6)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# MUR CSV parser — uses mask field to null out non-ocean cells
# ---------------------------------------------------------------------------

def _parse_csv(text: str) -> list[dict]:
    """
    Parse ERDDAP csvp response for MUR SST.

    MUR mask values (categorical):
      1 = open ocean          ← keep SST
      2 = land                ← null SST
      3 = lake                ← null SST
      4 = ice                 ← null SST
      5 = tidal / estuarine   ← null SST (sounds, estuaries)

    Using the mask field means no external library is needed for MUR —
    the data is already authoritative about what is ocean vs land/sound.
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
        try:
            lat = _float(raw[idx["latitude"]])
            lon = _float(raw[idx["longitude"]])
        except KeyError:
            continue
        if lat is None or lon is None:
            continue

        # ── Land mask via MUR mask field ──────────────────────────────────
        # mask == 1 means open ocean. Everything else (land, lakes, ice,
        # tidal waters including Pamlico Sound, Albemarle Sound, etc.)
        # is set to null so it won't render on the SST heatmap.
        mask_val = None
        if "mask" in idx and raw[idx["mask"]] not in ("", "NaN"):
            try:
                mask_val = int(float(raw[idx["mask"]]))
            except (ValueError, TypeError):
                pass

        # Only emit SST for open-ocean cells (mask == 1)
        is_ocean = (mask_val == 1)

        sst_raw = raw[idx.get("analysed_sst", -1)] if "analysed_sst" in idx else None
        sst     = _fahrenheit(sst_raw) if (sst_raw is not None and is_ocean) else None

        row = {
            "lat":  lat,
            "lon":  lon,
            "sst":  sst,
            "error": round(float(raw[idx["analysis_error"]]) * 1.8, 4)
                     if "analysis_error" in idx
                     and raw[idx["analysis_error"]] not in ("", "NaN")
                     else None,
            "ice":  _float(raw[idx["sea_ice_fraction"]]) if "sea_ice_fraction" in idx else None,
            "mask": mask_val,
        }
        rows.append(row)

    ocean_count = sum(1 for r in rows if r["sst"] is not None)
    log.info("  MUR parsed: %d rows, %d ocean, %d masked (land/lake/ice/tidal)",
             len(rows), ocean_count, len(rows) - ocean_count)
    return rows


def _actual_extent(rows: list[dict]) -> dict:
    if not rows:
        return {}
    lats = [r["lat"] for r in rows]
    lons = [r["lon"] for r in rows]
    return {
        "lat_min": min(lats), "lat_max": max(lats),
        "lon_min": min(lons), "lon_max": max(lons),
    }


def _fetch_day_json(session, date, dest) -> bool:
    url = _build_url(date)
    log.info("Downloading MUR  %s  ->  %s", date.isoformat(), dest.name)
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

    extent  = _actual_extent(rows)
    ocean   = sum(1 for r in rows if r["sst"] is not None)
    payload = {
        "date":          date.isoformat(),
        "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z"),
        "dataset":       "jplMURSST41",
        "source":        "https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41",
        "region": {
            "lat_min": LAT_MIN, "lat_max": LAT_MAX,
            "lon_min": LON_MIN, "lon_max": LON_MAX,
            "stride":  LAT_STRIDE,
        },
        "actual_extent": extent,
        "units": {
            "sst":   "fahrenheit",
            "error": "fahrenheit",
            "ice":   "fraction_0_to_1",
            "mask":  "categorical_1=ocean_2=land_3=lake_4=ice_5=tidal",
        },
        "land_mask_note": "sst=null for mask!=1 (land/lake/ice/tidal including sounds)",
        "row_count":   len(rows),
        "ocean_count": ocean,
        "rows":        rows,
    }

    tmp = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    tmp.rename(dest)
    log.info("  Saved %s  (%d rows, %d ocean, %.1f KB)",
             dest.name, len(rows), ocean, dest.stat().st_size / 1024)
    return True


def _sha256(path: pathlib.Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _purge_old_files(output_dir, cutoff) -> list:
    deleted = []
    for pattern in ("MUR_SST_????????.json", "MUR_SST_????????.jpg"):
        for f in sorted(output_dir.glob(pattern)):
            date_str = f.stem.split("_")[-1]
            try:
                file_date = datetime.date(
                    int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            except ValueError:
                continue
            if file_date < cutoff:
                log.info("Purging old file: %s", f.name)
                f.unlink()
                deleted.append(f.name)
    return deleted


def _write_latest(output_dir, newest_date) -> None:
    date_str = newest_date.strftime('%Y%m%d')
    for ext in (".json", ".jpg"):
        src = output_dir / f"MUR_SST_{date_str}{ext}"
        dst = output_dir / f"latest{ext}"
        if src.exists():
            dst.write_bytes(src.read_bytes())
            log.info("latest%s updated  ->  %s", ext, src.name)


def _write_manifest(output_dir, fetched) -> None:
    files_on_disk = []
    for f in sorted(output_dir.glob("MUR_SST_????????.json")):
        date_str = f.stem.split("_")[-1]
        iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        entry    = next((x for x in fetched if x.get("filename") == f.name), None)
        img_name = f"MUR_SST_{date_str}.jpg"
        img_path = output_dir / img_name
        files_on_disk.append({
            "filename":   f.name,
            "date":       iso_date,
            "size_bytes": f.stat().st_size,
            "sha256":     entry["sha256"] if entry else _sha256(f),
            "row_count":  entry["row_count"] if entry else None,
            "image":      {"filename": img_name,
                           "size_bytes": img_path.stat().st_size if img_path.exists() else None},
        })

    manifest = {
        "generated_utc":  datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z"),
        "dataset":        "jplMURSST41",
        "source":         "https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41",
        "retention_days": RETENTION_DAYS,
        "region": {"lat_min": LAT_MIN, "lat_max": LAT_MAX,
                   "lon_min": LON_MIN, "lon_max": LON_MAX, "stride": LAT_STRIDE},
        "units":  {"sst": "fahrenheit", "error": "fahrenheit",
                   "ice": "fraction_0_to_1"},
        "image_settings": {"colorbar": "KT_thermal",
                           "sst_min_c": SST_COLORBAR_MIN,
                           "sst_max_c": SST_COLORBAR_MAX,
                           "width_px": IMG_WIDTH, "height_px": IMG_HEIGHT},
        "file_count": len(files_on_disk),
        "files":      files_on_disk,
    }

    manifest_path = output_dir / "manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    log.info("Manifest written: %d file(s)", len(files_on_disk))


# ---------------------------------------------------------------------------
# GOES-19 pipeline
# ---------------------------------------------------------------------------

def _build_goes19_url(date, hour) -> str:
    ts        = f"{date.isoformat()}T{hour:02d}:00:00Z"
    time_part = f"[({ts}):1:({ts})]"
    lat_part  = f"[({LAT_MIN}):{GOES19_STRIDE}:({LAT_MAX})]"
    lon_part  = f"[({LON_MIN}):{GOES19_STRIDE}:({LON_MAX})]"
    return f"{ERDDAP_GOES19}?{GOES19_VARIABLE}{time_part}{lat_part}{lon_part}"


def _find_latest_goes19_hour(session, date) -> "int | None":
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


def _parse_goes19_csv(text: str, land_mask=None,
                      lats_index=None, lons_index=None) -> list[dict]:
    """
    Parse GOES-19 SST csvp.
    SST is already null on land (IR sensor). Optionally applies regionmask
    as a second-pass safety net for any residual land bleed.

    land_mask: 2-D bool array (lat x lon), True = land — from _build_land_mask()
    lats_index / lons_index: sorted unique lat/lon lists for index lookup
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

        # Secondary land mask via regionmask
        if sst is not None and land_mask is not None:
            try:
                import numpy as np
                lat_i = int(round((lat - LAT_MIN) / (LAT_MAX - LAT_MIN) * (land_mask.shape[0] - 1)))
                lon_i = int(round((lon - LON_MIN) / (LON_MAX - LON_MIN) * (land_mask.shape[1] - 1)))
                lat_i = max(0, min(lat_i, land_mask.shape[0] - 1))
                lon_i = max(0, min(lon_i, land_mask.shape[1] - 1))
                if land_mask[lat_i, lon_i]:
                    sst = None  # land cell — suppress
            except Exception:
                pass

        rows.append({"lat": lat, "lon": lon, "sst": sst})

    return rows


def _fetch_goes19_day_json(session, date, hour, dest) -> bool:
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

    # Build regionmask land mask for GOES-19 secondary pass
    import numpy as np
    lats_arr = np.arange(LAT_MIN, LAT_MAX + 0.01, GOES19_STRIDE * 0.009)  # approx
    lons_arr = np.arange(LON_MIN, LON_MAX + 0.01, GOES19_STRIDE * 0.009)
    land_mask = _build_land_mask(lats_arr, lons_arr)

    rows = _parse_goes19_csv(r.text, land_mask=land_mask)
    if not rows:
        log.warning("  GOES-19: No rows parsed for %s — skipping.", date.isoformat())
        return False

    ocean = sum(1 for r in rows if r["sst"] is not None)
    cloud = len(rows) - ocean
    log.info("  GOES-19 %s %02d:00Z: %d rows (%d ocean, %d cloud/land/null)",
             date.isoformat(), hour, len(rows), ocean, cloud)

    extent  = _actual_extent(rows)
    payload = {
        "date":          date.isoformat(),
        "hour_utc":      hour,
        "obs_time_utc":  f"{date.isoformat()}T{hour:02d}:00:00Z",
        "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "dataset":       "goes19SSThourly",
        "source":        "https://cwcgom.aoml.noaa.gov/erddap/griddap/goes19SSThourly",
        "sensor":        "GOES-19 ABI (NOAA/AOML hourly SST)",
        "cloud_note":    "sst=null means cloud-covered or land — no gap fill applied",
        "region": {"lat_min": LAT_MIN, "lat_max": LAT_MAX,
                   "lon_min": LON_MIN, "lon_max": LON_MAX, "stride": GOES19_STRIDE},
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


def _purge_goes19_files(output_dir, cutoff) -> list:
    deleted = []
    for pattern in ("GOES19_SST_????????.json", "GOES19_SST_????????.jpg"):
        for f in sorted(output_dir.glob(pattern)):
            date_str = f.stem.split("_")[-1]
            try:
                file_date = datetime.date(
                    int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            except ValueError:
                continue
            if file_date < cutoff:
                log.info("Purging old GOES-19 file: %s", f.name)
                f.unlink()
                deleted.append(f.name)
    return deleted


def _write_goes19_latest(output_dir, newest_date) -> None:
    date_str = newest_date.strftime('%Y%m%d')
    for ext in (".json", ".jpg"):
        src = output_dir / f"GOES19_SST_{date_str}{ext}"
        dst = output_dir / f"goes19_latest{ext}"
        if src.exists():
            dst.write_bytes(src.read_bytes())
            log.info("goes19_latest%s updated  ->  %s", ext, src.name)


def _write_goes19_manifest(output_dir, fetched) -> None:
    files_on_disk = []
    for f in sorted(output_dir.glob("GOES19_SST_????????.json")):
        date_str = f.stem.split("_")[-1]
        iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        entry    = next((x for x in fetched if x.get("filename") == f.name), None)

        ocean = entry.get("ocean_count") if entry else None
        total = entry.get("row_count")   if entry else None
        hour  = entry.get("hour_utc")    if entry else None
        if ocean is None or total is None:
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    meta = json.load(fh)
                ocean = meta.get("ocean_count")
                total = meta.get("row_count")
                hour  = meta.get("hour_utc")
            except Exception:
                pass

        cloud   = (total - ocean) if (ocean is not None and total is not None) else None
        pct_cov = round(ocean / total * 100, 1) if (ocean and total) else None
        img_name = f"GOES19_SST_{date_str}.jpg"
        img_path = output_dir / img_name
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
            "image":        {"filename": img_name,
                             "size_bytes": img_path.stat().st_size if img_path.exists() else None},
        })

    sortable = [f for f in files_on_disk if f["ocean_count"] is not None]
    sortable.sort(key=lambda x: x["ocean_count"], reverse=True)
    rank_map = {f["filename"]: i + 1 for i, f in enumerate(sortable)}
    for f in files_on_disk:
        f["coverage_rank"] = rank_map.get(f["filename"])

    best = sortable[0] if sortable else None
    manifest = {
        "generated_utc":  datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "dataset":        "goes19SSThourly",
        "source":         "https://cwcgom.aoml.noaa.gov/erddap/griddap/goes19SSThourly",
        "sensor":         "GOES-19 ABI (NOAA/AOML hourly SST)",
        "retention_days": RETENTION_DAYS,
        "region": {"lat_min": LAT_MIN, "lat_max": LAT_MAX,
                   "lon_min": LON_MIN, "lon_max": LON_MAX, "stride": GOES19_STRIDE},
        "units":          {"sst": "fahrenheit"},
        "image_settings": {"colorbar": "KT_thermal",
                           "sst_min_c": SST_COLORBAR_MIN,
                           "sst_max_c": SST_COLORBAR_MAX,
                           "width_px": IMG_WIDTH, "height_px": IMG_HEIGHT},
        "file_count":     len(files_on_disk),
        "best_coverage":  best["filename"] if best else None,
        "files":          files_on_disk,
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

    # ── MUR SST ─────────────────────────────────────────────────────────────
    log.info("--- MUR SST ---")
    _purge_old_files(OUTPUT_DIR, cutoff_date)

    target_dates: list[datetime.date] = []
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
        for date in target_dates:
            date_str = date.strftime('%Y%m%d')
            json_filename = f"MUR_SST_{date_str}.json"
            json_dest     = OUTPUT_DIR / json_filename
            success = _fetch_day_json(session, date, json_dest)
            if success:
                with open(json_dest, "r", encoding="utf-8") as fh:
                    meta = json.load(fh)
                fetched.append({
                    "filename":  json_filename,
                    "date":      date.isoformat(),
                    "sha256":    _sha256(json_dest),
                    "row_count": meta.get("row_count"),
                })
            img_dest = OUTPUT_DIR / f"MUR_SST_{date_str}.jpg"
            _fetch_image(session, _build_mur_image_url(date), img_dest,
                         f"MUR {date.isoformat()}")

    if fetched:
        _write_latest(OUTPUT_DIR, max(target_dates))
    _write_manifest(OUTPUT_DIR, fetched)

    # ── GOES-19 ──────────────────────────────────────────────────────────────
    log.info("--- GOES-19 Hourly SST ---")
    _purge_goes19_files(OUTPUT_DIR, cutoff_date)

    goes_dates: list[datetime.date] = []
    goes_hours: dict[datetime.date, int] = {}
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
        for date in goes_dates:
            date_str      = date.strftime('%Y%m%d')
            hour          = goes_hours[date]
            json_filename = f"GOES19_SST_{date_str}.json"
            json_dest     = OUTPUT_DIR / json_filename
            success = _fetch_goes19_day_json(session, date, hour, json_dest)
            if success:
                with open(json_dest, "r", encoding="utf-8") as fh:
                    meta = json.load(fh)
                goes_fetched.append({
                    "filename":    json_filename,
                    "date":        date.isoformat(),
                    "hour_utc":    hour,
                    "sha256":      _sha256(json_dest),
                    "row_count":   meta.get("row_count"),
                    "ocean_count": meta.get("ocean_count"),
                    "cloud_count": meta.get("cloud_count"),
                })
            img_dest = OUTPUT_DIR / f"GOES19_SST_{date_str}.jpg"
            _fetch_image(session, _build_goes19_image_url(date, hour), img_dest,
                         f"GOES-19 {date.isoformat()} {hour:02d}:00Z")

    if goes_fetched:
        _write_goes19_latest(OUTPUT_DIR, max(goes_dates))
    _write_goes19_manifest(OUTPUT_DIR, goes_fetched)

    log.info("=== Done. MUR %d/%d | GOES-19 %d/%d day(s) retrieved. ===",
             len(fetched),      len(target_dates),
             len(goes_fetched), len(goes_dates))


if __name__ == "__main__":
    main()
