"""
StaticLayersRetrieval.py
========================
Fetches two static reference layers for the Mid-Atlantic offshore fishing
region and writes them as JSON files into DailySST/.

  DailySST/
    bathymetry.json   – GEBCO_2020 depth grid (~1.8 km resolution, ~8 MB)
    wrecks.json       – NOAA AWOIS + ENC wrecks (GeoJSON FeatureCollection)

Run once to populate, or re-run via manual workflow dispatch to refresh.

Bathymetry
----------
  GEBCO_2020 via NOAA CoastWatch ERDDAP (dataset: GEBCO_2020)
  Stride 4 = ~1.8 km resolution — enough for depth contours and canyon
  visualization while keeping the file under ~10 MB.
  Set BATHY_STRIDE = 1 for full 450 m resolution (~135 MB — requires Git LFS).

Wrecks
------
  NOAA AWOIS CSV direct download (no REST API, no SSL issues).
  URL: https://nauticalcharts.noaa.gov/data/docs/awois.csv
  Filtered to bounding box in-script.
  Also fetches ENC wrecks from the Coast Survey GeoJSON export if available.

Dependencies
------------
  pip install requests
"""

import csv
import io
import json
import logging
import pathlib
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration — keep in sync with DailySSTRetrieval.py
# ---------------------------------------------------------------------------

LAT_MIN = 33.70
LAT_MAX = 39.00
LON_MIN = -80.85
LON_MAX = -72.21

# Bathymetry stride:
#   1  = full 15 arc-sec (~450 m)  — ~135 MB, requires Git LFS
#   4  = ~1.8 km                   — ~8 MB   (default, fits GitHub)
#   10 = ~4.5 km                   — ~1.5 MB
BATHY_STRIDE = 4

OUTPUT_DIR = pathlib.Path(__file__).resolve().parent / "DailySST"

ERDDAP_BATHY = "https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020.csvp"

# NOAA AWOIS direct CSV download — stable, no REST/SSL issues
AWOIS_CSV_URL = "https://nauticalcharts.noaa.gov/data/docs/awois-a.csv"

# Fallback: NOAA CoastWatch ERDDAP has an ENC wrecks dataset too
ENC_ERDDAP_URL = (
    "https://coastwatch.pfeg.noaa.gov/erddap/tabledap/noaa_coastwatch_enc_wrecks.csvp"
    f"?longitude,latitude,vesslterms,depth,history,catwrk"
    f"&longitude>={LON_MIN}&longitude<={LON_MAX}"
    f"&latitude>={LAT_MIN}&latitude<={LAT_MAX}"
)

TIMEOUT = 180
MAX_RETRIES = 3
BACKOFF = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


# ---------------------------------------------------------------------------
# Bathymetry — GEBCO via ERDDAP
# ---------------------------------------------------------------------------

def _fetch_bathymetry(session: requests.Session) -> list[dict]:
    url = (
        f"{ERDDAP_BATHY}"
        f"?elevation"
        f"[({LAT_MIN}):{BATHY_STRIDE}:({LAT_MAX})]"
        f"[({LON_MIN}):{BATHY_STRIDE}:({LON_MAX})]"
    )
    log.info("Fetching GEBCO bathymetry (stride=%d) …", BATHY_STRIDE)
    log.info("  URL: %s", url)

    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()

    reader = csv.reader(io.StringIO(r.text))
    all_rows = list(reader)
    # csvp: row 0 = headers+units, row 1 = units, rows 2+ = data
    rows = []
    for raw in all_rows[2:]:
        if len(raw) < 3:
            continue
        try:
            lat  = round(float(raw[0]), 6)
            lon  = round(float(raw[1]), 6)
            elev = float(raw[2])          # metres, negative = below sea level
        except (ValueError, IndexError):
            continue

        depth_ft = None if elev >= 0 else round(abs(elev) * 3.28084, 1)
        rows.append({"lat": lat, "lon": lon, "depth_ft": depth_ft})

    ocean = sum(1 for r in rows if r["depth_ft"] is not None)
    log.info("  Parsed %d points (%d ocean, %d land/null).",
             len(rows), ocean, len(rows) - ocean)
    return rows


def write_bathymetry(session: requests.Session) -> pathlib.Path:
    rows = _fetch_bathymetry(session)
    payload = {
        "dataset":    "GEBCO_2020",
        "source":     "https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020",
        "resolution": f"15 arc-seconds x stride {BATHY_STRIDE} (~{BATHY_STRIDE * 0.45:.1f} km)",
        "stride":     BATHY_STRIDE,
        "units":      {"depth_ft": "feet below surface (positive = deeper), null = land"},
        "region": {
            "lat_min": LAT_MIN, "lat_max": LAT_MAX,
            "lon_min": LON_MIN, "lon_max": LON_MAX,
        },
        "point_count": len(rows),
        "points": rows,
    }
    dest = OUTPUT_DIR / "bathymetry.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    log.info("bathymetry.json written  (%.1f MB)", dest.stat().st_size / 1e6)
    return dest


# ---------------------------------------------------------------------------
# Wrecks — NOAA AWOIS direct CSV download
# ---------------------------------------------------------------------------

def _fetch_awois_wrecks(session: requests.Session) -> list[dict]:
    """
    Download the AWOIS CSV directly from NOAA and filter to bounding box.
    CSV columns vary by version but always include LATDEC, LONDEC, VESSLTERMS,
    DEPTH, HISTORY.
    """
    log.info("Fetching AWOIS wrecks CSV …")
    try:
        r = session.get(AWOIS_CSV_URL, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as exc:
        log.warning("  AWOIS download failed: %s", exc)
        return []

    features = []
    reader = csv.DictReader(io.StringIO(r.text))

    # Normalise header names — NOAA uses inconsistent casing across versions
    def get(row, *keys):
        for k in keys:
            for rk in row:
                if rk.strip().upper() == k.upper():
                    v = row[rk].strip()
                    return v if v else None
        return None

    for row in reader:
        try:
            lat = float(get(row, "LATDEC", "LAT", "LATITUDE") or "")
            lon = float(get(row, "LONDEC", "LON", "LONGITUDE") or "")
        except (ValueError, TypeError):
            continue

        # Filter to bounding box
        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
            continue

        raw_depth = get(row, "DEPTH", "DEPTH_M", "DEPTHLSD")
        try:
            depth_ft = round(float(raw_depth), 1) if raw_depth else None
        except ValueError:
            depth_ft = None

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [round(lon, 6), round(lat, 6)]},
            "properties": {
                "name":         get(row, "VESSLTERMS", "NAME", "FEATURE") or "Unknown",
                "depth_ft":     depth_ft,
                "source":       "AWOIS",
                "history":      get(row, "HISTORY", "REMARKS", "DESCRIPTION"),
                "year_sunk":    get(row, "YEARSUNK", "YEAR"),
                "feature_type": get(row, "CATWRK", "FEATTYPE", "TYPE"),
            },
        })

    log.info("  AWOIS: %d features in bounding box.", len(features))
    return features


def _fetch_enc_wrecks(session: requests.Session) -> list[dict]:
    """
    Try to fetch ENC wrecks from CoastWatch ERDDAP tabledap.
    This dataset may not exist — we catch all errors gracefully.
    """
    log.info("Fetching ENC wrecks from ERDDAP (best-effort) …")
    try:
        r = session.get(ENC_ERDDAP_URL, timeout=60)
        r.raise_for_status()
    except Exception as exc:
        log.info("  ENC ERDDAP not available (%s) — skipping.", exc)
        return []

    features = []
    reader = csv.reader(io.StringIO(r.text))
    all_rows = list(reader)
    if len(all_rows) < 3:
        return features

    headers = [h.split(" (")[0].strip().lower() for h in all_rows[0]]
    idx = {name: i for i, name in enumerate(headers)}

    for raw in all_rows[2:]:
        if len(raw) < 2:
            continue
        try:
            lon = float(raw[idx["longitude"]])
            lat = float(raw[idx["latitude"]])
        except (KeyError, ValueError, IndexError):
            continue

        raw_depth = raw[idx["depth"]] if "depth" in idx else None
        try:
            depth_ft = round(float(raw_depth) * 3.28084, 1) if raw_depth else None
        except (ValueError, TypeError):
            depth_ft = None

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [round(lon, 6), round(lat, 6)]},
            "properties": {
                "name":         raw[idx["vesslterms"]] if "vesslterms" in idx else "Unknown",
                "depth_ft":     depth_ft,
                "source":       "ENC",
                "history":      raw[idx["history"]] if "history" in idx else None,
                "year_sunk":    None,
                "feature_type": raw[idx["catwrk"]] if "catwrk" in idx else None,
            },
        })

    log.info("  ENC ERDDAP: %d features.", len(features))
    return features


def write_wrecks(session: requests.Session) -> pathlib.Path:
    features = []
    features.extend(_fetch_awois_wrecks(session))
    features.extend(_fetch_enc_wrecks(session))

    # Deduplicate by rounded coordinate (AWOIS and ENC overlap significantly)
    seen = set()
    unique = []
    for f in features:
        key = (
            round(f["geometry"]["coordinates"][0], 3),
            round(f["geometry"]["coordinates"][1], 3),
        )
        if key not in seen:
            seen.add(key)
            unique.append(f)

    log.info("Wrecks total: %d unique features (%d before dedup).",
             len(unique), len(features))

    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "sources": {
                "AWOIS": AWOIS_CSV_URL,
                "ENC":   "https://coastwatch.pfeg.noaa.gov/erddap/tabledap/",
            },
            "region": {
                "lat_min": LAT_MIN, "lat_max": LAT_MAX,
                "lon_min": LON_MIN, "lon_max": LON_MAX,
            },
            "units": {
                "depth_ft": "feet below surface, null if uncharted"
            },
        },
        "feature_count": len(unique),
        "features": unique,
    }

    dest = OUTPUT_DIR / "wrecks.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, separators=(",", ":"))
    log.info("wrecks.json written  (%d features, %.2f MB)",
             len(unique), dest.stat().st_size / 1e6)
    return dest


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=== Static Layers Retrieval ===")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    session = _make_session()

    try:
        write_bathymetry(session)
    except Exception as exc:
        log.error("Bathymetry fetch failed: %s", exc)

    try:
        write_wrecks(session)
    except Exception as exc:
        log.error("Wrecks fetch failed: %s", exc)

    log.info("=== Done ===")


if __name__ == "__main__":
    main()
