"""
StaticLayersRetrieval.py
========================
Fetches two static reference layers for the Mid-Atlantic offshore fishing
region and writes them as JSON files into DailySST/.

  DailySST/
    bathymetry.json   – GEBCO_2020 depth grid (ERDDAP, ~450 m resolution)
    wrecks.json       – NOAA ENC wrecks + obstructions (GeoJSON FeatureCollection)

These files do not change daily.  Run this script once to populate them,
or add it as a manual-dispatch workflow step.  The SST workflow does NOT
need to call this script on every run.

Bathymetry source
-----------------
  GEBCO_2020 via NOAA CoastWatch ERDDAP (dataset: GEBCO_2020)
  Variable   : elevation (meters, negative = below sea level)
  Resolution : 15 arc-seconds (~450 m)
  Endpoint   : https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020.csvp

  Output row structure:
    { "lat": 36.85, "lon": -75.98, "depth_ft": -312.3 }

  Depth is converted from meters to feet and sign-flipped so that depth_ft
  is a positive number below the surface (e.g. 312 ft deep).  Land cells
  (elevation > 0) are written as null so the frontend can skip them.

Wrecks source
-------------
  NOAA Office of Coast Survey — ENC Wrecks & Obstructions
  REST endpoint: https://wrecks.nauticalcharts.noaa.gov/arcgis/rest/services/
                   public_wrecks/Wrecks_And_Obstructions/MapServer/{layer}/query
  Layers used:
    0  = ENC wrecks (all scales merged — most comprehensive)
    3  = AWOIS wrecks (historic positions, depth info where available)

  The REST API accepts a geometry bounding box filter and returns GeoJSON.
  Results are paginated (max 1000 per call); the script pages through all
  records automatically.

  Output: standard GeoJSON FeatureCollection with properties:
    name, depth_ft (charted depth in feet, null if unknown),
    source ("ENC" or "AWOIS"), history (AWOIS description where available)

Dependencies
------------
  pip install requests
"""

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

# GEBCO stride — 1 = full 15 arc-sec (~450 m).
# Increase to reduce file size (e.g. 4 = ~1.8 km, 10 = ~4.5 km).
BATHY_STRIDE = 1

OUTPUT_DIR = pathlib.Path(__file__).parent / "DailySST"

ERDDAP_BATHY = "https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020.csvp"

# NOAA Wrecks REST service
WRECKS_BASE = (
    "https://wrecks.nauticalcharts.noaa.gov/arcgis/rest/services/"
    "public_wrecks/Wrecks_And_Obstructions/MapServer"
)
# Layer IDs: 0 = ENC wrecks coastal/approach scale, 4 = AWOIS wrecks
WRECK_LAYERS = {
    "ENC":   0,
    "AWOIS": 4,
}

TIMEOUT = 120
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
    """
    Download GEBCO_2020 elevation for the bounding box via ERDDAP csvp.
    Returns a list of { lat, lon, depth_ft } dicts.
    Land cells (elevation >= 0) are included with depth_ft = null so the
    frontend knows the grid point exists but is not ocean.
    """
    url = (
        f"{ERDDAP_BATHY}"
        f"?elevation"
        f"[({LAT_MIN}):{BATHY_STRIDE}:({LAT_MAX})]"
        f"[({LON_MIN}):{BATHY_STRIDE}:({LON_MAX})]"
    )
    log.info("Fetching GEBCO bathymetry …")
    log.info("  URL: %s", url)

    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()

    rows = []
    lines = r.text.splitlines()
    # csvp: row 0 = headers with units, row 1 = units row, rows 2+ = data
    if len(lines) < 3:
        log.warning("  GEBCO response too short — no data rows.")
        return rows

    import csv, io
    reader = csv.reader(io.StringIO(r.text))
    all_rows = list(reader)
    # Skip header (row 0) and units (row 1)
    for raw in all_rows[2:]:
        if len(raw) < 3:
            continue
        try:
            lat   = round(float(raw[0]), 6)
            lon   = round(float(raw[1]), 6)
            elev  = float(raw[2])          # metres, negative = below sea level
        except (ValueError, IndexError):
            continue

        if elev >= 0:
            # Land or exactly sea level — keep point but no depth
            depth_ft = None
        else:
            # Convert metres below sea level → positive feet
            depth_ft = round(abs(elev) * 3.28084, 1)

        rows.append({"lat": lat, "lon": lon, "depth_ft": depth_ft})

    log.info("  Parsed %d bathymetry points (%d ocean, %d land/null).",
             len(rows),
             sum(1 for r in rows if r["depth_ft"] is not None),
             sum(1 for r in rows if r["depth_ft"] is None))
    return rows


def write_bathymetry(session: requests.Session) -> pathlib.Path:
    rows = _fetch_bathymetry(session)
    payload = {
        "dataset":    "GEBCO_2020",
        "source":     "https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020",
        "resolution": "15 arc-seconds (~450 m)",
        "stride":     BATHY_STRIDE,
        "units":      {"depth_ft": "feet below surface (positive), null = land"},
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
# Wrecks — NOAA ENC + AWOIS REST API
# ---------------------------------------------------------------------------

def _query_wreck_layer(session: requests.Session,
                       layer_id: int,
                       source_label: str) -> list[dict]:
    """
    Page through the NOAA Wrecks REST service for one layer within the
    bounding box and return a list of GeoJSON-style feature dicts.
    """
    features = []
    offset = 0
    page_size = 1000

    # Bounding box in the format the Esri REST API expects
    bbox = f"{LON_MIN},{LAT_MIN},{LON_MAX},{LAT_MAX}"

    while True:
        params = {
            "where":          "1=1",
            "geometry":       bbox,
            "geometryType":   "esriGeometryEnvelope",
            "inSR":           "4326",
            "spatialRel":     "esriSpatialRelIntersects",
            "outFields":      "*",
            "returnGeometry": "true",
            "f":              "geojson",
            "resultOffset":   offset,
            "resultRecordCount": page_size,
        }

        url = f"{WRECKS_BASE}/{layer_id}/query"
        log.info("  Querying %s layer (offset=%d) …", source_label, offset)

        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            log.warning("  %s query failed at offset %d: %s",
                        source_label, offset, exc)
            break

        batch = data.get("features", [])
        if not batch:
            break

        for feat in batch:
            props = feat.get("properties") or feat.get("attributes") or {}
            geom  = feat.get("geometry", {})
            coords = geom.get("coordinates")

            if not coords or len(coords) < 2:
                continue

            lon_f, lat_f = coords[0], coords[1]

            # Normalise depth — field names differ between ENC and AWOIS
            depth_m = (
                props.get("depthsnd")       # ENC charted sounding in metres
                or props.get("drval1")      # ENC depth range value 1
                or props.get("depth")       # AWOIS depth field
            )
            try:
                depth_ft = round(float(depth_m) * 3.28084, 1) if depth_m else None
            except (ValueError, TypeError):
                depth_ft = None

            # Vessel / feature name
            name = (
                props.get("vesslterms")     # ENC vessel terms
                or props.get("name")        # AWOIS name
                or props.get("objnam")      # ENC object name
                or "Unknown"
            )

            feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon_f, lat_f]},
                "properties": {
                    "name":      name,
                    "depth_ft":  depth_ft,
                    "source":    source_label,
                    "history":   props.get("history") or props.get("remarks") or None,
                    "year_sunk": props.get("yearsunk") or None,
                    "feature_type": props.get("catwrk") or props.get("feattype") or None,
                },
            }
            features.append(feature)

        log.info("    Got %d features (total so far: %d)", len(batch), len(features))

        if len(batch) < page_size:
            break  # last page
        offset += page_size
        time.sleep(0.3)  # be polite

    return features


def write_wrecks(session: requests.Session) -> pathlib.Path:
    all_features = []
    for label, layer_id in WRECK_LAYERS.items():
        feats = _query_wreck_layer(session, layer_id, label)
        all_features.extend(feats)
        log.info("  %s: %d features", label, len(feats))

    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "source":      "NOAA Office of Coast Survey — ENC Wrecks & Obstructions",
            "service":     WRECKS_BASE,
            "layers":      WRECK_LAYERS,
            "region": {
                "lat_min": LAT_MIN, "lat_max": LAT_MAX,
                "lon_min": LON_MIN, "lon_max": LON_MAX,
            },
            "units": {"depth_ft": "feet below surface, null if uncharted"},
            "note": (
                "ENC layer = authoritative charted wrecks from electronic "
                "navigational charts. AWOIS layer = historic positions from "
                "the Automated Wreck and Obstruction Information System."
            ),
        },
        "feature_count": len(all_features),
        "features": all_features,
    }

    dest = OUTPUT_DIR / "wrecks.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, separators=(",", ":"))
    log.info("wrecks.json written  (%d features, %.2f MB)",
             len(all_features), dest.stat().st_size / 1e6)
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
