"""
StaticLayersRetrieval.py
========================
Adds NOAA ENC-derived coastline (vector) for chart-quality shoreline.
"""

import csv
import io
import json
import logging
import math
import pathlib
import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

LAT_MIN = 33.70
LAT_MAX = 39.00
LON_MIN = -78.89
LON_MAX = -72.21

BATHY_STRIDE = 2

OUTPUT_DIR = pathlib.Path(__file__).resolve().parent / "DailySST"

ERDDAP_BATHY = "https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020.csvp"

# NOAA WFS (coastline)
NOAA_WFS = "https://gis.ngdc.noaa.gov/arcgis/services/DEM_mosaics/DEM_global_mosaic/ImageServer/WFSServer"

TIMEOUT = 180

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

def _make_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=2)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

# ---------------------------------------------------------------------------
# Bathymetry (unchanged)
# ---------------------------------------------------------------------------

def _fetch_bathymetry(session):
    url = (
        f"{ERDDAP_BATHY}"
        f"?elevation"
        f"[({LAT_MIN}):{BATHY_STRIDE}:({LAT_MAX})]"
        f"[({LON_MIN}):{BATHY_STRIDE}:({LON_MAX})]"
    )

    log.info("Fetching bathymetry...")
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()

    reader = csv.reader(io.StringIO(r.text))
    rows = list(reader)[2:]

    data = []
    for r in rows:
        try:
            lat = float(r[0])
            lon = float(r[1])
            elev = float(r[2])
        except:
            continue

        depth_ft = None if elev >= 0 else abs(elev) * 3.28084
        data.append({"lat": lat, "lon": lon, "depth_ft": depth_ft})

    return data

# ---------------------------------------------------------------------------
# NOAA COASTLINE (NEW)
# ---------------------------------------------------------------------------

def write_noaa_coastline(session):
    """
    Fetch coastline from NOAA WFS (vector) and output clean GeoJSON.
    """

    log.info("Fetching NOAA ENC coastline...")

    # Bounding box
    bbox = f"{LON_MIN},{LAT_MIN},{LON_MAX},{LAT_MAX}"

    params = {
        "service": "WFS",
        "request": "GetFeature",
        "version": "1.1.0",
        "typeName": "DEM_global_mosaic:footprint",
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
        "bbox": bbox
    }

    r = session.get(NOAA_WFS, params=params, timeout=TIMEOUT)
    r.raise_for_status()

    data = r.json()

    features = []

    for feat in data.get("features", []):
        geom = feat.get("geometry")
        if not geom:
            continue

        if geom["type"] == "Polygon":
            rings = geom["coordinates"]
        elif geom["type"] == "MultiPolygon":
            rings = [r for poly in geom["coordinates"] for r in poly]
        else:
            continue

        for ring in rings:
            if len(ring) < 20:
                continue

            coords = [[round(pt[0],5), round(pt[1],5)] for pt in ring]

            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": coords
                },
                "properties": {
                    "type": "coastline",
                    "source": "NOAA ENC",
                    "style": {
                        "color": "#000000",
                        "width": 2
                    }
                }
            })

    dest = OUTPUT_DIR / "noaa_coastline.json"

    with open(dest, "w") as f:
        json.dump({
            "type": "FeatureCollection",
            "features": features
        }, f)

    log.info("NOAA coastline written (%d features)", len(features))

    return dest

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    session = _make_session()

    try:
        _fetch_bathymetry(session)
    except Exception as e:
        log.warning("Bathymetry skipped: %s", e)

    try:
        write_noaa_coastline(session)
    except Exception as e:
        log.error("NOAA coastline failed: %s", e)

    log.info("Done.")

if __name__ == "__main__":
    main()
