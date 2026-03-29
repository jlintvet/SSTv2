"""
StaticLayersRetrieval.py
========================
Fetches:
- Bathymetry (GEBCO via NCEI ERDDAP, fallback to ETOPO)
- Depth contours
- Smoothed coastline (derived)
- Coastline (Census TIGERweb)

Outputs into DailySST/
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

TIMEOUT = 180

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def _make_session():
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=2)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s

# ---------------------------------------------------------------------------
# Bathymetry — tries multiple public ERDDAP sources in order
# ---------------------------------------------------------------------------

def _parse_erddap_csvp(text):
    """Parse ERDDAP .csvp response into list of dicts."""
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)[2:]  # skip header + units row
    data = []
    for row in rows:
        try:
            lat   = float(row[0])
            lon   = float(row[1])
            elev  = float(row[2])
        except (IndexError, ValueError):
            continue
        depth_ft = None if elev >= 0 else abs(elev) * 3.28084
        data.append({"lat": lat, "lon": lon, "depth_ft": depth_ft})
    return data


def _try_erddap(session, base_url, var, stride=2):
    url = (
        f"{base_url}"
        f"?{var}"
        f"[({LAT_MIN}):{stride}:({LAT_MAX})]"
        f"[({LON_MIN}):{stride}:({LON_MAX})]"
    )
    log.info("  Trying: %s", base_url)
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return _parse_erddap_csvp(r.text)


BATHY_SOURCES = [
    # 1. NCEI ERDDAP — GEBCO 2023 (more permissive than coastwatch)
    (
        "https://www.ncei.noaa.gov/erddap/griddap/GEBCO_2023.csvp",
        "elevation",
    ),
    # 2. NCEI ERDDAP — ETOPO 2022 (1 arc-minute global relief)
    (
        "https://www.ncei.noaa.gov/erddap/griddap/ETOPO_2022_v1_60s.csvp",
        "z",
    ),
    # 3. coastwatch pfeg — original GEBCO 2020
    (
        "https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020.csvp",
        "elevation",
    ),
]


def _fetch_bathymetry(session):
    log.info("Fetching bathymetry...")

    last_err = None
    for base_url, var in BATHY_SOURCES:
        try:
            data = _try_erddap(session, base_url, var, stride=BATHY_STRIDE)
            if data:
                log.info("  Bathymetry: got %d points from %s", len(data), base_url)
                return data
        except Exception as e:
            log.warning("  Source failed (%s): %s", base_url, e)
            last_err = e

    raise RuntimeError(f"All bathymetry sources failed. Last error: {last_err}")

# ---------------------------------------------------------------------------
# Chaikin smoothing
# ---------------------------------------------------------------------------

def _chaikin_smooth(coords, iterations=2):
    if len(coords) < 3:
        return coords

    for _ in range(iterations):
        new_coords = []
        for i in range(len(coords) - 1):
            x1, y1 = coords[i]
            x2, y2 = coords[i + 1]
            q = [0.75 * x1 + 0.25 * x2, 0.75 * y1 + 0.25 * y2]
            r = [0.25 * x1 + 0.75 * x2, 0.25 * y1 + 0.75 * y2]
            new_coords.extend([q, r])
        coords = new_coords

    return coords

# ---------------------------------------------------------------------------
# Grid builder
# ---------------------------------------------------------------------------

def _build_grid(rows, for_coastline=False):
    lats = sorted(set(r["lat"] for r in rows))
    lons = sorted(set(r["lon"] for r in rows))

    lat_idx = {v: i for i, v in enumerate(lats)}
    lon_idx = {v: i for i, v in enumerate(lons)}

    n_rows = len(lats)
    n_cols = len(lons)

    flat = [math.nan] * (n_rows * n_cols)

    for r in rows:
        i = lat_idx[r["lat"]] * n_cols + lon_idx[r["lon"]]
        if for_coastline:
            flat[i] = 1.0 if r["depth_ft"] is None else -r["depth_ft"]
        else:
            if r["depth_ft"] is not None:
                flat[i] = r["depth_ft"]

    if not for_coastline:
        for _ in range(6):
            new_flat = flat[:]
            changed = False
            for row in range(n_rows):
                for col in range(n_cols):
                    i = row * n_cols + col
                    if not math.isnan(flat[i]):
                        continue
                    vals = []
                    for dr, dc in [(-1,0),(1,0),(0,-1),(0,1)]:
                        nr, nc = row+dr, col+dc
                        if 0 <= nr < n_rows and 0 <= nc < n_cols:
                            v = flat[nr*n_cols+nc]
                            if not math.isnan(v):
                                vals.append(v)
                    if vals:
                        new_flat[i] = sum(vals)/len(vals)
                        changed = True
            flat = new_flat
            if not changed:
                break

    grid = [flat[r*n_cols:(r+1)*n_cols] for r in range(n_rows)]
    return lats, lons, grid

# ---------------------------------------------------------------------------
# Contours
# ---------------------------------------------------------------------------

def _grid_to_geojson_contours(lats, lons, grid, depth_ft):
    from contourpy import contour_generator

    cg = contour_generator(x=lons, y=lats, z=grid)
    lines = cg.lines(depth_ft)

    MIN_POINTS = 10 if depth_ft == 0 else 6

    output = []
    for line in lines:
        if len(line) < MIN_POINTS:
            continue
        coords = [[float(p[0]), float(p[1])] for p in line]
        coords = _chaikin_smooth(coords, iterations=3 if depth_ft == 0 else 1)
        if depth_ft == 0 and len(coords) < 30:
            continue
        output.append(coords)

    return output

# ---------------------------------------------------------------------------
# Write contours
# ---------------------------------------------------------------------------

def write_contours(rows):
    log.info("Generating contours...")

    lats, lons, grid = _build_grid(rows)
    all_features = []

    for depth in [30, 60, 100, 200, 300, 600, 1000, 1500, 2000]:
        for coords in _grid_to_geojson_contours(lats, lons, grid, depth):
            all_features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {"depth_ft": depth}
            })

    # Derived coastline
    lats_c, lons_c, grid_c = _build_grid(rows, for_coastline=True)
    for coords in _grid_to_geojson_contours(lats_c, lons_c, grid_c, 0.0):
        all_features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {"depth_ft": 0, "type": "coastline_derived"}
        })

    dest = OUTPUT_DIR / "bathymetry_contours.json"
    with open(dest, "w") as f:
        json.dump({"type": "FeatureCollection", "features": all_features}, f)

    log.info("Contours written (%d features)", len(all_features))

# ---------------------------------------------------------------------------
# Coastline (Census TIGERweb — publicly open, no auth required)
# ---------------------------------------------------------------------------

def write_noaa_coastline(session):
    log.info("Fetching coastline from Census TIGERweb...")

    url = (
        "https://tigerweb.geo.census.gov/arcgis/rest/services/"
        "TIGERweb/coastline/MapServer/0/query"
    )

    params = {
        "where": "1=1",
        "outFields": "*",
        "geometry": f"{LON_MIN},{LAT_MIN},{LON_MAX},{LAT_MAX}",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outSR": "4326",
        "f": "geojson",
    }

    r = session.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()

    data = r.json()

    if "error" in data:
        raise RuntimeError(f"TIGERweb error: {data['error']}")

    features = []

    for feat in data.get("features", []):
        geom = feat.get("geometry")
        if not geom:
            continue

        gtype = geom.get("type", "")
        if gtype == "LineString":
            rings = [geom["coordinates"]]
        elif gtype == "MultiLineString":
            rings = geom["coordinates"]
        elif gtype == "Polygon":
            rings = geom["coordinates"]
        elif gtype == "MultiPolygon":
            rings = [ring for poly in geom["coordinates"] for ring in poly]
        else:
            continue

        for ring in rings:
            if len(ring) < 5:
                continue
            coords = [[round(pt[0], 5), round(pt[1], 5)] for pt in ring]
            features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {
                    "type": "coastline",
                    "source": "Census TIGERweb",
                    "style": {"color": "#000000", "width": 2}
                }
            })

    if not features:
        log.warning("No coastline features returned — check bbox or service URL.")

    dest = OUTPUT_DIR / "noaa_coastline.json"
    with open(dest, "w") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)

    log.info("Coastline written (%d features)", len(features))

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    session = _make_session()
    rows = _fetch_bathymetry(session)
    write_contours(rows)
    write_noaa_coastline(session)
    log.info("Done.")

if __name__ == "__main__":
    main()
