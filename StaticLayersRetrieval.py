"""
StaticLayersRetrieval.py
========================
Fetches static reference layers for the Mid-Atlantic offshore fishing region
and writes them as JSON files into DailySST/.
"""

import csv
import io
import json
import logging
import math
import pathlib
import re
import xml.etree.ElementTree as ET

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

BATHY_STRIDE = 2

OUTPUT_DIR    = pathlib.Path(__file__).resolve().parent / "DailySST"
GPX_FILENAMES = [
    ("Fishing_Spots_HatterasNC.gpx",   "HatterasNC"),
    ("Fishing_Spots_MoreheadNC.gpx",   "MoreheadNC"),
    ("Fishing_spots_ChesapeakeMD.gpx", "ChesapeakeMD"),
]

ERDDAP_BATHY = "https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020.csvp"

TIMEOUT    = 180
MAX_RETRIES = 3
BACKOFF    = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
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
# Bathymetry
# ---------------------------------------------------------------------------

def _fetch_bathymetry(session: requests.Session) -> list[dict]:
    url = (
        f"{ERDDAP_BATHY}"
        f"?elevation"
        f"[({LAT_MIN}):{BATHY_STRIDE}:({LAT_MAX})]"
        f"[({LON_MIN}):{BATHY_STRIDE}:({LON_MAX})]"
    )

    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()

    reader   = csv.reader(io.StringIO(r.text))
    all_rows = list(reader)

    rows = []
    for raw in all_rows[2:]:
        try:
            lat  = float(raw[0])
            lon  = float(raw[1])
            elev = float(raw[2])
        except:
            continue

        depth_ft = None if elev >= 0 else abs(elev) * 3.28084
        rows.append({"lat": lat, "lon": lon, "depth_ft": depth_ft})

    return rows

# ---------------------------------------------------------------------------
# GRID BUILDER (UPDATED)
# ---------------------------------------------------------------------------

def _build_grid(rows: list[dict], for_coastline: bool = False):
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
            if r["depth_ft"] is None:
                flat[i] = 1.0
            else:
                flat[i] = -r["depth_ft"]
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
# CONTOURS (UPDATED)
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
        output.append(coords)

    return output


def write_contours(rows):
    lats, lons, grid = _build_grid(rows)

    all_features = []

    CONTOUR_DEPTHS_FT = [30,60,100,200,300,600,1000,1500,2000]

    for depth in CONTOUR_DEPTHS_FT:
        lines = _grid_to_geojson_contours(lats, lons, grid, depth)

        for coords in lines:
            all_features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {"depth_ft": depth}
            })

    # -------------------------------
    # COASTLINE (NEW)
    # -------------------------------
    lats_c, lons_c, grid_c = _build_grid(rows, for_coastline=True)
    coast_lines = _grid_to_geojson_contours(lats_c, lons_c, grid_c, 0.0)

    for coords in coast_lines:
        all_features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {
                "depth_ft": 0,
                "type": "coastline",
                "style": {
                    "color": "#000000",
                    "width": 2
                }
            }
        })

    dest = OUTPUT_DIR / "bathymetry_contours.json"
    with open(dest, "w") as f:
        json.dump({"type":"FeatureCollection","features":all_features}, f)

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    session = _make_session()
    rows = _fetch_bathymetry(session)

    write_contours(rows)

    print("Done.")


if __name__ == "__main__":
    main()
