"""
StaticLayersRetrieval.py
========================
Fetches:
- Bathymetry (GEBCO_2023 primary via NCEI, fallback GEBCO_2020 + ETOPO)
- Depth contours (fathom-aligned depths, dual ft/fathom labeling, shelf_break flag)
- Bathymetry grid JSON (raw depth grid for feature detection algorithms)
- Coastline line (Natural Earth 10m, public domain)
- Land mask polygons (Natural Earth 10m, public domain)

Caching
-------
Bathymetry, coastline, and land mask are static datasets. Files are skipped
on re-run unless they are missing or older than CACHE_DAYS (default: 30 days).
This prevents unnecessary re-fetching in daily CI/GitHub Actions workflows.

Contour depth levels (fathom-aligned for offshore fishing)
----------------------------------------------------------
  10 fm  =   60 ft  — nearshore / inshore boundary
  20 fm  =  120 ft  — inner shelf
  30 fm  =  180 ft  — mid shelf
  50 fm  =  300 ft  — outer shelf
 100 fm  =  600 ft  — inner shelf break (wahoo, mahi-mahi zone)
 200 fm  = 1200 ft  — TRUE SHELF BREAK (billfish, tuna, swordfish) ← most important
 300 fm  = 1800 ft  — upper slope
 500 fm  = 3000 ft  — canyon heads, deep drop
1000 fm  = 6000 ft  — abyssal / very deep water

The 200 fm (1200 ft) contour is flagged with shelf_break=true in properties
for special UI treatment (bolder stroke, permanent label, etc.).

Outputs into DailySST/
  bathymetry_contours.json  — GeoJSON LineStrings with depth_ft + depth_fathoms
  bathymetry_grid.json      — Raw 2D depth grid for feature detection
  noaa_coastline.json       — GeoJSON LineStrings (Natural Earth 10m)
  landmask.json             — GeoJSON Polygons / MultiPolygons (Natural Earth 10m)
"""

import csv
import datetime
import io
import json
import logging
import math
import pathlib
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

# stride=1 → native GEBCO resolution (~450 m grid spacing)
# stride=2 → ~900 m — faster download, lower shelf-edge accuracy
BATHY_STRIDE = 1

# Re-fetch bathymetry only if output files are older than this many days.
# Bathymetry updates ~annually; 30 days is a safe default for CI runs.
CACHE_DAYS = 30

OUTPUT_DIR = pathlib.Path(__file__).resolve().parent / "DailySST"
TIMEOUT    = 300   # seconds — stride=1 downloads are larger; give extra headroom

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Natural Earth sources (GitHub raw — public domain, no auth)
# ---------------------------------------------------------------------------
NE_BASE          = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson"
NE_COASTLINE_URL = f"{NE_BASE}/ne_10m_coastline.geojson"
NE_LAND_URL      = f"{NE_BASE}/ne_10m_land.geojson"

# ---------------------------------------------------------------------------
# Contour depth levels — fathom-aligned for offshore fishing
# 1 fathom = 6 feet exactly
# ---------------------------------------------------------------------------
#
# Chosen to match how offshore fishermen navigate and communicate:
#   - 100 fm (600 ft)  : inner shelf break — wahoo, mahi-mahi zone
#   - 200 fm (1200 ft) : TRUE SHELF BREAK — the primary billfish / tuna / swordfish boundary.
#                        This is the single most important depth contour in the dataset.
#                        Flagged shelf_break=true for special UI rendering.
#   - 500 fm (3000 ft) : canyon heads — Baltimore, Wilmington, Norfolk, Hudson
#
CONTOUR_DEPTHS_FT = [60, 120, 180, 300, 600, 1200, 1800, 3000, 6000]

SHELF_BREAK_FT = 1200   # 200 fathoms — flagged in contour properties

# ---------------------------------------------------------------------------
# ERDDAP bathymetry sources — tried in order until one succeeds
#
# Priority rationale:
#   1. GEBCO_2020 via coastwatch.pfeg  — 15 arc-second (~450 m), confirmed reliable.
#      GEBCO_2023/2024/2025 are not hosted on any public ERDDAP griddap server as of
#      April 2026; they are only available as direct downloads from gebco.net.
#      GEBCO_2020 remains the highest-resolution GEBCO version on ERDDAP.
#   2. ETOPO_2022 (15 arc-sec) via oceanwatch.pifsc.noaa.gov  — NOAA's own 2022
#      topography model, same 15 arc-second resolution as GEBCO, different server.
#      Incorporates updated multibeam surveys through 2022.
#   3. ETOPO_2022 (60 arc-sec) via NCEI  — lower resolution last resort (~1800 m).
#      Use only if both higher-resolution sources fail.
#
# Note: To upgrade to a newer GEBCO grid when it becomes available on ERDDAP,
# add it as the first entry. Check: https://coastwatch.pfeg.noaa.gov/erddap/griddap/
# ---------------------------------------------------------------------------
BATHY_SOURCES = [
    ("https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020.csvp",          "elevation"),  # 15 arc-sec
    ("https://oceanwatch.pifsc.noaa.gov/erddap/griddap/ETOPO_2022_v1_15s.csvp",  "z"),          # 15 arc-sec
    ("https://www.ncei.noaa.gov/erddap/griddap/ETOPO_2022_v1_60s.csvp",           "z"),          # 60 arc-sec (lower res)
]

# ---------------------------------------------------------------------------
# HTTP session with retry
# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

# ---------------------------------------------------------------------------
# Cache validation
# ---------------------------------------------------------------------------
def _bathy_cache_valid() -> bool:
    """
    Returns True if bathymetry output files exist and are fresh enough to skip
    re-fetching. Checks both contours and the raw grid — if either is missing
    or stale, a full re-fetch is triggered.
    """
    required = [
        OUTPUT_DIR / "bathymetry_contours.json",
        OUTPUT_DIR / "bathymetry_grid.json",
    ]
    cutoff = datetime.datetime.now() - datetime.timedelta(days=CACHE_DAYS)
    for path in required:
        if not path.exists():
            log.info("Cache miss: %s not found — will re-fetch.", path.name)
            return False
        mtime = datetime.datetime.fromtimestamp(path.stat().st_mtime)
        if mtime < cutoff:
            log.info("Cache stale: %s is %d days old (limit: %d) — will re-fetch.",
                     path.name, (datetime.datetime.now() - mtime).days, CACHE_DAYS)
            return False
    log.info("Bathymetry cache is valid (files < %d days old) — skipping fetch.", CACHE_DAYS)
    return True

def _static_cache_valid(path: pathlib.Path) -> bool:
    """
    Simple existence check for coastline / land mask.
    These change even less often than bathymetry; re-fetch only if absent.
    Delete the file manually to force a refresh.
    """
    if path.exists():
        log.info("%s exists — skipping fetch. (Delete to force refresh.)", path.name)
        return True
    return False

# ---------------------------------------------------------------------------
# Bathymetry fetch
# ---------------------------------------------------------------------------
def _parse_erddap_csvp(text: str) -> list[dict]:
    """
    Parse ERDDAP csvp response.
    Returns list of dicts with lat, lon, depth_ft, depth_fathoms.
    Land / above-sea-level cells (elevation >= 0) are included with
    depth_ft=None so the grid builder can identify land areas.
    """
    reader = csv.reader(io.StringIO(text))
    rows   = list(reader)[2:]   # skip header + units rows
    data   = []
    for row in rows:
        try:
            lat  = float(row[0])
            lon  = float(row[1])
            elev = float(row[2])   # metres, negative = below sea level
        except (IndexError, ValueError):
            continue
        if elev >= 0:
            # Land or sea surface — no depth value
            data.append({"lat": lat, "lon": lon, "depth_ft": None, "depth_fathoms": None})
        else:
            depth_m       = abs(elev)
            depth_ft      = round(depth_m * 3.28084, 1)
            depth_fathoms = round(depth_m / 1.8288,  2)   # 1 fathom = 1.8288 m exactly
            data.append({"lat": lat, "lon": lon,
                         "depth_ft": depth_ft, "depth_fathoms": depth_fathoms})
    return data

def _try_erddap_source(session: requests.Session, base_url: str,
                       var: str, stride: int) -> list[dict]:
    url = (
        f"{base_url}"
        f"?{var}"
        f"[({LAT_MIN}):{stride}:({LAT_MAX})]"
        f"[({LON_MIN}):{stride}:({LON_MAX})]"
    )
    log.info("  Trying %s ...", base_url)
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return _parse_erddap_csvp(r.text)

def _fetch_bathymetry(session: requests.Session) -> list[dict]:
    log.info("Fetching bathymetry  (stride=%d, ~%.0f m resolution) ...",
             BATHY_STRIDE, BATHY_STRIDE * 450)
    last_err = None
    for base_url, var in BATHY_SOURCES:
        try:
            data = _try_erddap_source(session, base_url, var, BATHY_STRIDE)
            if data:
                ocean = sum(1 for r in data if r["depth_ft"] is not None)
                log.info("  Got %d points (%d ocean) from %s", len(data), ocean, base_url)
                return data
        except Exception as exc:
            log.warning("  Source failed (%s): %s", base_url, exc)
            last_err = exc
    raise RuntimeError(f"All bathymetry sources failed. Last error: {last_err}")

# ---------------------------------------------------------------------------
# Grid builder
# Constructs a 2-D depth grid (lats × lons) from the flat row list.
# NaN marks land cells and cells with no data.
# Gap-fill: up to 6 passes of 4-neighbour averaging to fill isolated NaN cells
# caused by ERDDAP stride rounding near coastlines.
# ---------------------------------------------------------------------------
def _build_grid(rows: list[dict]) -> tuple[list, list, list]:
    """
    Returns (lats, lons, grid) where grid[i][j] is depth in feet for cell
    (lats[i], lons[j]), or math.nan for land / no-data cells.
    """
    lats    = sorted(set(r["lat"] for r in rows))
    lons    = sorted(set(r["lon"] for r in rows))
    lat_idx = {v: i for i, v in enumerate(lats)}
    lon_idx = {v: i for i, v in enumerate(lons)}
    n_rows  = len(lats)
    n_cols  = len(lons)

    flat = [math.nan] * (n_rows * n_cols)
    for r in rows:
        if r["depth_ft"] is not None:
            idx      = lat_idx[r["lat"]] * n_cols + lon_idx[r["lon"]]
            flat[idx] = r["depth_ft"]

    # Gap-fill passes — fills isolated NaN ocean cells near the coast
    for _ in range(6):
        new_flat = flat[:]
        changed  = False
        for row in range(n_rows):
            for col in range(n_cols):
                i = row * n_cols + col
                if not math.isnan(flat[i]):
                    continue
                neighbours = []
                for dr, dc in [(-1, 0), (1, 0), (0, -1), (0, 1)]:
                    nr, nc = row + dr, col + dc
                    if 0 <= nr < n_rows and 0 <= nc < n_cols:
                        v = flat[nr * n_cols + nc]
                        if not math.isnan(v):
                            neighbours.append(v)
                if neighbours:
                    new_flat[i] = sum(neighbours) / len(neighbours)
                    changed      = True
        flat = new_flat
        if not changed:
            break

    grid = [flat[r * n_cols:(r + 1) * n_cols] for r in range(n_rows)]
    return lats, lons, grid

# ---------------------------------------------------------------------------
# Chaikin corner-cutting smoothing
# ---------------------------------------------------------------------------
def _chaikin_smooth(coords: list, iterations: int = 2) -> list:
    if len(coords) < 3:
        return coords
    for _ in range(iterations):
        new_coords = []
        for i in range(len(coords) - 1):
            x1, y1 = coords[i]
            x2, y2 = coords[i + 1]
            new_coords.append([0.75 * x1 + 0.25 * x2, 0.75 * y1 + 0.25 * y2])
            new_coords.append([0.25 * x1 + 0.75 * x2, 0.25 * y1 + 0.75 * y2])
        coords = new_coords
    return coords

# ---------------------------------------------------------------------------
# Contour generation
# ---------------------------------------------------------------------------
def _extract_contour_lines(lats: list, lons: list,
                            grid: list, depth_ft: float) -> list[list]:
    """
    Extract contour lines at the given depth (feet) from the 2-D grid.
    Returns a list of coordinate sequences (each a list of [lon, lat] pairs).
    """
    from contourpy import contour_generator
    cg    = contour_generator(x=lons, y=lats, z=grid)
    lines = cg.lines(depth_ft)

    MIN_POINTS = 6
    output     = []
    for line in lines:
        if len(line) < MIN_POINTS:
            continue
        coords = [[float(p[0]), float(p[1])] for p in line]
        coords = _chaikin_smooth(coords, iterations=2)
        output.append(coords)
    return output

def write_contours(lats: list, lons: list, grid: list) -> None:
    """
    Generate GeoJSON contours for all fishing-relevant depth levels.
    Each feature carries both depth_ft and depth_fathoms for UI unit switching,
    human-readable label strings, and a shelf_break flag for the 200 fm line.
    """
    log.info("Generating depth contours for %d levels ...", len(CONTOUR_DEPTHS_FT))
    features = []

    for depth_ft in CONTOUR_DEPTHS_FT:
        depth_fathoms = depth_ft / 6   # exact: 1 fm = 6 ft

        # Label strings — UI can show either depending on user preference
        label_ft      = f"{depth_ft} ft"
        label_fathoms = f"{int(depth_fathoms)} fm"   # all our levels are whole fathoms

        is_shelf_break = (depth_ft == SHELF_BREAK_FT)
        lines          = _extract_contour_lines(lats, lons, grid, depth_ft)

        for coords in lines:
            features.append({
                "type": "Feature",
                "geometry": {
                    "type":        "LineString",
                    "coordinates": coords,
                },
                "properties": {
                    # Numeric values — use for calculations / filtering
                    "depth_ft":      depth_ft,
                    "depth_fathoms": int(depth_fathoms),

                    # Label strings — ready for map annotation
                    # UI switches between label_ft and label_fathoms based on user preference
                    "label_ft":      label_ft,
                    "label_fathoms": label_fathoms,

                    # Special flag — the 200 fm shelf break gets a bolder stroke
                    # and a permanent label in the UI regardless of zoom level
                    "shelf_break":   is_shelf_break,
                },
            })

        log.info("  %4d ft (%3d fm) — %d contour segments", depth_ft, int(depth_fathoms), len(lines))

    dest = OUTPUT_DIR / "bathymetry_contours.json"
    tmp  = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"type": "FeatureCollection", "features": features}, fh,
                  separators=(",", ":"))
    tmp.rename(dest)
    log.info("Contours written: %d features across %d depth levels  (%.1f KB)",
             len(features), len(CONTOUR_DEPTHS_FT), dest.stat().st_size / 1024)

# ---------------------------------------------------------------------------
# Bathymetry grid output
# Raw 2-D depth arrays for use by the feature detection algorithm.
# Mirrors the SST _grid.json format for consistency across the data pipeline.
# ---------------------------------------------------------------------------
def write_bathymetry_grid(lats: list, lons: list, grid: list) -> None:
    """
    Export the raw bathymetry grid as compact JSON.

    Schema:
    {
      "meta": { ... },
      "lats": [...],           // 1-D sorted lat array
      "lons": [...],           // 1-D sorted lon array
      "depth_ft":      [[...]] // 2-D grid [lat_idx][lon_idx], null = land / no data
      "depth_fathoms": [[...]] // same grid in fathoms (1 fm = 6 ft)
    }

    Used by feature detection for:
      - Shelf break detection (depth threshold crossing + gradient)
      - Slope calculation (dDepth/dDistance for canyon identification)
      - Canyon head identification (local depth maxima along contour)
      - SST / depth cross-referencing (thermal break × shelf break alignment)
    """
    log.info("Writing bathymetry grid JSON ...")

    # Build both grids in a single pass — NaN → null, round values
    grid_ft      = []
    grid_fathoms = []
    for row in grid:
        ft_row = []
        fm_row = []
        for cell in row:
            if math.isnan(cell):
                ft_row.append(None)
                fm_row.append(None)
            else:
                ft_row.append(round(cell,        1))
                fm_row.append(round(cell / 6.0,  2))
        grid_ft.append(ft_row)
        grid_fathoms.append(fm_row)

    res_lat = round(lats[1] - lats[0], 6) if len(lats) > 1 else None
    res_lon = round(lons[1] - lons[0], 6) if len(lons) > 1 else None

    payload = {
        "meta": {
            "generated_utc":       (datetime.datetime.now(datetime.timezone.utc)
                                    .isoformat(timespec="seconds")
                                    .replace("+00:00", "Z")),
            "source":              "GEBCO_2020 (primary) | ETOPO_2022_v1_15s | ETOPO_2022_v1_60s",
            "stride":              BATHY_STRIDE,
            "res_lat_deg":         res_lat,
            "res_lon_deg":         res_lon,
            "n_lats":              len(lats),
            "n_lons":              len(lons),
            "region": {
                "lat_min": LAT_MIN, "lat_max": LAT_MAX,
                "lon_min": LON_MIN, "lon_max": LON_MAX,
            },
            "units": {
                "depth_ft":      "feet below surface (positive = deeper); null = land or no data",
                "depth_fathoms": "fathoms (1 fm = 6 ft exactly); null = land or no data",
            },
            "contour_depths_ft":    CONTOUR_DEPTHS_FT,
            "shelf_break_ft":       SHELF_BREAK_FT,
            "shelf_break_fathoms":  int(SHELF_BREAK_FT / 6),
            "note": (
                "Null cells are land or areas with no bathymetric data. "
                "Positive values represent ocean depth below the surface, "
                "not elevation above it."
            ),
        },
        "lats":          [round(v, 5) for v in lats],
        "lons":          [round(v, 5) for v in lons],
        "depth_ft":      grid_ft,
        "depth_fathoms": grid_fathoms,
    }

    dest = OUTPUT_DIR / "bathymetry_grid.json"
    tmp  = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    tmp.rename(dest)
    log.info("Bathymetry grid written: %d × %d cells  (%.1f KB)",
             len(lats), len(lons), dest.stat().st_size / 1024)

# ---------------------------------------------------------------------------
# Bbox clipping helpers
# ---------------------------------------------------------------------------
PAD = 0.5   # degrees of padding around bbox — retains features that cross the edge

def _pt_in_bbox(lon: float, lat: float) -> bool:
    return (LON_MIN - PAD <= lon <= LON_MAX + PAD and
            LAT_MIN - PAD <= lat <= LAT_MAX + PAD)

def _ring_intersects_bbox(ring: list) -> bool:
    return any(_pt_in_bbox(pt[0], pt[1]) for pt in ring)

def _clip_linestring(coords: list) -> list[list]:
    """Split a linestring into segments wherever points leave the padded bbox."""
    segments: list[list] = []
    current:  list       = []
    for pt in coords:
        if _pt_in_bbox(pt[0], pt[1]):
            current.append([round(pt[0], 5), round(pt[1], 5)])
        else:
            if len(current) >= 2:
                segments.append(current)
            current = []
    if len(current) >= 2:
        segments.append(current)
    return segments

# ---------------------------------------------------------------------------
# Coastline lines (Natural Earth 10m)
# ---------------------------------------------------------------------------
def write_noaa_coastline(session: requests.Session) -> None:
    log.info("Fetching Natural Earth 10m coastline (lines) ...")
    r = session.get(NE_COASTLINE_URL, timeout=TIMEOUT)
    r.raise_for_status()
    data     = r.json()
    features = []

    for feat in data.get("features", []):
        geom  = feat.get("geometry", {})
        gtype = geom.get("type", "")
        if gtype == "LineString":
            all_coords = [geom["coordinates"]]
        elif gtype == "MultiLineString":
            all_coords = geom["coordinates"]
        else:
            continue
        for coords in all_coords:
            for segment in _clip_linestring(coords):
                if len(segment) < 3:
                    continue
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "LineString", "coordinates": segment},
                    "properties": {
                        "type":   "coastline",
                        "source": "Natural Earth 10m",
                        "style":  {"color": "#000000", "width": 2},
                    },
                })

    dest = OUTPUT_DIR / "noaa_coastline.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump({"type": "FeatureCollection", "features": features}, fh)
    log.info("Coastline lines written: %d features  (%.1f KB)",
             len(features), dest.stat().st_size / 1024)

# ---------------------------------------------------------------------------
# Land mask polygons (Natural Earth 10m)
# Used by renderer to fill land areas over the SST raster before drawing
# the coastline stroke on top.
# ---------------------------------------------------------------------------
def write_land_mask(session: requests.Session) -> None:
    log.info("Fetching Natural Earth 10m land polygons ...")
    r = session.get(NE_LAND_URL, timeout=TIMEOUT)
    r.raise_for_status()
    data     = r.json()
    features = []

    for feat in data.get("features", []):
        geom  = feat.get("geometry", {})
        gtype = geom.get("type", "")
        if gtype == "Polygon":
            polys = [geom["coordinates"]]
        elif gtype == "MultiPolygon":
            polys = geom["coordinates"]
        else:
            continue

        clipped_polys = []
        for poly in polys:
            if not poly:
                continue
            exterior = poly[0]
            if not _ring_intersects_bbox(exterior):
                continue   # polygon entirely outside bbox — skip
            clipped_rings = []
            for ring in poly:
                clipped = [[round(pt[0], 5), round(pt[1], 5)] for pt in ring]
                if len(clipped) >= 3:
                    clipped_rings.append(clipped)
            if clipped_rings:
                clipped_polys.append(clipped_rings)

        if not clipped_polys:
            continue

        geom_out = (
            {"type": "Polygon",      "coordinates": clipped_polys[0]}
            if len(clipped_polys) == 1
            else {"type": "MultiPolygon", "coordinates": clipped_polys}
        )
        features.append({
            "type":       "Feature",
            "geometry":   geom_out,
            "properties": {"type": "land", "source": "Natural Earth 10m"},
        })

    dest = OUTPUT_DIR / "landmask.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump({"type": "FeatureCollection", "features": features}, fh)
    log.info("Land mask written: %d polygon features  (%.1f KB)",
             len(features), dest.stat().st_size / 1024)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    session = _make_session()

    # ── Bathymetry (contours + raw grid) ────────────────────────────────────
    log.info("=== Bathymetry ===")
    if _bathy_cache_valid():
        log.info("Using cached bathymetry — skipping fetch.")
    else:
        rows = _fetch_bathymetry(session)
        # Build the grid once and pass to both writers — avoids double computation
        log.info("Building depth grid ...")
        lats, lons, grid = _build_grid(rows)
        log.info("Grid: %d lats × %d lons", len(lats), len(lons))
        write_contours(lats, lons, grid)
        write_bathymetry_grid(lats, lons, grid)

    # ── Coastline lines ─────────────────────────────────────────────────────
    log.info("=== Coastline ===")
    if not _static_cache_valid(OUTPUT_DIR / "noaa_coastline.json"):
        write_noaa_coastline(session)

    # ── Land mask polygons ──────────────────────────────────────────────────
    log.info("=== Land Mask ===")
    if not _static_cache_valid(OUTPUT_DIR / "landmask.json"):
        write_land_mask(session)

    log.info("=== Done. ===")


if __name__ == "__main__":
    main()
