"""
StaticLayersRetrieval.py
========================
Fetches:
- Bathymetry (GEBCO_2023 primary via NCEI, fallback GEBCO_2020 + ETOPO)
- Depth contours (fathom-aligned depths, dual ft/fathom labeling, shelf_break flag)
- Bathymetry grid JSON (raw depth grid for feature detection algorithms)
- Coastline line (Natural Earth 10m, public domain)
- Land mask polygons (Natural Earth 10m, public domain)
- Wrecks / fishing spots (GPX → wrecks.json, always rebuilt on every run)
Caching
-------
Bathymetry, coastline, and land mask are static datasets. Files are skipped
on re-run unless they are missing or older than CACHE_DAYS (default: 30 days).
Wrecks are always rebuilt on every run — source GPX files can change at any
time and a stale wrecks.json is worse than the small overhead of rebuilding.
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
  bathymetry_grid.json      — Raw 2D depth grid (ft only, int-rounded) for
                              feature detection. Consumers derive fathoms as
                              depth_ft / 6.
  noaa_coastline.json       — GeoJSON LineStrings (Natural Earth 10m)
  landmask.json             — GeoJSON Polygons / MultiPolygons (Natural Earth 10m)
  wrecks.json               — GeoJSON FeatureCollection from source GPX files
"""
import csv
import datetime
import io
import json
import logging
import math
import pathlib
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
# stride=1 → native GEBCO resolution (~450 m grid spacing)
# stride=2 → ~900 m — faster download, lower shelf-edge accuracy
BATHY_STRIDE = 1
# Re-fetch bathymetry only if output files are older than this many days.
# Bathymetry updates ~annually; 30 days is a safe default for CI runs.
CACHE_DAYS = 30
# Bump this whenever write_bathymetry_grid() changes its output schema.
# _bathy_cache_valid() invalidates any cached bathymetry_grid.json whose
# meta.schema_version does not match this value, forcing a re-fetch even
# if the file is newer than CACHE_DAYS.
#   v1 = depth_ft (1 decimal) + depth_fathoms (2 decimal) grids
#   v2 = depth_ft only, int-rounded; fathoms derived client-side
BATHY_GRID_SCHEMA_VERSION = 2
OUTPUT_DIR = pathlib.Path(__file__).resolve().parent / "DailySST"
TIMEOUT    = 300   # seconds — stride=1 downloads are larger; give extra headroom
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
# Wrecks / fishing spots — GPX source files
#
# Keys   : filename (relative to OUTPUT_DIR)
# Values : region label used in wrecks.json properties
#
# To add a new region: drop the GPX file into DailySST/ and add an entry here.
# wrecks.json is always rebuilt on every run (no cache check).
# ---------------------------------------------------------------------------
WRECK_GPX_FILES = {
    "Fishing_Spots_HatterasNC.gpx":  "HatterasNC",
    "Fishing_Spots_MoreheadNC.gpx":  "MoreheadNC",
    "Fishing_spots_ChesapeakeMD.gpx": "ChesapeakeMD",
}
WRECK_SOURCE_LABEL = "Fishing Status (fishingstatus.com)"
WRECK_SYMBOL_DESCRIPTIONS = {
    "Wreck": "charted or known shipwreck",
    "Rocks": "rock, ledge, reef, or bottom structure",
}
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
CONTOUR_DEPTHS_FT = [60, 120, 180, 300, 600, 1200, 1800, 3000, 6000]
SHELF_BREAK_FT = 1200   # 200 fathoms — flagged in contour properties
# ---------------------------------------------------------------------------
# ERDDAP bathymetry sources — tried in order until one succeeds
# ---------------------------------------------------------------------------
BATHY_SOURCES = [
    ("https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020.csvp",          "elevation"),
    ("https://oceanwatch.pifsc.noaa.gov/erddap/griddap/ETOPO_2022_v1_15s.csvp",  "z"),
    ("https://www.ncei.noaa.gov/erddap/griddap/ETOPO_2022_v1_60s.csvp",           "z"),
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
    # Schema-version check: if the cached grid was written by an older version
    # of this script, invalidate the cache so the new schema gets written out
    # even when files are still within the CACHE_DAYS window.
    grid_path = OUTPUT_DIR / "bathymetry_grid.json"
    try:
        with open(grid_path, "r", encoding="utf-8") as fh:
            cached_meta = json.load(fh).get("meta", {})
        cached_version = cached_meta.get("schema_version")
        if cached_version != BATHY_GRID_SCHEMA_VERSION:
            log.info(
                "Cache schema mismatch: %s is schema v%s, current v%d — will re-fetch.",
                grid_path.name, cached_version, BATHY_GRID_SCHEMA_VERSION,
            )
            return False
    except (OSError, ValueError) as exc:
        log.info("Cache unreadable (%s) — will re-fetch.", exc)
        return False
    log.info(
        "Bathymetry cache is valid (files < %d days old, schema v%d) — skipping fetch.",
        CACHE_DAYS, BATHY_GRID_SCHEMA_VERSION,
    )
    return True
def _static_cache_valid(path: pathlib.Path) -> bool:
    if path.exists():
        log.info("%s exists — skipping fetch. (Delete to force refresh.)", path.name)
        return True
    return False
# ---------------------------------------------------------------------------
# Wrecks — GPX parsing and JSON output
# ---------------------------------------------------------------------------
_GPX_NS = {"gpx": "http://www.topografix.com/GPX/1/1"}
def _parse_gpx_file(path: pathlib.Path, region: str) -> list[dict]:
    """
    Parse a GPX 1.1 file and return a list of GeoJSON-style feature dicts.
    Handles both full-namespace GPX (xmlns="http://www.topografix.com/GPX/1/1")
    and bare/namespace-stripped GPX (as produced by the clean step).
    Properties extracted per waypoint:
      name   — <name> text
      symbol — <sym> text (e.g. "Rocks", "Wreck")
      fs_id  — ID parsed from <desc><![CDATA[ID#XXXXXXXX]]></desc>, or None
      region — the region label passed in
      source — WRECK_SOURCE_LABEL
    """
    try:
        tree = ET.parse(path)
        root = tree.getroot()
    except ET.ParseError as e:
        log.warning("  Could not parse %s: %s", path.name, e)
        return []
    # Support both namespaced and bare GPX tags
    tag = root.tag
    if tag.startswith("{"):
        ns_uri = tag[1:tag.index("}")]
        ns     = {"gpx": ns_uri}
        wpt_tag  = "gpx:wpt"
        name_tag = "gpx:name"
        sym_tag  = "gpx:sym"
        desc_tag = "gpx:desc"
    else:
        ns       = {}
        wpt_tag  = "wpt"
        name_tag = "name"
        sym_tag  = "sym"
        desc_tag = "desc"
    features = []
    for wpt in root.findall(wpt_tag, ns):
        try:
            lat = float(wpt.get("lat"))
            lon = float(wpt.get("lon"))
        except (TypeError, ValueError):
            continue
        name_el = wpt.find(name_tag, ns)
        sym_el  = wpt.find(sym_tag,  ns)
        desc_el = wpt.find(desc_tag, ns)
        name   = name_el.text.strip() if name_el is not None and name_el.text else ""
        symbol = sym_el.text.strip()  if sym_el  is not None and sym_el.text  else "Rocks"
        # Extract Fishing Status ID from CDATA description, e.g. "ID#377565"
        fs_id = None
        if desc_el is not None and desc_el.text:
            import re
            m = re.search(r"ID#(\d+)", desc_el.text)
            if m:
                fs_id = m.group(1)
        feature = {
            "type": "Feature",
            "geometry": {
                "type":        "Point",
                "coordinates": [round(lon, 5), round(lat, 5)],
            },
            "properties": {
                "name":   name,
                "symbol": symbol,
                "region": region,
                "source": WRECK_SOURCE_LABEL,
            },
        }
        if fs_id is not None:
            feature["properties"]["fs_id"] = fs_id
        features.append(feature)
    return features
def write_wrecks_json() -> None:
    """
    Parse all source GPX files defined in WRECK_GPX_FILES, combine into a
    single GeoJSON FeatureCollection, and write DailySST/wrecks.json.
    Output schema:
    {
      "type": "FeatureCollection",
      "metadata": {
        "source": "Fishing Status (fishingstatus.com)",
        "gpx_files": [...],
        "regions":   [...],
        "region":    { lat/lon bbox },
        "symbols":   { "Wreck": "...", "Rocks": "..." }
      },
      "feature_count": N,
      "features": [
        {
          "type": "Feature",
          "geometry": { "type": "Point", "coordinates": [lon, lat] },
          "properties": {
            "name":   "...",
            "symbol": "Rocks" | "Wreck",
            "fs_id":  "377565",   // omitted if not present in GPX
            "region": "HatterasNC",
            "source": "Fishing Status (fishingstatus.com)"
          }
        }, ...
      ]
    }
    """
    log.info("Building wrecks.json from %d GPX file(s) ...", len(WRECK_GPX_FILES))
    all_features   = []
    gpx_files_used = []
    for gpx_name, region in WRECK_GPX_FILES.items():
        gpx_path = OUTPUT_DIR / gpx_name
        if not gpx_path.exists():
            log.warning("  GPX not found, skipping: %s", gpx_path)
            continue
        features = _parse_gpx_file(gpx_path, region)
        log.info("  %-40s → %d waypoints  (region: %s)", gpx_name, len(features), region)
        all_features.extend(features)
        gpx_files_used.append(gpx_name)
    if not all_features:
        log.warning("No waypoints found — wrecks.json not written.")
        return
    regions_present = list(dict.fromkeys(          # preserve insertion order, dedupe
        f["properties"]["region"] for f in all_features
    ))
    fc = {
        "type": "FeatureCollection",
        "metadata": {
            "source":    WRECK_SOURCE_LABEL,
            "generated": datetime.datetime.utcnow().isoformat() + "Z",
            "gpx_files": gpx_files_used,
            "regions":   regions_present,
            "region": {
                "lat_min": LAT_MIN,
                "lat_max": LAT_MAX,
                "lon_min": LON_MIN,
                "lon_max": LON_MAX,
            },
            "symbols": WRECK_SYMBOL_DESCRIPTIONS,
        },
        "feature_count": len(all_features),
        "features":      all_features,
    }
    dest = OUTPUT_DIR / "wrecks.json"
    tmp  = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(fc, fh, separators=(",", ":"))
    tmp.rename(dest)
    size_kb = dest.stat().st_size / 1024
    log.info("wrecks.json written: %d features across %d region(s)  (%.1f KB)",
             len(all_features), len(regions_present), size_kb)
# ---------------------------------------------------------------------------
# Bathymetry fetch
# ---------------------------------------------------------------------------
def _parse_erddap_csvp(text: str) -> list[dict]:
    reader = csv.reader(io.StringIO(text))
    rows   = list(reader)[2:]
    data   = []
    for row in rows:
        try:
            lat  = float(row[0])
            lon  = float(row[1])
            elev = float(row[2])
        except (IndexError, ValueError):
            continue
        if elev >= 0:
            data.append({"lat": lat, "lon": lon, "depth_ft": None, "depth_fathoms": None})
        else:
            depth_m       = abs(elev)
            depth_ft      = round(depth_m * 3.28084, 1)
            depth_fathoms = round(depth_m / 1.8288,  2)
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
# ---------------------------------------------------------------------------
def _build_grid(rows: list[dict]) -> tuple[list, list, list]:
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
    log.info("Generating depth contours for %d levels ...", len(CONTOUR_DEPTHS_FT))
    features = []
    for depth_ft in CONTOUR_DEPTHS_FT:
        depth_fathoms  = depth_ft / 6
        label_ft       = f"{depth_ft} ft"
        label_fathoms  = f"{int(depth_fathoms)} fm"
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
                    "depth_ft":      depth_ft,
                    "depth_fathoms": int(depth_fathoms),
                    "label_ft":      label_ft,
                    "label_fathoms": label_fathoms,
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
#
# Size-optimized schema (v2):
#   • depth_ft is the only depth grid. Fathoms are NOT stored — consumers
#     derive them as depth_ft / 6 (1 fathom = 6 ft exactly).
#   • depth_ft values are rounded to the nearest integer foot. GEBCO's
#     intrinsic accuracy is much coarser than 1 ft, so this is lossless in
#     practice and ~halves the serialized byte count vs. 1-decimal floats.
#   • lats / lons are rounded to 5 decimal places (~1.1 m precision).
#   • JSON is written compact (no indent, no whitespace).
#   • math.isnan cells are serialized as null (= land or no data).
#
# Together these changes cut bathymetry_grid.json from ~23.6 MB to roughly
# one quarter of that size (drop fathoms grid ~50% + int-round depths ~30-50%
# of the remainder).
# ---------------------------------------------------------------------------
def write_bathymetry_grid(lats: list, lons: list, grid: list) -> None:
    log.info("Writing bathymetry grid JSON ...")
    grid_ft = []
    for row in grid:
        ft_row = []
        for cell in row:
            if math.isnan(cell):
                ft_row.append(None)
            else:
                ft_row.append(int(round(cell)))
        grid_ft.append(ft_row)
    res_lat = round(lats[1] - lats[0], 6) if len(lats) > 1 else None
    res_lon = round(lons[1] - lons[0], 6) if len(lons) > 1 else None
    payload = {
        "meta": {
            "generated_utc":       (datetime.datetime.now(datetime.timezone.utc)
                                    .isoformat(timespec="seconds")
                                    .replace("+00:00", "Z")),
            "source":              "GEBCO_2020 (primary) | ETOPO_2022_v1_15s | ETOPO_2022_v1_60s",
            "schema_version":      BATHY_GRID_SCHEMA_VERSION,
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
                "depth_ft": "feet below surface, rounded to nearest integer (positive = deeper); null = land or no data",
            },
            "fathoms_note":        "Fathoms are not stored. Derive client-side: depth_fathoms = depth_ft / 6 (1 fathom = 6 ft exactly).",
            "contour_depths_ft":   CONTOUR_DEPTHS_FT,
            "shelf_break_ft":      SHELF_BREAK_FT,
            "shelf_break_fathoms": int(SHELF_BREAK_FT / 6),
        },
        "lats":     [round(v, 5) for v in lats],
        "lons":     [round(v, 5) for v in lons],
        "depth_ft": grid_ft,
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
PAD = 0.5
def _pt_in_bbox(lon: float, lat: float) -> bool:
    return (LON_MIN - PAD <= lon <= LON_MAX + PAD and
            LAT_MIN - PAD <= lat <= LAT_MAX + PAD)
def _ring_intersects_bbox(ring: list) -> bool:
    return any(_pt_in_bbox(pt[0], pt[1]) for pt in ring)
def _clip_linestring(coords: list) -> list[list]:
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
                continue
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
    # ── Wrecks / fishing spots ───────────────────────────────────────────────
    # Always rebuild — source GPX files can change between runs.
    log.info("=== Wrecks ===")
    write_wrecks_json()
    log.info("=== Done. ===")
if __name__ == "__main__":
    main()
