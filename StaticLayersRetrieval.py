"""
StaticLayersRetrieval.py
========================
Fetches:
- Bathymetry (NCEI Coastal Relief Model 2023, 1 arc-second, via OPeNDAP)
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
  bathymetry_grid.json      — Raw 2D depth grid for feature detection
  noaa_coastline.json       — GeoJSON LineStrings (Natural Earth 10m)
  landmask.json             — GeoJSON Polygons / MultiPolygons (Natural Earth 10m)
  wrecks.json               — GeoJSON FeatureCollection from source GPX files
"""
import datetime
import os
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
_REGION_CONFIGS = {
    "mid_atlantic": {"lat_min": 33.70, "lat_max": 39.00, "lon_min": -78.89, "lon_max": -72.21, "suffix": ""},
    "ga_sc":        {"lat_min": 29.80, "lat_max": 35.20, "lon_min": -82.00, "lon_max": -75.20, "suffix": "_ga_sc"},
}
_REGION = os.environ.get("REGION", "mid_atlantic").strip()
if _REGION not in _REGION_CONFIGS:
    print(f"WARNING: Unknown REGION={_REGION!r}, falling back to mid_atlantic")
    _REGION = "mid_atlantic"
_RCFG = _REGION_CONFIGS[_REGION]
_BATHY_SUFFIX = _RCFG["suffix"]

LAT_MIN = _RCFG["lat_min"]
LAT_MAX = _RCFG["lat_max"]
LON_MIN = _RCFG["lon_min"]
LON_MAX = _RCFG["lon_max"]
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
# Wrecks / fishing spots — GPX source files
#
# Keys   : filename (relative to OUTPUT_DIR)
# Values : region label used in wrecks.json properties
#
# To add a new region: drop the GPX file into DailySST/ and add an entry here.
# wrecks.json is always rebuilt on every run (no cache check).
# ---------------------------------------------------------------------------
WRECK_GPX_FILES = {
    "Fishing_Spots_HatterasNC.gpx":   "HatterasNC",
    "Fishing_Spots_MoreheadNC.gpx":   "MoreheadNC",
    "Fishing_spots_ChesapeakeMD.gpx": "ChesapeakeMD",
    "Fishing_Spots_OceanCityMD.gpx":  "OceanCityMD",
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
NE_OCEAN_URL     = f"{NE_BASE}/ne_10m_ocean.geojson"
# ---------------------------------------------------------------------------
# Contour depth levels — fathom-aligned for offshore fishing
# 1 fathom = 6 feet exactly
# ---------------------------------------------------------------------------
CONTOUR_DEPTHS_FT = [60, 120, 180, 300, 600, 1200, 1800, 3000, 6000]
SHELF_BREAK_FT = 1200   # 200 fathoms — flagged in contour properties
# ---------------------------------------------------------------------------
# CRM bathymetry source — NCEI Coastal Relief Model 2023 via OPeNDAP
# Resolution: 1 arc-second (~30 m).  Stride 15 → ~450 m effective spacing.
# Variable z: meters, positive-up (negative = ocean depth, positive = land).
# ---------------------------------------------------------------------------
_CRM_BASE = "https://www.ngdc.noaa.gov/thredds/dodsC/crm/cudem/"
_CRM_STRIDE = 15   # 1 arc-sec × 15 = ~450 m; matches old GEBCO stride=1

# (filename, lat_min, lat_max, lon_min, lon_max)
_CRM_VOLUMES = [
    ("crm_vol1_2023.nc", 39.0, 46.0, -77.0, -65.0),  # NE Atlantic
    ("crm_vol2_2023.nc", 32.0, 39.0, -83.0, -68.0),  # SE Atlantic
    ("crm_vol3_2023.nc", 24.0, 32.0, -84.0, -76.0),  # FL / E Gulf
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
        OUTPUT_DIR / f"bathymetry_contours{_BATHY_SUFFIX}.json",
        OUTPUT_DIR / f"bathymetry_grid{_BATHY_SUFFIX}.json",
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
    if path.exists():
        log.info("%s exists — skipping fetch. (Delete to force refresh.)", path.name)
        return True
    return False
# ---------------------------------------------------------------------------
# Wrecks — GPX parsing and JSON output
# ---------------------------------------------------------------------------
_GPX_NS = {"gpx": "http://www.topografix.com/GPX/1/1"}

# Matches plain-text DMS lines: "Name  DD°/o MM' SS[.S]" N  DD°/o MM' SS[.S]" W"
import re as _re
_DMS_RE = _re.compile(
    r'^(.+?)\s+'
    r'(\d+)[°oO]\s*(\d+)\'\s*(\d+(?:\.\d+)?)"?\s*N\s+'
    r'(\d+)[°oO]\s*(\d+)\'\s*(\d+(?:\.\d+)?)"?\s*W\s*$'
)

def _dms_to_dd(deg: str, mins: str, secs: str) -> float:
    return float(deg) + float(mins) / 60.0 + float(secs) / 3600.0

def _parse_text_dms_file(path: pathlib.Path, region: str) -> list[dict]:
    """
    Parse a plain-text file where each line is:
        Name  DD° MM' SS[.S]" N  DD° MM' SS[.S]" W
    Degree symbol may be °, o, or O. Lines without valid coordinates are skipped.
    All waypoints are assigned symbol "Wreck" (no symbol field in this format).
    """
    features = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            m = _DMS_RE.match(line)
            if not m:
                log.debug("  Skipping unparseable line in %s: %r", path.name, line[:80])
                continue
            name, lat_d, lat_m, lat_s, lon_d, lon_m, lon_s = m.groups()
            lat = round(_dms_to_dd(lat_d, lat_m, lat_s), 5)
            lon = round(_dms_to_dd(lon_d, lon_m, lon_s), 5)
            lon = -lon  # file is always W longitude
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "name":   name.strip(),
                    "symbol": "Wreck",
                    "region": region,
                    "source": WRECK_SOURCE_LABEL,
                },
            })
    return features


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
    except ET.ParseError:
        # Not valid XML — try plain-text DMS format
        log.info("  %s is not XML; attempting plain-text DMS parse", path.name)
        return _parse_text_dms_file(path, region)

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
def _fetch_bathymetry(session: requests.Session) -> list[dict]:
    """
    Fetch bathymetry from NCEI CRM 2023 via OPeNDAP (netCDF4).
    Iterates over the CRM volumes that overlap the current region, fetches
    the z grid at _CRM_STRIDE resolution, and returns rows in the same
    {lat, lon, depth_ft, depth_fathoms} format used by the rest of the pipeline.
    """
    import netCDF4 as nc4
    import numpy as np

    log.info("Fetching CRM bathymetry (stride=%d, ~%d m resolution) ...",
             _CRM_STRIDE, _CRM_STRIDE * 30)
    rows: list[dict] = []

    for vol_file, v_lat_min, v_lat_max, v_lon_min, v_lon_max in _CRM_VOLUMES:
        olat_min = max(LAT_MIN, v_lat_min)
        olat_max = min(LAT_MAX, v_lat_max)
        olon_min = max(LON_MIN, v_lon_min)
        olon_max = min(LON_MAX, v_lon_max)
        if olat_min >= olat_max or olon_min >= olon_max:
            continue

        url = _CRM_BASE + vol_file
        log.info("  Opening %s (lat %.2f-%.2f, lon %.2f-%.2f) ...",
                 vol_file, olat_min, olat_max, olon_min, olon_max)
        try:
            ds = nc4.Dataset(url)
        except Exception as exc:
            log.warning("  Could not open %s: %s", vol_file, exc)
            continue

        try:
            vol_lats = np.array(ds.variables['lat'][:])
            vol_lons = np.array(ds.variables['lon'][:])

            lat_idx = np.where(
                (vol_lats >= olat_min - 1e-7) & (vol_lats <= olat_max + 1e-7)
            )[0]
            lon_idx = np.where(
                (vol_lons >= olon_min - 1e-7) & (vol_lons <= olon_max + 1e-7)
            )[0]
            if len(lat_idx) == 0 or len(lon_idx) == 0:
                log.warning("  No data in %s for this region.", vol_file)
                continue

            li0, li1 = int(lat_idx[0]),  int(lat_idx[-1])
            lo0, lo1 = int(lon_idx[0]),  int(lon_idx[-1])

            z_raw = ds.variables['z'][li0:li1+1:_CRM_STRIDE,
                                      lo0:lo1+1:_CRM_STRIDE]
            fetch_lats = vol_lats[li0:li1+1:_CRM_STRIDE]
            fetch_lons = vol_lons[lo0:lo1+1:_CRM_STRIDE]
        finally:
            ds.close()

        # Unmask / clean fill values
        if hasattr(z_raw, 'filled'):
            z_arr = z_raw.filled(np.nan).astype(float)
        else:
            z_arr = np.asarray(z_raw, dtype=float)
        z_arr[z_arr <= -99990.0] = np.nan

        n_lat, n_lon = z_arr.shape
        log.info("  Got %d × %d = %d cells from %s",
                 n_lat, n_lon, n_lat * n_lon, vol_file)

        lat_rep = np.repeat(fetch_lats, n_lon)
        lon_rep = np.tile(fetch_lons, n_lat)
        z_flat  = z_arr.ravel()

        for lat, lon, z in zip(lat_rep, lon_rep, z_flat):
            lat = round(float(lat), 7)
            lon = round(float(lon), 7)
            if np.isnan(z) or z >= 0:
                rows.append({"lat": lat, "lon": lon,
                             "depth_ft": None, "depth_fathoms": None})
            else:
                depth_m = float(-z)
                rows.append({"lat": lat, "lon": lon,
                             "depth_ft":      round(depth_m * 3.28084, 1),
                             "depth_fathoms": round(depth_m / 1.8288,  2)})

    if not rows:
        raise RuntimeError("CRM fetch returned no data — check OPeNDAP connectivity.")
    ocean = sum(1 for r in rows if r["depth_ft"] is not None)
    log.info("CRM: total %d points (%d ocean).", len(rows), ocean)
    return rows

# ---------------------------------------------------------------------------
# Ocean mask — retired (Natural Earth 10m polygon approach)
# ---------------------------------------------------------------------------
# Replaced by GEBCO-sign land mask in _build_grid (see step 1 above).
# GEBCO rows with elev >= 0 are land (depth_ft=None); those cells are tracked
# in a land[] boolean array and skipped by gap-fill.
def _point_in_ring(lon: float, lat: float, ring: list) -> bool:
    """Standard ray-casting point-in-polygon test for a single ring."""
    inside = False
    n      = len(ring)
    j      = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > lat) != (yj > lat)) and (
            lon < (xj - xi) * (lat - yi) / (yj - yi) + xi
        ):
            inside = not inside
        j = i
    return inside


# ---------------------------------------------------------------------------
# Grid builder
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Morphological open-ocean mask
# ---------------------------------------------------------------------------
def _morphological_open_ocean(land: list[bool], n_rows: int, n_cols: int,
                               radius: int = 6) -> list[bool]:
    """
    Compute open-ocean cells via morphological opening (erode then dilate).

    Narrow water bodies (sounds, bays, rivers) that are < 2*radius cells wide
    are removed.  The open ocean is preserved with its original shape.

    radius=6 at 450 m GEBCO resolution ≈ 2.7 km — closes off Bogue Sound,
    White Oak River, etc. while keeping the offshore 10-fathom contour zone.

    Uses BFS for O(n_cells) performance (not point-in-polygon).
    """
    from collections import deque
    N = n_rows * n_cols

    # ── Step 1: BFS distance-from-land for every cell ───────────────────────
    dist_land = [-1] * N          # -1 = unvisited ocean; land cells get 0
    q: deque = deque()
    for i, is_land in enumerate(land):
        if is_land:
            dist_land[i] = 0
            q.append(i)
    while q:
        i = q.popleft()
        row, col = divmod(i, n_cols)
        d = dist_land[i] + 1
        for dr, dc in ((-1, 0), (1, 0), (0, -1), (0, 1)):
            nr, nc = row + dr, col + dc
            if 0 <= nr < n_rows and 0 <= nc < n_cols:
                j = nr * n_cols + nc
                if dist_land[j] == -1:      # unvisited ocean
                    dist_land[j] = d
                    q.append(j)

    # ── Step 2: Eroded ocean = cells with dist_land > radius ────────────────
    # (cells in narrow water bodies are too close to land and disappear)

    # ── Step 3: BFS from eroded-ocean cells up to radius steps ──────────────
    # Dilate back so open ocean near shore is restored.
    dist_open = [-1] * N
    q2: deque = deque()
    for i in range(N):
        if dist_land[i] > radius:           # survived erosion
            dist_open[i] = 0
            q2.append(i)
    while q2:
        i = q2.popleft()
        if dist_open[i] >= radius:
            continue
        row, col = divmod(i, n_cols)
        d = dist_open[i] + 1
        for dr, dc in ((-1, 0), (1, 0), (0, -1), (0, 1)):
            nr, nc = row + dr, col + dc
            if 0 <= nr < n_rows and 0 <= nc < n_cols:
                j = nr * n_cols + nc
                if dist_open[j] == -1 and not land[j]:
                    dist_open[j] = d
                    q2.append(j)

    # open ocean = reachable from an eroded-ocean seed within radius steps
    return [dist_open[i] >= 0 for i in range(N)]

def _build_grid(rows: list[dict]) -> tuple[list, list, list]:
    lats    = sorted(set(r["lat"] for r in rows))
    lons    = sorted(set(r["lon"] for r in rows))
    lat_idx = {v: i for i, v in enumerate(lats)}
    lon_idx = {v: i for i, v in enumerate(lons)}
    n_rows  = len(lats)
    n_cols  = len(lons)
    flat = [math.nan] * (n_rows * n_cols)
    # ── GEBCO-sign land mask ─────────────────────────────────────────────────
    # GEBCO encodes land via elevation sign: elev >= 0 -> depth_ft=None (land).
    # Build a boolean land[] array so gap-fill never assigns an ocean depth to a
    # land cell (which would cause contourpy to draw contours over dry land).
    # Replaces the previous NE-polygon _point_in_ring approach, which timed out
    # (45+ min) on a 2 M-cell grid vs a 50 K-vertex Atlantic Ocean polygon.
    land = [True] * (n_rows * n_cols)   # default True; flipped False for ocean cells
    for r in rows:
        idx = lat_idx[r["lat"]] * n_cols + lon_idx[r["lon"]]
        if r["depth_ft"] is not None:
            flat[idx] = r["depth_ft"]
            land[idx] = False           # confirmed ocean cell
    land_count = sum(land)
    log.info("GEBCO land mask: %d land cell(s), %d ocean cell(s)",
             land_count, n_rows * n_cols - land_count)
    # ── Morphological open-ocean mask ────────────────────────────────────────
    # Erode then dilate the ocean mask to remove narrow water bodies (sounds,
    # bays, rivers) that GEBCO correctly assigns depth to but where we do not
    # want bathymetric contours (e.g. Bogue Sound, White Oak River).
    MORPH_RADIUS = 6   # ~2.7 km at 450 m GEBCO resolution
    open_ocean = _morphological_open_ocean(land, n_rows, n_cols, MORPH_RADIUS)
    open_count = sum(open_ocean)
    log.info("Open-ocean mask (r=%d): %d open-ocean cell(s), %d enclosed/land cell(s)",
             MORPH_RADIUS, open_count, n_rows * n_cols - open_count)
    # Pin land + enclosed water bodies to NaN so contourpy never draws there
    for i in range(n_rows * n_cols):
        if not open_ocean[i]:
            flat[i] = math.nan
    # ── Gap-fill (open-ocean data voids only) ───────────────────────────────
    for _ in range(6):
        new_flat = flat[:]
        changed  = False
        for row in range(n_rows):
            for col in range(n_cols):
                i = row * n_cols + col
                if not open_ocean[i] or not math.isnan(flat[i]):
                    continue            # skip non-open-ocean and already-filled cells
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
def _split_vertical_runs(coords: list,
                          win: int = 25,
                          max_lon_var: float = 0.012,
                          min_lat_span: float = 0.04) -> list[list]:
    """
    Split a contour line at any nearly-vertical sub-section: a window of
    consecutive points that spans > min_lat_span in latitude but < max_lon_var
    in longitude.  Real shelf isobaths have natural horizontal variation; a run
    this narrow in longitude is a grid-anomaly artifact (single-column spike).

    Returns a list of valid sub-segments (may be just [coords] if no spike found).
    """
    n = len(coords)
    if n < win * 2:
        return [coords]
    spike = [False] * n
    for start in range(n - win):
        end = start + win
        lons_w = [coords[i][0] for i in range(start, end)]
        lats_w = [coords[i][1] for i in range(start, end)]
        if (max(lons_w) - min(lons_w) < max_lon_var
                and max(lats_w) - min(lats_w) > min_lat_span):
            for i in range(start, end):
                spike[i] = True
    segments: list[list] = []
    current: list = []
    for i, c in enumerate(coords):
        if spike[i]:
            if len(current) >= 6:
                segments.append(current)
            current = []
        else:
            current.append(c)
    if len(current) >= 6:
        segments.append(current)
    return segments if segments else [coords]



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
        # Reject spike artifacts: single-column/row grid noise produces
        # (single column/row of bad depth values -> long zero-width contour).
        # contourpy: x=lons, y=lats so p[0]=lon, p[1]=lat.
        xs = [p[0] for p in line]; ys = [p[1] for p in line]
        lon_span = max(xs) - min(xs); lat_span = max(ys) - min(ys)
        min_span = min(lon_span, lat_span); max_span = max(lon_span, lat_span)
        if min_span < 0.033 and max_span > 0.1:
            continue
        coords = [[float(p[0]), float(p[1])] for p in line]
        coords = _chaikin_smooth(coords, iterations=2)
        # Sub-section spike filter
        for seg in _split_vertical_runs(coords):
            if len(seg) >= MIN_POINTS:
                output.append(seg)
    return output

def write_bathymetry_points(rows: list) -> None:
    """Write bathymetry_{suffix}.json — flat points list used for depth lookup in frontend."""
    log.info("Writing bathymetry points JSON (%d rows) ...", len(rows))
    ocean_pts = [r for r in rows if r.get("depth_ft") is not None]
    actual = {
        "lat_min": min(r["lat"] for r in ocean_pts) if ocean_pts else LAT_MIN,
        "lat_max": max(r["lat"] for r in ocean_pts) if ocean_pts else LAT_MAX,
        "lon_min": min(r["lon"] for r in ocean_pts) if ocean_pts else LON_MIN,
        "lon_max": max(r["lon"] for r in ocean_pts) if ocean_pts else LON_MAX,
    }
    payload = {
        "dataset":       "GEBCO_2020 (primary) | ETOPO_2022_v1_15s | ETOPO_2022_v1_60s",
        "source":        "ERDDAP griddap",
        "resolution":    f"stride={BATHY_STRIDE} (~{BATHY_STRIDE * 450:.0f} m)",
        "stride":        BATHY_STRIDE,
        "units":         {"depth_ft": "feet below surface; null = land/no data"},
        "region":        {"lat_min": LAT_MIN, "lat_max": LAT_MAX,
                          "lon_min": LON_MIN, "lon_max": LON_MAX},
        "actual_extent": actual,
        "point_count":   len(ocean_pts),
        "points":        [{"lat": round(r["lat"], 6),
                           "lon": round(r["lon"], 6),
                           "depth_ft": round(r["depth_ft"], 1)} for r in ocean_pts],
    }
    dest = OUTPUT_DIR / f"bathymetry{_BATHY_SUFFIX}.json"
    tmp  = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    tmp.rename(dest)
    log.info("Bathymetry points written: %d ocean points  (%.1f KB)",
             len(ocean_pts), dest.stat().st_size / 1024)

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
    dest = OUTPUT_DIR / f"bathymetry_contours{_BATHY_SUFFIX}.json"
    tmp  = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"type": "FeatureCollection", "features": features}, fh,
                  separators=(",", ":"))
    tmp.rename(dest)
    log.info("Contours written: %d features across %d depth levels  (%.1f KB)",
             len(features), len(CONTOUR_DEPTHS_FT), dest.stat().st_size / 1024)
# ---------------------------------------------------------------------------
# Bathymetry grid output
# ---------------------------------------------------------------------------
def write_bathymetry_grid(lats: list, lons: list, grid: list) -> None:
    log.info("Writing bathymetry grid JSON ...")
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
        },
        "lats":          [round(v, 5) for v in lats],
        "lons":          [round(v, 5) for v in lons],
        "depth_ft":      grid_ft,
        "depth_fathoms": grid_fathoms,
    }
    dest = OUTPUT_DIR / f"bathymetry_grid{_BATHY_SUFFIX}.json"
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
    # Small / medium rings: any vertex inside the padded bbox
    if any(_pt_in_bbox(pt[0], pt[1]) for pt in ring):
        return True
    # Large containing rings (e.g. Atlantic Ocean polygon): the entire
    # bbox sits inside the ring but no vertices are near it.
    # Check whether the bbox centre is enclosed by this ring.
    centre_lon = (LON_MIN + LON_MAX) / 2
    centre_lat = (LAT_MIN + LAT_MAX) / 2
    return _point_in_ring(centre_lon, centre_lat, ring)
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
        write_bathymetry_points(rows)
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
