"""
FishingHotspotAnalyzer.py
=========================
Scores ocean grid points for target-species fishing probability by fusing:
  • SST            — VIIRS composite (DailySSTData/VIIRS/Bundled/viirs_composite.json)
  • Temp breaks    — SST gradient magnitude computed from composite
  • Bathymetry     — depth grid (DailySST/bathymetry_grid.json)
  • Chlorophyll-a  — local repo files (SSTv2/Chlorophyll/CHL_YYYYMMDD.json)
  • Sea color Kd490— local repo files (SSTv2/SeaColor/SEACOLOR_YYYYMMDD.json)
Species habitat parameters live in species_config.json — edit and push to
GitHub to tune without touching Python.
Output: DailySST/fishing_hotspots_YYYY-MM-DD.json
Usage:
  python FishingHotspotAnalyzer.py
  SPECIES=yellowfin python FishingHotspotAnalyzer.py   # single species only
  DATE=2026-05-20   python FishingHotspotAnalyzer.py   # specific date
  SKIP_CHL=1        python FishingHotspotAnalyzer.py   # skip CHL/kd490 data
Dependencies:
  pip install numpy --break-system-packages
  scipy is used for convex hull if available; falls back to bounding polygon.
"""
import datetime
import json
import logging
import math
import os
import pathlib
import sys
from collections import deque
import numpy as np
# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
_ROOT        = pathlib.Path(__file__).resolve().parent
VIIRS_DIR    = _ROOT / "DailySSTData" / "VIIRS" / "Bundled"   # composite input
STATIC_DIR   = _ROOT / "DailySST"                              # bathymetry input
OUTPUT_DIR   = _ROOT / "DailySST"                              # hotspot output
CONFIG_PATH  = _ROOT / "species_config.json"
CHL_DIR      = _ROOT / "SSTv2" / "Chlorophyll"                # local CHL files
SEACOLOR_DIR = _ROOT / "SSTv2" / "SeaColor"                   # local kd490 files
LAT_MIN = 33.70
LAT_MAX = 39.00
LON_MIN = -78.89
LON_MAX = -72.21
# Grid resolution for scoring (degrees) — matches composite resolution ~0.02°
SCORE_GRID_RES = 0.04   # coarser than composite for speed; interpolated from composite
# Clustering: minimum cells in a zone, score threshold for hot cells
CLUSTER_MIN_CELLS = 8        # ~0.04° × 0.04° × 8 cells ≈ 15 nm² minimum
CLUSTER_SCORE_THRESH = 0.50  # per-cell threshold before weighting
# Break detection: gradient magnitude °F per degree of lat/lon
BREAK_WEAK_THRESHOLD     = 0.4   # °F/° — minimal front
BREAK_MODERATE_THRESHOLD = 0.8   # °F/° — defined front
BREAK_STRONG_THRESHOLD   = 1.5   # °F/° — sharp front
# How far back to search for local CHL/kd490 files (days)
CHL_LOOKBACK_DAYS = 10
KEEP_HOTSPOT_DAYS = 7  # purge hotspot files older than this
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)
# ─────────────────────────────────────────────────────────────────────────────
# Data loaders
# ─────────────────────────────────────────────────────────────────────────────
def load_composite(date: datetime.date) -> dict | None:
    """Load the VIIRS composite JSON (always the latest, rebuilt each bundler run)."""
    path = VIIRS_DIR / "viirs_composite.json"
    if not path.exists():
        log.warning("viirs_composite.json not found — run VIIRSHourlyBundler.py first.")
        return None
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    log.info("Composite loaded: %d lat × %d lon, coverage %.1f%%",
             len(data["latSet"]), len(data["lonSet"]),
             data.get("coverage_pct", 0))
    return data
def load_bathymetry_grid() -> dict | None:
    """Load the pre-built bathymetry grid JSON."""
    path = STATIC_DIR / "bathymetry_grid.json"
    if not path.exists():
        log.warning("bathymetry_grid.json not found — run StaticLayersRetrieval.py first.")
        return None
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    log.info("Bathymetry grid loaded: %d × %d cells",
             len(data["lats"]), len(data["lons"]))
    return data
def _load_rows_json(path: pathlib.Path, value_key: str,
                    key_precision: int = 2) -> dict:
    """
    Parse a point-list JSON file produced by DailyChlorophyllandSeaColorRetrieval.py.
    Format:
      { ..., "rows": [{"lat": f, "lon": f, "<value_key>": f|null, ...}, ...] }
    key_precision controls rounding of lat/lon keys:
      2  (0.01°)  — for CHL at ~0.022° grid spacing
      1  (0.1°)   — for SEACOLOR at ~0.167° grid spacing
    Returns: {(lat_rounded, lon_rounded) -> float}  — valid (non-null, >0) points only.
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    rows   = data.get("rows", [])
    result = {}
    for row in rows:
        val = row.get(value_key)
        if val is None:
            continue
        try:
            fv = float(val)
        except (TypeError, ValueError):
            continue
        if math.isnan(fv) or fv <= 0:
            continue
        lat = round(float(row["lat"]), key_precision)
        lon = round(float(row["lon"]), key_precision)
        result[(lat, lon)] = fv
    return result
# Map color_class string → approximate kd490 value (used as kd490 proxy from CHL file)
_COLOR_CLASS_KD490 = {
    "blue_water":  0.06,
    "mixed":       0.10,
    "green_water": 0.20,
}
def _load_color_class_as_kd490(path: pathlib.Path) -> dict:
    """
    Extract color_class from a CHL rows file and convert to approximate kd490.
    Used as fallback when no dedicated SEACOLOR file is available.
    CHL is at ~0.022° grid → use 2dp keys.
    Returns: {(lat_2dp, lon_2dp) -> kd490_approx}
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    rows   = data.get("rows", [])
    result = {}
    for row in rows:
        cc = row.get("color_class")
        if cc not in _COLOR_CLASS_KD490:
            continue
        lat = round(float(row["lat"]), 2)
        lon = round(float(row["lon"]), 2)
        result[(lat, lon)] = _COLOR_CLASS_KD490[cc]
    return result
_SEACOLOR_KEY_PRECISION = 1   # SEACOLOR grid ~0.167° → snap to 0.1°
def load_local_chl(date: datetime.date) -> dict:
    """
    Load chlorophyll from local repo file: SSTv2/Chlorophyll/CHL_YYYYMMDD.json
    Searches backwards up to CHL_LOOKBACK_DAYS for the most recent available file.
    Returns {(lat_2dp, lon_2dp) -> chl_mg_m3}
    """
    for delta in range(CHL_LOOKBACK_DAYS + 1):
        target = date - datetime.timedelta(days=delta)
        fname  = f"CHL_{target.strftime('%Y%m%d')}.json"
        path   = CHL_DIR / fname
        if path.exists():
            try:
                lookup = _load_rows_json(path, "chlorophyll")
                log.info("Loaded CHL from %s  (%d valid points, %d days old)",
                         fname, len(lookup), delta)
                return lookup
            except Exception as exc:
                log.warning("Failed to parse %s: %s — trying older file.", fname, exc)
    log.warning("No local CHL file found within %d days — CHL scoring will be neutral.",
                CHL_LOOKBACK_DAYS)
    return {}
def load_local_kd490(date: datetime.date) -> dict:
    """
    Load sea color Kd490 from local repo: SSTv2/SeaColor/SEACOLOR_YYYYMMDD.json
    Searches backwards up to CHL_LOOKBACK_DAYS for the most recent available file.
    Falls back to color_class-derived kd490 from the CHL file if no SEACOLOR file exists.
    Returns {(lat_2dp, lon_2dp) -> kd490}
    """
    for delta in range(CHL_LOOKBACK_DAYS + 1):
        target = date - datetime.timedelta(days=delta)
        fname  = f"SEACOLOR_{target.strftime('%Y%m%d')}.json"
        path   = SEACOLOR_DIR / fname
        if path.exists():
            try:
                lookup = _load_rows_json(path, "kd490",
                                         key_precision=_SEACOLOR_KEY_PRECISION)
                log.info("Loaded kd490 from %s  (%d valid points, %d days old)",
                         fname, len(lookup), delta)
                return lookup
            except Exception as exc:
                log.warning("Failed to parse %s: %s — trying older file.", fname, exc)
    # Fallback: derive kd490 approximation from color_class in the CHL file
    log.info("No SEACOLOR file found — deriving kd490 from color_class in CHL file.")
    for delta in range(CHL_LOOKBACK_DAYS + 1):
        target = date - datetime.timedelta(days=delta)
        fname  = f"CHL_{target.strftime('%Y%m%d')}.json"
        path   = CHL_DIR / fname
        if path.exists():
            try:
                lookup = _load_color_class_as_kd490(path)
                log.info("  kd490 proxy from %s  (%d color_class points, %d days old)",
                         fname, len(lookup), delta)
                return lookup
            except Exception as exc:
                log.warning("  Failed to read color_class from %s: %s", fname, exc)
    log.warning("No kd490 source found — kd490 scoring will be neutral.")
    return {}
# ─────────────────────────────────────────────────────────────────────────────
# Composite → scored grid helpers
# ─────────────────────────────────────────────────────────────────────────────
def build_composite_lookup(composite: dict) -> dict:
    """
    Convert flat composite arrays to a (lat, lon) → sst_f dict.
    composite.latSet is ascending (south→north).
    """
    lat_set = composite["latSet"]
    lon_set = composite["lonSet"]
    sst_arr = composite["sst"]
    n_lon   = len(lon_set)
    grid    = {}
    for li, lat in enumerate(lat_set):
        for loi, lon in enumerate(lon_set):
            val = sst_arr[li * n_lon + loi]
            if val is not None and val > 0:
                grid[(round(lat, 3), round(lon, 3))] = val
    return grid
def compute_sst_gradient(composite: dict) -> dict:
    """
    Compute SST gradient magnitude at each grid point using central differences.
    Returns dict: {(lat, lon) -> gradient °F per degree lat/lon}
    """
    lat_set = composite["latSet"]   # ascending
    lon_set = composite["lonSet"]   # ascending
    sst_arr = composite["sst"]
    n_lat   = len(lat_set)
    n_lon   = len(lon_set)
    d_lat   = (lat_set[-1] - lat_set[0]) / max(n_lat - 1, 1)
    d_lon   = (lon_set[-1] - lon_set[0]) / max(n_lon - 1, 1)
    def val(li, loi):
        v = sst_arr[li * n_lon + loi]
        return v if (v is not None and v > 0) else None
    grad = {}
    for li in range(n_lat):
        for loi in range(n_lon):
            v = val(li, loi)
            if v is None:
                continue
            # Central differences; use one-sided at edges
            vN = val(li + 1, loi) if li < n_lat - 1 else None
            vS = val(li - 1, loi) if li > 0 else None
            vE = val(li, loi + 1) if loi < n_lon - 1 else None
            vW = val(li, loi - 1) if loi > 0 else None
            if vN is not None and vS is not None:
                dlat = (vN - vS) / (2 * d_lat)
            elif vN is not None:
                dlat = (vN - v) / d_lat
            elif vS is not None:
                dlat = (v - vS) / d_lat
            else:
                dlat = 0.0
            if vE is not None and vW is not None:
                dlon = (vE - vW) / (2 * d_lon)
            elif vE is not None:
                dlon = (vE - v) / d_lon
            elif vW is not None:
                dlon = (v - vW) / d_lon
            else:
                dlon = 0.0
            magnitude = math.sqrt(dlat ** 2 + dlon ** 2)
            grad[(round(lat_set[li], 3), round(lon_set[loi], 3))] = magnitude
    return grad
def build_bathy_lookup(bathy: dict) -> dict:
    """
    Convert 2D bathymetry grid to (lat_2dp, lon_2dp) → depth_ft dict.
    bathy.lats is ascending; depth_ft is None for land.
    Keys are rounded to 2 decimal places (0.01° ≈ 1 km) to match the composite
    SST grid resolution and allow O(1) lookups in build_score_grid.
    Bathy is at ~0.004° so multiple bathy cells map to each 0.01° bin — the
    last non-null value wins (depth variation within 1km is negligible for scoring).
    """
    lats    = bathy["lats"]
    lons    = bathy["lons"]
    grid_ft = bathy["depth_ft"]
    result  = {}
    for li, lat in enumerate(lats):
        for loi, lon in enumerate(lons):
            val = grid_ft[li][loi]
            if val is not None:
                result[(round(lat, 2), round(lon, 2))] = val
    return result
# ─────────────────────────────────────────────────────────────────────────────
# Sub-score functions (all return 0.0 – 1.0)
# ─────────────────────────────────────────────────────────────────────────────
def score_sst(sst_f: float, target: float, sst_min: float, sst_max: float) -> float:
    """Gaussian peak at target, zero outside [sst_min, sst_max]."""
    if sst_f < sst_min or sst_f > sst_max:
        return 0.0
    sigma = (sst_max - sst_min) / 4.0
    return math.exp(-0.5 * ((sst_f - target) / sigma) ** 2)
def score_break(gradient_mag: float | None) -> float:
    """Score 0–1 based on gradient magnitude. None = 0.3 (neutral)."""
    if gradient_mag is None:
        return 0.3
    if gradient_mag >= BREAK_STRONG_THRESHOLD:
        return 1.0
    if gradient_mag >= BREAK_MODERATE_THRESHOLD:
        return 0.75
    if gradient_mag >= BREAK_WEAK_THRESHOLD:
        return 0.45
    return 0.15
def score_depth(depth_ft: float | None, d_min: float, d_max: float,
                d_ideal_min: float, d_ideal_max: float) -> float:
    """1.0 in ideal range, smooth falloff to edges of acceptable range, 0 outside."""
    if depth_ft is None:
        return 0.0
    if depth_ft < d_min or depth_ft > d_max:
        return 0.0
    if d_ideal_min <= depth_ft <= d_ideal_max:
        return 1.0
    # Ramp up from d_min to d_ideal_min
    if depth_ft < d_ideal_min:
        span = max(d_ideal_min - d_min, 1)
        return (depth_ft - d_min) / span
    # Ramp down from d_ideal_max to d_max
    span = max(d_max - d_ideal_max, 1)
    return (d_max - depth_ft) / span
def score_chl(chl: float | None, chl_min: float, chl_max: float) -> float:
    """1.0 inside range, partial credit outside, 0.45 if no data."""
    if chl is None:
        return 0.45   # neutral — no data
    if chl <= 0:
        return 0.1
    if chl_min <= chl <= chl_max:
        return 1.0
    # Partial credit just outside range
    center = (chl_min + chl_max) / 2.0
    spread = (chl_max - chl_min) / 2.0
    dist   = abs(chl - center) - spread
    return max(0.0, 1.0 - dist / spread)
def score_color(kd490: float | None, kd490_max: float) -> float:
    """Bluer water = lower kd490 = better score. 0.50 if no data."""
    if kd490 is None:
        return 0.50
    if kd490 <= kd490_max * 0.5:
        return 1.0
    if kd490 <= kd490_max:
        return 0.75
    # Penalty beyond threshold
    excess = (kd490 - kd490_max) / kd490_max
    return max(0.0, 0.5 - excess * 0.5)
# ─────────────────────────────────────────────────────────────────────────────
# Scoring grid construction
# ─────────────────────────────────────────────────────────────────────────────
def build_score_grid(composite_lookup: dict, gradient_lookup: dict,
                     bathy_lookup: dict, chl_lookup: dict, kd490_lookup: dict,
                     sp: dict) -> list[tuple]:
    """
    Score every composite grid point for one species.

    Hard gates (point skipped entirely, not added to results):
      • SST outside [sst_min, sst_max]
      • Depth is known (bathy available) but outside [depth_min, depth_max]
        — eliminates bay/estuarine water and land-adjacent pixels.
        — points with no bathy data (depth_ft=None) are NOT hard-gated so
          truly offshore areas with patchy bathy coverage still score.

    Returns list of (lat, lon, score, meta_dict).
    meta_dict includes raw values AND per-factor sub-scores for frontend display.
    """
    results = []
    w = sp["weights"]
    sst_min, sst_max         = sp["sst_range_f"]
    d_min, d_max             = sp["depth_range_ft"]
    d_ideal_min, d_ideal_max = sp["depth_ideal_ft"]
    chl_min, chl_max         = sp["chl_range_mg_m3"]
    kd490_max                = sp["kd490_max"]
    break_required           = sp.get("break_required", False)

    for (lat, lon), sst_f in composite_lookup.items():
        # ── SST hard gate ────────────────────────────────────────────────────
        s_sst = score_sst(sst_f, sp["sst_target_f"], sst_min, sst_max)
        if s_sst == 0.0:
            continue   # out of species SST range — skip

        # ── O(1) overlay lookups ─────────────────────────────────────────────
        lat2      = round(lat, 2)
        lon2      = round(lon, 2)
        lat1      = round(lat, 1)
        lon1      = round(lon, 1)
        gradient  = gradient_lookup.get((lat, lon))
        depth_ft  = bathy_lookup.get((lat2, lon2))   # bathy keyed at 2dp
        chl       = chl_lookup.get((lat2, lon2))      # CHL keyed at 2dp
        kd490_val = kd490_lookup.get((lat1, lon1))    # SEACOLOR keyed at 1dp

        # ── Depth hard gate (known depth only) ───────────────────────────────
        s_depth = score_depth(depth_ft, d_min, d_max, d_ideal_min, d_ideal_max)
        if depth_ft is not None and s_depth == 0.0:
            continue   # wrong depth zone (too shallow/deep) — skip

        # ── Remaining sub-scores ─────────────────────────────────────────────
        s_break = score_break(gradient)
        s_chl   = score_chl(chl, chl_min, chl_max)
        s_color = score_color(kd490_val, kd490_max)

        # ── Weighted total ───────────────────────────────────────────────────
        total = (w["sst"]   * s_sst   +
                 w["break"] * s_break +
                 w["depth"] * s_depth +
                 w["chl"]   * s_chl   +
                 w["color"] * s_color)

        # Cap score if break is required but very weak
        if break_required and s_break < 0.3:
            total = min(0.50, total)

        sc = round(total, 4)

        meta = {
            "sst_f":       round(sst_f, 1),
            "sst_score":   round(s_sst, 3),
            "gradient":    round(gradient, 3) if gradient is not None else None,
            "break_score": round(s_break, 3),
            "depth_ft":    round(depth_ft, 0) if depth_ft is not None else None,
            "depth_score": round(s_depth, 3),
            "chl":         round(chl, 3)       if chl is not None else None,
            "chl_score":   round(s_chl, 3),
            "kd490":       round(kd490_val, 4) if kd490_val is not None else None,
            "color_score": round(s_color, 3),
        }
        results.append((lat, lon, sc, meta))

    return results
# ─────────────────────────────────────────────────────────────────────────────
# Clustering (flood fill on hot cells)
# ─────────────────────────────────────────────────────────────────────────────
def cluster_hot_cells(scored_points: list[tuple],
                      min_score: float,
                      grid_res: float = 0.04) -> list[list[tuple]]:
    """
    Find connected components of cells scoring >= min_score.
    Adjacency: 8-connected (Moore neighborhood) at grid_res degree spacing.
    Returns list of clusters, each cluster is list of (lat, lon, score, meta).
    """
    hot = {(r[0], r[1]): r for r in scored_points if r[2] >= min_score}
    if not hot:
        return []
    visited = set()
    clusters = []
    eps = grid_res * 1.5   # adjacency threshold
    for start_key in hot:
        if start_key in visited:
            continue
        # BFS
        cluster = []
        queue   = deque([start_key])
        visited.add(start_key)
        while queue:
            lat, lon = queue.popleft()
            cluster.append(hot[(lat, lon)])
            # Check 8 neighbors
            for dlat in [-grid_res, 0, grid_res]:
                for dlon in [-grid_res, 0, grid_res]:
                    if dlat == 0 and dlon == 0:
                        continue
                    nlat = round(lat + dlat, 3)
                    nlon = round(lon + dlon, 3)
                    nkey = (nlat, nlon)
                    if nkey in hot and nkey not in visited:
                        # Also accept slight misalignment
                        visited.add(nkey)
                        queue.append(nkey)
        clusters.append(cluster)
    return clusters
def convex_hull_polygon(points: list[tuple]) -> list[list[float]]:
    """
    Compute convex hull of (lat, lon) points.
    Uses scipy if available, otherwise returns bounding polygon.
    Returns list of [lat, lon] coordinate pairs.
    """
    if len(points) < 3:
        return [[p[0], p[1]] for p in points]
    try:
        from scipy.spatial import ConvexHull  # type: ignore
        pts = np.array([[p[0], p[1]] for p in points])
        hull = ConvexHull(pts)
        hull_pts = pts[hull.vertices].tolist()
        hull_pts.append(hull_pts[0])   # close the ring
        return [[round(p[0], 4), round(p[1], 4)] for p in hull_pts]
    except Exception:
        pass
    # Fallback: bounding box padded by 0.05°
    lats = [p[0] for p in points]
    lons = [p[1] for p in points]
    pad  = 0.05
    s, n = min(lats) - pad, max(lats) + pad
    w, e = min(lons) - pad, max(lons) + pad
    return [[s, w], [n, w], [n, e], [s, e], [s, w]]
def _break_label(gradient: float | None) -> str:
    if gradient is None or gradient < BREAK_WEAK_THRESHOLD:
        return "none"
    if gradient < BREAK_MODERATE_THRESHOLD:
        return "weak"
    if gradient < BREAK_STRONG_THRESHOLD:
        return "moderate"
    return "strong"
def zone_from_cluster(cluster: list[tuple], rank: int) -> dict:
    """Summarize a cluster into a zone dict for output JSON."""
    scores = [r[2] for r in cluster]
    metas  = [r[3] for r in cluster]
    best_idx = scores.index(max(scores))
    best_meta = metas[best_idx]
    best_lat  = cluster[best_idx][0]
    best_lon  = cluster[best_idx][1]
    # Compute approximate area in sq nautical miles
    lats = [r[0] for r in cluster]
    lons = [r[1] for r in cluster]
    lat_span_nm  = (max(lats) - min(lats)) * 60
    lon_span_nm  = (max(lons) - min(lons)) * 60 * math.cos(math.radians(sum(lats) / len(lats)))
    area_sq_nm   = round(lat_span_nm * lon_span_nm, 1)
    polygon = convex_hull_polygon([(r[0], r[1]) for r in cluster])
    # Centroid weighted by score
    w_sum = sum(scores)
    center_lat = round(sum(r[0] * r[2] for r in cluster) / w_sum, 4)
    center_lon = round(sum(r[1] * r[2] for r in cluster) / w_sum, 4)
    return {
        "rank":       rank,
        "score":      round(sum(scores) / len(scores), 3),
        "peak_score": round(max(scores), 3),
        "cell_count": len(cluster),
        "area_sq_nm": area_sq_nm,
        "center":     [center_lat, center_lon],
        "polygon":    polygon,
        "conditions": {
            "sst_f":          best_meta.get("sst_f"),
            "sst_score":      best_meta.get("sst_score"),
            "break_strength": _break_label(best_meta.get("gradient")),
            "break_score":    best_meta.get("break_score"),
            "depth_ft":       best_meta.get("depth_ft"),
            "depth_score":    best_meta.get("depth_score"),
            "chl_mg_m3":      best_meta.get("chl"),
            "chl_score":      best_meta.get("chl_score"),
            "kd490":          best_meta.get("kd490"),
            "color_score":    best_meta.get("color_score"),
        },
    }
# ─────────────────────────────────────────────────────────────────────────────
# Per-species analysis
# ─────────────────────────────────────────────────────────────────────────────
def analyze_species(species_key: str, sp_config: dict,
                    composite_lookup: dict, gradient_lookup: dict,
                    bathy_lookup: dict, chl_lookup: dict,
                    kd490_lookup: dict, composite_step: float = 0.02) -> dict:
    """Run the full scoring pipeline for one species. Returns zones dict."""
    log.info("  Analyzing %s ...", sp_config["display_name"])
    min_score = sp_config.get("min_zone_score", 0.60)
    scored = build_score_grid(
        composite_lookup, gradient_lookup, bathy_lookup,
        chl_lookup, kd490_lookup, sp_config
    )
    hot_count = sum(1 for r in scored if r[2] >= min_score)
    log.info("    %d / %d scored points above %.2f threshold",
             hot_count, len(scored), min_score)
    clusters = cluster_hot_cells(scored, min_score, grid_res=composite_step)
    log.info("    %d raw cluster(s) found", len(clusters))
    # Filter by minimum size, sort by mean score desc
    valid = [c for c in clusters if len(c) >= CLUSTER_MIN_CELLS]
    valid.sort(key=lambda c: sum(r[2] for r in c) / len(c), reverse=True)
    top3 = valid[:3]
    zones = [zone_from_cluster(c, rank=i + 1) for i, c in enumerate(top3)]
    log.info("    → %d zone(s) for %s", len(zones), sp_config["display_name"])
    return {"zones": zones, "grid_points_scored": len(scored), "hot_cells": hot_count}
# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────
def write_hotspots(date: datetime.date, species_results: dict) -> pathlib.Path:
    payload = {
        "generated": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "date":      date.isoformat(),
        "species":   species_results,
    }
    dest = OUTPUT_DIR / f"fishing_hotspots_{date.isoformat()}.json"
    tmp  = dest.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    tmp.rename(dest)
    log.info("Wrote %s  (%.1f KB)", dest.name, dest.stat().st_size / 1024)
    return dest
def purge_old_hotspots(keep_days: int) -> None:
    cutoff = datetime.date.today() - datetime.timedelta(days=keep_days)
    for p in OUTPUT_DIR.glob("fishing_hotspots_????-??-??.json"):
        try:
            file_date = datetime.date.fromisoformat(p.stem.replace("fishing_hotspots_", ""))
            if file_date < cutoff:
                p.unlink()
                log.info("Purged old hotspot file: %s", p.name)
        except ValueError:
            pass
# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    # Date
    date_env = os.environ.get("DATE", "").strip()
    if date_env:
        date = datetime.date.fromisoformat(date_env)
    else:
        date = datetime.date.today()
    log.info("=== FishingHotspotAnalyzer  date=%s ===", date.isoformat())
    # Species filter
    species_filter = os.environ.get("SPECIES", "").strip().lower() or None
    # Load species config
    if not CONFIG_PATH.exists():
        log.error("species_config.json not found at %s", CONFIG_PATH)
        sys.exit(1)
    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)
    all_species = config["species"]
    if species_filter:
        if species_filter not in all_species:
            log.error("Unknown species '%s'. Valid: %s", species_filter, list(all_species))
            sys.exit(1)
        all_species = {species_filter: all_species[species_filter]}
    enabled = {k: v for k, v in all_species.items() if v.get("enabled", True)}
    log.info("Species to analyze: %s", list(enabled))
    # ── Load local data ──────────────────────────────────────────────────────
    log.info("=== Loading local data ===")
    composite = load_composite(date)
    if composite is None:
        log.error("Cannot proceed without composite SST.")
        sys.exit(1)
    bathy = load_bathymetry_grid()
    if bathy is None:
        log.warning("Bathymetry unavailable — depth scoring will be neutral.")
    # Build lookup dicts
    log.info("Building composite lookup ...")
    composite_lookup = build_composite_lookup(composite)
    log.info("  %d ocean SST points", len(composite_lookup))

    # Compute actual composite grid step for clustering adjacency
    lat_set = composite["latSet"]
    lon_set = composite["lonSet"]
    lat_step = (lat_set[-1] - lat_set[0]) / max(len(lat_set) - 1, 1)
    lon_step = (lon_set[-1] - lon_set[0]) / max(len(lon_set) - 1, 1)
    composite_step = min(abs(lat_step), abs(lon_step))
    log.info("  Composite step: %.4f°", composite_step)

    log.info("Computing SST gradient (break detection) ...")
    gradient_lookup = compute_sst_gradient(composite)
    log.info("  %d gradient points", len(gradient_lookup))
    bathy_lookup = {}
    if bathy:
        log.info("Building bathymetry lookup ...")
        bathy_lookup = build_bathy_lookup(bathy)
        log.info("  %d depth points", len(bathy_lookup))
    # ── Load local CHL / SeaColor data ──────────────────────────────────────
    skip_chl     = os.environ.get("SKIP_CHL", "").strip() == "1"
    chl_lookup   = {}
    kd490_lookup = {}
    if not skip_chl:
        log.info("=== Loading chlorophyll from local files ===")
        chl_lookup = load_local_chl(date)
        log.info("=== Loading sea color (kd490) from local files ===")
        kd490_lookup = load_local_kd490(date)
    else:
        log.info("SKIP_CHL=1 — skipping CHL/kd490 data.")
    # ── Analyze each species ─────────────────────────────────────────────────
    log.info("=== Scoring species ===")
    results = {}
    for key, sp in enabled.items():
        try:
            results[key] = analyze_species(
                key, sp,
                composite_lookup, gradient_lookup,
                bathy_lookup, chl_lookup, kd490_lookup,
                composite_step=composite_step
            )
        except Exception as exc:
            log.error("  Failed for %s: %s", key, exc, exc_info=True)
            results[key] = {"zones": [], "error": str(exc)}
    # ── Write output ─────────────────────────────────────────────────────────
    log.info("=== Writing output ===")
    write_hotspots(date, results)
    purge_old_hotspots(KEEP_HOTSPOT_DAYS)
    log.info("=== Done. ===")
if __name__ == "__main__":
    main()
