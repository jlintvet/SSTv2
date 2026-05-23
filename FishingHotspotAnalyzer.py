"""
FishingHotspotAnalyzer.py
=========================
Scores ocean grid points for target-species fishing probability by fusing:
  • SST            — VIIRS composite (DailySSTData/VIIRS/Bundled/viirs_composite.json)
  • Temp breaks    — SST gradient magnitude computed from composite
  • Bathymetry     — depth grid (DailySST/bathymetry_grid.json)
  • Chlorophyll-a  — local repo files (SSTv2/Chlorophyll/CHL_YYYYMMDD.json)
  • Sea color Kd490— local repo files (SSTv2/SeaColor/SEACOLOR_YYYYMMDD.json)
  • Seasonality    — prime_months / peak_months from species_config.json

Species habitat parameters live in species_config.json — edit and push to
GitHub to tune without touching Python.

Output: DailySST/fishing_hotspots_YYYY-MM-DD.json
Each zone includes: habitat_score, seasonal_factor, adjusted_score,
                    in_season, conditions (with sub-scores), narrative.

Usage:
  python FishingHotspotAnalyzer.py
  SPECIES=yellowfin python FishingHotspotAnalyzer.py   # single species only
  DATE=2026-05-20   python FishingHotspotAnalyzer.py   # specific date
  SKIP_CHL=1        python FishingHotspotAnalyzer.py   # skip CHL/kd490 data
  SKIP_NARRATIVE=1  python FishingHotspotAnalyzer.py   # skip AI narrative

Dependencies:
  pip install numpy --break-system-packages
  pip install anthropic --break-system-packages   # optional — for AI narrative
  scipy is used for convex hull if available; falls back to bounding polygon.

Environment variables:
  ANTHROPIC_API_KEY  — if set, generates AI narrative per zone via Claude API
"""
import calendar
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
VIIRS_DIR    = _ROOT / "DailySSTData" / "VIIRS" / "Bundled"
STATIC_DIR   = _ROOT / "DailySST"
OUTPUT_DIR   = _ROOT / "DailySST"
CONFIG_PATH  = _ROOT / "species_config.json"
CHL_DIR      = _ROOT / "SSTv2" / "Chlorophyll"
SEACOLOR_DIR = _ROOT / "SSTv2" / "SeaColor"

LAT_MIN = 33.70
LAT_MAX = 39.00
LON_MIN = -78.89
LON_MAX = -72.21

CLUSTER_MIN_CELLS = 8
CLUSTER_SCORE_THRESH = 0.50

BREAK_WEAK_THRESHOLD     = 0.4   # °C per grid-cell (central-diff magnitude)
BREAK_MODERATE_THRESHOLD = 0.8
BREAK_STRONG_THRESHOLD   = 1.5

CHL_LOOKBACK_DAYS = 10
KEEP_HOTSPOT_DAYS = 7

# CHL lat/lon is at ~0.04° resolution (4 km CMEMS grid).
# We snap incoming coordinates to this bin so lookups work regardless
# of whether the source data has 5-decimal precision like 33.70278.
CHL_SNAP_DEG   = 0.04   # snap CHL lat/lon to nearest 0.04°
BATHY_SNAP_DEG = 0.02   # bathy grid precision

# Seasonal scoring multipliers
SEASONAL_PEAK_MULT  = 1.00   # month is in peak_months
SEASONAL_PRIME_MULT = 0.80   # month is in prime_months (not peak)
SEASONAL_OFF_MULT   = 0.45   # month is outside prime season

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


def _snap(v: float, step: float) -> float:
    """Round v to the nearest multiple of step."""
    return round(round(v / step) * step, 6)


# ─────────────────────────────────────────────────────────────────────────────
# Data loaders
# ─────────────────────────────────────────────────────────────────────────────
def load_composite(date: datetime.date) -> dict | None:
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
                    snap_step: float = CHL_SNAP_DEG) -> dict:
    """
    Load a rows-format JSON file and return a {(snapped_lat, snapped_lon): value} dict.
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
        lat = _snap(float(row["lat"]), snap_step)
        lon = _snap(float(row["lon"]), snap_step)
        result[(lat, lon)] = fv
    return result


_COLOR_CLASS_KD490 = {
    "blue_water":  0.06,
    "mixed":       0.10,
    "green_water": 0.20,
}


def _load_color_class_as_kd490(path: pathlib.Path, snap_step: float = CHL_SNAP_DEG) -> dict:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    rows   = data.get("rows", [])
    result = {}
    for row in rows:
        cc = row.get("color_class")
        if cc not in _COLOR_CLASS_KD490:
            continue
        lat = _snap(float(row["lat"]), snap_step)
        lon = _snap(float(row["lon"]), snap_step)
        result[(lat, lon)] = _COLOR_CLASS_KD490[cc]
    return result


def load_local_chl(date: datetime.date) -> dict:
    for delta in range(CHL_LOOKBACK_DAYS + 1):
        target = date - datetime.timedelta(days=delta)
        fname  = f"CHL_{target.strftime('%Y%m%d')}.json"
        path   = CHL_DIR / fname
        if path.exists():
            try:
                lookup = _load_rows_json(path, "chlorophyll", snap_step=CHL_SNAP_DEG)
                log.info("Loaded CHL from %s  (%d valid points, %d days old)",
                         fname, len(lookup), delta)
                return lookup
            except Exception as exc:
                log.warning("Failed to parse %s: %s — trying older file.", fname, exc)
    log.warning("No local CHL file found within %d days — CHL scoring will be neutral.",
                CHL_LOOKBACK_DAYS)
    return {}


def load_local_kd490(date: datetime.date) -> dict:
    SEACOLOR_SNAP = 0.04
    for delta in range(CHL_LOOKBACK_DAYS + 1):
        target = date - datetime.timedelta(days=delta)
        fname  = f"SEACOLOR_{target.strftime('%Y%m%d')}.json"
        path   = SEACOLOR_DIR / fname
        if path.exists():
            try:
                lookup = _load_rows_json(path, "kd490", snap_step=SEACOLOR_SNAP)
                log.info("Loaded kd490 from %s  (%d valid points, %d days old)",
                         fname, len(lookup), delta)
                return lookup
            except Exception as exc:
                log.warning("Failed to parse %s: %s — trying older file.", fname, exc)
    log.info("No SEACOLOR file found — deriving kd490 from color_class in CHL file.")
    for delta in range(CHL_LOOKBACK_DAYS + 1):
        target = date - datetime.timedelta(days=delta)
        fname  = f"CHL_{target.strftime('%Y%m%d')}.json"
        path   = CHL_DIR / fname
        if path.exists():
            try:
                lookup = _load_color_class_as_kd490(path, snap_step=CHL_SNAP_DEG)
                log.info("  kd490 proxy from %s  (%d color_class points, %d days old)",
                         fname, len(lookup), delta)
                return lookup
            except Exception as exc:
                log.warning("  Failed to read color_class from %s: %s", fname, exc)
    log.warning("No kd490 source found — kd490 scoring will be neutral.")
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# Composite → lookup helpers
# ─────────────────────────────────────────────────────────────────────────────
def build_composite_lookup(composite: dict) -> dict:
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
    Compute SST gradient magnitude using central differences.
    One-sided differences are used ONLY at true grid edges (domain boundary),
    NOT at data-void (cloud/null) boundaries — suppressing false breaks.
    """
    lat_set = composite["latSet"]
    lon_set = composite["lonSet"]
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
            at_south = (li == 0)
            at_north = (li == n_lat - 1)
            at_west  = (loi == 0)
            at_east  = (loi == n_lon - 1)
            vN = val(li + 1, loi) if not at_north else None
            vS = val(li - 1, loi) if not at_south else None
            vE = val(li, loi + 1) if not at_east  else None
            vW = val(li, loi - 1) if not at_west  else None
            if vN is not None and vS is not None:
                dlat = (vN - vS) / (2 * d_lat)
            elif vN is not None and at_south:
                dlat = (vN - v) / d_lat
            elif vS is not None and at_north:
                dlat = (v - vS) / d_lat
            else:
                dlat = 0.0
            if vE is not None and vW is not None:
                dlon = (vE - vW) / (2 * d_lon)
            elif vE is not None and at_west:
                dlon = (vE - v) / d_lon
            elif vW is not None and at_east:
                dlon = (v - vW) / d_lon
            else:
                dlon = 0.0
            magnitude = math.sqrt(dlat ** 2 + dlon ** 2)
            grad[(round(lat_set[li], 3), round(lon_set[loi], 3))] = magnitude
    return grad


def build_bathy_lookup(bathy: dict) -> dict:
    lats    = bathy["lats"]
    lons    = bathy["lons"]
    grid_ft = bathy["depth_ft"]
    result  = {}
    for li, lat in enumerate(lats):
        for loi, lon in enumerate(lons):
            val = grid_ft[li][loi]
            if val is not None:
                result[(_snap(lat, BATHY_SNAP_DEG), _snap(lon, BATHY_SNAP_DEG))] = val
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Sub-score functions (0.0 – 1.0)
# ─────────────────────────────────────────────────────────────────────────────
def score_sst(sst_f: float, target: float, sst_min: float, sst_max: float) -> float:
    if sst_f < sst_min or sst_f > sst_max:
        return 0.0
    sigma = (sst_max - sst_min) / 4.0
    return math.exp(-0.5 * ((sst_f - target) / sigma) ** 2)


def score_break(gradient_mag: float | None) -> float:
    """
    FIX: Reduced neutral (None) value from 0.30 to 0.20 to stop missing
    gradient data from inflating scores in featureless open water.
    """
    if gradient_mag is None:
        return 0.20
    if gradient_mag >= BREAK_STRONG_THRESHOLD:
        return 1.0
    if gradient_mag >= BREAK_MODERATE_THRESHOLD:
        return 0.75
    if gradient_mag >= BREAK_WEAK_THRESHOLD:
        return 0.45
    return 0.15


def score_depth(depth_ft: float | None, d_min: float, d_max: float,
                d_ideal_min: float, d_ideal_max: float) -> float:
    if depth_ft is None:
        return 0.0
    if depth_ft < d_min or depth_ft > d_max:
        return 0.0
    if d_ideal_min <= depth_ft <= d_ideal_max:
        return 1.0
    if depth_ft < d_ideal_min:
        span = max(d_ideal_min - d_min, 1)
        return (depth_ft - d_min) / span
    span = max(d_max - d_ideal_max, 1)
    return (d_max - depth_ft) / span


def score_chl(chl: float | None, chl_min: float, chl_max: float) -> float:
    """
    FIX: Reduced neutral (None) value from 0.45 to 0.25.
    Added hard low gate at 0.10 for values well below the floor.
    Hard per-cell disqualification for chl_required species is enforced
    upstream in build_score_grid before this function is called.
    """
    if chl is None:
        return 0.25
    if chl <= 0:
        return 0.05
    if chl < chl_min * 0.5:
        return 0.10
    if chl_min <= chl <= chl_max:
        return 1.0
    center = (chl_min + chl_max) / 2.0
    spread = (chl_max - chl_min) / 2.0
    dist   = abs(chl - center) - spread
    return max(0.0, 1.0 - dist / spread)


def score_color(kd490: float | None, kd490_max: float) -> float:
    """
    FIX: Reduced neutral (None) value from 0.50 to 0.35.
    """
    if kd490 is None:
        return 0.35
    if kd490 <= kd490_max * 0.5:
        return 1.0
    if kd490 <= kd490_max:
        return 0.75
    excess = (kd490 - kd490_max) / kd490_max
    return max(0.0, 0.5 - excess * 0.5)


def score_seasonality(month: int, prime_months: list, peak_months: list) -> float:
    if month in peak_months:
        return SEASONAL_PEAK_MULT
    if month in prime_months:
        return SEASONAL_PRIME_MULT
    return SEASONAL_OFF_MULT


# ─────────────────────────────────────────────────────────────────────────────
# Scoring grid construction
# ─────────────────────────────────────────────────────────────────────────────
def build_score_grid(composite_lookup: dict, gradient_lookup: dict,
                     bathy_lookup: dict, chl_lookup: dict, kd490_lookup: dict,
                     sp: dict) -> list[tuple]:
    """
    Score every composite grid point for one species.

    Gates applied in order (cell is skipped if any gate fails):
      1. SST range hard gate  — sst_f outside sst_range_f → skip
      2. SST score min gate   — s_sst < sst_score_min → skip
         Prevents cloud-edge false-break zones in wrong-temperature water
         from scoring high because break/CHL compensate for poor SST fit.
         Set sst_score_min in species_config.json (0.0 = disabled).
      3. Depth gate           — depth known and out of range → skip
      4. CHL hard gate        — chl_required=true and CHL data available
                                and cell CHL < 60% of floor → skip
                                Skipped entirely if no CHL file loaded,
                                to avoid wiping all zones on data-missing days.
      5. Break cap            — break_required=true and s_break < 0.3
                                → total capped at 0.50 (not a hard skip)
    """
    results = []
    w = sp["weights"]
    sst_min, sst_max         = sp["sst_range_f"]
    d_min, d_max             = sp["depth_range_ft"]
    d_ideal_min, d_ideal_max = sp["depth_ideal_ft"]
    chl_min, chl_max         = sp["chl_range_mg_m3"]
    kd490_max                = sp["kd490_max"]
    break_required           = sp.get("break_required", False)
    chl_required             = sp.get("chl_required", False)
    sst_score_min            = sp.get("sst_score_min", 0.0)
    chl_data_available       = len(chl_lookup) > 0

    for (lat, lon), sst_f in composite_lookup.items():

        # Gate 1: SST range
        s_sst = score_sst(sst_f, sp["sst_target_f"], sst_min, sst_max)
        if s_sst == 0.0:
            continue

        # Gate 2: SST score minimum
        # Eliminates cells where SST is technically in range but a poor fit,
        # preventing strong break scores at cloud edges from rescuing them.
        if s_sst < sst_score_min:
            continue

        # Snap to auxiliary grids
        chl_lat   = _snap(lat, CHL_SNAP_DEG)
        chl_lon   = _snap(lon, CHL_SNAP_DEG)
        bathy_lat = _snap(lat, BATHY_SNAP_DEG)
        bathy_lon = _snap(lon, BATHY_SNAP_DEG)

        gradient  = gradient_lookup.get((lat, lon))
        depth_ft  = bathy_lookup.get((bathy_lat, bathy_lon))
        chl       = chl_lookup.get((chl_lat, chl_lon))
        kd490_val = kd490_lookup.get((chl_lat, chl_lon))

        # Gate 3: Depth
        s_depth = score_depth(depth_ft, d_min, d_max, d_ideal_min, d_ideal_max)
        if depth_ft is not None and s_depth == 0.0:
            continue

        # Gate 4: CHL hard gate (chl_required species only, only when data loaded)
        if chl_required and chl_data_available:
            if chl is None or chl < chl_min * 0.6:
                continue

        s_break = score_break(gradient)
        s_chl   = score_chl(chl, chl_min, chl_max)
        s_color = score_color(kd490_val, kd490_max)

        total = (w["sst"]   * s_sst   +
                 w["break"] * s_break +
                 w["depth"] * s_depth +
                 w["chl"]   * s_chl   +
                 w["color"] * s_color)

        # Gate 5: Break cap (soft — reduces score, does not skip)
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
# Clustering
# ─────────────────────────────────────────────────────────────────────────────
def cluster_hot_cells(scored_points: list[tuple],
                      min_score: float,
                      grid_res: float = 0.04) -> list[list[tuple]]:
    hot = {(r[0], r[1]): r for r in scored_points if r[2] >= min_score}
    if not hot:
        return []
    visited  = set()
    clusters = []
    for start_key in hot:
        if start_key in visited:
            continue
        cluster = []
        queue   = deque([start_key])
        visited.add(start_key)
        while queue:
            lat, lon = queue.popleft()
            cluster.append(hot[(lat, lon)])
            for dlat in [-grid_res, 0, grid_res]:
                for dlon in [-grid_res, 0, grid_res]:
                    if dlat == 0 and dlon == 0:
                        continue
                    nlat = round(lat + dlat, 3)
                    nlon = round(lon + dlon, 3)
                    nkey = (nlat, nlon)
                    if nkey in hot and nkey not in visited:
                        visited.add(nkey)
                        queue.append(nkey)
        clusters.append(cluster)
    return clusters


def convex_hull_polygon(points: list[tuple]) -> list[list[float]]:
    if len(points) < 3:
        return [[p[0], p[1]] for p in points]
    try:
        from scipy.spatial import ConvexHull  # type: ignore
        pts  = np.array([[p[0], p[1]] for p in points])
        hull = ConvexHull(pts)
        hull_pts = pts[hull.vertices].tolist()
        hull_pts.append(hull_pts[0])
        return [[round(p[0], 4), round(p[1], 4)] for p in hull_pts]
    except Exception:
        pass
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


# ─────────────────────────────────────────────────────────────────────────────
# AI / template narrative generation
# ─────────────────────────────────────────────────────────────────────────────
def _template_narrative(date: datetime.date, sp: dict, zone: dict) -> str:
    month      = date.month
    month_name = calendar.month_name[month]
    prime      = sp.get("prime_months", list(range(1, 13)))
    peak       = sp.get("peak_months", [])
    name       = sp["display_name"]
    cond       = zone["conditions"]
    sst        = cond.get("sst_f")
    sweet      = sp.get("sst_sweet_spot_f", sp["sst_range_f"])
    brk        = cond.get("break_strength", "none")
    depth      = cond.get("depth_ft")
    chl        = cond.get("chl_mg_m3")
    side       = sp.get("prefer_side", "warm")
    score      = zone.get("habitat_score", zone.get("score", 0))
    area       = zone.get("area_sq_nm", 0)

    if month in peak:
        s_season = f"Peak season timing for {name} — this is prime time."
    elif month in prime:
        s_season = f"{month_name} is within the prime season window for {name}."
    else:
        s_season = f"{month_name} is outside the typical prime season; fish may be scattered."

    if sst is not None:
        delta = sst - ((sweet[0] + sweet[1]) / 2)
        if sweet[0] <= sst <= sweet[1]:
            if abs(delta) < 0.5:
                s_sst = f"SST of {sst}°F is dead center in the sweet spot."
            elif delta > 0:
                s_sst = f"SST is {sst}°F — warm end of the sweet spot ({sweet[0]}–{sweet[1]}°F)."
            else:
                s_sst = f"SST is {sst}°F — cool end of the sweet spot ({sweet[0]}–{sweet[1]}°F)."
        else:
            s_sst = f"SST of {sst}°F is within range but off the ideal {sweet[0]}–{sweet[1]}°F window."
    else:
        s_sst = "SST data is limited in this area."

    break_phrases = {
        "strong":   "There's a sharp temperature break in the zone — concentrate on the seam.",
        "moderate": "A defined temperature break is present; work the edges.",
        "weak":     "A subtle thermal gradient exists here; look for associated color changes.",
        "none":     "No significant temperature break detected — focus on depth contours and structure.",
    }
    s_break = break_phrases.get(brk, "")

    depth_str = f"{int(depth):,}ft" if depth else "target depth"
    if side == "warm":
        tactic_prefix = f"Set up on the warm side of any edge in {depth_str}"
    else:
        tactic_prefix = f"Work the inshore/cool side of the break in {depth_str}"

    chl_note = ""
    if chl is not None:
        if chl > 0.5:
            chl_note = " High chlorophyll suggests productive water nearby — look for the clean edge."
        elif chl < 0.1:
            chl_note = " Very clear water (low CHL) confirms blue Gulf Stream influence."

    s_tactic = f"{tactic_prefix}.{chl_note} Zone covers ~{area} sq nm (score {score:.0%})."
    return f"{s_season} {s_sst} {s_break} {s_tactic}"


def generate_zone_narrative(date: datetime.date, sp: dict, zone: dict,
                             skip: bool = False) -> str:
    if skip:
        return _template_narrative(date, sp, zone)

    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return _template_narrative(date, sp, zone)

    try:
        import anthropic  # type: ignore
        cond  = zone["conditions"]
        sweet = sp.get("sst_sweet_spot_f", sp["sst_range_f"])
        prime = sp.get("prime_months", [])
        peak  = sp.get("peak_months", [])
        month = date.month

        if month in peak:
            season_status = "PEAK season"
        elif month in prime:
            season_status = "prime season"
        else:
            season_status = "off-season"

        sst        = cond.get("sst_f")
        brk        = cond.get("break_strength", "none")
        depth      = cond.get("depth_ft")
        chl        = cond.get("chl_mg_m3")
        kd490      = cond.get("kd490")
        side       = sp.get("prefer_side", "warm")
        area       = zone.get("area_sq_nm", 0)
        hab_score  = zone.get("habitat_score", 0)
        adj_score  = zone.get("score", 0)
        center     = zone["center"]

        sst_delta = round(sst - sp["sst_target_f"], 1) if sst else None
        sst_rel = (
            "right on target" if sst_delta is not None and abs(sst_delta) < 0.5
            else f"{abs(sst_delta)}°F {'above' if sst_delta and sst_delta > 0 else 'below'} target"
            if sst_delta is not None else "unknown"
        )

        prompt = f"""You are a seasoned offshore fishing captain at Cape Hatteras, NC. Write exactly 3 sentences — a specific, practical fishing narrative for this zone. Use the voice of someone who fishes these waters regularly. No generic advice. Each sentence must be grounded in the actual numbers below.

Zone data:
  Species: {sp["display_name"]}
  Date: {date.strftime("%B %d")} ({season_status})
  Center: {center[0]:.2f}N, {abs(center[1]):.2f}W
  Area: {area} sq nm
  SST: {sst}°F ({sst_rel}, sweet spot {sweet[0]}–{sweet[1]}°F)
  Temp break: {brk}
  Depth: {depth}ft
  Chlorophyll: {chl} mg/m³  (None = no recent data)
  Water clarity kd490: {kd490}  (lower = bluer/cleaner)
  Preferred side of break: {side}
  Habitat score: {hab_score:.0%}, adjusted for season: {adj_score:.0%}
  Seasonal notes: {sp.get("seasonal_context", "")[:200]}

Rules:
- Sentence 1: State what the SST and break conditions mean RIGHT NOW — be specific about the numbers and what they tell a captain before departure.
- Sentence 2: Give one concrete tactical call for THIS zone — where exactly to start, what to look for when you get there.
- Sentence 3: One honest caveat or timing note based on the season and conditions.
- Total under 80 words. No bullet points. No preamble. Just the 3 sentences."""

        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=160,
            messages=[{"role": "user", "content": prompt}]
        )
        narrative = message.content[0].text.strip()
        log.info("    Narrative generated via Claude API (%d chars)", len(narrative))
        return narrative
    except Exception as exc:
        log.warning("    Narrative API call failed: %s — using template.", exc)
        return _template_narrative(date, sp, zone)


# ─────────────────────────────────────────────────────────────────────────────
# Zone summarization
# ─────────────────────────────────────────────────────────────────────────────
def zone_from_cluster(cluster: list[tuple], rank: int) -> dict:
    scores    = [r[2] for r in cluster]
    metas     = [r[3] for r in cluster]
    best_idx  = scores.index(max(scores))
    best_meta = metas[best_idx]

    lats = [r[0] for r in cluster]
    lons = [r[1] for r in cluster]
    lat_span_nm = (max(lats) - min(lats)) * 60
    lon_span_nm = (max(lons) - min(lons)) * 60 * math.cos(math.radians(sum(lats) / len(lats)))
    area_sq_nm  = round(lat_span_nm * lon_span_nm, 1)

    polygon = convex_hull_polygon([(r[0], r[1]) for r in cluster])

    w_sum      = sum(scores)
    center_lat = round(sum(r[0] * r[2] for r in cluster) / w_sum, 4)
    center_lon = round(sum(r[1] * r[2] for r in cluster) / w_sum, 4)

    return {
        "rank":          rank,
        "habitat_score": round(sum(scores) / len(scores), 3),
        "peak_score":    round(max(scores), 3),
        "cell_count":    len(cluster),
        "area_sq_nm":    area_sq_nm,
        "center":        [center_lat, center_lon],
        "polygon":       polygon,
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
        "seasonal_factor": None,
        "score":           None,
        "in_season":       None,
        "narrative":       None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Per-species analysis
# ─────────────────────────────────────────────────────────────────────────────
def analyze_species(species_key: str, sp_config: dict,
                    composite_lookup: dict, gradient_lookup: dict,
                    bathy_lookup: dict, chl_lookup: dict,
                    kd490_lookup: dict,
                    composite_step: float = 0.02,
                    date: datetime.date | None = None,
                    skip_narrative: bool = False) -> dict:
    log.info("  Analyzing %s ...", sp_config["display_name"])
    if date is None:
        date = datetime.date.today()
    month      = date.month
    min_score  = sp_config.get("min_zone_score", 0.60)
    prime      = sp_config.get("prime_months", list(range(1, 13)))
    peak       = sp_config.get("peak_months", [])
    seas_mult  = score_seasonality(month, prime, peak)
    seas_label = ("peak" if month in peak
                  else ("prime" if month in prime else "off-season"))

    scored = build_score_grid(
        composite_lookup, gradient_lookup, bathy_lookup,
        chl_lookup, kd490_lookup, sp_config
    )
    hot_count = sum(1 for r in scored if r[2] >= min_score)
    log.info("    %d / %d scored points above %.2f threshold",
             hot_count, len(scored), min_score)

    clusters = cluster_hot_cells(scored, min_score, grid_res=composite_step)
    log.info("    %d raw cluster(s) found", len(clusters))

    valid = [c for c in clusters if len(c) >= CLUSTER_MIN_CELLS]
    valid.sort(key=lambda c: sum(r[2] for r in c) / len(c), reverse=True)
    top3 = valid[:3]

    zones = []
    for i, c in enumerate(top3):
        z = zone_from_cluster(c, rank=i + 1)
        z["seasonal_factor"] = seas_mult
        z["score"]           = round(z["habitat_score"] * seas_mult, 3)
        z["in_season"]       = seas_mult >= SEASONAL_PRIME_MULT
        log.info("    Zone %d: habitat=%.3f × seasonal=%.2f → score=%.3f  (%s)",
                 i + 1, z["habitat_score"], seas_mult, z["score"], seas_label)
        z["narrative"] = generate_zone_narrative(date, sp_config, z,
                                                 skip=skip_narrative)
        zones.append(z)

    zones.sort(key=lambda z: z["score"], reverse=True)
    for i, z in enumerate(zones):
        z["rank"] = i + 1

    log.info("    -> %d zone(s) for %s  [seasonality: %s  mult=%.2f]",
             len(zones), sp_config["display_name"], seas_label, seas_mult)

    return {
        "zones":              zones,
        "grid_points_scored": len(scored),
        "hot_cells":          hot_count,
        "seasonal_factor":    seas_mult,
        "seasonal_status":    seas_label,
    }


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

    date_env = os.environ.get("DATE", "").strip()
    date = datetime.date.fromisoformat(date_env) if date_env else datetime.date.today()
    log.info("=== FishingHotspotAnalyzer  date=%s  month=%s ===",
             date.isoformat(), calendar.month_name[date.month])

    species_filter = os.environ.get("SPECIES", "").strip().lower() or None
    skip_narrative = os.environ.get("SKIP_NARRATIVE", "").strip() == "1"

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

    log.info("=== Loading local data ===")
    composite = load_composite(date)
    if composite is None:
        log.error("Cannot proceed without composite SST.")
        sys.exit(1)

    bathy = load_bathymetry_grid()
    if bathy is None:
        log.warning("Bathymetry unavailable — depth scoring will be neutral.")

    log.info("Building composite lookup ...")
    composite_lookup = build_composite_lookup(composite)
    log.info("  %d ocean SST points", len(composite_lookup))

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

    if skip_narrative:
        log.info("SKIP_NARRATIVE=1 — using template narratives only.")
    elif os.environ.get("ANTHROPIC_API_KEY"):
        log.info("ANTHROPIC_API_KEY set — will generate AI narratives via Claude API.")
    else:
        log.info("ANTHROPIC_API_KEY not set — using template narratives.")

    log.info("=== Scoring species ===")
    results = {}
    for key, sp in enabled.items():
        try:
            results[key] = analyze_species(
                key, sp,
                composite_lookup, gradient_lookup,
                bathy_lookup, chl_lookup, kd490_lookup,
                composite_step=composite_step,
                date=date,
                skip_narrative=skip_narrative,
            )
        except Exception as exc:
            log.error("  Failed for %s: %s", key, exc, exc_info=True)
            results[key] = {"zones": [], "error": str(exc)}

    log.info("=== Writing output ===")
    write_hotspots(date, results)
    purge_old_hotspots(KEEP_HOTSPOT_DAYS)
    log.info("=== Done. ===")


if __name__ == "__main__":
    main()
