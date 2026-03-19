"""
StaticLayersRetrieval.py
========================
Fetches static reference layers for the Mid-Atlantic offshore fishing region
and writes them as JSON files into DailySST/.

  DailySST/
    bathymetry.json           – GEBCO_2020 depth grid (~32 MB at stride 2)
    bathymetry_contours.json  – GeoJSON LineStrings at fishing-relevant depths
    wrecks.json               – Named fishing spots merged from all three GPX files

Run once to populate, or re-run via manual workflow dispatch to refresh.

Bathymetry
----------
  GEBCO_2020 via NOAA CoastWatch ERDDAP (dataset: GEBCO_2020)
  Stride 2 = ~900 m resolution — enough to render narrow features like
  Norfolk Canyon without the spike artifact caused by stride 4.

Points of interest / wrecks
----------------------------
  Parsed from three Fishing Status community GPX exports split by UI region:
    Fishing_Spots_HatterasNC.gpx    – Cape Hatteras / Diamond Shoals area
    Fishing_Spots_MoreheadNC.gpx    – Morehead City / Cape Lookout area
    Fishing_Spots_ChesapeakeMD.gpx  – Chesapeake / Virginia Beach area
  All three are merged, deduplicated by coordinate, and written to wrecks.json.
  To update: replace any GPX file with a new export and re-run.
  No network request is made for this layer.

Dependencies
------------
  pip install requests contourpy
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

# Bathymetry stride:
#   1  = full 15 arc-sec (~450 m)  — ~135 MB, requires Git LFS
#   2  = ~900 m                    — ~32 MB (default)
#   4  = ~1.8 km                   — ~8 MB (too coarse, canyon = spike artifact)
#   10 = ~4.5 km                   — ~1.5 MB
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

    reader   = csv.reader(io.StringIO(r.text))
    all_rows = list(reader)
    rows     = []
    for raw in all_rows[2:]:
        if len(raw) < 3:
            continue
        try:
            lat  = round(float(raw[0]), 6)
            lon  = round(float(raw[1]), 6)
            elev = float(raw[2])
        except (ValueError, IndexError):
            continue
        depth_ft = None if elev >= 0 else round(abs(elev) * 3.28084, 1)
        rows.append({"lat": lat, "lon": lon, "depth_ft": depth_ft})

    ocean = sum(1 for r in rows if r["depth_ft"] is not None)
    log.info("  Parsed %d points (%d ocean, %d land/null).",
             len(rows), ocean, len(rows) - ocean)
    return rows


def _actual_extent(rows: list[dict]) -> dict:
    if not rows:
        return {}
    lats = [r["lat"] for r in rows]
    lons = [r["lon"] for r in rows]
    return {
        "lat_min": round(min(lats), 6), "lat_max": round(max(lats), 6),
        "lon_min": round(min(lons), 6), "lon_max": round(max(lons), 6),
    }


def write_bathymetry(session: requests.Session) -> tuple[pathlib.Path, list[dict]]:
    rows   = _fetch_bathymetry(session)
    extent = _actual_extent(rows)
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
        "actual_extent": extent,
        "point_count":   len(rows),
        "points":        rows,
    }
    dest = OUTPUT_DIR / "bathymetry.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    log.info("bathymetry.json written  (%.1f MB)", dest.stat().st_size / 1e6)
    return dest, rows


# ---------------------------------------------------------------------------
# Bathymetry contours — marching squares via contourpy
# ---------------------------------------------------------------------------

CONTOUR_DEPTHS_FT = [30, 60, 100, 200, 300, 600, 1000, 1500, 2000]


def _build_grid(rows: list[dict]):
    """
    Build a 2-D depth grid from the flat row list.
    Land/null = NaN so contourpy skips them.
    Sparse ocean gaps are filled by iterative neighbor-average to prevent
    isolated single-cell features producing closing-contour artifacts.
    """
    lats_set = sorted(set(r["lat"] for r in rows))
    lons_set = sorted(set(r["lon"] for r in rows))
    lat_idx  = {v: i for i, v in enumerate(lats_set)}
    lon_idx  = {v: i for i, v in enumerate(lons_set)}
    n_rows   = len(lats_set)
    n_cols   = len(lons_set)

    flat = [math.nan] * (n_rows * n_cols)
    for r in rows:
        if r["depth_ft"] is not None:
            flat[lat_idx[r["lat"]] * n_cols + lon_idx[r["lon"]]] = r["depth_ft"]

    # Iterative neighbor-average fill — ocean gaps only (land stays NaN)
    for _ in range(6):
        changed  = False
        new_flat = flat[:]
        for row in range(n_rows):
            for col in range(n_cols):
                i = row * n_cols + col
                if not math.isnan(flat[i]):
                    continue
                neighbors = []
                for dr, dc in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                    nr, nc = row + dr, col + dc
                    if 0 <= nr < n_rows and 0 <= nc < n_cols:
                        v = flat[nr * n_cols + nc]
                        if not math.isnan(v):
                            neighbors.append(v)
                if neighbors:
                    new_flat[i] = sum(neighbors) / len(neighbors)
                    changed = True
        flat = new_flat
        if not changed:
            break

    grid = [flat[r * n_cols:(r + 1) * n_cols] for r in range(n_rows)]
    return lats_set, lons_set, grid


def _grid_to_geojson_contours(lats, lons, grid, depth_ft: float) -> list:
    from contourpy import contour_generator
    cg    = contour_generator(x=lons, y=lats, z=grid, name="serial")
    lines = cg.lines(depth_ft)
    MIN_POINTS = 6
    features = []
    for line in lines:
        if len(line) < MIN_POINTS:
            continue
        coords = [[round(float(pt[0]), 5), round(float(pt[1]), 5)] for pt in line]
        features.append(coords)
    return features


def write_contours(rows: list[dict]) -> pathlib.Path:
    try:
        import contourpy  # noqa: F401
    except ImportError:
        log.error("contourpy not installed — run: pip install contourpy")
        raise

    log.info("Generating depth contours at %s ft …", CONTOUR_DEPTHS_FT)
    lats, lons, grid = _build_grid(rows)
    log.info("  Grid: %d lats x %d lons", len(lats), len(lons))

    all_features = []
    for depth_ft in CONTOUR_DEPTHS_FT:
        lines = _grid_to_geojson_contours(lats, lons, grid, depth_ft)
        for coords in lines:
            all_features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {
                    "depth_ft":    depth_ft,
                    "depth_label": f"{depth_ft} ft",
                    "fishing_note": {
                        30:   "nearshore bottom limit",
                        60:   "inshore reef and wreck belt",
                        100:  "king mackerel / cobia line",
                        200:  "amberjack / grouper deep edge",
                        300:  "outer shelf — mahi and wahoo",
                        600:  "shelf break — primary pelagic zone",
                        1000: "upper slope — swordfish at night",
                        1500: "mid-slope",
                        2000: "canyon floor",
                    }.get(depth_ft),
                },
            })
        log.info("  %d ft: %d line segments", depth_ft, len(lines))

    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "dataset":           "GEBCO_2020",
            "source":            "https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020",
            "contour_depths_ft": CONTOUR_DEPTHS_FT,
            "units":             {"depth_ft": "feet below surface"},
            "region": {
                "lat_min": LAT_MIN, "lat_max": LAT_MAX,
                "lon_min": LON_MIN, "lon_max": LON_MAX,
            },
            "actual_extent": {
                "lat_min": round(min(lats), 6), "lat_max": round(max(lats), 6),
                "lon_min": round(min(lons), 6), "lon_max": round(max(lons), 6),
            },
        },
        "feature_count": len(all_features),
        "features":      all_features,
    }

    dest = OUTPUT_DIR / "bathymetry_contours.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, separators=(",", ":"))
    log.info("bathymetry_contours.json written  (%d features, %.2f MB)",
             len(all_features), dest.stat().st_size / 1e6)
    return dest


# ---------------------------------------------------------------------------
# Land mask — derived from MUR SST mask field
# ---------------------------------------------------------------------------

def write_landmask() -> pathlib.Path:
    """
    Generate landmask.json from any available MUR_SST_YYYYMMDD.json file
    at the full native 1-km (~0.01 deg) MUR resolution.

    MUR SST includes a 'mask' field per pixel:
      1 = open ocean
      2 = land
      5 = lake / inland water
      (other values = sea ice, etc.)

    We write every non-ocean pixel at its exact lat/lon — no binning —
    so the coastline overlay on the GOES-19 canvas is sharp and accurate.
    File is larger (~3-5 MB) but is generated once and never changes.
    """
    mur_files = sorted(OUTPUT_DIR.glob("MUR_SST_????????.json"), reverse=True)
    if not mur_files:
        log.error("No MUR SST files found in %s — cannot generate landmask.", OUTPUT_DIR)
        return OUTPUT_DIR / "landmask.json"

    src = mur_files[0]
    log.info("Generating landmask from %s at native 1-km resolution …", src.name)

    with open(src, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    rows = data.get("rows", [])
    if not rows:
        log.error("No rows in %s — cannot generate landmask.", src.name)
        return OUTPUT_DIR / "landmask.json"

    points = []
    for r in rows:
        mask = r.get("mask")
        if mask is None:
            continue
        # mask values:
        #   1 = open ocean
        #   2 = land          ← only this should be treated as land
        #   5 = lake/inland water/sounds/estuaries ← keep transparent, not land
        if mask == 2:
            points.append({"lat": r["lat"], "lon": r["lon"]})

    log.info("  %d land/non-ocean points at native ~0.01-deg resolution.", len(points))

    payload = {
        "generated_from": src.name,
        "resolution_deg": 0.01,
        "note":           "mask==2 (land only) pixels from MUR SST — inland water excluded",
        "point_count":    len(points),
        "points":         points,
    }

    dest = OUTPUT_DIR / "landmask.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    log.info("landmask.json written  (%d points, %.2f MB)",
             len(points), dest.stat().st_size / 1e6)
    return dest


# ---------------------------------------------------------------------------
# Fishing spots — parsed from GPX file
# ---------------------------------------------------------------------------

# GPX namespace
_GPX_NS = {"gpx": "http://www.topografix.com/GPX/1/1"}


def _parse_gpx_file(gpx_path: pathlib.Path, region: str) -> tuple[list[dict], int]:
    """
    Parse a single Fishing Status GPX file and return (features, skipped).
    Sanitizes common XML issues (unescaped & outside CDATA) before parsing.
    Features are NOT yet deduplicated — caller handles that after merging.
    """
    # Read raw bytes and fix unescaped & that aren't already part of an
    # XML entity reference (e.g. &amp; &lt; &gt; &quot; &apos; &#NNN;)
    raw = gpx_path.read_bytes()
    # Replace bare & not followed by an entity/char reference
    import re as _re
    raw = _re.sub(rb'&(?!amp;|lt;|gt;|quot;|apos;|#)', b'&amp;', raw)

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        log.error("Failed to parse %s: %s", gpx_path.name, exc)
        # Show the offending line for easier debugging
        line_no = exc.position[0] if exc.position else 0
        lines = raw.split(b'\n')
        if line_no:
            for i in range(max(0, line_no - 2), min(len(lines), line_no + 1)):
                log.error("  Line %d: %s", i + 1,
                          lines[i].decode("utf-8", errors="replace"))
        return [], 0

    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    features = []
    skipped  = 0

    for wpt in root.findall(f"{ns}wpt"):
        try:
            lat = float(wpt.get("lat"))
            lon = float(wpt.get("lon"))
        except (TypeError, ValueError):
            skipped += 1
            continue

        # Filter to bounding box
        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
            skipped += 1
            continue

        # Use explicit is-not-None — ElementTree elements with no children
        # evaluate as False in Python, so `el or fallback` short-circuits
        # past a valid (but childless) element. Fix: check is not None.
        def _find(tag):
            el = wpt.find(f"{ns}{tag}")
            if el is not None:
                return el
            return wpt.find(tag)

        name_el = _find("name")
        name    = name_el.text.strip() if name_el is not None and name_el.text else "Unknown"

        sym_el  = _find("sym")
        symbol  = sym_el.text.strip() if sym_el is not None and sym_el.text else "Unknown"

        desc_el = _find("desc")
        desc    = desc_el.text.strip() if desc_el is not None and desc_el.text else ""
        fs_id   = re.search(r"ID#(\d+)", desc)
        fs_id   = fs_id.group(1) if fs_id else None

        features.append({
            "type": "Feature",
            "geometry": {
                "type":        "Point",
                "coordinates": [round(lon, 6), round(lat, 6)],
            },
            "properties": {
                "name":   name,
                "symbol": symbol,   # "Wreck" | "Rocks"
                "fs_id":  fs_id,
                "region": region,   # "HatterasNC" | "MoreheadNC" | "ChesapeakeMD"
                "source": "Fishing Status (fishingstatus.com)",
            },
        })

    return features, skipped


def write_wrecks(_session=None, bathy_rows: list | None = None) -> pathlib.Path:
    """
    Parse all GPX files in GPX_FILENAMES from OUTPUT_DIR, merge them,
    deduplicate by coordinate, filter shallow points, and write wrecks.json.

    Points in less than MIN_WRECK_DEPTH_FT of water are suppressed — they
    are either on land, in harbours, or too shallow to be relevant offshore
    fishing spots.  Depth is looked up from the bathymetry grid; if no
    bathymetry data was loaded the depth filter is skipped with a warning.

    Each file is a Fishing Status community GPX export for a different
    UI region (Hatteras NC, Morehead NC, Chesapeake VA).  Missing files
    are warned about but do not abort the run — the remaining files are
    still processed.

    Each waypoint becomes a GeoJSON Point feature with:
      name     — waypoint name from <n> tag
      symbol   — "Wreck" or "Rocks" from <sym> tag
      fs_id    — Fishing Status ID from <desc> tag (e.g. "ID#5262")
      region   — source file region label
    """
    MIN_WRECK_DEPTH_FT = 50

    # Build sorted lat/lon arrays for fast nearest-neighbour depth lookup.
    # Exact key matching fails due to floating-point rounding between the
    # ERDDAP-returned grid coords and the snapped waypoint coords, so we
    # use bisect to find the closest grid point instead.
    import bisect as _bisect

    depth_lookup: dict = {}
    sorted_lats: list = []
    sorted_lons: list = []
    if bathy_rows:
        for r in bathy_rows:
            depth_lookup[(r["lat"], r["lon"])] = r["depth_ft"]
        sorted_lats = sorted(set(r["lat"] for r in bathy_rows))
        sorted_lons = sorted(set(r["lon"] for r in bathy_rows))
        log.info("Depth lookup built from %d bathymetry points.", len(depth_lookup))
    else:
        log.warning("No bathymetry data available — depth filter will be skipped.")

    def _nearest(val: float, arr: list) -> float:
        """Return the value in sorted arr closest to val."""
        i = _bisect.bisect_left(arr, val)
        if i == 0:
            return arr[0]
        if i == len(arr):
            return arr[-1]
        before, after = arr[i - 1], arr[i]
        return after if (after - val) < (val - before) else before

    def _depth_at(lat: float, lon: float) -> "float | None":
        """Return depth_ft at the nearest bathymetry grid point to (lat, lon)."""
        if not depth_lookup:
            return None
        snap_lat = _nearest(lat, sorted_lats)
        snap_lon = _nearest(lon, sorted_lons)
        return depth_lookup.get((snap_lat, snap_lon))
    all_features: list[dict] = []
    found_files:  list[str]  = []

    for filename, region in GPX_FILENAMES:
        gpx_path = OUTPUT_DIR / filename
        if not gpx_path.exists():
            log.warning("GPX file not found (skipping): %s", gpx_path)
            continue
        log.info("Parsing %s …", gpx_path)
        features, skipped = _parse_gpx_file(gpx_path, region)
        log.info("  %s: %d features parsed, %d outside bounds / skipped.",
                 filename, len(features), skipped)
        all_features.extend(features)
        found_files.append(filename)

    if not found_files:
        log.error("No GPX files found. Place at least one of %s in %s and re-run.",
                  [f for f, _ in GPX_FILENAMES], OUTPUT_DIR)
        return OUTPUT_DIR / "wrecks.json"

    log.info("Total features before dedup: %d (from %d file(s))",
             len(all_features), len(found_files))

    # Deduplicate by coordinate and filter out shallow points
    seen     = set()
    unique   = []
    shallow  = 0
    for f in all_features:
        lon, lat = f["geometry"]["coordinates"]
        key = (round(lon, 4), round(lat, 4))
        if key in seen:
            continue
        seen.add(key)
        # Depth filter — suppress only points where depth is positively
        # known to be shallower than MIN_WRECK_DEPTH_FT.
        # If depth is None it means the nearest GEBCO cell is tagged as
        # land/unresolved — at 900 m stride many nearshore wrecks snap to
        # a land cell, so we keep those rather than silently dropping them.
        if depth_lookup:
            depth = _depth_at(lat, lon)
            if depth is not None and depth < MIN_WRECK_DEPTH_FT:
                shallow += 1
                continue
        unique.append(f)

    log.info("  %d unique features after dedup.", len(seen))
    if depth_lookup:
        log.info("  %d suppressed (shallower than %d ft or on land).",
                 shallow, MIN_WRECK_DEPTH_FT)

    # Summary by symbol type
    sym_counts: dict[str, int] = {}
    for f in unique:
        s = f["properties"]["symbol"]
        sym_counts[s] = sym_counts.get(s, 0) + 1
    log.info("  Symbol breakdown: %s", sym_counts)

    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "source":    "Fishing Status (fishingstatus.com)",
            "gpx_files": found_files,
            "regions":   [r for _, r in GPX_FILENAMES],
            "region": {
                "lat_min": LAT_MIN, "lat_max": LAT_MAX,
                "lon_min": LON_MIN, "lon_max": LON_MAX,
            },
            "symbols": {
                "Wreck": "charted or known shipwreck",
                "Rocks": "rock, ledge, reef, or bottom structure",
            },
        },
        "feature_count": len(unique),
        "features":      unique,
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

    bathy_rows = []
    try:
        _, bathy_rows = write_bathymetry(session)
    except Exception as exc:
        log.error("Bathymetry fetch failed: %s", exc)

    try:
        if bathy_rows:
            write_contours(bathy_rows)
        else:
            log.warning("Skipping contours — no bathymetry rows available.")
    except Exception as exc:
        log.error("Contour generation failed: %s", exc)

    try:
        write_wrecks(bathy_rows=bathy_rows)
    except Exception as exc:
        log.error("Wrecks/POI parsing failed: %s", exc)

    try:
        write_landmask()
    except Exception as exc:
        log.error("Land mask generation failed: %s", exc)

    log.info("=== Done ===")


if __name__ == "__main__":
    main()
