"""
StaticLayersRetrieval.py
========================
Fetches static reference layers for the Mid-Atlantic offshore fishing region
and writes them as JSON files into DailySST/.

  DailySST/
    bathymetry.json           – GEBCO_2020 depth grid (~32 MB at stride 2)
    bathymetry_contours.json  – GeoJSON LineStrings at fishing-relevant depths
    wrecks.json               – Named fishing spots parsed from fishing_spots.gpx

Run once to populate, or re-run via manual workflow dispatch to refresh.

Bathymetry
----------
  GEBCO_2020 via NOAA CoastWatch ERDDAP (dataset: GEBCO_2020)
  Stride 2 = ~900 m resolution — enough to render narrow features like
  Norfolk Canyon without the spike artifact caused by stride 4.

Points of interest / wrecks
----------------------------
  Parsed from DailySST/fishing_spots.gpx — a Fishing Status community GPX
  export containing named rocks, ledges, wrecks, and artificial reefs.
  To update: replace fishing_spots.gpx with a new export and re-run.
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

OUTPUT_DIR   = pathlib.Path(__file__).resolve().parent / "DailySST"
GPX_FILENAME = "fishing_spots.gpx"

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
# Fishing spots — parsed from GPX file
# ---------------------------------------------------------------------------

# GPX namespace
_GPX_NS = {"gpx": "http://www.topografix.com/GPX/1/1"}


def write_wrecks(_session=None) -> pathlib.Path:
    """
    Parse fishing_spots.gpx from OUTPUT_DIR and write wrecks.json.
    The GPX file is a Fishing Status community export containing named
    rocks, ledges, wrecks, and artificial reefs.

    Each waypoint is converted to a GeoJSON Point feature with:
      name     — waypoint name from <n> tag
      symbol   — "Wreck" or "Rocks" from <sym> tag
      fs_id    — Fishing Status ID from <desc> tag (e.g. "ID#5262")
    """
    gpx_path = OUTPUT_DIR / GPX_FILENAME
    if not gpx_path.exists():
        log.error("GPX file not found: %s", gpx_path)
        log.error("Place %s in the DailySST/ folder and re-run.", GPX_FILENAME)
        return OUTPUT_DIR / "wrecks.json"

    log.info("Parsing %s …", gpx_path)
    tree = ET.parse(gpx_path)
    root = tree.getroot()

    # Handle both namespaced and non-namespaced GPX
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

        # Name — standard GPX <name> tag
        name_el = wpt.find(f"{ns}n") or wpt.find("n")
        name    = name_el.text.strip() if name_el is not None and name_el.text else "Unknown"

        # Symbol type
        sym_el = wpt.find(f"{ns}sym") or wpt.find("sym")
        symbol = sym_el.text.strip() if sym_el is not None and sym_el.text else "Unknown"

        # Fishing Status ID from description
        desc_el = wpt.find(f"{ns}desc") or wpt.find("desc")
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
                "source": "Fishing Status (fishingstatus.com)",
            },
        })

    log.info("  Parsed %d features (%d outside bounds / skipped).",
             len(features), skipped)

    # Deduplicate by coordinate
    seen   = set()
    unique = []
    for f in features:
        key = (
            round(f["geometry"]["coordinates"][0], 4),
            round(f["geometry"]["coordinates"][1], 4),
        )
        if key not in seen:
            seen.add(key)
            unique.append(f)

    log.info("  %d unique features after dedup.", len(unique))

    # Summary by symbol type
    sym_counts = {}
    for f in unique:
        s = f["properties"]["symbol"]
        sym_counts[s] = sym_counts.get(s, 0) + 1
    log.info("  Symbol breakdown: %s", sym_counts)

    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "source":  "Fishing Status (fishingstatus.com)",
            "gpx_file": GPX_FILENAME,
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
        write_wrecks()
    except Exception as exc:
        log.error("Wrecks/POI parsing failed: %s", exc)

    log.info("=== Done ===")


if __name__ == "__main__":
    main()
