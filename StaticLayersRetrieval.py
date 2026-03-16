"""
StaticLayersRetrieval.py
========================
Fetches two static reference layers for the Mid-Atlantic offshore fishing
region and writes them as JSON files into DailySST/.

  DailySST/
    bathymetry.json           – GEBCO_2020 depth grid (~1.8 km resolution, ~8 MB)
    bathymetry_contours.json  – GeoJSON LineStrings at fishing-relevant depths (~1-3 MB)
    wrecks.json               – NOAA ENC wrecks GeoJSON FeatureCollection

Run once to populate, or re-run via manual workflow dispatch to refresh.

Bathymetry
----------
  GEBCO_2020 via NOAA CoastWatch ERDDAP (dataset: GEBCO_2020)
  Stride 4 = ~1.8 km resolution (~8 MB, fits GitHub's 100 MB limit).
  Set BATHY_STRIDE = 1 for full 450 m resolution (~135 MB, requires Git LFS).

Wrecks
------
  NOAA has retired AWOIS. The current authoritative source is ENC Direct to GIS
  hosted at encdirect.noaa.gov. Wreck points are queried across four ENC scale
  bands: harbour, approach, coastal, and general — giving the most complete
  picture from inshore to offshore.

  Each band's wreck layer is queried with a bounding box filter and results are
  deduplicated by coordinate before writing.

Dependencies
------------
  pip install requests
"""

import csv
import io
import json
import logging
import pathlib
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration — keep in sync with DailySSTRetrieval.py
# ---------------------------------------------------------------------------

LAT_MIN = 33.70
LAT_MAX = 39.00
LON_MIN = -78.89
LON_MAX = -72.21

# Bathymetry stride:
#   1  = full 15 arc-sec (~450 m)  — ~135 MB, requires Git LFS
#   4  = ~1.8 km                   — ~8 MB (default, fits GitHub)
#   10 = ~4.5 km                   — ~1.5 MB
BATHY_STRIDE = 4

OUTPUT_DIR = pathlib.Path(__file__).resolve().parent / "DailySST"

ERDDAP_BATHY = "https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020.csvp"

# ENC Direct to GIS — the current NOAA-authoritative wreck source
# Each entry is (scale_band_name, wreck_point_layer_id)
# Layer IDs confirmed from encdirect.noaa.gov ArcGIS REST metadata:
#   enc_harbour  / layer 36  = harbour scale wreck points
#   enc_approach / layer 36  = approach scale wreck points
#   enc_coastal  / layer 36  = coastal scale wreck points
#   enc_general  / layer 36  = general scale wreck points
ENC_SERVICES = [
    ("harbour",  "https://encdirect.noaa.gov/arcgis/rest/services/encdirect/enc_harbour/MapServer/36/query"),
    ("approach", "https://encdirect.noaa.gov/arcgis/rest/services/encdirect/enc_approach/MapServer/36/query"),
    ("coastal",  "https://encdirect.noaa.gov/arcgis/rest/services/encdirect/enc_coastal/MapServer/36/query"),
    ("general",  "https://encdirect.noaa.gov/arcgis/rest/services/encdirect/enc_general/MapServer/36/query"),
]

TIMEOUT = 180
MAX_RETRIES = 3
BACKOFF = 2

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

    reader = csv.reader(io.StringIO(r.text))
    all_rows = list(reader)
    rows = []
    for raw in all_rows[2:]:           # skip header + units rows
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
    """Return the min/max lat/lon actually returned by ERDDAP for this request."""
    if not rows:
        return {}
    lats = [r["lat"] for r in rows]
    lons = [r["lon"] for r in rows]
    return {
        "lat_min": round(min(lats), 6), "lat_max": round(max(lats), 6),
        "lon_min": round(min(lons), 6), "lon_max": round(max(lons), 6),
    }


def write_bathymetry(session: requests.Session) -> tuple[pathlib.Path, list[dict]]:
    rows = _fetch_bathymetry(session)
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
        "point_count": len(rows),
        "points": rows,
    }
    dest = OUTPUT_DIR / "bathymetry.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, separators=(",", ":"))
    log.info("bathymetry.json written  (%.1f MB)", dest.stat().st_size / 1e6)
    return dest, rows



# ---------------------------------------------------------------------------
# Bathymetry contours — marching squares via contourpy
# ---------------------------------------------------------------------------

# Fishing-relevant depth thresholds in feet.
# These are the lines that matter on the water:
#   30   = nearshore bottom limit (flounder, sea bass)
#   60   = inshore reef / wreck belt
#   100  = king mackerel / cobia transition
#   200  = amberjack / grouper deep edge
#   300  = outer shelf — mahi, wahoo in season
#   600  = shelf break — primary pelagic target zone
#   1000 = upper slope — deep dropfish, swordfish at night
#   1500 = mid-slope
#   2000 = canyon floor / deep water
CONTOUR_DEPTHS_FT = [30, 60, 100, 200, 300, 600, 1000, 1500, 2000]


def _build_grid(rows: list[dict]):
    """
    Convert the flat list of {lat, lon, depth_ft} rows into a 2-D grid.
    Returns (lats, lons, grid_2d) where grid_2d[i][j] is depth at lats[i], lons[j].

    Two classes of NaN exist in the raw GEBCO data at stride 4:
      1. Land cells — elevation >= 0, written as None in rows, must stay NaN
         so contourpy never draws contours across land.
      2. Sparse ocean gaps — grid cells that fall between GEBCO sample points
         due to the stride. These appear as NaN surrounded by valid ocean values
         and cause contourpy to draw tight closing contours (the diamond/spike
         artifact) around each isolated filled cell.

    The fix: for cells that are NaN but have ocean neighbors within a 1-cell
    radius, fill with the average of those neighbors (ocean gap fill).
    For cells that are NaN and have NO ocean neighbors, leave as NaN (land).
    This fills sparse gaps without spreading ocean values onto land.
    """
    import math

    lats_set = sorted(set(r["lat"] for r in rows))
    lons_set = sorted(set(r["lon"] for r in rows))
    lat_idx  = {v: i for i, v in enumerate(lats_set)}
    lon_idx  = {v: i for i, v in enumerate(lons_set)}
    n_rows   = len(lats_set)
    n_cols   = len(lons_set)

    # Step 1: place known ocean depths; land/null cells remain NaN
    flat = [math.nan] * (n_rows * n_cols)
    for r in rows:
        if r["depth_ft"] is not None:
            flat[lat_idx[r["lat"]] * n_cols + lon_idx[r["lon"]]] = r["depth_ft"]

    # Step 2: iterative neighbor-average fill for sparse ocean gaps only.
    # Run up to 6 passes — each pass can fill cells that were filled in the
    # previous pass, gradually closing gaps without ever touching true land.
    for _ in range(6):
        changed = False
        new_flat = flat[:]
        for row in range(n_rows):
            for col in range(n_cols):
                i = row * n_cols + col
                if not math.isnan(flat[i]):
                    continue
                # Gather 4-connected ocean neighbors
                neighbors = []
                for dr, dc in ((-1,0),(1,0),(0,-1),(0,1)):
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

    # Step 3: reshape to 2D — true land cells still NaN, contourpy skips them
    grid = [flat[r * n_cols:(r + 1) * n_cols] for r in range(n_rows)]
    return lats_set, lons_set, grid


def _grid_to_geojson_contours(lats, lons, grid, depth_ft: float) -> list:
    """
    Run marching squares at a single depth threshold and return a list of
    GeoJSON LineString coordinate arrays.
    Uses contourpy (lightweight, no matplotlib dependency).
    contourpy requires z as a 2D array (list of lists) — grid is already
    in that shape from _build_grid, so pass it directly.
    """
    from contourpy import contour_generator

    cg = contour_generator(
        x=lons,
        y=lats,
        z=grid,
        name="serial",
    )
    lines = cg.lines(depth_ft)   # returns list of (N,2) arrays of [lon, lat]

    features = []
    for line in lines:
        if len(line) < 2:
            continue
        coords = [[round(float(pt[0]), 5), round(float(pt[1]), 5)] for pt in line]
        features.append(coords)
    return features


def write_contours(rows: list[dict]) -> pathlib.Path:
    """
    Build contour GeoJSON from the already-fetched bathymetry grid rows.
    No additional network request needed.
    """
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
                "geometry": {
                    "type": "LineString",
                    "coordinates": coords,
                },
                "properties": {
                    "depth_ft":   depth_ft,
                    "depth_label": f"{depth_ft} ft",
                    # Fishing significance for UI tooltips / styling
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
            "dataset":      "GEBCO_2020",
            "source":       "https://coastwatch.pfeg.noaa.gov/erddap/griddap/GEBCO_2020",
            "contour_depths_ft": CONTOUR_DEPTHS_FT,
            "units":        {"depth_ft": "feet below surface"},
            "region": {
                "lat_min": LAT_MIN, "lat_max": LAT_MAX,
                "lon_min": LON_MIN, "lon_max": LON_MAX,
            },
            "actual_extent": {
                "lat_min": round(min(lats), 6), "lat_max": round(max(lats), 6),
                "lon_min": round(min(lons), 6), "lon_max": round(max(lons), 6),
            },
            "note": (
                "Each Feature is a LineString at a fixed depth. "
                "Style by depth_ft property for a nautical chart look: "
                "heavier weight for 100/300/600 ft lines, lighter for others."
            ),
        },
        "feature_count": len(all_features),
        "features": all_features,
    }

    dest = OUTPUT_DIR / "bathymetry_contours.json"
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, separators=(",", ":"))
    log.info("bathymetry_contours.json written  (%d features, %.2f MB)",
             len(all_features), dest.stat().st_size / 1e6)
    return dest

# ---------------------------------------------------------------------------
# Wrecks — NOAA ENC Direct to GIS (encdirect.noaa.gov)
# ---------------------------------------------------------------------------

def _query_enc_layer(session: requests.Session,
                     scale_band: str,
                     url: str) -> list[dict]:
    """
    Page through one ENC wreck layer with a bounding box filter.
    Returns a list of GeoJSON-style feature dicts.
    """
    features = []
    offset = 0
    page_size = 1000
    bbox = f"{LON_MIN},{LAT_MIN},{LON_MAX},{LAT_MAX}"

    while True:
        params = {
            "where":             "1=1",
            "geometry":          bbox,
            "geometryType":      "esriGeometryEnvelope",
            "inSR":              "4326",
            "spatialRel":        "esriSpatialRelIntersects",
            "outFields":         "*",
            "returnGeometry":    "true",
            "f":                 "geojson",
            "resultOffset":      offset,
            "resultRecordCount": page_size,
        }

        log.info("  Querying ENC %s wrecks (offset=%d) …", scale_band, offset)
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            log.warning("  %s query failed at offset %d: %s", scale_band, offset, exc)
            break

        batch = data.get("features", [])
        if not batch:
            break

        for feat in batch:
            props = feat.get("properties") or {}
            geom  = feat.get("geometry", {})
            coords = geom.get("coordinates")
            if not coords or len(coords) < 2:
                continue

            lon_f, lat_f = float(coords[0]), float(coords[1])

            # Depth — ENC uses VALSOU (value of sounding) in metres
            valsou = props.get("VALSOU") or props.get("valsou")
            try:
                depth_ft = round(float(valsou) * 3.28084, 1) if valsou else None
            except (ValueError, TypeError):
                depth_ft = None

            # Wreck category — CATWRK codes:
            # 1=non-dangerous, 2=dangerous, 3=distributed remains,
            # 4=submerged, 5=partly submerged
            catwrk_map = {
                "1": "non-dangerous",
                "2": "dangerous",
                "3": "distributed remains",
                "4": "submerged",
                "5": "partly submerged",
            }
            catwrk_raw = str(props.get("CATWRK") or props.get("catwrk") or "")
            catwrk = catwrk_map.get(catwrk_raw, catwrk_raw or None)

            name = (
                props.get("OBJNAM") or props.get("objnam")
                or props.get("NOBJNM") or props.get("nobjnm")
                or "Unknown"
            )

            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [round(lon_f, 6), round(lat_f, 6)],
                },
                "properties": {
                    "name":       name,
                    "depth_ft":   depth_ft,
                    "wreck_type": catwrk,
                    "scale_band": scale_band,
                    "source":     "NOAA ENC Direct",
                },
            })

        log.info("    %s: %d features so far.", scale_band, len(features))
        if len(batch) < page_size:
            break
        offset += page_size
        time.sleep(0.3)

    return features


def write_wrecks(session: requests.Session) -> pathlib.Path:
    all_features = []
    for scale_band, url in ENC_SERVICES:
        feats = _query_enc_layer(session, scale_band, url)
        all_features.extend(feats)
        log.info("  %s total: %d features.", scale_band, len(feats))

    # Deduplicate by coordinate rounded to 3 decimal places (~100m)
    seen = set()
    unique = []
    for f in all_features:
        key = (
            round(f["geometry"]["coordinates"][0], 3),
            round(f["geometry"]["coordinates"][1], 3),
        )
        if key not in seen:
            seen.add(key)
            unique.append(f)

    log.info("Wrecks: %d unique features (%d before dedup).",
             len(unique), len(all_features))

    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "source":  "NOAA ENC Direct to GIS (encdirect.noaa.gov)",
            "note":    "AWOIS has been retired by NOAA. ENC Direct is the current authoritative source.",
            "updated": "weekly (NOAA updates ENC Direct every Saturday)",
            "region": {
                "lat_min": LAT_MIN, "lat_max": LAT_MAX,
                "lon_min": LON_MIN, "lon_max": LON_MAX,
            },
            "units": {"depth_ft": "feet below surface, null if uncharted"},
            "wreck_types": {
                "non-dangerous":       "charted wreck, not a hazard",
                "dangerous":           "hazard to surface navigation",
                "distributed remains": "scattered wreck debris",
                "submerged":           "fully submerged",
                "partly submerged":    "partially above water",
            },
        },
        "feature_count": len(unique),
        "features": unique,
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
        write_wrecks(session)
    except Exception as exc:
        log.error("Wrecks fetch failed: %s", exc)

    log.info("=== Done ===")


if __name__ == "__main__":
    main()
