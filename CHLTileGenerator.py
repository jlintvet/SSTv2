#!/usr/bin/env python3
"""
CHLTileGenerator.py
====================
EXPERIMENTAL / TEST -- mid_atlantic only.

Generates a chlorophyll (CHL) raster XYZ tile pyramid + a contour-line
GeoJSON from the existing chl_composite.json canonical-grid bundle
(produced by CHLSeaColorBundler.py), instead of relying on the frontend's
single stretched-canvas Leaflet imageOverlay.

Why this exists
----------------
See SST_RENDERING.md problem #8 ("CHL and Sea Color overlay edges are hard
staircase walls"). The frontend currently renders CHL via bilinear
interpolation on a fixed 1280x1000 canvas, then applies a flat
`blur(4px)` CSS filter (see `blurOverlay` in glSandwich.js) to hide the
4km/2km cell-boundary artifacts. That blur can't tell a real chlorophyll
front from a grid-cell edge, so it smooths both together -- which is why
CHL looks softer than SST at every zoom, not just at the boundaries.

This script instead resamples with GDAL lanczos at each zoom level (the
same technique already used by BathyTileGenerator.py for the Shaded Relief
raster), so CHL can look sharp at every zoom without an indiscriminate
blur, plus draws real contour "fronts" (log-scale mg/m3 levels) as vector
lines -- which read as sharp regardless of the underlying grid coarseness.

Pipeline: fetch chl_composite.json (0.02 deg canonical grid, mid_atlantic)
          -> bounded nearest-neighbor gap-fill (mirrors gapFillGrid's
             "don't invent data far from an observation" philosophy)
          -> VRT -> gdaldem color-relief (pseudo-log CHL ramp, alpha at
             NODATA) -> gdal2tiles (lanczos) -> upload PNG tiles to S3.
          -> contourpy contour lines at CONTOUR_LEVELS_MGM3, Chaikin-smoothed
             -> upload contours.json to S3 alongside the tiles.

Mid-atlantic only for this test -- not wired into the daily workflow.
Manual workflow_dispatch only (chl-tiles.yml), same pattern as
bathy-tiles.yml. Do NOT extend to other regions or schedule this until
Jon has reviewed the test tiles.

Usage:
    python CHLTileGenerator.py

System requirements (installed in GitHub Actions workflow):
    sudo apt-get install -y gdal-bin

Python requirements:
    pip install requests numpy Pillow boto3 scipy contourpy
"""

import os
import sys
import json
import time
import shutil
import logging
import subprocess
from pathlib import Path
from glob import glob

import numpy as np
import requests
import boto3
from PIL import Image

try:
    from scipy.ndimage import distance_transform_edt, gaussian_filter
    _SCIPY_AVAILABLE = True
except ImportError:
    _SCIPY_AVAILABLE = False

try:
    from contourpy import contour_generator
    _CONTOURPY_AVAILABLE = True
except ImportError:
    _CONTOURPY_AVAILABLE = False


# ── Logging ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

Image.MAX_IMAGE_PIXELS = None

# ── Region config -- mid_atlantic only for this test ──────────────────────
# Must match CHLSeaColorBundler.py's mid_atlantic bounds + GRID_STEP exactly,
# since we consume its canonical-grid output directly (no resampling of our
# own at fetch time).
REGION_CONFIGS = {
    "mid_atlantic": {
        "lat_min": 33.70, "lat_max": 39.00,
        "lon_min": -78.89, "lon_max": -72.21,
    },
}
GRID_STEP = 0.02  # degrees -- must match CHLSeaColorBundler.py

# ── Source data ────────────────────────────────────────────────────────────
CHL_COMPOSITE_URL = (
    "https://raw.githubusercontent.com/jlintvet/SSTv2/main/"
    "SSTv2/Chlorophyll/Bundled/chl_composite.json"
)

# ── Tile output ─────────────────────────────────────────────────────────────
ZOOM_MIN = 5
ZOOM_MAX = 11

# ── AWS / CloudFront -- reuses the existing bathy-tiles bucket + distribution
# (already has working credentials/invalidation wired up), new key prefix. ──
S3_BUCKET  = "sst-bathy-tiles"
CLOUDFRONT = "https://d3qy1jhzqojgwx.cloudfront.net"
S3_PREFIX  = "chl"   # -> chl/{region}/{z}/{x}/{y}.png, chl/{region}/contours.json

# ── NODATA sentinel (CHL mg/m3 is always >= 0, so -1 is safe) ──────────────
NODATA = -1.0

# ── Gap-fill cap ────────────────────────────────────────────────────────────
# Cells farther than this from a real observation stay transparent rather
# than being filled -- mirrors gapFillGrid's MAX_FILL_DIST philosophy
# (frontend, SST): smooth over small gaps for visual continuity, but never
# invent data far from an observation. 15 cells * 0.02 deg ~= 0.3 deg
# ~= 33 km at this latitude.
MAX_FILL_CELLS = 15

# ── Chlorophyll color ramp (mg/m3) ──────────────────────────────────────────
# gdaldem color-relief only linearly interpolates between adjacent stops, so
# there's no true "log color-relief" mode -- clustering stops near the low
# end approximates the standard log-scale ocean-color palette (NASA/ESA CHL
# convention: deep blue/purple = oligotrophic open ocean, green/yellow =
# moderate, orange/red/brown = eutrophic or turbid coastal water).
#
# If this becomes a real (non-test) feature, keep CHL_COLOR_STOPS below in
# sync with a matching CHL_GRADIENT in SSTLive.jsx -- same pattern already
# enforced for SLA_STOPS/SLA_GRADIENT (see SST_RENDERING.md problem #9).
CHL_COLOR_STOPS = [
    (0.02,  (35,  10,  85)),   # deep purple  -- ultra-oligotrophic
    (0.05,  (35,  55, 150)),
    (0.10,  (25, 110, 205)),
    (0.20,  (15, 150, 205)),
    (0.30,  (15, 180, 170)),
    (0.50,  (35, 190, 115)),
    (1.00,  (95, 200,  55)),
    (2.00,  (175, 210, 35)),
    (3.00,  (230, 205, 25)),
    (5.00,  (240, 165, 15)),
    (10.00, (235, 105, 15)),
    (20.00, (215,  55, 20)),
    (30.00, (175,  25, 25)),
    (60.00, (110,  10, 20)),   # deep red-brown -- highly turbid/eutrophic
]

# Contour levels (mg/m3) -- standard log-scale CHL stops per Jon's call
# (2026-07-26), same convention NASA/NOAA ocean-color products use.
CONTOUR_LEVELS_MGM3 = [0.1, 0.3, 1, 3, 10, 30]


# ──────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────

def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command, stream output to log."""
    log.info("$ %s", " ".join(str(c) for c in cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout.strip():
        for line in result.stdout.strip().splitlines():
            log.info("  %s", line)
    if result.stderr.strip():
        for line in result.stderr.strip().splitlines():
            log.warning("  %s", line)
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed (rc={result.returncode}): {' '.join(str(c) for c in cmd)}")
    return result


def gdal2tiles_cmd() -> str:
    """Locate gdal2tiles -- name differs by distro/version."""
    for name in ("gdal2tiles.py", "gdal2tiles"):
        if shutil.which(name):
            return name
    raise RuntimeError("gdal2tiles not found. Install gdal-bin.")


# ──────────────────────────────────────────────────────────────────────────
# Step 1 -- Fetch chl_composite.json and reshape onto a north-first grid
# ──────────────────────────────────────────────────────────────────────────

def fetch_chl_composite(region: str) -> tuple[np.ndarray, dict]:
    """
    Fetch the existing canonical-grid CHL composite (already built by
    CHLSeaColorBundler.py -- we do NOT re-fetch from CMEMS here).

    Returns (grid, geo):
      - grid: float32 [n_rows x n_cols], NaN = no observation, rows
        north -> south (row 0 = northmost), matching the convention
        write_vrt() expects (same as BathyTileGenerator.py).
      - geo: dict with lat/lon bounds, grid dims, res_deg, plus the
        composite's own metadata (generated timestamp, coverage_pct) for
        the manifest.
    """
    cfg = REGION_CONFIGS[region]
    log.info("Fetching CHL composite: %s", CHL_COMPOSITE_URL)
    resp = requests.get(CHL_COMPOSITE_URL, timeout=60)
    resp.raise_for_status()
    comp = resp.json()

    lat_set = comp["latSet"]   # ascending (south -> north)
    lon_set = comp["lonSet"]   # ascending (west -> east)
    chl_flat = comp["chl"]     # flat, row-major: chl[i * n_lons + j] -> (latSet[i], lonSet[j])
    n_lats, n_lons = len(lat_set), len(lon_set)

    if abs(lat_set[0] - cfg["lat_min"]) > 0.001 or abs(lat_set[-1] - cfg["lat_max"]) > 0.001:
        raise RuntimeError(
            f"chl_composite.json bounds don't match REGION_CONFIGS['{region}']: "
            f"got lat {lat_set[0]}-{lat_set[-1]}, expected {cfg['lat_min']}-{cfg['lat_max']}"
        )
    if abs(lon_set[0] - cfg["lon_min"]) > 0.001 or abs(lon_set[-1] - cfg["lon_max"]) > 0.001:
        raise RuntimeError(
            f"chl_composite.json bounds don't match REGION_CONFIGS['{region}']: "
            f"got lon {lon_set[0]}-{lon_set[-1]}, expected {cfg['lon_min']}-{cfg['lon_max']}"
        )

    # South-first 2D array (row 0 = southmost, matches ascending latSet), then
    # flip to north-first for the VRT/GeoTransform convention.
    arr = np.full((n_lats, n_lons), np.nan, dtype=np.float32)
    for i in range(n_lats):
        row = chl_flat[i * n_lons:(i + 1) * n_lons]
        arr[i, :] = [np.nan if v is None else v for v in row]
    arr_north_first = arr[::-1, :].copy()

    valid = arr[~np.isnan(arr)]
    log.info("CHL composite: %d x %d grid, %d/%d valid cells (%.1f%%), generated %s",
             n_lats, n_lons, valid.size, arr.size, 100.0 * valid.size / arr.size,
             comp.get("generated"))

    geo = dict(
        lat_min=cfg["lat_min"], lat_max=cfg["lat_max"],
        lon_min=cfg["lon_min"], lon_max=cfg["lon_max"],
        n_rows=n_lats, n_cols=n_lons, res_deg=GRID_STEP,
        lat_set=lat_set, lon_set=lon_set,
        generated=comp.get("generated"),
        coverage_pct=comp.get("coverage_pct"),
        window_days=comp.get("window_days"),
    )
    return arr_north_first, geo


# ──────────────────────────────────────────────────────────────────────────
# Step 2 -- Bounded gap-fill (nearest valid neighbor, capped distance)
# ──────────────────────────────────────────────────────────────────────────

def gap_fill_bounded(grid: np.ndarray, max_cells: int) -> np.ndarray:
    """
    Fill NaN cells with a normalized Gaussian-weighted blend of nearby valid
    cells, but only within `max_cells` (Euclidean, grid units) of an actual
    observation -- cells farther than that stay NaN (rendered transparent).
    This is a display-continuity fill, not a fabrication of unseen data,
    mirroring the frontend's existing gapFillGrid MAX_FILL_DIST cap.

    IMPORTANT: this must be a smooth blend, not nearest-neighbor lookup.
    chl_composite.json is very sparse (~16% raw coverage at the time this
    was written, real observations ~25 cells apart on average) -- a pure
    nearest-neighbor fill (distance_transform_edt's return_indices) turns
    into a Voronoi tessellation: every gap cell copies whichever single
    observation happens to be closest, with a hard seam exactly at the
    boundary between two observations' "territory." That produced the
    large flat-colored blobs with sharp circular edges seen in the first
    version of this script's test tiles. The Gaussian-weighted approach
    below (same NODATA-safe normalized-convolution technique as
    BathyTileGenerator.py's _smooth_elevation) blends ALL nearby valid
    cells with distance-based weight instead of picking one owner, which
    verified against the real composite data reduced the p99 adjacent-cell
    jump from ~28-30 mg/m3 down to ~11-16 for the same fill area.
    """
    invalid = np.isnan(grid)
    n_invalid = int(invalid.sum())
    if n_invalid == 0:
        log.info("Gap-fill: no NaN cells, nothing to do")
        return grid
    if not _SCIPY_AVAILABLE:
        log.warning("scipy not installed -- skipping gap-fill (%d NaN cells stay transparent)", n_invalid)
        return grid

    valid = ~invalid
    # sigma heuristic: most of the Gaussian's weight falls within max_cells,
    # so the effective fill radius roughly matches the old hard cap.
    sigma = max_cells / 2.5
    values = np.where(valid, grid, 0.0).astype(np.float32)
    weights = valid.astype(np.float32)
    fv = gaussian_filter(values, sigma=sigma)
    fw = gaussian_filter(weights, sigma=sigma)
    with np.errstate(invalid="ignore", divide="ignore"):
        blended = np.where(fw > 1e-6, fv / fw, np.nan)

    dist = distance_transform_edt(invalid)
    fillable = invalid & (dist <= max_cells) & ~np.isnan(blended)
    out = np.where(fillable, blended, grid)

    n_filled = int(fillable.sum())
    n_remaining = int(np.isnan(out).sum())
    log.info("Gap-fill: %d/%d NaN cells filled (Gaussian blend, sigma=%.1f, within %d cells of an observation), %d remain transparent",
             n_filled, n_invalid, sigma, max_cells, n_remaining)
    return out


# ──────────────────────────────────────────────────────────────────────────
# Step 3 -- Write elevation-style VRT (no GDAL Python bindings needed)
# ──────────────────────────────────────────────────────────────────────────

def write_vrt(grid: np.ndarray, geo: dict, workdir: Path) -> Path:
    """
    Write the CHL grid (NaN -> NODATA sentinel) as float32 raw binary +
    GDAL VRT sidecar. Same approach as BathyTileGenerator.py's write_vrt().
    """
    flt_path = workdir / "chl.flt"
    vrt_path = workdir / "chl.vrt"

    out = np.where(np.isnan(grid), NODATA, grid).astype("<f4")
    out.tofile(flt_path)

    n_rows, n_cols = geo["n_rows"], geo["n_cols"]
    res = geo["res_deg"]
    lon_min, lat_max = geo["lon_min"], geo["lat_max"]

    gt_x0, gt_xres = lon_min, res
    gt_y0, gt_yres = lat_max, -res
    line_offset = n_cols * 4

    vrt_xml = f"""\
<VRTDataset rasterXSize="{n_cols}" rasterYSize="{n_rows}">
  <SRS dataAxisToSRSAxisMapping="2,1">GEOGCS["WGS 84",DATUM["WGS_1984",\
SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],\
UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],\
AUTHORITY["EPSG","4326"]]</SRS>
  <GeoTransform>{gt_x0:.8f}, {gt_xres:.10f}, 0, \
{gt_y0:.8f}, 0, {gt_yres:.10f}</GeoTransform>
  <VRTRasterBand dataType="Float32" band="1" subClass="VRTRawRasterBand">
    <SourceFilename relativeToVRT="1">chl.flt</SourceFilename>
    <ImageOffset>0</ImageOffset>
    <PixelOffset>4</PixelOffset>
    <LineOffset>{line_offset}</LineOffset>
    <ByteOrder>LSB</ByteOrder>
    <NoDataValue>{NODATA}</NoDataValue>
  </VRTRasterBand>
</VRTDataset>
"""
    vrt_path.write_text(vrt_xml)
    log.info("Wrote VRT: %s (%d x %d)", vrt_path.name, n_cols, n_rows)
    return vrt_path


# ──────────────────────────────────────────────────────────────────────────
# Step 4 -- GDAL color-relief (no hillshade -- this is a concentration map,
# not terrain; NODATA cells become transparent via -alpha + the "nv" stop)
# ──────────────────────────────────────────────────────────────────────────

def build_color_ramp_text() -> str:
    lines = [f"{val:<8g}{r:>4}{g:>4}{b:>4}" for val, (r, g, b) in CHL_COLOR_STOPS]
    lines.append("nv        0   0   0")
    return "\n".join(lines) + "\n"


def generate_color_relief(vrt: Path, workdir: Path) -> Path:
    colors_path = workdir / "chl_colors.txt"
    colors_path.write_text(build_color_ramp_text())
    out = workdir / "chl_color_relief.tif"
    run([
        "gdaldem", "color-relief", str(vrt), str(colors_path), str(out),
        "-of", "GTiff",
        "-alpha",   # NODATA (the "nv" stop) -> alpha 0, transparent
    ])
    return out


# ──────────────────────────────────────────────────────────────────────────
# Step 5 -- Tile generation
# ──────────────────────────────────────────────────────────────────────────

def generate_tiles(color_relief_tif: Path, tiles_dir: Path) -> None:
    """Run gdal2tiles to produce XYZ PNG tiles at zoom ZOOM_MIN-ZOOM_MAX,
    lanczos-resampled -- the whole point of this test: proper per-zoom
    resampling instead of one client-side stretched canvas + CSS blur."""
    tiles_dir.mkdir(parents=True, exist_ok=True)
    run([
        gdal2tiles_cmd(),
        "-z", f"{ZOOM_MIN}-{ZOOM_MAX}",
        "-r", "lanczos",
        "--xyz",
        str(color_relief_tif),
        str(tiles_dir),
    ])
    n_tiles = len(glob(str(tiles_dir / "**/*.png"), recursive=True))
    log.info("Generated %d tiles in %s", n_tiles, tiles_dir)


# ──────────────────────────────────────────────────────────────────────────
# Step 6 -- Contour lines (log-scale CHL fronts)
# ──────────────────────────────────────────────────────────────────────────

def _chaikin_smooth(coords: list, iterations: int = 2) -> list:
    """Corner-cutting smoothing -- identical algorithm to
    StaticLayersRetrieval.py's _chaikin_smooth (bathymetry contours)."""
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


def _extract_contour_lines(lats: list, lons: list, grid_ma: np.ma.MaskedArray, level: float) -> list[list]:
    cg = contour_generator(x=lons, y=lats, z=grid_ma, corner_mask=True)
    lines = cg.lines(level)
    MIN_POINTS = 6
    output = []
    for line in lines:
        if len(line) < MIN_POINTS:
            continue
        coords = [[float(p[0]), float(p[1])] for p in line]
        coords = _chaikin_smooth(coords, iterations=2)
        if len(coords) >= MIN_POINTS:
            output.append(coords)
    return output


def build_contours(grid_north_first: np.ndarray, geo: dict) -> dict:
    """
    Contour lines at CONTOUR_LEVELS_MGM3, drawn over the (gap-filled) grid.
    Grid must be north-first (row 0 = north) to match write_vrt(); contourpy
    wants y ascending, so we flip back to south-first + ascending lats here.
    """
    if not _CONTOURPY_AVAILABLE:
        log.warning("contourpy not installed -- skipping contour generation")
        return {"type": "FeatureCollection", "features": []}

    lats = geo["lat_set"]     # ascending
    lons = geo["lon_set"]     # ascending
    grid_south_first = grid_north_first[::-1, :]   # row 0 = south, matches ascending lats
    grid_ma = np.ma.masked_invalid(grid_south_first)

    features = []
    for level in CONTOUR_LEVELS_MGM3:
        lines = _extract_contour_lines(lats, lons, grid_ma, level)
        for coords in lines:
            features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coords},
                "properties": {"value_mgm3": level, "label": f"{level} mg/m³"},
            })
        log.info("  contour %.2g mg/m3 -- %d segments", level, len(lines))

    return {"type": "FeatureCollection", "features": features}


# ──────────────────────────────────────────────────────────────────────────
# Step 7 -- Upload to S3
# ──────────────────────────────────────────────────────────────────────────

def upload_tiles(tiles_dir: Path, region: str) -> None:
    s3 = boto3.client("s3")
    tile_paths = sorted(glob(str(tiles_dir / "**/*.png"), recursive=True))
    total = len(tile_paths)
    log.info("Uploading %d tiles -> s3://%s/%s/%s/ ...", total, S3_BUCKET, S3_PREFIX, region)

    uploaded, errors = 0, 0
    t0 = time.time()
    for i, local_path in enumerate(tile_paths, 1):
        rel = Path(local_path).relative_to(tiles_dir)
        s3_key = f"{S3_PREFIX}/{region}/{rel.as_posix()}"
        try:
            s3.upload_file(
                local_path, S3_BUCKET, s3_key,
                ExtraArgs={"ContentType": "image/png", "CacheControl": "max-age=86400"},
            )
            uploaded += 1
        except Exception as exc:
            log.error("Failed to upload %s: %s", s3_key, exc)
            errors += 1
        if i % 500 == 0 or i == total:
            elapsed = time.time() - t0
            rate = uploaded / elapsed if elapsed > 0 else 0
            log.info("  %d/%d uploaded (%.1f/s, %d errors)", i, total, rate, errors)

    log.info("Tile upload complete: %d ok, %d errors", uploaded, errors)
    if errors:
        raise RuntimeError(f"{errors} tiles failed to upload -- check S3 permissions.")


def upload_json(payload: dict, region: str, filename: str) -> None:
    s3 = boto3.client("s3")
    s3_key = f"{S3_PREFIX}/{region}/{filename}"
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    s3.put_object(
        Bucket=S3_BUCKET, Key=s3_key, Body=body,
        ContentType="application/json", CacheControl="max-age=3600",
    )
    log.info("Uploaded %s (%.1f KB) -> s3://%s/%s", filename, len(body) / 1024, S3_BUCKET, s3_key)


def invalidate_cloudfront(region: str) -> None:
    dist_id = os.environ.get("CLOUDFRONT_DISTRIBUTION_ID", "").strip()
    if not dist_id:
        log.warning("CLOUDFRONT_DISTRIBUTION_ID not set -- tiles/JSON cached until TTL expires")
        return
    try:
        cf = boto3.client("cloudfront")
        cf.create_invalidation(
            DistributionId=dist_id,
            InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": [f"/{S3_PREFIX}/{region}/*"]},
                "CallerReference": str(int(time.time())),
            },
        )
        log.info("CloudFront invalidation submitted: /%s/%s/*", S3_PREFIX, region)
    except Exception as exc:
        log.warning("CloudFront invalidation failed: %s", exc)


# ──────────────────────────────────────────────────────────────────────────
# Full pipeline
# ──────────────────────────────────────────────────────────────────────────

def process_region(region: str) -> None:
    log.info("=" * 60)
    log.info("CHL tile test: region=%s", region)
    log.info("=" * 60)

    workdir = Path(f"/tmp/chltiles_{region}")
    tiles_dir = workdir / "tiles"
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)

    try:
        t0 = time.time()
        grid_nf, geo = fetch_chl_composite(region)

        grid_filled_nf = gap_fill_bounded(grid_nf, MAX_FILL_CELLS)

        vrt = write_vrt(grid_filled_nf, geo, workdir)

        t1 = time.time()
        cr_tif = generate_color_relief(vrt, workdir)
        log.info("Color-relief: %.1fs", time.time() - t1)

        t2 = time.time()
        generate_tiles(cr_tif, tiles_dir)
        log.info("Tiling: %.1fs", time.time() - t2)

        t3 = time.time()
        contours = build_contours(grid_filled_nf, geo)
        log.info("Contours: %.1fs (%d total features)", time.time() - t3, len(contours["features"]))

        t4 = time.time()
        upload_tiles(tiles_dir, region)
        upload_json(contours, region, "contours.json")
        manifest = {
            "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source_composite_generated": geo.get("generated"),
            "source_composite_coverage_pct": geo.get("coverage_pct"),
            "region": region,
            "zoom_min": ZOOM_MIN,
            "zoom_max": ZOOM_MAX,
            "gap_fill_max_cells": MAX_FILL_CELLS,
            "color_stops_mgm3": [v for v, _ in CHL_COLOR_STOPS],
            "contour_levels_mgm3": CONTOUR_LEVELS_MGM3,
            "tile_url": f"{CLOUDFRONT}/{S3_PREFIX}/{region}/{{z}}/{{x}}/{{y}}.png",
            "contours_url": f"{CLOUDFRONT}/{S3_PREFIX}/{region}/contours.json",
        }
        upload_json(manifest, region, "manifest.json")
        log.info("Upload: %.1fs", time.time() - t4)

        invalidate_cloudfront(region)

        log.info("Done: %s complete in %.1fs total", region, time.time() - t0)
        log.info("Tile URL:     %s/%s/%s/{z}/{x}/{y}.png", CLOUDFRONT, S3_PREFIX, region)
        log.info("Contours URL: %s/%s/%s/contours.json", CLOUDFRONT, S3_PREFIX, region)
        log.info("Manifest URL: %s/%s/%s/manifest.json", CLOUDFRONT, S3_PREFIX, region)

    finally:
        if workdir.exists():
            shutil.rmtree(workdir)
            log.info("Cleaned up %s", workdir)


if __name__ == "__main__":
    region_env = os.environ.get("REGION", "mid_atlantic").strip()
    if region_env != "mid_atlantic":
        log.error(
            "This script only supports REGION=mid_atlantic right now -- "
            "it's an experimental test (see CHLTileGenerator.py docstring). "
            "Got REGION=%r.", region_env
        )
        sys.exit(1)

    process_region(region_env)
    log.info("All done.")
