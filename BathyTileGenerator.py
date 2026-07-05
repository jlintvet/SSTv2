#!/usr/bin/env python3
"""
BathyTileGenerator.py
=====================
Generates bathymetric imagery XYZ tiles from NCEI CRM 2023 via OPeNDAP.
Pipeline: OPeNDAP fetch → merged elevation GeoTIFF → hillshade + color-relief
          → multiply blend + ocean-alpha mask → gdal2tiles → S3 upload.

Usage:
    python BathyTileGenerator.py                # mid_atlantic
    REGION=ga_sc python BathyTileGenerator.py
    REGION=all python BathyTileGenerator.py

System requirements (installed in GitHub Actions workflow):
    sudo apt-get install -y gdal-bin

Python requirements:
    pip install netCDF4 numpy Pillow boto3
"""

import os
import sys
import subprocess
import shutil
import struct
import time
import logging
from pathlib import Path
from glob import glob

import numpy as np
import netCDF4
import boto3
from PIL import Image

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# PIL decompression bomb limit — our rasters are ~115M pixels (legitimate, not an attack)
Image.MAX_IMAGE_PIXELS = None

# ── Region config ──────────────────────────────────────────────────────────
REGION_CONFIGS = {
    "mid_atlantic": {
        "lat_min": 33.70, "lat_max": 39.00,
        "lon_min": -78.89, "lon_max": -72.21,
    },
    "ga_sc": {
        "lat_min": 29.80, "lat_max": 35.20,
        "lon_min": -82.00, "lon_max": -75.20,
    },
}

# ── CRM source ─────────────────────────────────────────────────────────────
# stride=1 → 1 arc-sec (~30m)  Full resolution, ~460MB in RAM per region
# stride=2 → 2 arc-sec (~60m)  Half resolution, ~115MB — default for safety
# At zoom 5-12 the visual difference is imperceptible; bump to 1 for max detail.
CRM_STRIDE = int(os.environ.get("CRM_STRIDE", "2"))

CRM_BASE = "https://www.ngdc.noaa.gov/thredds/dodsC/crm/cudem/"
CRM_VOLUMES = [
    # (filename,       lat_min, lat_max, lon_min, lon_max)
    ("crm_vol1_2023.nc", 39.0,  46.0,  -77.0,  -65.0),  # NE Atlantic
    ("crm_vol2_2023.nc", 32.0,  39.0,  -83.0,  -68.0),  # SE Atlantic
    ("crm_vol3_2023.nc", 24.0,  32.0,  -84.0,  -76.0),  # FL / E Gulf
]

# ── Tile output ────────────────────────────────────────────────────────────
ZOOM_MIN = 5
ZOOM_MAX = 12

# ── AWS / CloudFront ───────────────────────────────────────────────────────
S3_BUCKET    = "sst-bathy-tiles"
CLOUDFRONT   = "https://d3qy1jhzqojgwx.cloudfront.net"

# ── Color ramp (elevation in meters; negative = below sea level) ───────────
# Format: elevation_m  R  G  B
# nv = nodata → transparent (alpha=0 handled separately in blend step)
COLOR_RAMP = """\
200    230 215 175
50     218 205 160
10     210 195 145
0      200 185 135
-1     188 175 125
-5     165 198 158
-15    122 192 162
-30    88  182 170
-60    70  172 186
-100   56  158 196
-200   44  136 190
-310   34  108 178
-366   28  100 170
-600   20   78 158
-914   12   56 136
-1829   6   34  94
-3000   3   18  58
-6000   1    8  30
nv      0    0   0
"""

# ── NODATA sentinel ────────────────────────────────────────────────────────
NODATA = -9999.0

# ── OPeNDAP chunk size ─────────────────────────────────────────────────────
# The CRM server times out on requests > ~5M cells. At stride 2 the full
# mid_atlantic region is ~115M cells — must be fetched in lat chunks.
# 0.25° × (1800/stride2) rows × 12025 cols ≈ 2.7M cells per chunk → safe.
LAT_CHUNK_DEG = 0.25


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
    """Locate gdal2tiles — name differs by distro/version."""
    for name in ("gdal2tiles.py", "gdal2tiles"):
        if shutil.which(name):
            return name
    raise RuntimeError("gdal2tiles not found. Install gdal-bin.")


# ──────────────────────────────────────────────────────────────────────────
# Step 1 — Fetch CRM elevation data via OPeNDAP
# ──────────────────────────────────────────────────────────────────────────

def _place_chunk(master: np.ndarray, z_arr: np.ndarray,
                 fetch_lats: np.ndarray, fetch_lons: np.ndarray,
                 lat_max: float, lon_min: float,
                 res_deg: float, n_rows: int, n_cols: int) -> int:
    """Place a fetched z chunk into the master grid. Returns cell count placed."""
    # Ensure rows run north → south
    if len(fetch_lats) > 1 and fetch_lats[0] < fetch_lats[-1]:
        z_arr      = z_arr[::-1]
        fetch_lats = fetch_lats[::-1]

    n_r, n_c = z_arr.shape
    row0 = round((lat_max - fetch_lats[0]) / res_deg)
    col0 = round((fetch_lons[0]  - lon_min)  / res_deg)

    row_end = min(row0 + n_r, n_rows)
    col_end = min(col0 + n_c, n_cols)
    r_src   = row_end - row0
    c_src   = col_end - col0

    if r_src <= 0 or c_src <= 0:
        return 0

    master[row0:row_end, col0:col_end] = z_arr[:r_src, :c_src]
    return r_src * c_src


def _clean_z(z_raw) -> np.ndarray:
    """Unmask and replace fill/NaN with NODATA sentinel."""
    if hasattr(z_raw, 'filled'):
        arr = z_raw.filled(np.nan).astype(np.float32)
    else:
        arr = np.asarray(z_raw, dtype=np.float32)
    arr[arr <= -99990.0] = np.nan
    return np.where(np.isnan(arr), NODATA, arr)


def fetch_crm_region(lat_min: float, lat_max: float,
                     lon_min: float, lon_max: float) -> tuple[np.ndarray, dict]:
    """
    Fetch CRM 2023 elevation data for the given bbox via OPeNDAP.

    The CRM OPeNDAP server times out on requests larger than ~5M cells.
    At stride 2 the full mid_atlantic region is ~115M cells, so we chunk
    the lat dimension into LAT_CHUNK_DEG pieces (~2.7M cells each) and
    reopen the dataset for each chunk to avoid persistent connection issues.

    Returns (elevation_array, geo_info):
      - elevation_array: float32 [n_rows × n_cols], rows north→south
      - geo_info: dict with lat/lon bounds, grid dimensions, res_deg
    """
    res_deg = CRM_STRIDE / 3600.0

    n_rows = round((lat_max - lat_min) / res_deg) + 1
    n_cols = round((lon_max - lon_min) / res_deg) + 1
    master = np.full((n_rows, n_cols), NODATA, dtype=np.float32)

    log.info("Master grid: %d rows × %d cols (%.0f MB)",
             n_rows, n_cols, n_rows * n_cols * 4 / 1e6)

    any_data = False

    for vol_file, v_lat_min, v_lat_max, v_lon_min, v_lon_max in CRM_VOLUMES:
        olat_min = max(lat_min, v_lat_min)
        olat_max = min(lat_max, v_lat_max)
        olon_min = max(lon_min, v_lon_min)
        olon_max = min(lon_max, v_lon_max)
        if olat_min >= olat_max or olon_min >= olon_max:
            continue

        url = CRM_BASE + vol_file
        log.info("Volume %s  lat %.2f–%.2f  lon %.2f–%.2f",
                 vol_file, olat_min, olat_max, olon_min, olon_max)

        # Read axis arrays once (small — no timeout risk)
        try:
            ds0 = netCDF4.Dataset(url)
            vol_lats = np.array(ds0.variables['lat'][:])
            vol_lons = np.array(ds0.variables['lon'][:])
            ds0.close()
        except Exception as exc:
            log.warning("Cannot open %s: %s — skipping", vol_file, exc)
            continue

        lon_idx = np.where(
            (vol_lons >= olon_min - 1e-7) & (vol_lons <= olon_max + 1e-7)
        )[0]
        if len(lon_idx) == 0:
            log.warning("No lon overlap in %s", vol_file)
            continue
        lo0, lo1    = int(lon_idx[0]), int(lon_idx[-1])
        fetch_lons  = vol_lons[lo0:lo1+1:CRM_STRIDE]

        # Chunk lat dimension to stay under OPeNDAP timeout
        chunk_start = olat_min
        chunk_num   = 0
        while chunk_start < olat_max - 1e-7:
            chunk_end = min(chunk_start + LAT_CHUNK_DEG, olat_max)

            lat_idx = np.where(
                (vol_lats >= chunk_start - 1e-7) & (vol_lats <= chunk_end + 1e-7)
            )[0]
            if len(lat_idx) == 0:
                chunk_start = chunk_end
                continue

            li0, li1 = int(lat_idx[0]), int(lat_idx[-1])
            fetch_lats_chunk = vol_lats[li0:li1+1:CRM_STRIDE]

            # Retry each chunk up to 3× on DAP failure
            z_arr = None
            for attempt in range(3):
                try:
                    ds = netCDF4.Dataset(url)
                    z_raw = ds.variables['z'][li0:li1+1:CRM_STRIDE,
                                              lo0:lo1+1:CRM_STRIDE]
                    ds.close()
                    z_arr = _clean_z(z_raw)
                    break
                except Exception as exc:
                    try:
                        ds.close()
                    except Exception:
                        pass
                    if attempt < 2:
                        wait = 10 * (attempt + 1)
                        log.warning("  Chunk %d attempt %d failed: %s — retry in %ds",
                                    chunk_num, attempt + 1, exc, wait)
                        time.sleep(wait)
                    else:
                        log.error("  Chunk %d failed after 3 attempts — skipping",
                                  chunk_num)

            if z_arr is not None:
                placed = _place_chunk(master, z_arr, fetch_lats_chunk, fetch_lons,
                                      lat_max, lon_min, res_deg, n_rows, n_cols)
                log.info("  Chunk %2d  lat %.3f–%.3f  placed %d cells",
                         chunk_num, chunk_start, chunk_end, placed)
                any_data = True

            chunk_start = chunk_end
            chunk_num  += 1

    if not any_data:
        raise RuntimeError("CRM fetch returned no data — check OPeNDAP connectivity.")

    ocean_cells = int(np.sum((master < 0) & (master != NODATA)))
    log.info("Merged grid complete: %d ocean cells", ocean_cells)

    geo = dict(lat_min=lat_min, lat_max=lat_max,
               lon_min=lon_min, lon_max=lon_max,
               n_rows=n_rows, n_cols=n_cols, res_deg=res_deg)
    return master, geo


# ──────────────────────────────────────────────────────────────────────────
# Step 2 — Write elevation as raw binary + VRT (no GDAL Python bindings)
# ──────────────────────────────────────────────────────────────────────────

def write_vrt(elev: np.ndarray, geo: dict, workdir: Path) -> Path:
    """
    Write elevation array as float32 raw binary + GDAL VRT sidecar.
    Returns path to the VRT file (usable by all GDAL command-line tools).
    """
    flt_path = workdir / "elevation.flt"
    vrt_path = workdir / "elevation.vrt"

    # Write raw float32, C order (row-major), little-endian
    elev.astype('<f4').tofile(flt_path)

    n_rows, n_cols = geo['n_rows'], geo['n_cols']
    res = geo['res_deg']
    lon_min, lat_max = geo['lon_min'], geo['lat_max']

    # GeoTransform: top-left corner, pixel size
    gt_x0    = lon_min
    gt_xres  = res
    gt_y0    = lat_max
    gt_yres  = -res   # negative — rows go south

    line_offset = n_cols * 4   # bytes per row (float32)

    vrt_xml = f"""\
<VRTDataset rasterXSize="{n_cols}" rasterYSize="{n_rows}">
  <SRS dataAxisToSRSAxisMapping="2,1">GEOGCS["WGS 84",DATUM["WGS_1984",\
SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],\
UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],\
AUTHORITY["EPSG","4326"]]</SRS>
  <GeoTransform>{gt_x0:.8f}, {gt_xres:.10f}, 0, \
{gt_y0:.8f}, 0, {gt_yres:.10f}</GeoTransform>
  <VRTRasterBand dataType="Float32" band="1" subClass="VRTRawRasterBand">
    <SourceFilename relativeToVRT="1">elevation.flt</SourceFilename>
    <ImageOffset>0</ImageOffset>
    <PixelOffset>4</PixelOffset>
    <LineOffset>{line_offset}</LineOffset>
    <ByteOrder>LSB</ByteOrder>
    <NoDataValue>{NODATA}</NoDataValue>
  </VRTRasterBand>
</VRTDataset>
"""
    vrt_path.write_text(vrt_xml)
    log.info("Wrote VRT: %s (%d × %d)", vrt_path.name, n_cols, n_rows)
    return vrt_path


# ──────────────────────────────────────────────────────────────────────────
# Step 3 — GDAL hillshade + color-relief
# ──────────────────────────────────────────────────────────────────────────

def generate_hillshade(vrt: Path, workdir: Path) -> Path:
    out = workdir / "hillshade.tif"
    run([
        "gdaldem", "hillshade", str(vrt), str(out),
        "-z", "3",       # vertical exaggeration — dramatic canyon walls
        "-az", "315",    # light from NW
        "-alt", "45",    # sun angle
        "-compute_edges",
        "-of", "GTiff",
    ])
    return out


def generate_color_relief(vrt: Path, workdir: Path) -> Path:
    colors_path = workdir / "bathy_colors.txt"
    colors_path.write_text(COLOR_RAMP)
    out = workdir / "color_relief.tif"
    run([
        "gdaldem", "color-relief", str(vrt), str(colors_path), str(out),
        "-of", "GTiff",
        "-alpha",   # include alpha channel (nodata → transparent)
    ])
    return out


# ──────────────────────────────────────────────────────────────────────────
# Step 4 — Multiply blend + ocean alpha mask
# ──────────────────────────────────────────────────────────────────────────

def blend_and_mask(hillshade_tif: Path, color_tif: Path,
                   elev: np.ndarray, geo: dict, workdir: Path) -> Path:
    """
    Multiply the hillshade onto the color relief to produce the lit depth image.
    Apply an alpha mask: ocean pixels (elev < 0) → opaque, land → transparent.
    Output is a georeferenced RGBA PNG GeoTIFF.
    """
    hs_img = Image.open(hillshade_tif).convert("L")   # grayscale
    cr_img = Image.open(color_tif).convert("RGBA")

    hs = np.array(hs_img, dtype=np.float32) / 255.0   # 0-1
    cr = np.array(cr_img, dtype=np.float32)            # 0-255 RGBA

    # Multiply blend: each RGB channel × hillshade brightness
    blended_rgb = np.clip(cr[:, :, :3] * hs[:, :, np.newaxis], 0, 255).astype(np.uint8)

    # Alpha channel: ocean cells opaque, land + nodata transparent
    ocean_mask = (elev < 0) & (elev != NODATA)
    alpha = np.where(ocean_mask, 255, 0).astype(np.uint8)

    rgba = np.dstack([blended_rgb, alpha])
    blended_img = Image.fromarray(rgba, mode="RGBA")

    # Save as PNG first (PIL handles RGBA cleanly)
    png_path = workdir / "blended.png"
    blended_img.save(png_path, "PNG")
    log.info("Saved blended PNG: %s", png_path.name)

    # Georeference the PNG using gdal_translate
    tif_path = workdir / "blended.tif"
    run([
        "gdal_translate",
        "-of", "GTiff",
        "-a_srs", "EPSG:4326",
        "-a_ullr",
        str(geo['lon_min']), str(geo['lat_max']),
        str(geo['lon_max']), str(geo['lat_min']),
        str(png_path), str(tif_path),
    ])
    return tif_path


# ──────────────────────────────────────────────────────────────────────────
# Step 5 — Tile generation
# ──────────────────────────────────────────────────────────────────────────

def generate_tiles(blended_tif: Path, tiles_dir: Path) -> None:
    """Run gdal2tiles to produce XYZ PNG tiles at zoom 5–12."""
    tiles_dir.mkdir(parents=True, exist_ok=True)
    run([
        gdal2tiles_cmd(),
        "-z", f"{ZOOM_MIN}-{ZOOM_MAX}",
        "-r", "lanczos",
        "--xyz",          # standard XYZ convention (Leaflet default)
        str(blended_tif),
        str(tiles_dir),
    ])
    n_tiles = len(glob(str(tiles_dir / "**/*.png"), recursive=True))
    log.info("Generated %d tiles in %s", n_tiles, tiles_dir)


# ──────────────────────────────────────────────────────────────────────────
# Step 6 — Upload to S3
# ──────────────────────────────────────────────────────────────────────────

def upload_tiles(tiles_dir: Path, region: str) -> None:
    """Upload all PNG tiles to S3 under bathy/{region}/{z}/{x}/{y}.png."""
    s3 = boto3.client("s3")
    tile_paths = sorted(glob(str(tiles_dir / "**/*.png"), recursive=True))
    total = len(tile_paths)
    log.info("Uploading %d tiles → s3://%s/bathy/%s/ …", total, S3_BUCKET, region)

    uploaded = 0
    errors   = 0
    t0 = time.time()
    for i, local_path in enumerate(tile_paths, 1):
        # local path: .../tiles/{z}/{x}/{y}.png
        rel = Path(local_path).relative_to(tiles_dir)
        s3_key = f"bathy/{region}/{rel.as_posix()}"
        try:
            s3.upload_file(
                local_path, S3_BUCKET, s3_key,
                ExtraArgs={
                    "ContentType":  "image/png",
                    "CacheControl": "max-age=31536000, immutable",
                },
            )
            uploaded += 1
        except Exception as exc:
            log.error("Failed to upload %s: %s", s3_key, exc)
            errors += 1

        if i % 500 == 0 or i == total:
            elapsed = time.time() - t0
            rate = uploaded / elapsed if elapsed > 0 else 0
            log.info("  %d/%d uploaded (%.1f/s, %d errors)", i, total, rate, errors)

    log.info("Upload complete: %d ok, %d errors. CloudFront URL pattern:",
             uploaded, errors)
    log.info("  %s/bathy/%s/{z}/{x}/{y}.png", CLOUDFRONT, region)
    if errors:
        raise RuntimeError(f"{errors} tiles failed to upload — check S3 permissions.")


# ──────────────────────────────────────────────────────────────────────────
# Full pipeline for one region
# ──────────────────────────────────────────────────────────────────────────

def process_region(region: str) -> None:
    cfg = REGION_CONFIGS[region]
    lat_min, lat_max = cfg['lat_min'], cfg['lat_max']
    lon_min, lon_max = cfg['lon_min'], cfg['lon_max']

    log.info("=" * 60)
    log.info("Region: %s  (lat %.2f–%.2f, lon %.2f–%.2f)  stride=%d",
             region, lat_min, lat_max, lon_min, lon_max, CRM_STRIDE)
    log.info("=" * 60)

    workdir  = Path(f"/tmp/bathy_{region}")
    tiles_dir = workdir / "tiles"
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True)

    try:
        # 1. Fetch
        t0 = time.time()
        elev, geo = fetch_crm_region(lat_min, lat_max, lon_min, lon_max)
        log.info("Fetch: %.1fs", time.time() - t0)

        # 2. Write VRT
        vrt = write_vrt(elev, geo, workdir)

        # 3. Hillshade + color-relief
        t1 = time.time()
        hs_tif = generate_hillshade(vrt, workdir)
        cr_tif = generate_color_relief(vrt, workdir)
        log.info("GDAL render: %.1fs", time.time() - t1)

        # 4. Blend + mask
        t2 = time.time()
        blended_tif = blend_and_mask(hs_tif, cr_tif, elev, geo, workdir)
        log.info("Blend: %.1fs", time.time() - t2)

        # 5. Tile generation
        t3 = time.time()
        generate_tiles(blended_tif, tiles_dir)
        log.info("Tiling: %.1fs", time.time() - t3)

        # 6. Upload
        t4 = time.time()
        upload_tiles(tiles_dir, region)
        log.info("Upload: %.1fs", time.time() - t4)

        log.info("✓ %s complete in %.1fs total", region, time.time() - t0)

    finally:
        # Clean up workdir to free disk space before next region
        if workdir.exists():
            shutil.rmtree(workdir)
            log.info("Cleaned up %s", workdir)


# ──────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    region_env = os.environ.get("REGION", "mid_atlantic").strip()
    if region_env == "all":
        regions = list(REGION_CONFIGS.keys())
    elif region_env in REGION_CONFIGS:
        regions = [region_env]
    else:
        log.error("Unknown REGION=%r. Choose: %s or 'all'",
                  region_env, ", ".join(REGION_CONFIGS))
        sys.exit(1)

    for r in regions:
        process_region(r)

    log.info("All done.")
