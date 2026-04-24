# StaticLayersRetrieval.py — Reference Documentation

**Script:** `StaticLayersRetrieval.py`  
**Output directory:** `DailySST/`  
**Last updated:** April 2026

---

## Overview

This script fetches and processes all static geographic layers needed by the SSTv2 application. "Static" means the data does not change daily — bathymetry updates roughly annually, coastlines and land masks are essentially permanent, and wrecks/fishing spots change only when GPX source files are manually updated. The script is designed to run in CI (GitHub Actions) on every pipeline execution, with caching to avoid unnecessary re-downloads.

Five output files are produced:

| File | Format | Contents |
|---|---|---|
| `bathymetry_contours.json` | GeoJSON FeatureCollection | Depth contour LineStrings at 9 fathom-aligned levels |
| `bathymetry_grid.json` | JSON (custom schema) | Raw 2D depth grid for feature-detection algorithms |
| `noaa_coastline.json` | GeoJSON FeatureCollection | Coastline LineStrings clipped to bbox |
| `landmask.json` | GeoJSON FeatureCollection | Land polygon mask clipped to bbox |
| `wrecks.json` | GeoJSON FeatureCollection | Wrecks and fishing spots from GPX sources |

---

## Geographic Bounding Box

All data is fetched and clipped to the following region, covering the Mid-Atlantic / Southeast US offshore fishing area from Cape Hatteras to the New York Bight:

```
LAT_MIN = 33.70   LAT_MAX = 39.00
LON_MIN = -78.89  LON_MAX = -72.21
```

Coastline and land mask features are clipped with a 0.5° padding beyond the bbox edges to avoid visible cutoff at tile boundaries.

---

## Data Sources

### 1. Bathymetry — ERDDAP Griddap (Primary)

Bathymetry is fetched from NOAA ERDDAP servers via the OPeNDAP griddap protocol, which allows requesting a lat/lon bbox subset as a CSV download. The script tries sources in priority order, falling back automatically on failure.

**Source priority list (as of April 2026):**

| Priority | Server | Dataset ID | Variable | Lon Convention |
|---|---|---|---|---|
| 1 | `coastwatch.pfeg.noaa.gov` | `GEBCO_2020` | `elevation` | −180 → 180 |
| 2 | `coastwatch.pfeg.noaa.gov` | `ETOPO_2022_v1_15s` | `z` | −180 → 180 |
| 3 | `oceanwatch.pifsc.noaa.gov` | `ETOPO_2022_v1_15s` | `z` | **0 → 360** |
| 4 | `ncei.noaa.gov/erddap` | `ETOPO_2022_v1_15s` | `z` | −180 → 180 |
| 5 | `ncei.noaa.gov/erddap` | `ETOPO_2022_v1_30s` | `z` | −180 → 180 |
| 6 | `ncei.noaa.gov/erddap` | `ETOPO_2022_v1_60s` | `z` | −180 → 180 |

**Dataset descriptions:**

- **GEBCO_2020** — General Bathymetric Chart of the Oceans 2020 global grid. 15 arc-second (~450 m) resolution. Public domain. Preferred source when available.
- **ETOPO_2022_v1_15s** — NOAA NCEI ETOPO 2022 global relief model at 15 arc-second resolution. Integrates land topography and ocean bathymetry. Public domain.
- **ETOPO_2022_v1_30s / 60s** — Coarser-resolution versions of ETOPO 2022. Used as fallbacks when the 15s dataset is unavailable or times out.

**ERDDAP query URL format:**
```
https://<server>/erddap/griddap/<datasetID>.csvp
  ?<variable>[(<LAT_MIN>):<stride>:(<LAT_MAX>)][(<LON_MIN>):<stride>:(<LON_MAX>)]
```

The response is a CSV with two header rows (row 0 = variable names, row 1 = units) followed by data rows of `latitude, longitude, elevation_meters`.

### 2. Coastline — Natural Earth 10m (GitHub Raw)

```
https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_coastline.geojson
```

- Source: Natural Earth, public domain
- Resolution: 1:10m scale (suitable for regional/national maps)
- Geometry type: GeoJSON LineString / MultiLineString
- Processing: clipped to bbox with 0.5° padding; segments shorter than 3 points are dropped

### 3. Land Mask — Natural Earth 10m (GitHub Raw)

```
https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_land.geojson
```

- Source: Natural Earth, public domain
- Resolution: 1:10m scale
- Geometry type: GeoJSON Polygon / MultiPolygon
- Processing: only polygons whose exterior ring intersects the padded bbox are retained; coordinates rounded to 5 decimal places (~1.1 m precision)

### 4. Wrecks / Fishing Spots — GPX Files (Local)

Source GPX files are placed manually in `DailySST/` and registered in the `WRECK_GPX_FILES` dict:

| GPX File | Region Label |
|---|---|
| `Fishing_Spots_HatterasNC.gpx` | `HatterasNC` |
| `Fishing_Spots_MoreheadNC.gpx` | `MoreheadNC` |
| `Fishing_spots_ChesapeakeMD.gpx` | `ChesapeakeMD` |

- **Source:** fishingstatus.com (exported GPX)
- **Waypoint types:** `Wreck` (charted or known shipwreck), `Rocks` (rock, ledge, reef, or bottom structure)
- **ID extraction:** Each waypoint's `<desc>` CDATA field may contain an `ID#XXXXXXXX` string from Fishing Status, which is extracted and stored as `fs_id` in the output properties.
- **GPX namespace handling:** The parser handles both fully namespaced GPX 1.1 (`xmlns="http://www.topografix.com/GPX/1/1"`) and bare/namespace-stripped GPX files.

To add a new region: drop the GPX file into `DailySST/` and add an entry to `WRECK_GPX_FILES`.

---

## Processing Pipeline

### Bathymetry Processing

#### Step 1 — HTTP Fetch with Retry and Stride Degradation

The HTTP session uses `urllib3.Retry` with:
- 3 total retries
- Backoff factor of 2 (waits 2s, 4s, 8s between attempts)
- Retry on HTTP 429, 500, 502, 503, 504

On transient network failures (connection reset, chunked encoding break, read timeout), the stride automatically degrades:

```
stride ladder: [BATHY_STRIDE, BATHY_STRIDE×2, BATHY_STRIDE×4]
  e.g. with BATHY_STRIDE=1: [1, 2, 4]
```

At `stride=1` the bbox produces ~2M points (~30 MB CSV). Coarser strides reduce this proportionally. Non-transient errors (HTTP 4xx) skip immediately to the next source without retrying at a coarser stride, since changing stride cannot fix a bad dataset ID or IP block.

**User-Agent:** NOAA ERDDAP servers return HTTP 403 for default `python-requests` User-Agent strings. The script sets:
```
SSTv2/1.0 (+https://github.com/jlintvet/SSTv2) python-requests
```

**GitHub Actions IP blocks:** PFEG CoastWatch (`coastwatch.pfeg.noaa.gov`) blocks shared GitHub Actions runner IPs with HTTP 403. This is a server-side policy, not a UA issue. The script falls through to OceanWatch PIFSC or NCEI ERDDAP, both of which accept GHA traffic.

#### Step 2 — Longitude Convention Remapping (pos360 sources)

The OceanWatch PIFSC server stores longitudes in 0–360° format. Since the bbox uses negative longitudes (−78.89 to −72.21), those values must be remapped before the query URL is built:

```
lon_min_query = LON_MIN + 360  →  −78.89 + 360 = 281.11
lon_max_query = LON_MAX + 360  →  −72.21 + 360 = 287.79
```

After the response is parsed, longitudes > 180° are remapped back to −180–180 (subtract 360) so all downstream processing operates on a uniform coordinate system.

#### Step 3 — CSV Parsing and Fill-Value Guard (`_parse_erddap_csvp`)

ERDDAP's `.csvp` format skips the first 2 rows (header + units), then provides `lat, lon, elevation_m` rows. Elevation is in meters, sign convention: negative = below sea level (ocean), positive = above sea level (land).

**Critical fill-value guard:** ERDDAP uses several sentinel values for missing/invalid data. These must be rejected before the sign check, because a value like `−9999` passes the `< 0` test and would otherwise be interpreted as a real depth of 9,999 m (~32,800 ft), producing the vertical "tower" artifact.

Guard logic:
```python
ERDDAP_FILL_THRESHOLD_M = -10_000  # meters — below any real ocean depth

if math.isnan(elev) or elev < ERDDAP_FILL_THRESHOLD_M:
    # treat as land / no data
    depth_ft = None
```

Known ERDDAP fill values this catches: `−9.99e34`, `−9.99e33`, `9.96921e36`, `−32767`, `−9999`.

Points with `depth_ft = None` are treated as land or no-data throughout the pipeline.

#### Step 4 — Grid Assembly (`_build_grid`)

The flat list of `(lat, lon, depth_ft)` points is assembled into a 2D grid:
- Axes are sorted ascending (important for `contourpy` which requires monotonically increasing coordinates)
- Land/no-data cells are stored as `math.nan`
- A **6-pass nearest-neighbour gap fill** interpolates over `NaN` cells using the mean of their 4 cardinal neighbours (N/S/E/W). This fills small land-masked gaps at coastlines and any isolated missing points from the ERDDAP response. The pass stops early if no cells changed.

#### Step 5 — Post-Grid Sanity Check (`_sanity_check_grid`)

A last-resort guard that scans every longitude column in the assembled grid. Any column whose maximum depth exceeds `MAX_OCEAN_DEPTH_FT = 36,000 ft` (slightly above the Mariana Trench at ~35,876 ft) is zeroed out by setting all its cells to `NaN`. This prevents a fill-value leak that survived the parser from reaching the contour generator and producing the tower artifact.

#### Step 6 — Contour Generation (`_extract_contour_lines`, `write_contours`)

Depth contours are generated using the `contourpy` library (`contour_generator(x=lons, y=lats, z=grid).lines(depth_ft)`). Contour segments with fewer than 6 points are dropped as noise.

Each contour segment is smoothed using **2 iterations of Chaikin corner-cutting**, which replaces each edge with two new points at the 1/4 and 3/4 positions. This rounds sharp grid-aligned corners while preserving overall shape and is lossless in terms of geographic accuracy at these scales.

**Contour depth levels (fathom-aligned):**

| Depth (ft) | Depth (fm) | Fishing significance |
|---|---|---|
| 60 | 10 fm | Nearshore / inshore boundary |
| 120 | 20 fm | Inner shelf |
| 180 | 30 fm | Mid shelf |
| 300 | 50 fm | Outer shelf |
| 600 | 100 fm | Inner shelf break — wahoo, mahi-mahi zone |
| **1200** | **200 fm** | **TRUE SHELF BREAK — billfish, tuna, swordfish** ← most important |
| 1800 | 300 fm | Upper slope |
| 3000 | 500 fm | Canyon heads, deep drop |
| 6000 | 1000 fm | Abyssal / very deep water |

The 200 fm (1,200 ft) contour has `shelf_break: true` in its GeoJSON properties for special UI treatment (bolder stroke, permanent label).

#### Step 7 — Grid JSON Output (`write_bathymetry_grid`, schema v2)

The depth grid is serialized as a compact JSON file. Key schema decisions:

- **Depth stored in feet only, integer-rounded.** GEBCO's intrinsic accuracy is far coarser than 1 ft, so rounding is lossless in practice and roughly halves the serialized file size versus 1-decimal floats.
- **Fathoms are not stored.** Consumers derive them client-side: `depth_fathoms = depth_ft / 6` (exact, since 1 fathom = 6 ft exactly).
- **`null` encodes land or no-data cells** (JSON serialization of `math.nan`).
- **JSON written compact** (no indent, no whitespace) using `separators=(",", ":")`.
- **`schema_version`** in the `meta` block is incremented whenever the output schema changes (currently v2). The cache validator checks this and forces a re-fetch if the cached file has an older schema version, even if it is within `CACHE_DAYS`.

---

## Caching Strategy

| Layer | Cache behavior | Invalidation triggers |
|---|---|---|
| Bathymetry contours + grid | Re-fetch if files are > `CACHE_DAYS` (30) days old | File missing, file stale, schema version mismatch |
| Coastline | Fetch once; never re-fetched automatically | Delete file to force refresh |
| Land mask | Fetch once; never re-fetched automatically | Delete file to force refresh |
| Wrecks | **Always rebuilt** on every run | No cache — source GPX can change at any time |

To force a full bathymetry re-fetch, delete `DailySST/bathymetry_contours.json` and `DailySST/bathymetry_grid.json` before running.

---

## Diagnostics

### `_run_depth_diagnostics(rows)`

Called immediately after `_fetch_bathymetry()` returns, before any grid work. Outputs to the log:

- Total point count, ocean vs. land/null split
- Full percentile distribution of ocean depths (min, p01, p10, p25, p50, p75, p90, p99, max)
- Top-10 deepest raw points, flagged if > `MAX_OCEAN_DEPTH_FT` (fill-value leak indicator)
- Per-longitude-column max-depth sweep — flags any column with max depth > 3× median as a tower-artifact suspect
- Per-latitude-row max-depth sweep — flags anomalous horizontal band artifacts

**Interpretation guide:**

| Log output | Meaning | Action |
|---|---|---|
| `*** FILL VALUE LEAK? ***` on a top-10 point | Fill-value sentinel reached the parser | Check `_parse_erddap_csvp` fill-value guard |
| Suspicious lons list | That longitude column is the tower artifact source | `_sanity_check_grid` will auto-zero it; investigate source |
| All checks `PASSED` | Raw data is clean | Artifact may be in cached files — delete and re-run |
| `max > 36,000 ft` | Definitive fill-value leak | Fix `ERDDAP_FILL_THRESHOLD_M` or add the specific sentinel value |

---

## Known Issues and Operational Notes

### GitHub Actions IP Blocks (HTTP 403)
`coastwatch.pfeg.noaa.gov` blocks shared GitHub Actions runner IPs. This is a server-side network policy and cannot be resolved by changing the User-Agent. The script automatically falls through to `oceanwatch.pifsc.noaa.gov` (priority 3) and `ncei.noaa.gov/erddap` (priorities 4–6), both of which accept GHA traffic as of April 2026.

If all sources begin returning 403, the most likely cause is a CI runner IP range change by GitHub. Check NOAA's ERDDAP catalog for new dataset IDs or consider hosting a local ERDDAP mirror.

### The Tower Artifact
A vertical spike of anomalously deep values (visible as a narrow north-south tower in the bathymetric colormap) was traced to ERDDAP fill-value sentinels (`-9999` or similar) passing through the original parser unfiltered. A single bad elevation value of −9,999 m converts to ~32,800 ft, coloring an entire longitude column with the deepest colormap color. Fixed by:
1. Adding `ERDDAP_FILL_THRESHOLD_M = -10_000` guard in `_parse_erddap_csvp`
2. Adding `_sanity_check_grid` as a last-resort column-zeroing pass
3. Adding `_run_depth_diagnostics` for future early detection

### OceanWatch PIFSC Longitude Convention (0–360°)
`oceanwatch.pifsc.noaa.gov` stores longitudes as 0–360°, not −180–180°. Querying it directly with the bbox's negative longitudes would produce an empty or geographically wrong response. The `lon_convention="pos360"` parameter in `_try_erddap_source` handles the remapping transparently in both directions.

### ERDDAP Dataset ID Drift
NOAA periodically retires or renames dataset IDs. IDs that 404'd and were removed:
- `https://oceanwatch.pifsc.noaa.gov/erddap/griddap/ETOPO_2022_v1_15s.csvp` — returned 404 in a previous CI run (since restored)
- `https://www.ncei.noaa.gov/erddap/griddap/ETOPO_2022_v1_60s.csvp` — returned 404 in April 2026

If a source starts 404ing, verify current dataset IDs at `<server>/erddap/griddap/index.html`.

---

## Configuration Reference

| Constant | Default | Description |
|---|---|---|
| `BATHY_STRIDE` | `1` | ERDDAP grid stride. `1` = native ~450 m resolution; `2` = ~900 m |
| `CACHE_DAYS` | `30` | Days before bathymetry is considered stale and re-fetched |
| `BATHY_GRID_SCHEMA_VERSION` | `2` | Incremented when output schema changes; triggers cache invalidation |
| `TIMEOUT` | `300` | HTTP request timeout in seconds |
| `MAX_OCEAN_DEPTH_FT` | `36,000` | Maximum plausible ocean depth; anything deeper = fill-value suspect |
| `ERDDAP_FILL_THRESHOLD_M` | `−10,000` | Minimum valid elevation in meters; below this = treat as no-data |
| `SHELF_BREAK_FT` | `1,200` | Depth of the shelf-break contour (200 fm); flagged in GeoJSON properties |

---

## Adding New Sources

**To add a new bathymetry ERDDAP source:**
1. Verify the dataset is live: `https://<server>/erddap/griddap/<datasetID>.html`
2. Note the variable name (typically `elevation` for GEBCO, `z` for ETOPO)
3. Note the longitude convention (check if lons in the data range 0–360 or −180–180)
4. Add a tuple to `BATHY_SOURCES`: `("<url>.csvp", "<var>", "neg180"|"pos360")`

**To add a new wreck/fishing-spot region:**
1. Export a GPX file from fishingstatus.com and place it in `DailySST/`
2. Add an entry to `WRECK_GPX_FILES`: `"filename.gpx": "RegionLabel"`
3. The next run will automatically include it in `wrecks.json`

---

## Output Schema Reference

### `bathymetry_contours.json`
```json
{
  "type": "FeatureCollection",
  "features": [{
    "type": "Feature",
    "geometry": { "type": "LineString", "coordinates": [[lon, lat], ...] },
    "properties": {
      "depth_ft": 1200,
      "depth_fathoms": 200,
      "label_ft": "1200 ft",
      "label_fathoms": "200 fm",
      "shelf_break": true
    }
  }]
}
```

### `bathymetry_grid.json` (schema v2)
```json
{
  "meta": {
    "schema_version": 2,
    "generated_utc": "2026-04-24T12:00:00Z",
    "source": "GEBCO_2020 (primary) | ETOPO_2022_v1_15s | ...",
    "stride": 1,
    "res_lat_deg": 0.004167,
    "res_lon_deg": 0.004167,
    "n_lats": 1272,
    "n_lons": 1604,
    "region": { "lat_min": 33.7, "lat_max": 39.0, "lon_min": -78.89, "lon_max": -72.21 },
    "units": { "depth_ft": "feet below surface, rounded to nearest integer; null = land or no data" },
    "fathoms_note": "depth_fathoms = depth_ft / 6",
    "contour_depths_ft": [60, 120, 180, 300, 600, 1200, 1800, 3000, 6000],
    "shelf_break_ft": 1200,
    "shelf_break_fathoms": 200
  },
  "lats": [33.7, 33.70417, ...],
  "lons": [-78.89, -78.88583, ...],
  "depth_ft": [[null, null, 42, 87, ...], ...]
}
```
`depth_ft[row][col]` corresponds to `lats[row]` and `lons[col]`. `null` = land or no data. Derive fathoms client-side: `depth_fathoms = depth_ft / 6`.

### `wrecks.json`
```json
{
  "type": "FeatureCollection",
  "metadata": {
    "source": "Fishing Status (fishingstatus.com)",
    "generated": "2026-04-24T12:00:00Z",
    "gpx_files": ["Fishing_Spots_HatterasNC.gpx", ...],
    "regions": ["HatterasNC", "MoreheadNC", "ChesapeakeMD"],
    "symbols": { "Wreck": "charted or known shipwreck", "Rocks": "rock, ledge, reef, or bottom structure" }
  },
  "feature_count": 847,
  "features": [{
    "type": "Feature",
    "geometry": { "type": "Point", "coordinates": [-75.12345, 35.67890] },
    "properties": {
      "name": "Spar Wreck",
      "symbol": "Wreck",
      "fs_id": "377565",
      "region": "HatterasNC",
      "source": "Fishing Status (fishingstatus.com)"
    }
  }]
}
```
`fs_id` is omitted if no `ID#` was found in the GPX waypoint's `<desc>` field.
