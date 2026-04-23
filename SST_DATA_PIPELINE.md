# SST Data Pipeline — SSTv2

This document describes the full architecture of the SST data ingest pipeline: how `sst_data_fetcher.py` pulls data from NOAA, what each source provides, how the GitHub Actions workflow orchestrates it, and how the output feeds the frontend. It is the canonical reference for understanding, debugging, or extending the pipeline.

---

## Overview

Three independent satellite data sources serve three distinct UI views. Each answers a different question about current ocean conditions and they are intentionally not redundant — they use different sensors, different algorithms, and different temporal cadences.

```
DailySSTData/
    MUR/              mur_YYYYMMDD.csv
    GOES/Composite/   goes_composite_YYYYMMDD.csv
    VIIRS/Passes/     viirs_{platform}_{YYYYMMDD_HHMM}.csv
```

All CSV files share the same column schema:
```
lat,lon,sst
35.70,-75.50,18.2
```
- Header row always included
- SST in **Celsius**, 3 decimal places (`SST_DECIMALS = 3`)
- lat/lon rounded to 4 decimal places (`COORD_DECIMALS = 4`) so string-key lookups in the frontend match exactly
- No ordering guarantee — frontend does not depend on row order

**Access method:** Plain HTTPS GET requests. No authentication, no xarray, no OPeNDAP. MUR and GOES use ERDDAP `.csv0` endpoints. VIIRS uses direct NetCDF4 file downloads from the STAR NESDIS NRT file server.

---

## Region bounds

```python
NORTH, SOUTH = 39.00, 33.70
WEST,  EAST  = -78.89, -72.21
```

**Must match** the frontend's `BOUNDS` constant in `TestSST.jsx` and every backend function (`sstSummary.ts`, `getVIIRSData.ts`, `getGOESCompositeData.ts`). If these diverge, mask alignment breaks, grid snapping misfires, or the frontend auto-fit shows the wrong area.

---

## Source 1 — MUR Daily Composite

**Dataset:** `jplMURSST41` — JPL Multi-scale Ultra-high Resolution v4.1  
**Provider:** NASA Jet Propulsion Laboratory  
**Access:** NOAA CoastWatch ERDDAP, no authentication  
**Output:** `DailySSTData/MUR/mur_YYYYMMDD.csv`  
**Cadence:** One file per day, 5-day rolling window  
**Resolution:** 0.01° native, fetched at stride 5 (~0.05°, ~5.5 km) → ~8,892 ocean points per day after land filter  
**Time axis:** 09:00:00Z daily  
**Latency:** 1–2 days behind real time; the most recent UTC day is often not yet published at run time  
**Processing level:** L4 — fully gap-filled analysis  
**Frontend label:** "Daily Composite"  
**Response units:** **Celsius** (see Kelvin gotcha below)

### What it is
MUR is a smoothed optimal interpolation analysis that blends observations from multiple polar-orbiting satellites (MODIS Aqua/Terra, AMSR2 microwave, VIIRS) with in-situ buoy measurements into a single gap-free daily field. Every pixel has a value — cloud cover is interpolated through. It is an estimated analysis, not a direct measurement.

### What it is good for
The highest-resolution view of large-scale thermal structure. Gulf Stream position, shelf break fronts, warm core rings, and cold upwelling are all clearly resolved. Because it is fully gap-filled it gives users a complete spatial picture even after cloudy days. Best used as the authoritative daily reference.

### Limitations
The gap-filling smooths over fine-scale features and can lag rapidly evolving conditions by a day or more. Coastal areas near land boundaries can be unreliable. The 1–2 day publication lag means the most recent day in the pipeline is usually T-1 or T-2, not today.

### Mirror fallback

MUR mirrors in priority order:
1. `coastwatch.noaa.gov` (NESDIS — different org, independent rate limit, most tolerant of CI runner IPs)
2. `coastwatch.pfeg.noaa.gov` (PFEG primary)
3. `upwell.pfeg.noaa.gov` (PFEG sibling)

All three mirror `jplMURSST41`. Mirror logic:
- Each request is polite-throttled (1.5s between requests to the same host)
- On TCP reset or timeout: raise immediately, fall through to next mirror without retry
- After 2 connection resets on a host in one run, that host is blacklisted for the remainder of the run
- 403 is an immediate permanent blacklist for that run (GitHub Actions runner IPs are frequently rate-limited by pfeg/upwell)
- 404 means "no data for this date" — legitimate, not retried
- A 403 on one mirror does not affect the others — blacklist is per-host

Files already on disk from a previous run are skipped without any HTTP call (cache check before fetch).

---

## Source 2 — GOES-19 Geo-polar Blended Daily Composite

**Dataset:** `noaacwBLENDEDsstDLDaily` — NOAA Geo-polar Blended Day+Night Diurnal Correction  
**Provider:** NOAA NESDIS / Office of Satellite Products and Operations  
**Access:** NOAA CoastWatch ERDDAP (`coastwatch.noaa.gov`), no authentication  
**Output:** `DailySSTData/GOES/Composite/goes_composite_YYYYMMDD.csv`  
**Cadence:** One file per day, most recent available  
**Resolution:** 0.05° native, fetched at stride 2 (~0.10°) → ~2,237 ocean points per day after land filter  
**Time axis:** 12:00:00Z  
**Latency:** ~1 day behind real time  
**Processing level:** L4 — fully gap-filled analysis  
**Frontend label:** "GOES Comp"  
**Response units:** Celsius

### What it is
The Geo-polar Blended analysis combines geostationary infrared sensors (GOES-19 ABI as primary input, plus Himawari-9 AHI and Meteosat SEVIRI) with polar-orbiting sensors (NOAA-20/21 VIIRS, MetOp AVHRR) into a single daily gap-free field. The "Diurnal Correction" variant uses both daytime and nighttime observations with a correction applied for solar heating effects during the day.

### What it is good for
A second independent view of the same ocean on the same day, using a fundamentally different blending methodology and a different primary sensor than MUR. GOES-19 ABI is the primary geostationary input — high-frequency infrared observations that MUR does not use directly. Where MUR and GOES agree, confidence in the thermal structure is high. Where they disagree — particularly in coastal areas or after rapidly changing weather — there is genuine uncertainty and VIIRS direct observations should be weighted more heavily.

### Limitations
Coarser resolution than MUR (0.05° vs 0.01°). Like MUR it is an analysis product, not a direct measurement. The stride-2 fetch (0.10°) further reduces spatial detail but was necessary to avoid ERDDAP slow-streaming hangs that previously caused 20+ minute job stalls at stride 1.

### Note on the old "GOES Hourly" data
Previous versions of the pipeline replicated a single daily GOES snapshot into 24 identical hourly CSV files written to `DailySSTData/GOES/Hourly/`. Those 24 files were byte-for-byte identical copies of one daily snapshot and provided no additional information. They have been permanently removed. The workflow runs `rm -rf DailySSTData/GOES/Hourly` before each fetch to purge any remaining files from the repo. The current composite is the honest representation of what this dataset provides.

---

## Source 3 — VIIRS Multi-pass (Per-swath Granules)

**Dataset:** ACSPO VIIRS L3U 10-minute granules  
**Provider:** NOAA STAR / NESDIS  
**Satellites:** Suomi-NPP, NOAA-20, NOAA-21  
**Access:** STAR NESDIS NRT file server, no authentication  
**Output:** `DailySSTData/VIIRS/Passes/viirs_{platform}_{YYYYMMDD_HHMM}.csv`  
**Cadence:** ~2 passes per satellite per day → 4–6 files per 24-hour window across all platforms  
**Resolution:** 0.02° native (~2 km) — highest resolution of the three sources  
**Latency:** 1–3 hours behind real time — fastest of the three sources  
**Processing level:** L3U — gridded swath, not gap-filled  
**Frontend label:** "VIIRS Passes"  
**Response units:** Kelvin in file, converted to Celsius by pipeline (`sst_c = sst_k - 273.15`)

### What it is
VIIRS is a multispectral radiometer flying on three NOAA polar-orbiting satellites in a ~1:30pm local solar time sun-synchronous orbit. Each satellite makes one ascending (daytime) and one descending (nighttime) pass over the Mid-Atlantic per day, producing a ~3000 km wide swath of direct thermal measurements. The ACSPO algorithm processes raw radiances into skin SST retrievals, applies a cloud mask, and writes cloud-free pixels at full resolution. Cloudy and land pixels are written as fill values (NaN after parsing).

### What it is good for
Ground truth. Unlike MUR and GOES, VIIRS pass files contain direct satellite measurements — no interpolation, no gap-filling, no blending. Cloud-free pixels show the actual sea surface temperature at the moment the satellite passed overhead, at 2 km resolution. This makes VIIRS the highest-fidelity view of fine-scale features: sharp thermal fronts, warm core rings, coastal upwelling filaments, and the precise position of the Gulf Stream wall. The 1–3 hour latency also makes it the most current of the three sources.

### Limitations
Cloud cover is the primary constraint. The ACSPO algorithm masks cloudy pixels as fill values — on overcast days a pass may return no usable data at all over the region. This is correct behavior, not an error. Users will see spatial gaps in the heatmap corresponding to cloud cover at the time of the pass. The swath geometry also means only the portion of the bbox directly under the satellite track has data — adjacent areas outside the ~3000 km swath are empty.

### Pass timing for the Mid-Atlantic (~36°N, 75.5°W = UTC-5.03h)
All three satellites fly the same orbit type. Pass center times are derived from the ~1:30pm local solar time ascending node:

| Pass | UTC time | Local time | Status |
|---|---|---|---|
| Night (descending) | ~06:10 UTC | ~01:10 AM | ✓ Confirmed from CI logs 2026-04-22 |
| Day (ascending) | ~18:33 UTC | ~01:33 PM | ✓ Confirmed from CI logs 2026-04-22 (N21 at 18:40) |

The pipeline targets ±20 minutes around each center time, limiting downloads to ~5 granules per pass per platform instead of scanning the full 24-hour directory (144 granules/satellite/day). This window is tunable via `VIIRS_PASS_CENTER_WINDOW_MIN` in the script.

### File server
Primary: `https://www.star.nesdis.noaa.gov/pub/socd2/coastwatch/sst/nrt/viirs`  
Fallback: `https://coastwatch.noaa.gov/pub/socd/mecb/coastwatch/viirs/nrt`

The pipeline probes both at startup and uses the first that responds with HTTP 200 or 403 (403 on root but 200 on subdirs is also usable).

### Granule fetch process
1. List directory via HTTP for each platform × (year, doy) pair
2. Parse filenames with regex to extract timestamp
3. Pre-filter to pass center windows — skip granules outside ±20 min of known centers
4. Cache check — skip granules already written in a previous run
5. Download full granule (~25 MB NetCDF4) into memory using `netCDF4.Dataset(memory=...)`
6. Slice to bbox (with 1° padding to catch swath edges)
7. Convert fill values to NaN, drop NaN rows (removes cloud + land pixels)
8. Clip to exact bbox, apply SST sanity filter (-2°C to 40°C)
9. Write CSV

### Cloud masking and land filtering
Cloud-covered pixels are masked by NOAA's ACSPO algorithm before files are written. They appear as fill values in the NetCDF4, become NaN during parsing, and are dropped by `dropna()`. **No additional cloud or land filter is applied to VIIRS output.** Land masking is also handled upstream by ACSPO via the `l2p_flags` land bit.

A vectorized polygon-based land filter was previously applied to VIIRS output but caused 5–10 minute hangs on 36k-point granules due to the scale of numpy broadcasting required (36k points × 24 polygons × ~100 vertices). It was removed after confirming ACSPO handles both cloud and land masking upstream. The 104 "inland" points the filter was previously catching were already fill values that were dropped by `dropna()` — the filter was finding no real ocean data to remove.

### Note on retired VIIRS dataset
The dataset previously used (`noaacwL3CollatednppC`) was frozen mid-2025 when Suomi-NPP entered its end-of-life phase and was retired from operational production. The current pipeline replaces it with direct L3U granule downloads from the STAR NESDIS NRT file server, which serves all three active platforms (NPP, NOAA-20, NOAA-21) with 1–3 hour latency.

---

## Source Comparison

| | MUR | GOES Blended | VIIRS Passes |
|---|---|---|---|
| **Type** | L4 analysis | L4 analysis | L3U direct observation |
| **Gap-filled** | ✓ Yes | ✓ Yes | ✗ No — cloud holes present |
| **Resolution** | 0.01° (~1 km) | 0.05° (~5 km) | 0.02° (~2 km) |
| **Fetch stride** | 5 → 0.05° | 2 → 0.10° | Native 0.02° |
| **Latency** | 1–2 days | ~1 day | 1–3 hours |
| **Passes per day** | 1 (daily composite) | 1 (daily composite) | 4–6 (real distinct passes) |
| **Primary sensor** | MODIS + AMSR2 + VIIRS + buoys | GOES-19 ABI + VIIRS + AVHRR | VIIRS only |
| **Cloud handling** | Interpolated through | Interpolated through | Masked — pixels absent |
| **Land handling** | ERDDAP serves inland water — filtered at ingest | Same | ACSPO pre-masked — no filter needed |
| **Best for** | Full spatial coverage, fronts, reference | Cross-check, second opinion | Ground truth, fine features, recency |
| **Provider** | NASA JPL | NOAA NESDIS | NOAA STAR |

### How to use them together
- **MUR** gives you the complete picture — use it to orient to the day's overall thermal structure. No gaps, highest resolution, authoritative.
- **GOES** provides a cross-check using a different algorithm and different primary sensors. Agreement with MUR = high confidence in the thermal structure. Disagreement = genuine uncertainty; look at VIIRS for ground truth in that area.
- **VIIRS** shows what the satellite actually saw at the moment of the pass. Use it to ground-truth MUR/GOES in specific areas, identify sharp features that MUR may have smoothed over, and get the most current reading available. Gaps in VIIRS are real — they indicate cloud cover at pass time.

---

## The Kelvin Gotcha

The NetCDF variable `analysed_sst` has `units: kelvin` in its metadata. But ERDDAP's `.csv0` endpoint automatically applies the `scale_factor` and `add_offset` transforms and **serves Celsius over the wire**.

If `"units": "K"` is set in the ingest config, the parser subtracts 273.15 from already-Celsius values. Real SST of 8.5°C becomes -264.65°C. The frontend's `cToF()` renders this as **-444°F on the legend**. The heatmap colors still look correct (all values shift by the same constant, relative positions preserved) which makes this bug hard to notice visually — but the legend and hover tooltips show absurd values.

**Every `MUR_MIRRORS` entry and `GOES_CFG` entry must have `"units": "C"`.** This is correct even though it contradicts the NetCDF metadata.

VIIRS L3U files genuinely store SST in Kelvin. The pipeline reads the raw NetCDF variable and explicitly converts: `sst_c = np.ma.filled(sst_sub, np.nan) - 273.15`.

---

## Land Filter (MUR and GOES only)

MUR and GOES are L4 SST products that fill inland water bodies (Chesapeake Bay, Rappahannock River, Pamlico Sound, Albemarle Sound, tidal estuaries) with real water temperatures. For a fishing-focused Atlantic-ocean map, these render as colored blocks painted over dry land at map zoom.

`sst_data_fetcher.py` applies a Natural Earth 1:10m coastline filter to MUR and GOES output after parsing but before writing. Every row whose `(lat, lon)` falls inside a land polygon is dropped. VIIRS is exempt — ACSPO pre-masks land upstream.

### Implementation
- Polygons fetched once per run via HTTPS from the `nvkelso/natural-earth-vector` GitHub repo and cached in `_LAND_RINGS_CACHE`
- Only polygons whose bounding box intersects the region are kept (~24 polygons)
- **Vectorized ray-casting** using numpy broadcasting — all N points tested against all M ring vertices simultaneously via array operations, not a Python loop
- Per-ring bbox pre-filter: points clearly outside a polygon's bounding box skip the full ray-cast
- Typical output: ~8,892 ocean points (MUR) and ~2,237 ocean points (GOES) after filtering

### Expected drop rates
Sanity check: `dropped / (dropped + kept)` should be roughly 25–40% for the Mid-Atlantic region. If all points are dropped (suspect Natural Earth URL returning 404) or none are dropped (suspect NORTH/SOUTH/EAST/WEST constants changed), the filter has failed.

### Previous implementation note
An earlier pure-Python ray-casting implementation ran point-by-point in a Python loop: ~36k points × 24 polygons × ~100 vertices = ~86M Python iterations, taking 5–10 minutes per granule. The vectorized replacement runs the same computation in numpy C code in under 1 second. The Python loop implementation must not be re-introduced.

### If land filtering breaks
- Natural Earth URL returning 404 or malformed response → polygons cache as empty list → filter silently passes all points → inland water appears on map
- `NORTH/SOUTH/EAST/WEST` constants changed without updating filter bounds → wrong polygons loaded
- Winding-order bug → points inside polygons test as outside (rare; NE uses consistent CCW exterior / CW holes)
- Fallback: vendor the GeoJSON into the repo to remove the single point of failure (1:10m is ~25 MB)

---

## Timeout Architecture

ERDDAP and the VIIRS file server can both hang in ways that `requests` default timeouts do not catch. Two layers of protection are applied:

### Layer 1 — `timeout=(connect, read)` tuple
```python
HTTP_TIMEOUT = (15, 90)   # 15s connect, 90s read
```
The read timeout caps the silence between consecutive bytes from the server. A server that starts streaming but then stalls will be caught here — but only if it goes completely silent. A server trickle-streaming 1 byte every 89 seconds will not be caught.

### Layer 2 — SIGALRM hard ceiling
```python
with hard_timeout(ERDDAP_HARD_TIMEOUT_S, label):   # 180s absolute
    resp = requests.get(...)
```
A `SIGALRM`-based context manager enforces an absolute wall-clock ceiling regardless of how slowly the server streams. Fires `_TimeoutError` which is caught and treated as a failed attempt. Linux/macOS only — works on GitHub Actions (Ubuntu).

```
ERDDAP_HARD_TIMEOUT_S = 180   # 3 min max per ERDDAP request
VIIRS_HARD_TIMEOUT_S  = 90    # 90s max per granule download
```

### Why this was necessary
The previous `timeout=120` (a scalar) only covered connection establishment, not total transfer time. The GOES endpoint (`noaacwBLENDEDsstDLDaily`) at stride 1 would begin streaming then stall, causing 20+ minute hangs that triggered the GitHub Actions 60-minute job timeout. Switching to stride 2 (smaller response) and adding the SIGALRM ceiling resolved the hangs.

---

## GitHub Actions Workflow

**File:** `.github/workflows/sst-fetch.yml`  
**Trigger:** Daily cron `0 10 * * *` UTC + manual dispatch with optional `target_date` and `sources` inputs  
**Runner:** `ubuntu-latest`  
**Job timeout:** 60 minutes  
**Python:** 3.11

### Steps in order
1. Checkout repo
2. Set up Python 3.11 with pip cache
3. `pip install requests numpy pandas netCDF4`
4. Resolve target date (defaults to 2 days ago if not specified)
5. **Remove stale GOES Hourly files:** `rm -rf DailySSTData/GOES/Hourly` — purges fake hourly CSVs from old pipeline versions permanently
6. Run `python -u sst_data_fetcher.py` — all output via tee to `sst_fetch.log`
7. Debug output — list all files in `DailySSTData/`, tail last 100 lines of log
8. **Verify outputs** — count CSVs per source, emit `::warning::` if any source produces zero files
9. Clean up stale non-CSV artifacts (`.parquet`, `.geojson`, `*_grid.json`)
10. List final `DailySSTData/` contents
11. Commit data files
12. Commit fetch log to `DailySSTData/logs/`
13. Notify on failure

### Per-source verification (step 8)
```bash
mur_count=$(find DailySSTData/MUR/ -name "*.csv" | wc -l)
goes_count=$(find DailySSTData/GOES/Composite/ -name "*.csv" | wc -l)
viirs_count=$(find DailySSTData/VIIRS/Passes/ -name "*.csv" | wc -l)
```
Zero files on any source emits a `::warning::` annotation visible in the Actions summary without failing the job.

---

## Health Check After a Run

Look at the GitHub Actions log for the workflow run:

**MUR:**
```
✓ MUR 20260421 (cached)          ← most days will be cached
MUR 20260422 (coastwatch.noaa.gov) … ✗ RuntimeError   ← T+0 often 404 (not yet published)
✗ MUR 20260422 — all mirrors failed. Last: ... 404     ← expected for most-recent day
```

**GOES:**
```
✓ GOES composite 20260422 (cached)     ← or:
✓ Geo-polar Blended 2026-04-22         ← fresh fetch
```

**VIIRS:**
```
✓ VIIRS NRT base: https://www.star.nesdis.noaa.gov/...
Target windows this run: 2
  2026-04-22 05:50 – 06:30 UTC  (0610UTC)
  2026-04-22 18:13 – 18:53 UTC  (1833UTC)
npp 20260422_0550 … ✗ swath overlaps bbox but all pixels cloud/land masked
npp 20260422_0610 … → DailySSTData/VIIRS/Passes/viirs_npp_20260422_0610.csv  (36737 pts)
VIIRS summary: 2 written, 3 cached, 8 miss/cloud, 134 outside windows.
```

**Land filter sanity check:**
```
(land filter: dropped 94 inland, kept 8892 ocean)   ← MUR: ~10% drop expected
(land filter: dropped 70 inland, kept 2237 ocean)   ← GOES: ~24% drop expected
```

**Signs of a healthy run:**
- 4–5 MUR days cached (T-1 through T-5), T+0 missing = normal
- 1 GOES composite present
- At least 1–2 VIIRS pass files per run (cloud-dependent)
- Land filter dropping 10–30% of points, not 0% and not 100%
- `✓ Pipeline complete` at end

**Signs of a problem:**
- All MUR mirrors returning 403 → GitHub Actions IP blacklisted by NOAA (transient, resolves next day)
- VIIRS summary showing 0 written and 0 cached → pass window may need adjustment, or file server URL changed
- Land filter dropping 0 points → Natural Earth URL failed, inland water will appear on map
- Job hitting 60-minute timeout → something is not timing out correctly; check SIGALRM is firing

---

## Known Issues and Mitigations

| Issue | Cause | Mitigation |
|---|---|---|
| pfeg/upwell 403 on CI runner IPs | GitHub Actions IPs are rate-limited by NOAA PFEG | `coastwatch.noaa.gov` tried first; per-host blacklist isolates failures |
| ERDDAP slow-streaming hang | Server starts response then stalls | `(connect, read)` timeout tuple + SIGALRM hard ceiling |
| GOES hang at stride 1 | Large response (~14k rows) stalls mid-transfer | Stride 2 (0.10°, ~3.5k rows) — quarter the transfer size |
| MUR T+0 unavailable | 1–2 day publication lag from NASA JPL | 5-day rolling window; T-1 is always available |
| VIIRS granule cloud cover | Cannot predict before downloading | Pass center targeting limits wasted downloads to ~5/pass/platform |
| VIIRS land filter hang | 36k pts × 24 polygons in Python loop = 86M iterations | Filter removed — ACSPO pre-masks land upstream |
| VIIRS all-cloud pass | Cloud cover at pass time | Expected; logged as "swath overlaps bbox but all pixels cloud/land masked" |
| Retired VIIRS dataset (`noaacwL3CollatednppC`) | S-NPP retired mid-2025 | Replaced with direct L3U granule downloads from STAR NESDIS NRT |
| Fake GOES hourly files in repo | Old pipeline replicated 1 daily snapshot into 24 identical files | `rm -rf DailySSTData/GOES/Hourly` in workflow before each run |

---

## When to Change Ingest Code

1. **Different region:** Update `NORTH/SOUTH/EAST/WEST` in the ingest, frontend `BOUNDS`, and every backend function. Redeploy all three.
2. **New dataset added:** Follow the `MUR_MIRRORS` pattern — mirror list, fetch with per-cfg units, filter through `filter_to_ocean` (unless pre-masked upstream), write to a dedicated subdirectory.
3. **Units change upstream:** Update `"units"` in config, re-run and verify legend shows reasonable °F values (~50–80°F in season).
4. **Pass windows drift:** VIIRS orbital plane precesses slowly over time. If passes are consistently missed, check CI logs for the nearest hit/miss timestamps and adjust `VIIRS_PASS_CENTERS_UTC`. Increase `VIIRS_PASS_CENTER_WINDOW_MIN` by 10 as a temporary measure.
5. **Natural Earth URL breaks:** Vendor the GeoJSON into the repo as a fallback. 1:10m is ~25 MB — inflates repo size but removes the single point of failure.
6. **File server URL changes:** Update `VIIRS_BASE_CANDIDATES`. The pipeline probes both candidates at startup and uses the first live one.

---

## Current File State (as of 2026-04-23)

```
DailySSTData/VIIRS/Passes/
    viirs_npp_20260422_0610.csv    — NPP night pass    (36,737 pts)
    viirs_n20_20260422_0630.csv    — NOAA-20 night pass
    viirs_n21_20260422_1840.csv    — NOAA-21 day pass  ← first confirmed day pass hit
    viirs_npp_20260423_0550.csv    — NPP night pass
    viirs_n20_20260423_0610.csv    — NOAA-20 night pass

DailySSTData/GOES/Composite/
    goes_composite_20260420.csv
    goes_composite_20260421.csv
    goes_composite_20260422.csv

DailySSTData/MUR/
    mur_20260415.csv through mur_20260421.csv   (7 days)

DailySSTData/logs/
    sst_fetch_2026-04-18.log through sst_fetch_2026-04-21.log
```
