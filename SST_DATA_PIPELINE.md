# SST Data Pipeline — Ingest

This document describes how `sst_data_fetcher.py` pulls SST data from NOAA and writes the CSVs that feed the frontend. The pipeline runs daily via GitHub Actions (see `.github/workflows/sst_data_fetch.yml`).

---

## Data sources

Three SST products, all from NOAA CoastWatch ERDDAP. **No authentication required**, no xarray, no NetCDF, no OPeNDAP — plain HTTPS GET requests to `.csv0` endpoints.

### MUR (Daily Composite)

- **Dataset:** `jplMURSST41` — JPL Multi-scale Ultra-high Resolution v4.1
- **Native resolution:** 0.01° global, daily
- **What we pull:** Last 5 published days, strided by 5 → 0.05° (~5.5 km), ~6500 points/day after land filter
- **Time axis:** 09:00:00Z daily
- **Publication lag:** 1–2 days typical; the most recent UTC day is often not yet available at run time
- **Frontend label:** "Daily Composite"
- **Response units:** **Celsius** (important — see Kelvin gotcha below)

### GOES Blended (Hourly)

- **Dataset:** `noaacwBLENDEDsstDLDaily` — NOAA Geo-polar Blended Day+Night
- **Native resolution:** 0.05° global, daily
- **What we pull:** Most recent available day, replicated into 24 hourly slots so the hourly UI still works
- **Time axis:** 12:00:00Z
- **Publication lag:** 1–2 days typical
- **Frontend label:** "Hourly" (it's technically daily blended; we fake hourly slots from one day)
- **Response units:** Celsius

### GOES Composite

Same dataset as above (`noaacwBLENDEDsstDLDaily`), most recent hour only. Written to `DailySSTData/GOES/Composite/goes_composite_YYYYMMDD.csv`. **Frontend label:** "GOES Comp".

### VIIRS — retired, do not re-enable

The dataset we used (`noaacwL3CollatednppC`) was frozen mid-2025 when S-NPP was retired. NOAA-20/21 VIIRS feeds the Geo-polar Blended product we already use above, so VIIRS signal is still represented. The ingest no longer pulls a separate VIIRS file. The frontend VIIRS button has been removed.

---

## Mirror fallback

`coastwatch.pfeg.noaa.gov` has a habit of soft-blacklisting runner IPs (silent TCP resets, not HTTP 403). The ingest walks a list of mirrors per MUR request:

1. `coastwatch.pfeg.noaa.gov` (primary, PFEG)
2. `coastwatch.noaa.gov` (NESDIS — different org, independent rate-limit)
3. `upwell.pfeg.noaa.gov` (PFEG sibling)

All three mirror `jplMURSST41`. Logic:

- Each request polite-throttled (2s between requests to same host).
- On TCP reset: don't retry, raise immediately, fall through to next mirror.
- After 2 resets on a host in one run, that host is blacklisted and subsequent dates skip it straight to next mirror.
- 403 is an immediate permanent blacklist for that run.
- 404 is a "no data for this date" — legitimate, not a failure we retry.

GOES uses only `coastwatch.noaa.gov` (has worked reliably).

---

## The Kelvin gotcha

The NetCDF variable `analysed_sst` has `units: kelvin` in its metadata. But ERDDAP's `.csv0` endpoint applies the `scale_factor` and `add_offset` transforms automatically and **serves Celsius over the wire**.

If you set `"units": "K"` in the ingest config, the parser will subtract 273.15 from already-Celsius values. Real SST of 8.5°C becomes -264.65°C, which the frontend's `cToF()` renders as **-444°F on the legend**. The colors on the map still look right (all values shift by the same constant, relative positions preserved) but the legend and hover tooltips go absurd.

**Every MUR_MIRRORS entry and GOES_CFG entry must have `"units": "C"`.** This is the canonical answer even though it contradicts the NetCDF metadata.

---

## Land filter

MUR and GOES Blended are L4 SST products that fill inland water bodies (Chesapeake Bay, Rappahannock River, Pamlico Sound, Albemarle Sound, tidal estuaries) with real water temperatures. For a fishing-focused Atlantic-ocean map, these render as colored blocks painted over dry land at map zoom.

`sst_data_fetcher.py` applies a Natural Earth 1:10m coastline filter at ingest, after parsing but before writing. Every row whose `(lat, lon)` falls inside a land polygon (accounting for holes) is dropped.

- Polygons are fetched once per pipeline run (cached in `_LAND_POLYS_CACHE`).
- Point-in-polygon test: ray-cast, standard algorithm.
- Performance: ~9000 points × ~24 region-relevant polygons × ~1500 ring vertices = ~325M float operations in Python. Takes ~200ms per CSV, negligible at pipeline scale.
- Dropped-count is logged per day: `(land filter: dropped N inland points, kept M ocean points)`. Sanity check: dropped should be ~25–40% of input for our region.

If land filtering breaks (all points filtered out, or none filtered), suspect:
- Natural Earth URL returning 404 or a malformed response.
- `NORTH/SOUTH/EAST/WEST` constants changed.
- Winding-order bug (rare; NE uses consistent CCW exterior / CW holes).

---

## Output format

Every CSV:
```
lat,lon,sst
35.70,-75.50,18.2
35.70,-75.45,18.3
...
```

- Columns: `lat, lon, sst` exactly. Header row included.
- SST in **Celsius**, 2 decimals (`SST_DECIMALS = 2`).
- lat/lon rounded to 3 decimals (`COORD_DECIMALS = 3`) so string-key lookups in the frontend match exactly.
- Rows form a regular lat/lon grid (every lat appears with every lon), **minus** rows dropped by the land filter.
- No ordering guarantee — frontend doesn't depend on row order.

Write path:
- `DailySSTData/MUR/mur_YYYYMMDD.csv` (5 files rolling)
- `DailySSTData/GOES/Hourly/goes_YYYYMMDD_HH.csv` (24 files per most-recent day)
- `DailySSTData/GOES/Composite/goes_composite_YYYYMMDD.csv` (1 file)

---

## Region bounds

```python
NORTH, SOUTH = 39.00, 33.70
WEST,  EAST  = -78.89, -72.21
```

**Must match** the frontend's `BOUNDS` constant in `TestSST.jsx` and whatever `sstSummary.ts` / `getVIIRSData.ts` / `getGOESCompositeData.ts` use on the backend. If these diverge, mask alignment breaks, the grid snapping misfires, or the frontend's auto-fit shows the wrong area.

---

## Failure policy

- Each date fetched independently. A failure on one day is logged and skipped; other days still write.
- No mock/synthetic data is ever written. If ERDDAP returns empty, the CSV is not created. The frontend displays "No data available" cleanly.
- Pipeline exits with a non-zero status only on catastrophic failure (no dates at all fetched). A partial day or missing mirror is not a failure.

---

## When to change ingest code

1. **Different region:** update `NORTH/SOUTH/EAST/WEST` in the ingest, frontend `BOUNDS`, and every backend function. Redeploy all three.
2. **New dataset added:** follow the `MUR_MIRRORS` pattern — mirror list, `fetch_one_day` with per-cfg units, filter through `filter_to_ocean`, write to a dedicated subdirectory.
3. **Units change upstream (if NOAA ever switches ERDDAP output):** update `"units"` in config, re-run to verify legend numbers are reasonable.
4. **Natural Earth URL breaks:** vendor the GeoJSON into the repo as a fallback. 1:10m is ~25 MB, would inflate the repo but removes the single point of failure.

---

## Health check after a run

Look at the GitHub Actions log for the workflow run:

- `✓ Geo-polar Blended data available for YYYY-MM-DD` — GOES probe succeeded
- `✓ GOES YYYYMMDD_HH` × 24 — all hourly slots written
- `✓ MUR YYYYMMDD` × (4 or 5) — MUR days written (missing the most recent day is expected)
- `(land filter: dropped X inland points, kept Y ocean points)` — Y/(X+Y) should be 0.60–0.75 for our region
- `✓ Pipeline complete`

Then hard-refresh the app and confirm the legend shows reasonable °F values (roughly 50–80°F in season), the coastline is clean, and all three SST source buttons populate data.
