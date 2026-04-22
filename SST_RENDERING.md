# SST Rendering Pipeline — Frontend

This document describes how `TestSST.jsx` renders SST data onto the Mapbox basemap without the heatmap bleeding onto land. If you break rendering later, read this before changing things.

---

## The three problems that took us a long time to solve

Every one of these was a dead end we walked down before finding the right fix. They will feel plausible again if you're new to the code.

### 1. SST colors bleed onto land

**Symptom:** Ocean-colored blocks appear over Virginia, Maryland, eastern NC, Chesapeake Bay, Pamlico Sound.

**What doesn't work:**
- Inserting the SST raster layer below Mapbox's land layers via z-order. Mapbox's `light-v11` paints `landcover` *before* `water` in the style order, so any layer inserted above `water` will also be above `landcover`. There is no single z-index slot in `light-v11` where SST sits over water but under land. Don't waste time on `map.addLayer(..., beforeLayerId)` tricks.
- Relying on the source data (MUR, GOES Blended) to mark land as NaN. These are L4 SST products. They fill Chesapeake Bay, Rappahannock River, Pamlico Sound, and other inland water bodies with real water temperatures because those *are* water. As far as JPL/NOAA are concerned, there's nothing to filter.
- A browser-side raster mask that pokes transparent holes in the canvas. Conceptually correct, but `raster-resampling: linear` in Mapbox then interpolates across the transparent holes and smears ocean color back over land at display time.

**What works:**
Filter out all inland points at **ingest time** using Natural Earth 1:10m coastline. The CSVs written to `DailySSTData/` contain only true-Atlantic-ocean points. The browser-side mask is then redundant but harmless. See `SST_DATA_PIPELINE.md` for the ingest side.

### 2. SST shifts northward relative to the coastline (Mercator bug)

**Symptom:** After masking is applied, Outer Banks and mid-Atlantic coast appear to have SST painted north of where the coastline actually is. The offset grows with latitude.

**Cause:** Our canvas is painted in equirectangular pixel space (each row is equal lat degrees). Mapbox's `image` source renders it in Web Mercator space, where 1° of latitude occupies more vertical screen pixels near the poles than near the equator. The canvas gets linearly stretched between the two lat/lon corner points *after* they're projected to Mercator — so rows near the top land too far north and rows near the bottom land too far south.

**Fix:** Paint the canvas in Mercator-Y space. For each canvas row `py`:

```js
const mercY = (lat) => Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI / 180) / 2));
const invMercY = (y) => (2 * Math.atan(Math.exp(y)) - Math.PI / 2) * 180 / Math.PI;

const mY = mercYNorth - (py / (CANVAS_H - 1)) * (mercYNorth - mercYSouth);
const lat = invMercY(mY);
```

Then snap `lat` to the nearest grid row. This is in `gridToDataURL()` in `TestSST.jsx`. If you ever see coastline drift returning, check that this Mercator inversion is still there.

### 3. Hourly / GOES Comp buttons show "No data available" even though the files are in the repo

**Cause:** `normalizeSSTResponse()` had a `firstGrid.length > 100` threshold that rejected small grids as malformed. The backend functions `getVIIRSData` and `getGOESCompositeData` return legit `{days: [...]}` responses, just with fewer points.

**Fix:** Threshold is now `> 0`. Any non-empty grid is accepted.

---

## Layer architecture

```
┌──────────────────────────────────────────┐
│ Mapbox symbol layers (labels, roads)     │ ← drawn last (top)
├──────────────────────────────────────────┤
│ SST raster (sst-source / sst-layer)      │ ← inserted before first symbol
├──────────────────────────────────────────┤
│ Chlorophyll / SeaColor overlay           │ ← same insertion point
├──────────────────────────────────────────┤
│ Bathymetry contour lines                 │
├──────────────────────────────────────────┤
│ Mapbox water (blue fill)                 │
│ Mapbox landcover (gray fill)             │ ← SST would paint over
│ Mapbox background                        │    this if not filtered
└──────────────────────────────────────────┘
```

SST must be **ocean-only by the time it hits the canvas**. Layer z-order cannot save you — the ingest has to filter inland points.

---

## The `gridToDataURL` function

Lives in `TestSST.jsx`. Signature: `gridToDataURL(latSet, lonSet, grid, valMin, valMax, colorFn, isOcean) → {dataURL, west, east, north, south}`.

Contract:
- `latSet` descending (north → south), `lonSet` ascending (west → east).
- `grid` is a flat object keyed by `"${lat}_${lon}"` strings, value = SST °F.
- `isOcean` is the frontend coastline mask function (redundant now that ingest filters, but still checked as a safety net).
- Canvas is **fixed 512 × 400 pixels**, independent of source grid resolution.
- Each canvas pixel resolves to a geographic lat/lon via inverse Mercator (see Mercator fix above), then snaps to the nearest source-grid cell.
- Land pixels (per `isOcean`) are left transparent. Data NaN is left transparent.
- Return includes four lat/lon corners expanded by half a grid cell on each side — this aligns pixel centers with cell centers, so Mapbox's `raster-resampling: linear` interpolates smoothly without shifting data by half a cell.

---

## Water mask plumbing

The mask is built once per map mount, from Natural Earth 1:10m (`ne_10m_land.geojson`, ~25 MB, cached by browser). It produces a `(lat, lon) => boolean` function: true = ocean.

Storage pattern is specific and intentional:
- `waterMaskRef` (`useRef`) — **authoritative**, survives re-renders and Mapbox style reloads.
- `waterMaskVersion` (`useState`, counter) — changing this triggers dependent effects to re-run. Incremented whenever a new mask is stored.
- `maskBuildStartedRef` — guards against building the mask more than once per mount, because Mapbox's `style.load` event fires multiple times.

The SST effect's dependency array uses `waterMaskVersion`, not the function reference. This prevents stale-closure bugs when the mask arrives after the first paint.

The `styledata` listener watches for Mapbox wiping user-added layers during internal tile reloads. If `sst-layer` goes missing after a styledata event, it bumps `waterMaskVersion` to force the layer to re-add itself. Without this, SST sometimes briefly renders correctly then reverts to an unmasked state.

---

## Diagnostic logging

All frontend SST code emits `[SST:*]` and `[MASK]` log lines. Keep these in place — they were essential for narrowing down every bug. Spot-check values in `[MASK]`:

- `[MASK] polys intersecting bounds:` should be 10–30 for our region.
- `[MASK] classified: N land, M ocean cells` should be a roughly 1:2 ratio for the BOUNDS box.
- `[MASK] spot checks` — Richmond VA must be LAND, mid-Atlantic (36.5, -74.5) must be OCEAN. If either is inverted, your bounds or your polygon winding order is broken.

---

## Constants that matter

These must match across frontend, backend, and ingest:

```
NORTH = 39.00
SOUTH = 33.70
WEST  = -78.89
EAST  = -72.21
```

If you change these in one place, change them everywhere or the mask and coordinate-snap logic will silently misalign.

---

## If rendering breaks, check these in order

1. Browser console: `[SST:layer] painting raster  mask=YES  maskVer=N  grid=Kpts` — is K roughly right (thousands for MUR, thousands for GOES)?
2. `[MASK] spot checks` — is the mask oriented correctly?
3. Network tab: fetch to `sstSummary` / `getVIIRSData` / `getGOESCompositeData` — is the response shape `{days: [{grid: [...]}]}` with reasonable point counts?
4. Ingest log in GitHub Actions: does `land filter: dropped N inland points, kept M ocean points` show M ≫ N? If inland filtering is too aggressive, ocean points are disappearing.
5. Mercator math still present in `gridToDataURL`? If coastline drifts, this is usually the cause.
