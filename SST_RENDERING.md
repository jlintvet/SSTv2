# SST Rendering Pipeline — Frontend

This document describes how the Leaflet-based `SSTHeatmapLeaflet.jsx` (and its dev counterpart `SSTHeatmapLeaflet_chl_range.jsx`) renders SST, chlorophyll, and sea color data onto the CartoDB basemap. If you break rendering, read this before changing anything.

> **Architecture note:** The app was originally Mapbox-based (`TestSST.jsx`). It was migrated to Leaflet (`SSTHeatmapLeaflet.jsx`). Some notes in older git history refer to Mapbox-specific problems (`styledata`, `addSource`, `addLayer`) that no longer apply. The pipeline described here is the current Leaflet version.

---

## Problems that took a long time to solve — do not revisit

Every one of these was a dead end. They will feel plausible again if you're new to the code.

### 1. SST colors bleed onto land

**Symptom:** Ocean-colored blocks appear over Virginia, Maryland, eastern NC, Chesapeake Bay, Pamlico Sound.

**What doesn't work:**
- Relying on the source data (MUR, GOES Blended) to mark land as NaN. These are L4 SST products. They fill Chesapeake Bay, Rappahannock River, Pamlico Sound, and other inland water bodies with real water temperatures because those *are* water.
- A browser-side raster mask that pokes transparent holes in the canvas. Bilinear interpolation in the renderer then bleeds ocean color back across the transparent holes.
- Filtering CMEMS CHL/SeaColor rows using the MUR ocean mask (`mask==1`). MUR's open-ocean definition **includes Chesapeake Bay and Pamlico Sound**, so `mask==1` does NOT exclude estuaries. Do not add MUR mask filtering to `DailyChlorophyllandSeaColorRetrieval.py`.

**What works:**
Filter out inland points at **ingest time** using the prebaked Natural Earth coastline mask. The CSVs written to `DailySSTData/` contain only true-Atlantic-ocean points. The browser-side `waterMaskRef` mask provides a secondary check.

---

### 2. SST shifts northward relative to the coastline (Mercator bug)

**Symptom:** Outer Banks and mid-Atlantic coast appear to have SST painted north of where the coastline actually is. The offset grows with latitude.

**Cause:** Canvas painted in equirectangular pixel space (equal lat degrees per row). Leaflet renders image overlays in Web Mercator space. The canvas gets linearly stretched between the two corner points after Mercator projection, so rows near the top land too far north.

**Fix:** Paint the canvas in Mercator-Y space. For each canvas row `py`:

```js
const mercY = (lat) => Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI / 180) / 2));
const invMercY = (y) => (2 * Math.atan(Math.exp(y)) - Math.PI / 2) * 180 / Math.PI;

const mY = mercYNorth - (py / (CANVAS_H - 1)) * (mercYNorth - mercYSouth);
const lat = invMercY(mY);
```

This is in `gridToDataURL()`. **This applies to ALL three layers (SST, chlorophyll, sea color).** Do not add per-layer projection logic.

---

### 3. Hourly / GOES Comp buttons show "No data available"

**Cause:** `normalizeSSTResponse()` had a `firstGrid.length > 100` threshold that rejected small grids as malformed.

**Fix:** Threshold is now `> 0`. Any non-empty grid is accepted.

---

### 4. Sea color layer shifted north/south

**Symptom:** Sea color data appears shifted relative to coastline. SST and chlorophyll are correctly aligned.

**Cause:** Sea color (KD490) from CMEMS is a coarse ~0.17° grid (~1,280 rows). When passed directly to `gridToDataURL` on its own grid, the bilinear interpolation finds few valid neighbor quads near the data edges, causing `wsum < 0.25` transparency halos that look like a shift. More importantly: using the native coarse-grid bounds places the image at slightly different coordinates than the SST reference grid.

**Fix:** Pre-expand the sea color grid onto the SST reference grid (`latSet/lonSet`) using bilinear interpolation via `expandCoarseGrid()`. Then pass `latSet/lonSet` and the expanded grid to `gridToDataURL`. This guarantees the image bounds are identical to the SST layer. See the `expandCoarseGrid` function in `SSTHeatmapLeaflet.jsx`.

---

### 5. Chlorophyll layer shifted west relative to coastline

**Symptom:** The entire CHL data overlay appears shifted west when compared against SST or the basemap coastline. SST and sea color are correctly aligned. The shift is significant — data appears on land or in incorrect water bodies.

**Cause:** CHL from CMEMS Sentinel-3 OLCI 300m (`cmems_obs-oc_glo_bgc-plankton_nrt_l3-olci-300m_P1D`) uses a native ~0.011° grid. When rendered on its own native grid (`latSet2/lonSet2`), the image bounds (computed as `lonWest - lonStep/2` to `lonEast + lonStep/2`) end up at slightly different coordinates than the SST reference grid. Even though the CMEMS coordinate values in the JSON are correct (verified: lon -78.8861 to -72.2139), the image gets placed at misaligned bounds in Leaflet's `imageOverlay`.

**What doesn't work:**
- Modifying `gridToDataURL` bounds calculation — doc says "do not modify this function."
- Adding exclusion boxes for Chesapeake Bay / Pamlico Sound — the data is correctly positioned, the whole layer is just shifted.
- Filtering with the MUR ocean mask — MUR includes estuaries, this filters the wrong things.
- Adding a coordinate offset correction in Python — coordinates in the JSON are already correct.

**Fix:** Apply the same pattern as sea color — pre-expand CHL onto the SST reference grid using `expandCoarseGrid()`, then pass `latSet/lonSet` to `gridToDataURL`. This ensures CHL uses the same image bounds as SST.

In the overlay `useEffect`:

```js
const useRefGrid = activeDataLayer === "seacolor" || activeDataLayer === "chlorophyll";
const renderLatSet = useRefGrid ? latSet : latSet2;
const renderLonSet = useRefGrid ? lonSet : lonSet2;
const renderGrid   = useRefGrid ? expandCoarseGrid(latSet2, lonSet2, overlayGrid, latSet, lonSet) : overlayGrid;
```

Since CHL at ~0.011° is close to the SST grid at 0.01°, `expandCoarseGrid` produces nearly identical values — no quality loss. Cloud-covered cells (null in the CMEMS data) are excluded from interpolation; if no valid neighbor exists, the expanded cell is omitted and renders transparent.

---

## Layer architecture (Leaflet)

```
┌──────────────────────────────────────────┐
│ Leaflet markers / popups                 │ ← top
├──────────────────────────────────────────┤
│ Fish hotspot SVG overlays                │
├──────────────────────────────────────────┤
│ Bathymetry / wreck markers               │
├──────────────────────────────────────────┤
│ Isotherm / temp-break polylines          │
├──────────────────────────────────────────┤
│ CHL / SeaColor / Composite imageOverlay  │ ← overlayLayerRef
├──────────────────────────────────────────┤
│ SST imageOverlay                         │ ← sstOverlayRef
├──────────────────────────────────────────┤
│ Wind velocity (L.velocityLayer)          │
├──────────────────────────────────────────┤
│ CartoDB tile layer                       │ ← bottom
└──────────────────────────────────────────┘
```

Leaflet image overlays sit above the tile layer automatically. SST is rendered first, then the CHL/SeaColor/Composite overlay on top. No z-index tricks needed.

SST must still be **ocean-only by the time it hits the canvas**. The basemap's land areas show through transparent pixels — so land-filtered SST is correct. The `waterMaskRef` ocean mask function is passed to `gridToDataURL` as `isOcean` to skip land pixels.

---

## The `gridToDataURL` function

Lives in `SSTHeatmapLeaflet.jsx`. Signature: `gridToDataURL(latSet, lonSet, grid, valMin, valMax, colorFn, isOcean, rangeMin, rangeMax) → Promise<{dataURL, west, east, north, south}>`.

Contract:
- `latSet` descending (north → south), `lonSet` ascending (west → east).
- `grid` is a flat object keyed by `"${lat}_${lon}"` strings.
- `isOcean` is the frontend coastline mask function; checked per pixel. Pass `waterMaskRef.current`.
- Canvas is **fixed 512 × 400 pixels**.
- Each canvas pixel resolves to a geographic lat/lon via inverse Mercator (problem 2 above).
- Each pixel value is **bilinearly interpolated** from the 4 surrounding source-grid cells.
- Missing or null neighbors drop out of the weighted sum (renormalized by `wsum`). If `wsum < 0.25`, pixel is transparent.
- Returns bounds expanded by half a grid cell on each side — aligns pixel centers with cell centers.
- Returns a `dataURL` as a blob URL (revoke these on cleanup via `blobUrlsRef.current`).
- **Do not modify this function to fix overlay alignment issues.** Alignment problems are solved upstream by `expandCoarseGrid` or by correcting arguments.

---

## Overlay `useEffect` — correct call pattern

```
SST layer:       gridToDataURL(latSet,  lonSet,  grid,             ...)  ← SST grid, direct
Chlorophyll:     gridToDataURL(latSet,  lonSet,  expandedCHL,      ...)  ← expanded onto SST grid
Sea color:       gridToDataURL(latSet,  lonSet,  expandedSeaColor, ...)  ← expanded onto SST grid
Composite:       gridToDataURL(latSet2, lonSet2, overlayGrid,      ...)  ← composite own grid, direct
```

Both CHL and SeaColor use `expandCoarseGrid(latSet2, lonSet2, overlayGrid, latSet, lonSet)` before calling `gridToDataURL`. The composite layer uses its own grid directly (it is already on the same resolution as SST).

The overlay bounds (`[[south, west], [north, east]]`) passed to `L.imageOverlay` always come from the `gridToDataURL` return value — never hardcoded.

---

## Water mask plumbing

Built once per map mount from a prebaked binary mask at `DailySSTData/ocean_mask.json` (falls back to Natural Earth 1:10m download if unavailable). Produces a `(lat, lon) => boolean` function: true = ocean.

Storage:
- `waterMaskRef` (`useRef`) — authoritative, survives re-renders.
- `waterMaskVersion` (`useState`, counter) — incrementing triggers dependent effects to re-run.
- `maskBuildStartedRef` — guards against building the mask more than once per mount.

SST and overlay effects both depend on `waterMaskVersion` and defer rendering if `waterMaskRef.current` is null.

---

## CHL data pipeline (Python → frontend)

**Backend:** `DailyChlorophyllandSeaColorRetrieval.py` runs via GitHub Actions. Fetches CMEMS `cmems_obs-oc_glo_bgc-plankton_nrt_l3-olci-300m_P1D` (Sentinel-3 OLCI, 300m) with `CMEMS_STRIDE = 4` (effective ~1.2km). Returns ~71,939 rows per day (all grid cells including cloud-covered nulls). Lon normalization: `lon - 360 if lon > 180 else lon` ensures lons are negative (-78.89 to -72.21). Coord range logged as: `lat 33.7028–38.9972  lon -78.8861–-72.2139`.

**Frontend loading:** `getChlorophyllData` Base44 function fetches CHL JSON and returns `{ days: [{ date, grid: [{lat, lon, chlorophyll}], stats: {min, max} }] }`. `normalizeSSTResponse` passes this through as-is (since `data.days` already has a valid grid). The `stats` field is required by the overlay `useEffect` for `min2/max2`.

**Key variables in overlay `useEffect`:**
- `day.grid` → array of `{lat, lon, chlorophyll}` objects
- `latSet2/lonSet2` → unique sorted lat/lon arrays from `day.grid` (the native CMEMS grid)
- `overlayGrid` → `{ "${lat}_${lon}": chlorophyll_value }` (includes null values for cloud)
- `renderGrid` → result of `expandCoarseGrid(latSet2, lonSet2, overlayGrid, latSet, lonSet)` (on SST grid)

---

## Auth and region access

The app uses Supabase for auth and subscriptions, not Base44 auth. Key files:
- `src/lib/supabase.js` — client singleton.
- `src/hooks/useAuth.js` — wraps Supabase session state.
- `src/hooks/useRegionAccess.js` — fetches `user_subscriptions`, creates free trial on first login.
- `src/components/auth/AuthGate.jsx` — shows login modal when no session; wraps children in `RegionAccessProvider`.

**Supabase tables:** `auth.users`, `public.user_subscriptions` (`user_id`, `tier`, `regions[]`, `trial_ends_at`), `public.regions`.

**Email confirmation redirect:** Supabase → Authentication → URL Configuration → Site URL must be `https://lintvetsstv2.base44.app`. If users get `localhost refused to connect` after clicking confirmation email, this setting was reset.

---

## Constants — must match across frontend, backend, ingest

```
NORTH = 39.00
SOUTH = 33.70
WEST  = -78.89
EAST  = -72.21
```

---

## If rendering breaks, check in this order

1. **CHL shifted west** → check that `expandCoarseGrid` is called for the chlorophyll branch and result passed with `latSet/lonSet` to `gridToDataURL` (problem 5). Both CHL and SeaColor should use this pattern.
2. **SeaColor shifted** → same as above — check `expandCoarseGrid` is being called for sea color branch (problem 4).
3. **SST or any layer shifts north/south** → check Mercator math is still in `gridToDataURL` (problem 2).
4. **CHL overlay blank / no data** → check `day.stats` exists; `min2=day.stats.min` will throw if stats is missing. Also check `expandCoarseGrid` result isn't empty (all nulls from full cloud cover).
5. **Mask not applying** → check `waterMaskRef.current` is not null before calling `gridToDataURL`.
6. **Users get localhost error on email confirmation** → fix Supabase Site URL.
7. **Users land on upgrade screen after login** → check `user_subscriptions` table — row may not exist.
