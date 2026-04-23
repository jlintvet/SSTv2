# SST Rendering Pipeline — Frontend

This document describes how `TestSST.jsx` renders SST, chlorophyll, and sea color data onto the Mapbox basemap. If you break rendering, read this before changing anything.

---

## Problems that took a long time to solve — do not revisit

Every one of these was a dead end. They will feel plausible again if you're new to the code.

### 1. SST colors bleed onto land

**Symptom:** Ocean-colored blocks appear over Virginia, Maryland, eastern NC, Chesapeake Bay, Pamlico Sound.

**What doesn't work:**
- Inserting the SST raster layer below Mapbox's land layers via z-order. Mapbox's `light-v11` paints `landcover` *before* `water` in the style order, so any layer inserted above `water` will also be above `landcover`. There is no single z-index slot in `light-v11` where SST sits over water but under land. Don't waste time on `map.addLayer(..., beforeLayerId)` tricks.
- Relying on the source data (MUR, GOES Blended) to mark land as NaN. These are L4 SST products. They fill Chesapeake Bay, Rappahannock River, Pamlico Sound, and other inland water bodies with real water temperatures because those *are* water. As far as JPL/NOAA are concerned, there's nothing to filter.
- A browser-side raster mask that pokes transparent holes in the canvas. Conceptually correct, but `raster-resampling: linear` in Mapbox then interpolates across the transparent holes and smears ocean color back over land at display time.

**What works:**
Filter out all inland points at **ingest time** using Natural Earth 1:10m coastline. The CSVs written to `DailySSTData/` contain only true-Atlantic-ocean points. The browser-side mask is then redundant but harmless. See `SST_DATA_PIPELINE.md` for the ingest side.

---

### 2. SST shifts northward relative to the coastline (Mercator bug)

**Symptom:** Outer Banks and mid-Atlantic coast appear to have SST painted north of where the coastline actually is. The offset grows with latitude.

**Cause:** Our canvas is painted in equirectangular pixel space (each row is equal lat degrees). Mapbox's `image` source renders it in Web Mercator space, where 1° of latitude occupies more vertical screen pixels near the poles than near the equator. The canvas gets linearly stretched between the two lat/lon corner points *after* they're projected to Mercator — so rows near the top land too far north and rows near the bottom land too far south.

**Fix:** Paint the canvas in Mercator-Y space. For each canvas row `py`:

```js
const mercY = (lat) => Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI / 180) / 2));
const invMercY = (y) => (2 * Math.atan(Math.exp(y)) - Math.PI / 2) * 180 / Math.PI;

const mY = mercYNorth - (py / (CANVAS_H - 1)) * (mercYNorth - mercYSouth);
const lat = invMercY(mY);
```

Then snap `lat` to the nearest grid row. This is in `gridToDataURL()` in `TestSST.jsx`. If you ever see coastline drift returning, check that this Mercator inversion is still there.

**This applies to ALL three layers (SST, chlorophyll, sea color).** `gridToDataURL` handles it for all of them since they all go through the same function. Do not add per-layer projection logic.

---

### 3. Hourly / GOES Comp buttons show "No data available"

**Cause:** `normalizeSSTResponse()` had a `firstGrid.length > 100` threshold that rejected small grids as malformed. The backend functions `getVIIRSData` and `getGOESCompositeData` return legit `{days: [...]}` responses, just with fewer points.

**Fix:** Threshold is now `> 0`. Any non-empty grid is accepted.

---

### 4. Chlorophyll and sea color layers flicker continuously

**Symptom:** The overlay layer flashes on and off repeatedly, never settling. Console shows `[SST:map] styledata — forcing re-paint` repeating dozens of times (observed: 72×).

**Cause:** The `styledata` event handler was triggering `setWaterMaskVersion` on every Mapbox internal tile reload. Each `waterMaskVersion` bump caused the overlay `useEffect` to remove and re-add the overlay layer, which itself triggered another `styledata` event — a perfect infinite loop.

**Fix:** Add a 3-second cooldown to the `styledata` handler and increase the debounce from 150ms to 500ms:

```js
let styleReloadTimer = null;
let lastRepaintAt = 0;
map.on("styledata", () => {
  if (styleReloadTimer) clearTimeout(styleReloadTimer);
  styleReloadTimer = setTimeout(() => {
    try {
      const now = Date.now();
      if (!map.getLayer("sst-layer") && (now - lastRepaintAt) > 3000) {
        lastRepaintAt = now;
        setWaterMaskVersion(v => v + 1);
      }
    } catch(_) {}
  }, 500);
});
```

Do not revert the debounce to 150ms or remove the cooldown — the loop will return.

---

### 5. Sea color layer shifts north/south (Mercator + coarse grid)

**Symptom:** Sea color data appears shifted northward at the top of the map and southward at the bottom. Chlorophyll and SST are correctly aligned.

**Cause:** Sea color has a much coarser grid than chlorophyll (32×39 at ~0.16° spacing vs 239×270 for chlorophyll). When `gridToDataURL` performs bilinear interpolation on a coarse grid, it finds very few valid neighbor quads near the edges of the data, causing `wsum < 0.25` transparency halos and apparent shift. The fix is NOT to change `gridToDataURL`, NOT to override the Mapbox image coordinates with `dataBounds`, and NOT to use `latSet/lonSet` (SST grid) as the canvas dimensions — all of these were tried and broke other layers.

**What doesn't work:**
- Passing `latSet/lonSet` (SST grid dimensions) instead of `latSet2/lonSet2` to `gridToDataURL` for the overlay — makes chlorophyll render as polka dots because SST grid points don't align with chlorophyll key strings.
- Overriding the Mapbox `addSource` coordinates with `dataBounds` or the SST source's coordinates — shifts chlorophyll east of the coastline.
- Nearest-neighbor resampling in a pre-expansion step — produces blocky square pixels.

**What works:** Pre-expand the sea color grid onto the SST grid using **bilinear interpolation** before passing to `gridToDataURL`. This produces a dense grid keyed to `latSet/lonSet` coordinates, which `gridToDataURL` can interpolate smoothly. The `addSource` coordinates continue to use `imgWest/imgEast/imgNorth/imgSouth` from the `gridToDataURL` return value — unchanged.

```js
function expandCoarseGrid(latSet2, lonSet2, overlayGrid, targetLatSet, targetLonSet) {
  const expanded = {};
  for (const lat of targetLatSet) {
    let r0 = 0;
    for (let i = 0; i < latSet2.length - 1; i++) {
      if (lat <= latSet2[i] && lat >= latSet2[i + 1]) { r0 = i; break; }
    }
    const r1 = Math.min(r0 + 1, latSet2.length - 1);
    const latFrac = latSet2[r0] === latSet2[r1] ? 0 :
      (latSet2[r0] - lat) / (latSet2[r0] - latSet2[r1]);

    for (const lon of targetLonSet) {
      let c0 = 0;
      for (let i = 0; i < lonSet2.length - 1; i++) {
        if (lon >= lonSet2[i] && lon <= lonSet2[i + 1]) { c0 = i; break; }
      }
      const c1 = Math.min(c0 + 1, lonSet2.length - 1);
      const lonFrac = lonSet2[c0] === lonSet2[c1] ? 0 :
        (lon - lonSet2[c0]) / (lonSet2[c1] - lonSet2[c0]);

      const vNW = overlayGrid[`${latSet2[r0]}_${lonSet2[c0]}`];
      const vNE = overlayGrid[`${latSet2[r0]}_${lonSet2[c1]}`];
      const vSW = overlayGrid[`${latSet2[r1]}_${lonSet2[c0]}`];
      const vSE = overlayGrid[`${latSet2[r1]}_${lonSet2[c1]}`];

      let sum = 0, wsum = 0;
      const wNW = (1 - latFrac) * (1 - lonFrac);
      const wNE = (1 - latFrac) * lonFrac;
      const wSW = latFrac * (1 - lonFrac);
      const wSE = latFrac * lonFrac;

      if (vNW != null && Number.isFinite(vNW)) { sum += vNW * wNW; wsum += wNW; }
      if (vNE != null && Number.isFinite(vNE)) { sum += vNE * wNE; wsum += wNE; }
      if (vSW != null && Number.isFinite(vSW)) { sum += vSW * wSW; wsum += wSW; }
      if (vSE != null && Number.isFinite(vSE)) { sum += vSE * wSE; wsum += wSE; }

      if (wsum >= 0.25) expanded[`${lat}_${lon}`] = sum / wsum;
    }
  }
  return expanded;
}
```

In the overlay `useEffect`, **only the sea color branch** uses this expansion:

```js
// Sea color only — pre-expand coarse grid onto SST resolution before painting
const expandedGrid = expandCoarseGrid(latSet2, lonSet2, overlayGrid, latSet, lonSet);
Promise.resolve(gridToDataURL(latSet, lonSet, expandedGrid, min2, max2, colorFn, waterMaskRef.current))
```

The chlorophyll branch passes `latSet2, lonSet2, overlayGrid` directly — unchanged.

**The `addSource` coordinates are always `imgWest/imgEast/imgNorth/imgSouth` from the `gridToDataURL` return for both layers. Never override these.**

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
│ Isotherm / temp-break contour lines      │
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
- `grid` is a flat object keyed by `"${lat}_${lon}"` strings.
- `isOcean` is the frontend coastline mask function (redundant now that ingest filters, but still checked as a safety net).
- Canvas is **fixed 512 × 400 pixels**, independent of source grid resolution.
- Each canvas pixel resolves to a geographic lat/lon via inverse Mercator (see problem 2 above).
- Each pixel value is **bilinearly interpolated** from the 4 surrounding source-grid cells. Do NOT revert to nearest-neighbor — that makes the display blocky, and `raster-resampling: linear` on the Mapbox layer only smooths pixel edges, it cannot recover smooth gradient from blocky source pixels.
- Missing neighbors (NaN or land-filtered) drop out of the weighted sum, renormalized by `wsum`. If `wsum < 0.25`, the pixel is left transparent.
- Return includes four lat/lon corners expanded by half a grid cell on each side — this aligns pixel centers with cell centers.
- **Do not modify this function to fix overlay alignment issues.** The function is correct. Overlay alignment problems are solved upstream by `expandCoarseGrid` (sea color) or are a sign of incorrect arguments being passed.

---

## Overlay `useEffect` — correct call pattern

```
SST layer:       gridToDataURL(latSet,  lonSet,  grid,          ...)  ← SST grid, direct
Chlorophyll:     gridToDataURL(latSet2, lonSet2, overlayGrid,   ...)  ← CHL own grid, direct
Sea color:       gridToDataURL(latSet,  lonSet,  expandedGrid,  ...)  ← expanded onto SST grid
```

All three use `imgWest/imgEast/imgNorth/imgSouth` from the return value for `map.addSource` coordinates.

---

## Water mask plumbing

The mask is built once per map mount, from a prebaked binary mask at `DailySSTData/ocean_mask.json` (falls back to Natural Earth 1:10m if unavailable). It produces a `(lat, lon) => boolean` function: true = ocean.

Storage pattern is specific and intentional:
- `waterMaskRef` (`useRef`) — **authoritative**, survives re-renders and Mapbox style reloads.
- `waterMaskVersion` (`useState`, counter) — changing this triggers dependent effects to re-run. Incremented whenever a new mask is stored.
- `maskBuildStartedRef` — guards against building the mask more than once per mount.

The SST and overlay effects both depend on `waterMaskVersion`. Both defer rendering if `waterMaskRef.current` is null.

The `styledata` listener watches for Mapbox wiping user-added layers. It has a 500ms debounce and a 3-second cooldown to prevent the infinite re-render loop described in problem 4 above.

---

## Auth and region access layer

The app uses Supabase (not Base44) for user authentication and subscription management. This is intentional — Base44 auth was avoided to allow future migration off Base44 without re-building auth.

**Key files:**
- `src/lib/supabase.js` — Supabase client singleton with hardcoded project URL and anon key.
- `src/hooks/useAuth.js` — wraps Supabase session state, fires on `onAuthStateChange`.
- `src/hooks/useRegionAccess.js` — fetches `user_subscriptions` row, creates free trial on first login, expires trials, provides `permittedRegions`/`daysLeft`/`tier` via React context.
- `src/components/auth/AuthGate.jsx` — shows login/register modal when no session exists. Wraps authenticated children in `RegionAccessProvider` so subscription data is fetched exactly once.
- `src/components/auth/TrialBanner.jsx` — dismissible banner shown when `tier === 'free_trial'`.
- `src/components/auth/UserMenu.jsx` — avatar dropdown with tier, region, sign out.
- `src/components/region/RegionGate.jsx` — checks `permittedRegions.includes(region)`. Shows `RegionSelect` page if access denied.
- `src/pages/RegionSelect.jsx` — region selection page showing all east coast regions. Only `outer_banks` is selectable; others are greyed out as coming soon.

**Supabase tables:**
- `auth.users` — managed by Supabase. View in Authentication → Users.
- `public.user_subscriptions` — `user_id`, `tier`, `regions[]`, `trial_ends_at`, `stripe_customer_id`. Edit rows directly in Table Editor to manually extend trials or activate subscriptions.
- `public.regions` — `slug`, `label`, `bounds` (jsonb). Currently seeded with `outer_banks`.

**Subscription tiers:** `free_trial` → `active` → `expired` / `cancelled`. Stripe webhooks will flip `tier` to `active` on payment (not yet implemented — `handleUpgrade` is a placeholder alert).

**Email confirmation redirect:** Supabase → Authentication → URL Configuration → Site URL must be set to `https://lintvetsstv2.base44.app`. If users get a `localhost refused to connect` error after clicking their confirmation email, this setting has been reset.

**`RegionAccessProvider` context pattern:** `useRegionAccess` must be called inside `RegionAccessProvider`. The provider is mounted by `AuthGate` only after a valid session exists. Calling `useRegionAccess` outside the provider throws. This is intentional — it prevents subscription fetches from firing for unauthenticated users and ensures the fetch happens exactly once regardless of how many components call the hook.

---

## Diagnostic logging

All frontend SST code emits `[SST:*]` and `[MASK]` log lines. Keep these in place.

**Healthy startup sequence looks like:**
```
[SST:MUR] response shape: Object
[SST:MUR] Grid OK: N lats × M lons ...
[SST:map] style.load fired
[SST:layer] mask not ready yet, deferring paint
[MASK] prebaked loaded in Xms (266×335, 11139 bytes)
[MASK] spot checks — Richmond VA (37.5,-77.4): LAND(ok), mid-Atlantic (36.5,-74.5): OCEAN(ok)
[SST:map] styledata — forcing re-paint   ← should appear only 1-2 times, not dozens
```

**Warning signs:**
- `styledata — forcing re-paint` appearing more than 3 times → infinite loop, check the cooldown guard.
- `[SST:MUR] WARNING: Grid appears SCATTERED` → backend returning non-grid data, not a frontend issue.
- `[SST:layer] mask not ready yet` appearing repeatedly → mask build failed, check network tab for `ocean_mask.json`.

Spot-check values in `[MASK]`:
- Richmond VA (37.5, -77.4) must be **LAND**.
- Mid-Atlantic (36.5, -74.5) must be **OCEAN**.
- If either is inverted, the mask orientation is broken.

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

1. **Flicker / continuous repaint** → check `styledata` handler has 500ms debounce + 3s cooldown (problem 4).
2. **SST or chlorophyll shifts north/south** → check Mercator math is still in `gridToDataURL` (problem 2).
3. **Sea color shifts north/south** → check `expandCoarseGrid` is being called for sea color branch and result is passed as `expandedGrid` to `gridToDataURL` with `latSet/lonSet` (problem 5).
4. **Chlorophyll shows polka dots** → someone passed `latSet/lonSet` instead of `latSet2/lonSet2` to `gridToDataURL` for chlorophyll. Revert.
5. **Chlorophyll or sea color shifted east of coastline** → someone overrode `addSource` coordinates with `dataBounds` or SST source coordinates. Revert to `imgWest/imgEast/imgNorth/imgSouth`.
6. **Mask not applying** → check `waterMaskRef.current` is not null before calling `gridToDataURL`. Both SST and overlay effects guard on this.
7. **Users get localhost error on email confirmation** → fix Supabase Site URL (auth section above).
8. **Users land on upgrade screen after login** → check `user_subscriptions` table in Supabase — row may not have been created. Check `useRegionAccess` insert error in console.
