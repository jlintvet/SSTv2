# SST Temperature Break Contour Layer
**OceanCast / SSTv2 — Developer & User Reference**

---

## What It Does

The Temp Break layer traces temperature contour lines directly on the SST heatmap. It draws two distinct lines simultaneously — a subtle reference isotherm and a bold "fishing edge" highlight — so you can see both where a target temperature exists and where that temperature sits on a genuinely hard thermal wall.

Toggle it on with the **〰️ Temp Break** button in the SST control panel (desktop) or the 〰️ icon in the mobile bottom bar. It only activates when the SST data layer is selected.

---

## The Two Lines Explained

### Dotted White Line — The Isotherm

This is the pure mathematical contour: every point on the map where SST equals exactly your target temperature (e.g. 68°F).

The algorithm (Marching Squares) walks the SST grid cell by cell and interpolates the exact crossing point on each cell edge where the temperature transitions through the target value. The result is a continuous line tracing the full target-temperature boundary across the entire map — even through areas where the temperature change is very gradual, like a broad slow warming zone in the middle of the Gulf Stream.

Because this line exists *everywhere* the temperature happens to hit the target, it can be long, wiggly, and not especially meaningful on its own. That's why it's rendered subtly — thin, dashed, semi-transparent white — so it reads as reference context rather than a signal.

### Electric Blue Line — The Temperature Break

This is a filtered subset of that same isotherm. Before running Marching Squares, the algorithm checks each grid cell's **gradient** — the maximum temperature difference between that cell and its four direct neighbors. Only cells where that difference meets or exceeds your **Sensitivity** slider value (default 2°F per cell) are kept; the rest are masked out. Marching Squares then runs again on the masked field, so it only traces the target contour *through the steep-gradient zones*.

The result: the electric blue only appears where the isotherm is also a genuine thermal wall — a place where water temperature changes sharply over a short distance. That's the fishing edge. Bait concentrates there, and predators work the break. The glow effect (a wide blurred halo behind the crisp line) makes it pop visually against the SST color ramp.

---

## The Controls

| Control | What It Does |
|---|---|
| **〰️ Temp Break** button | Toggles the entire layer on/off |
| **Target Temp slider** | Sets the target temperature in °F. Auto-initializes to the midpoint of the current SST range. Range spans the current data min→max. |
| **Sensitivity slider** | Sets the gradient threshold in °F per grid cell. Low = permissive (more blue). High = strict (only hard breaks show blue). |

### Sensitivity Guide

| Setting | Behavior | Best For |
|---|---|---|
| 0.5°F | Very permissive — almost any gradient qualifies, blue closely follows the white line | Finding any thermal structure |
| 1.5–3°F | Moderate — isolates meaningful edges, filters out broad gradual warmings | **Typical offshore fishing use** |
| 5–8°F | Strict — only violent temperature walls appear | Locating the sharpest Gulf Stream edges |

---

## How the Algorithm Works

### Step 1 — Build the scalar field
The sparse `lat_lon → SST` grid lookup object is converted into a dense 2D `Float32Array` with NaN for missing/masked cells.

### Step 2 — Full isotherm (Marching Squares)
Standard Marching Squares is run on the full field at the target temperature. For each 2×2 cell, a 4-bit case index is computed from which corners are above/below the iso-value. The 16 standard MS edge-crossing cases emit line segments, which are then chained into polylines via endpoint quantization and adjacency matching — producing compact GeoJSON instead of raw segment soup.

### Step 3 — Gradient field
For each grid cell, the maximum absolute temperature difference to any of its 4 direct (cardinal) neighbors is computed and stored in a parallel `Float32Array`.

### Step 4 — Temperature break (masked Marching Squares)
A copy of the scalar field is made. Any cell whose gradient falls below the sensitivity threshold is set to NaN, masking it out. Marching Squares runs again on this masked field at the same target temperature. The result is contour segments that only exist inside high-gradient zones.

### Step 5 — GeoJSON assembly
Both contour sets are assembled into a single GeoJSON `FeatureCollection`. Each feature is tagged with a `kind` property (`"isotherm"` or `"break"`) so Mapbox filter expressions can style them independently.

### Step 6 — Mapbox rendering (3 layers)
| Layer ID | Filter | Style |
|---|---|---|
| `isotherm-layer` | `kind == "isotherm"` | White dashed line, 1.5px, 65% opacity |
| `tempbreak-layer-glow` | `kind == "break"` | Wide (7px) blurred cyan halo for glow effect |
| `tempbreak-layer` | `kind == "break"` | Crisp `#00cfff` electric-blue line, 2.5px |

All three layers are inserted below the first Mapbox symbol layer so labels and place names always render on top.

---

## Key Files

| File | Purpose |
|---|---|
| `SSTIsoTherms.jsx` | Full page component including the isotherm engine |
| `buildIsothermGeoJSON()` | Main entry point — calls both MS passes, returns FeatureCollection |
| `marchingSquares()` | Core contour tracer — handles all 16 MS cases + polyline chaining |
| `computeTempBreakContour()` | Builds gradient field, masks low-gradient cells, calls marchingSquares |
| `IsothermControls` | Self-contained React sub-component for the control panel sliders |

---

## Notes & Gotchas

- **Grid resolution matters.** The SST grid is ~0.05° per cell (~3 miles). The sensitivity threshold is in °F *per grid cell*, not per mile. A 2°F sensitivity means the temperature changes at least 2°F within ~3 miles.
- **Scattered grids produce no contours.** If the backend returns scattered (non-regular) points, the MS algorithm will find no valid 2×2 cells to process. The amber warning banner at the top of the page indicates this condition.
- **Layer auto-clears on source switch.** Switching SST source (MUR → VIIRS → etc.) re-triggers the isotherm effect via the `waterMaskVersion` dependency, so contours always match the currently displayed data.
- **Only active on SST layer.** The `{activeDataLayer === "sst"}` guard means the button and contours disappear when Chlorophyll or Sea Color is selected — those layers don't have a meaningful temperature field to contour.
- **Performance.** The Marching Squares pass runs in a `setTimeout(..., 60ms)` defer so it doesn't block the React render. On a typical MUR grid (~2,000 cells) it completes in under 30ms.

---

*Last updated: April 2026*
