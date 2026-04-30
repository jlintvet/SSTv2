# Wind Data Retrieval & Display System

**Project:** OceanCast / SSTLive  
**Region:** Outer Banks (OBX), North Carolina  
**Last Updated:** April 2026  

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Data Pipeline — Python Backend](#2-data-pipeline--python-backend)
3. [GitHub Storage](#3-github-storage)
4. [GitHub Actions Workflow](#4-github-actions-workflow)
5. [Frontend Architecture](#5-frontend-architecture)
6. [Wind Display Modes](#6-wind-display-modes)
7. [Color & Rendering](#7-color--rendering)
8. [Time Slider Component](#8-time-slider-component)
9. [Leaflet-Velocity Integration](#9-leaflet-velocity-integration)
10. [Known Issues & Lessons Learned](#10-known-issues--lessons-learned)
11. [File Index](#11-file-index)
12. [Recreate From Scratch Checklist](#12-recreate-from-scratch-checklist)

---

## 1. System Overview

The wind system fetches 168-hour (7-day) hourly wind forecast data from Open-Meteo's free GFS/HRRR API, writes a single static JSON file to GitHub, and serves it directly to the frontend. No backend call at runtime — the frontend fetches the static file directly from GitHub raw CDN.

```
Open-Meteo API (GFS/HRRR)
        ↓
  Getwinddata.py  ← runs every 3h via GitHub Actions
        ↓
  WindData/wind_latest.json  (GitHub repo)
        ↓
  raw.githubusercontent.com CDN
        ↓
  SSTLive.jsx frontend
        ↓
  leaflet-velocity particles + canvas raster overlay
```

---

## 2. Data Pipeline — Python Backend

### File
`Getwinddata.py` — lives in the **repo root**. Runs as a standalone script (no handler wrapper).

### Grid Definition

```python
LAT_MIN, LAT_MAX, LAT_STEP = 32.0, 40.0, 0.25   # must exceed map's north bound
LON_MIN, LON_MAX, LON_STEP = -79.0, -72.5, 0.25
```

- **~858 grid points** (27 lats × ~27 lons with current bounds, adjust as needed)
- Grid must extend **north past the map's `regionBounds.north`** or the raster will have a blank top edge
- Grid is wider than the SST data extent — this is intentional so wind covers the whole map

### API — Open-Meteo

```
GET https://api.open-meteo.com/v1/forecast
```

**Key parameters:**
| Parameter | Value | Notes |
|---|---|---|
| `latitude` | comma-separated list | Up to 50 per request |
| `longitude` | comma-separated list | Paired with latitude |
| `hourly` | `wind_u_component_10m,wind_v_component_10m,wind_speed_10m` | U, V in m/s converted; speed in chosen unit |
| `wind_speed_unit` | `kn` | Returns knots — marine standard |
| `forecast_days` | `7` | 168 hours |
| `timezone` | `UTC` | Always UTC |
| `cell_selection` | `nearest` | Nearest land/sea point |
| `models` | `gfs_seamless` | GFS + HRRR blend — best for US coastal |

### Batching

Open-Meteo is **GET-only** (POST returns 400). URLs exceeding ~8KB return 414 Too Large. Solution: batch 50 points per request.

```python
BATCH_SIZE = 50
# ~858 points = 18 batches
# Each batch: ~2s response time
# Total fetch: ~40-60s
```

Retry logic: 3 attempts per batch, backoff of 5s × attempt number. 300ms pause between batches to avoid rate limiting. Timeout: 120s per batch.

### U/V Component Convention

Open-Meteo returns standard meteorological vectors:
- **U** = eastward component (positive = wind blowing toward east)
- **V** = northward component (positive = wind blowing toward north)

**Do NOT negate these values.** Pass them as-is to leaflet-velocity and set `angleConvention: "meteoCW"` on the display layer. Negating U/V was tried and produced wrong directions.

### leaflet-velocity JSON Format

Each hour's data is shaped into the exact format leaflet-velocity expects:

```json
[
  {
    "header": {
      "parameterUnit": "knots",
      "parameterCategory": 2,
      "parameterNumber": 2,
      "parameterNumberName": "eastward_wind",
      "lo1": -79.0,   "lo2": -72.5,
      "la1": 40.0,    "la2": 32.0,
      "dx": 0.25,     "dy": 0.25,
      "nx": 27,       "ny": 33,
      "refTime": "2026-04-30T12:00:00Z"
    },
    "data": [u0, u1, u2, ...]
  },
  {
    "header": { "...parameterNumber": 3, "parameterNumberName": "northward_wind", ... },
    "data": [v0, v1, v2, ...]
  }
]
```

- `la1` = northernmost lat, `la2` = southernmost lat
- `data` arrays are **row-major, north→south, west→east**
- `nx * ny` must equal `len(data)`

### Output JSON Structure

```json
{
  "generated_at": "2026-04-30T15:00:00Z",
  "source": "Open-Meteo GFS/HRRR",
  "grid": {
    "lats": [40.0, 39.75, ...],
    "lons": [-79.0, -78.75, ...],
    "nx": 27,
    "ny": 33
  },
  "maxSpeed": 41.9,
  "hours": [
    {
      "time": "2026-04-30T00:00",
      "velocityJSON": [...],   // leaflet-velocity format
      "grid": [                // raw points for raster rendering
        {"lat": 40.0, "lon": -79.0, "u": 3.2, "v": -1.1, "speed": 7.4},
        ...
      ]
    },
    ...168 total
  ]
}
```

The per-hour `grid` array is required — the frontend needs raw speed values to build the color raster. File size: ~2–4 MB.

### GitHub Write

Uses GitHub Contents API to upsert the file:

```python
# GET current SHA first (required for updates)
GET https://api.github.com/repos/{REPO}/contents/{PATH}?ref=main
# → extract .sha

# PUT with content (base64) + sha
PUT https://api.github.com/repos/{REPO}/contents/{PATH}
Body: { "message": "wind data update ...", "content": "<base64>", "branch": "main", "sha": "<sha>" }
```

Token: `GITHUB_TOKEN` environment variable. Must have **Contents: Read & Write** on the repo.

---

## 3. GitHub Storage

- **Repo:** `jlintvet/SSTv2`
- **Branch:** `main`
- **File path:** `WindData/wind_latest.json`
- **Raw URL:** `https://raw.githubusercontent.com/jlintvet/SSTv2/main/WindData/wind_latest.json`

Single file, always overwritten. No versioning or per-day files. Frontend always gets latest forecast.

---

## 4. GitHub Actions Workflow

**File:** `.github/workflows/update_wind_data.yml`

```yaml
on:
  schedule:
    - cron: '15 */3 * * *'   # every 3 hours at :15 past
  workflow_dispatch:          # manual trigger button in Actions tab
```

**Key settings:**
- `runs-on: ubuntu-latest`
- `timeout-minutes: 10`
- `python-version: '3.11'`
- `pip install requests` (only dependency beyond stdlib)
- `run: python Getwinddata.py` — script is in repo root
- `GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}` — auto-provisioned, no manual secret needed

**Required repo permission:**
`Settings → Actions → General → Workflow permissions → Read and write permissions`

**First run:** Must be triggered manually via Actions tab → "Run workflow" button to create the initial `wind_latest.json` before the schedule kicks in.

---

## 5. Frontend Architecture

### Data Fetch

Wind data is fetched **once per session**, the first time either wind mode is activated:

```javascript
const WIND_DATA_URL = "https://raw.githubusercontent.com/jlintvet/SSTv2/main/WindData/wind_latest.json";

useEffect(() => {
  if (!windActive || windData || windLoading) return;
  fetch(WIND_DATA_URL)
    .then(r => r.json())
    .then(d => {
      setWindData(d);
      // Seek to current UTC hour
      const nowISO = new Date().toISOString().slice(0, 13);
      const idx = d.hours.findIndex(h => h.time.startsWith(nowISO));
      setWindHourIndex(idx >= 0 ? idx : 0);
    });
}, [windActive]);
```

### State Variables (in `SSTPage`)

```javascript
const [windData,        setWindData]        = useState(null);
const [windLoading,     setWindLoading]     = useState(false);
const [windHourIndex,   setWindHourIndex]   = useState(0);
const [showWindOverlay, setShowWindOverlay] = useState(false);
const [windPlaying,     setWindPlaying]     = useState(false);
```

`activeDataLayer === "windmap"` is the wind map mode (stored in existing `activeDataLayer` state, not a separate flag).

### Refs (in `SSTHeatmapLeaflet`)

```javascript
const velocityLayerRef     = useRef(null);  // leaflet-velocity particles
const windRasterOverlayRef = useRef(null);  // color fill raster imageOverlay
```

### Two-Effect Pattern (critical — do not merge back into one)

**Effect A — CREATE** (deps: `windActive, windData, showWindOverlay, isWindMap, repaintTrigger`)
- Tears down and recreates the velocity particle layer
- Runs when mode switches or data first loads
- Does NOT run on every hour tick

**Effect B — UPDATE** (deps: `windHourIndex, isWindMap`)
- Calls `velocityLayer.setData(hourData.velocityJSON)` in place — no teardown
- Rebuilds the raster overlay for the new hour
- This is what makes animation smooth — no blank frames between hours

Merging these back into one effect causes blank canvas flicker during play.

---

## 6. Wind Display Modes

### Mode A — Wind Overlay

Activated by: **"Wind Overlay"** toggle button in control panel (independent of data layer selector)

- White semi-transparent particle streamlines over whatever data layer is active (SST, CHL, Sea Color)
- No color gradient raster underneath
- Wind speed legend **hidden** (would compete with SST colors)
- Particles: `opacity: 0.65`, `lineWidth: 1.8`

### Mode B — Wind Map

Activated by: **"Wind Map"** button in Data Layer section

- Color gradient raster fill (ocean-masked) showing wind speed
- White direction particles on top
- SST raster hidden
- Wind speed legend shown in place of SST legend
- Bathy contours still visible underneath
- Particles: `opacity: 0.85`, `lineWidth: 2.0`

Both modes share the same time slider and play/pause controls.

---

## 7. Color & Rendering

### Wind Speed Color Scale

```javascript
const WIND_SPEED_STOPS = [
  [0,    [0,   0,   255]],   // deep blue — calm
  [0.07, [0,   85,  255]],
  [0.14, [0,   153, 255]],
  [0.21, [0,   204, 255]],
  [0.28, [0,   255, 204]],
  [0.35, [0,   255, 136]],
  [0.43, [0,   255, 0  ]],
  [0.5,  [136, 255, 0  ]],
  [0.57, [204, 255, 0  ]],
  [0.64, [255, 255, 0  ]],   // yellow
  [0.71, [255, 204, 0  ]],
  [0.78, [255, 153, 0  ]],
  [0.85, [255, 102, 0  ]],
  [0.92, [255, 51,  0  ]],
  [1.0,  [255, 0,   0  ]],   // red — strong
];
```

### Raster Rendering

The wind raster uses `gridToDataURL()` — the same canvas renderer as SST. Key points:

1. Build a render grid from `regionBounds` at 0.25° steps (not from the wind data's own lat/lon arrays)
2. **Bilinear interpolate** wind speed from the 4 surrounding wind grid cells to each render point
3. Clamp lat/lon to wind grid extent before interpolating (prevents null returns at edges)
4. Pass `waterMaskRef.current` as the ocean mask — same mask as SST
5. The resulting imageOverlay bounds come from the render grid, not regionBounds directly

**Critical:** Do NOT pass `null` for the ocean mask — this causes land coverage. Do NOT use `latSet`/`lonSet` from SST props — this causes misalignment when SST grid doesn't cover full map bounds.

### Ocean Mask

The prebaked mask at `DailySSTData/ocean_mask.json` is anchored to fixed `bounds.south`/`bounds.west` coordinates. Any lat/lon passed to `isOcean()` outside those bounds may return incorrect values. The render grid must stay within the mask's anchor region — which is why `regionBounds` (the SST display region) works but the raw wind grid bounds (which extend to -79°W) do not.

---

## 8. Time Slider Component

`WindTimeSlider` — fixed position at map bottom, full width, Windy-style.

### Layout

```
┌─────────────────────────────────────────────────────────────┐
│  [tooltip bubble: "Thu 30 - 2 PM"]                          │  ← 28px
├──────┬──────────────────────────────────────────┬───────────┤
│  ▶   │  Thu 30 │  Fri 1  │  Sat 2  │  Sun 3 … │  kt legend│  ← 52px
│      │         [══════════●══════════]           │           │
└──────┴──────────────────────────────────────────┴───────────┘
```

- Background: `rgba(23,28,38,0.72)` with `backdrop-filter: blur(8px)`
- Tooltip: amber `#f59e0b` pill above slider thumb
- Day columns: clickable, bold+white when active, gray otherwise
- Clicking a day jumps `windHourIndex` to `day.startIdx`
- Play speed: **2333ms per hour step** (≈15% of original 350ms) — gives particles time to render
- Play auto-stops at last hour; pressing play from end resets to hour 0

### Wind Speed Legend (Wind Map mode only)

- Gradient bar: `linear-gradient(to right, WIND_COLOR_SCALE...)`
- Tick labels positioned under bar at even intervals: 0, 5, 10, 15, 20, 25, 30 kt
- Unit label `kt` to the left
- **Hidden** in Wind Overlay mode

---

## 9. Leaflet-Velocity Integration

### Loading

Loaded from CDN at runtime (no npm install):

```javascript
// CSS
link.href = "https://cdn.jsdelivr.net/npm/leaflet-velocity@1.9.2/dist/leaflet-velocity.css";
// JS
script.src = "https://cdn.jsdelivr.net/npm/leaflet-velocity@1.9.2/dist/leaflet-velocity.min.js";
```

Script loads asynchronously. The CREATE effect checks `if (!L.velocityLayer)` and retries via `setRepaintTrigger` if not yet available.

### Layer Configuration

```javascript
L.velocityLayer({
  displayValues: true,
  displayOptions: {
    velocityType:    "Wind",
    position:        "bottomleft",
    emptyString:     "No wind data",
    angleConvention: "meteoCW",   // ← CRITICAL: must match Open-Meteo convention
    showCardinal:    true,
    speedUnit:       "kt",
  },
  data:               hourData.velocityJSON,
  minVelocity:        0,
  maxVelocity:        windData.maxSpeed,
  velocityScale:      0.005,
  colorScale:         whiteScale,       // always white particles
  opacity:            0.65–0.85,
  particleAge:        40,
  particleMultiplier: 0.0008,
  lineWidth:          1.8–2.0,
})
```

### angleConvention

This is the most error-prone setting. History:
- `bearingCW` → arrows pointed ~90° wrong
- Negating U/V → 180° flip, still wrong
- `meteoCW` with un-negated U/V → correct, matches Windy

**Rule:** Pass U/V exactly as received from Open-Meteo. Set `angleConvention: "meteoCW"`.

### CSS Fixes Required

Two Tailwind conflicts must be overridden:

```css
/* Prevents image overlays collapsing to 0px */
.leaflet-container img.leaflet-image-layer,
.leaflet-container img.leaflet-tile,
.leaflet-pane img {
  max-width: none !important;
  max-height: none !important;
}

/* Prevents velocity canvas being clipped at map top */
.leaflet-velocity-layer canvas { max-width: none !important; max-height: none !important; }
.leaflet-overlay-pane canvas { overflow: visible !important; }
```

These are injected into `<head>` at module load time, not in a stylesheet.

---

## 10. Known Issues & Lessons Learned

### Direction Convention (resolved)
`leaflet-velocity` requires `angleConvention: "meteoCW"` for Open-Meteo GFS/HRRR data. Do not negate U/V components. This caused multiple debugging sessions.

### URL Length Limit (resolved)
Open-Meteo GET requests with >~100 lat/lon pairs exceed URL length limits (414 Too Large). Solution: batch 50 points per request. POST with JSON body returns 400 — API is GET-only.

### Ocean Mask Alignment
The prebaked mask is anchored to SST `regionBounds`. Wind raster must use the same bounds when rendering. Attempting to render a wider grid (e.g. the full -79 to -72.5°W wind extent) causes the mask to return incorrect values for out-of-bounds coordinates, producing misaligned or land-covered rendering.

### Particle Blanking During Play (resolved)
A single `useEffect` watching `windHourIndex` was tearing down and recreating the velocity layer on every frame. At 350ms intervals, the new layer never finished rendering before the next frame. Solution: split into CREATE effect (no hour dep) and UPDATE effect (calls `setData()` in place).

### Build Cache
Base44's bundler may serve stale compiled output. After deploying JSX changes, force a fresh build by hard-reloading (`Cmd+Shift+R`) and clearing site data. A build comment at the top of the file (`// build 2026-04-30-X`) changes the file hash and forces recompilation.

### Northern Coverage Gap
If the wind data grid's `LAT_MAX` is below the map's `regionBounds.north`, the raster will have a blank strip at the top. `LAT_MAX` in `Getwinddata.py` must exceed the map's north bound by at least one grid step (0.25°). Current value: 40.0°N.

### Particle Count / Density
`particleMultiplier` at the default leaflet-velocity value (`0.003–0.004`) produces far too many particles at OBX zoom level, making light winds appear like hurricane conditions. Correct value for this region: `0.0008`.

---

## 11. File Index

| File | Location | Purpose |
|---|---|---|
| `Getwinddata.py` | Repo root | Fetches Open-Meteo, writes wind_latest.json to GitHub |
| `.github/workflows/update_wind_data.yml` | Repo | Scheduled GitHub Action (every 3h) |
| `WindData/wind_latest.json` | Repo (generated) | Static wind data file served via CDN |
| `SSTLive.jsx` | Base44 app | Full frontend — SST + wind display |

---

## 12. Recreate From Scratch Checklist

### Backend & Pipeline

- [ ] Create `Getwinddata.py` in repo root with grid bounds that exceed map's `regionBounds` on all sides by ≥0.25°
- [ ] Confirm `LAT_MAX` > map's north bound (currently needs 40.0° for OBX region)
- [ ] Set `forecast_days=7`, `models=gfs_seamless`, `wind_speed_unit=kn`
- [ ] Implement batch fetching (50 points/request) with retry + 300ms pause
- [ ] Include per-hour `grid` array (raw points) in output — required for raster rendering
- [ ] Include top-level `grid.lats`/`grid.lons` arrays in output
- [ ] Set `GITHUB_TOKEN` env var; test GitHub write (GET sha → PUT content)
- [ ] Create `WindData/` directory in repo (GitHub creates on first write)

### GitHub Actions

- [ ] Create `.github/workflows/update_wind_data.yml`
- [ ] Set cron to `'15 */3 * * *'`
- [ ] Add `workflow_dispatch` for manual trigger
- [ ] Enable `Settings → Actions → General → Read and write permissions`
- [ ] Run manually first to create initial `wind_latest.json`

### Frontend

- [ ] Add leaflet-velocity CDN script + CSS injection at module load (check `!L.velocityLayer` before using)
- [ ] Add CSS overrides for Tailwind conflicts and canvas overflow
- [ ] Add state: `windData`, `windLoading`, `windHourIndex`, `showWindOverlay`, `windPlaying`
- [ ] Fetch from `raw.githubusercontent.com` (not via backend function)
- [ ] Set initial `windHourIndex` to current UTC hour on load
- [ ] Implement TWO separate effects (CREATE + UPDATE) — do not merge
- [ ] CREATE effect deps: `[windActive, windData, showWindOverlay, isWindMap, repaintTrigger]`
- [ ] UPDATE effect deps: `[windHourIndex, isWindMap]` — calls `setData()` in place
- [ ] Use `angleConvention: "meteoCW"` — do not negate U/V
- [ ] Wind raster: build render grid from `regionBounds` at 0.25° steps
- [ ] Wind raster: bilinear interpolate from wind grid, clamp to grid extent
- [ ] Wind raster: pass `waterMaskRef.current` (not null) to `gridToDataURL`
- [ ] Hide wind legend in overlay mode; show only in wind map mode
- [ ] Set `particleMultiplier: 0.0008` for correct density at OBX zoom
- [ ] Set play interval to 2333ms (not 350ms)
- [ ] Time slider: day columns clickable, bold active day tracks with `windHourIndex`

---

*Document generated April 2026. Reflects system state as of SSTLive build 2026-04-30-C.*
