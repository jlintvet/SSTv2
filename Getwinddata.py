import requests
import json
import base64
import os
import sys
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO   = "jlintvet/SSTv2"
GITHUB_PATH   = "WindData/wind_latest.json"
GITHUB_BRANCH = "main"

# OBX region grid — matches the map display bounds (slightly wider than SST data
# extent to ensure wind particles cover the full visible map area)
LAT_MIN, LAT_MAX, LAT_STEP = 32.0, 38.5, 0.25
LON_MIN, LON_MAX, LON_STEP = -79.0, -72.5, 0.25

# ── Build grid ────────────────────────────────────────────────────────────────
lats, lons = [], []
lat = LAT_MAX
while lat >= LAT_MIN - 0.001:
    lats.append(round(lat, 2))
    lat -= LAT_STEP
lon = LON_MIN
while lon <= LON_MAX + 0.001:
    lons.append(round(lon, 2))
    lon += LON_STEP

grid_lats, grid_lons = [], []
for la in lats:
    for lo in lons:
        grid_lats.append(la)
        grid_lons.append(lo)

print(f"Grid: {len(lats)} lats x {len(lons)} lons = {len(grid_lats)} points")

# ── Fetch Open-Meteo in batches (GET, 50 points per request) ─────────────────
# Open-Meteo is GET-only. With 700+ points the URL exceeds server limits (414).
# Solution: split into batches of 50 points, make multiple requests, merge.
BATCH_SIZE = 50

import time

def fetch_batch(lat_batch, lon_batch, retries=3):
    params = {
        "latitude":        ",".join(str(x) for x in lat_batch),
        "longitude":       ",".join(str(x) for x in lon_batch),
        "hourly":          "wind_u_component_10m,wind_v_component_10m,wind_speed_10m",
        "wind_speed_unit": "kn",
        "forecast_days":   "7",
        "timezone":        "UTC",
        "cell_selection":  "nearest",
        "models":          "gfs_seamless",
    }
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params=params,
                timeout=120,
            )
            r.raise_for_status()
            result = r.json()
            return result if isinstance(result, list) else [result]
        except Exception as e:
            if attempt < retries:
                wait = 5 * attempt
                print(f"    Attempt {attempt} failed ({e}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise

batches = [
    (grid_lats[i:i+BATCH_SIZE], grid_lons[i:i+BATCH_SIZE])
    for i in range(0, len(grid_lats), BATCH_SIZE)
]
print(f"Fetching from Open-Meteo in {len(batches)} batches of up to {BATCH_SIZE} points...")

raw = []
for bi, (lat_batch, lon_batch) in enumerate(batches):
    print(f"  Batch {bi+1}/{len(batches)} ({len(lat_batch)} points)...")
    try:
        raw.extend(fetch_batch(lat_batch, lon_batch))
    except Exception as e:
        print(f"ERROR: Batch {bi+1} failed after retries: {e}")
        sys.exit(1)
    time.sleep(0.3)  # brief pause between batches to avoid rate limiting

if not raw:
    print("ERROR: No data returned from Open-Meteo")
    sys.exit(1)

print(f"Got {len(raw)} point responses total")

# ── Parse per-hour grids ──────────────────────────────────────────────────────
times   = raw[0].get("hourly", {}).get("time", [])
n_hours = len(times)
print(f"Parsing {n_hours} hours ({times[0] if times else '?'} → {times[-1] if times else '?'})")

hour_grids = [[] for _ in range(n_hours)]
for idx, point_resp in enumerate(raw):
    la     = grid_lats[idx]
    lo     = grid_lons[idx]
    u_arr  = point_resp.get("hourly", {}).get("wind_u_component_10m", [])
    v_arr  = point_resp.get("hourly", {}).get("wind_v_component_10m", [])
    sp_arr = point_resp.get("hourly", {}).get("wind_speed_10m", [])
    for h in range(n_hours):
        u  = u_arr[h]  if h < len(u_arr)  else None
        v  = v_arr[h]  if h < len(v_arr)  else None
        sp = sp_arr[h] if h < len(sp_arr) else None
        if u is not None and v is not None:
            hour_grids[h].append({"lat": la, "lon": lo, "u": u, "v": v, "speed": sp})

# ── Shape into leaflet-velocity JSON ──────────────────────────────────────────
nx = len(lons)
ny = len(lats)
lat_index = {la: i for i, la in enumerate(lats)}
lon_index = {lo: i for i, lo in enumerate(lons)}

base_header = {
    "parameterUnit":     "knots",
    "parameterCategory": 2,
    "surface":           103,
    "surfaceValues":     10,
    "lo1": lons[0],
    "lo2": lons[-1],
    "la1": lats[0],
    "la2": lats[-1],
    "dx":  LON_STEP,
    "dy":  LAT_STEP,
    "nx":  nx,
    "ny":  ny,
    "refTime": datetime.now(timezone.utc).isoformat(),
}

def build_velocity_json(pts):
    u_data = [0.0] * (ny * nx)
    v_data = [0.0] * (ny * nx)
    for pt in pts:
        ri = lat_index.get(pt["lat"])
        ci = lon_index.get(pt["lon"])
        if ri is None or ci is None:
            continue
        i2 = ri * nx + ci
        u_data[i2] = pt["u"] or 0.0
        v_data[i2] = pt["v"] or 0.0
    return [
        {"header": {**base_header, "parameterNumberName": "eastward_wind",  "parameterNumber": 2}, "data": u_data},
        {"header": {**base_header, "parameterNumberName": "northward_wind", "parameterNumber": 3}, "data": v_data},
    ]

print("Building velocity JSON for each hour...")
hours = []
for h, t in enumerate(times):
    hours.append({
        "time":         t,
        "velocityJSON": build_velocity_json(hour_grids[h]),
    })

all_speeds = [
    pt["speed"]
    for pts in hour_grids
    for pt in pts
    if pt.get("speed") is not None
]
max_speed = round(max(all_speeds), 1) if all_speeds else 30.0
print(f"Max speed: {max_speed} kt")

payload = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "source":       "Open-Meteo GFS/HRRR",
    "grid":         {"lats": lats, "lons": lons, "nx": nx, "ny": ny},
    "maxSpeed":     max_speed,
    "hours":        hours,
}

# ── Commit to GitHub ──────────────────────────────────────────────────────────
if not GITHUB_TOKEN:
    print("ERROR: GITHUB_TOKEN env var not set")
    sys.exit(1)

json_bytes   = json.dumps(payload, separators=(",", ":")).encode("utf-8")
content_b64  = base64.b64encode(json_bytes).decode("utf-8")
file_size_kb = len(json_bytes) / 1024
print(f"Payload size: {file_size_kb:.1f} KB")

gh_headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept":        "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
file_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}"

# Get current file SHA (required by GitHub API to overwrite an existing file)
sha = None
try:
    get_resp = requests.get(file_url, headers=gh_headers,
                            params={"ref": GITHUB_BRANCH}, timeout=15)
    if get_resp.status_code == 200:
        sha = get_resp.json().get("sha")
        print(f"Existing file SHA: {sha[:8]}...")
    else:
        print("No existing file — will create fresh")
except Exception as e:
    print(f"Warning: could not fetch existing SHA: {e}")

commit_body = {
    "message": f"wind data update {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M')}Z",
    "content": content_b64,
    "branch":  GITHUB_BRANCH,
}
if sha:
    commit_body["sha"] = sha

print(f"Writing to GitHub: {GITHUB_PATH}")
try:
    put_resp = requests.put(file_url, headers=gh_headers,
                            json=commit_body, timeout=30)
    put_resp.raise_for_status()
except Exception as e:
    print(f"ERROR: GitHub write failed: {e}")
    if put_resp is not None:
        print(f"Status: {put_resp.status_code}")
        print(f"Body: {put_resp.text[:500]}")
    sys.exit(1)

print(f"Done. {n_hours} hours, {len(grid_lats)} grid points, {file_size_kb:.1f} KB")
print(f"URL: https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{GITHUB_PATH}")
