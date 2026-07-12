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

# Grid covers mid_atlantic (33.7–39.0N, -78.89–-72.21W), ga_sc
# (29.80–35.20N, -82.0–-75.20W), ne_fl (26.00–30.50N, -81.97–-76.14W), and
# va_ri (37.26–41.51N, -77.46–-68.97W) with margin on all sides. LAT_MIN was
# widened from 29.0 to 25.0 to reach ne_fl's southern edge (26.00N); LAT_MAX
# widened from 40.0 to 42.0 and LON_MAX widened from -72.5 to -68.0 to reach
# va_ri's northern (41.51N) and eastern (-68.97W) edges.
LAT_MIN, LAT_MAX, LAT_STEP = 25.0, 42.0, 0.25
LON_MIN, LON_MAX, LON_STEP = -82.5, -68.0, 0.25

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
failed_batches = 0
for bi, (lat_batch, lon_batch) in enumerate(batches):
    print(f"  Batch {bi+1}/{len(batches)} ({len(lat_batch)} points)...")
    try:
        raw.extend(fetch_batch(lat_batch, lon_batch))
    except Exception as e:
        # Don't let one flaky batch abort the whole hourly run. The grid
        # widened from 51 to 82 batches for va_ri (2026-07-12), raising the
        # odds that some single batch hits a transient Open-Meteo failure;
        # previously that aborted the entire run via sys.exit(1) with no
        # commit at all, silently leaving the pre-widening file in place
        # through every subsequent hourly attempt. Skip this batch's points
        # instead -- they're absent from hour_grids and default to 0 wind,
        # same as any other missing point, rather than losing the whole
        # region's update over one bad batch.
        failed_batches += 1
        print(f"WARNING: Batch {bi+1} failed after retries, skipping ({len(lat_batch)} points lost): {e}")
    time.sleep(0.3)  # brief pause between batches to avoid rate limiting

if not raw:
    print("ERROR: No data returned from Open-Meteo (all batches failed)")
    sys.exit(1)
if failed_batches:
    print(f"Completed with {failed_batches}/{len(batches)} batch(es) failed -- proceeding with partial grid")

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
        # Pass U/V as-is from Open-Meteo (standard meteorological "wind going toward")
        # Direction convention is handled by angleConvention in leaflet-velocity
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
        "grid":         hour_grids[h],   # speed points needed for wind map raster
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
api_base = f"https://api.github.com/repos/{GITHUB_REPO}"

# The Contents API's single-shot PUT (base64 content inline in the JSON
# body) rejects large files with a 422 "file is too large to be processed"
# -- hit in production once the va_ri grid widening (61x41 -> 69x59 points)
# pushed this payload past ~40 MB. Use the Git Data API instead: it handles
# arbitrarily large blobs via a multi-step blob/tree/commit/ref sequence,
# exactly what GitHub's own 422 error message recommends switching to.
print(f"Writing to GitHub via Git Data API: {GITHUB_PATH}")
try:
    # 1) Current branch ref -> latest commit SHA
    ref_resp = requests.get(f"{api_base}/git/ref/heads/{GITHUB_BRANCH}",
                             headers=gh_headers, timeout=15)
    ref_resp.raise_for_status()
    latest_commit_sha = ref_resp.json()["object"]["sha"]

    # 2) Latest commit -> its tree SHA (used as base_tree so every other
    #    file in the repo is carried forward unchanged)
    commit_resp = requests.get(f"{api_base}/git/commits/{latest_commit_sha}",
                                headers=gh_headers, timeout=15)
    commit_resp.raise_for_status()
    base_tree_sha = commit_resp.json()["tree"]["sha"]

    # 3) Create a blob for the new file content
    blob_resp = requests.post(f"{api_base}/git/blobs", headers=gh_headers,
                               json={"content": content_b64, "encoding": "base64"},
                               timeout=120)
    blob_resp.raise_for_status()
    blob_sha = blob_resp.json()["sha"]

    # 4) Create a new tree that replaces just this one path
    tree_resp = requests.post(f"{api_base}/git/trees", headers=gh_headers,
                               json={
                                   "base_tree": base_tree_sha,
                                   "tree": [{
                                       "path": GITHUB_PATH,
                                       "mode": "100644",
                                       "type": "blob",
                                       "sha":  blob_sha,
                                   }],
                               }, timeout=30)
    tree_resp.raise_for_status()
    new_tree_sha = tree_resp.json()["sha"]

    # 5) Create a new commit on top of the latest one
    new_commit_resp = requests.post(f"{api_base}/git/commits", headers=gh_headers,
                                     json={
                                         "message": f"wind data update {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M')}Z",
                                         "tree":    new_tree_sha,
                                         "parents": [latest_commit_sha],
                                     }, timeout=30)
    new_commit_resp.raise_for_status()
    new_commit_sha = new_commit_resp.json()["sha"]

    # 6) Fast-forward the branch ref to the new commit
    update_ref_resp = requests.patch(f"{api_base}/git/refs/heads/{GITHUB_BRANCH}",
                                      headers=gh_headers,
                                      json={"sha": new_commit_sha, "force": False},
                                      timeout=30)
    update_ref_resp.raise_for_status()
    print(f"Committed as {new_commit_sha[:8]}")
except Exception as e:
    print(f"ERROR: GitHub write failed: {e}")
    resp = locals().get("update_ref_resp") or locals().get("new_commit_resp") or \
           locals().get("tree_resp") or locals().get("blob_resp") or \
           locals().get("commit_resp") or locals().get("ref_resp")
    if resp is not None:
        print(f"Status: {resp.status_code}")
        print(f"Body: {resp.text[:500]}")
    sys.exit(1)

print(f"Done. {n_hours} hours, {len(grid_lats)} grid points, {file_size_kb:.1f} KB")
print(f"URL: https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{GITHUB_PATH}")
