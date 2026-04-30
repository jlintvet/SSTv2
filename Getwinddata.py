import requests
import json
import base64
import os
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = "jlintvet/SSTv2"
GITHUB_PATH  = "WindData/wind_latest.json"
GITHUB_BRANCH = "main"

# OBX region grid
LAT_MIN, LAT_MAX, LAT_STEP = 33.5, 37.0, 0.25
LON_MIN, LON_MAX, LON_STEP = -77.0, -74.0, 0.25


def handler(req, res):
    """
    Scheduled Base44 function — runs every 3 hours.
    1. Fetches 48h of U/V wind from Open-Meteo GFS/HRRR for OBX grid
    2. Shapes data into leaflet-velocity JSON per hour
    3. Commits wind_latest.json to GitHub (overwrite)
    """

    # ── Build grid ────────────────────────────────────────────────────────────
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

    # ── Fetch Open-Meteo ──────────────────────────────────────────────────────
    params = {
        "latitude":        ",".join(str(x) for x in grid_lats),
        "longitude":       ",".join(str(x) for x in grid_lons),
        "hourly":          "wind_u_component_10m,wind_v_component_10m,wind_speed_10m",
        "wind_speed_unit": "kn",
        "forecast_days":   "2",
        "timezone":        "UTC",
        "cell_selection":  "nearest",
        "models":          "gfs_seamless",
    }

    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params=params,
            timeout=60,
        )
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        return res.status(502).json({"error": "Open-Meteo fetch failed", "detail": str(e)})

    if isinstance(raw, dict):
        raw = [raw]
    if not raw:
        return res.status(502).json({"error": "Empty response from Open-Meteo"})

    # ── Parse per-hour grids ──────────────────────────────────────────────────
    times   = raw[0].get("hourly", {}).get("time", [])
    n_hours = len(times)

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

    # ── Shape into leaflet-velocity JSON ──────────────────────────────────────
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

    hours = []
    for h, t in enumerate(times):
        hours.append({
            "time":         t,
            "velocityJSON": build_velocity_json(hour_grids[h]),
            # Omit raw grid from the static file to keep size reasonable
            # Frontend only needs velocityJSON + time for the slider
        })

    all_speeds = [
        pt["speed"]
        for pts in hour_grids
        for pt in pts
        if pt.get("speed") is not None
    ]
    max_speed = round(max(all_speeds), 1) if all_speeds else 30.0

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source":       "Open-Meteo GFS/HRRR",
        "grid":         {"lats": lats, "lons": lons, "nx": nx, "ny": ny},
        "maxSpeed":     max_speed,
        "hours":        hours,
    }

    # ── Commit to GitHub ──────────────────────────────────────────────────────
    if not GITHUB_TOKEN:
        return res.status(500).json({"error": "GITHUB_TOKEN env var not set"})

    json_bytes   = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    content_b64  = base64.b64encode(json_bytes).decode("utf-8")
    file_size_kb = len(json_bytes) / 1024

    gh_headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    file_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}"

    # Get current SHA so we can overwrite (required by GitHub API for updates)
    sha = None
    try:
        get_resp = requests.get(file_url, headers=gh_headers,
                                params={"ref": GITHUB_BRANCH}, timeout=15)
        if get_resp.status_code == 200:
            sha = get_resp.json().get("sha")
    except Exception:
        pass  # File may not exist yet on first run — that's fine

    commit_body = {
        "message": f"wind data update {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M')}Z",
        "content": content_b64,
        "branch":  GITHUB_BRANCH,
    }
    if sha:
        commit_body["sha"] = sha

    try:
        put_resp = requests.put(file_url, headers=gh_headers,
                                json=commit_body, timeout=30)
        put_resp.raise_for_status()
    except Exception as e:
        return res.status(502).json({
            "error":  "GitHub write failed",
            "detail": str(e),
            "status": getattr(put_resp, "status_code", None),
            "body":   getattr(put_resp, "text", None)[:500] if put_resp else None,
        })

    return res.json({
        "ok":           True,
        "hours":        n_hours,
        "grid_points":  len(grid_lats),
        "max_speed_kt": max_speed,
        "file_size_kb": round(file_size_kb, 1),
        "github_path":  f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{GITHUB_PATH}",
        "generated_at": payload["generated_at"],
    })
