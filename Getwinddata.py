import requests
import math
from datetime import datetime, timezone

def handler(req, res):
    """
    Base44 backend function: getWindData
    Fetches 48h of hourly 10m wind U/V components from Open-Meteo GFS+HRRR
    for a grid of points covering the OBX region.
    Returns data shaped for leaflet-velocity + time-indexed structure
    for the frontend timeline scrubber.
    """

    # ── Grid definition ──────────────────────────────────────────────────────
    # OBX region: 33.5–37N, 74–77W at 0.25° spacing → ~96 grid points
    LAT_MIN, LAT_MAX, LAT_STEP = 33.5, 37.0, 0.25
    LON_MIN, LON_MAX, LON_STEP = -77.0, -74.0, 0.25

    lats = []
    lat = LAT_MAX
    while lat >= LAT_MIN - 0.001:
        lats.append(round(lat, 2))
        lat -= LAT_STEP

    lons = []
    lon = LON_MIN
    while lon <= LON_MAX + 0.001:
        lons.append(round(lon, 2))
        lon += LON_STEP

    # Build flat list of all grid point pairs
    grid_lats, grid_lons = [], []
    for la in lats:
        for lo in lons:
            grid_lats.append(la)
            grid_lons.append(lo)

    # ── Fetch from Open-Meteo GFS/HRRR ──────────────────────────────────────
    # wind_u_component_10m = U (east/west, positive = eastward)
    # wind_v_component_10m = V (north/south, positive = northward)
    # forecast_days=2 → 48 hours; models=gfs_seamless = GFS+HRRR blend
    params = {
        "latitude":        ",".join(str(x) for x in grid_lats),
        "longitude":       ",".join(str(x) for x in grid_lons),
        "hourly":          "wind_u_component_10m,wind_v_component_10m,wind_speed_10m",
        "wind_speed_unit": "kn",          # knots — marine standard
        "forecast_days":   "2",
        "timezone":        "UTC",
        "cell_selection":  "nearest",
        "models":          "gfs_seamless",  # GFS + HRRR blend, best for US coastal
    }

    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params=params,
            timeout=30
        )
        resp.raise_for_status()
        raw = resp.json()
    except Exception as e:
        return res.status(502).json({"error": "Open-Meteo fetch failed", "detail": str(e)})

    # Open-Meteo returns a list when multiple lat/lon pairs are passed
    if isinstance(raw, dict):
        raw = [raw]

    if not raw:
        return res.status(502).json({"error": "Empty response from Open-Meteo"})

    # ── Parse into per-hour grid ─────────────────────────────────────────────
    times = raw[0].get("hourly", {}).get("time", [])   # ["2026-04-29T00:00", ...]
    n_hours = len(times)                                 # should be 48

    # hour_grids[h] = list of {lat, lon, u, v, speed} for every grid point
    hour_grids = [[] for _ in range(n_hours)]

    for idx, point_resp in enumerate(raw):
        la  = grid_lats[idx]
        lo  = grid_lons[idx]
        u_arr  = point_resp.get("hourly", {}).get("wind_u_component_10m", [])
        v_arr  = point_resp.get("hourly", {}).get("wind_v_component_10m", [])
        sp_arr = point_resp.get("hourly", {}).get("wind_speed_10m", [])

        for h in range(n_hours):
            u  = u_arr[h]  if h < len(u_arr)  else None
            v  = v_arr[h]  if h < len(v_arr)  else None
            sp = sp_arr[h] if h < len(sp_arr) else None
            if u is not None and v is not None:
                hour_grids[h].append({"lat": la, "lon": lo, "u": u, "v": v, "speed": sp})

    # ── Build leaflet-velocity JSON for each hour ────────────────────────────
    # leaflet-velocity expects a two-element list:
    # [
    #   { "header": { parameterNumberName, la1, la2, lo1, lo2, dx, dy, nx, ny, ... },
    #     "data": [u0, u1, ...] },          # row-major, north→south, west→east
    #   { "header": { ... }, "data": [v0, v1, ...] }
    # ]
    nx = len(lons)
    ny = len(lats)

    base_header = {
        "parameterUnit":     "knots",
        "parameterCategory": 2,
        "surface":           103,
        "surfaceValues":     10,
        "lo1": lons[0],
        "lo2": lons[-1],
        "la1": lats[0],   # northernmost lat
        "la2": lats[-1],  # southernmost lat
        "dx":  LON_STEP,
        "dy":  LAT_STEP,
        "nx":  nx,
        "ny":  ny,
        "refTime": datetime.now(timezone.utc).isoformat(),
    }

    def build_velocity_json(pts):
        u_data = [0.0] * (ny * nx)
        v_data = [0.0] * (ny * nx)

        lat_index = {la: i for i, la in enumerate(lats)}
        lon_index = {lo: i for i, lo in enumerate(lons)}

        for pt in pts:
            ri = lat_index.get(pt["lat"])
            ci = lon_index.get(pt["lon"])
            if ri is None or ci is None:
                continue
            idx2 = ri * nx + ci
            u_data[idx2] = pt["u"] or 0.0
            v_data[idx2] = pt["v"] or 0.0

        return [
            {
                "header": {**base_header, "parameterNumberName": "eastward_wind",  "parameterNumber": 2},
                "data": u_data,
            },
            {
                "header": {**base_header, "parameterNumberName": "northward_wind", "parameterNumber": 3},
                "data": v_data,
            },
        ]

    # Build velocity JSON for every hour
    hours = []
    for h, t in enumerate(times):
        hours.append({
            "time":         t,
            "velocityJSON": build_velocity_json(hour_grids[h]),
            "grid":         hour_grids[h],   # raw points for hover tooltips
        })

    # ── Wind speed stats for color scale ─────────────────────────────────────
    all_speeds = [
        pt["speed"]
        for pts in hour_grids
        for pt in pts
        if pt.get("speed") is not None
    ]
    max_speed = max(all_speeds) if all_speeds else 30.0

    return res.json({
        "hours":    hours,
        "lats":     lats,
        "lons":     lons,
        "maxSpeed": max_speed,
        "source":   "Open-Meteo GFS/HRRR",
    })
