#!/usr/bin/env python3
"""
Pre-bake the ocean mask for the app's region.

Frontend used to fetch 25MB of Natural Earth 1:10m GeoJSON on every page
load and run ~500ms of point-in-polygon classification in the browser.
This script does it once and writes a compact JSON the frontend loads
in <50ms.

Output: DailySSTData/ocean_mask.json
Format: { bounds:{n,s,e,w}, step:0.02, rows:N, cols:M, packed:<base64> }
   packed = 1 bit per cell, row-major (north→south, west→east),
            big-endian within each byte, base64-encoded.

Run once and commit. Re-run only if:
  - Region bounds (NORTH/SOUTH/EAST/WEST) change
  - You want finer or coarser resolution than 0.02°

Requires: pip install requests
"""
import base64
import json
import os
import sys

import requests

# Must match ingest + frontend bounds.
NORTH, SOUTH = 39.00, 33.70
WEST,  EAST  = -78.89, -72.21
STEP = 0.02  # ~2 km cells, finer than MUR's 0.05° stride

NE_LAND_URL = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_land.geojson"
OUT_PATH = "DailySSTData/ocean_mask.json"


def point_in_ring(px, py, ring):
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > py) != (yj > py)) and (px < (xj - xi) * (py - yi) / (yj - yi + 1e-12) + xi):
            inside = not inside
        j = i
    return inside


def is_land(lat, lon, polys):
    for poly in polys:
        if point_in_ring(lon, lat, poly[0]):
            in_hole = False
            for h in range(1, len(poly)):
                if point_in_ring(lon, lat, poly[h]):
                    in_hole = True
                    break
            if not in_hole:
                return True
    return False


def main():
    print(f"Fetching {NE_LAND_URL} ...")
    r = requests.get(NE_LAND_URL, timeout=120)
    r.raise_for_status()
    gj = r.json()
    print(f"  {len(r.content)//1024} KB downloaded")

    polys = []
    for f in gj.get("features", []):
        g = f.get("geometry", {})
        t = g.get("type")
        if t == "Polygon":
            polys.append(g["coordinates"])
        elif t == "MultiPolygon":
            polys.extend(g["coordinates"])

    # Trim to BB — speeds up classification by 50-100×.
    kept = []
    for poly in polys:
        ring = poly[0]
        mnl = min(p[0] for p in ring); mxl = max(p[0] for p in ring)
        mnla = min(p[1] for p in ring); mxla = max(p[1] for p in ring)
        if mxl >= WEST and mnl <= EAST and mxla >= SOUTH and mnla <= NORTH:
            kept.append(poly)
    print(f"  {len(polys)} total polygons, {len(kept)} intersect region")

    # Grid dims — rows run north→south (top-down, same as canvas convention).
    rows = int(round((NORTH - SOUTH) / STEP)) + 1
    cols = int(round((EAST  - WEST)  / STEP)) + 1
    print(f"Classifying {rows} × {cols} = {rows*cols} cells ...")

    # Pack 1 bit per cell, row-major. 1 = ocean, 0 = land.
    bits = bytearray((rows * cols + 7) // 8)
    land_count = 0
    ocean_count = 0
    for ri in range(rows):
        lat = NORTH - ri * STEP
        for ci in range(cols):
            lon = WEST + ci * STEP
            land = is_land(lat, lon, kept)
            if land:
                land_count += 1
            else:
                ocean_count += 1
                idx = ri * cols + ci
                bits[idx >> 3] |= (0x80 >> (idx & 7))
        if ri % 20 == 0:
            print(f"  row {ri}/{rows}")

    print(f"  {land_count} land, {ocean_count} ocean")

    out = {
        "bounds": {"n": NORTH, "s": SOUTH, "e": EAST, "w": WEST},
        "step": STEP,
        "rows": rows,
        "cols": cols,
        "packed": base64.b64encode(bits).decode("ascii"),
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(out, f)

    size_kb = os.path.getsize(OUT_PATH) // 1024
    print(f"Wrote {OUT_PATH} ({size_kb} KB)")
    print("Commit this file. Re-run only when bounds or STEP change.")


if __name__ == "__main__":
    main()
