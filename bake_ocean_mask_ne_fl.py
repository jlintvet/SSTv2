#!/usr/bin/env python3
# retrigger: force Ocean_Mask.yml push-path trigger (2026-07-11 — original combined commit with workflow-file edit did not fire it)
"""
Pre-bake the ocean mask for the Northeast Florida region.

Output: DailySSTData/ne_fl/ocean_mask.json
Format: { bounds:{n,s,e,w}, step:0.02, rows:N, cols:M, packed:<base64> }

Run once and commit. Re-run only if region bounds or STEP change.
Requires: pip install requests
"""
import base64
import json
import os

import requests

NORTH, SOUTH = 30.50, 26.00
WEST,  EAST  = -81.75, -77.27
STEP = 0.02

NE_LAND_URL = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_land.geojson"
OUT_PATH = "DailySSTData/ne_fl/ocean_mask.json"


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
    print("Downloading Natural Earth 10m land polygons ...")
    resp = requests.get(NE_LAND_URL, timeout=120)
    resp.raise_for_status()
    gj = resp.json()

    polys = []
    for f in gj.get("features", []):
        g = f.get("geometry", {})
        t = g.get("type")
        if t == "Polygon":
            polys.append(g["coordinates"])
        elif t == "MultiPolygon":
            polys.extend(g["coordinates"])

    kept = []
    for poly in polys:
        ring = poly[0]
        mnl = min(p[0] for p in ring); mxl = max(p[0] for p in ring)
        mnla = min(p[1] for p in ring); mxla = max(p[1] for p in ring)
        if mxl >= WEST and mnl <= EAST and mxla >= SOUTH and mnla <= NORTH:
            kept.append(poly)
    print(f"  {len(polys)} total polygons, {len(kept)} intersect NE FL region")

    rows = int(round((NORTH - SOUTH) / STEP)) + 1
    cols = int(round((EAST  - WEST)  / STEP)) + 1
    print(f"Classifying {rows} x {cols} = {rows*cols} cells ...")

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


if __name__ == "__main__":
    main()
