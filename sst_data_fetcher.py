#!/usr/bin/env python3
"""
=========================================================
SST DATA FETCHER — REAL-TIME PIPELINE (FINAL)
=========================================================
GOES (PRIMARY)
- Last 24 hourly passes
- Each hour saved individually
- Latest hour used for composite
VIIRS (SECONDARY)
- Today only
MUR (FALLBACK)
- Last 5 days
---------------------------------------------------------
OUTPUT STRUCTURE (STRICT)
---------------------------------------------------------
SSTv2/DailySSTData/
    GOES/Hourly/
    GOES/Composite/
    VIIRS/
    MUR/
---------------------------------------------------------
IMPORTANT
---------------------------------------------------------
- This script NEVER writes to SSTv2/DailySST/
- All directories are auto-created
- Safe for GitHub Actions
=========================================================
"""
import os
import json
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
# =========================================================
# CONFIG
# =========================================================
BASE_DIR = "SSTv2/DailySSTData"
DIRS = {
    "goes_hourly": os.path.join(BASE_DIR, "GOES", "Hourly"),
    "goes_composite": os.path.join(BASE_DIR, "GOES", "Composite"),
    "viirs": os.path.join(BASE_DIR, "VIIRS"),
    "mur": os.path.join(BASE_DIR, "MUR"),
}
# =========================================================
# SETUP
# =========================================================
def ensure_dirs():
    for d in DIRS.values():
        os.makedirs(d, exist_ok=True)
# =========================================================
# MOCK DATA (REPLACE WITH REAL FETCH)
# =========================================================
def generate_sst(n):
    return pd.DataFrame({
        "lat": np.random.uniform(20, 45, n),
        "lon": np.random.uniform(-85, -60, n),
        "sst": np.random.uniform(10, 30, n)
    })
# =========================================================
# OUTPUT WRITER
# =========================================================
def write_outputs(df, base_path):
    print(f"→ Writing to {base_path}")
    df.to_csv(base_path + ".csv", index=False)
    df.to_parquet(base_path + ".parquet", index=False)
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    for row in df.itertuples():
        geojson["features"].append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row.lon, row.lat]
            },
            "properties": {
                "sst": row.sst
            }
        })
    with open(base_path + ".geojson", "w") as f:
        json.dump(geojson, f)
    with open(base_path + "_grid.json", "w") as f:
        json.dump({"count": len(df)}, f)
# =========================================================
# GOES
# =========================================================
def fetch_goes():
    print("\nGOES (last 24 hours)")
    now = datetime.utcnow()
    results = []
    for i in range(24):
        ts = now - timedelta(hours=i)
        stamp = ts.strftime("%Y%m%d_%H")
        df = generate_sst(1500)
        print(f"✓ GOES {stamp} ({len(df)} pts)")
        path = os.path.join(DIRS["goes_hourly"], f"goes_{stamp}")
        write_outputs(df, path)
        results.append((ts, df))
    return results
def build_goes_composite(goes_data):
    print("\nGOES composite (latest hour only)")
    latest = sorted(goes_data, key=lambda x: x[0], reverse=True)[0]
    ts, df = latest
    stamp = ts.strftime("%Y%m%d")
    path = os.path.join(DIRS["goes_composite"], f"goes_composite_{stamp}")
    write_outputs(df, path)
# =========================================================
# VIIRS
# =========================================================
def fetch_viirs():
    print("\nVIIRS (today)")
    today = datetime.utcnow().strftime("%Y%m%d")
    df = generate_sst(2000)
    print(f"✓ VIIRS {today} ({len(df)} pts)")
    path = os.path.join(DIRS["viirs"], f"viirs_{today}")
    write_outputs(df, path)
# =========================================================
# MUR
# =========================================================
def fetch_mur():
    print("\nMUR (last 5 days)")
    for i in range(5):
        ts = datetime.utcnow() - timedelta(days=i + 1)
        stamp = ts.strftime("%Y%m%d")
        df = generate_sst(3000)
        print(f"✓ MUR {stamp} ({len(df)} pts)")
        path = os.path.join(DIRS["mur"], f"mur_{stamp}")
        write_outputs(df, path)
# =========================================================
# MAIN
# =========================================================
def main():
    print("Starting SST pipeline...")
    ensure_dirs()
    goes = fetch_goes()
    build_goes_composite(goes)
    fetch_viirs()
    fetch_mur()
    print("\n✓ Pipeline complete")
if __name__ == "__main__":
    main()
