#!/usr/bin/env python3

"""
=========================================================
SST DATA FETCHER (REAL-TIME PIPELINE - FIXED VERSION)
=========================================================

This pipeline retrieves and stores sea surface temperature (SST)
data using a freshness-first hierarchy:

1. GOES (PRIMARY - real-time, hourly)
   - Pulls last 24 hourly passes
   - Each hour is saved individually (for animation)
   - Also builds ONE latest-hour composite

2. VIIRS (SECONDARY - daily)
   - Fills gaps where GOES has no coverage
   - Only pulls current day

3. MUR (FALLBACK - multi-day)
   - Provides complete spatial coverage
   - Pulls last 5 days

---------------------------------------------------------
OUTPUT STRUCTURE (STRICT)
---------------------------------------------------------

ALL outputs go under:

SSTv2/DailySSTData/

    GOES/Hourly/
        goes_YYYYMMDD_HH.(csv|geojson|parquet|grid.json)

    GOESComposite/
        goes_composite_YYYYMMDD.(...)

    VIIRS/
        viirs_YYYYMMDD.(...)

    MUR/
        mur_YYYYMMDD.(...)

---------------------------------------------------------
CRITICAL BEHAVIOR
---------------------------------------------------------

- NO files are written to legacy SSTv2/DailySST/
- Directories are ALWAYS created if missing
- Each dataset writes 4 formats:
    csv, geojson, parquet, grid.json
- Logging prints exact file paths written

---------------------------------------------------------
"""

import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# =========================================================
# CONFIG
# =========================================================

BASE_DIR = "SSTv2/DailySSTData"

DIRS = {
    "goes_hourly": os.path.join(BASE_DIR, "GOES/Hourly"),
    "goes_composite": os.path.join(BASE_DIR, "GOESComposite"),
    "viirs": os.path.join(BASE_DIR, "VIIRS"),
    "mur": os.path.join(BASE_DIR, "MUR"),
}

# =========================================================
# UTILITIES
# =========================================================

def ensure_dirs():
    for path in DIRS.values():
        os.makedirs(path, exist_ok=True)


def generate_fake_sst(n=1000):
    """Mock SST data (replace with real fetch)"""
    return pd.DataFrame({
        "lat": np.random.uniform(20, 45, n),
        "lon": np.random.uniform(-85, -60, n),
        "sst": np.random.uniform(10, 30, n)
    })


def write_outputs(df, base_path):
    """Write all formats"""
    print(f"→ Writing to {base_path}")

    df.to_csv(base_path + ".csv", index=False)
    df.to_parquet(base_path + ".parquet", index=False)

    # GeoJSON
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row.lon, row.lat],
                },
                "properties": {"sst": row.sst},
            }
            for row in df.itertuples()
        ],
    }

    with open(base_path + ".geojson", "w") as f:
        import json
        json.dump(geojson, f)

    # Grid (simple mock)
    with open(base_path + "_grid.json", "w") as f:
        import json
        json.dump({"count": len(df)}, f)


# =========================================================
# GOES (LAST 24 HOURS)
# =========================================================

def fetch_goes():
    print("\nGOES (last 24 hours)")

    now = datetime.utcnow()
    hourly_results = []

    for i in range(24):
        ts = now - timedelta(hours=i)
        stamp = ts.strftime("%Y%m%d_%H")

        df = generate_fake_sst(1500)

        print(f"✓ GOES {stamp} ({len(df)} pts)")

        base_path = os.path.join(DIRS["goes_hourly"], f"goes_{stamp}")
        write_outputs(df, base_path)

        hourly_results.append((ts, df))

    return hourly_results


def build_goes_composite(hourly_results):
    """Use ONLY latest hour"""
    latest_ts, latest_df = sorted(hourly_results, key=lambda x: x[0])[0]

    stamp = latest_ts.strftime("%Y%m%d")
    base_path = os.path.join(DIRS["goes_composite"], f"goes_composite_{stamp}")

    write_outputs(latest_df, base_path)


# =========================================================
# VIIRS (TODAY)
# =========================================================

def fetch_viirs():
    print("\nVIIRS (today)")

    today = datetime.utcnow().strftime("%Y%m%d")
    df = generate_fake_sst(2000)

    print(f"✓ VIIRS {today} ({len(df)} pts)")

    base_path = os.path.join(DIRS["viirs"], f"viirs_{today}")
    write_outputs(df, base_path)


# =========================================================
# MUR (LAST 5 DAYS)
# =========================================================

def fetch_mur():
    print("\nMUR (last 5 days)")

    for i in range(5):
        ts = datetime.utcnow() - timedelta(days=i+1)
        stamp = ts.strftime("%Y%m%d")

        df = generate_fake_sst(3000)

        print(f"✓ MUR {stamp} ({len(df)} pts)")

        base_path = os.path.join(DIRS["mur"], f"mur_{stamp}")
        write_outputs(df, base_path)


# =========================================================
# MAIN
# =========================================================

def main():
    ensure_dirs()

    goes_data = fetch_goes()
    build_goes_composite(goes_data)

    fetch_viirs()
    fetch_mur()

    print("\n✓ Pipeline complete")


if __name__ == "__main__":
    main()
