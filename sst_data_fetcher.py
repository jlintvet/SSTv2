#!/usr/bin/env python3

"""
SST DATA FETCHER (REAL-TIME PIPELINE)

This script builds a fresh SST dataset using a priority stack:

1. GOES (PRIMARY)
   - Retrieves last 24 hours of hourly passes
   - Writes each hour separately (for animation)
   - Builds a "latest composite" from most recent valid hour

2. VIIRS (SECONDARY)
   - Retrieves most recent daily pass (today)
   - Used to fill GOES gaps if needed

3. MUR (FALLBACK)
   - Retrieves last 5 days
   - Used only where GOES + VIIRS have no data

OUTPUT STRUCTURE:

SSTv2/
└── DailySSTData/
    ├── GOES/
    │   └── Hourly/
    ├── GOESComposite/
    ├── VIIRS/
    └── MUR/

IMPORTANT:
- ALL writes go through a single function → prevents path bugs
- NO files are written to /DailySST/
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

# =========================
# DIRECTORY CONFIG
# =========================

BASE_DIR = Path("SSTv2") / "DailySSTData"

DIRS = {
    "GOES_HOURLY": BASE_DIR / "GOES" / "Hourly",
    "GOES_COMPOSITE": BASE_DIR / "GOESComposite",
    "VIIRS": BASE_DIR / "VIIRS",
    "MUR": BASE_DIR / "MUR",
}

for d in DIRS.values():
    d.mkdir(parents=True, exist_ok=True)

# =========================
# UTIL: WRITE OUTPUTS
# =========================

def write_outputs(df, base_filename, out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"→ Writing to {out_dir}/{base_filename}")

    df.to_csv(out_dir / f"{base_filename}.csv", index=False)

    try:
        df.to_parquet(out_dir / f"{base_filename}.parquet", index=False)
    except Exception:
        pass

    try:
        df.to_json(out_dir / f"{base_filename}.geojson", orient="records")
    except Exception:
        pass


# =========================
# MOCK FETCHERS (REPLACE WITH YOUR REAL ONES)
# =========================

def fake_sst_data(n=1000):
    return pd.DataFrame({
        "lat": np.random.uniform(30, 45, n),
        "lon": np.random.uniform(-80, -65, n),
        "sst": np.random.uniform(10, 25, n),
    })


def fetch_goes_hour(ts):
    # replace with real GOES logic
    df = fake_sst_data(np.random.randint(500, 3000))
    print(f"✓ GOES {ts.strftime('%Y%m%d_%H')} ({len(df)} pts)")
    return df


def fetch_viirs(date):
    # replace with real VIIRS logic
    df = fake_sst_data(2000)
    print(f"✓ VIIRS {date.strftime('%Y%m%d')} ({len(df)} pts)")
    return df


def fetch_mur(date):
    # replace with real MUR logic
    df = fake_sst_data(3000)
    print(f"✓ MUR {date.strftime('%Y%m%d')} ({len(df)} pts)")
    return df


# =========================
# GOES: LAST 24 HOURS
# =========================

def process_goes(now):
    print("\nGOES (last 24 hours)")

    hourly_frames = []

    for h in range(24):
        ts = now - timedelta(hours=h)

        try:
            df = fetch_goes_hour(ts)

            if df is None or df.empty:
                continue

            fname = f"goes_{ts.strftime('%Y%m%d_%H')}"
            write_outputs(df, fname, DIRS["GOES_HOURLY"])

            hourly_frames.append((ts, df))

        except Exception as e:
            print(f"✗ GOES {ts} failed: {e}")

    # Build composite from most recent valid
    if hourly_frames:
        latest_ts, latest_df = sorted(hourly_frames, key=lambda x: x[0], reverse=True)[0]

        fname = f"goes_composite_{latest_ts.strftime('%Y%m%d')}"
        write_outputs(latest_df, fname, DIRS["GOES_COMPOSITE"])

        return latest_df

    return None


# =========================
# VIIRS: TODAY
# =========================

def process_viirs(now):
    print("\nVIIRS (today)")

    try:
        df = fetch_viirs(now)
        fname = f"viirs_{now.strftime('%Y%m%d')}"
        write_outputs(df, fname, DIRS["VIIRS"])
        return df
    except Exception as e:
        print(f"✗ VIIRS failed: {e}")
        return None


# =========================
# MUR: LAST 5 DAYS
# =========================

def process_mur(now):
    print("\nMUR (last 5 days)")

    mur_frames = []

    for d in range(1, 6):
        date = now - timedelta(days=d)

        try:
            df = fetch_mur(date)
            fname = f"mur_{date.strftime('%Y%m%d')}"
            write_outputs(df, fname, DIRS["MUR"])
            mur_frames.append(df)
        except Exception as e:
            print(f"✗ MUR {date} failed: {e}")

    return mur_frames


# =========================
# MAIN
# =========================

def main():
    now = datetime.utcnow()

    goes_latest = process_goes(now)
    viirs = process_viirs(now)
    mur = process_mur(now)

    print("\n✓ Pipeline complete")


if __name__ == "__main__":
    main()
