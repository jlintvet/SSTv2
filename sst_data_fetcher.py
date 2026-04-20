"""
Real-Time SST Pipeline (Clean Version)
=====================================

GOES:  last 24 hours (hourly files, preserved)
VIIRS: same-day (gap fill)
MUR:   last 5 days (fallback)

Outputs:
SSTv2/DailySSTData/
  GOES/Hourly/
  GOESComposite/
  VIIRS/
  MUR/
"""

import datetime
import requests
import pandas as pd
import numpy as np
from pathlib import Path
import csv
import io

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
BBOX = {
    "lon_min": -78.89,
    "lon_max": -72.21,
    "lat_min": 33.70,
    "lat_max": 39.00,
}

BASE_DIR = Path("SSTv2") / "DailySSTData"

DIR_GOES_HOURLY = BASE_DIR / "GOES" / "Hourly"
DIR_GOES_COMP   = BASE_DIR / "GOESComposite"
DIR_VIIRS       = BASE_DIR / "VIIRS"
DIR_MUR         = BASE_DIR / "MUR"

for d in [DIR_GOES_HOURLY, DIR_GOES_COMP, DIR_VIIRS, DIR_MUR]:
    d.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def parse_csvp(text, source, date):
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if len(rows) < 3:
        return pd.DataFrame()

    headers = [h.split(" (")[0] for h in rows[0]]
    idx = {h: i for i, h in enumerate(headers)}

    out = []
    for r in rows[2:]:
        try:
            lat = float(r[idx["latitude"]])
            lon = float(r[idx["longitude"]])
            val = float(r[list(idx.keys())[-1]])
            if -3 < val < 40:
                out.append({
                    "lat": lat,
                    "lon": lon,
                    "sst_c": val,
                    "source": source,
                    "date": str(date)
                })
        except:
            continue

    return pd.DataFrame(out)


def write_outputs(df, name, out_dir, label):
    base = out_dir / f"{name}_{label}"
    print(f"→ writing {base}")

    df.to_csv(base.with_suffix(".csv"), index=False)
    df.to_parquet(base.with_suffix(".parquet"))

# ─────────────────────────────────────────────────────────────
# GOES (LAST 24 HOURS)
# ─────────────────────────────────────────────────────────────
def fetch_goes():
    print("\nGOES (last 24 hours)")
    now = datetime.datetime.utcnow()
    results = []

    for i in range(24):
        t = now - datetime.timedelta(hours=i)
        label = t.strftime("%Y%m%d_%H")

        url = (
            "https://cwcgom.aoml.noaa.gov/erddap/griddap/goes19SSThourly.csvp"
            f"?sst[({t.strftime('%Y-%m-%dT%H:00:00Z')})]"
            f"[({BBOX['lat_min']}):1:({BBOX['lat_max']})]"
            f"[({BBOX['lon_min']}):1:({BBOX['lon_max']})]"
        )

        try:
            r = SESSION.get(url, timeout=20)
            df = parse_csvp(r.text, "GOES", t.date())
            if not df.empty:
                print(f"✓ GOES {label} ({len(df)} pts)")
                write_outputs(df, "goes19", DIR_GOES_HOURLY, label)
                results.append((df, label))
        except:
            pass

    return results

# ─────────────────────────────────────────────────────────────
# VIIRS (TODAY ONLY)
# ─────────────────────────────────────────────────────────────
def fetch_viirs():
    print("\nVIIRS (today)")
    today = datetime.date.today()

    url = (
        "https://coastwatch.noaa.gov/erddap/griddap/noaacrwsstDaily.csvp"
        f"?analysed_sst[({today}T12:00:00Z)]"
        f"[({BBOX['lat_min']}):1:({BBOX['lat_max']})]"
        f"[({BBOX['lon_min']}):1:({BBOX['lon_max']})]"
    )

    try:
        r = SESSION.get(url, timeout=20)
        df = parse_csvp(r.text, "VIIRS", today)
        if not df.empty:
            print(f"✓ VIIRS {len(df)} pts")
            write_outputs(df, "viirs", DIR_VIIRS, today.strftime("%Y%m%d"))
            return df
    except:
        pass

    return None

# ─────────────────────────────────────────────────────────────
# MUR (LAST 5 DAYS)
# ─────────────────────────────────────────────────────────────
def fetch_mur():
    print("\nMUR (last 5 days)")
    results = []

    for i in range(5):
        d = datetime.date.today() - datetime.timedelta(days=i)

        url = (
            "https://upwell.pfeg.noaa.gov/erddap/griddap/jplMURSST41.csvp"
            f"?analysed_sst[({d}T09:00:00Z)]"
            f"[({BBOX['lat_min']}):1:({BBOX['lat_max']})]"
            f"[({BBOX['lon_min']}):1:({BBOX['lon_max']})]"
        )

        try:
            r = SESSION.get(url, timeout=30)
            df = parse_csvp(r.text, "MUR", d)
            if not df.empty:
                print(f"✓ MUR {d} ({len(df)} pts)")
                write_outputs(df, "mur", DIR_MUR, d.strftime("%Y%m%d"))
                results.append(df)
        except:
            pass

    return results

# ─────────────────────────────────────────────────────────────
# COMPOSITE (LATEST GOES ONLY)
# ─────────────────────────────────────────────────────────────
def build_composite(goes, viirs, mur):
    if not goes:
        return

    latest_goes, label = goes[0]
    df = latest_goes.copy()

    if viirs is not None:
        df = pd.concat([df, viirs])

    if mur:
        df = pd.concat([df, mur[0]])

    df = df.groupby(["lat", "lon"]).mean(numeric_only=True).reset_index()

    write_outputs(df, "composite", DIR_GOES_COMP, label)
    print("✓ Composite written")

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    goes = fetch_goes()
    viirs = fetch_viirs()
    mur = fetch_mur()

    build_composite(goes, viirs, mur)

if __name__ == "__main__":
    main()
