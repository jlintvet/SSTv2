#!/usr/bin/env python3
"""
Fetch latest observations from NOAA NDBC buoys covering all four RipLoc
regions (mid_atlantic: MD/VA/NC offshore + Chesapeake Bay; va_ri: VA-to-RI
incl. NJ/NY/CT Long Island Sound; ga_sc: GA/SC offshore; ne_fl: NE FL to
Ft Lauderdale) and write a single compact JSON for the map "Weather Buoys"
overlay. One shared file works for every region because the frontend
(SSTHeatmapLeaflet.jsx) already filters to buoys within 75nm of the
selected departure location, regardless of which region is active.

Output: DailySST/Buoys/buoys_latest.json
  { generated_utc, source, buoys: [ {id,name,lat,lon,obs:{...}} ] }

Data sources (both reachable from GitHub Actions, not browser-CORS-friendly):
  - https://www.ndbc.noaa.gov/data/stations/station_table.txt   (coords + names)
  - https://www.ndbc.noaa.gov/data/realtime2/<ID>.txt           (latest obs)
"""
import json, re, datetime
import requests
from pathlib import Path

# Stations from the curated MD/VA/NC + Chesapeake list (mid_atlantic)
STATIONS = ["44014","44099","44089","44064","44062","44063","OCSM2","44072",
            "44080","44061","44042","44056","41025","44086","41063",
            # added (kept only if inside the region bbox below):
            "41159","CLKN7","BFTN7","41013","OCPN7","41110",
            # va_ri (VA to RI incl. NJ/NY/CT/Long Island Sound) — verified against
            # NDBC's live station_table.txt / activestations.xml 2026-07-17:
            "44009","44091","44025","44065","44017","44097","44020","44008","44085",
            # ga_sc (GA/SC offshore) — same verification pass:
            "41004","41008","41024","41029","41033","41003","41112",
            # ne_fl (NE FL to Ft Lauderdale) — same verification pass:
            "41009","41010","41068","41069","41113","41117","41122"]

# Friendly display names (fall back to station_table name if missing)
NAMES = {
    "44014": "Virginia Beach",  "44099": "Cape Henry",     "44089": "Wallops Island",
    "44064": "First Landing",   "44062": "Gooses Reef",    "OCSM2": "Ocean City, MD",
    "44080": "Baltimore Harbor","44061": "Upper Potomac",  "44042": "Lower Potomac",
    "44056": "Duck FRF",        "41025": "Diamond Shoals", "44086": "Nags Head",
    "41063": "Raleigh Bay",
    "44072": "York Spit",
    "41159": "Onslow Bay Outer", "CLKN7": "Cape Lookout", "BFTN7": "Beaufort, NC",
    "41013": "Frying Pan Shoals", "OCPN7": "Ocean Crest Pier", "41110": "Masonboro",
    # va_ri
    "44009": "Delaware Bay",      "44091": "Barnegat, NJ",     "44025": "Long Island, NY",
    "44065": "New York Harbor",   "44017": "Montauk Point, NY","44097": "Block Island, RI",
    "44020": "Nantucket Sound",   "44008": "Nantucket",        "44085": "Buzzards Bay, MA",
    # ga_sc
    "41004": "Edisto, SC",        "41008": "Grays Reef, GA",   "41024": "Sunset Beach, NC",
    "41029": "Capers Nearshore, SC", "41033": "Fripp Nearshore, SC",
    "41003": "Jacksonville Offshore, FL", "41112": "Fernandina Beach, FL",
    # ne_fl
    "41009": "Canaveral, FL",     "41010": "Canaveral East, FL", "41068": "Fort Pierce, FL",
    "41069": "Ponce Inlet, FL",   "41113": "Canaveral Nearshore, FL",
    "41117": "St. Augustine, FL", "41122": "Hollywood Beach, FL",
}

# App regions (matches frontend src/config/regionConfig.js REGION_CONFIGS bounds);
# buoys outside every one of these boxes are dropped. Keep in sync with that file.
REGIONS = [
    {"lat_min": 33.70, "lat_max": 39.00, "lon_min": -78.84, "lon_max": -72.21},  # mid_atlantic
    {"lat_min": 29.80, "lat_max": 35.20, "lon_min": -82.00, "lon_max": -75.20},  # ga_sc
    {"lat_min": 26.00, "lat_max": 30.50, "lon_min": -81.97, "lon_max": -76.14},  # ne_fl
    {"lat_min": 37.26, "lat_max": 41.51, "lon_min": -77.46, "lon_max": -68.97},  # va_ri
]
def in_region(lat, lon):
    if lat is None or lon is None:
        return False
    return any(r["lat_min"] <= lat <= r["lat_max"] and r["lon_min"] <= lon <= r["lon_max"]
               for r in REGIONS)

OUT           = Path("DailySST/Buoys/buoys_latest.json")
STATION_TABLE = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
REALTIME      = "https://www.ndbc.noaa.gov/data/realtime2/{}.txt"
TIMEOUT       = (8, 30)

def _num(s):
    # NDBC realtime2 marks missing values as "MM". Everything else is a real reading
    # (pressure ~950-1050 mb is valid, so no blanket large-value sentinel).
    if s is None: return None
    s = s.strip()
    if s in ("MM", ""): return None
    try:
        return float(s)
    except ValueError:
        return None

def _ms_to_kt(v): return None if v is None else round(v * 1.94384, 1)
def _m_to_ft(v):  return None if v is None else round(v * 3.28084, 1)
def _c_to_f(v):   return None if v is None else round(v * 9/5 + 32, 1)

def parse_latlon(location):
    m = re.search(r"([\d.]+)\s*([NS])\s+([\d.]+)\s*([EW])", location or "")
    if not m: return None
    lat = float(m.group(1)); lat = -lat if m.group(2).upper() == "S" else lat
    lon = float(m.group(3)); lon = -lon if m.group(4).upper() == "W" else lon
    return (round(lat, 5), round(lon, 5))

def load_station_meta(session):
    meta = {}
    try:
        r = session.get(STATION_TABLE, timeout=TIMEOUT); r.raise_for_status()
        for line in r.text.splitlines():
            if line.startswith("#") or "|" not in line: continue
            f = line.split("|")
            if len(f) < 7: continue
            sid = f[0].strip().upper()
            ll = parse_latlon(f[6])
            if ll: meta[sid] = {"lat": ll[0], "lon": ll[1], "name": f[4].strip()}
    except Exception as e:
        print(f"  station_table fetch failed: {e}")
    return meta

def parse_realtime(text):
    """Parse NDBC realtime2 standard-met file; return obs dict from newest row."""
    rows = [l for l in text.splitlines() if l and not l.startswith("#")]
    if not rows: return None
    c = rows[0].split()
    g = lambda i: c[i] if i < len(c) else "MM"
    try:
        obs_time = datetime.datetime(int(g(0)), int(g(1)), int(g(2)),
                                     int(g(3)), int(g(4)),
                                     tzinfo=datetime.timezone.utc).isoformat()
    except Exception:
        obs_time = None
    obs = {
        "time":              obs_time,
        "wind_dir_deg":      _num(g(5)),
        "wind_kt":           _ms_to_kt(_num(g(6))),
        "gust_kt":           _ms_to_kt(_num(g(7))),
        "wave_ft":           _m_to_ft(_num(g(8))),
        "dom_period_s":      _num(g(9)),
        "avg_period_s":      _num(g(10)),
        "mean_wave_dir_deg": _num(g(11)),
        "pressure_mb":       _num(g(12)),
        "air_temp_f":        _c_to_f(_num(g(13))),
        "water_temp_f":      _c_to_f(_num(g(14))),
        "dewpoint_f":        _c_to_f(_num(g(15))),
    }
    # treat an all-empty row as no obs
    return obs if any(v is not None for k, v in obs.items() if k != "time") else (obs if obs_time else None)

OBS_MAX_AGE_H = 6  # discard obs older than this; buoy shows as inactive

def _fresh(obs):
    """Return obs if its timestamp is within OBS_MAX_AGE_H, else None."""
    if obs is None: return None
    t = obs.get("time")
    if not t: return None
    try:
        age = datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(t)
        if age.total_seconds() > OBS_MAX_AGE_H * 3600:
            return None
    except Exception:
        pass
    return obs

def main():
    s = requests.Session()
    meta = load_station_meta(s)
    buoys = []
    for sid in STATIONS:
        key = sid.upper()
        m = meta.get(key, {})
        obs = None
        try:
            r = s.get(REALTIME.format(key), timeout=TIMEOUT); r.raise_for_status()
            obs = _fresh(parse_realtime(r.text))
        except Exception as e:
            print(f"  {sid}: realtime fetch failed: {e}")
        lat, lon = m.get("lat"), m.get("lon")
        if lat is not None and lon is not None and not in_region(lat, lon):
            print(f"  {sid}: outside region bbox ({lat},{lon}) — skipped")
            continue
        buoys.append({
            "id": sid,
            "name": NAMES.get(key) or m.get("name") or sid,
            "lat": lat, "lon": lon,
            "obs": obs,
        })
    out = {
        "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source": "NDBC",
        "buoys": buoys,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, separators=(",", ":")))
    n_obs = sum(1 for b in buoys if b["obs"])
    n_xy  = sum(1 for b in buoys if b["lat"] is not None)
    print(f"wrote {OUT}: {len(buoys)} buoys, {n_xy} with coords, {n_obs} with obs")

if __name__ == "__main__":
    main()
