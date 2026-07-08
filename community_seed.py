#!/usr/bin/env python3
"""
community_seed.py — temporary beta seeding for the riploc community layer.

See community-seed-content-spec.md (in SSTProductionRepo) for the full spec.

Creates/maintains 100 fictitious users and a light trickle of community pins +
tips so the map and leaderboard look active during beta. Every artifact is
traceable and fully reversible: deleting the seed auth users cascades away all
their pins/tips/points/profiles (ON DELETE CASCADE). The `seed_users` registry
table is the authoritative list.

Subcommands:
  status     Print counts (no writes).
  create     One-time: create the 100 users + profiles + registry + a light
             back-fill of recent report pins.  Requires SEED_CONFIRM_CREATE=true.
  tick       Recurring: post 1-3 fresh pins + a few tips, behind the kill switch.
             (The GitHub Action runs this ~3x/day.)
  teardown   Delete ALL seed users (cascades away everything).  Requires
             SEED_CONFIRM_TEARDOWN=true.

Auth/DB: uses the Supabase service_role key (bypasses RLS).  NEVER commit the
key — it is read from env (SUPABASE_SERVICE_ROLE_KEY), supplied as a CI secret.
"""
import os
import sys
import json
import time
import random
import math
import secrets
import logging
import datetime as dt
import urllib.request
import urllib.error

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S")
log = logging.getLogger("community_seed")

# ── Config ──────────────────────────────────────────────────────────────────
def _clean_base(u):
    """Accept the bare project URL (https://<ref>.supabase.co) even if the
    /rest/v1 or /auth/v1 suffix or trailing slash/whitespace was pasted in."""
    u = (u or "").strip().rstrip("/")
    for suffix in ("/rest/v1", "/auth/v1"):
        if u.endswith(suffix):
            u = u[: -len(suffix)]
    return u.rstrip("/")

SUPABASE_URL = _clean_base(os.environ.get("SUPABASE_URL", ""))
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

SEED_DOMAIN   = "seed.riploc.invalid"          # non-deliverable, reserved
NUM_USERS     = int(os.environ.get("SEED_NUM_USERS", "100"))
BATCH         = os.environ.get("SEED_BATCH", dt.date.today().isoformat())
PINS_PER_RUN  = (int(os.environ.get("SEED_PINS_PER_RUN_MIN", "2")),
                 int(os.environ.get("SEED_PINS_PER_RUN_MAX", "3")))   # ~3 runs/day => 5-10/day
LIVE_FRACTION = float(os.environ.get("SEED_LIVE_FRACTION", "0.30"))
TIP_FRACTION  = float(os.environ.get("SEED_TIP_FRACTION", "0.25"))
BACKFILL_DAYS = int(os.environ.get("SEED_BACKFILL_DAYS", "5"))
BACKFILL_PER_DAY = (3, 6)

LIVE_POINTS, REPORT_POINTS = 5000, 1000        # match the app's economy
LIVE_TTL_H,  REPORT_TTL_D  = 24, 7

# ── Content pools ───────────────────────────────────────────────────────────
# Offshore Mid-Atlantic structure — canyons / shelf-edge lumps, all deep open
# water (never land/sound/bay). Jittered slightly per pin.
SPOTS = [
    ("Norfolk Canyon", 37.05, -74.74), ("Washington Canyon", 37.42, -74.52),
    ("Poor Man's Canyon", 37.62, -74.40), ("Baltimore Canyon", 38.18, -73.86),
    ("Wilmington Canyon", 38.44, -73.64), ("Spencer Canyon", 38.72, -73.42),
    ("Lindenkohl Canyon", 38.60, -73.55), ("The Hot Dog", 38.02, -74.62),
    ("The Jackspot", 38.10, -74.78), ("Massey's Canyon", 37.84, -74.62),
    ("Tea's Canyon", 37.30, -74.65), ("The Rockpile", 37.20, -74.90),
    ("The Fingers", 38.30, -73.95), ("The Hambone", 35.62, -74.86),
    ("26 Mile Hill", 35.74, -75.05), ("The Point (offshore)", 35.15, -75.10),
]
NAME_A = ["Reel", "Salt", "Tight", "Wahoo", "Mahi", "Canyon", "Offshore", "Blue",
          "Gaff", "Tuna", "Knot", "Bluewater", "Outcast", "Pelagic", "Transom",
          "Chum", "Ballyhoo", "Spreader", "Rigged", "Sportfish"]
NAME_B = ["Runner", "Chaser", "Hunter", "Slayer", "Capt", "Time", "Life", "Magnet",
          "Whisperer", "Reels", "Charters", "Dreams", "_OBX", "_VB", "_OC", "Knots"]
SPECIES = ["yellowfin", "mahi", "wahoo", "blue_marlin"]   # app keys (blue_marlin = "Marlin")
NOTES = [
    "Weed line at the 30 fathom, marks stacked.", "Pulled a couple gaffers off a temp break.",
    "Slow pick, water was green inshore of the edge.", "Bird play over the lumps at first light.",
    "Found 74 degree water and they were chewing.", "Trolled ballyhoo, best bite mid-morning.",
    "Bailing dolphin under a weed paddy.", "Marked bait deep on the break, few knockdowns.",
    "Clean blue water past the canyon, scattered.", "Short bite window but quality fish.",
    "Tight to the structure, jigging worked.", "Spreader bar got crushed on the troll.",
    "Cooler water moved in, had to run further.", "Steady pick all day, nothing huge.",
    "Solid color change set up right on the edge.",
]


def _req(method, path, body=None, headers=None, base="rest"):
    if not SUPABASE_URL or not SERVICE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set")
    root = f"{SUPABASE_URL}/{'auth/v1' if base == 'auth' else 'rest/v1'}"
    url = f"{root}/{path}"
    h = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}",
         "Content-Type": "application/json"}
    if headers:
        h.update(headers)
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {path} -> {e.code}: {e.read().decode()[:300]}")


def rest_get(table, query=""):
    return _req("GET", f"{table}?{query}") or []

def rest_insert(table, rows, prefer="return=representation"):
    return _req("POST", table, rows, {"Prefer": prefer})

def rest_update(table, query, patch):
    return _req("PATCH", f"{table}?{query}", patch, {"Prefer": "return=minimal"})

def admin_create_user(email, password, meta):
    return _req("POST", "admin/users",
                {"email": email, "password": password, "email_confirm": True,
                 "app_metadata": meta}, base="auth")

def admin_delete_user(uid):
    return _req("DELETE", f"admin/users/{uid}", base="auth")


# ── Kill switch ─────────────────────────────────────────────────────────────
def seed_enabled():
    try:
        rows = rest_get("seed_config", "id=eq.1&select=enabled,end_date")
    except RuntimeError as e:
        log.warning("seed_config unreadable (%s) — run community-seed-schema.sql first", e)
        return False
    if not rows:
        log.warning("no seed_config row — seeding disabled")
        return False
    cfg = rows[0]
    if not cfg.get("enabled"):
        log.info("kill switch: seed_config.enabled = false — exiting")
        return False
    if cfg.get("end_date") and cfg["end_date"] < dt.date.today().isoformat():
        log.info("past end_date %s — exiting", cfg["end_date"])
        return False
    return True


# ── Helpers ─────────────────────────────────────────────────────────────────
def gen_names(n):
    out, seen = [], set()
    i = 0
    while len(out) < n:
        nm = random.choice(NAME_A) + random.choice(NAME_B)
        if random.random() < 0.4:
            nm += str(random.randint(2, 99))
        if nm not in seen:
            seen.add(nm); out.append(nm)
        i += 1
        if i > n * 50:
            nm = f"Angler{len(out)}"; out.append(nm)
    return out

def active_seed_users():
    return rest_get("seed_users", "active=eq.true&select=user_id,display_name")

def list_seed_auth():
    """Map email -> id for existing auth users on the seed domain. Lets create
    recover from a partial run and lets teardown catch orphans not in the registry."""
    out, page = {}, 1
    while True:
        try:
            data = _req("GET", f"admin/users?page={page}&per_page=1000", base="auth")
        except RuntimeError as e:
            log.warning("admin list users failed: %s", e); break
        users = data.get("users", []) if isinstance(data, dict) else (data or [])
        if not users:
            break
        for u in users:
            em = u.get("email") or ""
            if em.endswith("@" + SEED_DOMAIN):
                out[em] = u["id"]
        if len(users) < 1000:
            break
        page += 1
    return out

def load_zones():
    """Active admin-drawn zones (seed_zones). Empty -> fall back to built-in SPOTS."""
    try:
        return rest_get("seed_zones",
                        "active=eq.true&select=name,center_lat,center_lon,radius_nm,species,weight")
    except RuntimeError as e:
        log.warning("seed_zones unreadable (%s) -- using built-in spots", e)
        return []

def _pick_zone(zones):
    w = [max(1, int(z.get("weight") or 1)) for z in zones]
    return random.choices(zones, weights=w, k=1)[0]

def _rand_point_in_circle(lat, lon, radius_nm):
    # uniform point within the circle the admin drew (so it stays in their water)
    r  = float(radius_nm or 8) * 1852.0 * math.sqrt(random.random())   # metres
    th = random.random() * 2 * math.pi
    dlat = (r * math.cos(th)) / 111320.0
    dlon = (r * math.sin(th)) / (111320.0 * math.cos(math.radians(lat)))
    return lat + dlat, lon + dlon

def make_pin(user, kind, created_at=None, zones=None):
    if zones:
        z = _pick_zone(zones)
        lat, lon = _rand_point_in_circle(z["center_lat"], z["center_lon"], z.get("radius_nm"))
        pool = z.get("species") or SPECIES
    else:
        _, blat, blon = random.choice(SPOTS)
        lat = blat + random.uniform(-0.04, 0.04)
        lon = blon + random.uniform(-0.04, 0.04)
        pool = SPECIES
    lat = round(lat, 5); lon = round(lon, 5)
    spp = random.sample(pool, k=min(len(pool), random.randint(1, 2)))
    qty = {s: random.choice([0, 1, 1, 2, 2, 3, 4, 5, 6]) for s in spp}
    now = dt.datetime.now(dt.timezone.utc)
    base = created_at or now
    ttl = dt.timedelta(hours=LIVE_TTL_H) if kind == "live" else dt.timedelta(days=REPORT_TTL_D)
    row = {
        "user_id": user["user_id"], "display_name": user["display_name"],
        "type": kind, "lat": lat, "lon": lon,
        "species": spp, "quantity": qty,
        "water_temp": round(random.uniform(74, 82), 1),   # TODO: sample VIIRS composite
        "notes": random.choice(NOTES) if random.random() < 0.5 else None,
        "venmo_handle": None, "cashapp_handle": None,      # seed pins are never tippable
        "points_awarded": LIVE_POINTS if kind == "live" else REPORT_POINTS,
        "expires_at": (base + ttl).isoformat(),
        "is_flagged": False,
    }
    if created_at:
        row["created_at"] = created_at.isoformat()
    return row

def bump_points(uid, kind):
    cur = rest_get("user_points", f"user_id=eq.{uid}&select=total_points,report_count,live_count")
    pts = LIVE_POINTS if kind == "live" else REPORT_POINTS
    if cur:
        c = cur[0]
        rest_update("user_points", f"user_id=eq.{uid}", {
            "total_points": (c.get("total_points") or 0) + pts,
            "report_count": (c.get("report_count") or 0) + (0 if kind == "live" else 1),
            "live_count": (c.get("live_count") or 0) + (1 if kind == "live" else 0),
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        })
    else:
        rest_insert("user_points", {
            "user_id": uid, "total_points": pts,
            "report_count": 0 if kind == "live" else 1,
            "live_count": 1 if kind == "live" else 0,
        }, prefer="return=minimal")

def maybe_tip(pin, users):
    """Seed a tip from a different seed user to the pin owner."""
    others = [u for u in users if u["user_id"] != pin["user_id"]]
    if not others:
        return
    tipper = random.choice(others)
    # $10 increments, $10-$100, weighted to the low end (no tip over $100).
    cents = random.choice([1000, 1000, 1000, 1000, 2000, 2000, 2000, 3000, 3000,
                           4000, 5000, 5000, 6000, 7000, 8000, 10000])
    platform = random.choice(["venmo", "cashapp"])
    try:
        rest_insert("community_tips", {
            "location_id": pin["id"], "tipper_user_id": tipper["user_id"],
            "recipient_user_id": pin["user_id"], "amount_cents": cents, "platform": platform,
        }, prefer="return=minimal")
        rest_update("community_locations", f"id=eq.{pin['id']}", {
            "tip_count": (pin.get("tip_count") or 0) + 1,
            "tip_total_cents": (pin.get("tip_total_cents") or 0) + cents,
        })
        cur = rest_get("user_points", f"user_id=eq.{pin['user_id']}&select=tips_received_cents")
        if cur:
            rest_update("user_points", f"user_id=eq.{pin['user_id']}",
                        {"tips_received_cents": (cur[0].get("tips_received_cents") or 0) + cents})
    except RuntimeError as e:
        log.warning("tip skipped: %s", e)


# ── Commands ────────────────────────────────────────────────────────────────
def cmd_status():
    su = rest_get("seed_users", "select=user_id")
    cl = rest_get("community_locations", "select=id&limit=1")
    log.info("seed_users: %d | seed_config enabled: %s", len(su), seed_enabled())

def cmd_create():
    if os.environ.get("SEED_CONFIRM_CREATE") != "true":
        log.error("refusing to create — set SEED_CONFIRM_CREATE=true"); sys.exit(2)
    registered    = {r["email"] for r in rest_get("seed_users", "select=email")}
    existing_auth = list_seed_auth()   # reuse auth users from any prior partial run
    log.info("create: %d registered, %d seed auth users already exist",
             len(registered), len(existing_auth))
    names = gen_names(NUM_USERS)
    created = []
    for i in range(1, NUM_USERS + 1):
        email = f"seed-{i:03d}@{SEED_DOMAIN}"
        if email in registered:
            continue
        dn  = names[i - 1]
        uid = existing_auth.get(email)
        try:
            if not uid:
                u = admin_create_user(email, secrets.token_urlsafe(18),
                                      {"is_seed": True, "seed_batch": BATCH})
                uid = u["id"]
            # Profile is best-effort: a signup trigger usually creates it, and ALL
            # community content uses the denormalized display_name on the pins, so a
            # seed user does not require a profile row. Ignore NOT-NULL/other failures.
            try:
                rest_insert("user_profiles", {"id": uid, "display_name": dn},
                            prefer="resolution=merge-duplicates,return=minimal")
            except RuntimeError:
                pass
            rest_insert("seed_users",
                        {"user_id": uid, "email": email, "display_name": dn,
                         "batch": BATCH, "active": True}, prefer="return=minimal")
            created.append({"user_id": uid, "display_name": dn})
        except RuntimeError as e:
            log.warning("register %s failed: %s", email, e)
        time.sleep(0.03)
    log.info("registered %d seed users", len(created))

    # Light back-fill so the map/leaderboard aren't empty at launch.
    users = active_seed_users()
    zones = load_zones()
    if users and BACKFILL_DAYS > 0:
        n = 0
        for d in range(1, BACKFILL_DAYS + 1):
            for _ in range(random.randint(*BACKFILL_PER_DAY)):
                when = dt.datetime.now(dt.timezone.utc) - dt.timedelta(
                    days=d, hours=random.randint(0, 12), minutes=random.randint(0, 59))
                u = random.choice(users)
                pin = rest_insert("community_locations", make_pin(u, "report", created_at=when, zones=zones))[0]
                bump_points(u["user_id"], "report")
                if random.random() < TIP_FRACTION:
                    maybe_tip(pin, users)
                n += 1
        log.info("back-filled %d recent report pins", n)

def cmd_tick():
    if not seed_enabled():
        return
    users = active_seed_users()
    if not users:
        log.warning("no active seed users — run create first"); return
    zones = load_zones()
    n = random.randint(*PINS_PER_RUN)
    posted = 0
    for _ in range(n):
        u = random.choice(users)
        kind = "live" if random.random() < LIVE_FRACTION else "report"
        try:
            pin = rest_insert("community_locations", make_pin(u, kind, zones=zones))[0]
            bump_points(u["user_id"], kind)
            if kind == "report" and random.random() < TIP_FRACTION:
                maybe_tip(pin, users)
            posted += 1
        except RuntimeError as e:
            log.warning("pin skipped: %s", e)
    log.info("tick: posted %d pin(s)", posted)

def cmd_teardown():
    if os.environ.get("SEED_CONFIRM_TEARDOWN") != "true":
        log.error("refusing to teardown — set SEED_CONFIRM_TEARDOWN=true"); sys.exit(2)
    ids = {u["user_id"] for u in rest_get("seed_users", "select=user_id")}
    ids |= set(list_seed_auth().values())   # safety net: orphans not in the registry
    log.info("teardown: deleting %d seed users (cascades all their content)", len(ids))
    deleted = 0
    for uid in ids:
        try:
            admin_delete_user(uid); deleted += 1
        except RuntimeError as e:
            log.warning("delete %s failed: %s", uid, e)
        time.sleep(0.03)
    log.info("deleted %d seed users. (Registry rows cascade-removed.)", deleted)
    log.info("Reminder: set seed_config.enabled=false and disable the workflow.")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    log.info("Supabase REST base: %s/rest/v1", SUPABASE_URL or "(unset)")
    {"status": cmd_status, "create": cmd_create, "tick": cmd_tick,
     "teardown": cmd_teardown}.get(cmd, lambda: (log.error("unknown cmd %s", cmd), sys.exit(2)))()

if __name__ == "__main__":
    main()
