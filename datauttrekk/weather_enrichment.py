import os
import csv
import time
import math
import json
import pandas as pd
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pyproj import Transformer
import concurrent.futures
import threading

# =======================
# KONFIG
# =======================
INPUT_CSV  = "Fallvilt_trdlag_2016-2026_adttotallengder.csv"
OUTPUT_CSV = "Fallvilt_beriket_med_vær.csv"

CHUNK_SIZE = 600
REQUEST_TIMEOUT = 15
RETRY_BACKOFF = [1.0, 2.0, 4.0]  # 3 forsøk
USER_AGENT = "viltvarsel/1.0 (christian.sorli@yourdomain.no)"

# Frost endpoints
FROST_SOURCES_URL = "https://frost.met.no/sources/v0.jsonld"
FROST_OBS_URL     = "https://frost.met.no/observations/v0.jsonld"

# P1D-aggregater (dagssummer/-maks/-min/-snitt)
DAILY_ELEMENTS = ",".join([
    "max(air_temperature P1D)",
    "min(air_temperature P1D)",
    "mean(air_temperature P1D)",
    "sum(precipitation_amount P1D)",
    "max(wind_speed P1D)",
    "mean(wind_speed P1D)",
    "max(wind_speed_of_gust P1D)"
])

# Time-/instant-elementer for fallback (og snødybde)
RAW_ELEMENTS = ",".join([
    "snow_depth",
    "surface_snow_thickness",
    "snow_depth_surface",
    "air_temperature",
    "precipitation_amount",
    "wind_speed",
    "wind_speed_of_gust",
    "precipitation_type"
])

# Output-kolonner (vi lagrer verdier som "tall + enhet" i strenger, som i dine eksempler)
OUT_COLS = [
    "snow_depth",
    "max_temperature",
    "min_temperature",
    "mean_temperature",
    "total_precipitation",
    "max_wind_speed",
    "mean_wind_speed",
    "max_wind_gust",
    "precipitation_type",
    "weather_station_id"  # <--- added
]

# =======================
# AUTENTISERING / HEADERS
# =======================
load_dotenv()
CLIENT_ID = os.getenv("clientID")
CLIENT_SECRET = os.getenv("clientSecret")
if not CLIENT_ID or not CLIENT_SECRET:
    raise RuntimeError("Mangler clientID/clientSecret i .env")

HEADERS = {"Accept": "application/json", "User-Agent": USER_AGENT}
AUTH = (CLIENT_ID, CLIENT_SECRET)

# =======================
# PROJ / PARSING
# =======================
transformer = Transformer.from_crs("EPSG:25833", "EPSG:4326", always_xy=True)

def parse_coord(val):
    if pd.isna(val):
        return None
    s = str(val).strip().replace("\u00A0", "").replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def utm33_to_lonlat(east, north):
    lon, lat = transformer.transform(east, north)
    return lon, lat

# =======================
# HTTP HJELPERE MED RETRY
# =======================
# NEW: persistent HTTP session
session = requests.Session()
session.headers.update(HEADERS)
session.auth = AUTH
def frost_get(url, params):
    attempts = len(RETRY_BACKOFF) + 1
    for i in range(attempts):
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429,) or r.status_code >= 500:
                if i < attempts - 1:
                    time.sleep(RETRY_BACKOFF[i])
                    continue
            # hard failure
            try:
                err = r.json()
            except Exception:
                err = {"raw": r.text}
            raise RuntimeError(f"{url} feilet ({r.status_code}): {json.dumps(err, ensure_ascii=False)}")
        except (requests.RequestException, TimeoutError) as e:
            if i < attempts - 1:
                time.sleep(RETRY_BACKOFF[i])
                continue
            raise

# =======================
# CACHER
# =======================
station_cache = {}        # key=(grid_lon, grid_lat) -> "SNxxxxx"
daily_cache   = {}        # key=(station_id, date_iso) -> dict med ferdige strenger
raw_cache     = {}        # key=(station_id, date_iso) -> raw "data" (liste)

def grid50km(lon, lat):
    step = 0.45  # ~50 km
    return (round(lon/step)*step, round(lat/step)*step)

# =======================
# FROST-OPERASJONER
# =======================
def nearest_station_id(lon, lat):
    key = grid50km(lon, lat)
    if key in station_cache:
        return station_cache[key]
    params = {
        "geometry": f"nearest(POINT({lon} {lat}))",
        "fields": "id,name,geometry"
    }
    js = frost_get(FROST_SOURCES_URL, params)
    data = js.get("data", [])
    if not data:
        raise RuntimeError("Ingen stasjon funnet for punktet.")
    sn = data[0]["id"]  # "SNxxxxx"
    station_cache[key] = sn
    return sn

def get_p1d_summary(station_id, date_iso):
    params = {
        "sources": station_id,
        "elements": DAILY_ELEMENTS,
        "referencetime": date_iso
    }
    js = frost_get(FROST_OBS_URL, params)
    rows = js.get("data", [])
    if not rows:
        return None
    obs = {}
    for row in rows:
        for ob in row.get("observations", []):
            obs[ob.get("elementId")] = (ob.get("value"), ob.get("unit"))
    # Bygg strenger "verdi enhet" (eller tomt hvis None)
    def fmt(eid):
        v,u = obs.get(eid, (None, None))
        return (f"{v} {u}".strip() if v is not None else "")
    return {
        "max_temperature":     fmt("max(air_temperature P1D)"),
        "min_temperature":     fmt("min(air_temperature P1D)"),
        "mean_temperature":    fmt("mean(air_temperature P1D)"),
        "total_precipitation": fmt("sum(precipitation_amount P1D)"),
        "max_wind_speed":      fmt("max(wind_speed P1D)"),
        "mean_wind_speed":     fmt("mean(wind_speed P1D)"),
        "max_wind_gust":       fmt("max(wind_speed_of_gust P1D)")
    }

def get_raw_day(station_id, date_iso):
    key = (station_id, date_iso)
    if key in raw_cache:
        return raw_cache[key]
    d0 = datetime.fromisoformat(date_iso).date()
    d1 = d0 + timedelta(days=1)
    window = f"{d0}T00:00:00Z/{d1}T00:00:00Z"
    params = {
        "sources": station_id,
        "elements": RAW_ELEMENTS,
        "referencetime": window
    }
    js = frost_get(FROST_OBS_URL, params)
    rows = js.get("data", [])
    raw_cache[key] = rows
    return rows

def snow_depth_from_raw(raw_rows):
    # return første tilgjengelige snødybde
    for row in raw_rows:
        for ob in row.get("observations", []):
            if ob.get("elementId") in ("snow_depth","surface_snow_thickness","snow_depth_surface"):
                v = ob.get("value"); u = ob.get("unit")
                if v is not None:
                    return f"{v} {u}".strip() if u else str(v)
    return ""

def compute_daily_from_raw(raw_rows):
    if not raw_rows:
        return {}
    recs = []
    for row in raw_rows:
        for ob in row.get("observations", []):
            recs.append({
                "element": ob.get("elementId"),
                "value": ob.get("value"),
                "unit":  ob.get("unit")
            })
    if not recs:
        return {}
    df = pd.DataFrame.from_records(recs)
    def unit_of(elem):
        s = df.loc[df["element"]==elem, "unit"]
        return s.iloc[0] if not s.empty else ""
    def as_float(elem):
        s = df.loc[df["element"]==elem, "value"]
        return s.astype(float) if not s.empty else pd.Series(dtype=float)

    T  = as_float("air_temperature")
    P  = as_float("precipitation_amount")
    W  = as_float("wind_speed")
    WG = as_float("wind_speed_of_gust")
    PT = df.loc[df["element"]=="precipitation_type","value"]  # kan være str/int

    result = {
        "max_temperature":     f"{T.max()} {unit_of('air_temperature')}"           if not T.empty  else "",
        "min_temperature":     f"{T.min()} {unit_of('air_temperature')}"           if not T.empty  else "",
        "mean_temperature":    f"{T.mean()} {unit_of('air_temperature')}"          if not T.empty  else "",
        "total_precipitation": f"{P.sum()} {unit_of('precipitation_amount')}"      if not P.empty  else "",
        "max_wind_speed":      f"{W.max()} {unit_of('wind_speed')}"                if not W.empty  else "",
        "mean_wind_speed":     f"{W.mean()} {unit_of('wind_speed')}"               if not W.empty  else "",
        "max_wind_gust":       f"{WG.max()} {unit_of('wind_speed_of_gust')}"       if not WG.empty else "",
        "precipitation_type":  (str(PT.mode().iloc[0]) if not PT.empty and not PT.mode().empty else "")
    }
    return result

# Global lock for thread‑safe cache access
cache_lock = threading.Lock()

def get_daily_weather(lon, lat, date_iso):
    """Returner dict for OUT_COLS (strenger) gitt lon/lat og dato (YYYY-MM-DD)."""
    # 1) Finn stasjon (caches)
    station_id = nearest_station_id(lon, lat)

    # 2) P1D aggregater (caches per station×dato)
    cache_key = (station_id, date_iso)
    with cache_lock:
        if cache_key in daily_cache:
            return daily_cache[cache_key]

    p1d = get_p1d_summary(station_id, date_iso)
    # 3) Hent raw (trengs uansett for snødybde)
    raw = get_raw_day(station_id, date_iso)
    snow = snow_depth_from_raw(raw)

    if not p1d or all(not v for v in p1d.values()):
        # fallback – beregn fra rå
        p1d = compute_daily_from_raw(raw)

    # Sikre alle felter + snødybde
    out = {k: p1d.get(k,"") for k in OUT_COLS if k!="snow_depth"}
    out["snow_depth"] = snow
    out["weather_station_id"] = station_id  # <--- added

    with cache_lock:
        daily_cache[cache_key] = out
    return out

# =======================
# HOVEDPIPELINE
# =======================

kommune_dato_cache = {}   # key = (Kommune, date_iso) -> weather dict

def process_row(row, idx, new_cols, kommune_dato_cache, kommune_cache_lock):
    kommune = str(row.get("Kommune", "")).strip()
    d = row.get("Dato")

    out_row = list(row.values)

    if pd.isna(d):
        out_row.extend([""] * len(new_cols))
        return idx, out_row

    date_iso = pd.to_datetime(d).strftime("%Y-%m-%d")

    cache_key = (kommune, date_iso)
    with kommune_cache_lock:
        if cache_key in kommune_dato_cache:
            wx = kommune_dato_cache[cache_key]
            out_row.extend([wx.get(c, "") for c in new_cols])
            return idx, out_row

    east = parse_coord(row.get("UTM33 øst"))
    north = parse_coord(row.get("UTM33 nord"))

    if east is None or north is None:
        out_row.extend([""] * len(new_cols))
        return idx, out_row

    try:
        lon, lat = utm33_to_lonlat(east, north)
    except Exception:
        out_row.extend([""] * len(new_cols))
        return idx, out_row

    try:
        wx = get_daily_weather(lon, lat, date_iso)
    except Exception:
        wx = {c: "" for c in new_cols}

    with kommune_cache_lock:
        kommune_dato_cache[cache_key] = wx

    out_row.extend([wx.get(c, "") for c in new_cols])
    return idx, out_row

def main():
    # Forbered output: skriv header én gang
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)

    # Les input-header
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")
        base_header = next(reader)

    new_cols = [c for c in OUT_COLS if c not in base_header]
    out_header = base_header + new_cols

    # Åpne output-fil for streaming append
    out_f = open(OUTPUT_CSV, "w", newline="", encoding="utf-8")
    out_writer = csv.writer(out_f, delimiter=";")
    out_writer.writerow(out_header)

    # Progressbar
    total_rows = sum(1 for _ in open(INPUT_CSV, encoding="utf-8")) - 1
    pbar = tqdm(total=total_rows, desc="Beriker med værdata", unit="rader")

    kommune_dato_cache = {}   # key = (Kommune, date_iso) -> weather dict
    kommune_cache_lock = threading.Lock()

    # STREAMING: ikke bygg store chunks, skriv rad for rad
    for chunk in pd.read_csv(INPUT_CSV, sep=";", chunksize=CHUNK_SIZE, low_memory=False):
        # Normaliser dato
        chunk["Dato"] = pd.to_datetime(chunk["Dato"], format="%d.%m.%Y", errors="coerce")

        # Submit rows to thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            futures = [
                executor.submit(process_row, row, idx, new_cols, kommune_dato_cache, kommune_cache_lock)
                for idx, row in chunk.iterrows()
            ]
            # Ensure order by sorting futures by idx
            for future in concurrent.futures.as_completed(futures):
                idx, out_row = future.result()
                out_writer.writerow(out_row)
                pbar.update(1)

    pbar.close()
    out_f.close()
    print(f"🎉 Ferdig! Skrev {OUTPUT_CSV}")

if __name__ == "__main__":
    main()