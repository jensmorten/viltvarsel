import csv
from typing import Dict, Any, Tuple
import requests
from tqdm import tqdm

# --- Config (add these) ---
X_CLIENT = "fallvilt-posisjon-enricher" 

# --- Config ---
INPUT_FILE = "Fallvilt.csv"
OUTPUT_FILE = "Fallvilt_nvdb_enriched.csv"

# NVDB posisjon endpoint (Les V4, produksjon)
POSISJON_URL = "https://nvdbapiles.atlas.vegvesen.no/vegnett/api/v4/posisjon"

# Max distance (meters) from the given point to the road network
MAKS_AVSTAND = 200

# Networking / concurrency
TIMEOUT = 10.0        # seconds
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5                  # seconds, exponential backoff base
CONCURRENCY = 16                     # tune: 8–64; lower if you hit 429/5xx

# Columns expected in input CSV (Norwegian headers, semicolon separated)
COL_OST = "UTM33 øst"
COL_NORD = "UTM33 nord"

# New columns to append (flattened from the posisjon result)
NEW_COLS = [
    "vegsystemreferanse.kortform",
    "vegkategori",
    "fase",
    "vegnr",
    "strekning",
    "delstrekning",
    "arm",
    "adskilte_løp",
    "trafikantgruppe",
    "retning",
    "meter",
    "veglenkesekvensid",
    "relativPosisjon",
    "veglenkesekvens.kortform",
    "geometri.wkt",
    "geometri.srid",
    "kommune (treff)",
    "avstand_vegnettet_m",
]

def parse_float_locale(value: str):
    """
    Convert a string that may use decimal comma to float-compatible dot.
    Returns None if input is empty or cannot be parsed.
    """
    if value is None:
        return None
    v = value.strip().replace(" ", "")
    if v == "":
        return None
    v = v.replace(",", ".")
    try:
        return float(v)
    except ValueError:
        return None

def blank_result() -> Dict[str, Any]:
    return {col: "" for col in NEW_COLS}

def extract_fields(hit: Dict[str, Any]) -> Dict[str, Any]:
    result = blank_result()
    vsr = hit.get("vegsystemreferanse", {}) or {}
    vegsystem = vsr.get("vegsystem", {}) or {}
    strek = vsr.get("strekning", {}) or {}
    vls = hit.get("veglenkesekvens", {}) or {}
    geom = hit.get("geometri", {}) or {}

    result["vegsystemreferanse.kortform"] = vsr.get("kortform", "")
    result["vegkategori"] = vegsystem.get("vegkategori", "")
    result["fase"] = vegsystem.get("fase", "")
    result["vegnr"] = vegsystem.get("nummer", "")
    result["strekning"] = strek.get("strekning", "")
    result["delstrekning"] = strek.get("delstrekning", "")
    result["arm"] = strek.get("arm", "")
    result["adskilte_løp"] = strek.get("adskilte_løp", "")
    result["trafikantgruppe"] = strek.get("trafikantgruppe", "")
    result["retning"] = strek.get("retning", "")
    result["meter"] = strek.get("meter", "")
    result["veglenkesekvensid"] = vls.get("veglenkesekvensid", "")
    result["relativPosisjon"] = vls.get("relativPosisjon", "")
    result["veglenkesekvens.kortform"] = vls.get("kortform", "")
    result["geometri.wkt"] = geom.get("wkt", "")
    result["geometri.srid"] = geom.get("srid", "")
    result["kommune (treff)"] = hit.get("kommune", "")
    result["avstand_vegnettet_m"] = hit.get("avstand", "")
    return result

import math

def posisjon_lookup(ost: float, nord: float, headers: dict) -> Dict[str, Any]:
    """
    Async call to NVDB posisjon. Returns flattened dict matching NEW_COLS.
    Also logs the exact HTTP request and any error body on failure.
    """
    params = {
        "maks_avstand": MAKS_AVSTAND,
        "nord": nord,
        "ost": ost,
        # Optional flags; keep them off unless needed
        # "detaljerte_lenker": False,
        # "konnekteringslenker": False,
    }

    # Defensive: remove None/NaN values (these will yield 400s)
    params = {k: v for k, v in params.items() if v is not None and not (isinstance(v, float) and math.isnan(v))}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(POSISJON_URL, params=params, headers=headers, timeout=TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    return extract_fields(data[0])  # take first hit
                return blank_result()
            err_text = resp.text
            print("Error body (truncated to 1k):", err_text[:1000])
            import time
            time.sleep(RETRY_BACKOFF ** attempt)
        except requests.RequestException as e:
            print(f"RequestError on attempt {attempt}: {e}")
            import time
            time.sleep(RETRY_BACKOFF ** attempt)
    return blank_result()

def enrich_row_with_posisjon(
    row: Dict[str, Any],
    cache: Dict[Tuple[float, float], Dict[str, Any]],
    headers: dict
) -> Dict[str, Any]:
    ost = parse_float_locale(row.get(COL_OST))
    nord = parse_float_locale(row.get(COL_NORD))
    if ost is None or nord is None:
        return blank_result()

    key = (ost, nord)
    cached = cache.get(key)
    if cached is not None:
        return cached

    enriched = posisjon_lookup(ost, nord, headers)
    cache[key] = enriched
    return enriched


def main():
    # Read rows first (unchanged)
    with open(INPUT_FILE, mode="r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile, delimiter=";")
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    # Append new columns (unchanged)
    for col in NEW_COLS:
        if col not in fieldnames:
            fieldnames.append(col)

    # ✅ REQUIRED by NVDB Les V4: X-Client must be set
    headers = {
        "Accept": "application/json",
        "User-Agent": "fallvilt-posisjon-enricher/2.0-async",
        "X-Client": X_CLIENT,
    }

    cache: Dict[Tuple[float, float], Dict[str, Any]] = {}
    results: Dict[int, Dict[str, Any]] = {}
    for i in tqdm(range(len(rows)), desc="Enriching rows", unit="row"):
        enriched = enrich_row_with_posisjon(rows[i], cache, headers)
        results[i] = enriched

    # Write output CSV once, preserving original order
    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for i in range(len(rows)):
            row = rows[i]
            row.update(results.get(i, blank_result()))
            writer.writerow(row)

    print(f"✅ Enriched data written to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()