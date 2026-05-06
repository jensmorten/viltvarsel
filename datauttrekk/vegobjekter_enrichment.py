import csv
import asyncio
import httpx
from tqdm import tqdm
from typing import Dict, Tuple
import sys

# Input and output file paths
input_file = 'Fallvilt_nvdb_enriched.csv'
output_file = 'Fallvilt_vegobjekter.csv'

# Column name for vegsystemreferanse - THIS IS THE FIX: use column name instead of hardcoded index
VEGSYSTEMREFERANSE_COLNAME = "vegsystemreferanse.kortform"

# REQUIRED by NVDB Les V4: X-Client must be set
headers = {
    "Accept": "application/json",
    "User-Agent": "fallvilt-posisjon-enricher/2.0-async",
    "X-Client": "fallvilt-posisjon-enricher",
}

vegobjekter = [
    {"id": 540, "navn": "ÅDT, total"},
    {"id": 105, "navn": "Fartsgrense"},
]

# --- Parallelization controls ---
MAX_CONCURRENCY = 16           # total concurrent HTTP calls
REQUEST_TIMEOUT = 20.0         # seconds
RETRY_BACKOFF = [0.5, 1.0, 2.0]  # simple backoff delays for transient errors

# Simple in-memory cache: (vegsystemreferanse, obj_id) -> Tuple[str, str]  (value, objekt_id)
CacheKey = Tuple[str, int]
cache: Dict[CacheKey, Tuple[str, str]] = {}

# Error tracking
request_errors: Dict[str, int] = {}

sem = asyncio.Semaphore(MAX_CONCURRENCY)

async def hent_egenskapsverdi_for_vegobjekt(
    client: httpx.AsyncClient,
    vegsystemreferanse: str,
    obj_id: int,
    egenskapsnavn: str,
) -> Tuple[str, str]:
    """
    Henter verdien til egenskapsnavn for et gitt vegobjekt-type-id på vegsystemreferanse
    OG id-en til selve objektinstansen (første objekt i svarlisten).
    Returnerer (verdi_str, objekt_id_str), tomme strenger hvis ikke funnet.
    Med enkel retry for transient 5xx/timeout.
    """
    cache_key = (vegsystemreferanse, obj_id)
    if cache_key in cache:
        return cache[cache_key]

    url = f"https://nvdbapiles.atlas.vegvesen.no/vegobjekter/api/v4/vegobjekter/{obj_id}"
    params = {
        "vegsystemreferanse": vegsystemreferanse,
        "inkluder": "egenskaper",
    }

    # retries for transient failures
    attempts = len(RETRY_BACKOFF) + 1
    for i in range(attempts):
        try:
            async with sem:
                resp = await client.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()

                # Finn første objekt-id i objekter-listen (dersom noen finnes)
                objekter = data.get("objekter", [])
                objekt_id_str = ""
                if objekter:
                    try:
                        objekt_id_str = "" if objekter[0].get("id") is None else str(objekter[0].get("id"))
                    except Exception:
                        objekt_id_str = ""

                # Finn egenskapsverdi for ønsket navn
                verdi_str = ""
                for obj in objekter:
                    for e in obj.get("egenskaper", []):
                        if e.get("navn") == egenskapsnavn:
                            verdi_str = "" if e.get("verdi") is None else str(e.get("verdi"))
                            cache[cache_key] = (verdi_str, objekt_id_str)
                            return (verdi_str, objekt_id_str)

                # Ikke funnet egenskap; cache tom verdi men behold objekt_id om vi fant den
                cache[cache_key] = (verdi_str, objekt_id_str)
                return (verdi_str, objekt_id_str)

            elif 500 <= resp.status_code < 600:
                # server side error: retry
                if i < attempts - 1:
                    await asyncio.sleep(RETRY_BACKOFF[i])
                    continue
                else:
                    error_key = f"HTTP {resp.status_code}"
                    request_errors[error_key] = request_errors.get(error_key, 0) + 1
                    return ("", "")
            else:
                # 4xx eller annet: ikke retry
                error_key = f"HTTP {resp.status_code}"
                request_errors[error_key] = request_errors.get(error_key, 0) + 1
                return ("", "")
        except (httpx.HTTPError, asyncio.TimeoutError) as e:
            if i < attempts - 1:
                await asyncio.sleep(RETRY_BACKOFF[i])
                continue
            error_key = f"Exception: {type(e).__name__}"
            request_errors[error_key] = request_errors.get(error_key, 0) + 1
            return ("", "")

    return ("", "")  # fallback

async def prosesser():
    # Read CSV header and rows
    with open(input_file, mode='r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')
        header = next(reader)
        rows = list(reader)

    # Map existing column names to their index
    existing_idx: Dict[str, int] = {col_name: i for i, col_name in enumerate(header)}

    # Find vegsystemreferanse column index (required)
    if VEGSYSTEMREFERANSE_COLNAME not in existing_idx:
        print(f"❌ FEIL: Kolonnen '{VEGSYSTEMREFERANSE_COLNAME}' ble ikke funnet!")
        print(f"   Tilgjengelige kolonner: {', '.join(header)}")
        sys.exit(1)
    
    vegsystemreferanse_idx = existing_idx[VEGSYSTEMREFERANSE_COLNAME]

    # Vegobjekter already present in the input (by column name)
    existing_vegobjekter = [vo for vo in vegobjekter if vo["navn"] in existing_idx]
    missing_vegobjekter = [vo for vo in vegobjekter if vo["navn"] not in existing_idx]

    # Output header: keep original columns; append only missing vegobjekt columns
    new_columns = []
    for vo in missing_vegobjekter:
        new_columns.append(f"Vegobjekt_{vo['id']}_id")
        new_columns.append(vo["navn"])
    out_header = header + new_columns

    async with httpx.AsyncClient() as client:
        pbar = tqdm(total=len(rows), desc="Processing rows", unit="row")

        with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile, delimiter=';')
            writer.writerow(out_header)

            for row_idx, row in enumerate(rows, start=2):
                # Extract vegsystemreferanse from correct column (THIS IS THE FIX)
                vegsystemreferanse = ""
                if vegsystemreferanse_idx < len(row):
                    vegsystemreferanse = row[vegsystemreferanse_idx].strip()
                
                if not vegsystemreferanse:
                    merged = [""] * (len(missing_vegobjekter) * 2)
                    writer.writerow(row + merged)
                    pbar.update(1)
                    continue

                tasks = [
                    hent_egenskapsverdi_for_vegobjekt(
                        client, vegsystemreferanse, vo["id"], vo["navn"]
                    )
                    for vo in missing_vegobjekter
                ]

                results = await asyncio.gather(*tasks, return_exceptions=False)

                merged = []
                for (value_str, objekt_id_str) in results:
                    merged.append(objekt_id_str)
                    merged.append(value_str)

                writer.writerow(row + merged)
                pbar.update(1)

        pbar.close()
    
    print(f"\n✅ Vegobjekter enrichment ferdig")
    print(f"   Input: {input_file}")
    print(f"   Output: {output_file}")
    print(f"   Rader behandlet: {len(rows)}")
    print(f"   Vegobjekter hentet: {len(missing_vegobjekter)}")
    print(f"   Cache hits: {len(cache)}")
    
    if request_errors:
        print(f"\n   ⚠️  Feil oppstod (teller):")
        for error_type, count in sorted(request_errors.items()):
            print(f"      - {error_type}: {count}")


if __name__ == "__main__":
    asyncio.run(prosesser())
