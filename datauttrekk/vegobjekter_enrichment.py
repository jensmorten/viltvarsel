import csv
import asyncio
import httpx
from tqdm import tqdm
from typing import Dict, Tuple

# Input and output file paths
input_file = 'Fallvilt_trdlag_2016-2026_enriched.csv'
output_file = 'Fallvilt_trdlag_2016-2026_vegobjekter.csv'  # generalized name

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
                    return ("", "")
            else:
                # 4xx eller annet: ikke retry
                return ("", "")
        except (httpx.HTTPError, asyncio.TimeoutError):
            if i < attempts - 1:
                await asyncio.sleep(RETRY_BACKOFF[i])
                continue
            return ("", "")

    return ("", "")  # fallback

async def prosesser():
    # Read CSV header and rows
    with open(input_file, mode='r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')
        header = next(reader)
        rows = list(reader)

    # --- Minimal change starts: detect existing vegobjekt columns in input ---
    # Map existing column names to their index
    existing_idx: Dict[str, int] = {col_name: i for i, col_name in enumerate(header)}

    # Vegobjekter already present in the input (by column name)
    existing_vegobjekter = [vo for vo in vegobjekter if vo["navn"] in existing_idx]
    missing_vegobjekter = [vo for vo in vegobjekter if vo["navn"] not in existing_idx]

    # Output header: keep original columns; append only missing vegobjekt columns
    # Now we add two columns per missing vegobjekt: ID + value
    new_columns = []
    for vo in missing_vegobjekter:
        new_columns.append(f"Vegobjekt_{vo['id']}_id")   # NEW ID COLUMN
        new_columns.append(vo["navn"])                   # value column
    out_header = header + new_columns
    # --- Minimal change ends ---

    async with httpx.AsyncClient() as client:
        # Prepare progress bar
        pbar = tqdm(total=len(rows), desc="Processing rows", unit="row")

        # Open writer once
        with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile, delimiter=';')
            writer.writerow(out_header)

            for row in rows:
                vegsystemreferanse = row[13]  # adjust index if needed

                # Only fetch missing vegobjekter (we don't rewrite existing ones)
                tasks = [
                    hent_egenskapsverdi_for_vegobjekt(
                        client, vegsystemreferanse, vo["id"], vo["navn"]
                    )
                    for vo in missing_vegobjekter
                ]

                # Each result is a tuple: (value_str, objekt_id_str)
                results = await asyncio.gather(*tasks, return_exceptions=False)

                # Interleave as: ID1, Value1, ID2, Value2, ...
                merged = []
                for (value_str, objekt_id_str) in results:
                    merged.append(objekt_id_str)  # ID first
                    merged.append(value_str)      # value second

                # Write original row + fetched columns
                writer.writerow(row + merged)
                pbar.update(1)

        pbar.close()

if __name__ == "__main__":
    asyncio.run(prosesser())