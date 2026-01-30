import csv
import asyncio
import httpx
from tqdm import tqdm
from typing import Dict, Tuple, Optional

# ---- Files ----
# Use the output from your previous script as input here:
input_file = 'Fallvilt_trdlag_2016-2026_vegobjekter.csv'
output_file = 'Fallvilt_trdlag_2016-2026_adttotallengder.csv'

# ---- NVDB headers (same style as before) ----
headers = {
    "Accept": "application/json",
    "User-Agent": "fallvilt-posisjon-enricher/2.1-async",
    "X-Client": "fallvilt-posisjon-enricher",
}

# ---- Controls ----
MAX_CONCURRENCY = 16
REQUEST_TIMEOUT = 20.0
RETRY_BACKOFF = [0.5, 1.0, 2.0]

sem = asyncio.Semaphore(MAX_CONCURRENCY)

# Simple cache: objekt-id (string) -> lengde (string)
cache: Dict[str, str] = {}

VEGOBJEKT_TYPE_ID = 540
ID_COL_NAME = f"Vegobjekt_{VEGOBJEKT_TYPE_ID}_id"
LENGDE_COL_NAME = f"Vegobjekt_{VEGOBJEKT_TYPE_ID}_lengde"


async def hent_lengde_for_objekt(
    client: httpx.AsyncClient,
    objekt_id: str,
) -> str:
    """
    Henter 'lengde' for et spesifikt vegobjekt (type 540) ved å slå opp på /vegobjekter/540/{objekt_id}.
    Returnerer tom streng hvis ikke funnet.
    Med enkel retry/backoff for 5xx/timeout.
    """
    if not objekt_id:
        return ""

    if objekt_id in cache:
        return cache[objekt_id]

    url = f"https://nvdbapiles.atlas.vegvesen.no/vegobjekter/{VEGOBJEKT_TYPE_ID}/{objekt_id}"

    attempts = len(RETRY_BACKOFF) + 1
    for i in range(attempts):
        try:
            async with sem:
                resp = await client.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 200:
                data = resp.json()
                lokasjon = data.get("lokasjon") or {}
                lengde = lokasjon.get("lengde", data.get("lengde", ""))
                # Behold som streng for CSV
                val = "" if lengde is None else str(lengde)
                cache[objekt_id] = val
                return val

            elif 500 <= resp.status_code < 600:
                # Server-feil -> retry
                if i < attempts - 1:
                    await asyncio.sleep(RETRY_BACKOFF[i])
                    continue
                else:
                    return ""
            else:
                # 4xx/annet -> ikke retry
                return ""
        except (httpx.HTTPError, asyncio.TimeoutError):
            if i < attempts - 1:
                await asyncio.sleep(RETRY_BACKOFF[i])
                continue
            return ""

    return ""


async def prosesser():
    # Read CSV
    with open(input_file, mode='r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')
        header = next(reader)
        rows = list(reader)

    # Locate columns
    col_index: Dict[str, int] = {name: i for i, name in enumerate(header)}

    if ID_COL_NAME not in col_index:
        raise ValueError(
            f"Fant ikke kolonnen '{ID_COL_NAME}' i input ({input_file}). "
            f"Sørg for å kjøre forrige script først."
        )

    id_idx = col_index[ID_COL_NAME]

    # Add length column if missing
    write_header = header[:]
    if LENGDE_COL_NAME in col_index:
        lengde_idx_existing = col_index[LENGDE_COL_NAME]
        add_new_col = False
    else:
        write_header.append(LENGDE_COL_NAME)
        add_new_col = True

    async with httpx.AsyncClient() as client:
        pbar = tqdm(total=len(rows), desc="Fetching lengde for 540", unit="row")

        with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile, delimiter=';')
            writer.writerow(write_header)

            # Prepare tasks in small batches to avoid huge memory usage
            # We'll process row-by-row here while still using concurrency per request
            # by launching per-row tasks only when needed.
            for row in rows:
                objekt_id = row[id_idx].strip() if id_idx < len(row) else ""

                # If length column already exists and has a value, keep it
                if not add_new_col:
                    # Just write row as-is; no overwrite
                    writer.writerow(row)
                    pbar.update(1)
                    continue

                # When adding new column: fetch length if we have an id; else append ""
                if objekt_id:
                    lengde_val = await hent_lengde_for_objekt(client, objekt_id)
                else:
                    lengde_val = ""

                writer.writerow(row + [lengde_val])
                pbar.update(1)

        pbar.close()


if __name__ == "__main__":
    asyncio.run(prosesser())