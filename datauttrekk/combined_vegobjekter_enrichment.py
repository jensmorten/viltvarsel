import csv
import time
from concurrent.futures import ThreadPoolExecutor
import requests
from requests.exceptions import RequestException
from tqdm import tqdm
from typing import Dict, Tuple, Optional

# ---- Files ----
input_file = 'Fallvilt_nvdb_enriched.csv'
final_output_file = 'Fallvilt_nvdb_adttotallengder.csv'

# ---- NVDB headers ----
headers = {
    "Accept": "application/json",
    "User-Agent": "fallvilt-posisjon-enricher/2.1-async",
    "X-Client": "fallvilt-posisjon-enricher",
}

# ---- Vegobjekter to fetch ----
vegobjekter = [
    {"id": 540, "navn": "ÅDT, total"},
    {"id": 105, "navn": "Fartsgrense"},
]

# ---- Controls ----

MAX_CONCURRENCY = 16
REQUEST_TIMEOUT = 20.0
RETRY_BACKOFF = [0.5, 1.0, 2.0]

# Caches
egenskapverdi_cache: Dict[Tuple[str, int], Tuple[str, str]] = {}  # (vegsystemreferanse, obj_id) -> (verdi, objekt_id)
lengde_cache: Dict[str, str] = {}  # objekt_id -> lengde


def hent_egenskapsverdi_for_vegobjekt(
    vegsystemreferanse: str,
    obj_id: int,
    egenskapsnavn: str,
    session: requests.Session,
) -> Tuple[str, str]:
    """
    Henter verdien til egenskapsnavn for et gitt vegobjekt-type-id på vegsystemreferanse
    OG id-en til selve objektinstansen (første objekt i svarlisten).
    Returnerer (verdi_str, objekt_id_str), tomme strenger hvis ikke funnet.
    Med enkel retry for transient 5xx/timeout.
    """
    cache_key = (vegsystemreferanse, obj_id)
    if cache_key in egenskapverdi_cache:
        return egenskapverdi_cache[cache_key]

    url = f"https://nvdbapiles.atlas.vegvesen.no/vegobjekter/api/v4/vegobjekter/{obj_id}"
    params = {"vegsystemreferanse": vegsystemreferanse, "inkluder": "egenskaper"}

    attempts = len(RETRY_BACKOFF) + 1
    for i in range(attempts):
        try:
            resp = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()

                objekter = data.get("objekter", [])
                objekt_id_str = ""
                if objekter:
                    try:
                        objekt_id_str = "" if objekter[0].get("id") is None else str(objekter[0].get("id"))
                    except Exception:
                        objekt_id_str = ""

                verdi_str = ""
                for obj in objekter:
                    for e in obj.get("egenskaper", []):
                        if e.get("navn") == egenskapsnavn:
                            verdi_str = "" if e.get("verdi") is None else str(e.get("verdi"))
                            egenskapverdi_cache[cache_key] = (verdi_str, objekt_id_str)
                            return (verdi_str, objekt_id_str)

                egenskapverdi_cache[cache_key] = (verdi_str, objekt_id_str)
                return (verdi_str, objekt_id_str)

            elif 500 <= resp.status_code < 600:
                if i < attempts - 1:
                    time.sleep(RETRY_BACKOFF[i])
                    continue
                else:
                    return ("", "")
            else:
                return ("", "")
        except RequestException:
            if i < attempts - 1:
                time.sleep(RETRY_BACKOFF[i])
                continue
            return ("", "")

    return ("", "")


def hent_lengde_for_objekt(
    objekt_id: str,
    session: requests.Session,
) -> str:
    """
    Henter 'lengde' for et spesifikt vegobjekt (type 540) ved å slå opp på /vegobjekter/540/{objekt_id}.
    Returnerer tom streng hvis ikke funnet.
    Med enkel retry/backoff for 5xx/timeout.
    """
    if not objekt_id:
        return ""

    if objekt_id in lengde_cache:
        return lengde_cache[objekt_id]

    url = f"https://nvdbapiles.atlas.vegvesen.no/vegobjekter/540/{objekt_id}"

    attempts = len(RETRY_BACKOFF) + 1
    for i in range(attempts):
        try:
            resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                lokasjon = data.get("lokasjon") or {}
                lengde = lokasjon.get("lengde", data.get("lengde", ""))
                val = "" if lengde is None else str(lengde)
                lengde_cache[objekt_id] = val
                return val

            elif 500 <= resp.status_code < 600:
                if i < attempts - 1:
                    time.sleep(RETRY_BACKOFF[i])
                    continue
                else:
                    return ""
            else:
                return ""
        except RequestException:
            if i < attempts - 1:
                time.sleep(RETRY_BACKOFF[i])
                continue
            return ""

    return ""


def prosesser():
    # Read CSV header and rows
    with open(input_file, mode='r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')
        header = next(reader)
        rows = list(reader)

    # Detect existing vegobjekt columns
    existing_idx: Dict[str, int] = {col_name: i for i, col_name in enumerate(header)}

    # Vegobjekter already present in the input
    existing_vegobjekter = [vo for vo in vegobjekter if vo["navn"] in existing_idx]
    missing_vegobjekter = [vo for vo in vegobjekter if vo["navn"] not in existing_idx]

    # Build output header: original columns + new vegobjekt columns
    new_columns = []
    vegobjekt_540_id_column = None
    for vo in missing_vegobjekter:
        id_col = f"Vegobjekt_{vo['id']}_id"
        new_columns.append(id_col)
        new_columns.append(vo["navn"])
        if vo['id'] == 540:
            vegobjekt_540_id_column = id_col

    # Add lengde column for vegobjekt 540
    lengde_col_name = "Vegobjekt_540_lengde"
    if lengde_col_name not in new_columns:
        new_columns.append(lengde_col_name)

    out_header = header + new_columns

    pbar = tqdm(total=len(rows), desc="Processing rows", unit="row")

    with requests.Session() as session:
        session.headers.update(headers)
        with open(final_output_file, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile, delimiter=';')
            writer.writerow(out_header)

            with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
                for row in rows:
                    vsr_index = existing_idx["vegsystemreferanse.kortform"]
                    vegsystemreferanse = row[vsr_index]

                    # Submit tasks for vegobjekter missing in header
                    futures = [
                        executor.submit(
                            hent_egenskapsverdi_for_vegobjekt,
                            vegsystemreferanse,
                            vo["id"],
                            vo["navn"],
                            session,
                        )
                        for vo in missing_vegobjekter
                    ]

                    # Retrieve results in the same order as submissions
                    results = [f.result() for f in futures]

                    merged = []
                    objekt_540_id = None
                    for i, (value_str, objekt_id_str) in enumerate(results):
                        merged.append(objekt_id_str)
                        merged.append(value_str)
                        if missing_vegobjekter[i]['id'] == 540:
                            objekt_540_id = objekt_id_str

                    # Fetch lengde for vegobjekt 540 if we have an ID
                    if objekt_540_id:
                        lengde_val = hent_lengde_for_objekt(objekt_540_id, session)
                    else:
                        lengde_val = ""

                    merged.append(lengde_val)

                    writer.writerow(row + merged)
                    pbar.update(1)

    pbar.close()


if __name__ == "__main__":
    prosesser()
