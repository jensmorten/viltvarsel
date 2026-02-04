import csv
import asyncio
import httpx
from tqdm import tqdm

# -------------------------------------
# Config
# -------------------------------------
INPUT_FILE = "Fallvilt_månedsberiket.csv"
OUTPUT_FILE = "Fallvilt_tidspunkter.csv"

API_BASE = "https://www.hjorteviltregisteret.no/api/v0/fallvilt"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "fallvilt-hendelsesdato-enricher/1.1-async"
}

MAX_CONCURRENCY = 16
REQUEST_TIMEOUT = 20.0
RETRY_BACKOFF = [0.5, 1.0, 2.0]

sem = asyncio.Semaphore(MAX_CONCURRENCY)

# Cache: id -> (HendelsesDatoTid, UkjentTidspunkt)
cache = {}


# --------------------------------------------------
# Fetch HendelsesDatoTid + UkjentTidspunkt for given Fallvilt-ID
# --------------------------------------------------
async def fetch_fallvilt_data(
    client: httpx.AsyncClient,
    fallvilt_id: str
) -> tuple[str, str]:
    """
    Fetches (HendelsesDatoTid, UkjentTidspunkt) for given fallvilt ID.
    Returns ("", "") if not found or error.
    Uses caching and retry for transient errors.
    """
    if fallvilt_id in cache:
        return cache[fallvilt_id]

    url = f"{API_BASE}/{fallvilt_id}"

    attempts = len(RETRY_BACKOFF) + 1
    for i in range(attempts):
        try:
            async with sem:
                resp = await client.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)

            # Success
            if resp.status_code == 200:
                data = resp.json()

                hendelse = data.get("HendelsesDatoTid", "")
                ukjent = data.get("UkjentTidspunkt", "")

                # Convert boolean to "true"/"false" or keep as empty
                if isinstance(ukjent, bool):
                    ukjent = "true" if ukjent else "false"
                else:
                    ukjent = ""

                cache[fallvilt_id] = (hendelse, ukjent)
                return (hendelse, ukjent)

            # Retry on server errors
            elif 500 <= resp.status_code < 600:
                if i < attempts - 1:
                    await asyncio.sleep(RETRY_BACKOFF[i])
                    continue
                else:
                    return ("", "")

            # No retry for 4xx errors
            else:
                return ("", "")

        except (httpx.HTTPError, asyncio.TimeoutError):
            if i < attempts - 1:
                await asyncio.sleep(RETRY_BACKOFF[i])
                continue
            return ("", "")

    return ("", "")  # fallback


# --------------------------------------------------
# Main processing
# --------------------------------------------------
async def prosesser():
    # Read input CSV
    with open(INPUT_FILE, mode="r", encoding="utf-8") as infile:
        reader = csv.reader(infile, delimiter=";")
        header = next(reader)
        rows = list(reader)

    # Add new columns
    out_header = header + ["HendelsesDatoTid", "UkjentTidspunkt"]

    # Index of Fallvilt-ID column
    try:
        fallvilt_idx = header.index("Fallvilt-ID")
    except ValueError:
        raise Exception("Kolonnen 'Fallvilt-ID' finnes ikke i CSV!")

    async with httpx.AsyncClient() as client:
        pbar = tqdm(total=len(rows), desc="Fetching fallvilt-data", unit="row")

        with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as outfile:
            writer = csv.writer(outfile, delimiter=";")
            writer.writerow(out_header)

            for row in rows:
                fallvilt_id = row[fallvilt_idx]

                # Fetch HendelsesDatoTid + UkjentTidspunkt
                hendelse, ukjent = await fetch_fallvilt_data(client, fallvilt_id)

                writer.writerow(row + [hendelse, ukjent])
                pbar.update(1)

        pbar.close()


if __name__ == "__main__":
    asyncio.run(prosesser())