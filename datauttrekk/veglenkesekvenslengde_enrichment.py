import csv
import asyncio
import httpx
from tqdm import tqdm
from typing import Dict

# =======================
# Config
# =======================
input_file = 'Fallvilt_trdlag_2016-2026_vegobjekter.csv'
output_file = 'Fallvilt_trdlag_2016-2026_veglenkesekvenslengde.csv'

# Name of the input column that contains veglenkesekvensId (exact header text)
VEGLENKESEKV_ID_COL = 'veglenkesekvensid'

# Name of the output column to write length to
LENGTH_COL = 'Veglenkesekvenslengde'

# If True and the LENGTH_COL already exists in the input file, overwrite it
# If False and LENGTH_COL exists, keep existing values and skip API calls for those rows
REFRESH_EXISTING = False

# REQUIRED by NVDB Les V4: X-Client must be set
headers = {
    "Accept": "application/json",
    "User-Agent": "fallvilt-veglenkesekvenslengde/1.0-async",
    "X-Client": "fallvilt-veglenkesekvenslengde",
}

# --- Parallelization / retry controls ---
MAX_CONCURRENCY = 16
REQUEST_TIMEOUT = 20.0  # seconds
RETRY_BACKOFF = [0.5, 1.0, 2.0, 4.0]  # simple backoff delays

# Base URL for veglenkesekvenser (NVDB vegnett API v4)
BASE_URL = "https://nvdbapiles.atlas.vegvesen.no/vegnett/api/v4/veglenkesekvenser"

# Simple in-memory cache: veglenkesekvensId -> str(lengde)
cache: Dict[int, str] = {}

sem = asyncio.Semaphore(MAX_CONCURRENCY)


def _parse_int(value: str) -> int | None:
    """
    Try to parse veglenkesekvensId robustly.
    Returns int or None if parsing fails.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        # most IDs are pure ints
        return int(s)
    except ValueError:
        # handle cases like "1742829.0"
        try:
            return int(float(s))
        except ValueError:
            return None


async def fetch_veglenkesekvens_lengde(client: httpx.AsyncClient, veglenkesekvens_id: int) -> str:
    """
    Fetches 'lengde' for a given veglenkesekvensId.
    Returns empty string if not found or on non-retriable errors.
    Includes simple retries for 429/5xx/timeouts.
    """
    if veglenkesekvens_id in cache:
        return cache[veglenkesekvens_id]

    url = f"{BASE_URL}/{veglenkesekvens_id}"

    attempts = len(RETRY_BACKOFF) + 1
    for i in range(attempts):
        try:
            async with sem:
                resp = await client.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 200:
                data = resp.json()
                lengde = data.get("lengde")
                val = "" if lengde is None else str(lengde)
                cache[veglenkesekvens_id] = val
                return val

            # Retry on rate limit or server errors
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                if i < attempts - 1:
                    await asyncio.sleep(RETRY_BACKOFF[i])
                    continue
                else:
                    return ""

            # For 4xx (other than 429), do not retry
            return ""

        except (httpx.HTTPError, asyncio.TimeoutError):
            if i < attempts - 1:
                await asyncio.sleep(RETRY_BACKOFF[i])
                continue
            return ""

    return ""  # Fallback (shouldn't get here)


async def prosesser():
    # Read CSV header and rows
    with open(input_file, mode='r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')
        header = next(reader)
        rows = list(reader)

    # Build index mapping
    col_idx = {name: i for i, name in enumerate(header)}

    if VEGLENKESEKV_ID_COL not in col_idx:
        raise ValueError(
            f"Input column '{VEGLENKESEKV_ID_COL}' not found in CSV header. "
            f"Available columns: {header}"
        )

    vegsekv_idx = col_idx[VEGLENKESEKV_ID_COL]

    # Prepare output header:
    # - if LENGTH_COL exists and REFRESH_EXISTING=False -> keep as-is
    # - else (missing) -> append it
    length_col_exists = LENGTH_COL in col_idx
    if length_col_exists:
        out_header = header[:]  # keep original order
        length_idx = col_idx[LENGTH_COL]
    else:
        out_header = header + [LENGTH_COL]
        length_idx = None  # will be last column in output

    async with httpx.AsyncClient() as client:
        with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile, delimiter=';')
            writer.writerow(out_header)

            pbar = tqdm(total=len(rows), desc="Processing rows", unit="row")

            for row in rows:
                # Decide if we should fetch new value
                should_fetch = True

                if length_col_exists and not REFRESH_EXISTING:
                    # Keep the existing value; write row unchanged
                    writer.writerow(row)
                    pbar.update(1)
                    continue

                # Parse veglenkesekvensId
                veglenkesekvens_id = _parse_int(row[vegsekv_idx])

                if veglenkesekvens_id is None:
                    # No valid ID -> write empty length
                    if length_col_exists:
                        # overwrite existing length (if refreshing)
                        new_row = list(row)
                        new_row[length_idx] = ""
                        writer.writerow(new_row)
                    else:
                        # append new column with empty value
                        writer.writerow(row + [""])
                    pbar.update(1)
                    continue

                # Fetch length
                length_val = await fetch_veglenkesekvens_lengde(client, veglenkesekvens_id)

                # Write output row
                if length_col_exists:
                    new_row = list(row)
                    new_row[length_idx] = length_val
                    writer.writerow(new_row)
                else:
                    writer.writerow(row + [length_val])

                pbar.update(1)

            pbar.close()


if __name__ == "__main__":
    asyncio.run(prosesser())