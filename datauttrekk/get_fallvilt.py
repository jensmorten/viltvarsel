#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Henter ALLE fallvilt i Trøndelag (fylkesnr=50) fra 2025-01-01 til dagens dato,
filtrert på arsak=PåkjørtAvMotorkjøretøy, paginerer alle sider og lagrer til CSV.

Endringer / oppførsel:
- Ekte projeksjon fra WGS84 (lat/lon) til UTM sone 33N (EPSG:32633) -> 'UTM33 øst' (Easting, m) og 'UTM33 nord' (Northing, m).
- Legger til standardiserte kolonner: `Dato`, `År`, `Kommune`, `Stedfesting`, `Art`, `Kjønn`, `Alder`, `Årsak`, `Utfall`, `Merkelappnummer`, `Fallvilt-ID`, `UTM33 øst`, `UTM33 nord`.
- BEHOLDER alle opprinnelige kolonner fra API-responsen; fjerner ingen råkolonner.
- Hvis en original kolonne har et annet navn enn standarden, opprettes standardkolonnen ved siden av (dvs. vi legger til eller eventuelt overskriver med samme navn, men aldri sletter opprinnelige kolonner).
"""

import datetime as dt
import math
import sys
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

BASE_URL = "https://www.hjorteviltregisteret.no/api/v0/fallvilt"

# ---------------------------
# UTM 33N (WGS84 -> EPSG:32633)
# ---------------------------


def wgs84_to_utm33(lat_deg: float, lon_deg: float):
    """
    Konverterer WGS84-geografiske koordinater (grader) til UTM sone 33N (EPSG:32633).
    Returnerer (Easting, Northing) i meter.

    Kilde: Standard UTM-formler (Transverse Mercator) med WGS84-ellipsoideparametre.
    """

    # WGS84-ellipsoide
    a = 6378137.0                     # semi-major axis
    f = 1 / 298.257223563
    e2 = f * (2 - f)                  # eksentrisitet^2
    ep2 = e2 / (1.0 - e2)             # sekundær eksentrisitet^2

    # UTM-konstanter
    k0 = 0.9996
    lon0_deg = 15.0                   # sone 33N har sentralmeridian 15°E
    lon0 = math.radians(lon0_deg)
    false_easting = 500000.0
    false_northing = 0.0              # nordlige halvkule

    # Inn
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)

    # Hjelpestørrelser
    sin_lat = math.sin(lat)
    cos_lat = math.cos(lat)
    tan_lat = math.tan(lat)

    N = a / math.sqrt(1.0 - e2 * sin_lat * sin_lat)
    T = tan_lat * tan_lat
    C = ep2 * cos_lat * cos_lat
    A = (lon - lon0) * cos_lat

    # Meridional arc (M)
    e4 = e2 * e2
    e6 = e4 * e2
    M = (a * ((1 - e2/4 - 3*e4/64 - 5*e6/256) * lat
         - (3*e2/8 + 3*e4/32 + 45*e6/1024) * math.sin(2*lat)
         + (15*e4/256 + 45*e6/1024) * math.sin(4*lat)
         - (35*e6/3072) * math.sin(6*lat)))

    # Easting (x) og Northing (y)
    A2 = A * A
    A3 = A2 * A
    A4 = A2 * A2
    A5 = A4 * A
    A6 = A4 * A2

    x = (k0 * N * (A
         + (1 - T + C) * A3 / 6.0
         + (5 - 18*T + T*T + 72*C - 58*ep2) * A5 / 120.0)
         + false_easting)

    y = (k0 * (M + N * tan_lat * (A2 / 2.0
         + (5 - T + 9*C + 4*C*C) * A4 / 24.0
         + (61 - 58*T + T*T + 600*C - 330*ep2) * A6 / 720.0))
         + false_northing)

    return x, y  # (E, N)

# ---------------------------
# API-henting
# ---------------------------


def build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "fallvilt-client/1.2",
        "Accept": "application/json",
    })
    return session


def fetch_page(session: requests.Session, params: Dict[str, Any], page: int, timeout: int = 30) -> List[Dict[str, Any]]:
    q = dict(params)
    q["page"] = page
    resp = session.get(BASE_URL, params=q, timeout=timeout)
    if resp.status_code != 200:
        try:
            err = resp.json()
        except Exception:
            err = resp.text
        raise RuntimeError(f"HTTP {resp.status_code} ved page={page}: {err}")
    try:
        data = resp.json()
    except ValueError as e:
        raise RuntimeError(
            f"Kunne ikke parse JSON for page={page}: {e}") from e
    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        return data.get("items", [])
    return []


def paginate_all(
    fra_dato: str = "2025-01-01",
    fylkesnr: int = 50,
    page_size: int = 1000,
    arsak: str = "PåkjørtAvMotorkjøretøy",
    til_dato: Optional[str] = None,
    sleep_between: float = 0.0,
) -> List[Dict[str, Any]]:
    session = build_session()
    params: Dict[str, Any] = {
        "fraDato": fra_dato,
        "fylkesnr": fylkesnr,
        "pageSize": page_size,
        "arsak": arsak,
    }
    if til_dato:
        params["tilDato"] = til_dato

    all_rows: List[Dict[str, Any]] = []
    page = 1
    while True:
        rows = fetch_page(session, params, page=page)
        n = len(rows)
        print(f"Side {page}: {n} rader")
        if n == 0:
            break
        all_rows.extend(rows)
        if n < page_size:
            break
        page += 1
        if sleep_between:
            time.sleep(sleep_between)
    return all_rows

# ---------------------------
# Transformasjon til ønsket CSV
# ---------------------------


def _first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def to_csv_custom(rows: List[Dict[str, Any]], out_csv: str) -> None:
    # Ønsket sluttkolonner i rekkefølge
    desired = ["Dato", "År", "Kommune", "Stedfesting", "Art", "Kjønn", "Alder",
               "Årsak", "Utfall", "Merkelappnummer", "Fallvilt-ID", "UTM33 øst", "UTM33 nord", "OppdatertDatoTid"]

    if not rows:
        pd.DataFrame(columns=desired).to_csv(
            out_csv, index=False, encoding="utf-8")
        print(f"Ingen rader hentet. Tom CSV skrevet til {out_csv}")
        return

    df = pd.json_normalize(rows, sep=".")

    # Finn sannsynlige kilder
    hend_col = _first_existing(
        df, ["HendelsesDatoTid", "hendelsesDatoTid", "hendelse.datoTid", "HendelseDato"])
    komnr_col = _first_existing(
        df, ["Kommune.KommuneNummer", "kommune.kommuneNummer", "KommuneNummer", "kommune.nummer", "kommune.kode"])
    komnav_col = _first_existing(
        df, ["Kommune.KommuneNavn", "kommune.kommuneNavn", "KommuneNavn", "kommune.navn"])
    sted_col = _first_existing(
        df, ["Stedfesting", "stedfesting", "hendelse.stedfesting", "Hendelse.Stedfesting"])
    art_col = _first_existing(df, ["Art", "art", "dyr.art", "Dyr.Art"])
    kjonn_col = _first_existing(
        df, ["Kjonn", "kjonn", "Kjønn", "dyr.kjonn", "Dyr.Kjonn"])
    alder_col = _first_existing(
        df, ["Alder", "alder", "dyr.alder", "Dyr.Alder"])
    arsak_col = _first_existing(df, ["Arsak", "arsak"])
    utfall_col = _first_existing(df, ["Utfall", "utfall"])
    merk_col = _first_existing(
        df, ["Merkelappnummer", "merkelappnummer", "merkelapp.nummer", "Merkelapp.Nummer"])
    fvid_col = _first_existing(df, ["FallviltId", "fallviltId", "FallviltID"])
    lat_col = _first_existing(
        df, ["Latitude", "latitude", "posisjon.latitude", "geo.latitude"])
    lon_col = _first_existing(
        df, ["Longitude", "longitude", "posisjon.longitude", "geo.longitude"])

    # Dato og År
    if hend_col:
        ts = pd.to_datetime(df[hend_col], errors="coerce", utc=False)
        df["Dato"] = ts.dt.strftime("%Y-%m-%d")
        df["År"] = ts.dt.year
    else:
        df["Dato"] = pd.NA
        df["År"] = pd.NA

    # Kommune "<nr> <navn>"
    if komnr_col or komnav_col:
        nr = df[komnr_col].astype("string") if komnr_col else ""
        nv = df[komnav_col].astype("string") if komnav_col else ""
        df["Kommune"] = (nr.fillna("") + " " + nv.fillna("")).str.strip()
    else:
        df["Kommune"] = pd.NA

    # Enkle kopier/omdøp til nye navn
    df["Stedfesting"] = df[sted_col] if sted_col else pd.NA
    df["Art"] = df[art_col] if art_col else pd.NA
    df["Kjønn"] = df[kjonn_col] if kjonn_col else (
        df["Kjonn"] if "Kjonn" in df.columns else pd.NA)
    df["Alder"] = df[alder_col] if alder_col else pd.NA
    df["Årsak"] = df[arsak_col] if arsak_col else (
        df["Arsak"] if "Arsak" in df.columns else pd.NA)
    df["Utfall"] = df[utfall_col] if utfall_col else pd.NA
    df["Merkelappnummer"] = df[merk_col] if merk_col else pd.NA
    df["Fallvilt-ID"] = df[fvid_col] if fvid_col else (
        df["FallviltId"] if "FallviltId" in df.columns else pd.NA)

    # Riktig projeksjon: WGS84 -> UTM 33N (EPSG:32633)
    if lat_col and lon_col:
        def _proj_row(lat, lon):
            if pd.isna(lat) or pd.isna(lon):
                return pd.Series([pd.NA, pd.NA])
            try:
                e, n = wgs84_to_utm33(float(lat), float(lon))
                return pd.Series([e, n])
            except Exception:
                return pd.Series([pd.NA, pd.NA])

        utm = df[[lat_col, lon_col]].apply(
            lambda s: _proj_row(s[lat_col], s[lon_col]), axis=1)
        utm.columns = ["UTM33 øst", "UTM33 nord"]
        df["UTM33 øst"] = utm["UTM33 øst"]
        df["UTM33 nord"] = utm["UTM33 nord"]
    else:
        df["UTM33 øst"] = pd.NA
        df["UTM33 nord"] = pd.NA

    # Ikke fjern noen råkolonner fra API-responsen.
    # Sørg for at alle ønskede standardkolonner finnes (legg til som NaN hvis mangler).
    for col in desired:
        if col not in df.columns:
            df[col] = pd.NA

    # Legg til OppdatertDatoTid med dagens dato og tid
    df["OppdatertDatoTid"] = dt.datetime.now().isoformat()

    final_cols = list(df.columns)
    # Ensure desired columns appear at the end in desired order if they aren't already
    for col in desired:
        if col in final_cols:
            # move it to end preserving relative desired order
            final_cols.remove(col)
            final_cols.append(col)
        else:
            final_cols.append(col)

    out = df.reindex(columns=final_cols).copy()
    out.to_csv(out_csv, sep=";", index=False, encoding="utf-8")
    print(f"Skrev {len(out)} rader og {len(out.columns)} kolonner til {out_csv} (inkluderer alle opprinnelige API-kolonner)")

# ---------------------------
# CLI
# ---------------------------


def main():
    rows = paginate_all(
        fra_dato="2025-01-01",
        fylkesnr=50,
        page_size=1000,
        arsak="PåkjørtAvMotorkjøretøy",
    )

    to_csv_custom(rows, "fallvilt.csv")


if __name__ == "__main__":
    main()
