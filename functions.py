
from astral import LocationInfo
from astral.sun import sun
import pandas as pd
from astral.sun import elevation
#import statsmodels.formula.api as smf
#import statsmodels.api as sm
import numpy as np
#from streamlit_folium import st_folium
from pyproj import Transformer
import branca.colormap as cm
import asyncio
import httpx
import folium   # ← DENNE mangla
from typing import Optional, Dict

def map_arsrisiko_til_yrke(arsrisiko):
    """
    Mapper årsrisiko (per årsverk) til illustrativ yrkessammenlikning.

    Parametre
    ----------
    arsrisiko : float
        Risiko per årsverk (f.eks. 0.012 = 12 per 1000)
    ----------
    str : tekst for illustrativ samanlikning
    """

    yrkesdf = pd.DataFrame({
    "yrkesgruppe": [
        "Leiarar",
        "Høgskuleyrke",
        "Kontoryrke",
        "Sals- og serviceyrke",
        "Renhaldarar, hjelpearbeidarar mv.",
        "Bønder, fiskarar mv.",
        "Prosess- og maskinoperatørar, transportarbeidarar mv.",
        "Håndverkarar",
    ],
    "ulykker_per_arsverk": [
        4.0 / 1000,
        6.5 / 1000,
        7.7 / 1000,
        13.9 / 1000,
        15.5 / 1000,
        19.2 / 1000,
        20.1 / 1000,
        20.5 / 1000,
    ],
    "intervall_låg": [
        3.0 / 1000,
        5.0 / 1000,
        7.0 / 1000,
        12.0 / 1000,
        14.5 / 1000,
        17.5 / 1000,
        19.6 / 1000,
        20.2 / 1000,
    ],
    "intervall_høg": [
        5.0 / 1000,
        7.0 /1000,
        12.0 / 1000,
        14.5 / 1000,
        17.5 / 1000,
        19.6 / 1000,
        20.2 / 1000,
        20.7 /1000
    ]
    
    })

    # Sorter for tryggleik (lav → høg risiko)
    yrkesdf = yrkesdf.sort_values("intervall_låg")

    # Lågare enn lågaste kjente yrke (Ledere)
    if arsrisiko < yrkesdf["intervall_låg"].min():
        return "lågare risiko enn dei fleste yrke"

    # Høgare enn høgaste kjente yrke (Håndverkere)
    if arsrisiko > 10* yrkesdf["intervall_høg"].max():
        return ">10x høgare risiko enn høgrisikoyrke"
    elif arsrisiko > 5* yrkesdf["intervall_høg"].max():
        return ">5x høgare risiko enn høgrisikoyrke"
    elif arsrisiko > 2* yrkesdf["intervall_høg"].max():
        return ">2x høgare risiko enn høgrisikoyrke"
    elif arsrisiko > yrkesdf["intervall_høg"].max():
        return "1-2x høgare risiko enn høyrisikoyrke"
    
    # Finn intervall som treff
    treff = yrkesdf[
        (yrkesdf["intervall_låg"] <= arsrisiko) &
        (yrkesdf["intervall_høg"] >= arsrisiko)
    ]

    if not treff.empty:
        # Dersom fleire treff (overlapp), vel nærmaste midtpunkt
        treff = treff.copy()
        treff["avstand"] = (treff["ulykker_per_arsverk"] - arsrisiko).abs()
        return treff.sort_values("avstand").iloc[0]["yrkesgruppe"]

    # Fallback (burde eigentleg ikkje skje)
    return "Ukjend risikonivå"


def lyskategori_fra_tidspunkt(ts):
    TRONDELAG = LocationInfo(
    name="Trøndelag",
    region="Norway",
    timezone="Europe/Oslo",
    latitude=63.4,
    longitude=10.4,
    )
    if pd.isna(ts):
        return None

    # Bruk solhøgde (grader over/under horisont)
    solhoyde = elevation(TRONDELAG.observer, ts)
    if solhoyde > 12:
        return "Dag"
    elif solhoyde >-12:
        return "Skumring"
    else:
        return "Natt"
    

def maaned_til_arstid(m):
    if m in [12, 1, 2]:
        return "vinter"
    elif m in [3, 4, 5]:
        return "vår"
    elif m in [6, 7, 8]:
        return "sommar"
    else:
        return "haust"
    

def lag_arstidsjustering(model, referanse="haust"):
    """
    Lag justeringsfaktorar for årstid frå ein statsmodels GLM (NB / Poisson).

    Referansekategori får faktor 1.0.
    """
    params = model.params

    # Finn alle årstids-koeffisientar
    arstid_params = {
        k: v for k, v in params.items()
        if k.startswith("C(årstid)")
    }

    # Referanse (den som ikkje er i params)
    arstid_justering = {referanse: 1.0}

    # Legg til dei estimerte årstidene
    for k, beta in arstid_params.items():
        # Trekk ut årstidsnamn, t.d. C(årstid)[T.Sommar] -> Sommar
        arstid = k.split("[T.")[1].rstrip("]")
        arstid_justering[arstid] = np.round(float(np.exp(beta)),2)

    return arstid_justering

def lag_lysjustering(model, referanse="Dag", damping=1, normaliser=False):
    """
    Lag justeringsfaktorar for lysforhold frå GLM,
    med valfri damping for å unngå overtolking.
    """

    params = model.params
    lys_justering = {referanse: 1.0}

    for k, beta in params.items():
        if k.startswith("C(lyskategori)[T."):
            lys = k.split("[T.")[1].rstrip("]")
            # Demp effekten
            beta_dempet = damping * beta
            lys_justering[lys] = np.round(float(np.exp(beta_dempet)))

    if normaliser:
        mean_factor = np.mean(list(lys_justering.values()))
        lys_justering = {
            k: v / mean_factor for k, v in lys_justering.items()
        }

    return lys_justering

# Felles NVDB-headers
headers = {
    "Accept": "application/json",
    "User-Agent": "nvdb-wkt-fetcher/1.0",
    "X-Client": "nvdb-wkt-fetcher",
}

# --- Konfigurasjon ---
vegobjekt_540_id = 1024046936

# Sett kilde-CRS her: UTM sone 33N (Trøndelag). Om nødvendig, bytt til 32632.
src_epsg = 32633

# Konstanter
VEGOBJEKT_TYPE_ID = 540
REQUEST_TIMEOUT = 20.0
RETRY_BACKOFF = [0.5, 1.0, 2.0]
MAX_CONCURRENCY = 16

sem = asyncio.Semaphore(MAX_CONCURRENCY)

# Cache: objekt_id -> wkt-streng
wkt_cache: Dict[str, str] = {}

def parse_linestring_wkt(wkt_text):
    w = wkt_text.strip()
    # Finn innholdet innenfor første par med parenteser
    start = w.find('(')
    end = w.rfind(')')
    if start == -1 or end == -1:
        raise ValueError("Ugyldig WKT: mangler parenteser")
    body = w[start+1:end].strip()

    coords = []
    for part in body.split(','):
        nums = part.strip().split()
        if len(nums) < 2:
            continue
        x = float(nums[0]); y = float(nums[1])
        z = float(nums[2]) if len(nums) >= 3 else None
        coords.append((x, y, z))
    return coords

async def hent_wkt_for_objekt(client: httpx.AsyncClient, objekt_id: str) -> str:
    """
    Henter WKT-geometri for et vegobjekt (type 540) fra NVDB API LES.
    Returnerer tom streng dersom data mangler eller ikke finnes.
    """
    if not objekt_id:
        return ""

    if objekt_id in wkt_cache:
        return wkt_cache[objekt_id]

    url = f"https://nvdbapiles.atlas.vegvesen.no/vegobjekter/{VEGOBJEKT_TYPE_ID}/{objekt_id}"

    attempts = len(RETRY_BACKOFF) + 1

    for i in range(attempts):
        try:
            async with sem:
                resp = await client.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 200:
                data = resp.json()

                # Primært: geometri.wkt (øverst i JSON)
                geom = data.get("geometri", {})
                wkt = geom.get("wkt")

                # Sekundært fallback: lokasjon.geometri.wkt
                if not wkt:
                    lok = data.get("lokasjon", {})
                    lok_geom = lok.get("geometri", {})
                    wkt = lok_geom.get("wkt", "")

                wkt = wkt or ""
                wkt_cache[objekt_id] = wkt
                return wkt

            elif 500 <= resp.status_code < 600:
                if i < attempts - 1:
                    await asyncio.sleep(RETRY_BACKOFF[i])
                    continue
                else:
                    return ""
            else:
                return ""

        except (httpx.HTTPError, asyncio.TimeoutError):
            if i < attempts - 1:
                await asyncio.sleep(RETRY_BACKOFF[i])
                continue
            return ""

    return ""

def lag_felles_kart(wkt_dict, risiko_dict, src_epsg=32633):
    transformer = Transformer.from_crs(src_epsg, 4326, always_xy=True)

    risikoar = [v for v in risiko_dict.values() if v is not None]
    vmin, vmax = min(risikoar), max(risikoar)

    cmap = cm.LinearColormap(
        colors=["#2c7bb6", "#ffffbf", "#d7191c"],
        vmin=vmin,
        vmax=vmax,
    )
    cmap.caption = "Risiko (frekvens)"

    m = None
    alle_punkt = []

    veglinjer = folium.FeatureGroup(name="Vegstrekningar")
    id_labels = folium.FeatureGroup(name="Veg-ID")

    for veg_id, wkt in wkt_dict.items():
        if not wkt:
            continue

        pts_xyz = parse_linestring_wkt(wkt)
        lonlat = [transformer.transform(x, y) for (x, y, _) in pts_xyz]
        latlon = [(lat, lon) for (lon, lat) in lonlat]
        alle_punkt.extend(latlon)

        risiko = risiko_dict.get(veg_id)
        color = cmap(risiko) if risiko is not None else "gray"

        if m is None:
            m = folium.Map(location=latlon[len(latlon)//2], zoom_start=10)

        # Veglinje
        folium.PolyLine(
            latlon,
            color=color,
            weight=5,
            opacity=0.9,
            tooltip=f"Veg_ID {veg_id}<br>Risiko: {risiko:.2E}"
        ).add_to(veglinjer)

        # ID-etikett
        mid_idx = len(latlon) // 2
        folium.Marker(
            location=latlon[mid_idx],
            icon=folium.DivIcon(
                html=f"""
                <div style="
                    font-size: 11px;
                    color: black;
                    background-color: rgba(255,255,255,0.8);
                    padding: 2px 4px;
                    border-radius: 4px;
                    border: 1px solid #999;
                    white-space: nowrap;
                ">
                    {veg_id}
                </div>
                """
            )
        ).add_to(id_labels)

    if m and alle_punkt:
        veglinjer.add_to(m)
        id_labels.add_to(m)
        cmap.add_to(m)
        m.fit_bounds(alle_punkt)
        folium.LayerControl(collapsed=False).add_to(m)

    return m

async def hent_alle_wkt(veg_ids):
    async with httpx.AsyncClient() as client:
        tasks = [
            hent_wkt_for_objekt(client, str(vid))
            for vid in veg_ids
        ]
        wkts = await asyncio.gather(*tasks)

    return dict(zip(veg_ids, wkts))