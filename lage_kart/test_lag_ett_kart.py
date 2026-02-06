# --- Biblioteker ---
import folium
from pyproj import Transformer
import branca.colormap as cm
import asyncio
import httpx
from typing import Optional, Dict

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

# --- 1) Parse WKT (enkel parser for LINESTRING og LINESTRING Z) ---
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

# hent wkt med hent_wkt_for_objekt funksjonen

async def main():
    """Main async function to fetch WKT and create map"""
    async with httpx.AsyncClient() as client:
        # Hent WKT for vegobjektet
        wkt = await hent_wkt_for_objekt(client, str(vegobjekt_540_id))
        
        if not wkt:
            print(f"Feil: Kunne ikke hente WKT for objekt {vegobjekt_540_id}")
            return

        # --- 1) Parse WKT ---
        pts_xyz = parse_linestring_wkt(wkt)

        # --- 2) Transformér til WGS84 (EPSG:4326) ---
        transformer = Transformer.from_crs(src_epsg, 4326, always_xy=True)
        lonlatz = [(*transformer.transform(x, y), z) for (x, y, z) in pts_xyz]
        lats_lons = [(lat, lon) for (lon, lat, z) in lonlatz]
        zs = [z for (_, _, z) in lonlatz if z is not None]

        # --- 3) Lag Folium-kart ---
        mid = lats_lons[len(lats_lons)//2]
        m = folium.Map(location=mid, zoom_start=12, tiles="OpenStreetMap")

        folium.PolyLine(lats_lons, color="red", weight=4, opacity=0.9).add_to(m)
        folium.Marker(lats_lons[0], tooltip="Start").add_to(m)
        folium.Marker(lats_lons[-1], tooltip="Slutt").add_to(m)
        m.fit_bounds(lats_lons)

        # --- 4) Farge etter høyde Z ---
        if zs:
            m = folium.Map(location=mid, zoom_start=12, tiles="OpenStreetMap")
            vmin, vmax = min(zs), max(zs)
            cmap = cm.LinearColormap(["#2c7bb6", "#abd9e9", "#ffffbf", "#fdae61", "#d7191c"], vmin=vmin, vmax=vmax)
            cmap.caption = "Høyde (meter)"
            cmap.add_to(m)

            for i in range(len(lats_lons)-1):
                (lat1, lon1), (lat2, lon2) = lats_lons[i], lats_lons[i+1]
                z1 = lonlatz[i][2]; z2 = lonlatz[i+1][2]
                z_mid = None
                if z1 is not None and z2 is not None:
                    z_mid = 0.5*(z1 + z2)
                color = cmap(z_mid) if z_mid is not None else "gray"
                folium.PolyLine([(lat1, lon1), (lat2, lon2)], color=color, weight=5, opacity=0.95).add_to(m)

            folium.Marker(lats_lons[0], tooltip=f"Start ({zs[0]:.1f} m)").add_to(m)
            folium.Marker(lats_lons[-1], tooltip=f"Slutt ({zs[-1]:.1f} m)").add_to(m)
            m.fit_bounds(lats_lons)

        # Lagre som HTML
        m.save("veg_trondelag.html")
        print("Ferdig! Åpne filen 'veg_trondelag.html' i nettleseren.")

if __name__ == "__main__":
    asyncio.run(main())