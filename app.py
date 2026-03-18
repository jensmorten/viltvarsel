import streamlit as st
import pandas as pd
from datetime import date, datetime
import numpy as np
from astral import LocationInfo
from astral.sun import elevation
from zoneinfo import ZoneInfo
import asyncio
from io import BytesIO
import json

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from streamlit.components.v1 import html

from functions import hent_alle_wkt, lag_felles_kart


# --------------------------------------------------
# Page config
# --------------------------------------------------

st.set_page_config(
    page_title="Dyrepåkøyrslar i Trøndelag – risikostrekninger",
    layout="wide"
)

# --------------------------------------------------
# Helpers
# --------------------------------------------------

def finn_årstid(dato):
    m = dato.month
    if m in (12, 1, 2):
        return "vinter"
    elif m in (3, 4, 5):
        return "vår"
    elif m in (6, 7, 8):
        return "sommar"
    else:
        return "haust"


def finn_lys(now):
    TRONDELAG = LocationInfo(
        name="Trøndelag",
        region="Norway",
        timezone="Europe/Oslo",
        latitude=63.4,
        longitude=10.4,
    )
    solhoyde = elevation(TRONDELAG.observer, now)
    if solhoyde > 12:
        return "dag"
    elif solhoyde > -12:
        return "skumring"
    else:
        return "natt"


def lag_aggregert_df(df, metric_col, top_n):
    return (
        df
        .groupby("Vegobjekt_540_id", as_index=False)
        .agg({
            "Vegnamn": "first",
            metric_col: "sum",
            "antall_kollisjoner": "sum",
            "ÅDT, total_avg": "mean",
            "Vegobjekt_540_lengde_avg": "mean",
            "UTM33_øst_int_avg": "mean",
            "UTM_nord_int_avg": "mean",
        })
        .sort_values(metric_col, ascending=False)
        .head(top_n)
    )


def klargjor_visning(df, metric_col, metric_label, include_art=True):
    df = df.copy()

    df["Vegobjekt_540_id"] = df["Vegobjekt_540_id"].astype("Int64")
    df["Ådt_avg"] = df["ÅDT, total_avg"].astype("Int64")
    df["Vegobjekt_540_lengde"] = df["Vegobjekt_540_lengde_avg"].astype("Int64")

    df["lenke"] = (
        "https://vegkart.atlas.vegvesen.no/#kartlag:geodata/@"
        + df["UTM33_øst_int_avg"].astype(str)
        + ","
        + df["UTM_nord_int_avg"].astype(str)
        + ",10/valgt:"
        + df["Vegobjekt_540_id"].astype(str)
        + ":540"
    )

    df = df.rename(columns={
        "Vegobjekt_540_id": "Veg_ID",
        "Ådt_avg": "ÅDT (Årsdøgntrafikk)",
        "Vegobjekt_540_lengde": "Lengde (m)",
        "antall_kollisjoner": "kollisjonar siste år",
        metric_col: metric_label
    })

    cols = [
        'Veg_ID', 'Vegnamn', 'lenke',
        'ÅDT (Årsdøgntrafikk)', 'Lengde (m)',
        'kollisjonar siste år'
    ]

    if include_art and "Art" in df.columns:
        cols.insert(3, "Art")

    if metric_label in df.columns:
        cols.append(metric_label)

    return df[cols].reset_index(drop=True)


def _fmt_dato(s):
    return pd.to_datetime(s).strftime("%d.%m.%y kl. %H:%M")


# --------------------------------------------------
# Time context
# --------------------------------------------------

DAGENS_ÅRSTID = finn_årstid(date.today())
LYSFORHOLD_NO = finn_lys(datetime.now(tz=ZoneInfo("Europe/Oslo")))

# --------------------------------------------------
# Load data (Azure)
# --------------------------------------------------

credential = ClientSecretCredential(
    tenant_id=st.secrets["Tenant_ID"],
    client_id=st.secrets["Client_ID"],
    client_secret=st.secrets["Client_secret_value"]
)

service_client = DataLakeServiceClient(
    "https://onelake.dfs.fabric.microsoft.com",
    credential=credential
)

fs = service_client.get_file_system_client("Viltmedaljong")

def read_file(path):
    file_client = fs.get_file_client(path)
    return file_client.download_file().readall()

df = pd.read_csv(
    BytesIO(read_file("vilt_lakehouse.lakehouse/Files/fallvilt/silver/fallvilt_silver.csv")),
    sep=";"
)

ARSTID_JUSTERING = json.loads(
    read_file("vilt_lakehouse.lakehouse/Files/fallvilt/silver/ARSTID_JUSTERING.json")
)

LYS_JUSTERING = json.loads(
    read_file("vilt_lakehouse.lakehouse/Files/fallvilt/silver/LYSJUSTERING.json")
)

METADATA = json.loads(
    read_file("vilt_lakehouse.lakehouse/Files/fallvilt/silver/metadata.json")
)

sist_oppdatert = _fmt_dato(METADATA['sist_oppdatert'])
første_kollisjon = _fmt_dato(METADATA['første_kollisjon'])
siste_kollisjon = _fmt_dato(METADATA['siste_kollisjon'])

# --------------------------------------------------
# Sidebar
# --------------------------------------------------

st.sidebar.title("Innstillinger")

metric_choice = st.sidebar.radio(
    "Vis etter:",
    ["Historisk frekvens", "Predikert frekvens"]
)

artsvalg = st.sidebar.multiselect(
    "Velg dyrearter:",
    ["Elg", "Hjort", "Rådyr"],
    default=["Elg", "Hjort", "Rådyr"]
)

top_n = st.sidebar.slider("Tal strekninger:", 5, 50, 10, step=5)

if not artsvalg:
    st.warning("Vel minst éin dyreart.")
    st.stop()

# --------------------------------------------------
# Data prep (single source of truth)
# --------------------------------------------------

df_base = df[df["Art"].isin(artsvalg)].copy()

df_base["predikert_risiko"] = (
    df_base["frekvens"]
    * ARSTID_JUSTERING[DAGENS_ÅRSTID]
    * LYS_JUSTERING[LYSFORHOLD_NO]
)

metric_col = (
    "frekvens" if metric_choice == "Historisk frekvens"
    else "predikert_risiko"
)

metric_label = (
    "Historisk frekvens (kollisjon per kjøretøy per år per 100 km)"
    if metric_choice == "Historisk frekvens"
    else "Predikert frekvens (kollisjon per kjøretøy per år per 100 km)"
)

# --------------------------------------------------
# Views
# --------------------------------------------------

df_top = df_base.sort_values(metric_col, ascending=False).head(top_n)
df_top_koll = df_base.sort_values("antall_kollisjoner", ascending=False).head(top_n)
df_top_sum = lag_aggregert_df(df_base, metric_col, top_n)

df_visning = klargjor_visning(df_top, metric_col, metric_label, include_art=True)
df_visning_koll = klargjor_visning(df_top_koll, metric_col, metric_label, include_art=True)
df_visning_sum = klargjor_visning(df_top_sum, metric_col, metric_label, include_art=False)

# --------------------------------------------------
# UI
# --------------------------------------------------

lokal_tid = datetime.now(ZoneInfo("Europe/Oslo"))
local_tid_str = lokal_tid.strftime('%Y-%m-%d %H:%M')

st.title("🫎⚠️ Dyrepåkøyrslar i Trøndelag")

faktor = np.round(
    ARSTID_JUSTERING[DAGENS_ÅRSTID] * LYS_JUSTERING[LYSFORHOLD_NO], 2
)

txt = ""
if metric_choice == "Predikert frekvens":
    txt = f"""
ℹ️ Justering aktiv. Lokal tid: {local_tid_str}.
Årstid: **{DAGENS_ÅRSTID}**, lys: **{LYSFORHOLD_NO}**
→ faktor: {faktor}
"""

st.markdown(
    f"""
**Viser topp {top_n} vegstrekningar**  
Sortert etter: **{metric_label}**  
Dyreartar: **{", ".join(artsvalg)}**  

{txt}

Data oppdatert {sist_oppdatert}  
Kollisjonar: {første_kollisjon} → {siste_kollisjon}
"""
)

st.dataframe(
    df_visning,
    column_config={
        "lenke": st.column_config.LinkColumn(
            "Vegkart",
            display_text="Opne i Vegkart"
        )
    },
    width="content",
    hide_index=True
)

# --------------------------------------------------
# Kart
# --------------------------------------------------

if "kart" not in st.session_state:
    st.session_state.kart = None

if st.button("Vis kart"):
    with st.spinner("Hentar veggeometri frå NVDB …"):
        veg_ids = df_visning["Veg_ID"].dropna().astype(str).tolist()
        wkt_dict = asyncio.run(hent_alle_wkt(veg_ids))

        risiko_dict = dict(
            zip(df_visning["Veg_ID"].astype(str), df_visning[metric_label])
        )

        st.session_state.kart = lag_felles_kart(wkt_dict, risiko_dict)

if st.session_state.kart is not None:
    html(
        st.session_state.kart.get_root().render(),
        height=1200,
        width=1800
    )

# --------------------------------------------------
# Info
# --------------------------------------------------

with st.expander("ℹ️ Om tala"):
    st.markdown(
        """
        Historisk frekvens (per 100 km)  
        = observerte dyrepåkjørsler normaliserte for trafikkmengde og veglengd,  
        uttrykt som forventa tal på kollisjonar per 100 køyretøykilometer per år.

        Dette gir eit mål på grunnrisiko per køyrelengd for ein enkelt bil,
        og gjer det mogleg å samanlikne risiko mellom ulike vegstrekningar
        uavhengig av trafikkmengde og lengd.

        Samanlikning med yrkesrisiko (illustrativ) 
        For å gi eit meir intuitivt risikobilete er frekvensen omrekna til
        årleg risiko per bil, basert på ein føresetnad om årleg køyrelengd på 15000 km og at ein kollisjon i snitt gir same konsekvens som ei arbeidslulukke.
        Denne årsrisikoen blir samanlikna med melde arbeidsulukker
        (med fråver) per årsverk i ulike yrke (tal og kategoriar frå SSB). Så kolonna betyr "å kjøre 15000 km på denne strekninga vil gi om lag same risiko som å jobbe eit årsverk i dette yrket". Samanlikninga er meint som ei grovt illustrativ skala basert på desse føresetnadane. 

        Predikert frekvens er historisk grunnfrekvens justert med ein årstidsfaktor og faktor for lysforhald, estimert frå ein statistisk modell
        (Negativ binomial-regresjon) basert på observerte dyrepåkjørsler.
        """
    )
