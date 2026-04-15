import streamlit as st
import pandas as pd
from datetime import date, datetime
import numpy as np
from astral import LocationInfo
from astral.sun import elevation
from zoneinfo import ZoneInfo
import asyncio
from streamlit_folium import st_folium
#import folium
import os
import json
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO

from streamlit.components.v1 import html

from functions import (
    hent_alle_wkt,
    lag_felles_kart
)

st.set_page_config(
    page_title="Dyrepåkøyrslar i Trøndelag  – risikostrekninger",
    layout="wide"
)


# --------------------------------------------------
# Årstid frå dato
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


#ARSTID_JUSTERING = {
#    "haust": 1.00,
#    "vinter": 0.98,
#    "vår": 0.85,
#    "sommar": 0.79,
#}

DAGENS_ÅRSTID = finn_årstid(date.today())

#LYS_JUSTERING = {
#    'dag': 1.0, 
#    'natt': 1.09, 
#    'skumring': 1.08}

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
    elif solhoyde >-12:
        return "skumring"
    else:
        return "natt"
    
LYSFORHOLD_NO = finn_lys(datetime.now(tz=ZoneInfo("Europe/Oslo"))) 

# --------------------------------------------------
# Data loading
# --------------------------------------------------

#@st.cache_data
#def load_data():
#    df = pd.read_csv("frekvens_final.csv", encoding="utf-8")
#    return df


credential = ClientSecretCredential(
    tenant_id=st.secrets["Tenant_ID"],
    client_id=st.secrets["Client_ID"],
    client_secret=st.secrets["Client_secret_value"]
)

account_url = "https://onelake.dfs.fabric.microsoft.com"
service_client = DataLakeServiceClient(account_url, credential=credential)

file_system_client = service_client.get_file_system_client("Viltmedaljong")

file_client = file_system_client.get_file_client(
    "vilt_lakehouse.lakehouse/Files/fallvilt/silver/fallvilt_silver.csv"
)

try:
    download = file_client.download_file()
    data = download.readall()
    df = pd.read_csv(BytesIO(data), sep=";")
except:
    df = pd.read_csv("frekvens_script.csv", encoding="utf-8")

try:
    file_client_arstid = file_system_client.get_file_client(
        "vilt_lakehouse.lakehouse/Files/fallvilt/silver/ARSTID_JUSTERING.json"
        )
    download_arstid = file_client_arstid.download_file()
    data_arstid = download_arstid.readall()
    ARSTID_JUSTERING = json.loads(data_arstid)
except:
    ARSTID_JUSTERING = {
    "haust": 1.00,
    "vinter": 0.98,
    "vår": 0.85,
    "sommar": 0.79,
    }

try:
    # LYSJUSTERING.json
    file_client_lys = file_system_client.get_file_client(
        "vilt_lakehouse.lakehouse/Files/fallvilt/silver/LYSJUSTERING.json"
    )
    download_lys = file_client_lys.download_file()
    data_lys = download_lys.readall()
    LYS_JUSTERING = json.loads(data_lys)
except:
    LYS_JUSTERING = {
    'dag': 1.0, 
    'natt': 1.09, 
    'skumring': 1.08
    }

try:# metadata.json
    file_client_meta = file_system_client.get_file_client(
        "vilt_lakehouse.lakehouse/Files/fallvilt/silver/metadata.json"
    )

    download_meta = file_client_meta.download_file()
    meta_data = download_meta.readall()

    METADATA = json.loads(meta_data)

except:
    METADATA = {
    'sist_oppdatert': "01.01.2025 kl. 12:00" , 
    'første kollisjon': "01.01.2025 kl. 12:00", 
    'siste_kollisjon': "31.01.2025 kl. 12:00"
    }


def _fmt_dato(s):
    return pd.to_datetime(s).strftime("%d.%m.%y kl. %H:%M")

sist_oppdatert  = _fmt_dato(METADATA['sist_oppdatert'])
første_kollisjon = _fmt_dato(METADATA['første_kollisjon'])
siste_kollisjon  = _fmt_dato(METADATA['siste_kollisjon'])

# --------------------------------------------------
# Sidebar – brukarval
# --------------------------------------------------


st.sidebar.title("Innstillinger")

metric_choice = st.sidebar.radio(
    "Vis etter:",
    options=["Historisk frekvens", "Predikert frekvens"]
)


artsvalg = st.sidebar.multiselect(
    "Velg dyrearter:",
    options=["Elg", "Hjort", "Rådyr"],
    default=["Elg", "Hjort", "Rådyr"]
)

top_n = st.sidebar.slider(
    "Tal strekninger:",
    min_value=5,
    max_value=50,
    value=10,
    step=5
)

st.sidebar.markdown("""
🫎⚠️Viltfrekvens – dyrepåkjørsler per vegstrekning. Dette konseptutprøvingsprosjektet (PoC) utviklar ein datadreven 
modell for risiko for dyrepåkjørsler på norske vegstrekningar. Målet er å gi eit enkelt, samanliknbart risikomål 
per vegstrekning, og justere risikomålet for situasjonsbaserte variablar (årstid, lysforhald m.m.) 
som kan nyttast i sanntid i bil (infotainment / varsling).
<br>
<br>                    
ℹ️<a href= "https://github.com/jensmorten/viltvarsel/blob/main/README.md">Les meir om prosjektet</a>"""
,unsafe_allow_html=True)

# --------------------------------------------------
# Valider input
# --------------------------------------------------

if not artsvalg:
    st.warning("Vel minst éin dyreart.")
    st.stop()

# --------------------------------------------------
# Filtrer data
# --------------------------------------------------

df_filt = df[df["Art"].isin(artsvalg)].copy()

df_filt["predikert_risiko"] = (
    df_filt["frekvens"]
    * ARSTID_JUSTERING[DAGENS_ÅRSTID]
    * LYS_JUSTERING[LYSFORHOLD_NO]
)

if metric_choice == "Historisk frekvens":
    metric_col = "frekvens"
    metric_label = "Historisk frekvens (kollisjon per kjøretøy per år per 100 km)"
else:
    metric_col = "predikert_risiko"
    metric_label = "Predikert frekvens (kollisjon per kjøretøy per år per 100 km)"

df_top = (
    df_filt
    .sort_values(metric_col, ascending=False)
    .head(top_n)
)

df_top_kollisjon = (
    df_filt
    .sort_values('antall_kollisjoner', ascending=False)
    .head(top_n)
)

df_top_sum=df_filt[['Vegobjekt_540_id','Vegnamn',metric_col,'antall_kollisjoner','ÅDT, total_avg','Vegobjekt_540_lengde_avg','UTM33_øst_int_avg', 'UTM_nord_int_avg']].copy()

df_top_sum = (
    df_filt
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


# --------------------------------------------------
# Hovudvisning
# --------------------------------------------------

lokal_tid = datetime.now(ZoneInfo("Europe/Oslo"))

local_tid_str=lokal_tid.strftime('%Y-%m-%d %H:%M')

st.title("🫎⚠️ Dyrepåkøyrslar i Trøndelag")

faktor= np.round(ARSTID_JUSTERING[DAGENS_ÅRSTID] * LYS_JUSTERING[LYSFORHOLD_NO],2)

if metric_choice=="Predikert frekvens":
    txt= f"ℹ️ Justering av frekvens er aktiv. Lokal dato og tid er {local_tid_str}. Årstid **{DAGENS_ÅRSTID}** og lysforhald **{LYSFORHOLD_NO}** gir total justeringfaktor på {faktor} (samanlikna med haust/dag)"
else:
    txt = ""
print(datetime.now(ZoneInfo("Europe/Oslo")).tzname()) 



st.markdown(
    f"""
    **Viser topp {top_n} vegstrekningar**  
    Sortert etter: **{metric_label}**  
    Dyreartar: **{", ".join(artsvalg)}** \n
    {txt} \n
    Data er oppdatert {sist_oppdatert} og inneheld kollisjonar mellom {første_kollisjon} og {siste_kollisjon}. \n
    """
)

df_visning = df_top.copy()
df_visning_koll = df_top_kollisjon.copy()
df_visning_sum = df_top_sum.copy()

# -----------------------------
# Rydd datatypar for visning
# -----------------------------

df_visning["Vegobjekt_540_id"] = df_visning["Vegobjekt_540_id"].astype("Int64")
df_visning_koll["Vegobjekt_540_id"] = df_visning_koll["Vegobjekt_540_id"].astype("Int64")
df_visning_sum["Vegobjekt_540_id"] = df_visning_sum["Vegobjekt_540_id"].astype("Int64")

df_visning["Ådt_avg"] = df_visning["ÅDT, total_avg"].astype("Int64")
df_visning_koll["Ådt_avg"] = df_visning_koll["ÅDT, total_avg"].astype("Int64")
df_visning_sum["Ådt_avg"] = df_visning_sum["ÅDT, total_avg"].astype("Int64")

df_visning["Vegobjekt_540_lengde"] = df_visning["Vegobjekt_540_lengde_avg"].astype("Int64")
df_visning_koll["Vegobjekt_540_lengde"] = df_visning_koll["Vegobjekt_540_lengde_avg"].astype("Int64")
df_visning_sum["Vegobjekt_540_lengde"] = df_visning_sum["Vegobjekt_540_lengde_avg"].astype("Int64")

# -----------------------------
# Lag Vegkart-lenke
# -----------------------------

df_visning["lenke"] = (
    "https://vegkart.atlas.vegvesen.no/#kartlag:geodata"
    "/@"
    + df_visning["UTM33_øst_int_avg"].astype(str)
    + ","
    + df_visning["UTM_nord_int_avg"].astype(str)
    + ",10/valgt:"
    + df_visning["Vegobjekt_540_id"].astype(str)
    + ":540"
)

df_visning_koll["lenke"] = (
    "https://vegkart.atlas.vegvesen.no/#kartlag:geodata"
    "/@"
    + df_visning_koll["UTM33_øst_int_avg"].astype(str)
    + ","
    + df_visning_koll["UTM_nord_int_avg"].astype(str)
    + ",10/valgt:"
    + df_visning_koll["Vegobjekt_540_id"].astype(str)
    + ":540"
)

df_visning_sum["lenke"] = (
    "https://vegkart.atlas.vegvesen.no/#kartlag:geodata"
    "/@"
    + df_visning_sum["UTM33_øst_int_avg"].astype(str)
    + ","
    + df_visning_sum["UTM_nord_int_avg"].astype(str)
    + ",10/valgt:"
    + df_visning_sum["Vegobjekt_540_id"].astype(str)
    + ":540"
)


# -----------------------------
# Gi pene kolonnenamn
# -----------------------------

df_visning = df_visning.rename(columns={
    "Vegobjekt_540_id": "Veg_ID",
    "Ådt_avg": "ÅDT (Årsdøgntrafikk)",
    "Vegobjekt_540_lengde": "Lengde (m)",
    "antall_kollisjoner": "kollisjonar siste år",
    "samanlikning_yrke": "Samanlikning med risiko i yrke",
    metric_col: metric_label
}).copy()

df_visning_koll = df_visning_koll.rename(columns={
    "Vegobjekt_540_id": "Veg_ID",
    "Ådt_avg": "ÅDT (Årsdøgntrafikk)",
    "Vegobjekt_540_lengde": "Lengde (m)",
    "antall_kollisjoner": "kollisjonar siste år",
    "samanlikning_yrke": "Samanlikning med risiko i yrke",
    metric_col: metric_label
}).copy()

df_visning_sum = df_visning_sum.rename(columns={
    "Vegobjekt_540_id": "Veg_ID",
    "Ådt_avg": "ÅDT (Årsdøgntrafikk)",
    "Vegobjekt_540_lengde": "Lengde (m)",
    "antall_kollisjoner": "kollisjonar siste år",
    "samanlikning_yrke": "Samanlikning med risiko i yrke",
    metric_col: metric_label
}).copy()


#print(df_visning.columns)

#df_visning=df_visning[['Veg_ID', 'Art', 'ÅDT (Årsdøgntrafikk)', 'Lengde (m)','frekvens','lenke']].copy()
df_visning = df_visning[
    ['Veg_ID', 'Vegnamn', 'lenke', 'Art', 'ÅDT (Årsdøgntrafikk)', 'Lengde (m)', 'kollisjonar siste år', metric_label,'Samanlikning med risiko i yrke']
].copy()

df_visning_koll = df_visning_koll[
    ['Veg_ID', 'Vegnamn', 'lenke', 'Art', 'ÅDT (Årsdøgntrafikk)', 'Lengde (m)', 'kollisjonar siste år']
].copy()

df_visning_sum = df_visning_sum[
    ['Veg_ID', 'Vegnamn', 'lenke', 'ÅDT (Årsdøgntrafikk)', 'Lengde (m)', 'kollisjonar siste år', metric_label]
].copy()


# -----------------------------
# Styling
# -----------------------------

df_visning = df_visning.reset_index(drop=True)
df_visning_koll = df_visning_koll.reset_index(drop=True)
df_visning_sum = df_visning_sum.reset_index(drop=True)

styled_df = df_visning.style.format({
    metric_label: "{:.2E}",
    "ÅDT (Årsdøgntrafikk)": "{:.0f}",
    "Lengde (m)": "{:.0f}",
})

styled_df_koll = df_visning_koll.style.format({
    metric_label: "{:.2E}",
    "ÅDT (Årsdøgntrafikk)": "{:.0f}",
    "Lengde (m)": "{:.0f}",
})

styled_df_sum = df_visning_sum.style.format({
    metric_label: "{:.2E}",
    "ÅDT (Årsdøgntrafikk)": "{:.0f}",
    "Lengde (m)": "{:.0f}",
})


st.dataframe(
    styled_df,
    column_config={
        "lenke": st.column_config.LinkColumn(
            "Vegkart",
            display_text="Opne i Vegkart"
        )
    },
    width="content",
    hide_index=True
)

# st.markdown(
#     f"""
#     **Viser {top_n} vegstrekningar**  
#     Sortert etter: **Historisk frekvens siste år, alle valde dyreartar**  
#     Dyreartar: **{", ".join(artsvalg)}**
#     """
# )

# st.dataframe(
#     styled_df_sum,
#     column_config={
#         "lenke": st.column_config.LinkColumn(
#             "Vegkart",
#             display_text="Opne i Vegkart"
#         )
#     },
#     width="content",
#     hide_index=True
# )

# st.markdown(
#     f"""
#     **Viser {top_n} vegstrekningar**  
#     Sortert etter: **Antal kollisjonar siste år**  
#     Dyreartar: **{", ".join(artsvalg)}**
#     """
# )

# st.dataframe(
#     styled_df_koll,
#     column_config={
#         "lenke": st.column_config.LinkColumn(
#             "Vegkart",
#             display_text="Opne i Vegkart"
#         )
#     },
#     width="content",
#     hide_index=True
# )



risiko_dict = dict(
    zip(
        df_visning["Veg_ID"].astype(str),
        df_visning[metric_label]
    )
)

# Initier kart i session_state
if "kart" not in st.session_state:
    st.session_state.kart = None

# Knapp: lag kartet
if st.button("Vis kart"):
    with st.spinner("Hentar veggeometri frå NVDB …"):
        veg_ids = (
            df_visning["Veg_ID"]
            .dropna()
            .astype(str)
            .tolist()
        )

        wkt_dict = asyncio.run(hent_alle_wkt(veg_ids))

        risiko_dict = dict(
            zip(
                df_visning["Veg_ID"].astype(str),
                df_visning[metric_label]
            )
        )

        # LAG kartet og lagre i session_state
        st.session_state.kart = lag_felles_kart(
            wkt_dict,
            risiko_dict
        )

# Vis kartet dersom det finst
# Vis kartet dersom det finst
if st.session_state.kart is not None:
    html(
        st.session_state.kart.get_root().render(),
        height=1200,
        width=1800
    )
# --------------------------------------------------
# Enkel forklaring
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
