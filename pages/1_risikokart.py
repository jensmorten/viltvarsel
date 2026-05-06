import streamlit as st
import pandas as pd
import asyncio

from functions import hent_alle_wkt, lag_felles_kart

# --------------------------------------------------
# Sideoppsett
# --------------------------------------------------

st.set_page_config(
    page_title="Risikokart",
    layout="wide"
)

st.title("🗺️ Risikokart – dyrepåkjørslar")
st.markdown(
    "Utforsk korleis risiko varierer med **dyreart, årstid og lysforhold**."
)

# --------------------------------------------------
# Last data
# --------------------------------------------------

#@st.cache_data
def load_data():
    df=pd.read_csv("data/frekvens_årstid.csv", sep=",")
    #print(df.columns)
    return df
    
df = load_data()

# --------------------------------------------------
# Sidebar – brukarval
# --------------------------------------------------

st.sidebar.header("Innstilling for scenario")

artsvalg = st.sidebar.multiselect(
    "Dyreart",
    options=sorted(df["Art"].unique()),
    default=sorted(df["Art"].unique())
)
ÅRSTIDER = ["vinter", "vår", "sommar", "haust"]   # biologisk syklus
LYS = ["dag", "skumring", "natt"]                  # døgn

årstid = st.sidebar.select_slider(
    "Årstid",
    options=ÅRSTIDER,
    value="sommar"
)

lys = st.sidebar.select_slider(
    "Lysforhold",
    options=LYS,
    value="dag"
)

# --------------------------------------------------
# Valider input
# --------------------------------------------------

if not artsvalg:
    st.warning("Vel minst éin dyreart.")
    st.stop()

# --------------------------------------------------
# Filtrer data
# --------------------------------------------------


df_filt = df[
    (df["Art"].isin(artsvalg)) &
    (df["årstid"] == årstid) &
    (df["lysforhold"] == lys)
].copy()

#print(df_filt)

#if df_filt.empty:
#    st.warning("Ingen data for dette valet.")
#    st.stop()

# --------------------------------------------------
# Aggreger per vegstrekning
# --------------------------------------------------
df_filt["Vegobjekt_540_id"] = df_filt["Vegobjekt_540_id"].astype("Int64")

df_risiko = (
    df_filt
    .groupby("Vegobjekt_540_id", as_index=False)
    .agg({
        "frekvens": "sum",
        "antall_kollisjoner": "sum",
        "UTM_nord_int_avg": "mean",
        "UTM33_øst_int_avg": "mean",
    })
    .sort_values("frekvens", ascending=False)
)

st.markdown(
    f"""
    **Val:**  
    Art: **{", ".join(artsvalg)}**  
    Årstid: **{årstid}**  
    Lysforhold: **{lys}**  

    **Viser {len(df_risiko)} vegstrekningar**
    """
)

# --------------------------------------------------
# Klargjer kartdata
# --------------------------------------------------

#veg_ids = df_risiko["Vegobjekt_540_id"].dropna().astype("Int64").astype(str).tolist()

veg_ids = (
            df_risiko["Vegobjekt_540_id"]
            .dropna()
            .astype(str)
            .tolist()
        )

if not veg_ids:
    st.warning("Ingen vegstrekningar å vise på kartet.")
    st.stop()

with st.spinner("Hentar veggeometri frå NVDB …"):
    wkt_dict = asyncio.run(hent_alle_wkt(veg_ids))

# Dersom ingen geometriar
if not any(wkt_dict.values()):
    st.warning("Fann ingen gyldige veggeometriar for dette valet.")
    st.stop()

risiko_dict = dict(
    zip(
        df_risiko["Vegobjekt_540_id"].astype(str),
        df_risiko["frekvens"]
    )
)

# --------------------------------------------------
# Lag kart
# --------------------------------------------------

kart = lag_felles_kart(wkt_dict, risiko_dict)

if kart is None:
    st.warning("Klarte ikkje å lage kart for dette valet.")
    st.stop()

# --------------------------------------------------
# Vis kart
# --------------------------------------------------

st.components.v1.html(
    kart.get_root().render(),
    height=1200,
    width=1800
)

# --------------------------------------------------
# Forklaring
# --------------------------------------------------

with st.expander("ℹ️ Om risikokartet"):
    st.markdown(
        """
        Kartet viser **summert historisk risiko** for dyrepåkjørslar
        per vegstrekning, gitt val av dyreart, årstid og lysforhold.

        Risikoen er basert på observerte kollisjonar, normalisert
        for trafikkmengde og veglengd, og eignar seg til å samanlikne
        **relativ risiko** mellom vegstrekningar.

        Kombinasjonar med få treff (til dømes *dag + sommar*)
        vil naturleg gi færre eller ingen vegstrekningar.
        """
    )
