import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Dyrepåkjørsler – risikostrekninger",
    layout="wide"
)

# -------------------------------------------------------------------
# DATA – les rådata og forbered kolonner
# -------------------------------------------------------------------
@st.cache_data
def load_raw_data():
    df = pd.read_csv(
        "Fallvilt_trdlag_2016-2026_adttotallengder.csv",
        sep=";",
        low_memory=False
    )

    # Konverter dato
    try:
        df["Dato"] = pd.to_datetime(df["Dato"], dayfirst=True)
    except:
        pass

    # Kort vegstrekning
    if "vegsystemreferanse.kortform" in df.columns:
        df["vegstrekning_kort"] = (
            df["vegsystemreferanse.kortform"]
            .astype(str)
            .str.split()
            .str[:2]
            .str.join("_")
        )

    return df


df = load_raw_data()

# -------------------------------------------------------------------
# SIDEBAR – FILTRE
# -------------------------------------------------------------------
with st.sidebar:
    st.title("Filtre")

    # ---- TID ----
    st.subheader("Tid")
    år = st.multiselect("År", sorted(df["År"].dropna().unique()))

    if "Dato" in df.columns:
        dato_min = df["Dato"].min()
        dato_max = df["Dato"].max()
        dato = st.date_input("Dato-intervall", value=[dato_min, dato_max])
    else:
        dato = None

    # ---- GEOGRAFI ----
    st.subheader("Geografi")
    kommune = st.multiselect("Kommune", sorted(df["Kommune"].astype(str).unique()))
    vegkategori = st.multiselect("Vegkategori", sorted(df["vegkategori"].dropna().unique()))
    vegnummer = st.multiselect("Vegnummer", sorted(df["vegnr"].dropna().unique()))
    strekning = st.multiselect("Strekning", sorted(df["strekning"].dropna().unique()))

    avstand_veg = st.slider(
        "Avstand til vegnett (m)",
        float(df["avstand_vegnettet_m"].min()),
        float(df["avstand_vegnettet_m"].max()),
        (float(df["avstand_vegnettet_m"].min()), float(df["avstand_vegnettet_m"].max()))
    )

    # ---- DYR ----
    st.subheader("Dyr")
    arter = st.multiselect("Art", sorted(df["Art"].dropna().unique()))
    kjønn = st.multiselect("Kjønn", sorted(df["Kjønn"].dropna().unique()))
    alder = st.multiselect("Alder", sorted(df["Alder"].dropna().unique()))

    # ---- HENDELSE ----
    st.subheader("Hendelse")
    årsak = st.multiselect("Årsak", sorted(df["Årsak"].dropna().unique()))
    utfall = st.multiselect("Utfall", sorted(df["Utfall"].dropna().unique()))
    merkelapp = st.text_input("Merkelappnummer (søk)")
    fallvilt_id = st.text_input("Fallvilt-ID (søk)")

    # ---- TRAFIKK & VEG ----
    st.subheader("Trafikk & veg")

    ådt_min = int(df["ÅDT, total"].min())
    ådt_max = int(df["ÅDT, total"].max())
    ådt = st.slider("ÅDT total", ådt_min, ådt_max, (ådt_min, ådt_max))

    fartsgrense = st.multiselect("Fartsgrense", sorted(df["Fartsgrense"].dropna().unique()))
    vegobjekt_540 = st.multiselect("Vegobjekt 540-ID", sorted(df["Vegobjekt_540_id"].dropna().unique()))
    vegobjekt_105 = st.multiselect("Vegobjekt 105-ID", sorted(df["Vegobjekt_105_id"].dropna().unique()))

    lengde = st.slider(
        "Vegobjekt 540 lengde (m)",
        float(df["Vegobjekt_540_lengde"].min()),
        float(df["Vegobjekt_540_lengde"].max()),
        (float(df["Vegobjekt_540_lengde"].min()), float(df["Vegobjekt_540_lengde"].max()))
    )

    # ---- AVANSERT ----
    with st.expander("Avanserte filtre"):
        fase = st.multiselect("Fase", sorted(df["fase"].dropna().unique()))
        arm = st.multiselect("Arm", sorted(df["arm"].dropna().unique()))
        adskilte_løp = st.multiselect("Adskilte løp", sorted(df["adskilte_løp"].dropna().unique()))
        trafikantgruppe = st.multiselect("Trafikantgruppe", sorted(df["trafikantgruppe"].dropna().unique()))
        retning = st.multiselect("Retning", sorted(df["retning"].dropna().unique()))
        veglenkesekvensid = st.multiselect("Veglenkesekvens-ID", sorted(df["veglenkesekvensid"].dropna().unique()))
        relativ = st.slider(
            "Relativ posisjon",
            float(df["relativPosisjon"].min()),
            float(df["relativPosisjon"].max()),
            (float(df["relativPosisjon"].min()), float(df["relativPosisjon"].max()))
        )

# -------------------------------------------------------------------
# FILTRERING
# -------------------------------------------------------------------
df_filt = df.copy()

if år:
    df_filt = df_filt[df_filt["År"].isin(år)]

if dato is not None and isinstance(dato, list) and len(dato) == 2 and "Dato" in df_filt.columns:
    df_filt = df_filt[
        (df_filt["Dato"] >= pd.to_datetime(dato[0])) &
        (df_filt["Dato"] <= pd.to_datetime(dato[1]))
    ]

if kommune:
    df_filt = df_filt[df_filt["Kommune"].astype(str).isin(kommune)]

if vegkategori:
    df_filt = df_filt[df_filt["vegkategori"].isin(vegkategori)]

if vegnummer:
    df_filt = df_filt[df_filt["vegnr"].isin(vegnummer)]

if strekning:
    df_filt = df_filt[df_filt["strekning"].isin(strekning)]

df_filt = df_filt[
    (df_filt["avstand_vegnettet_m"] >= avstand_veg[0]) &
    (df_filt["avstand_vegnettet_m"] <= avstand_veg[1])
]

if arter:
    df_filt = df_filt[df_filt["Art"].isin(arter)]

if kjønn:
    df_filt = df_filt[df_filt["Kjønn"].isin(kjønn)]

if alder:
    df_filt = df_filt[df_filt["Alder"].isin(alder)]

if årsak:
    df_filt = df_filt[df_filt["Årsak"].isin(årsak)]

if utfall:
    df_filt = df_filt[df_filt["Utfall"].isin(utfall)]

if merkelapp:
    df_filt = df_filt[df_filt["Merkelappnummer"].astype(str).str.contains(merkelapp, na=False)]

if fallvilt_id:
    df_filt = df_filt[df_filt["Fallvilt-ID"].astype(str).str.contains(fallvilt_id, na=False)]

df_filt = df_filt[(df_filt["ÅDT, total"] >= ådt[0]) & (df_filt["ÅDT, total"] <= ådt[1])]

if fartsgrense:
    df_filt = df_filt[df_filt["Fartsgrense"].isin(fartsgrense)]

if vegobjekt_540:
    df_filt = df_filt[df_filt["Vegobjekt_540_id"].isin(vegobjekt_540)]

if vegobjekt_105:
    df_filt = df_filt[df_filt["Vegobjekt_105_id"].isin(vegobjekt_105)]

df_filt = df_filt[
    (df_filt["Vegobjekt_540_lengde"] >= lengde[0]) &
    (df_filt["Vegobjekt_540_lengde"] <= lengde[1])
]

# Avanserte
if fase:
    df_filt = df_filt[df_filt["fase"].isin(fase)]

if arm:
    df_filt = df_filt[df_filt["arm"].isin(arm)]

if adskilte_løp:
    df_filt = df_filt[df_filt["adskilte_løp"].isin(adskilte_løp)]

if trafikantgruppe:
    df_filt = df_filt[df_filt["trafikantgruppe"].isin(trafikantgruppe)]

if retning:
    df_filt = df_filt[df_filt["retning"].isin(retning)]

if veglenkesekvensid:
    df_filt = df_filt[df_filt["veglenkesekvensid"].isin(veglenkesekvensid)]

df_filt = df_filt[
    (df_filt["relativPosisjon"] >= relativ[0]) &
    (df_filt["relativPosisjon"] <= relativ[1])
]

# -------------------------------------------------------------------
# BEREGNING – antall kollisjoner og frekvens
# -------------------------------------------------------------------
df_calc = df_filt.copy()

if len(df_calc) > 0:
    df_calc["antall_kollisjoner"] = (
        df_calc.groupby(["Vegobjekt_540_id", "Art"]).transform("size")
    )

    df_calc["frekvens"] = df_calc["antall_kollisjoner"] / (
        df_calc["ÅDT, total"].round(0) + df_calc["Vegobjekt_540_lengde"].round(0)
    )

    df_calc = df_calc.sort_values("frekvens", ascending=False).drop_duplicates()

# -------------------------------------------------------------------
# VISNING
# -------------------------------------------------------------------
st.title("🐾 Dyrepåkjørsler – filtrert oversikt")

st.markdown(
    f"**Antall hendelser etter filtrering: {len(df_filt):,}**"
)

if len(df_calc) > 0:
    st.subheader("Rangert etter frekvens")
    st.dataframe(df_calc[
        ["Vegobjekt_540_id", "Art", "ÅDT, total", "Vegobjekt_540_lengde",
         "antall_kollisjoner", "frekvens"]
    ], use_container_width=True)
else:
    st.info("Ingen treff for de valgte filtrene.")

with st.expander("Om dataene"):
    st.write("""
    Datagrunnlaget kommer fra fallviltregisteret og NVDB.
    Frekvensen beregnes som antall kollisjoner delt på sum(ÅDT + lengde).
    """)