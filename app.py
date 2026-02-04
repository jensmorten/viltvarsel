import streamlit as st
import pandas as pd
from datetime import date
import numpy as np

st.set_page_config(
    page_title="Dyrepåkjørsler – risikostrekninger",
    layout="wide"
)

# --------------------------------------------------
# Årstid frå dato
# --------------------------------------------------

def finn_årstid(dato):
    m = dato.month
    if m in (12, 1, 2):
        return "Vinter"
    elif m in (3, 4, 5):
        return "Vår"
    elif m in (6, 7, 8):
        return "Sommar"
    else:
        return "Haust"


ARSTID_JUSTERING = {
    "Haust": 1.00,
    "Vinter": 0.88,
    "Vår": 0.65,
    "Sommar": 0.57,
}

DAGENS_ÅRSTID = finn_årstid(date.today())


# --------------------------------------------------
# Data loading
# --------------------------------------------------

#@st.cache_data
def load_data():
    df = pd.read_csv("frekvens_final.csv", encoding="utf-8")
    return df

df = load_data()

# --------------------------------------------------
# Sidebar – brukarval
# --------------------------------------------------

st.sidebar.title("Innstillinger")

metric_choice = st.sidebar.radio(
    "Vis etter:",
    options=["Historisk frekvens", "Predikert risiko"]
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
)

if metric_choice == "Historisk frekvens":
    metric_col = "frekvens"
    metric_label = "Historisk frekvens (kollisjon per kjøretøy per år per 100 km)"
else:
    metric_col = "predikert_risiko"
    metric_label = "Predikert risiko"

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


# --------------------------------------------------
# Hovudvisning
# --------------------------------------------------

st.title("🐾 Dyrepåkjørsler – farlege vegstrekningar")

st.markdown(
    f"""
    **Viser topp {top_n} vegstrekningar**  
    Sortert etter: **{metric_label}**  
    Dyrearter: **{", ".join(artsvalg)}**
    """
)

df_visning = df_top.copy()
df_visning_koll = df_top_kollisjon.copy()


# -----------------------------
# Rydd datatypar for visning
# -----------------------------

df_visning["Vegobjekt_540_id"] = df_visning["Vegobjekt_540_id"].astype("Int64")
df_visning_koll["Vegobjekt_540_id"] = df_visning_koll["Vegobjekt_540_id"].astype("Int64")


df_visning["Ådt_avg"] = df_visning["ÅDT, total_avg"].astype("Int64")
df_visning_koll["Ådt_avg"] = df_visning_koll["ÅDT, total_avg"].astype("Int64")

df_visning["Vegobjekt_540_lengde"] = df_visning["Vegobjekt_540_lengde_avg"].astype("Int64")
df_visning_koll["Vegobjekt_540_lengde"] = df_visning_koll["Vegobjekt_540_lengde_avg"].astype("Int64")


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
    + df_visning["UTM33_øst_int_avg"].astype(str)
    + ","
    + df_visning["UTM_nord_int_avg"].astype(str)
    + ",10/valgt:"
    + df_visning["Vegobjekt_540_id"].astype(str)
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

#print(df_visning.columns)

#df_visning=df_visning[['Veg_ID', 'Art', 'ÅDT (Årsdøgntrafikk)', 'Lengde (m)','frekvens','lenke']].copy()
df_visning = df_visning[
    ['Veg_ID', 'Art', 'ÅDT (Årsdøgntrafikk)', 'Lengde (m)', 'antall_kollisjonar siste år', metric_label, 'lenke','Samanlikning med risiko i yrke']
].copy()

df_visning_koll = df_visning_koll[
    ['Veg_ID', 'Art', 'ÅDT (Årsdøgntrafikk)', 'Lengde (m)', 'antall_kollisjonar siste år', metric_label, 'lenke','Samanlikning med risiko i yrke']
].copy()


# -----------------------------
# Styling
# -----------------------------

df_visning = df_visning.reset_index(drop=True)
df_visning_koll = df_visning_koll.reset_index(drop=True)

styled_df = df_visning.style.format({
    metric_label: "{:.2E}",
    "ÅDT (Årsdøgntrafikk)": "{:.0f}",
    "Lengde (m)": "{:.0f}",
})

styled_df_koll = styled_df_koll.style.format({
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
    width="content"
)

st.dataframe(
    styled_df_koll,
    column_config={
        "lenke": st.column_config.LinkColumn(
            "Vegkart",
            display_text="Opne i Vegkart"
        )
    },
    width="content"
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
        (med fråver) per årsverk i ulike yrke (tal og kategoriar frå SSB). Så kolonna betyr "å kjøre 15000 km på denne strekninga vil gi om lag same risiko som å jobbe eit årsverk i dette yrket". 

        Samanlikninga er meint som ei **grovt illustrativ skala** basert på desse føresetnadane. 

        Predikert risiko  
        = historisk grunnfrekvens justert med ein årstidsfaktor, estimert frå ein statistisk modell
        (Negativ binomial-regresjon) basert på observerte dyrepåkjørsler.
        """
    )
