import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Dyrepåkjørsler – risikostrekninger",
    layout="wide"
)

# --------------------------------------------------
# Data loading
# --------------------------------------------------

@st.cache_data
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
    options=["Historisk frekvens"]  # predikert kjem seinare
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

metric_col = "frekvens"
metric_label = "Historisk frekvens (kollisjon per kjøretøy–meter–år)"

df_top = (
    df_filt
    .sort_values(metric_col, ascending=False)
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

# -----------------------------
# Rydd datatypar for visning
# -----------------------------

df_visning["Vegobjekt_540_id"] = df_visning["Vegobjekt_540_id"].astype("Int64")

df_visning["Ådt_avg"] = df_visning["ÅDT, total_avg"].astype("Int64")
df_visning["Vegobjekt_540_lengde"] = df_visning["Vegobjekt_540_lengde_avg"].astype("Int64")

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

# -----------------------------
# Gi pene kolonnenamn
# -----------------------------

df_visning = df_visning.rename(columns={
    "Vegobjekt_540_id": "Veg_ID",
    "Ådt_avg": "ÅDT (Årsdøgntrafikk)",
    "Vegobjekt_540_lengde": "Lengde (m)",
    metric_col: metric_label
}).copy()

df_visning=df_visning[['Veg_ID', 'Art', 'ÅDT (Årsdøgntrafikk)', 'Lengde (m)','frekvens','lenke']].copy()

# -----------------------------
# Styling
# -----------------------------

styled_df = df_visning.style.format({
    metric_label: "{:.2E}",
    "ÅDT (Årsdøgntrafikk)": "{:.0f}",
    "Lengde (m)": "{:.0f}",
})

st.dataframe(
    styled_df,
    column_config={
        "lenke": st.column_config.LinkColumn(
            "Vegkart",
            display_text="Åpne i Vegkart"
        )
    }
)

# --------------------------------------------------
# Enkel forklaring
# --------------------------------------------------

with st.expander("ℹ️ Om tala"):
    st.markdown(
        """
        **Historisk frekvens**  
        = observerte dyrepåkjørsler normalisert på trafikkmengde og veglengde  
        
        Tallet viser **kollisjoner per kjøretøy–meter–år**  
        og er ment for **sammenlikning mellom vegstrekninger**.
        """
    )
