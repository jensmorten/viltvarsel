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
    df = pd.read_csv("frekvens_final.csv", sep=',', encoding='utf-8')
    return df

df = load_data()

# --------------------------------------------------
# Sidebar – brukarval
# --------------------------------------------------

st.sidebar.title("Innstillinger")

metric_choice = st.sidebar.radio(
    "Vis etter:",
    options=[
        "Historisk frekvens",
        "Predikert risiko"
    ]
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

df_filt = df[df["Art"].isin(artsvalg)]

if metric_choice == "Historisk frekvens":
    metric_col = "frekvens"
    metric_label = "Historisk frekvens (kollision per  kjøretøy-meter per år)"
else:
   None


# Top N
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
    Dyreartar: **{", ".join(artsvalg)}**
    """
)

df_visning = df_top.copy()

# ID som heiltal (utan desimalar)
df_visning["Vegobjekt_540_id"] = df_visning["Vegobjekt_540_id"].astype("Int64")

# Avrund ÅDT og lengde til heiltal
df_visning["ÅDT, total"] = df_visning["ÅDT, total"].round(0).astype("Int64")
df_visning["Vegobjekt_540_lengde"] = df_visning["Vegobjekt_540_lengde"].round(0).astype("Int64")

df_visning = df_visning.rename(columns={
    "Vegobjekt_540_id": "Veg-objekt ID",
    "Vegobjekt_540_lengde": "Lengde (m)",
    metric_col: metric_label
})

df_visning["lenke"] = (
    "https://vegkart.atlas.vegvesen.no/#kartlag:geodata"
    "/@186753,7056711,10/valgt:"
    + df_visning["Vegobjekt_540_id"].astype(str)
    + ":540"
)



styled_df = df_visning.style.format({
    metric_label: "{:.2E}",   # frekvens
    "ÅDT, total": "{:.0f}",   # heiltal
    "Lengde (m)": "{:.0f}",   # heiltal,
})


st.dataframe(
    styled_df,
    width='content',
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
        
        **Predikert risiko**  
        = modellert risiko basert på vegtype, landskap og vilttettheit  (ikke klart ennå)

        """
    )
