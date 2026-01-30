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
    df = pd.read_csv("frekvens_2.csv", sep=',', encoding='utf-8')
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
    metric_label = "Historisk frekvens (per  kjøretøy-meter per år)"
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

st.dataframe(
    df_top.rename(columns={
        metric_col: metric_label,
        "Vegobjekt_540_id": "Veg-objekt_id",
        "Vegobjekt_540_lengde": "Lengde (m)",
    }),
    use_container_width=True
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
