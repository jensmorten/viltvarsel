import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Dyrepåkjørsler – risikostrekningar",
    layout="wide"
)

# --------------------------------------------------
# Data loading
# --------------------------------------------------

@st.cache_data
def load_data():
    df = pd.read_csv("vegstrekning_moc.csv", sep=',')
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

species_choice = st.sidebar.multiselect(
    "Vel dyreartar:",
    options=["Elg", "Hjort", "Rådyr"],
    default=["Elg", "Hjort", "Rådyr"]
)

top_n = st.sidebar.slider(
    "Tal strekningar:",
    min_value=5,
    max_value=50,
    value=10,
    step=5
)

# --------------------------------------------------
# Valider input
# --------------------------------------------------

if not species_choice:
    st.warning("Vel minst éin dyreart.")
    st.stop()

# --------------------------------------------------
# Filtrer data
# --------------------------------------------------

df_filt = df[df["dyreart"].isin(artsvalg)]

if metric_choice == "Historisk frekvens":
    metric_col = "historisk_frekvens"
    metric_label = "Historisk frekvens (per mill. kjøretøykm)"
else:
    metric_col = "predikert_risiko"
    metric_label = "Predikert risiko (hazard)"

df_grouped = (
    df_filt
    .groupby(
        ["strekning_id", "vegnavn", "lengde_km", "kommune"],
        as_index=False
    )[metric_col]
    .sum()
)



# Top N
df_top = (
    df_grouped
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
    Dyreartar: **{", ".join(species_choice)}**
    """
)

st.dataframe(
    df_top.rename(columns={
        metric_col: metric_label,
        "road_name": "Veg",
        "length_km": "Lengd (km)",
        "municipality": "Kommune"
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
        = observerte dyrepåkjørsler normalisert på trafikkmengde  
        
        **Predikert risiko**  
        = modellert risiko basert på vegtype, landskap og vilttettheit  
        
        Tala er meint for samanlikning mellom strekningar –  
        ikkje som eksakt sannsyn for enkeltbilar.
        """
    )
