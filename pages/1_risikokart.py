import streamlit as st
import pandas as pd
import numpy as np
import asyncio
from functions import hent_alle_wkt, lag_felles_kart

st.set_page_config(
    page_title="Risikokart",
    layout="wide"
)

st.title("🗺️ Risikokart – dyrepåkjørslar")

# ------------------
# Last data
# ------------------

def load_data():
    return pd.read_csv("data/frekvens_årstid_script.csv")

df = load_data()
