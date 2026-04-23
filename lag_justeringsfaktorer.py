import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
import statsmodels.api as sm
from astral import LocationInfo
from astral.sun import sun
import pandas as pd
from astral.sun import elevation
from datetime import timedelta
import functions as f
import json 


df = pd.read_csv('data/Fallvilt_tidspunkter.csv', sep=";")

df["HendelsesDatoTid"] = pd.to_datetime(df["HendelsesDatoTid"])

if df["HendelsesDatoTid"].dt.tz is None:
    df["HendelsesDatoTid"] = df["HendelsesDatoTid"].dt.tz_localize("Europe/Oslo")

# tidsvindauge (tz-aware)
slutt = pd.Timestamp.now(tz="Europe/Oslo").normalize() - pd.Timedelta(days=1)
start = slutt - pd.DateOffset(years=1)

df = df[
    (df["HendelsesDatoTid"] >= start) &
    (df["HendelsesDatoTid"] <= slutt + pd.Timedelta(days=1))
].copy()


#Filtrer relevante veger og dyr
df=df[df['Art'].isin(['Elg', 'Hjort', 'Rådyr'])].copy()  
df=df[df['vegkategori'].isin(['E','F','K'])].copy()
#df=df[df['ÅDT, total']>100].copy()


df= df[df['UkjentTidspunkt']==False].copy()


# Representativ plassering for Trøndelag
df["årstid"] = df["HendelsesDatoTid"].dt.month.apply(f.maaned_til_arstid)
df["årstid"] = df["årstid"].astype("category").copy()

df["lyskategori"] = df["HendelsesDatoTid"].apply(f.lyskategori_fra_tidspunkt)
df["lyskategori"] = df["lyskategori"].astype("category")


df["eksponering"] = ( ###1/frevekns
    df["ÅDT, total"]
    * 365
    * df["Vegobjekt_540_lengde"]
    / 100000
)

df["log_eksponering"] = np.log(df["eksponering"])

df_agg = (
    df
    .groupby(
        ["Vegobjekt_105_id", "årstid", "lyskategori"],
        observed=True,      # fjern FutureWarning
        as_index=False
    )
    .agg(
        antall_kollisjoner=("Vegobjekt_105_id", "count"),
        log_eksponering=("log_eksponering", "first")
    )
)


df_agg.drop_duplicates(inplace=True)


model_nb = smf.glm(
    formula="antall_kollisjoner ~ C(lyskategori)+C(årstid)",
    data=df_agg,
    family=sm.families.NegativeBinomial(alpha=0.1),
    #family=sm.families.Poisson(),
    offset=df_agg["log_eksponering"]
).fit()

summary_text = model_nb.summary().as_text()


with open("log.txt", "a") as file:
    file.write(summary_text)
    file.write("\n\n")
    file.close()

ARSTID_JUSTERING = f.lag_arstidsjustering(model_nb)

LYSJUSTERING=f.lag_lysjustering(model_nb)

with open("ARSTID_JUSTERING.json", "w", encoding="utf-8") as f:
    json.dump(ARSTID_JUSTERING, f, indent=4, sort_keys=True)

with open("LYSJUSTERING.json", "w", encoding="utf-8") as f:
    json.dump(LYSJUSTERING, f, indent=4, sort_keys=True)