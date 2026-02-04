import pandas as pd
import numpy as np
import re

# -------------------------------
# 1. Les CSV
# -------------------------------

df = pd.read_csv("Fallvilt_beriket_med_vær.csv", sep=";", encoding="utf-8", low_memory=False)

# -------------------------------
# 2. Dato → datetime + år/måned
# -------------------------------

df["Dato"] = pd.to_datetime(df["Dato"], errors="coerce")
df["År"] = df["Dato"].dt.year
df["Måned"] = df["Dato"].dt.month

# -------------------------------
# 3. Funksjon for å trekke ut tall fra verdier som "4 cm", "-3.8 degC", "6.4 m/s"
# -------------------------------

def extract_num(value):
    if pd.isna(value):
        return np.nan
    # Finn første tall med optional minus og desimal
    match = re.search(r"-?\d+[\.,]?\d*", str(value))
    if match:
        return float(match.group().replace(",", "."))
    return np.nan

# -------------------------------
# 4. Rense værkolonner
# -------------------------------

weather_columns = ["snow_depth", "mean_temperature", "mean_wind_speed"]

for col in weather_columns:
    df[col] = df[col].apply(extract_num)

# -------------------------------
# 5. Ugyldige verdier → NaN
# -------------------------------

# Snødybde < 0 er umulig
df.loc[df["snow_depth"] < 0, "snow_depth"] = np.nan

# Vindhastighet < 0 er umulig
df.loc[df["mean_wind_speed"] < 0, "mean_wind_speed"] = np.nan

# Temperatur beholder vi (kan være negativ)
# -------------------------------

# 6. Beregn månedsmiddel per kommune + år + måned
# -------------------------------

monthly = (
    df.groupby(["Kommune", "År", "Måned"])[weather_columns]
      .mean()
      .reset_index()
      .rename(columns={
          "snow_depth": "monthly_snow_depth",
          "mean_temperature": "monthly_mean_temperature",
          "mean_wind_speed": "monthly_mean_wind_speed"
      })
)

# -------------------------------
# 7. Merge månedsmiddel tilbake inn i originaldata
# -------------------------------

df = df.merge(monthly, on=["Kommune", "År", "Måned"], how="left")

# -------------------------------
# 8. Lagre resultat
# -------------------------------

df.to_csv("Fallvilt_månedsberiket.csv", sep=";", encoding="utf-8", index=False)

print("Ferdig! Filen 'Fallvilt_månedsberiket.csv' er generert.")