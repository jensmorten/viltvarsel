import pandas as pd


df = pd.read_csv('Fallvilt_tidspunkter.csv', sep=";", low_memory=False)

df["HendelsesDatoTid"] = pd.to_datetime(df["HendelsesDatoTid"]).copy()
slutt = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
start = slutt - pd.DateOffset(years=1)

df = df.query(
    "@start <= HendelsesDatoTid <= @slutt + pd.Timedelta(days=1)"
).copy()


df=df[df['Art'].isin(['Elg', 'Hjort', 'Rådyr'])].copy()  
df=df[df['vegkategori'].isin(['E','F','K'])].copy()

