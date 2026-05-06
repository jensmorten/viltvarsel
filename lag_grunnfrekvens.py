import pandas as pd
import functions as f
import json

###Last data
df = pd.read_csv('data/Fallvilt_tidspunkter.csv', sep=";")

###Filtrer dynamisk 1 år tilbake
df["HendelsesDatoTid"] = pd.to_datetime(df["HendelsesDatoTid"]).copy()
slutt = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
start = slutt - pd.DateOffset(years=1)

df = df[
    (df["HendelsesDatoTid"] >= start) &
    (df["HendelsesDatoTid"] <= slutt)
].copy()

#Filtrer relevante veger og dyr
df=df[df['Art'].isin(['Elg', 'Hjort', 'Rådyr'])].copy()  
df=df[df['vegkategori'].isin(['E','F','K'])].copy()

df=df[df['ÅDT, total']>100].copy()

METADATA = {
        'sist_oppdatert': str(pd.Timestamp.now().normalize()),
        'første_kollisjon': str(df["HendelsesDatoTid"].min()),
        'siste_kollisjon': str(df["HendelsesDatoTid"].max())
    }

df["UTM33_øst_int"] = (
    df["UTM33 øst"]
    .astype(str)
    .str.split(".", n=1)
    .str[0].astype(int)
)

df["UTM_nord_int"] = (
    df["UTM33 nord"]
    .astype(str)
    .str.split(".", n=1)
    .str[0].astype(int)
)

df["Vegnamn"] = (
    df["vegsystemreferanse.kortform"]
    .astype(str)
    .str.split(" ", n=1)
    .str[0].astype(str)
)


###TA bare med relevante kolonner videre
df=df[['Vegobjekt_540_id','Vegnamn', 'Art','ÅDT, total','Vegobjekt_540_lengde', 'UTM_nord_int', 'UTM33_øst_int']].copy()


kolonner = [
    "ÅDT, total", ##vi antar bare 1 verdi for hvert vegobjekt-id, men i fall det er ulikt tar vi gjennomsnitt
    "Vegobjekt_540_lengde", ##vi antar bare 1 verdi for hvert vegobjekt-id, men i fall det er ulikt tar vi gjennomsnitt
    "UTM_nord_int", ##gjennomsnitt over posisjoner for kollision
    "UTM33_øst_int",##gjennomsnitt over posisjoner for kollision
]

for col in kolonner:
    df[f"{col}_avg"] = (
        df
        .groupby(["Vegobjekt_540_id", "Art"])[col]
        .transform("mean")
        .round(0)
        .astype("Int64")
    )

df["antall_kollisjoner"] = (
    df
    .groupby(["Vegobjekt_540_id", "Art"])
    .transform("size")
)


###For å lage en troverdig frekvens treng vi 3 eller fleire hendingar
df=df[df["antall_kollisjoner"] >=3].copy()

df["frekvens"] = (
    df["antall_kollisjoner"]*100000 ####per 100 km per bil per år
    /
    (
        df["ÅDT, total_avg"]
        * 365
        * df["Vegobjekt_540_lengde_avg"]
    )
)

df["årsrisiko"] = df["frekvens"]*150  ###antatt 150000 km i gjennomsnitt for en bil


df=df[['Vegobjekt_540_id', 
       'Vegnamn',
       'Art', 
       'ÅDT, total_avg',
       'Vegobjekt_540_lengde_avg', 
       'UTM_nord_int_avg', 
       'UTM33_øst_int_avg', 
       'antall_kollisjoner', 
       'frekvens', 'årsrisiko']].copy()

df=df.drop_duplicates()
df.dropna(inplace=True)

df["samanlikning_yrke"] = df["årsrisiko"].apply(
    lambda x: f.map_arsrisiko_til_yrke(x)
)

if df['Vegobjekt_540_id'].count()>0:
    df.to_csv("frekvens_silver_latest.csv",encoding='utf-8', index=False)
    print(f"🎈 Hurra! {len(df)} grunnfrekvensar lagra to .csv-file")
    with open("METADATA.json", "w", encoding="utf-8") as f:
        json.dump(METADATA, f, indent=4, sort_keys=True,  ensure_ascii=False)
else:
    print( str(df['Vegobjekt_540_id'].count()) + " rader i resultat. for lite? Ingenting lagra" )