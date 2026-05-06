import pandas as pd
import functions as f

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

df["årstid"] = df["HendelsesDatoTid"].apply(f.maaned_til_arstid)

df["lysforhold"] = df["HendelsesDatoTid"].apply(f.lyskategori_fra_tidspunkt)

df["UTM33_øst_int"] = (
    df["UTM33 øst"]
    .astype(str)
    .str.split(",", n=1)
    .str[0].astype(int)
)


df["UTM_nord_int"] = (
    df["UTM33 nord"]
    .astype(str)
    .str.split(",", n=1)
    .str[0].astype(int)
)

###TA bare med relevante kolonner videre
df=df[['Vegobjekt_540_id', 'Art','ÅDT, total','Vegobjekt_540_lengde', 'UTM_nord_int', 'UTM33_øst_int', 'årstid','lysforhold']].copy()

kolonner = [
    "ÅDT, total", ##vi antar bare 1 verdi for hvert vegobjekt-id, men i fall det er ulikt tar vi gjennomsnitt
    "Vegobjekt_540_lengde", ##vi antar bare 1 verdi for hvert vegobjekt-id, men i fall det er ulikt tar vi gjennomsnitt
    "UTM_nord_int", ##gjennomsnitt over posisjoner for kollision
    "UTM33_øst_int",##gjennomsnitt over posisjoner for kollision
]

for col in kolonner:
    df[f"{col}_avg"] = (
        df
        .groupby(["Vegobjekt_540_id", "Art", "årstid", "lysforhold"])[col]
        .transform("mean")
        .round(0)
        .astype("Int64")
    )

df["antall_kollisjoner"] = (
    df
    .groupby(["Vegobjekt_540_id", "Art", "årstid", "lysforhold"])
    .transform("size")
)

###For å lage en troverdig frekvens treng vi 3 eller fleire hendingar
df=df[df["antall_kollisjoner"] >=2].copy()

df["frekvens"] = (
    df["antall_kollisjoner"]*100000*4*3 ####per 100 km per bil per år * 4 årstider*3 lysforhold
    /
    (
        df["ÅDT, total_avg"]
        * 365
        * df["Vegobjekt_540_lengde_avg"]
    )
)

df["årsrisiko"] = df["frekvens"]*150  ###antatt 150000 km i gjennomsnitt for en bil


df=df[['Vegobjekt_540_id', 
       'Art', 'årstid', 'lysforhold',
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


df.to_csv("data/frekvens_årstid.csv",encoding='utf-8', index=False)
print(f"🎈 Hurra! {len(df)} grunnfrekvensar lagra to .csv-file")