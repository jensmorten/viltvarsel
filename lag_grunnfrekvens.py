import pandas as pd

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
df=df[['Vegobjekt_540_id', 'Art','ÅDT, total','Vegobjekt_540_lengde', 'UTM_nord_int', 'UTM33_øst_int']].copy()


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
       'Art', 
       'ÅDT, total_avg',
       'Vegobjekt_540_lengde_avg', 
       'UTM_nord_int_avg', 
       'UTM33_øst_int_avg', 
       'antall_kollisjoner', 
       'frekvens', 'årsrisiko']].copy()

df=df.drop_duplicates()
df.dropna(inplace=True)


yrkesrisiko = pd.DataFrame({
    "yrkesgruppe": [
        "Leiarar",
        "Høgskuleyrke",
        "Kontoryrke",
        "Sals- og serviceyrke",
        "Renhaldarar, hjelpearbeidarar mv.",
        "Bønder, fiskarar mv.",
        "Prosess- og maskinoperatørar, transportarbeidarar mv.",
        "Håndverkarar",
    ],
    "ulykker_per_arsverk": [
        4.0 / 1000,
        6.5 / 1000,
        7.7 / 1000,
        13.9 / 1000,
        15.5 / 1000,
        19.2 / 1000,
        20.1 / 1000,
        20.5 / 1000,
    ],
    "intervall_låg": [
        3.0 / 1000,
        5.0 / 1000,
        7.0 / 1000,
        12.0 / 1000,
        14.5 / 1000,
        17.5 / 1000,
        19.6 / 1000,
        20.2 / 1000,
    ],
    "intervall_høg": [
        5.0 / 1000,
        7.0 /1000,
        12.0 / 1000,
        14.5 / 1000,
        17.5 / 1000,
        19.6 / 1000,
        20.2 / 1000,
        20.7 /1000
    ]
    
})


def map_arsrisiko_til_yrke(arsrisiko, yrkesdf):
    """
    Mapper årsrisiko (per årsverk) til illustrativ yrkessammenlikning.

    Parametre
    ----------
    arsrisiko : float
        Risiko per årsverk (f.eks. 0.012 = 12 per 1000)
    yrkesdf : pandas.DataFrame
        Må innehalde kolonnane:
        - 'yrkesgruppe'
        - 'intervall_lav'
        - 'intervall_høg'

    Returnerer
    ----------
    str : tekst for illustrativ samanlikning
    """

    # Sorter for tryggleik (lav → høg risiko)
    yrkesdf = yrkesdf.sort_values("intervall_låg")

    # Lågare enn lågaste kjente yrke (Ledere)
    if arsrisiko < yrkesdf["intervall_låg"].min():
        return "lågare risiko enn dei fleste yrke"

    # Høgare enn høgaste kjente yrke (Håndverkere)
    if arsrisiko > 10* yrkesdf["intervall_høg"].max():
        return ">10x høgare risiko enn høgrisikoyrke"
    elif arsrisiko > 5* yrkesdf["intervall_høg"].max():
        return ">5x høgare risiko enn høgrisikoyrke"
    elif arsrisiko > 2* yrkesdf["intervall_høg"].max():
        return ">2x høgare risiko enn høgrisikoyrke"
    elif arsrisiko > yrkesdf["intervall_høg"].max():
        return "1-2x høgare risiko enn høyrisikoyrke"
    
    # Finn intervall som treff
    treff = yrkesdf[
        (yrkesdf["intervall_låg"] <= arsrisiko) &
        (yrkesdf["intervall_høg"] >= arsrisiko)
    ]

    if not treff.empty:
        # Dersom fleire treff (overlapp), vel nærmaste midtpunkt
        treff = treff.copy()
        treff["avstand"] = (treff["ulykker_per_arsverk"] - arsrisiko).abs()
        return treff.sort_values("avstand").iloc[0]["yrkesgruppe"]

    # Fallback (burde eigentleg ikkje skje)
    return "Ukjend risikonivå"

df["samanlikning_yrke"] = df["årsrisiko"].apply(
    lambda x: map_arsrisiko_til_yrke(x, yrkesrisiko)
)


df.to_csv("frekvens_script.csv",encoding='utf-8', index=False)