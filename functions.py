
from astral import LocationInfo
from astral.sun import sun
import pandas as pd
from astral.sun import elevation
import statsmodels.formula.api as smf
import statsmodels.api as sm
import numpy as np

def map_arsrisiko_til_yrke(arsrisiko):
    """
    Mapper årsrisiko (per årsverk) til illustrativ yrkessammenlikning.

    Parametre
    ----------
    arsrisiko : float
        Risiko per årsverk (f.eks. 0.012 = 12 per 1000)
    ----------
    str : tekst for illustrativ samanlikning
    """

    yrkesdf = pd.DataFrame({
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


def lyskategori_fra_tidspunkt(ts):
    TRONDELAG = LocationInfo(
    name="Trøndelag",
    region="Norway",
    timezone="Europe/Oslo",
    latitude=63.4,
    longitude=10.4,
    )
    if pd.isna(ts):
        return None

    # Bruk solhøgde (grader over/under horisont)
    solhoyde = elevation(TRONDELAG.observer, ts)
    if solhoyde > 12:
        return "Dag"
    elif solhoyde >-12:
        return "Skumring"
    else:
        return "Natt"
    

def maaned_til_arstid(m):
    if m in [12, 1, 2]:
        return "vinter"
    elif m in [3, 4, 5]:
        return "vår"
    elif m in [6, 7, 8]:
        return "sommar"
    else:
        return "haust"
    

def lag_arstidsjustering(model, referanse="haust"):
    """
    Lag justeringsfaktorar for årstid frå ein statsmodels GLM (NB / Poisson).

    Referansekategori får faktor 1.0.
    """
    params = model.params

    # Finn alle årstids-koeffisientar
    arstid_params = {
        k: v for k, v in params.items()
        if k.startswith("C(årstid)")
    }

    # Referanse (den som ikkje er i params)
    arstid_justering = {referanse: 1.0}

    # Legg til dei estimerte årstidene
    for k, beta in arstid_params.items():
        # Trekk ut årstidsnamn, t.d. C(årstid)[T.Sommar] -> Sommar
        arstid = k.split("[T.")[1].rstrip("]")
        arstid_justering[arstid] = np.round(float(np.exp(beta)),2)

    return arstid_justering

def lag_lysjustering(model, referanse="Dag", damping=1, normaliser=False):
    """
    Lag justeringsfaktorar for lysforhold frå GLM,
    med valfri damping for å unngå overtolking.
    """

    params = model.params
    lys_justering = {referanse: 1.0}

    for k, beta in params.items():
        if k.startswith("C(lyskategori)[T."):
            lys = k.split("[T.")[1].rstrip("]")
            # Demp effekten
            beta_dempet = damping * beta
            lys_justering[lys] = np.round(float(np.exp(beta_dempet)))

    if normaliser:
        mean_factor = np.mean(list(lys_justering.values()))
        lys_justering = {
            k: v / mean_factor for k, v in lys_justering.items()
        }

    return lys_justering


