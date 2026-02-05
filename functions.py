import pandas as pd


def map_arsrisiko_til_yrke(arsrisiko):
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