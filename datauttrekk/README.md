# Datauttrekk – pipeline

Skriptene her produserer `Fallvilt_tidspunkter.csv`, som er grunnlaget for alle frekvens- og risikoberegninger i applikasjonen.

## Hurtigstart

```bash
# 1. Opprett .env-fil med Frost API-nøkler (se Forutsetninger nedenfor)
# 2. Kjør hele pipelinen:
cd datauttrekk/
python run_pipeline.py
```

Scriptet kjører alle steg i riktig rekkefølge, skriver ut fremgang og stopper med feilmelding dersom et steg feiler.

## Forutsetninger

- Python-miljø med alle avhengigheter fra `../requirements.txt`
- En `.env`-fil i `datauttrekk/`-mappen med følgende innhold:
  ```
  clientID=<din Frost API-klient-ID>
  clientSecret=<din Frost API-klient-hemmelighet>
  ```
  API-nøkler fås gratis fra [frost.met.no](https://frost.met.no/auth/requestCredentials.html).

## Pipeline-steg

| Steg | Skript                                   | Inn                            | Ut                             |
| ---- | ---------------------------------------- | ------------------------------ | ------------------------------ |
| 1    | `get_fallvilt.py`                        | API (hjorteviltregisteret.no)  | `Fallvilt.csv`                 |
| 2    | `enrich_fallvilt_with_nvdb_position.py`  | `Fallvilt.csv`                 | `Fallvilt_nvdb_enriched.csv`   |
| 3    | `vegobjekter_enrichment.py`              | `Fallvilt_nvdb_enriched.csv`   | `Fallvilt_vegobjekter.csv`     |
| 4    | `adttotal_vegobjektlengde_enrichment.py` | `Fallvilt_vegobjekter.csv`     | `Fallvilt_adttotallengder.csv` |
| 5    | `weather_enrichment.py`                  | `Fallvilt_adttotallengder.csv` | `Fallvilt_beriket_med_vær.csv` |
| 6    | `calc_avg_montly_weather.py`             | `Fallvilt_beriket_med_vær.csv` | `Fallvilt_månedsberiket.csv`   |
| 7    | `tidspunkt_enrichment.py`                | `Fallvilt_månedsberiket.csv`   | `Fallvilt_tidspunkter.csv` ✅  |

Den ferdige `Fallvilt_tidspunkter.csv` må kopieres til `../data/` for å brukes av applikasjonen.

## Merknader

- `veglenkesekvenslengde_enrichment.py` og `combined_vegobjekter_enrichment.py` er sidegrener og inngår ikke i hovudkjeden over.
- Steg 1 henter kun data fra 2025 og frem til i dag (Trøndelag, årsak: PåkjørtAvMotorkjøretøy). For å hente et annet tidsrom, endre `fra_dato` i `get_fallvilt.py`.
- Steg 5 gjør mange API-kall mot Frost og kan ta lang tid for store datasett.
