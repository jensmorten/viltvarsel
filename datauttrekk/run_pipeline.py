#!/usr/bin/env python3
"""
Kjør hele datauttrekk-pipelinen i riktig rekkefølge.
Resultatet er Fallvilt_tidspunkter.csv som brukes av applikasjonen.

Forutsetninger:
  - Python-miljø med alle avhengigheter installert (se ../requirements.txt)
  - .env-fil i denne mappen med clientID og clientSecret for Frost API (met.no)

Kjøring:
  cd datauttrekk/
  python run_pipeline.py
"""

import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent

STEPS = [
    ("Steg 1/7 – Hent fallvilt fra API",            "get_fallvilt.py"),
    ("Steg 2/7 – Berik med NVDB-posisjon",          "enrich_fallvilt_with_nvdb_position.py"),
    ("Steg 3/7 – Berik med vegobjekter (ÅDT m.m.)", "vegobjekter_enrichment.py"),
    ("Steg 4/7 – Hent ÅDT-vegobjektlengder",        "adttotal_vegobjektlengde_enrichment.py"),
    ("Steg 5/7 – Berik med værdata (Frost API)",     "weather_enrichment.py"),
    ("Steg 6/7 – Beregn månedsmiddel vær",           "calc_avg_montly_weather.py"),
    ("Steg 7/7 – Berik med eksakt hendelsestidspunkt","tidspunkt_enrichment.py"),
]


def run_step(label: str, script: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  → {script}")
    print(f"{'='*60}")
    t0 = time.time()
    result = subprocess.run(
        [sys.executable, str(HERE / script)],
        cwd=HERE,
    )
    elapsed = time.time() - t0
    if result.returncode != 0:
        print(f"\n❌ Feil i {script} (returkode {result.returncode}). Pipeline stoppet.")
        sys.exit(result.returncode)
    print(f"✅ Ferdig på {elapsed:.1f}s")


def main() -> None:
    print("🚀 Starter datauttrekk-pipeline")
    total_start = time.time()

    for label, script in STEPS:
        run_step(label, script)

    total = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"🎉 Pipeline fullført på {total/60:.1f} min")
    print(f"   Resultat: {HERE / 'Fallvilt_tidspunkter.csv'}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
