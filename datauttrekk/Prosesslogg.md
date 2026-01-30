# Prosesslogg:

1. last ned datasett fra https://www.hjorteviltregisteret.no/fallviltinnsyn/liste/filter?alderskategorier=1,2,3,4&arsaker=1&arter=1,2,3,4,7,9,11,12,13,14,16&fromDate=2025-07-28&kjonn=1,2,3&omrader=50&toDate=2026-01-28&utfall=1,2,3,4,5,6,7
2. enrich med ådt total, ådt total objekt id og fartsgrense fra https://nvdbapiles.atlas.vegvesen.no/vegobjekter/api/v4/vegobjekter/{obj_id}
3. enrich med lengde for ådt total objekt id fra https://nvdbapiles.atlas.vegvesen.no/vegnett/api/v4/veglenkesekvenser
