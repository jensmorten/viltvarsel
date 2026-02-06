🐾 Viltfrekvens – dyrepåkjørsler per vegstrekning

Dette prosjektet utviklar ein datadreven modell for risiko for dyrepåkjørsler på norske vegstrekningar. Målet er å gi eit enkelt, samanliknbart risikomål per vegstrekning, og på sikt legge til situasjonsbaserte justeringar (årstid, lysforhold m.m.) som kan nyttast i sanntid i bil (infotainment / varsling).

👉 Live demo (Streamlit-app):
https://viltfrekvens.streamlit.app/

🎯 Føremål

Prosjektet svarar på spørsmålet:

Kor stor er risikoen for dyrepåkjørsel for ein enkelt bil når ein køyrer på ei gitt vegstrekning – og korleis varierer denne risikoen med forhold som årstid og lys?

I staden for å telje absolute tal på påkjørsler, blir risikoen normalisert for trafikkmengde og veglengd, slik at ein kan samanlikne vegstrekningar på ein meiningsfull måte.

📊 Grunnfrekvens

Kjernen i modellen er ein grunnfrekvens per vegstrekning, definert som:

forventa tal på dyrepåkjørsler per 100 køyretøykilometer per år

Dette blir berekna som:

$$
\text{frekvens}
=
\frac{
  \text{antall kollisjonar}
}{
  \text{ÅDT}
  \times 365
  \times \text{veglengd (km)}
}
\times 100
$$


Grunnfrekvensen:

er spesifikk for kvar vegstrekning

fangar implisitt opp vegtype, landskap, vilttettleik m.m.

gir eit stabilt utgangspunkt for vidare justeringar

🔧 Justeringar (prediksjon)

Oppå grunnfrekvensen kan ein leggje justeringsfaktorar basert på forhold som varierer over tid, og som er tilgjengelege ved prediksjonstidspunkt.

Per i dag er desse implementerte:

🌱 Årstid

Basert på ein negativ binomial-regresjon med eksponering (trafikk × lengd), blir grunnfrekvensen justert svakt opp eller ned etter årstid.

Justeringane er:

konservative

normaliserte (gjennomsnitt = 1)

meint for samanlikning, ikkje absolutte prognosar

☀️ Lysforhold (på veg inn)

Prosjektet utforskar bruk av solhøgde (grader) som kontinuerleg forklaringsvariabel i staden for grove kategoriar som «dag/natt».

Analysen viser:

høgast risiko ved låg sol

særleg tydeleg effekt om våren

svak, men konsistent samanheng

Dette legg grunnlag for dynamiske justeringar basert på tidspunkt og stad.

🧠 Modellval

Negativ binomial-regresjon er brukt i staden for Poisson, grunna overdispersjon

Eksponering (trafikk × veglengd) blir handtert via offset

Modellen er bevisst enkel og robust, tilpassa operativ bruk

Fokuset er:

rett retning på effektar

stabilitet

biologisk og trafikksikkerheitsfagleg plausibilitet

🚗 Samanlikning med yrkesrisiko (illustrativ)

For å gjere tala meir intuitive blir frekvensen omrekna til årleg risiko per bil, basert på ein føresetnad om:

15 000 km køyring per år

éin kollisjon ≈ éi melde arbeidsulukke (illustrativt)

Denne årsrisikoen blir samanlikna med melde arbeidsulukker per årsverk i ulike yrke (SSB), og brukt som ei pedagogisk skala, ikkje ei presis risikovurdering.

🗺️ Datakjelder

Dyrepåkjørsler: Hjorteviltregisteret

Vegnett og trafikk: Nasjonal vegdatabank (NVDB)

Sol og lys: Astronomiske berekningar (solhøgde)

🚧 Status og vidare arbeid

Dette er eit pågåande prosjekt. Planlagde steg:

betre handtering av lysforhold (inkl. låg sol / blending)

fleire dynamiske variablar (føre, vêr)

sanntidsbruk i bil

vidare validering mot uavhengige data

📜 Ansvarsfråsegn

Dette verktøyet er:

ikkje ein offisiell trafikksikkerheitsvurdering

meint for analyse, samanlikning og forsking

ikkje ein garanti for faktisk risiko i den enkelte situasjon

💬 Kontakt / bidrag

Innspel, kritikk og forslag er svært velkomne.
Prosjektet er ope for vidareutvikling og fagleg diskusjon.
