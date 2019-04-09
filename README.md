# OSIRIS queries via odbc
Met deze scripts kun je OSIRIS tabellen via odbc benaderen. De output wordt opgeslagen als een datapack object in een `.pkl` bestand. Deze datapack bevat:

1. De data als DataFrame (verder te bewerken met pandas)
2. De naam van de tabel
3. De sql die gebruikt is om de data op te halen
4. De tijd/datum waarop de data is opgehaald
5. De tijd die het kostte om de data op te halen
6. De historie van eventuele eerdere operaties op de data

Deze gegevens zijn met de `query` module te benaderen.

---

## Scripts

1. tabellen_referentie.py
2. tabellen_basis.py - verplicht argument: collegejaar
3. tabellen_ooa_dossier.py - verplicht argument: procescode
