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

De geïnstalleerde odbc-driver is 32-bit. Om deze driver te kunnen gebruiken, moet je een 32-bit versie van Python gebruiken. Dit kan door binnen Anaconda een virtual environment op te zetten. Start de omgeving vervolgens als volgt op:

> set CONDA_FORCE_32BIT = 1

> activate py32

Op dit moment zijn de volgende scripts beschikbaar.

1. tabellen_referentie.py
2. tabellen_basis.py - verplicht argument: collegejaar

    * s_sih: student_inschrijfhistorie
    * s_opl: student_opleiding
    * s_stu: student_student
    * s_adr: student_adres
    * s_ooa: student_aanmelddossiers
    * s_rub: student_aanmelddossiers_rubriekstatussen
    * s_fin: student_financiële_regels
3. tabellen_ooa_dossier.py - verplicht argument: procescode
4. tabellen_ad_hoc.py - verplicht argument: naam ad hoc sql
