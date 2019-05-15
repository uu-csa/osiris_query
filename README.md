# OSIRIS queries via odbc
*Tools om OSIRIS queries te definiëren, uit te voeren en op te slaan*

---

L.C. Vriend

Met deze tool kun je de OSIRIS tabellen via SQL benaderen en de resultaten voor verdere bewerking als `DataFrame` opslaan. Bovendien zorgt de tool voor een gestructureerde manier om de queries en de resultaten te organiseren. In de basis bestaat de tool uit de volgende onderdelen:

| Onderdeel  |
| ---------- |
| Definities |
| Resultaten |
| Scripts    |
| Code       |

De query definities worden beheerd in de map 'queries'. Met behulp van de scripts kunnen de queries worden uitgevoerd. De resultaten worden vervolgens in de map 'output' opgeslagen.

---

## Query definities
Een query is in de simpelste vorm een SQL statement in een text-bestand. Het is echter mogelijk om aanvullende informatie aan de query toe te voegen. Hiervoor dien je de query te definiëren volgens een vaste structuur. In [dit voorbeeld](https://github.com/uu-csa/osiris_query/blob/master/queries/_template_.ini) staat een uitgebreide toelichting hoe een dergelijk configuratiebestand eruit ziet.

In het kort stelt deze query definitie je in staat om de volgende elementen aan de SQL toe te voegen:

**Metadata**
* Een omschrijving van de query
* Het type query

**Configuratie van de outputtabel**
* De kolomnamen
* De datatypen van de kolommen

De **metadata** wordt later bij het uitvoeren van de query bij de resultaten opgeslagen. Tijdens het bewerken van de data is deze informatie daarom op elk moment op te raadplegen. De **output configuratie** maakt het mogelijk om de query resultaten op een efficiënte manier op te slaan. Bovendien kun je bepaalde bewerkingen in deze fase al uitvoeren.

### Templates
Het is mogelijk om variabelen op te nemen in een query definitie. Dit stelt je in staat om één query voor bijv. meerdere collegejaren of meerdere aanmeldprocessen te hergebruiken. Een variabele bestaat uit een naam tussen blokhaken: [variabele].

> Het is mogelijk om slechts een deel van de waarde van een variabele te selecteren met behulp van de zogenaamde [slice](https://docs.python.org/3/library/functions.html?highlight=slice#slice)-notatie. Geef achter de variabelenaam maar binnen de blokhaken tussen ronde haken het startkarakter en eindkarakter op, gescheiden door een dubbele punt.

#### Enkele voorbeelden van slice-notatie
Variabele | Slice | Resultaat
--------- | :---: | --------:
2019      | (:)   | 2019
2019      | (1:2) | 01
2019      | (:3)  | 20
2019      | (2:)  | 19

---

## Query resultaten
De output wordt via de [pickle](https://docs.python.org/3/library/pickle.html?highlight=pickle#module-pickle) routine opgeslagen in `.pkl` formaat. Het outputbestand bevat:

1. De data als `DataFrame` (verder te bewerken met [Pandas](https://pandas.pydata.org/))
2. De query definitie met metadata

Deze gegevens zijn met de `read_pickle` functie in de `query` module in te laden.

---

## Code
De code is opgebouwd rond twee objecten (`classes`):

1. `QueryDef`
2. `Query`

Deze objecten worden via scripts opgeroepen.

---

## Scripts

De koppeling naar OSIRIS gebeurt op basis van [pyodbc](https://github.com/mkleehammer/pyodbc/wiki). De geïnstalleerde odbc-driver op UU-computers is 32-bit. Om deze driver te kunnen gebruiken, moet je een 32-bit versie van Python gebruiken. Dit kan door binnen Anaconda een virtual environment op te zetten. Start de omgeving vervolgens als volgt op (evt. via het [`run_env.bat`](https://github.com/uu-csa/osiris_query/blob/master/run_env.bat) commando bijgevoegd in deze repo):

> `set CONDA_FORCE_32BIT = 1`  
> `activate py32`

In de command prompt kun je een script met het volgende commando uitvoeren:

> `python.exe <naam script> <argumenten>`

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
5. tabellen_betaalmail.py - verplicht argument: collegejaar
6. tabellen_ad_hoc.py
