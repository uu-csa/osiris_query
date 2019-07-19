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

## Installatie

De koppeling naar OSIRIS gebeurt op basis van [pyodbc](https://github.com/mkleehammer/pyodbc/wiki). De geïnstalleerde odbc-driver op UU-computers is 32-bit. Om deze driver te kunnen gebruiken, moet je een 32-bit versie van Python gebruiken. Dit kan door binnen Anaconda een virtual environment op te zetten. Deze environment kun je installeren via: [`create_32_bit_env.bat`](https://github.com/uu-csa/osiris_query/blob/master/create_32_bit_env.bat). 

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
De output wordt via de [pickle](https://docs.python.org/3/library/pickle.html?highlight=pickle#module-pickle) routine opgeslagen in `.pkl` formaat. De naam van het bestand is opgebouwd uit de naam van de query en indien aanwezig de gebruikte variabelen, bijv: `<query naam>_var_<variabele 1>_<variabele 2>`. Het outputbestand bevat:

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

## Queries uitvoeren

Om een of meerdere queries uit te voeren kun je [`run_query.bat`](https://github.com/uu-csa/osiris_query/blob/master/run_query.bat) gebruiken. Deze start het [`run_query.py`](https://github.com/uu-csa/osiris_query/blob/master/run_query.py) script. Het script geeft je een keuze menu waarin je een van de query sets kunt selecteren die gedefinieerd zijn in [`queries.json`](https://github.com/uu-csa/osiris_query/blob/master/config/queries.json) in de `config` folder. Nadat je een keuze hebt gemaakt, zal het script je vragen om de benodigde parameters in te vullen. Daarna worden de queries uitgevoerd en de resultaten opgeslagen in de betreffende folders.

### Zelf querysets toevoegen
Om zelf een of een serie queries toe te voegen aan het keuzemenu van `run_query` moet je de eerdergenoemde `queries.json` bewerken. Dit [`json`](https://nl.wikipedia.org/wiki/JSON) bestand is als volgt opgebouwd:

> Naam van de query set
>   * queries: lijst met verwijzing naar de querydefs (let op dat je ook naar de correcte folder verwijst)
>   * parameters: lijst met de parameters die gebruikt worden binnen de querydefs

Zorg ervoor dat de parameters die je hebt opgenomen gedefinieerd zijn in [`metaparam.json`](https://github.com/uu-csa/osiris_query/blob/master/config/metaparam.json). In dit bestand wordt vastgelegd hoe de parameter gebruikt moet worden. Het is als volgt opgebouwd:

> Naam van de parameter
>   * target: kan de waarde 'file' of de waarde 'querydef' hebben:
>       * 'file': deze parameter past de naam aan van de query die je wilt uitvoeren
>       * 'querydef': deze parameter past een variabele aan die binnen de query gebruikt worden
>   * description: een omschrijving van de parameter.
>   * type: welke soort waarde kan de parameter kan krijgen:
>       * 'str': de waarde is een string
>       * 'int': de waarde is een geheel getal (integer)

---

## Ad hoc scripts

Indien een query set alleen uitgevoerd kan worden met behulp van complexere parameter logica dan zul je daar zelf een script voor op moeten stellen. Een template dat je kunt gebruiken als basis vind je onder [`query_template.py`](https://github.com/uu-csa/osiris_query/blob/master/query_template.py).

Start de omgeving vervolgens als volgt op (evt. via het [`run_env.bat`](https://github.com/uu-csa/osiris_query/blob/master/run_env.bat) commando bijgevoegd in deze repo):

> `set CONDA_FORCE_32BIT = 1`  
> `activate py32`

In de command prompt kun je het script met het volgende commando uitvoeren:

> `python.exe <naam script> <evt. argumenten>`
