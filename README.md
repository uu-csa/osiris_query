# OSIRIS queries via odbc
*Tools om OSIRIS queries te definiëren, uit te voeren en op te slaan*

---

[L.C. Vriend](https://github.com/lcvriend/)

Met deze tool kun je de OSIRIS tabellen via SQL benaderen en de resultaten voor verdere bewerking in [pandas](https://pandas.pydata.org/) als `DataFrame` opslaan. Bovendien zorgt de tool voor een gestructureerde manier om je queries en resultaten te organiseren. In de basis bestaat de tool uit de volgende onderdelen:

| Onderdeel  |
| ---------- |
| Definities |
| Resultaten |
| Scripts    |
| Code       |

De query definities worden beheerd in de map 'definitions'. Met behulp van de scripts kunnen de queries worden uitgevoerd. De resultaten worden vervolgens in de map 'output' opgeslagen.

---

## Installatie

De koppeling naar OSIRIS gebeurt op basis van [pyodbc](https://github.com/mkleehammer/pyodbc/wiki). De geïnstalleerde odbc-driver op UU-computers is standaard 32-bit. Om deze driver te kunnen gebruiken, moet je een 32-bit versie van Python gebruiken. Dit kan door binnen Anaconda een virtual environment o

**LET OP** p te zetten. Deze envkun je installeren via:create_32_bit_env.bat`](https://github.com/zijn uucsa/odeze toolblob/mastr/create_32_bit_env.bat Indien je voor een bepaalde tabel de kolomnamen wilt opvragen start dan [`run_query`]()). 

---

## Query definities
Een query definitie bestaat in essentie uit een SQL statement. Daarnaast wordt in de definitie aanvullende gegevens over de query opgeslagen volgens een vaste structuur. [Hier](https://github.com/uu-csa/osiris_query/blob/master/queries/_template_.ini) vind je een geannoteerd voorbeeld van hoe een dergelijk configuratiebestand eruit ziet.

In het kort stelt bevat de query definitie de volgende elementen:

> **Definition**
> * Naam van de query
> * De bestandsnaam waaronder de data moet worden opgeslagen
> 
> **Meta**
> * Een omschrijving van de query (optioneel)
> * Het type query (optioneel)
> 
> **Query**
> * SQL statement
> 
> **Columns**
> * De kolomnamen (optioneel)
> * De datatypen van de kolommen (optioneel)
> 
> **Parameters**
> * De namen van de parameters (optioneel)
> * Het datatype van de parameters (optioneel)

De query definitie wordt bij het uitvoeren van de query bij de resultaten opgeslagen. Dit stelt je dus later in staat om tijdens het bewerken van je data altijd precies terug te zien waar de data vandaan komt.

### Query parameters
Het is mogelijk om parameters op te nemen in een query definitie. Dit stelt je in staat om één query voor bijv. meerdere collegejaren/examentypes/faculteiten/aanmeldprocessen/etc. te hergebruiken zonder elke variatie afzonderlijk in te richten. Een parameter bestaat uit een naam tussen blokhaken: [parameter] en kan binnen de query definitie voorkomen in de volgende velden:
* naam
* bestandsnaam
* omschrijving
* query

Parameters hebben extra functionaliteit om de gebruiker meer flexibiliteit te geven:

> #### Optellen/aftrekken
> Als de parameter verwijst naar een getal (zoals bv. een collegejaar) dan kun je binnen de blokhaken simpele optel- en aftreksommen maken. Dit doe je door achter de parameternaam maar binnen de blokhaken +/- `n` toe te voegen, waarbij `n` verwijst naar het getal dat je bij de waarde wilt optellen of aftrekken:
> Notatie          | Parameter | Operator | n   | Resultaat
> :--------------- | --------: | :------: | --: | --------:
> [collegejaar+1]  | 2019      | +        | 1   | 2020
> [collegejaar-12] | 2019      | -        | 12  | 2007
>
> #### Slice-notatie
> Het is mogelijk om slechts een deel van de waarde van een parameter te selecteren met behulp van de zogenaamde [slice](https://docs.python.org/3/library/functions.html?highlight=slice#slice)-notatie. Geef achter de parameternaam maar binnen de blokhaken tussen ronde haken het startkarakter en eindkarakter op, gescheiden door een dubbele punt:
> Notatie            | Parameter | Slice | Resultaat
> :----------------- | --------: | :---: | --------:
> [collegejaar(:)]   | 2019      | (:)   | 2019
> [collegejaar(1:2)] | 2019      | (1:2) | 01
> [collegejaar(:3)]  | 2019      | (:3)  | 20
> [collegejaar(2:)]  | 2019      | (2:)  | 19

---

## Query resultaten
Op basis van de query definitie worden resultaten uit de query database opgehaald. Deze resultaten worden als pakketje opgeslagen. Het pakketje bevat:

1. De data als `DataFrame` (verder te bewerken met [pandas](https://pandas.pydata.org/))
2. De query definitie met alle metadat.

Dit pakketje wordt via de [pickle](https://docs.python.org/3/library/pickle.html?highlight=pickle#module-pickle) routine opgeslagen in `.pkl` formaat in de 'output' folder. De naam van het bestand is opgebouwd volgens het formaat van de bestandsnaam die in de query definitie is opgegeven. Dit formaat kan ook parameters bevatten, bijv:

`inschrijfverzoeken_[collegejaar]_[examentype]_[faculteit]`.

 Daarnaast kun je in de bestandsnaam definiëren of de resultaten in een specifieke map geplaatst moeten worden:

`<folder>/<bestandsnaam>`

---

## Code
De code bevat de volgende modules:
* definition
* execution
* results

Deze modules worden doorgaans via [scripts](#queries-uitvoeren) opgeroepen. De eindgebruiker zal in de meeste situaties niet direct met deze objecten hoeven te werken.

### Definition
De basis van deze module is de [`QueryDef`](https://github.com/uu-csa/osiris_query/blob/master/src/querydef.py) class. Deze class regelt alles rondom de query definitie:

* Inlezen van de query definities. Deze zijn doorgaans in de [map 'definitions'](https://github.com/uu-csa/osiris_query/tree/master/queries) opgeslagen.
* Updaten van de query definities op basis van de opgegeven parameters.

Indien een query definitie parameters bevat dan kun je deze pas correct uitvoeren als de aanwezige parameters een waarde hebben gekregen. Binnen Python gebeurt dit door het `QueryDef` object de parameters met hun waarden als `dict` te voeden:

```Python
from query.definition import QueryDef

# laad de query definitie
qd = QueryDef.from_ini("inschrijfverzoeken")

# geef de parameters een waarde
parameters = {
    'collegejaar': 2019,
    'examentype': 'BA',
    'faculteit': 'REBO',
}

# vul de parameters in de definitie
qd(parameters)
```

### Execution
Deze module verzorgt de executie van de query door de verbinding met de database te leggen, vervolgens de SQL query uit te voeren en tot slot de opgehaalde resultaten op te slaan. De functie `run_query` legt de verbinding tussen de `QueryDef` (zie hierboven) en de `QueryResult` (zie hieronder) objecten.

### Results
De basis van deze module is de [`QueryResult`](https://github.com/uu-csa/osiris_query/blob/master/src/query.py) class. Je kunt opgeslagen resultaten als volgt laden:

```Python
from query.results import QueryResult

result = QueryResult.read_pickle("inschrijfverzoeken")
```

Het `QueryResult` object heeft de volgende attributen:
- frame: `DataFrame` (data object)
- nrecords: `int` (aantal records)
- timer: `float` (query tijd in seconden)
- dtime: `datetime` (datum waarop de query is uitgevoerd)
- qd: `QueryDef` (query definitie object)

---

## Queries uitvoeren

Om een of meerdere queries uit te voeren kun je [`run_query.bat`](https://github.com/uu-csa/osiris_query/blob/master/run_query.bat) gebruiken. Deze start het [`run_query.py`](https://github.com/uu-csa/osiris_query/blob/master/run_query.py) script. Het script geeft je een keuze menu waarin je een query set kunt selecteren. Een query set bestaat uit een verzameling query definities binnen een map in 'definities'. Door meerdere gekoppelde query definities in één map te plaatsen, kun je ze iaw gebundeld uitvoeren.

 Nadat je een keuze hebt gemaakt, zal het script je vragen om de benodigde parameters in te vullen. Daarna worden de queries uitgevoerd en de resultaten opgeslagen in de betreffende folders (op basis van de gedefinieerde bestandsnamen).

![run_query.bat](run_query.png?raw=true "OSIRIS query")

---

## Ad hoc scripts

Indien een query set alleen uitgevoerd kan worden met behulp van complexere parameter logica dan zul je daar zelf een script voor op moeten stellen. Een template dat je kunt gebruiken als basis vind je onder [`query_template.py`](https://github.com/uu-csa/osiris_query/blob/master/query_template.py).

Start de omgeving vervolgens als volgt op (evt. via het [`run_env.bat`](https://github.com/uu-csa/osiris_query/blob/master/run_env.bat) commando bijgevoegd in deze repo):

> `set CONDA_FORCE_32BIT = 1`  
> `activate py32`

In de command prompt kun je het script met het volgende commando uitvoeren:

> `python.exe <naam script> <evt. argumenten>`

---

## Database explorer

Bij het opstellen van queries is het handig om te weten welke tabellen er beschikbaar zijn. Je kunt hiervoor de [`database_explorer`](https://github.com/uu-csa/osiris_query/blob/master/database_explorer.ipynb) gebruiken. Met deze notebook kun je op basis van een string zoeken in de OSIRIS tabellen. Daarnaast kun je voor een tabel de kolomnamen opvragen. 

**LET OP** De kolomnamen kunnen alleen worden opgeroepen als deze al een keer zijn opgehaald met behulp van **OSIRIS_query**. Indien je voor een bepaalde tabel de kolomnamen wilt opvragen, start dan [`run_query`](https://github.com/uu-csa/osiris_query/blob/master/run_query.py) op. Selecteer vervolgens de optie 'osiris_table_column_names' en geef de naam van de tabel op. Vervolgens worden de kolomnamen van de opgegeven tabel opgehaald en opgeslagen. Daarna zijn ze raadpleegbaar in de `database_explorer`.
