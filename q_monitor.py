import timeit
start = timeit.default_timer()
import argparse
from src.querydef import QueryDef
from src.query import Query, connect, run_query


# PARSE COMMAND LINE ARGUMENTS
parser = argparse.ArgumentParser()
parser.add_argument(
    'collegejaar',
    help='Collegejaar waarvoor gegevens opgehaald worden.'
    )
arg = parser.parse_args()
parameters = vars(arg)

# CONNECT TO DATABASE
cursor = connect()

# QUERIES TO RUN
queries = [
    's_sih',
    's_opl',
    's_stud',
    's_stop',
    's_stat',
    's_adr',
    's_fin',
    's_fin_grp',
    's_fin_storno',
    's_ooa_aan',
]

# RUN QUERIES
for query in queries:
    run_query(f'monitor/{query}', cursor=cursor, parameters=parameters)

# STOP TIMER AND PRINT RUNTIME
stop = timeit.default_timer()
sec = stop - start
print(f"\n{'=' * 80}\nTotal runtime: {sec} seconds.")