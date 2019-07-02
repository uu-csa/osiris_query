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
    'vooropleiding/s_vopl',
    'vooropleiding/s_vak',
    'monitor/s_ooa_aan',
    'monitor/s_sih',
    'monitor/s_stop',
]

# RUN QUERIES
for query in queries:
    run_query(query, cursor=cursor, parameters=parameters)

# STOP TIMER AND PRINT RUNTIME
stop = timeit.default_timer()
sec = stop - start
print(f"\n{'=' * 80}\nTotal runtime: {sec} seconds.")
