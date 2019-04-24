import timeit
start = timeit.default_timer()

import pickle
import argparse
import pandas as pd
import src.query as qry
from src.querydef import QueryDef


# PARSE COMMAND LINE ARGUMENTS
parser = argparse.ArgumentParser()
parser.add_argument(
    'collegejaar',
    help='Collegejaar waarvoor gegevens opgehaald worden.'
    )
arg = parser.parse_args()
parameters = vars(arg)

# CONNECT TO DATABASE
cursor = qry.connect()

# QUERIES TO RUN
queries = [
    'b_sih',
    'b_opl',
    'b_stop',
    'b_adr_nl',
    'b_ooa_aan',
    'b_fin_storno',
    'b_fin_grp',
]

# RUN QUERIES
for query in queries:
    qd = QueryDef(f"betaalmail/{query}", parameters=parameters)
    qry.query(
        qd.outfile,
        qd.sql,
        cursor=cursor,
        description = qd.description,
        qtype=qd.qtype,
        columns=qd.columns,
        dtypes=qd.dtypes,
        remove_duplicates=qd.remove_duplicates,
        )

# STOP TIMER AND PRINT RUNTIME
stop = timeit.default_timer()
sec = stop - start
print(f"\n{'=' * 80}\nTotal runtime: {sec} seconds.")
