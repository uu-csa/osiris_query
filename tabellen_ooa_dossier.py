import timeit
start = timeit.default_timer()

import argparse
import pandas as pd
import src.query as qry
from src.querydef import QueryDef


def set_criteria(items):
    return ["is null" if item is None else f"= '{item}'" for item in items]

def combine_criteria(items):
    return [f"(s_ooa_aan.STATUS {item[0]} and s_ooa_aan.BESLUIT {item[1]})" for item in items]


# PARSE COMMAND LINE ARGUMENTS
parser = argparse.ArgumentParser()
parser.add_argument(
    'proces',
    help='Naam van het OOA-proces.'
    )
parser.add_argument(
    'status_besluit',
    choices=['I', 'A', 'T'],
    help=(
        'I: Ingediende formulieren\n'
        'A: Afgehandelde formulieren\n'
        'T: Alle ingevulde formulieren'
        )
    )
args = parser.parse_args()

# G:Geannuleerd,
# F:Afgehandeld,
# B:In behandeling,
# I:Ingediend,
status = {
    'I': ['I', 'B',],
    'A': ['G', 'F',],
    'T': ['I', 'B', 'G', 'F',],
}

# A:Afgewezen,
# T:Voldaan,
besluit = {
    'I': [None, None,],
    'A': ['T', 'T',],
    'T': [None, None, 'T', 'T',],
}

key = args.status_besluit
status = set_criteria(status[key])
besluit = set_criteria(besluit[key])
status_besluit = combine_criteria(list(zip(status, besluit)))
criterium = f"({' or '.join(status_besluit)})"

parameters = {
    'proces': args.proces,
    'status_besluit': criterium
}


# CONNECT TO DATABASE
cursor = qry.connect()


# RUN SQL QUERY
query = 's_ooa_dos'
qd = QueryDef(f"{query}", parameters=parameters)
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
