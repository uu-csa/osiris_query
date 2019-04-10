# set CONDA_FORCE_32BIT = 1
# activate py32

import timeit
import pickle
import argparse
import pandas as pd
from src import query


def set_criteria(items):
    return ["is null" if item is None else f"= '{item}'" for item in items]

def combine_criteria(items):
    return [f"(s_ooa.STATUS {item[0]} and s_ooa.BESLUIT {item[1]})" for item in items]


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


# RUN SQL QUERY
table = 's_ooa_dos'
sql = query.read_sql(table, parameters=parameters)
sec = query.query(f"{table}_{parameters['proces']}_{key}", sql, remove_dup=True)
