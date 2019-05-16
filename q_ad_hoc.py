# set CONDA_FORCE_32BIT = 1
# activate py32

import timeit
import pickle
import argparse
import pandas as pd
from src import query


# PARSE COMMAND LINE ARGUMENTS
parser = argparse.ArgumentParser()
parser.add_argument(
    'sql',
    help='sql in ad_hoc om uit te voeren.'
    )
arg = parser.parse_args()


# RUN SQL QUERY
table = arg.sql
sql = query.read_sql(f'ad_hoc/{table}')
sec = query.query(
    f"ad_hoc_{table}", sql
    )
