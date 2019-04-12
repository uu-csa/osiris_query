# set CONDA_FORCE_32BIT = 1
# activate py32

import timeit
start = timeit.default_timer()

import pickle
import argparse
import pandas as pd
from src import query


# PARSE COMMAND LINE ARGUMENTS
parser = argparse.ArgumentParser()
parser.add_argument(
    'collegejaar',
    help='Collegejaar waarvoor gegevens opgehaald worden.'
    )
arg = parser.parse_args()

parameters = {
    'collegejaar': arg.collegejaar,
}


# STUDENTENTABELLEN
tables = [
    ('s_sih', False),
    ('s_opl', False),
    ('s_stud', True),
    ('s_stop', False),
    ('s_adr', True),
    ('s_ooa_aan', False),
    ('s_ooa_rub', False),
    ('s_fin', False),
]


# RUN SQL QUERY
for tup in tables:
    table = tup[0]
    sql = query.read_sql(table, parameters=parameters)
    sec = query.query(
        f"{table}_{parameters['collegejaar']}", sql, remove_dup=tup[1]
        )


stop = timeit.default_timer()
sec = stop - start

print(f"\n{'=' * 80}\nTotal runtime: {sec} seconds.")


# # load query tables
# start = timeit.default_timer()

# qry = """
# df = df_sih.merge(df_sop, how='left', on='sih.SINH_ID')
# df['sop.AANVANGSJAAR_OPLEIDING'] = df['sop.AANVANGSJAAR_OPLEIDING'].fillna(2018)
# """

# pack = query.load_query(f'sih_{collegejaar}')
# df_sih = pack['frame']
# source = pack['source']
# table = source[0]['table']

# pack = query.load_query(f'sop_{collegejaar}')
# df_sop = pack['frame']
# table = source[0]['table']
# source.extend(pack['source'])

# # merge tables
# df = df_sih.merge(df_sop, how='left', on='sih.SINH_ID')
# col = 'sop.AANVANGSJAAR_OPLEIDING'
# df[col] = df[col].fillna(collegejaar)

# stop = timeit.default_timer()
# sec = stop - start
# print(table, sec)

# # pack data
# source = {
#     'source': source,
#     'table': table,
#     'query': qry,
#     'timer': sec,
# }
# pack = {
#     'source': [source],
#     'frame': df,
# }
# query.save_query(table, pack)
