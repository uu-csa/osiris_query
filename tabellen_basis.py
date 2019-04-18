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
    's_sih',
    's_opl',
    's_stud',
    's_stop',
    's_adr',
    's_ooa_aan',
    's_ooa_rub',
    's_fin',
]

# RUN QUERIES
for query in queries:
    qd = QueryDef(query, parameters=parameters)
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
