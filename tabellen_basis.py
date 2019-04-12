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
    ('s_sih', False, [
        's_sih.OPLEIDING',
        's_sih.CROHO',
        's_sih.ACTIEFCODE_OPLEIDING_CSA',
        's_sih.INSCHRIJVINGSTATUS',
        's_sih.EXAMENTYPE_CSA',
        's_sih.VOLTIJD_DEELTIJD',
        's_sih.LOTINGVORM',
        's_sih.DEELNAME_STUDIEKEUZECHECK',
        's_sih.RESULT_STUDIEKEUZECHECK',
        's_sih.TOELAATB_STUDIEKEUZECHECK',
        's_sih.TOELAATBAAR_QUA_AMD',
        's_sih.BETAALVORM',
        ]),
    ('s_opl', False, [
        's_opl.AANVANGSJAAR_OPLEIDING'
        ]),
    ('s_stud', True, [
        's_stud.NATIONALITEIT',
        's_stud.NATIONALITEIT2',
        's_stud.SL_INGELOGD_DIGID'
        ]),
    ('s_stop', False, [
        's_stop.CRITERIUM', 's_stop.KLEUR', 's_stop.TOELICHTING'
        ]),
    ('s_adr', True, [
        's_adr.ADRESTYPE'
    ]),
    ('s_ooa_aan', False, [
        's_ooa_aan.OPLEIDING',
        's_ooa_aan.IO_PROCES',
        's_ooa_aan.STATUS',
        's_ooa_aan.STATUS_AANBIEDING',
        's_ooa_aan.BESLUIT',
        ]),
    ('s_ooa_rub', False, [
        's_ooa_rub.IO_PROCES',
        's_ooa_rub.HOOFDSTUK',
        's_ooa_rub.STATUS',
        ]),
    ('s_fin', False, None),
]


# RUN SQL QUERY
for tup in tables:
    table = tup[0]
    sql = query.read_sql(table, parameters=parameters)
    sec = query.query(
        f"{table}_{parameters['collegejaar']}",
        sql,
        remove_dup=tup[1],
        categoricals=tup[2],
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
