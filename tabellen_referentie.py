# set CONDA_FORCE_32BIT = 1
# activate py32

import timeit
import query
import pickle
import pandas as pd

# REFERENTIETABELLEN

def r_nat():
    # nationaliteiten
    table = 'r_nat'
    sql = query.read_sql(table)
    query.query(table, sql)

def r_opl():
    # opleidingen
    start = timeit.default_timer()
    table = 'r_opl'
    sql = query.read_sql(table)
    query.query(table, sql)

    qry = """
        fac_name = {
            'RA': 'UCR',
            'UC': 'UCU',
            'IVLOS': 'GST',
        }
        df.loc[:, 'FACULTEIT'] = df['FACULTEIT'].replace(to_replace=fac_name)
    """

    pack = query.load_query('r_opl')
    df = pack['frame']
    source = pack['source']
    table = source[0]['table']

    fac_name = {
        'RA': 'UCR',
        'UC': 'UCU',
        'IVLOS': 'GST',
    }
    df.loc[:, 'FACULTEIT'] = df['FACULTEIT'].replace(to_replace=fac_name)

    stop = timeit.default_timer()
    sec = stop - start

    pack = query.pack_data(df, table, qry, sec, source=source)
    query.save_query(table, pack)

def r_ooa_sl():
    # sl-aanmeldprocessen
    table = 'r_ooa_sl'
    sql = query.read_sql(table)
    query.query(table, sql)

if __name__ == '__main__':
    r_nat()
    r_opl()
    r_ooa_sl()
