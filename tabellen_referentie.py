# set CONDA_FORCE_32BIT = 1
# activate py32

import timeit
import pickle
import pandas as pd
import src.query as qry
from src.querydef import QueryDef


# CONNECT TO DATABASE
cursor = qry.connect()


# REFERENTIETABELLEN
def r_nat():
    # nationaliteiten
    table = 'referentie/r_nat'
    qd = QueryDef(table)
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

def r_opl():
    # opleidingen
    start = timeit.default_timer()
    table = 'referentie/r_opl'
    qd = QueryDef(table)
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

    source = (
        "fac_name = {"
        "    'RA': 'UCR',"
        "    'UC': 'UCU',"
        "    'IVLOS': 'GST',"
        "}"
        "df.loc[:, 'faculteit'] = df['faculteit'].replace(to_replace=fac_name)"
        )

    pack = qry.load_datapack('referentie/r_opl')
    df = pack.frame
    table = pack.table
    query = pack.query

    fac_name = {
        'RA': 'UCR',
        'UC': 'UCU',
        'IVLOS': 'GST',
        }
    df.loc[:, 'faculteit'] = df['faculteit'].replace(to_replace=fac_name)

    stop = timeit.default_timer()
    sec = stop - start

    pack = qry.pack_data(
        frame=df,
        table=table,
        qtype='REF',
        source=source,
        query=query,
        timer=sec,
        description=None,
        nrecords=len(df),
        )
    qry.save_datapack(pack)

def r_ooa_sl():
    # sl-aanmeldprocessen
    table = 'referentie/r_ooa_sl'
    qd = QueryDef(table)
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


if __name__ == '__main__':
    r_nat()
    r_opl()
    r_ooa_sl()
