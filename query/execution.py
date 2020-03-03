# standard library
import sys
import json
import timeit

# third party
import pandas as pd
import pyodbc

# local
from query.config import PATHS
from query.definition import QueryDef
from query.results import QueryResult
from query.utils import reporter, getpw, get_credentials


def query(qd, cursor=None):
    """
    Store data from query in a dataframe.

    Read QueryDef.
    Run sql-query on the OSIRIS database.
    - Lookup login details from u:/uustprd.txt
    - Connect to database
    - Fetch records
    Return dataframe and query time in seconds.
    - (Optionally) Rename columns
    - (Optionally) Recast dtypes

    Parameters
    ==========
    :param qd: `QueryDef`
        Instance of `QueryDef` containing the query definition.

    Optional parameters
    ===================
    :param cursor: `cursor`, default `None`
        ODBC-connection to the database.
        If None the constructor will establish connection.

    Return
    ======
    :tuple: dataframe, seconds as `float`
    """

    # connection
    if not cursor:
        cursor = connect()

    # fetch records
    start = timeit.default_timer()
    try:
        cursor.execute(qd.sql)
    except:
        print(
            "\n"
            " ____ ____ _  _ ___  _  _ ___ ____ ____  "
            "  ____ ____ _   _ ____    _  _ ____\n"
            " |    |  | |\/| |__] |  |  |  |___ |__/  "
            "  [__  |__|  \_/  [__     |\ | |  |\n"
            " |___ |__| |  | |    |__|  |  |___ |  \  "
            "  ___] |  |   |   ___]    | \| |__|\n"
            "\n"
        )
        print(f"Query '{qd.name}' failed. The following error was returned:\n")

        for item in sys.exc_info():
            print(item)
        return pd.DataFrame(), 0

    if isinstance(qd.columns, dict):
        cols = qd.columns.keys()
        dtypes = {k: v for k, v in qd.columns.items() if v is not None}
    else:
        cols = qd.columns
        dtypes = None

    if not cols:
        cols = [column[0] for column in cursor.description]

    df = pd.DataFrame.from_records(
        cursor.fetchall(),
        columns=cols,
    )

    if dtypes:
        df = df.astype(dtypes)
    stop = timeit.default_timer()
    seconds = stop - start

    return df, seconds


def connect():
    # get login credentials
    creds = get_credentials(PATHS.login)

    # log on to database
    param = f'DSN={creds.dsn};UID={creds.uid};PWD={creds.pwd};CHARSET=UTF8'
    conn = pyodbc.connect(param)
    return conn.cursor()


@reporter
def run_query(qd, save=True, cursor=None):
    results = query(qd, cursor=cursor)
    q = QueryResult(qd, *results)
    if save:
        q.to_pickle()
    return q
