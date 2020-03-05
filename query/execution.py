# standard library
import sys
import json
import timeit
from collections import Iterable

# third party
import pandas as pd
import pyodbc

# local
from query.config import PATHS
from query.definition import QueryDef
from query.results import QueryResult
from query.utils import reporter, getpw, get_credentials


@reporter
def run_query(qd, cursor=None, save=True):
    """
    Run query and return results.

    Read QueryDef.
    - Lookup login details.
    - Connect to database.
    - Execute sql statement.
    - Fetch records.

    Return QueryDef
    - (Optionally) rename columns.
    - (Optionally) recast dtypes.
    - (Optionally) save results to disk.

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
    :QueryResult:
    """

    # connection
    if not cursor:
        cursor = connect()

    # fetch records
    start = timeit.default_timer()
    df = execute(cursor, qd)
    stop = timeit.default_timer()
    seconds = stop - start

    # store results
    q = QueryResult(qd, df, seconds)
    if save:
        q.to_pickle()
    return q


def execute(cursor, qd):
    """
    Execute sql statement from `qd`. Return dataframe.

    If the sql statement throws an error, the error message will be caught
    and printed. An empty dataframe is returned.

    Parameters
    ==========
    :param cursor: `cursor`
        ODBC-connection to the database.
    :param qd: `QueryDef`
        Instance of `QueryDef` containing the query definition.

    Return
    ======
    :pd.DataFrame:
    """

    try:
        cursor.execute(qd.sql)
        if isinstance(qd.columns, dict):
            cols = qd.columns.keys()
            dtypes = {k: v for k, v in qd.columns.items() if v is not None}
        else:
            cols = qd.columns
            dtypes = None

        if not cols:
            cols = [column[0] for column in cursor.description]

        df =  pd.DataFrame.from_records(
            cursor.fetchall(),
            columns=cols,
        )

        if dtypes:
            df = df.astype(dtypes)
        return df

    except pyodbc.Error:
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

        except_type, value, traceback = sys.exc_info()
        print(except_type)
        print(value.args[1].split('\n')[0])
        print()
        return pd.DataFrame()


def connect():
    "Connect to query database."

    # get login credentials
    creds = get_credentials(PATHS.login)

    # log on to database
    param = f'DSN={creds.dsn};UID={creds.uid};PWD={creds.pwd};CHARSET=UTF8'
    conn = pyodbc.connect(param)
    return conn.cursor()
