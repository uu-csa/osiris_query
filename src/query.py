# set CONDA_FORCE_32BIT = 1
# activate py32

import pyodbc
import datetime
import timeit
import pickle
import functools
import pandas as pd
from .config import PATH_LOGIN, PATH_OUTPUT


def reporter(func):
    @functools.wraps(func)
    def wrapper_reporter(*args, **kwargs):
        args_repr = [a for a in args]
        kwargs_repr = [f'{k}={v!r}' for k, v in kwargs.items()]
        signature = '\n'.join(args_repr + kwargs_repr)
        print("=" * 80)
        print(f"CALLING {func.__name__!r}")
        print("=" * 80)
        print(f"{signature}")
        value = func(*args, **kwargs)
        print("-" * 80)
        print(f"{func.__name__!r} returned {value!r}\n")
        return value
    return wrapper_reporter


@reporter
def connect():
    # get login details
    login = PATH_LOGIN.read_text().split('\n')[1].split(',')
    uid = login[0]
    pwd = login[1]

    # log on to database
    param = f'DSN=UUSTPRD;DBQ=UUSTPRD;STPRD;UID={uid};PWD={pwd};CHARSET=UTF8'
    conn = pyodbc.connect(param)
    return conn.cursor()


@reporter
def query(
    table,
    sql,
    cursor=None,
    columns=None,
    dtypes=None,
    remove_duplicates=False
    ):
    """
    Run sql-query on the OSIRIS database and return the results as a DataFrame, pack and pickle metadata (table name, query, time) with the DataFrame.

    - Lookup login details from u:/uustprd.txt
    - Connect to database
    - Fetch records
    - (Optionally) Rename columns
    - (Optionally) Recast dtypes
    - (Optionally) Remove duplicate rows
    - Pack and pickle (meta)data

    Parameters
    ==========
    :param table: `string`
        Name for the output table (used in metadata and as name for the .pkl file).
    :param sql: `string`
        sql query.

    Optional parameters
    ===================
    :paramn cursor: odbc cursor, default None
        If None the function will establish connection.
    :param columns: list, default None
        List of column names. If None column names will be inferred from sql.
    :param dtypes: dict, default None
        Dict of column name / dtype pairs.
    :param remove_duplicates: boolean, default False
        Remove duplicates from output if True.

    Return
    ======
    :query: table name as `string`, sec as `float`
    """

    # connection
    if not cursor:
        cursor = connect()

    # fetch records
    start = timeit.default_timer()
    cursor.execute(sql)
    df = pd.DataFrame.from_records(cursor.fetchall(), columns=columns)
    if remove_duplicates:
        df = df.drop_duplicates()
    if dtypes:
        df = df.astype(dtypes)
    stop = timeit.default_timer()
    sec = stop - start

    pack = pack_data(df, table, sql, sec)
    save_datapack(table, pack)

    return table, sec


def pack_data(df, table, sql, sec, source=None):
    dtime = datetime.datetime.now()
    # collect meta-information
    source = {
        'source': source,
        'table': table,
        'query': sql,
        'dtime': dtime,
        'timer': sec,
    }
    # pack data
    pack = {
        'source': [source],
        'frame': df,
    }
    return pack


def save_datapack(table, pack):
    with open(PATH_OUTPUT / f'{table}.pkl', 'wb') as f:
        pickle.dump(pack, f)
    return None


def load_datapack(table):
    with open(PATH_OUTPUT / f'{table}.pkl', 'rb') as f:
        pack = pickle.load(f)
    return pack


def load_frame(table, strip_col=True):
    pack = load_datapack(table)
    frame = pack['frame']

    if strip_col:
        def strip(x):
            if '(' in x:
                sub = x[x.find('(') + 1:x.rfind(')')]
                if '.' in sub:
                    x = x.replace(sub, sub.split('.')[1])
            if '.' in x:
                x = x.split('.')[1]
            return x
        frame.columns = [strip(col) for col in frame.columns]

    return frame


def load_source(table):
    pack = load_datapack(table)
    return pack['source']
