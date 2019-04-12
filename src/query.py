# set CONDA_FORCE_32BIT = 1
# activate py32

import os
import pyodbc
import datetime
import timeit
import pickle
import functools
import pandas as pd
import pprint as pp
from pathlib import Path

PATH_MOD = Path(__file__).resolve().parent.parent
PATH_QUERY = PATH_MOD / 'query_outputs'
PATH_SQL = PATH_MOD / 'sql'


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
def query(table, sql, cols=None, categoricals=None, remove_dup=False):
    """
    Run SQL-query on the OSIRIS database and return the results as a DataFrame, pack and pickle metadata (table name, query, time) with the DataFrame.

    - Lookup login details from u:/uustprd.txt
    - Connect to database
    - Fetch records
    - Pack and pickle (meta)data

    Parameters
    ==========
    :param table: `string`
        Name for the output table (used in metadata and as name for the .pkl file).
    :param sql: `string`
        SQL query.

    Optional parameters
    ===================
    :param cols: list, default None
        List of column names. If None column names will be inferred from sql.
    :param categoricals: list, default None
        List of column names to convert to categorical variables.
    :param remove_dup: boolean, default False
        Remove duplicates from output if True.

    Return
    ======
    :query: table name as `string`, sec as `float`
    """

    # get login details
    login = Path('u:/uustprd.txt').read_text().split('\n')[1].split(',')
    uid = login[0]
    pwd = login[1]

    # find column names
    if cols == None:
        cols = find_cols(sql)

    # log on to database
    param = f'DSN=UUSTPRD;DBQ=UUSTPRD;STPRD;UID={uid};PWD={pwd};CHARSET=UTF8'
    conn = pyodbc.connect(param)
    cursor = conn.cursor()

    # fetch records
    start = timeit.default_timer()
    cursor.execute(sql)
    df = pd.DataFrame.from_records(cursor.fetchall(), columns=cols)
    if remove_dup:
        df = df.drop_duplicates()
    if categoricals:
        for categorical in categoricals:
            df[categorical] = df[categorical].astype('category')
    stop = timeit.default_timer()
    sec = stop - start

    pack = pack_data(df, table, sql, sec)
    save_datapack(table, pack)

    return table, sec


def find_cols(sql):
    """
    Extract column names from sql query.
    - Select lines between 'select' and 'from'
    - If line contains 'as' extract name from that.

    Parameters
    ==========
    :param sql: `string`

    Return
    ======
    :find_cols: column names as `list` of `strings`
    """

    def alias(x):
        key_word = ' as '
        if key_word in x:
            return x.split(key_word)[1]
        return x

    # retrieve column names between 'select' and 'from'
    cols = list()
    for line in sql.split('\n'):
        if 'select' in line:
            continue
        if 'from' in line:
            break
        line = line.strip(' ,').replace('OST_', '')
        cols.append(line)
    return [alias(col) for col in cols]


def read_sql(sql, parameters=None):
    """
    Fetch sql query from PATH_SQL and set variables.

    Parameters
    ==========
    :param sql : `string`
        Name of the SQL query (without extension).

    Optional parameters
    ===================
    param parameters : `dict`, default `None`
        Dictionary of parameters to be replaced in the SQL query.

    Return
    ======
    :read_sql: `string`
    """

    with open(PATH_SQL / f'{sql}.txt', 'r') as f:
        sql = f.read()

    # replace parameter values
    if parameters:
        for key in parameters:
            sql = sql.replace(f'[{key}]', str(parameters[key]))

    return sql


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
    with open(PATH_QUERY / f'{table}.pkl', 'wb') as f:
        pickle.dump(pack, f)
    return None


def load_datapack(table):
    with open(PATH_QUERY / f'{table}.pkl', 'rb') as f:
        pack = pickle.load(f)
    return pack


def load_frame(table, strip_col=True):
    pack = load_datapack(table)
    frame = pack['frame']

    if strip_col:
        def strip_col(x):
            if '(' in x:
                sub = x[x.find('(') + 1:x.rfind(')')]
                if '.' in sub:
                    x = x.replace(sub, sub.split('.')[1])
            if '.' in x:
                x = x.split('.')[1]
            return x
        frame.columns = [strip_col(col) for col in frame.columns]

    return frame


def load_source(table):
    pack = load_datapack(table)
    return pack['source']
