# standard library
import datetime
import json
import pickle
import timeit
from collections import namedtuple

# third party
import pandas as pd
import pyodbc

# local
from src.config import PATH_CONFIG, PATH_LOGIN, PATH_OUTPUT
from src.querydef import QueryDef
from src.utils import reporter


with open(PATH_CONFIG / 'queries.json', 'r') as f:
    QUERIES = json.loads(f.read())


class Query:
    def __init__(self, qd, frame, sec=None):
        self.qd = qd
        self.frame = frame
        self.nrecords = len(frame)
        self.timer = sec
        self.dtime = datetime.datetime.now()

    @classmethod
    @reporter
    def from_qd(cls, qd, cursor=None):
        """
        Construct Query from QueryDef.
        Run sql-query on the OSIRIS database and return Query instance.

        - Lookup login details from u:/uustprd.txt
        - Connect to database
        - Fetch records
        - (Optionally) Rename columns
        - (Optionally) Recast dtypes
        - Pack (meta)data in namedtuple

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
        :query: table name as `string`, sec as `float`
        """

        # connection
        if not cursor:
            cursor = connect()

        # fetch records
        start = timeit.default_timer()
        cursor.execute(qd.sql)
        if isinstance(qd.columns, dict):
            cols = qd.columns.keys()
            dtypes = {k: v for k, v in qd.columns.items() if v is not None}
        else:
            cols = qd.columns
            dtypes = None

        df = pd.DataFrame.from_records(
            cursor.fetchall(),
            columns=cols,
            )

        if dtypes:
            df = df.astype(dtypes)
        stop = timeit.default_timer()
        sec = stop - start

        return cls(qd, df, sec=sec)

    def to_pickle(self, path=None):
        """
        Save Query to pickle.

        Optional key-word arguments
        ===========================
        :param path: `Path`
            Path to store pickled Query.
        """
        if not path:
            path = PATH_OUTPUT / f'{self.qd.outfile}.pkl'

        # pickle pack
        if not path.parent.exists():
            path.parent.mkdir(parents=True)
        with open(path, 'wb') as f:
            pickle.dump(self, f)

        # update query overview
        path_overview = PATH_OUTPUT / '_queries_overview_.xlsx'

        query_data = vars(self.qd).copy()
        del query_data['outfile']
        query_data.update(vars(self))
        for key in ['frame', 'qd']:
            del query_data[key]

        cols = list(query_data.keys())
        vals = list(query_data.values())

        try:
            df = pd.read_excel(path_overview, index_col=0)
        except FileNotFoundError:
            df = pd.DataFrame()
        row = {path: vals}
        df_row = pd.DataFrame.from_dict(row, orient='index', columns=cols)
        df = df.append(df_row, sort=False)
        df.to_excel(path_overview)
        return None


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


def read_pickle(query_name):
    with open(PATH_OUTPUT / f'{query_name}.pkl', 'rb') as f:
        return pickle.load(f)


def load_set(query_set, parameters=None):
    """
    Load a set of queries as defined in config/queries.json.

    Parameters
    ==========
    :param query_set: `str`
        Name of the query set as string.

    Optional key-word arguments
    ===========================
    :param parameters: `str` or `list`, default=None
        Parameters in order for finding the correct name.
        See `QueryDef`.

    Returns
    =======
    :load_set: `namedtuple`
        Namedtuple containing all `DataFrames` in the query set.
    """

    def get_name(x):
        x = x.split('/')[1]
        if '_' in x[:2]:
            return x[2:]
        return x

    queries = QUERIES[query_set]['queries']

    if parameters:
        if not isinstance(parameters, list):
            parameters = [parameters]

    DataSet = namedtuple('DataSet', [get_name(q) for q in queries])
    return DataSet(
        **{
            get_name(q):load_frame(f"{q}_var_{'_'.join(parameters)}")
            for q in queries
            }
        )


def load_frame(query_name):
    q = read_pickle(query_name)
    return q.frame


def run_query(query_name, cursor=None, parameters=None):
    qd = QueryDef.from_file(query_name, parameters=parameters)
    q = Query.from_qd(qd, cursor=cursor)
    q.to_pickle()
    return None
