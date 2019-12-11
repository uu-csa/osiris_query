# standard library
import datetime
import pickle
from collections import namedtuple

# third party
import pandas as pd

# local
from query.config import PATH_CONFIG, PATH_OUTPUT, load_registry


QUERIES = load_registry(PATH_CONFIG / 'queries.json')


class QueryResult:
    def __init__(self, qd, frame, seconds=None):
        self.qd       = qd
        self.frame    = frame
        self.nrecords = len(frame)
        self.timer    = seconds
        self.dtime    = datetime.datetime.now()

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


def read_pickle(query_name):
    if query_name.startswith('./'):
        path = Path(query_name[2:]).with_suffix('.pkl')
    else:
        path = (PATH_OUTPUT / f'{query_name}').with_suffix('.pkl')
    with open(path, 'rb') as f:
        return pickle.load(f)


def load_frame(query_name):
    q = read_pickle(query_name)
    return q.frame


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
        x = x.split('/')[-1]
        if '_' in x[:2]:
            return x[2:]
        return x

    if isinstance(query_set, list):
        queries = query_set
    else:
        queries = QUERIES[query_set]['queries']

    DataSet = namedtuple('DataSet', [get_name(q) for q in queries])

    if parameters:
        if not isinstance(parameters, list):
            parameters = [parameters]
        parameters = [str(p) for p in parameters]

        return DataSet(
            **{
                get_name(q):load_frame(f"{q}_var_{'_'.join(parameters)}")
                for q in queries
            }
        )
    else:
        return DataSet(**{get_name(q):load_frame(q) for q in queries})
