# standard library
import datetime
import pickle
from collections import namedtuple
from pathlib import Path

# local
from query.config import PATHS


class QueryResult:
    def __init__(self, qd, frame, seconds=None):
        self.qd       = qd
        self.frame    = frame
        self.nrecords = len(frame)
        self.timer    = seconds
        self.dtime    = datetime.datetime.now()


    def __repr__(self):
        return f"<{self.__class__.__name__}, '{self.qd.name}', {self.nrecords}>"


    def to_pickle(self, path=None):
        """
        Save Query to pickle.

        Optional key-word arguments
        ===========================
        :param path: `Path`
            Path to store pickled Query.
        """
        if not path:
            path = PATHS.output / f'{self.qd.filename}.pkl'

        # pickle pack
        if not path.parent.exists():
            path.parent.mkdir(parents=True)
        with open(path, 'wb') as f:
            pickle.dump(self, f)

        return None


    @staticmethod
    def read_pickle(query_name):
        if query_name.startswith('./'):
            path = Path(query_name[2:]).with_suffix('.pkl')
        else:
            path = (PATHS.output / f'{query_name}').with_suffix('.pkl')
        with open(path, 'rb') as f:
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
