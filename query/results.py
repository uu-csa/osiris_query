# standard library
import datetime
import pickle
from collections import namedtuple
from pathlib import Path

# local
from query.config import PATHS


class QueryResult:
    """
    QueryResult
    ===========

    This class provides access to the query results and its meta data.

    Attributes
    ==========
    qd : QueryDef
        Holds the meta data of the query.
    frame : DataFrame
        Df with the query data.
    nrecords: int
        Number of records in the query data.
    timer:
        Execution time.
    dtime:
        Execution date and time.

    Methods
    =======
    - to_pickle
    - read_pickle
    - view_sets
    - view_queries
    """

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
        Save QueryResult to pickle.

        Optional key-word arguments
        ===========================
        :param path: `Path`
            Path to store pickled QueryResult.
        """

        if not path:
            path = PATHS.output / f'{self.qd.filename}.pkl'

        # pickle pack
        if not path.parent.exists():
            path.parent.mkdir(parents=True)
        with open(path, 'wb') as f:
            pickle.dump(self, f)

        return None


    def to_excel(self, path=None):
        """
        Save dataframe from the QueryResult as an excel file.

        Optional key-word arguments
        ===========================
        :param path: `Path`
            Path to store the excel sheet.
        """

        if not path:
            path = PATHS.output / f'{self.qd.filename}.xlsx'

        # pickle pack
        if not path.parent.exists():
            path.parent.mkdir(parents=True)
        self.frame.to_excel(path)

        return None


    @staticmethod
    def read_pickle(name=None, path=None):
        """
        Read QueryResult from pickle.

        Optional key-word arguments
        ===========================
        :param name: `str`
            Name of the query to be loaded.
            For example: 'monitor/inschrijfhistorie_2019'
        :param path: `Path`
            Path to stored pickled QueryResult
        """

        if name is not None:
            path = (PATHS.output / f'{name}').with_suffix('.pkl')
        elif path is not None:
            path = Path(path).with_suffix('.pkl')
        else:
            raise ValueError(
                "Either query name or query path is needed to load query.")
        with open(path, 'rb') as f:
            return pickle.load(f)


    # @staticmethod
    # def view_sets():
    #     """
    #     Print available sets in PATHS.output.
    #     """

    #     path = PATHS.output
    #     for item in path.glob('**'):
    #         print(item.relative_to(path))
    #     return None


    # @staticmethod
    # def view_queries(queryset):
    #     """
    #     Print avialbable query results in PATHS.output / queryset.
    #     (Use view_sets to view available sets.)
    #     """

    #     base = PATHS.output
    #     path = PATHS.output / queryset
    #     for item in path.glob('**/*.*'):
    #         print(item.relative_to(base).with_suffix(''))
    #     return None


    @staticmethod
    def fetch_sets():
        """
        Return available sets in PATHS.output.
        """
        path = PATHS.output
        return [item.relative_to(path) for item in path.glob('**')]


    @classmethod
    def view_sets(cls):
        [print(item) for item in cls.fetch_sets()]
        return None


    @classmethod
    def fetch_queries(cls, queryset):
        """
        Return available query results in PATHS.output / queryset.
        """

        path = PATHS.output / queryset
        return [cls.read_pickle(file) for file in path.glob('**/*.pkl')]


    @classmethod
    def view_queries(cls, queryset):
        [print(item.qd.filename) for item in cls.fetch_queries(queryset)]
        return None


def load_set(queryset, parameters=None):
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

    gen = QueryResult.fetch_queries(queryset)

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
