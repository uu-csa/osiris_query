import re
import textwrap
import configparser
from collections import namedtuple
from .config import PATH_INPUT


class QueryDef:
    """
    QueryDef
    ========
    Class for loading and accessing query definitions.
    Initialize by passing the name of the query in PATH_INPUT.
    """
    def __init__(self, query_name, parameters=None):
        self.query_name = query_name
        self.parameters = parameters

        querydef = self.load_querydef(query_name)
        self.description = querydef.description
        if self.description is not None:
            self.description = self.set_param(
                self.description, parameters=parameters
                )
        else:
            self.description = None
        self.qtype = querydef.qtype
        self.sql = self.set_param(querydef.sql, parameters=parameters)
        self.columns = querydef.columns
        self.dtypes = querydef.dtypes
        self.remove_duplicates = querydef.remove_duplicates

        param_string = None
        if parameters is not None:
            param_values = [parameters[k] for k in parameters]
            param_values.insert(0, 'var')
            param_string = '_'.join(param_values)
        self.outfile = '_'.join(
            [x for x in [query_name, param_string] if x is not None]
            )


    def load_querydef(self, query_name):
        """
        Load query definition from file.

        Parameters
        ==========
        :param query_name: str
            Name of the query to load (without extension).

        Returns
        =======
        :load_querydef: tuple
            - description of query as string
            - sql statement as string
            - column names as list
            - columns/dtypes as dictionary
            - remove duplicates as boolean
        """

        tuple_names = [
            'description',
            'qtype',
            'sql',
            'columns',
            'dtypes',
            'remove_duplicates'
            ]
        querydef = namedtuple('querydef', tuple_names)

        ini_file = PATH_INPUT / f"{query_name}.ini"
        if ini_file.exists():
            ini = configparser.ConfigParser(
                allow_no_value=True,
                interpolation=None,
                )
            ini.read(ini_file)

            # description
            try:
                description = ini['query']['description'].strip('\n')
            except KeyError:
                description = None

            # qtype
            try:
                qtype = ini['query']['qtype']
            except KeyError:
                qtype = None

            # sql statement
            sql = format_sql(ini['query']['sql'])

            # columns
            try:
                columns = ini['columns']
                columns = {k:columns[k] for k in columns}
                colnames = [k for k in columns]
                dtypes = {k: v for k, v in columns.items() if v is not None}
            except KeyError:
                colnames = self.find_cols(sql)
                dtypes = None

            # remove duplicates
            try:
                remove_duplicates = ini.getboolean('duplicates', 'remove')
            except KeyError:
                remove_duplicates = None

            return querydef(
                description, qtype, sql, colnames, dtypes, remove_duplicates
                )
        else:
            txt_file = PATH_INPUT / f'{query_name}.txt'
            sql = txt_file.read_text()
            return querydef(None, None, sql, self.find_cols(sql), None, None)


    @staticmethod
    def set_param(x, parameters=None):
        """
        Set variables, if any.

        Parameters
        ==========
        :param x : `string`

        Optional parameters
        ===================
        param parameters : `dict`, default `None`
            Dictionary of parameters to be replaced in the string.

        Return
        ======
        :set_param: `string`
        """

        if parameters:
            for key in parameters:
                x = x.replace(f'[{key}]', str(parameters[key]))

                # slice logic on variables
                srch_str = fr'\[{key}\((.?:.?)\)\]'
                regex = re.compile(srch_str)
                match = re.search(regex, x)
                if match is None:
                    continue

                slice_ = match.group(1).split(':')
                left = None if slice_[0] == '' else int(slice_[0])
                right = None if slice_[1] == '' else int(slice_[1])
                key_slice = f'[{key}({match.group(1)})]'
                val_slice = parameters[key][left:right]
                x = x.replace(key_slice, val_slice)
        return x


    @staticmethod
    def find_cols(sql):
        """
        Extract column names from sql statement.
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


def format_sql(sql, tab_length=4):
    """
    Return formatted sql statement.
    """

    clauses = [
        'select',
        'insert',
        'update',
        'delete',
        'from',
        'where',
        'groupby',
        'order by',
        ]

    tab = ' ' * tab_length
    depth = 0
    lines = list()

    for line in sql.splitlines():
        if line == '':
            continue
        if not any(clause in line for clause in clauses):
            line = f'{(depth + 1) * tab}{line}'
        if '(' in line:
            depth += 1
        if ')' in line:
            depth -= 1

        lines.append(line)

    sql = '\n'.join(lines)

    return sql
