import re
import textwrap
import configparser
from collections import namedtuple
from .config import PATH_INPUT


class QueryDef:
    """
    QueryDef
    ========
    Class for storing query definitions.

    ## Constructor
    Use the constructor method `from_file` to initialize an instance of
    this class from a query file in PATH_INPUT. If the query file contains
    any parameters, these need to be passed in as well.

    ## Outfile
    The outfile is the name of the file where the query results will be stored.
    Normally, the parameters are joined as string and added to the outfile name:

    > [outfile]_var_[parameters]

    In case this is not desirable, pass a string to `param_repr` that will be
    used instead of the joined parameters.

    Attributes
    ==========
    query_name : name of the query as string
    description : description of the query as string
    qtype : query type as string
    sql : sql statement as string
    columns :
        - column names as list, or:
        - column name-dtype pairs as dictionary
    outfile : filename for storing the query results as string
    """

    def __init__(
        self,
        query_name,
        sql,
        description=None,
        qtype=None,
        columns=None,
        param_repr=None,
        ):

        self.query_name = query_name
        self.qtype = qtype
        self.description = description
        self.param_repr = param_repr
        self.columns = columns
        self.sql = sql
        self.outfile = '_'.join(
            [x for x in [query_name, param_repr] if x is not None]
            )


    def __str__(self):
        repr = (
            f"{self.__class__.__name__}\n"
            f"{'-' * 80}\n"
            f"{self.query_name!r}\n"
            f"{'-' * 80}\n"
            f"{self.sql}\n"
            f"{'-' * 80}\n"
            f"Description = {self.description}\n"
            f"Query type = {self.qtype}\n"
            )
        return repr


    @classmethod
    def from_file(cls, query_name, parameters=None, param_repr=None):
        """
        Construct QueryDef from file.

        Parameters
        ==========
        :param query_name: `str`
            Name of the file to load from the PATH_INPUT folder.
            (Suffix optional).

        Optional keyword arguments
        ==========================
        :param parameters: `dict`
            Dictionary of parameters.
        :param param_repr: `str`
            String to be used for representing passed in parameters.

        Returns
        =======
        :from_file: `QueryDef` instance
        """

        path = PATH_INPUT / query_name
        ini_file = path.with_suffix('.ini')

        # read .ini file
        if ini_file.exists():
            ini = configparser.ConfigParser(
                allow_no_value=True,
                interpolation=None,
                )
            ini.read(ini_file, encoding='utf-8')

            meta    = ini['meta']
            query   = ini['query']
            columns = ini['columns']

            description = meta.get('description', '').strip('\n')
            description = cls.set_param(description, parameters)
            qtype       = meta.get('qtype', '')
            sql         = cls.set_param(query['sql'], parameters)
            sql         = format_sql(sql)

            try:
                columns = {k:columns[k] for k in columns}
            except KeyError:
                columns = cls.find_cols(sql)

        # read as text
        else:
            txt_file = path.with_suffix('.txt')
            sql = txt_file.read_text()
            sql = format_sql(cls.set_param(sql, parameters))
            description = None
            qtype = None
            columns = cls.find_cols(sql)

        # set string representation for parameters
        if param_repr is not None:
            param_repr = f'var_{param_repr}'
        elif parameters is not None:
            param_values = [str(v) for v in parameters.values()]
            param_values.insert(0, 'var')
            param_repr = '_'.join(param_values)

        return cls(
            query_name,
            sql,
            description=description,
            qtype=qtype,
            columns=columns,
            param_repr=param_repr,
            )

    @staticmethod
    def set_param(x, parameters=None):
        """
        Set variables, if any.

        Parameters
        ==========
        :param x : `string`

        Optional keyword arguments
        ==========================
        param parameters : `dict`, default `None`
            Dictionary of parameters to be replaced in the string.

        Return
        ======
        :set_param: `string`
        """

        if parameters:
            for key in parameters:
                x = x.replace(f'[{key}]', str(parameters[key]))

                # simple arithmetic
                srch_str = fr'(\[({key})(-|\+)(\d+)\])'
                regex = re.compile(srch_str)
                matches = re.findall(regex, x)
                if matches is not None:
                    for match in matches:
                        left = parameters[key]
                        operator = match[2]
                        right = match[3]
                        
                        output = eval(left + operator + right)
                        x = x.replace(match[0], str(output))

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
        - Select lines between 'select' and 'from'.
        - Strip the part before the dot if present.
        - If line contains 'as' extract name from that.

        Parameters
        ==========
        :param sql: `string`

        Return
        ======
        :find_cols: column names as `list` of `strings`
        """

        def strip_dot(x):
            if '(' in x:
                sub = x[x.find('(') + 1:x.rfind(')')]
                if '.' in sub:
                    x = x.replace(sub, sub.split('.')[1])
            if '.' in x:
                x = x.split('.')[1]
            return x

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
            line = line.strip(' ,')
            line = strip_dot(line)
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


def get_queries(group):
    return [query.stem for query in (PATH_INPUT / group).glob('*.ini')]
