import re
import textwrap
from collections import namedtuple
from configparser import ConfigParser
from pathlib import Path
from query.config import load_ini, config_from_ini


class QueryDef:
    """
    QueryDef
    ========
    Class for storing query definitions.

    ## Constructor
    Use the constructor method `from_ini` to initialize an instance of
    this class from a query file.

    ## Prime for execution
    Call the instance and pass it the parameters as a dictionary.
    This will set all parameters within the query definition.

    Attributes
    ==========
    name: str
        Name of the query.
    filename: str
        Filename to be used for storing the query results.
    description: str
        Description of the query.
    qtype : str
        Query type.
    sql: str
        SQL statement.
    columns: dict
        Dictionary storing the column names and associated dtypes.
        - keys: column names;
        - values: dtypes (may be None).
    parameters: dict
        Dictionary storing the parameters used in the query definition.
        - keys: parameter name;
        - values: parameter type.
    """

    def __init__(
        self,
        name,
        filename,
        sql,
        columns=None,
        qtype=None,
        description=None,
        parameters=None,
    ):
        self.name        = name
        self.filename    = filename
        self.qtype       = qtype
        self.description = description
        self.parameters  = parameters
        self.columns     = columns
        self.sql         = sql


    def _repr_html_(self):
        def tag(x, tag, class_=None):
            class_ = f" class={class_}" if class_ is not None else ''
            return f"<{tag}{class_}>{x}</{tag}>"

        def item(k, v):
            return tag(f"{tag(k, 'th')}{tag(v, 'td')}", 'tr')

        def table(d, class_=None):
            items = [f"{tag(k, 'td')}{tag(v, 'td')}" for k,v in d.items()]
            rows = [tag(item, 'tr') for item in items]
            return tag(''.join(rows), 'table', class_=class_)

        style = tag(".qc th, td { text-align: left !important; }", 'style')
        classname = f"<code>&lt;{self.__class__.__name__}&gt;</code>"
        sql = tag(self.sql.replace('\n', '<br/> '), 'code')
        columns = '' if self.columns is None else table(self.columns, 'qc')
        params = '' if self.parameters is None else table(self.parameters, 'qc')

        string = (
            item('Name', self.name) +
            item('Filename', self.filename) +
            item('Qtype', self.qtype) +
            item('Description', self.description) +
            item('Columns', columns) +
            item('Parameters', params) +
            item('SQL', sql)
        )
        return style + classname + tag(string, 'table', 'qc')


    def __call__(self, parameters=None):
        if not parameters.keys() == self.parameters.keys():
            missing = set(self.parameters.keys()) - set(parameters.keys())
            raise ValueError(
                "Definition is underdefined. "
                f"Missing the following parameters: {missing}."
            )

        for key, value in parameters.items():
            try:
                if self.parameters[key] == 'int':
                    int(value)
            except ValueError:
                raise ValueError(
                    f"Value for parameter '{key}' is not "
                    f"of type {self.parameters[key]}."
                )

        self.name = self.set_param(self.name, parameters)
        self.filename = self.set_param(self.filename, parameters)
        self.description = self.set_param(self.description, parameters)
        self.sql = self.set_param(self.sql, parameters)


    @classmethod
    def from_ini(cls, path):
        path = Path(path).with_suffix('.ini')
        ini = config_from_ini(load_ini(path))
        fields = ini._fields

        # optional specifications
        columns = ini.columns if 'columns' in fields else None
        parameters = ini.parameters if 'parameters' in fields else None

        meta_exists = 'meta' in fields
        description = ini.meta.description if meta_exists else ''
        qtype = ini.meta.qtype if meta_exists else ''

        return cls(
            ini.definition.name,
            ini.definition.filename,
            format_sql(ini.query.sql),
            qtype=qtype,
            description=description.strip('\n'),
            columns=dict() if columns is None else columns._asdict(),
            parameters=dict() if parameters is None else parameters._asdict(),
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
            for key, value in parameters.items():
                x = x.replace(f'[{key}]', str(value))

                # simple arithmetic
                srch_str = fr'(\[({key})(-|\+)(\d+)\])'
                regex = re.compile(srch_str)
                matches = re.findall(regex, x)
                if matches is not None:
                    for match in matches:
                        left = value
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
                val_slice = value[left:right]
                x = x.replace(key_slice, val_slice)
        return x


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
