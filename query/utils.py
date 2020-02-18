import timeit
import functools
from configparser import ConfigParser
from pathlib import Path
from collections import namedtuple


def reporter(func):
    @functools.wraps(func)
    def wrapper_reporter(*args, **kwargs):
        start = timeit.default_timer()
        value = func(*args, **kwargs)
        stop = timeit.default_timer()
        sec = stop - start
        print(
            f"{func.__name__!r} returned {value!r} in {round(sec, 2)} seconds"
        )
        return value
    return wrapper_reporter


def getpw(path):
    # get login details
    path = Path(path)
    login = path.read_text().split('\n')[1].split(',')
    return login[0].strip(), login[1].strip()


def get_credentials(path):
    "Retrieve login credentials from file."

    if not path.exists():
        raise FileNotFoundError(
            f"File with login credentials not found: '{path}'"
        )
    config = ConfigParser()
    config.read(path)
    Creds = namedtuple('Creds', [item for item in config['credentials']])
    return Creds(**{k:v for k, v in config['credentials'].items()})


def studnum_to_string(
    df,
    colname='studentnummer',
    print_strings=True,
    as_set=True
):
    """
    Print or return the column 'studentnummer' from a `DataFrame` as a string.
    Blocks of 500 numbers are separated by an empty line.
    This is because OSIRIS accepts a string of max. 500 numbers in its filters.

    Parameters
    ==========
    :param df: `DataFrame`

    Optional key-word arguments
    ===========================
    :param colname: `str`, default='studentnummer'
        Name of the column containing the numbers to return.
    :param print_strings: `boolean`, default=True
        If True the function will print the string.
        If False the function will not print.
    :param as_set: `boolean`, default=True
        If True the function will return a set instead of a list.
        This means no duplicates.

    Returns
    =======
    :studnum_to_string: `str`
    """

    string = ''
    studentnummers = df[colname].to_list()
    if as_set:
        studentnummers = set(studentnummers)
    for idx, studentnummer in enumerate(studentnummers):
        if ((idx + 1) % 500):
            string += f"{studentnummer};"
        else:
            string += f"{studentnummer}\n\n"

    if print_strings:
        print(string)
        return None

    return string
