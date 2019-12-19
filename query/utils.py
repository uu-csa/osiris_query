import timeit
import functools
from pathlib import Path


def reporter(func):
    @functools.wraps(func)
    def wrapper_reporter(*args, **kwargs):
        start = timeit.default_timer()
        args_repr = [f'{a!s}' for a in args]
        kwargs_repr = [f'{k}={v!s}' for k, v in kwargs.items()]
        signature = '\n'.join(args_repr + kwargs_repr)
        value = func(*args, **kwargs)
        stop = timeit.default_timer()
        sec = stop - start
        print(f"{func.__name__!r} returned {value!r} in {sec} seconds")
        return value
    return wrapper_reporter


def getpw(path):
    # get login details
    path = Path(path)
    login = path.read_text().split('\n')[1].split(',')
    return login[0].strip(), login[1].strip()


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
