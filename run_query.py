# standard library
import sys
import timeit
from textwrap import indent
from os import system, name
from concurrent.futures import ThreadPoolExecutor

#local
from query.config import PATHS
from query.definition import QueryDef
from query.execution import connect, run_query


def clear():
    # for windows
    if name == 'nt':
        _ = system('cls')

    # for mac and linux(here, os.name is 'posix')
    else:
        _ = system('clear')


def print_line(symbol='='):
    print(symbol * 80)


def run(querydefs):
    start = timeit.default_timer()
    # CONNECT TO DATABASE
    # cursor = connect()

    # RUN QUERIES
    with ThreadPoolExecutor(max_workers=7) as executor:
        [executor.submit(run_query, query) for query in querydefs]

    # STOP TIMER AND PRINT RUNTIME
    stop = timeit.default_timer()
    sec = stop - start
    print(f"\n{'=' * 80}\nTotal runtime: {sec} seconds.")


if __name__ == '__main__':

    clear()
    while True:
        query_sets = [x.name for x in PATHS.definitions.iterdir() if x.is_dir()]
        options    = {str(idx):qs for idx, qs in enumerate(query_sets)}
        stop       = '.'

        print(
        u"""
           ____  _____ ________  _________
          / __ \/ ___//  _/ __ \/  _/ ___/   ____ ___  _____  _______  __
         / / / /\__ \ / // /_/ // / \__ \   / __ `/ / / / _ \/ ___/ / / /
        / /_/ /___/ // // _, _// / ___/ /  / /_/ / /_/ /  __/ /  / /_/ /
        \____//____/___/_/ |_/___//____/   \__, /\__,_/\___/_/   \__, /
                                             /_/                /____/    \u2122
            dev.
        """
        )

        print_line()
        print("SELECT QUERY SET TO RUN:")
        print_line('-')
        for idx, query_set in options.items():
            print(f"{idx:>2}.", query_set)
        print_line()
        print(f"Enter {stop} to exit")
        print_line()
        print()

        select = None
        while select not in options.keys() and select != stop:
            print(f"\033[F{' ' * 80}", end='')
            print('\rSelection: ', end='')
            select = input()
        if select == stop:
            break

        qds = list()
        for file in (PATHS.definitions / options[select]).glob('*'):
            qds.append(QueryDef.from_ini(file))

        parameters = dict()
        for qd in qds:
            parameters.update(qd.parameters)

        print()
        print_line()
        print(f"QUERIES BELONGING TO: '{options[select]}'")
        print_line('-')
        for qd in qds:
            print(f" - {qd.name}")
        print_line()
        print()

        print_line()
        print(f"SET PARAMETERS:\n")

        for key, ptype in parameters.items():
            val = None
            type_description = '' if ptype is None else f" ({ptype})"

            print()
            while not val:
                print(f"\033[F{' ' * 80}", end='')
                print(f"\r{key}{type_description}: ", end='')
                val = input()

                if ptype == 'int':
                    # reject if string is not convertable to integer
                    try:
                        int(val)
                        parameters[key] = val
                    except ValueError:
                        val = None
                        pass
                else:
                    parameters[key] = val

        for qd in qds:
            qd(parameters)

        print()
        print_line()
        print('QUERY DEFINITIONS:')
        for qd in qds:
            print_line()
            print('Name: ', qd.name)
            print('Filename: ', qd.filename)
            print_line('-')
            print('QType: ', qd.qtype)
            print('Description:\n', indent(qd.description, prefix='  '))
            print_line('-')
            print('SQL:\n', indent(qd.sql, prefix='   '))

        print()
        run(qds)
