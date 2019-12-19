# standard library
import sys
import timeit
from textwrap import indent, wrap
from os import system, name
from concurrent.futures import ThreadPoolExecutor, as_completed

# third party
import pandas as pd

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


def line_printer(symbol='='):
    print(symbol * 80)


def run(querydefs):
    start = timeit.default_timer()

    all_results = list()

    # RUN QUERIES
    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = [executor.submit(run_query, query) for query in querydefs]
        for future in as_completed(futures):
            result = future.result()
            data = vars(result.qd).copy()
            data['columns'] = '|'.join([col for col in data['columns']])
            data.update(vars(result))
            for key in ['parameters', 'frame', 'qd']:
                del data[key]
            all_results.append(data)

    # update query overview
    path_overview = PATHS.output / '_queries_overview_.xlsx'

    try:
        df = pd.read_excel(path_overview, index_col=0)
    except FileNotFoundError:
        columns = [
            'qtype',
            'name',
            'filename',
            'description',
            'sql',
            'columns',
            'nrecords',
            'timer',
            'dtime',
            ]
        df = pd.DataFrame(columns=columns)

    data = pd.DataFrame(all_results)
    df = df.append(data, sort=False, ignore_index=True)
    df.to_excel(path_overview)

    # STOP TIMER AND PRINT RUNTIME
    stop = timeit.default_timer()
    sec = stop - start

    line_printer('-')
    print(f"Total runtime: {round(sec, 2)} seconds.")


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

        line_printer()
        print("SELECT QUERY SET TO RUN:")
        line_printer('-')
        for idx, query_set in options.items():
            print(f"{idx:>2}.", query_set)
        line_printer('-')
        print(f" Enter {stop} to exit")
        line_printer()
        print()

        select = None
        while select not in options.keys() and select != stop:
            print(f"\033[F{' ' * 80}", end='')
            print('\rSelection: ', end='')
            select = input()
        if select == stop:
            break

        qds = list()
        for file in (PATHS.definitions / options[select]).glob('*.ini'):
            qds.append(QueryDef.from_ini(file))

        parameters = dict()
        for qd in qds:
            parameters.update(qd.parameters)

        print()
        line_printer()
        print(f"QUERIES BELONGING TO: '{options[select]}'")
        line_printer('-')
        for qd in qds:
            print(f" - {qd.name}")
        line_printer()
        print()

        line_printer()
        print(f"SET PARAMETERS:")
        line_printer()

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
        line_printer()
        print('QUERY DEFINITIONS:')
        for qd in qds:
            description = wrap(
                qd.description,
                width=80,
                initial_indent='  ',
                subsequent_indent='  ',
            )

            line_printer()
            print('Name:     ', qd.name)
            print('Filename: ', qd.filename)
            line_printer('-')
            print('QType:    ', qd.qtype)
            print('Description:\n', '\n'.join(description))
            line_printer('-')
            print('SQL:\n', indent(qd.sql, prefix='  '), '\n')

        line_printer()
        run(qds)
        line_printer()
