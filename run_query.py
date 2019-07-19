# standard library
from os import system, name
import timeit

#local
from src.config import PATH_CONFIG, load_registry
from src.querydef import QueryDef
from src.query import Query, connect, run_query


metaparams = load_registry(PATH_CONFIG / 'metaparam.json')
queries    = load_registry(PATH_CONFIG / 'queries.json')
options    = {str(idx):query for idx, query in enumerate(queries)}


def clear():
      # for windows
    if name == 'nt':
        _ = system('cls')

    # for mac and linux(here, os.name is 'posix')
    else:
        _ = system('clear')


def print_line(sep='='):
    print(sep * 80)



def run(queries, parameters):
    start = timeit.default_timer()
    # CONNECT TO DATABASE
    cursor = connect()

    # RUN QUERIES
    for query in queries:
        run_query(f"{query}", cursor=cursor, parameters=parameters)

    # STOP TIMER AND PRINT RUNTIME
    stop = timeit.default_timer()
    sec = stop - start
    print(f"\n{'=' * 80}\nTotal runtime: {sec} seconds.")


if __name__ == '__main__':

    clear()
    print(
    u"""
       ____  _____ ________  _________
      / __ \/ ___//  _/ __ \/  _/ ___/   ____ ___  _____  _______  __
     / / / /\__ \ / // /_/ // / \__ \   / __ `/ / / / _ \/ ___/ / / /
    / /_/ /___/ // // _, _// / ___/ /  / /_/ / /_/ /  __/ /  / /_/ /
    \____//____/___/_/ |_/___//____/   \__, /\__,_/\___/_/   \__, /
                                         /_/                /____/    \u2122
    """
    )

    print_line()
    print("SELECT QUERY SET TO RUN:")
    print_line('-')
    for idx, query in options.items():
        print(f"{idx:>2}.", query)
    print_line()
    print()

    select = None
    while select not in options.keys():
        print(f"\033[F{' ' * 80}", end='')
        print('\rSelection: ', end='')
        select = input()
    querydefs = queries[options[select]]['queries']

    print()
    print_line()
    print(f"Selected: {options[select]}")
    print_line('-')
    for qd in querydefs:
        print(qd)
    print_line()
    print()

    parameters = None
    if queries[options[select]].get('parameters', None):
        parameters = dict()

        print_line()
        print(f"SET PARAMETERS:\n")

        for param in queries[options[select]]['parameters']:
            val = None
            description = metaparams[param]['description']
            if description:
                print(f"Description: {description}")
                print_line('-')
                print()
            while not val:
                print(f"\033[F{' ' * 80}", end='')
                print(f"\r{param}: ", end='')
                val = input()
                if metaparams[param]['target'] == 'querydef':
                    if metaparams[param]['type'] == 'int':
                        # reject if string is not convertable to integer
                        try:
                            int(val)
                            parameters[param] = val
                        except ValueError:
                            val = None
                            pass
                    else:
                        parameters[param] = val
                elif metaparams[param]['target'] == 'file':
                    querydefs = [val]
        print()
    run(querydefs, parameters)
