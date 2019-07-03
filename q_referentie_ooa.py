# standard library
import timeit
start = timeit.default_timer()
import argparse

#local
from src.querydef import QueryDef
from src.query import Query, connect, run_query


QUERIES = [
    'r_ooa_sl',
    'r_ooa_ps',
]


def run():
    # PARSE COMMAND LINE ARGUMENTS
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'proces',
        help='Naam van het OOA-proces.'
        )
    args = parser.parse_args()
    parameters = {
        'proces': args.proces,
    }

    # CONNECT TO DATABASE
    cursor = connect()

    # RUN QUERIES
    ref_tables = ['r_ooa_sl']
    for ref in ref_tables:
        run_query(f'referentie/ooa/{ref}', cursor=cursor)

    ref_tables = ['r_ooa_ps']
    for ref in ref_tables:
        run_query(f'referentie/ooa/{ref}', cursor=cursor, parameters=parameters)

    # STOP TIMER AND PRINT RUNTIME
    stop = timeit.default_timer()
    sec = stop - start
    print(f"\n{'=' * 80}\nTotal runtime: {sec} seconds.")


if __name__ == '__main__':
    run()
