# standard library
import timeit
start = timeit.default_timer()
import argparse

#local
from src.querydef import QueryDef
from src.query import Query, connect, run_query


def run():
    # PARSE COMMAND LINE ARGUMENTS
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'query',
        help="Naam van de uit te voeren query in de map 'ad_hoc'."
        )
    arg = parser.parse_args()

    # CONNECT TO DATABASE
    cursor = connect()

    # RUN SQL QUERY
    run_query(f"ad_hoc/{arg.query}", cursor=cursor, parameters=None)

    # STOP TIMER AND PRINT RUNTIME
    stop = timeit.default_timer()
    sec = stop - start
    print(f"\n{'=' * 80}\nTotal runtime: {sec} seconds.")


if __name__ == '__main__':
    run()
