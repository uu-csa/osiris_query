# standard library
import timeit
start = timeit.default_timer()
import argparse

#local
from query.query import connect, run_query


QUERIES = [
    ### ENTER QUERIES HERE ###
]


def run():
    # PARSE COMMAND LINE ARGUMENTS
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'collegejaar',
        help='Collegejaar waarvoor gegevens opgehaald worden.'
        )
    arg = parser.parse_args()
    parameters = vars(arg)

    # CONNECT TO DATABASE
    cursor = connect()

    # RUN QUERIES
    for query in QUERIES:
        run_query(query, cursor=cursor, parameters=parameters)

    # STOP TIMER AND PRINT RUNTIME
    stop = timeit.default_timer()
    sec = stop - start
    print(f"\n{'=' * 80}\nTotal runtime: {sec} seconds.")

    return None


if __name__ == '__main__':
    run()
