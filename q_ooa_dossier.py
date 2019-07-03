# standard library
import timeit
start = timeit.default_timer()
import argparse

#local
from src.querydef import QueryDef
from src.query import Query, connect, run_query


def set_criteria(items):
    return ["is null" if item is None else f"= '{item}'" for item in items]

def combine_criteria(items):
    return [f"(s_ooa_aan.STATUS {item[0]} and s_ooa_aan.BESLUIT {item[1]})" for item in items]


QUERIES = [
    'ooa/s_ooa_dos',
]

def run():
    # PARSE COMMAND LINE ARGUMENTS
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'proces',
        help='Naam van het OOA-proces.'
        )
    parser.add_argument(
        'status_besluit',
        choices=['I', 'A', 'F', 'T'],
        help=(
            'I: Ingediend en in behandeling\n'
            'A: Afgehandeld en toegelaten\n'
            'F: Afgehandeld, toegelaten of afgewezen\n'
            'T: Ingediend en/of toegelaten'
            )
        )
    args = parser.parse_args()

    # G:Geannuleerd,
    # F:Afgehandeld,
    # B:In behandeling,
    # I:Ingediend,
    status = {
        'I': ['I', 'B',],
        'A': ['G', 'F',],
        'F': ['G', 'G', 'F', 'F',],
        'T': ['I', 'B', 'G', 'F',],
    }

    # A:Afgewezen,
    # T:Voldaan,
    besluit = {
        'I': [None, None,],
        'A': ['T', 'T',],
        'F': ['A', 'T', 'A', 'T',],
        'T': [None, None, 'T', 'T',],
    }

    key = args.status_besluit
    status = set_criteria(status[key])
    besluit = set_criteria(besluit[key])
    status_besluit = combine_criteria(list(zip(status, besluit)))
    joint = ' \nor '
    criterium = f"({joint.join(status_besluit)})"

    parameters = {
        'proces': args.proces,
        'status_besluit': criterium
    }

    # CONNECT TO DATABASE
    cursor = connect()

    # RUN SQL QUERY
    query = 'ooa/s_ooa_dos'
    qd = QueryDef.from_file(
        query,
        parameters=parameters,
        param_repr=f'{args.proces}_{args.status_besluit}')
    q_out = Query.from_qd(qd, cursor=cursor)
    q_out.to_pickle()

    # STOP TIMER AND PRINT RUNTIME
    stop = timeit.default_timer()
    sec = stop - start
    print(f"\n{'=' * 80}\nTotal runtime: {sec} seconds.")


if __name__ == '__main__':
    run()
