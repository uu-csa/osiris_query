import timeit
from src.querydef import QueryDef
from src.query import Query, connect, run_query, read_pickle


# CONNECT TO DATABASE
cursor = connect()


# REFERENTIETABELLEN
def r_opl():
    # opleidingen
    start = timeit.default_timer()

    query_name = 'r_opl'
    qd = QueryDef.from_file(f'referentie/{query_name}')
    q_output = Query.from_qd(qd, cursor=cursor)

    fac_name = {
        'RA': 'UCR',
        'UC': 'UCU',
        'IVLOS': 'GST',
        }
    q_output.frame.loc[:, 'faculteit'] = (
        q_output.frame['faculteit'].replace(to_replace=fac_name)
        )

    stop = timeit.default_timer()
    sec = stop - start

    q_output.sec = sec
    q_output.to_pickle()


if __name__ == '__main__':
    ref_tables = ['r_nat', 'r_ooa']
    for ref in ref_tables:
        run_query(ref)
    r_opl()
