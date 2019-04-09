# set CONDA_FORCE_32BIT = 1
# activate py32

import pyodbc
import timeit
import pickle
import pandas as pd
from pathlib import Path

login = Path('u:/uustprd.txt').read_text().split('\n')[1].split(',')

uid = login[0]
pwd = login[1]

param = f'DSN=UUSTPRD;DBQ=UUSTPRD;STPRD;UID={uid};PWD={pwd};CHARSET=UTF8'
conn = pyodbc.connect(param)
cursor = conn.cursor()

cols = [
    'owner',
    'table_name',
    'column_name',
    'data_type',
    'data_type_mod',
    'data_type_owner',
    'data_length',
    'data_precision',
    'data_scale',
    'nullable',
    'column_id',
    'default_length',
    'data_default',
    'num_distinct',
    'low_value',
    'density',
    'num_nulls',
    'num_buckets',
    'last_analyzed',
    'sample_size',
    'character_set_name',
    'char_col_decl_lenght',
    'global_stats',
    'users_stats',
    'avg_col_len',
    'char_length',
    'char_used',
    'v80_fmt_image',
    'data_upgraded',
    'histogram',
]

tbl = 'OST_STUDENT_INSCHRIJFHIST'

sql = (f"""
select *
  from ALL_TAB_COLUMNS
 where table_name = '{tbl}'
"""
)

print(sql)
start = timeit.default_timer()

cursor.execute(sql)
df = pd.DataFrame.from_records(cursor.fetchall(), columns=cols)

stop = timeit.default_timer()
time = stop - start
print(f'Query finished in {time:.2f} sec.')

pack = {
    'query': sql,
    'time': time,
    'frame': df,
}

with open (f'{tbl}.pkl', 'wb') as f:
    pickle.dump(pack, f)
