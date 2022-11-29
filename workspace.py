import pandas as pd, numpy as np
import pandas_plugin
import sqlite3 as sqlite
import re
from inject_attr import inject, Inject

sql = dict(
    raw=dict(con=sqlite.connect('data_raw.db'), cache=dict()),
    main=dict(con=sqlite.connect('data_main.db'), cache=dict()),
)


def fmt_query(*args, **kwargs) -> str:
    """
    Allow for more pythonic style of writing sql queries, for better
    readability, user experience, and user-error prevention. Underscores
    can be used to prefix/suffix python reserved word kwargs like FROM, IF, etc.
    --
    >>> fmt_query("SELECT name, age FROM my_table")
    SELECT name, age FROM my_table
    >>> fmt_query(select = "name, age", from_ = "my_table")
    SELECT name, age FROM my_table
    >>> fmt_query("name, age", FROM = "my_table")
    SELECT name, age FROM my_table
    """
    kwarg_query = " ".join([f'{k.strip("_").upper()} {v}' for k,v in kwargs.items()])
    if len(args) == 0:
        return kwarg_query
    return (("" if args[0].lower().strip().startswith('select') else "SELECT ")
            + args[0] + " " + kwarg_query
        ).strip()



def to_sql(df:pd.DataFrame, name:str, con:str):
    return df.to_sql(name, con=sql[con]['con'], index=None, if_exists='replace')


def read_sql(*args, con:str, table:str=None, **kwargs):
    """
    This method is specific to this script. It references global variables and funcs
    --
    >>> to_sql(pd.DataFrame({"a":[6],"b":[9]}), 'my_table', con='raw')
    >>> read_sql("select * from my_table", con="raw")
       a  b
    0  6  9
    >>> read_sql("my_table", con="raw")
       a  b
    0  6  9
    >>> read_sql(select="a AS A, b", from_="my_table", con='raw')
       A  b
    0  6  9
    """
    query = fmt_query(*args, **kwargs) if table is None else f"SELECT * FROM {table};"

    if (cached := sql[con]['cache'].get(query, None)) is not None:
        return cached.copy()

    df = sql[con]['cache'][query] = pd.read_sql(query, con=sql[con]['con'])
    return df


read_raw = lambda *args, **kwargs: read_sql(*args, **kwargs, con='raw')
read_main = lambda *args, **kwargs: read_sql(*args, **kwargs, con='main')
write_raw = lambda *args, **kwargs: to_sql(*args, **kwargs, con='raw')
write_main = lambda *args, **kwargs: to_sql(*args, **kwargs, con='main')



def head(*dfs, n=3, with_tail=False):
    '''
    Like display() and pd.DataFrame.head() got married and had a kid
    - `with_tail` will concat head and tail together.
    '''
    for df in dfs:
        print(f'{df.shape[1]} cols x {df.shape[0]} rows')
        if with_tail:
            display(pd.concat([df.head(n-1), df.tail(n-1)], axis=0))
        else:
            display(df.head(3))

