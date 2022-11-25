import pandas as pd, numpy as np
import pandas_plugin
import sqlite3 as sqlite
import re

sql = dict(
    raw=sqlite.connect('data_raw.db'),
    main=sqlite.connect('data_main.db'),
)
cache = dict(
    raw=dict(),
    main=dict(),
)

def to_sql(df:pd.DataFrame, name:str, con:str):
    return df.to_sql(name, con=sql[con], index=None, if_exists='replace')

def read_sql(query:str, con:str, where=None):
    if "select" not in query.lower():
        query = f"select * from {query}"
    if where:
        query += " WHERE " + where
    
    if query in cache[con]:
        return cache[con][query].copy()
    
    df = pd.read_sql(query, con=sql[con])
    cache[con][query] = df
    return df

read_raw = lambda *args, **kwargs: read_sql(*args, **kwargs, con='raw')
read_main = lambda *args, **kwargs: read_sql(*args, **kwargs, con='main')
write_raw = lambda *args, **kwargs: to_sql(*args, **kwargs, con='raw')
write_main = lambda *args, **kwargs: to_sql(*args, **kwargs, con='main')


# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------


def head(*dfs, n=3, with_tail=False):
    '''
    Like display() and pd.DataFrame.head() got married and had a kid
    - More flexible than display() with shorter previews by default
    - Better than .head() because it prints the ACTUAL shape of the
    dataframe, not the shape of the preview.
    - `with_tail` will concat head and tail together.
    '''
    for df in [*dfs]:
        print(f'{df.shape[1]} cols x {df.shape[0]} rows')
        if with_tail:
            display(pd.concat([df.head(n-1), df.tail(n-1)], axis=0))
        else:
            display(df.head(3))


def separate_by(df, to_match:list or str, index=[], keep=[], start=False, end=False, mode=None) -> (pd.DataFrame, pd.DataFrame):
    """
    Given a df and a substring, return two dfs:
    1. df containing: county + all columns whose name does NOT contain substring
    2. df containing: county + all columns whose name DOES contain substring
    """
    if not isinstance(to_match, list):
        to_match = [to_match]

    names = [item for sublist in [[c for c in df.columns if (
            c.startswith(txt) if start else c.endswith(txt) if end else txt in c
        )] for txt in to_match] for item in sublist]

    include = df.copy()[index + keep + names]
    exclude = df.copy().drop(columns = keep + names)

    if mode == 'include':
        return include
    if mode == 'exclude':
        return exclude
    return (include, exclude)

