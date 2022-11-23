import pandas as pd, numpy as np
import sqlite3 as sqlite
import re

sql_main = sqlite.connect('../data_main.db')
sql_raw = sqlite.connect('../data_raw.db')

def to_sql(df, name, con):
    return df.to_sql(name, con=con, index=None, if_exists='replace')

def read_sql(query:str, con, where=None):
    if "select " not in query.lower():
        query = f"select * from {query}"
    if where:
        query += " WHERE " + where
    return pd.read_sql(query, con=con)

read_raw = lambda *args, **kwargs: read_sql(*args, **kwargs, con=sql_raw)
read_main = lambda *args, **kwargs: read_sql(*args, **kwargs, con=sql_main)
write_raw = lambda *args, **kwargs: to_sql(*args, **kwargs, con=sql_raw)
write_main = lambda *args, **kwargs: to_sql(*args, **kwargs, con=sql_main)


'''
Some of the functions in this script will be added DIRECTLY to
classes within imported external packages, like pandas.
--
The two functions below, `_add_func_to_class`, and `_add_all_funs_to_classes`
allow this magic to happen.

Examples:
----------

>>> import pandas as pd
>>> def foo(self, text):
>>>     """
>>>     ::pd.DataFrame
>>>     This function does stuff
>>>     """
>>>     print(text)
>>>
>>> _add_func_to_class('foo')
>>> 
>>> df = pd.DataFrame()
>>> df.foo('hi')
hi

>>> import pandas as pd
>>> def bar(self, text):
>>>     """ ::pd.DataFrame,pd.Series
>>>     This function does other stuff
>>>     """
>>>     print(text)
>>> 
>>> _add_func_to_class('bar')
>>> 
>>> sr = pd.Series()
>>> df = pd.DataFrame()
>>> df.bar('hi')
hi
>>> sr.bar('hello')
hello

'''
def _add_func_to_class(name, func=None):
    if not func:
        func = globals().get(name)
    doc = func.__doc__
    if doc:
        # Find classes inside docstring with matching format
        objects = re.findall(r'^\s*:{2}(\S+)', doc)
        if len(objects) > 0:
            objects = objects[0].split(',')
            # Separate classes by comma
            for obj in objects:
                setattr(eval(obj), name, func)

def _add_all_funcs_to_classes():
    for name,func in list(globals().items()):
        if callable(func) or isinstance(func, property):
            # To define implementations of the same function on multiple
            # classes, all underscores at the end of each function will be removed
            name = name.rstrip('_')
            _add_func_to_class(name, func)


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


# Pandas added methods
# -------------------------------

def set_columns(self, new:list) -> pd.DataFrame:
    ''' ::pd.DataFrame '''
    self.columns = new
    return self


def rename_col(self, old:str, new:str) -> pd.DataFrame:
    ''' ::pd.DataFrame
    Traditional `rename(columns=dict())` syntax is verbose and harder to read and edit.
    '''
    return self.rename(columns={old:new})


def drop_cols(self, columns:str or list) -> pd.DataFrame:
    ''' ::pd.DataFrame '''
    cols = columns
    if isinstance(cols, str):
        cols = [cols]
    cols = [c for c in cols if c in self.columns]
    return self.drop(columns=cols)


def prefix_cols(self, cols:str or list, prefix:str) -> pd.DataFrame:
    ''' ::pd.DataFrame '''
    if isinstance(cols, str):
        cols = [cols]
    return self.rename(columns={col: f'{prefix}{col}' for col in cols})


def flatten_multi_level_cols(self) -> pd.DataFrame:
    ''' ::pd.DataFrame '''
    self.columns = self.columns.droplevel()
    self.columns.name = None
    return self


def col_replace(self, text:str or dict, replacement:str = None) -> pd.DataFrame:
    ''' ::pd.DataFrame
    Replace a pattern in each column NAME'''
    if replacement is None:
        if isinstance(text, dict):
            to_replace = text
        else:
            raise ValueError("Multiple values must be passed as dict")
    else:
        to_replace = {text: replacement}
    
    for old, new in to_replace.items():
        for c in self.columns:
            if c != old:
                self = self.rename(columns={c: c.replace(old, new)})
    return self


def coerce_type(self, dtype, subset:list=None, exclude:list=None) -> pd.DataFrame:
    ''' ::pd.DataFrame
    Iteratively try to set all columns to type
    '''
    cols = tuple(subset) if subset else tuple(self.columns)
    if exclude:
        cols = [c for c in cols if c not in exclude]
    for c in cols:
        try:
            self[c] = self[c].astype(dtype)
        except Exception:
            pass
    return self


def add_ordinal(self, col:str, order:list, replace=False) -> pd.DataFrame:
    """ ::pd.DataFrame
    Create ordinal col from existing categorical col. Pass an ascending list
    of categories. Example: Input ['A', 'B', 'C'] -> New column map: {'A': 1, 'B': 2, 'C': 3}
    """
    new = self[col].map({k: i+1 for i, k in enumerate(order)})
    self.insert_at(col, f'{col}_ord', new)
    if replace:
        self = self.drop(columns=col)
    return self


def insert_at(self, target:str or int, name:str, col:pd.Series) -> None:
    """ ::pd.DataFrame
    Insert col before target col name, or to index.
    Like df.insert(), but takes a column name as location, instead of int """
    if isinstance(target, int):
        idx = target
    else:
        idx = list(self.columns).index(target)
    self.insert(idx, name, col)
    return self


def add_binmax(self, name:str, cols:list, replace=False) -> pd.DataFrame:
    """ ::pd.DataFrame
    Shorthand for df.idxmax(), but lets you choose location and replace given columns """
    new = self[cols].idxmax(axis=1)
    self.insert_at(cols[0], name, new)
    if replace:
        self = self.drop(columns=cols)
    return self


def move_col(self, name:str, target:str or int) -> pd.DataFrame:
    """ ::pd.DataFrame
    Move col to before target col name, or to index.
    - Must not mutate original dataframe (so we can chain the func
      and re-run cells).
    - Placement must be correct: if target column is string, always
      place our column before the target column. If target is an index,
      the RESULTING dataframe must have our new column in the specified index.
    """
    cols = list(self.columns)

    if isinstance(target, int):
        idx = target
        cols.remove(name)
        cols.insert(idx, name)
    elif isinstance(target, str):
        idx = list(self.columns).index(target)
        cols[cols.index(name)] = "7dwIFmVgq5f1z"
        cols.insert(idx, name)
        cols.remove("7dwIFmVgq5f1z")

    return self[cols]


def combine_cols(self, name:str = None, cols:list=None, items:dict=None, replace=True, func=sum) -> pd.DataFrame:
    """ ::pd.DataFrame
    Given a list of column names, create a new column with their sum, and
    position it before the first col in 'cols'. So if replace=True, then
    the old columns will effectively be replaced in their original position.
    To do multiple sums, pass 'items' as dict with names as keys and col list as vals
    """
    if not items:
        items = {name: cols}
    for name, cols in items.items():
        new = func([self[c] for c in cols])
        self.insert_at(cols[0], name, new)
        if replace:
            self = self.drop(columns=cols)
    return self




_add_all_funcs_to_classes()