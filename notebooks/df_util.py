import pandas as pd, numpy as np
import re


def head(*dfs):
    for df in [*dfs]:
        print(f'{df.shape[1]} cols x {df.shape[0]} rows')
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
    Renames one column.
    Traditional `rename(columns=dict())` syntax is verbose and harder to read and edit.
    '''
    return self.rename(columns={old:new})


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


def add_ordinal(self, col:str, order:list, replace=False) -> pd.DataFrame:
    """ ::pd.DataFrame
    Create ordinal col from existing categorical col. Pass an ascending list
    of categories. Example: Input ['A', 'B', 'C'] -> New column map: {'A': 1, 'B': 2, 'C': 3}
    """
    new = self[col].map({k: i+1 for i, k in enumerate(order)})
    self.insert_at(f'{col}_ord', col, new)
    if replace:
        self = self.drop(columns=col)
    return self


def insert_at(self, name:str, target:str or int, col:pd.Series) -> None:
    """ ::pd.DataFrame
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
    self.insert_at(name, cols[0], new)
    if replace:
        self = self.drop(columns=cols)
    return self


def move_col(self, name:str, target:str or int) -> pd.DataFrame:
    """ ::pd.DataFrame
    Move named col to right before target col """
    col = self.pop(name)
    if isinstance(target, int):
        idx = target
    else:
        idx = list(self.columns).index(target)
    self.insert(idx, name, col)
    return self


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
        self.insert_at(name, cols[0], new)
        if replace:
            self = self.drop(columns=cols)
    return self







def _add_func_to_object(name, func=None):
    if not func:
        func = globals().get(name)
    doc = func.__doc__
    if doc:
        objects = re.findall(r'^\s*:{2}(\S+)', doc)
        if len(objects) > 0:
            objects = objects[0].split(',')
            for obj in objects:
                setattr(eval(obj), name, func)

def _add_all_funcs_to_objects():
    for name,func in list(globals().items()):
        if callable(func) or isinstance(func, property):
            name = name.rstrip('_')
            _add_func_to_object(name, func)


_add_all_funcs_to_objects()