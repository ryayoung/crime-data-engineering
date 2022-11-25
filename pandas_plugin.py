import pandas as pd
# from method_injector import add_all_funcs_to_classes

def set_columns(self, new:list) -> pd.DataFrame:
    ''' ::pd.DataFrame '''
    self.columns = new
    return self


def rename_col(self, old:str, new:str) -> pd.DataFrame:
    ''' ::pd.DataFrame
    Traditional `rename(columns=dict())` syntax is verbose and harder to read and edit.
    '''
    return self.rename(columns={old:new})


def drop_cols(self, *columns) -> pd.DataFrame:
    ''' ::pd.DataFrame '''
    cols = list(columns)
    if len(cols) == 1 and isinstance(cols[0], list):
        cols = cols[0]
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


execfile('method_injector.py')
add_all_funcs_to_classes()