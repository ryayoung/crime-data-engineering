import pandas as pd
from inject_attr import Inject

def args_to_list(*args) -> list:
    """
    >>> args_to_list('a', 'b')
    ['a', 'b']
    >>> args_to_list(['a', 'b'])
    ['a', 'b']
    >>> args_to_list(['a'], 'b')
    ['a', 'b']
    >>> args_to_list(['a'], ['b'])
    ['a', 'b']
    """
    is_list_or_tuple = lambda i: isinstance(i, list) or isinstance(i, tuple)
    return [ item for sublist in 
            [ i if is_list_or_tuple(i) else [i] for i in args ]
        for item in sublist ]



class _(Inject, to=pd.DataFrame):
    """
    """

    def set_columns(self, *new) -> pd.DataFrame:
        new = args_to_list(*new)
        self.columns = new
        return self


    def rename_col(self, old:str, new:str) -> pd.DataFrame:
        '''
        Traditional `rename(columns=dict())` syntax is verbose and harder to read and edit.
        '''
        return self.rename(columns={old:new})


    def drop_cols(self, *columns) -> pd.DataFrame:
        cols = args_to_list(*columns)
        cols = [c for c in cols if c in self.columns]
        return self.drop(columns=cols)


    def prefix_cols(self, cols:str or list, prefix:str) -> pd.DataFrame:
        """
        Like df.add_prefix(), but takes a subset of columns as first positional
        """
        if isinstance(cols, str):
            cols = [cols]
        return self.rename(columns={col: f'{prefix}{col}' for col in cols})


    def reset_multilevel_columns(self, *new_columns) -> pd.DataFrame:
        """
        Use this after df.pivot() to flatten and rename columns.
        """
        self = self.reset_index()
        self.columns = self.columns.droplevel()
        self.columns.name = None
        self = self.set_columns(*new_columns)
        return self


    def col_replace(self, text:str or dict, replacement:str = None) -> pd.DataFrame:
        '''
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
        '''
        Iteratively try to set all columns to type
        '''
        df = self.copy()
        cols = tuple(subset) if subset else tuple(self.columns)
        if exclude:
            cols = [c for c in cols if c not in exclude]
        for c in cols:
            try:
                df[c] = df[c].astype(dtype)
            except Exception:
                pass
        return df


    def insert_at(self, target:str or int, name:str, col:pd.Series) -> None:
        """
        Insert col before target col name, or to index.
        Like df.insert(), but takes a column name as location, instead of int """
        df = self.copy()
        if isinstance(target, int):
            idx = target
        else:
            idx = list(df.columns).index(target)
        df.insert(idx, name, col)
        return df


    def move_col(self, name:str, target:str or int) -> pd.DataFrame:
        """
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


    def separate_by(self, to_match:list or str, index=[], keep=[], start=False, end=False, mode=None) -> pd.DataFrame:
        """
        Given a df and a substring, filter for columns whose name does, or
        does not, contain a substring
        """
        if not isinstance(to_match, list):
            to_match = [to_match]

        names = [item for sublist in [[c for c in self.columns if (
                c.startswith(txt) if start else c.endswith(txt) if end else txt in c
            )] for txt in to_match] for item in sublist]

        if mode == 'include':
            return self.copy()[index + keep + names]
        if mode == 'exclude':
            return self.copy().drop(columns = keep + names)


    def display(self, text=None, head=True) -> None:
        if text is not None:
            print(text)
        to_display = self
        if head:
            to_display = self.head(3)
        display(to_display)
        return self