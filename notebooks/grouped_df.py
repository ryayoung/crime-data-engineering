import pandas as pd
from copy import deepcopy

def separate_by(df, txt, index, keep=[], start=False, end=False, mode="") -> (pd.DataFrame, pd.DataFrame):
    """
    Given a df and a substring, return two dfs:
    1. df containing: county + all columns whose name does NOT contain substring
    2. df containing: county + all columns whose name DOES contain substring
    """
    names = [c for c in df.columns if (
            c.startswith(txt) if start else c.endswith(txt) if end else txt in c
        )]

    include = df.copy()[index + keep + names]
    exclude = df.copy().drop(columns = keep + names)

    if mode == 'include': return include
        
    if mode == 'exclude': return exclude

    return (include, exclude)


def match_rename(df, text, replacement) -> pd.DataFrame:
    for c in df.columns:
        if c != text:
            df = df.rename(columns={c: c.replace(text, replacement)})
    return df



class GroupedDF(object):
    default_index = []
    groups: dict = None

    def __init__(self, df, index=[], custom={}, show_g_names=True):
        self.index = index
        if self.index == []: self.index = GroupedDF.default_index

        self.index = index
        self._df = deepcopy(df)
        self._show_g_names = show_g_names

        self._custom = custom
        self.refresh_groups()
    

    def refresh_groups(self):
        self._dict = {g: separate_by(self._df, g, self.index, start=True, mode='include') for g in GroupedDF.groups.keys()}

        if self._show_g_names == False:
            for k, v in self._dict.items():
                self._dict[k] = match_rename(v, f'{k}_', '')

        for name, cols in self._custom.items():
            self._dict[name] = self._df[cols]

        for k, v in self._dict.items():
            setattr(self, k, v)
    

    @classmethod
    def set_groups(cls, items: dict or list):

        if type(items) == list:
            cls.groups = {k: "" for k in items}
            return
        
        cls.groups = items


    @property
    def df(self):
        return self._df
    
    @df.setter
    def df(self, new):
        self._df = new
        self.refresh_groups()


    def __getattr__(self, name):
        return self._dict.get(name)

    def __getitem__(self, name):
        return self._dict[name]
    

    @property
    def dict(self):
        return self._dict
    
    @property
    def show_g_names(self):
        return self._show_g_names
    
    @show_g_names.setter
    def show_g_names(self, val:bool):
        self._show_g_names = val
        self.refresh_groups()
    

    def display(self, rows=3, exclude=[]):
        for k, v in self._dict.items():
            print(k, GroupedDF.groups[k], sep=': ')
            display(v.drop(columns=exclude).head(rows))
            print()