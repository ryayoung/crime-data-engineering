import pandas as pd

def standardize_district_name(
            name:str,
            # remove_lit:list = None,
            # remove_re:list = None
        ) -> str:
    '''
    Apply this iteratively (with pd.Series.apply())
    to school district name columns to standardize their naming conventions
    as best as possible prior to merging datasets with potentially
    very different naming conventions.
    '''
    import re
    name = name.upper()
    name = re.sub('S/D', '', name)
    name = re.sub('-|\.|\(|\)|/|:', '', name)
    name = re.sub(' CONSOLIDATED', '', name)
    name = re.sub('\s?SCHOOL DISTRICT', '', name)
    
    # Number patterns
    name = re.sub(r' RENO\s?(\d+)', r'\1', name)
    name = re.sub(r' NO\s?(\d+)', r'\1', name)
    name = re.sub(r' RD\s?(\d+)', r'\1', name)
    name = re.sub(r' RJ\s?(\d+)', r'\1', name)
    name = re.sub(r' RE\s?(\d+)J?T?', r'\1', name)
    name = re.sub(r' R\s?(\d+)J?', r'\1', name)
    name = re.sub(r' C\s?(\d+)', r'\1', name)

    # Remove spaces
    name = re.sub('\s', '', name)

    # Number patterns (text at end)
    name = re.sub(r'(\d+)R', r'\1', name)
    name = re.sub(r'(\d+)J', r'\1', name)
    name = re.sub(r'(\d+)JT', r'\1', name)

    # Delete text parts
    name = re.sub('RURAL', '', name)
    name = re.sub('SCHOOLS', '', name)
    # name = re.sub('SCHOOLDISTRICT', '', name)
    name = re.sub('SCHOOLDIST', '', name)
    name = re.sub('WATERSHED', '', name)
    name = name.strip()

    # Replace full
    name = name.replace(r'GILCREST', 'WELDCOUNTY')
    name = name.replace(r'FLORENCE', 'FREMONT')
    name = name.replace(r'CONSOLIDATED1', 'CUSTERCOUNTY1')
    name = re.sub(r'(PUEBLOCITY)(\d+)', r'\1', name)
    name = re.sub(r'^CREEDE$', r'CREEDE1', name)

    name = name.strip()
    # Push number out
    name = re.sub(r'(.*?)(\d+)(.*)', r'\1\3 \2', name)
    return name



def join_conflicts(
            df1: pd.DataFrame,
            df2: pd.DataFrame,
            col: str
        ) -> pd.DataFrame:
    '''
    Use when trying to join columns and see values that aren't shared.
    There's DEFINITELY a better way to do this. But I'm lazy, and nobody cares!
    '''
    import pandas as pd

    col_items1 = sorted(df1[col])
    col_items2 = sorted(df2[col])

    items1_diff = [i for i in col_items1 if i not in col_items2]
    items2_diff = [i for i in col_items2 if i not in col_items1]

    # Make lists same length.
    if len(items1_diff) > len(items2_diff):
        items2_diff += [None] * (len(items1_diff) - len(items2_diff))
    elif len(items2_diff) > len(items1_diff):
        items1_diff += [None] * (len(items2_diff) - len(items1_diff))
    
    return pd.DataFrame(list(zip(items1_diff, items2_diff)))


