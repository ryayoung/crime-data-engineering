import geopandas as gp
from shapely import wkt
import pandas as pd
from copy import deepcopy

class GeoDF(gp.GeoDataFrame):

    def __init__(self, df, geo=None, crs='epsg:4326'):
        if type(df) == str:
            df = pd.read_csv(df)
        if type(df) == pd.DataFrame:
            df = df.copy()

            cols = [c for c in df.columns if c.startswith('geo_')]
            for c in cols:
                df[c] = df[c].fillna('GEOMETRYCOLLECTION EMPTY')
                df[c] = gp.GeoSeries(df[c].apply(wkt.loads))

            if not geo:
                geo = cols[0]

            df['geometry'] = df[geo]

        super(GeoDF, self).__init__(df, crs=crs)
    

    def explore(self, tooltip=None, geo=None, **kwargs):
        if geo:
            self.set_geo(geo)
        
        if not tooltip:
            tooltip = self.columns[0]
            if 'county' in self.columns and 'dist' in self.columns:
                tooltip = ['county', 'dist']

        return super().loc[self['geometry'].astype(str) != 'GEOMETRYCOLLECTION EMPTY'].explore(tooltip=tooltip, **kwargs)


    def df(self):
        return self[self.geometry.astype(str) != 'GEOMETRYCOLLECTION EMPTY']


    def set_geo(self, geo, crs='epsg:4326'):
        self['geometry'] = self[geo]
    

    def copy(self):
        return GeoDF(super().copy())