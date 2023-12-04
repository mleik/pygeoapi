import logging
import json
import geopandas as gpd

import duckdb

from pygeoapi.provider.base import (BaseProvider, ProviderConnectionError,
                                    ProviderNotFoundError, ProviderItemNotFoundError)
from pygeoapi.util import get_typed_value


LOGGER = logging.getLogger(__name__)

class AzureDeltaStorageProvider(BaseProvider):
    """GeoParquet storage provider"""

    def __init__(self, provider_def):
        super().__init__(provider_def)
        self.ignore_cols = provider_def.get('ignore')
        self.con = duckdb.connect()
        self.con.install_extension('spatial')
        self.con.load_extension('spatial')
        self.fields = self.get_fields()
        '''
        self.file_sys = provider_def.get('file_system')
        self.account_name = provider_def.get('account_name')
        self.container_name = provider_def.get('container_name')
        self.path = provider_def.get('path')
        self.https = f"https://{self.account_name}.blob.core.windows.net/{self.container_name}/{self.path}"
        self.abfss = f"abfss://{self.container_name}@{self.account_name}.dfs.core.windows.net/{self.path}"
        sas = create_azure_container_sas(uri=self.https, read=True, list=True)
        auth = AzureAuth(AZURE_STORAGE_SAS_TOKEN=sas)
        conn = FileConnection(uri=self.https, auth=auth)
        self.delta_ref = DeltaLakeReader(connection=conn, wkt_column=True)
        self.delta_ref.geopandas_read().to_parquet(self.data)
        '''

    def get_fields(self):
        LOGGER.debug('Fetching schema for delta table')
        schema_df = self.con.sql(f"DESCRIBE (SELECT * FROM '{self.data}')").fetchdf()[['column_name', 'column_type']]
        fields = {}
        for _, row in schema_df.iterrows():
            if row['column_name'] in self.ignore_cols:
                continue
            if row['column_name'] == self.id_field:
                type_ = 'string'
            elif row['column_name'] == 'geometry':
                type_ = 'geometry'
            elif row['column_type'] in ['DOUBLE']:
                type_ = 'number'
            elif row['column_type'] in ['BIGINT']:
                type_ = 'integer'
            else:
                type_ = 'string'
            fields[row['column_name']] = {'type': type_}
        return fields
    
    def _load(self, offset=0, limit=10, resulttype='results', identifier=None,
              bbox=[],datetime_=None, skip_geometry=None, properties=[], select_properties=[], q=None):
        LOGGER.info(bbox)
        #bbox = [-1.054688,58.608334,6.416016,63.21383]
        if identifier is not None:
            entry_ = self.con.sql(f'''SELECT *, 
                                  ST_AsWKB(ST_FlipCoordinates(ST_GeomFromWKB(geometry))) as geom 
                                  FROM '{self.data}' WHERE {self.id_field} = {identifier}''').fetchdf().drop(self.ignore_cols, axis=1,errors='ignore').drop(['geometry'], axis=1)
            if len(entry_) > 0:
                return json.loads(gpd.GeoDataFrame(entry_, geometry=gpd.GeoSeries.from_wkb(entry_['geom'].map(lambda geometry: bytes(geometry))))
                                  .drop(['geom'], axis=1).set_index(self.id_field).to_json())
            return '{}'
        if bbox:
            minx, miny, maxx, maxy = bbox
            data_ = self.con.sql(f'''SELECT *, ST_AsWKB(ST_FlipCoordinates(ST_GeomFromWKB(geometry))) as geom, 
                                 ST_FlipCoordinates(ST_GeomFromWKB(geometry)) as tmp_geom
                                 FROM '{self.data}' WHERE ST_WITHIN(tmp_geom, ST_Envelope(ST_GeomFromText('LineString({minx} {miny}, {maxx} {maxy})'))) 
                                 LIMIT {limit} OFFSET {offset}''').fetchdf().drop(self.ignore_cols,axis=1,errors='ignore').drop(['tmp_geom', 'geometry'],axis=1)
        else:
            data_ = self.con.sql(f'''SELECT *, ST_AsWKB(ST_FlipCoordinates(ST_GeomFromWKB(geometry))) as geom 
                                 FROM '{self.data}' LIMIT {limit} OFFSET {offset}''').fetchdf().drop(self.ignore_cols, axis=1, errors='ignore').drop(['geometry'],axis=1)
        return json.loads(gpd.GeoDataFrame(data_, geometry=gpd.GeoSeries.from_wkb(data_['geom'].map(lambda geometry: bytes(geometry)))).set_index(self.id_field).drop(['geom'], axis=1).to_json())
    
    def get(self, identifier, **kwargs):
        item = self._load(identifier=identifier)
        if item:
            return item['features'][0]
        else:
            err = f'item {identifier} not found'
            LOGGER.error(err)
            raise ProviderItemNotFoundError(err)
    
    def query(self, offset=0, limit=10, resulttype='results', 
              bbox=[], datetime_=None, properties=[], sortby=[], 
              select_properties=[], skip_geometry=False, q=None, **kwargs):
        data = self._load(offset, limit, resulttype, bbox=bbox, 
                          properties=properties, select_properties=select_properties, skip_geometry=skip_geometry)
        return data