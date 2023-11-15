from datetime import datetime, timezone, timedelta
import logging
import os
import json

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, generate_container_sas, ContainerSasPermissions

from deltalake import DeltaTable
import geopandas as gpd


from pygeoapi.provider.base import (BaseProvider, ProviderConnectionError,
                                    ProviderNotFoundError, ProviderItemNotFoundError)
from pygeoapi.util import file_modified_iso8601, get_path_basename, url_join

LOGGER = logging.getLogger(__name__)

class AzureDeltaStorageProvider(BaseProvider):
    """Azure delta storage provider"""

    def __init__(self, provider_def):
        super().__init__(provider_def)
        self.geometry_col_name = provider_def.get('geometry_col_name')
        self.delta_account_name = provider_def.get('delta_account_name')
        self.delta_container_name = provider_def.get('delta_container_name')
        self.delta_path = provider_def.get('delta_path')
        self.sas = None
        if self.geometry_col_name is None:
            try:
                self.geometry_x = provider_def.get('geometry').get('x_field')
                self.geometry_y = provider_def.get('geometry').get('y_field')
            except:
                LOGGER.error('Invalid provider def')
        self.delta_abfss = provider_def.get('data')
        LOGGER.info(self.delta_abfss)

        self._delta_table = DeltaTable(f'abfss://{self.delta_container_name}@{self.delta_account_name}.dfs.core.windows.net/{self.delta_path}', 
                                       storage_options={'azure_storage_sas_token': self._get_or_create_sas()})


    def get_fields(self):
        LOGGER.debug('Fetching schema for delta table')
        return self._delta_table.schema()
    
    def _load(self, skip_geometry=None, properties=[], select_properties=[]):
        df = self._delta_table.to_pandas().head(3)
        if skip_geometry:
            return df
        gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['position']))
        return gdf
    
    def get(self, identifier):
        item = json.loads(self._load().to_json())
        if item:
            return item
        else:
            err = f'item {identifier} not found'
            LOGGER.error(err)
            raise ProviderItemNotFoundError(err)
    
    def query(self, offset=0, limit=10, resulttype='results', 
              bbox=[], datetime_=None, properties=[], sortby=[], 
              select_properties=[], skip_geometry=False, q=None, **kwargs):
        data = self._load()
        queried = data.iloc[offset:offset+limit]
        return json.loads(data.to_json())
    
    def _get_or_create_sas(self, credential=DefaultAzureCredential()):
        """Create a SAS token for a given blob container

        @params:
            uri (_type_): uri to azure blob

            credential (DefaultAzureCredential): to be able to generate an SAS tokeDefaults to DefaultAzureCredential().

            read (bool, optional): Set rights. Defaults to False.

            list (bool, optional): Set rights. Defaults to False.

            write (bool, optional): Set rights. Defaults to False.

            create (bool, optional): Set rights. Defaults to False.

            delete (bool, optional): Set rights. Defaults to False.

        Returns:
            sas_token
        """
        if self._valid_sas():
            return self.sas
        account_name = self.delta_account_name
        account_url = f'https://{account_name}.blob.core.windows.net'
        container_name = self.delta_container_name
        blob_name = self.delta_path
        blob_client = BlobServiceClient(account_url=account_url, container_name=container_name, blob_name=blob_name, credential=credential)
        user_delegation_key = blob_client.get_user_delegation_key(datetime.now(timezone.utc),datetime.now(timezone.utc)+timedelta(minutes=5))
        self.sas = generate_container_sas(account_name=account_name, container_name=container_name, user_delegation_key=user_delegation_key, 
                                            permission=ContainerSasPermissions(read=True, list=True), 
                                            expiry=datetime.now(timezone.utc)+timedelta(minutes=5))    
        return self.sas
    
    def _valid_sas(self):
        if not self.sas:
            return False
        elem = self.sas.split('&')
        end_time = datetime.strptime([x for x in elem if x.startswith('se')][0][3:], '%Y-%m-%dT%H:%M:%SZ')
        return end_time > datetime.now()
