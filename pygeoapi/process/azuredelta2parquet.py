import logging
import time

from eqspatial.Auth import AzureAuth
from eqspatial.Connections import FileConnection
from eqspatial.Readers import DeltaLakeReader
from eqspatial.helpers import create_azure_container_sas

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
  "id": "Delta2Parquet",
  "title": "Parquet Update From Delta",
  "description": "Updates the data from the delta lake.",
  "version": "1.0.0",
  "jobControlOptions": [
    "async-execute"
  ],
  "outputTransmission": [
    "value",
    "reference"
  ],
  "inputs": {
    "deltaAccount": {
      "title": "Delta Account Name",
      "description": "Azure account name where the delta table resides.",
      "minOccurs": 1,
      "maxOccurs": 1,
      "schema": {
        "type": "string"
      }},
      "deltaContainer": {
      "title": "Delta Container Name",
      "description": "Azure container name where the delta table resides.",
      "minOccurs": 1,
      "maxOccurs": 1,
      "schema": {
        "type": "string"
      }},
      "deltaBlobPath": {
      "title": "Delta Blob Path Name",
      "description": "Azure blob path where the delta table resides.",
      "minOccurs": 1,
      "maxOccurs": 1,
      "schema": {
        "type": "string"
      }},
    "parquetPath": {
      "title": "Parquet Path",
      "description": "Value to control the processing time.",
      "minOccurs": 1,
      "maxOccurs": 1,
      "schema": {
        "type": "string"
      }
    }
  },
  "outputs": {
    "status": {
      "schema": {
        "type": "string"
      }
    }
  },
  "links": [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
  'example': {
    'inputs': {
      'deltaAccount': 's576strdls00euwr6',
      'deltaContainer': 'silver',
      'deltaBlobPath': 'spire/ais/delta',
      'parquetPath': 'test/test.parquet'
      }
  }
}

class Delta2ParquetProcessor(BaseProcessor):
    """Echo Processor example"""
    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: pygeoapi.process.echo.echoProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):


        mimetype = 'application/json'

        account_name = data.get('deltaAccount')
        container_name = data.get('deltaContainer')
        path = data.get('deltaBlobPath')
        parquet_path = data.get('parquetPath')

        if account_name is None or container_name is None or path is None or parquet_path is None:
            raise ProcessorExecuteError(
                'Cannot run process without delta and parquet paths specified value')
        
        https = f"https://{account_name}.blob.core.windows.net/{container_name}/{path}"
        sas = create_azure_container_sas(uri=https, read=True, list=True)
        auth = AzureAuth(AZURE_STORAGE_SAS_TOKEN=sas)
        conn = FileConnection(uri=https, auth=auth)
        self.delta_ref = DeltaLakeReader(connection=conn, wkt_column=True)
        self.delta_ref.geopandas_read().to_parquet(parquet_path)
        outputs = {'status': 'ok'}

        return mimetype, outputs

    def __repr__(self):
        return f'<Delta2ParquetProcessor> {self.name}'