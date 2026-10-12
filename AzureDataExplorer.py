from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
import pandas as pd


class AzureDataExplorer:
    TENANT_ID = ""
    KUSTO_CLUSTER = ""

    def __init__(self):
        #self.connection_builder = KustoConnectionStringBuilder.with_aad_device_authentication(self.KUSTO_CLUSTER)
        self.connection_builder = KustoConnectionStringBuilder.with_aad_user_password_authentication(self.KUSTO_CLUSTER, 'example_user', 'example_password', self.TENANT_ID)
        self.connection_builder.authority_id = self.TENANT_ID
        self.azure_client = KustoClient(self.connection_builder)

    def query_from_adx(self, database, query):
        data = self.azure_client.execute(database, query)
        df = dataframe_from_result_table(data.primary_results[0])
        return df




