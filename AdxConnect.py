from __future__ import annotations

import logging
import os
from typing import Any, Optional

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None

try:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
    from azure.kusto.data.exceptions import KustoServiceError
except Exception:  # pragma: no cover
    KustoClient = None
    KustoConnectionStringBuilder = None
    KustoServiceError = Exception

logger = logging.getLogger(__name__)


class AdxConnect:
    """Python equivalent of a MATLAB ADX connection helper."""

    def __init__(
        self,
        data_source: Optional[str] = None,
        database_name: Optional[str] = None,
        connection_timeout: int = 60,
        **kwargs: Any,
    ):
        self.connection = None
        self.data_source = data_source or os.environ.get(
            "ADX_DATA_SOURCE",
            "example.cluster.kusto.windows.net",
        )
        self.database_name = database_name or os.environ.get("ADX_DATABASE_NAME", "example_database")
        try:
            self.connection_timeout = int(connection_timeout)
        except (TypeError, ValueError):
            self.connection_timeout = 60

        self.connect()

    def fetch(self, query: str):
        if not self.is_connection_open():
            raise RuntimeError(
                "AdxConnect: invalid connection. Expected an active ADX client connection."
            )
        if not isinstance(query, str):
            raise TypeError("query must be a string.")

        result = self.connection.execute(self.database_name, query)

        if hasattr(result, "primary_results") and getattr(result, "primary_results", None):
            table = result.primary_results[0]
            if hasattr(table, "to_dataframe"):
                return table.to_dataframe()
            if hasattr(table, "rows"):
                rows = list(table.rows)
                if hasattr(table, "columns"):
                    return pd.DataFrame(rows, columns=[c.column_name for c in table.columns])
                return rows

        if hasattr(result, "to_dataframe"):
            return result.to_dataframe()

        return result

    def close(self):
        self.connection = None

    def connect(self):
        if self.is_connection_open():
            return self.connection

        self.connection = self.make_adx_connection()
        return self.connection

    def ensure_connection(self):
        if not self.is_connection_open():
            self.connection = self.make_adx_connection()
        return self.connection

    def is_connection_open(self) -> bool:
        return self.connection is not None

    def make_adx_connection(self):
        if KustoClient is None or KustoConnectionStringBuilder is None:
            raise RuntimeError(
                "azure-kusto-data is required for AdxConnect. Install the package first."
            )

        cluster = self.data_source if self.data_source.startswith("https://") else f"https://{self.data_source}"

        tenant_id = os.environ.get("AZURE_TENANT_ID")
        client_id = os.environ.get("AZURE_CLIENT_ID")
        client_secret = os.environ.get("AZURE_CLIENT_SECRET")

        if tenant_id and client_id and client_secret:
            csb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                cluster,
                client_id,
                client_secret,
                tenant_id,
            )
        else:
            csb = KustoConnectionStringBuilder.with_aad_device_authentication(cluster)

        client = KustoClient(csb)
        logger.info("Connected to ADX cluster %s database %s", cluster, self.database_name)
        return client


__all__ = ["AdxConnect"]
