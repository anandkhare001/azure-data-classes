from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import pyodbc
import requests

logger = logging.getLogger(__name__)


class Databricks:
    """Python equivalent of the MATLAB Databricks helper."""

    TOKEN_URL = (
        ""
    )
    CLIENT_ID = ""
    HOST = ""
    HTTP_PATH = ""
    TOKEN_CACHE_FOLDER = Path(r"network drive path")

    def __init__(self, client_secret: Optional[str] = None, connect_timeout: int = 60, max_retry_duration: timedelta = timedelta(hours=1), expire_margin: timedelta = timedelta(seconds=30)):
        self._client_secret = client_secret or os.environ.get("DATABRICKS_CLIENT_SECRET", "")
        if not self._client_secret:
            raise ValueError("client_secret must be supplied or set via DATABRICKS_CLIENT_SECRET.")
        self.token = ""
        self.token_expire_by = datetime.now(tz=timezone.utc) - timedelta(days=365)
        self.expire_margin = expire_margin
        self.connect_timeout = connect_timeout
        self.max_retry_duration = max_retry_duration
        self.connection = None
        self._connect()

    def fetch(self, query: str) -> pd.DataFrame:
        if not self._is_connection_open():
            raise RuntimeError("No valid open Databricks connection available.")
        cursor = self.connection.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame([list(row) for row in rows], columns=columns)

    def run_data_query(self, turbine_id, metric_id, time_start: datetime, time_end: datetime, catalog: str = "example_catalog", value_type: str = "Value") -> pd.DataFrame:
        if isinstance(turbine_id, int):
            turbine_id = [turbine_id]
        if isinstance(metric_id, int):
            metric_id = [metric_id]

        limit = 100_000
        offset = 0
        chunks = []
        while True:
            q = self.data_query(turbine_id, metric_id, time_start, time_end, catalog, value_type)
            paged = f"{q} limit {limit} offset {offset}"
            retry_start = time.monotonic()
            while True:
                elapsed = time.monotonic() - retry_start
                if elapsed >= self.max_retry_duration.total_seconds():
                    raise RuntimeError(f"Databricks authorization/session retry exceeded maximum duration ({self.max_retry_duration}) for current query page (offset={offset}).")
                try:
                    chunk = self.fetch(paged)
                    break
                except Exception as exc:
                    msg = str(exc).lower()
                    is_auth = any(k in msg for k in ("unauthorized", "forbidden", "token expired"))
                    if not is_auth:
                        raise
                    logger.warning("Databricks auth/session error - refreshing token and reconnecting in 60 s... Error: %s", exc)
                    self.connection = None
                    self.token_expire_by = datetime.now(tz=timezone.utc) - timedelta(days=365)
                    time.sleep(60)
                    self._connect()
            chunks.append(chunk)
            if len(chunk) < limit:
                break
            offset += limit
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

    def close(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception:
                pass
        self.connection = None

    def _acquire_token(self):
        resp = requests.post(
            self.TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.CLIENT_ID,
                "client_secret": self._client_secret,
                "scope": "",
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        self.token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self.token_expire_by = datetime.now(tz=timezone.utc) + timedelta(seconds=expires_in)

    def _get_token(self) -> str:
        now_utc = datetime.now(tz=timezone.utc)
        if now_utc - self.expire_margin <= self.token_expire_by:
            return self.token

        cache_dir = self.TOKEN_CACHE_FOLDER
        cache_dir.mkdir(parents=True, exist_ok=True)
        now_ms = int(now_utc.timestamp() * 1000)
        for file in sorted(cache_dir.glob("*.token"), reverse=True):
            try:
                ts = int(file.stem)
            except ValueError:
                continue
            if ts >= now_ms - 60_000:
                self.token = file.read_text(encoding="utf-8").strip()
                return self.token
        self._acquire_token()
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / f"{now_ms + 3_600_000}.token").write_text(self.token, encoding="utf-8")
        return self.token

    def _connect(self):
        if self._is_connection_open():
            return
        retry_delay = 60
        retry_start = time.monotonic()
        last_error = ""
        while True:
            elapsed = time.monotonic() - retry_start
            if elapsed >= self.max_retry_duration.total_seconds():
                raise RuntimeError(f"Failed to establish Databricks connection within {self.max_retry_duration}. Last error: {last_error}")
            try:
                conn_str = (
                    f"DRIVER={{Simba Spark ODBC Driver}};"
                    f"Host={self.HOST};"
                    "Port=443;"
                    f"HTTPPath={self.HTTP_PATH};"
                    "SSL=1;"
                    "Schema=example_schema;"
                    "Min_TLS=1.2;"
                    f"SocketTimeout={self.connect_timeout};"
                    "UseSystemTrustStore=1;"
                    "EnableQueryResultDownload=1;"
                    "AuthMech=11;"
                    "Auth_Flow=0;"
                    f"Auth_AccessToken={self._get_token()};"
                )
                self.connection = pyodbc.connect(conn_str, autocommit=True)
                return
            except Exception as exc:
                last_error = str(exc)
                self.connection = None
                logger.warning("Connection attempt failed. Retrying in %d s...\nError: %s", retry_delay, last_error)
                time.sleep(retry_delay)

    def _ensure_connection(self):
        if not self._is_connection_open():
            self._connect()

    def _is_connection_open(self) -> bool:
        if self.connection is None:
            return False
        try:
            self.connection.cursor().execute("SELECT 1")
            return True
        except Exception:
            return False

    @staticmethod
    def metric_query(metric):
        if isinstance(metric, str):
            metric = [metric]
        pattern = "|".join(re.escape(str(item).replace("_", ".")) for item in metric)
        return (
            "write query here"
        )

    @staticmethod
    def turbine_query(turbine_id):
        if isinstance(turbine_id, int):
            turbine_id = [turbine_id]
        ids = ",".join(str(i) for i in turbine_id)
        return (
            "write query here"
        )

    @staticmethod
    def data_query(turbine_id, metric_id, time_begin: datetime, time_end: datetime, catalog: str = "example_catalog", value_type: str = "Value"):
        if isinstance(turbine_id, int):
            turbine_id = [turbine_id]
        if isinstance(metric_id, int):
            metric_id = [metric_id]
        if catalog not in {"example_catalog", "example_catalog_alt"}:
            raise ValueError("catalog must be either 'example_catalog' or 'example_catalog_alt'.")
        if value_type not in {"Value", "StringValue", "DateTimeValue"}:
            raise ValueError("value_type must be one of 'Value', 'StringValue', 'DateTimeValue'.")
        if time_end <= time_begin:
            raise ValueError("time_end must be greater than time_begin.")

        turbine_str = ",".join(str(i) for i in turbine_id)
        metric_str = ",".join(str(i) for i in metric_id)
        begin_str = time_begin.strftime("%Y-%m-%d %H:%M:%S")
        end_str = time_end.strftime("%Y-%m-%d %H:%M:%S")

        years = sorted({d.year for d in _date_range(time_begin.date(), time_end.date())})
        months = sorted({d.month for d in _date_range(time_begin.date(), time_end.date())})
        days = sorted({d.day for d in _date_range(time_begin.date(), time_end.date())})
        year_str = ",".join(f"'{y}'" for y in years)
        month_str = ",".join(f"'{m}'" for m in months)
        day_str = ",".join(f"'{d}'" for d in days)

        return (
            "write query here"
        )

    @staticmethod
    def find_metric_id(metric_names, metric_data):
        if isinstance(metric_names, str):
            metric_names = [metric_names]
        metric_data = metric_data.copy()
        if "METRICName" not in metric_data.columns:
            raise ValueError("metric_data must contain a 'METRICName' column.")
        rows = []
        ids = []
        for name in metric_names:
            candidates = metric_data[metric_data["METRICName"].astype(str).str.contains(re.escape(name.replace("_", ".")), case=False, na=False)]
            if candidates.empty:
                continue
            row = candidates.iloc[0].to_dict()
            rows.append(row)
            ids.append(int(row.get("METRICID", 0)))
        return rows, ids

    @staticmethod
    def create_turbine_info(raw_data):
        if raw_data is None or raw_data.empty:
            return -1
        valid = raw_data.dropna(subset=["TurbineName", "TurbineNumber", "CountryName", "ScadaParkName"]).copy()
        if valid.empty:
            return -1
        out = []
        for _, row in valid.iterrows():
            out.append({
                "TurbineName": _safe_str(row.get("TurbineName")),
                "TurbineNumber": _to_int(row.get("TurbineNumber")),
                "Country": _safe_str(row.get("CountryName")),
                "TurbineType": _safe_str(row.get("TurbineModel")),
                "TurbinePlatform": _safe_str(row.get("TurbinePlatform")),
                "ParkName": _safe_str(row.get("ScadaParkName")),
                "ControllerType": _safe_str(row.get("ControllerTypeDesc")),
                "ControllerFamily": _safe_str(row.get("ControllerFamily")),
                "TurbineIP": "",
                "IsOnshore": str(_safe_str(row.get("LocationTypeName"))).lower() == "onshore",
                "TurbinePitchType": _safe_str(row.get("TurbinePitchType")),
                "TimezoneOffset": _to_float(row.get("TimezoneOffsetMinutes")),
                "TurbineOwner": _safe_str(row.get("OwnerName")),
                "Altitude": _safe_str(str(row.get("Altitude", ""))),
                "Latitude": _geo_str(row.get("Latitude")),
                "Longitude": _geo_str(row.get("Longitude")),
                "ServiceStatus": _safe_str(row.get("StatusTypeDesc")),
                "Start_up_Date": _to_float(row.get("Start_up_Date")),
                "Cus_Wrty_Start": _to_float(row.get("Cus_Wrty_Start")),
                "Cus_Wrty_end": _to_float(row.get("Cus_Wrty_end")),
                "ServiceRegion": row.get("RegionName"),
                "TimeZone": row.get("Timezone"),
                "TurbineOEM": row.get("TurbineOEM"),
                "NominalPower": row.get("NominalPower"),
                "HubHeight": _hub_height(row.get("HubHeight")),
                "TurbineFunctionalLocation": row.get("TurbineFunctionalLocation"),
                "OEMTechnicalId": row.get("OEMTechnicalId"),
                "TurbineId": _to_int(row.get("TurbineId")),
                "TechnicalId": _to_int(row.get("TechnicalId")),
                "DeviceId": _to_int(row.get("DeviceId")),
            })
        return out


def _date_range(start_date, end_date):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)


def _safe_str(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "UNKNOWN"
    text = str(value).strip()
    return text if text else "UNKNOWN"


def _to_int(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return float("nan")
    try:
        return int(value)
    except Exception:
        return float("nan")


def _to_float(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return float("nan")
    try:
        return float(value)
    except Exception:
        return float("nan")


def _geo_str(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "UNKNOWN"
    if isinstance(value, str):
        return value.strip()
    try:
        return str(float(value))
    except Exception:
        return "UNKNOWN"


def _hub_height(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return float("nan")
    try:
        return str(float(value))
    except Exception:
        return float("nan")
