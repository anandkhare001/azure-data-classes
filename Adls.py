from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class Adls:
    """Python equivalent of the MATLAB Adls helper for Azure Data Lake Storage."""

    TOKEN_URL = (
        ""
    )
    CLIENT_ID = ""
    STORAGE_ACCOUNT = ""

    def __init__(
        self,
        client_secret: Optional[str] = None,
        temp_directory: Union[str, Path] = Path(os.environ.get("TEMP", ".")),
        token_cache_folder: Union[str, Path] = Path(os.environ.get("LOCALAPPDATA", ".")) / "adls-token-cache",
        expire_margin: timedelta = timedelta(seconds=60),
    ):
        self._client_secret = client_secret or os.environ.get("ADLS_CLIENT_SECRET", "")
        if not self._client_secret:
            raise ValueError("client_secret must be supplied or set via ADLS_CLIENT_SECRET.")
        self._token = ""
        self._token_expire_by = datetime.now(tz=timezone.utc) - timedelta(days=365)
        self.expire_margin = expire_margin
        self._token_cache_folder = Path(token_cache_folder)
        self.temp_directory = Path(temp_directory)
        if not self.temp_directory.is_dir():
            raise ValueError(f"temp_directory '{self.temp_directory}' must exist.")

    @property
    def token(self) -> str:
        now_utc = datetime.now(tz=timezone.utc)
        if now_utc - self.expire_margin <= self._token_expire_by:
            return self._token

        now_ms = int(now_utc.timestamp() * 1000)
        cache_dir = self._token_cache_folder
        cache_dir.mkdir(parents=True, exist_ok=True)
        token_files = sorted(cache_dir.glob("*.token"), reverse=True)
        for file in token_files:
            try:
                newest_ts = int(file.stem)
            except ValueError:
                continue
            if newest_ts >= now_ms - 60_000:
                self._token = file.read_text(encoding="utf-8").strip()
                return self._token

        self._acquire_token()
        cache_path = cache_dir / f"{now_ms + 3_600_000}.token"
        cache_path.write_text(self._token, encoding="utf-8")
        return self._token

    def find_blob(self, turbine_id: int, start_date: Optional[Union[str, datetime]] = None, end_date: Optional[Union[str, datetime]] = None) -> list[str]:
        start_dt, end_dt = _resolve_date_range(start_date, end_date)
        headers = self._auth_headers()
        files: list[str] = []

        for year in range(start_dt.year, end_dt.year + 1):
            folder = f"Unified/{year}/{turbine_id}"
            url = f"{self.STORAGE_ACCOUNT}/source?directory={folder}&resource=filesystem&recursive=false"
            try:
                resp = requests.get(url, headers=headers, timeout=20)
                self.get_status_log(resp)
                payload = resp.json()
                for item in payload.get("paths", []):
                    name = item.get("name")
                    if name and "isDirectory" not in item:
                        files.append(name)
            except Exception as exc:  # pragma: no cover
                logger.warning("No files found for turbine %s in year %d: %s", turbine_id, year, exc)

        if not files:
            logger.warning("No blob files found for turbine %s in given date range.", turbine_id)
            return []
        if start_date is not None or end_date is not None:
            return self.find_sampledata_file(files, start_dt, end_dt)
        return files

    def find_blob_for_park(self, park_name: str, start_date=None, end_date=None):
        try:
            from turbine_getInfo import turbine_getInfo  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("turbine_getInfo is required for find_blob_for_park.") from exc

        park_info = turbine_getInfo(park_name)
        turbine_ids = [row.get("TurbineNumber") for row in park_info]
        out = {}
        for tid in turbine_ids:
            try:
                out[str(tid)] = self.find_blob(tid, start_date, end_date)
            except Exception as exc:
                logger.warning("Blob not found for turbine %s: %s", tid, exc)
                out[str(tid)] = []
        max_rows = max((len(v) for v in out.values()), default=0)
        data = {k: v + [None] * (max_rows - len(v)) for k, v in out.items()}
        return pd.DataFrame(data)

    def get_blob(self, sampledata_file_name: str):
        if not isinstance(sampledata_file_name, str):
            raise TypeError("sampledata_file_name must be a str.")
        parts = sampledata_file_name.split("_")
        if len(parts) < 2:
            raise ValueError("Filename format is incorrect. Expected: <turbineId>_<date>-...")
        turbine_id = parts[0]
        year_part = parts[1].split("-")[0]

        local_dir = self.temp_directory / str(year_part) / str(turbine_id)
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = local_dir / sampledata_file_name

        if local_path.exists():
            age = datetime.now() - datetime.fromtimestamp(local_path.stat().st_mtime)
            if age < timedelta(days=7):
                return local_path, True
            local_path.unlink(missing_ok=True)

        url = f"{self.STORAGE_ACCOUNT}/source/Unified/{year_part}/{turbine_id}/{sampledata_file_name}"
        try:
            resp = requests.get(url, headers=self._auth_headers(), timeout=20, stream=True)
            self.get_status_log(resp)
            with local_path.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        fh.write(chunk)
            return local_path, True
        except Exception as exc:
            raise RuntimeError(f"HTTP request failed: {exc}") from exc

    def list_catalogue(self) -> pd.DataFrame:
        base_url = f"{self.STORAGE_ACCOUNT}/source?directory=Unified&resource=filesystem&recursive=true"
        pages: list[pd.DataFrame] = []
        headers = self._auth_headers()
        session = requests.Session()

        resp = session.get(base_url, headers=headers, timeout=60)
        self.get_status_log(resp)
        page = _parse_catalogue_response(resp.json())
        if page is not None:
            pages.append(page)

        while True:
            continuation = resp.headers.get("x-ms-continuation")
            if not continuation:
                break
            if datetime.now(tz=timezone.utc) + self.expire_margin > self._token_expire_by:
                self._acquire_token()
                headers = self._auth_headers()
            cont_url = f"{base_url}&continuation={requests.utils.quote(continuation)}"
            resp = session.get(cont_url, headers=headers, timeout=60)
            self.get_status_log(resp)
            page = _parse_catalogue_response(resp.json())
            if page is not None:
                pages.append(page)

        if not pages:
            logger.warning("No files found in catalogue.")
            return pd.DataFrame()
        return pd.concat(pages, ignore_index=True)

    @staticmethod
    def find_sampledata_file(input_file_list: list[str], start_date: Union[str, datetime], end_date: Union[str, datetime]) -> list[str]:
        if not isinstance(input_file_list, list):
            raise TypeError("input_file_list must be a list of str.")
        start_dt = _parse_date(start_date)
        end_dt = _parse_date(end_date)
        result = []
        for path in input_file_list:
            match = re.search(r"\d{4}-\d{2}-\d{2}", str(path))
            if not match:
                continue
            try:
                dt = datetime.strptime(match.group(), "%Y-%m-%d")
                if start_dt <= dt <= end_dt:
                    result.append(path)
            except ValueError:
                pass
        return result

    @staticmethod
    def get_status_log(response):
        if response.status_code != 200:
            raise RuntimeError(f"ADLS request failed [{response.status_code}]: {response.text[:200]}")

    def _acquire_token(self):
        resp = requests.post(
            self.TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.CLIENT_ID,
                "client_secret": self._client_secret,
                "scope": "https://storage.azure.com/.default",
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self._token_expire_by = datetime.now(tz=timezone.utc) + timedelta(seconds=expires_in)

    def _auth_headers(self):
        return {"Authorization": f"Bearer {self.token}", "x-ms-version": "2020-10-02"}


def _parse_date(value):
    if isinstance(value, datetime):
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    return datetime.strptime(str(value), "%Y-%m-%d")


def _resolve_date_range(start_date, end_date):
    start_dt = _parse_date(start_date) if start_date is not None else datetime(1, 1, 1)
    end_dt = _parse_date(end_date) if end_date is not None else datetime.now() + timedelta(days=1)
    return start_dt, end_dt


def _parse_catalogue_response(data):
    paths = data.get("paths") or []
    if not paths:
        return None
    records = [item for item in paths if not item.get("isDirectory")]
    if not records:
        return None
    df = pd.DataFrame(records)
    for col in ("etag", "expiryTime", "group", "owner", "permissions"):
        if col in df.columns:
            df.drop(columns=[col], inplace=True)
    if "contentLength" in df.columns:
        df["contentLength"] = pd.to_numeric(df["contentLength"], errors="coerce").astype("UInt64")
    if "creationTime" in df.columns:
        df["creationTime"] = pd.to_numeric(df["creationTime"], errors="coerce").astype("UInt64")
    if "lastModified" in df.columns:
        df["lastModified"] = pd.to_datetime(df["lastModified"], errors="coerce")
    if "name" in df.columns:
        df["name"] = df["name"].astype(str)
    return df
