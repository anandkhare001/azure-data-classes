from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Iterable, Sequence

import numpy as np
import requests

logger = logging.getLogger(__name__)


class SampleRest:
    """Python equivalent of the MATLAB SampleRest client for SAMPLE4."""

    PROJECT_LIST_URL = "https://api.example.invalid/endpoint"
    ITEM_LIST_URL = "https://api.example.invalid/endpoint"
    CALIBRATION_URL = "https://api.example.invalid/endpoint"
    TIMESERIES_URL = "https://api.example.invalid/endpoint"
    STATISTICS_URL = "https://api.example.invalid/endpoint"
    STEL_URL = "https://api.example.invalid/endpoint"
    DERIVED_ITEM_LIST_URL = "https://api.example.invalid/endpoint"
    DERIVED_SAMPLE_RATE_URL = "https://api.example.invalid/endpoint"
    DERIVED_TIMESERIES_URL = "https://api.example.invalid/endpoint"
    DERIVED_STATISTICS_URL = "https://api.example.invalid/endpoint"

    PROD_HOST = "api.example.invalid"
    DEV_HOST = "dev-api.example.invalid"
    SAMPLE_RATE_IN_HZ = [1, 2, 3, 5, 8, 10, 25, 100, 250, 500, 1000, 2500, 5000, 10000]
    SAMPLE_LENGTH_IN_SEC = 600
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, env: str | None = None):
        env = (env or os.environ.get("SampleServerToUse", "prod")).strip().lower()
        env = {"prod": "prod", "production": "prod", "dev": "dev", "development": "dev", "developement": "dev"}.get(env, env)
        if env not in {"prod", "dev"}:
            raise ValueError("Environment must be 'prod' or 'dev'.")
        self.environment = env

    def get_project_info(self, project_name: str | None = None, project_id: Any | None = None):
        if project_name is not None and not isinstance(project_name, str):
            raise TypeError("project_name must be str or None.")
        if project_id is not None and not isinstance(project_id, (int, float, bool)):
            raise TypeError("project_id must be numeric or None.")

        if project_name is None and project_id is None:
            payload_name = ""
            payload_id = ""
            exact = False
        elif project_name is not None and project_id is None:
            payload_name = project_name
            payload_id = ""
            exact = True
        elif project_id is not None and isinstance(project_id, bool):
            payload_name = project_name or ""
            payload_id = ""
            exact = False
        else:
            payload_name = project_name or ""
            payload_id = project_id
            exact = True

        info = self._get_data_from_rest(self.PROJECT_LIST_URL, projectName=payload_name, projectId=payload_id)
        info = _as_list(info)
        if project_name:
            needle = project_name.lower()
            if exact:
                return [p for p in info if str(p.get("projectName", "")).lower() == needle]
            return [p for p in info if needle in str(p.get("projectName", "")).lower()]
        return info

    def get_item_list(self, project_name: str, project_id: int, item_filter: str | list[str] | None = None, do_fetch_derived_items: bool = False):
        if not isinstance(project_name, str):
            raise TypeError("project_name must be a str.")
        if not isinstance(project_id, (int, float)):
            raise TypeError("project_id must be numeric.")
        if item_filter is not None and not isinstance(item_filter, (str, list)):
            raise TypeError("item_filter must be str or list[str].")
        if not isinstance(do_fetch_derived_items, bool):
            raise TypeError("do_fetch_derived_items must be bool.")

        url = self.DERIVED_ITEM_LIST_URL if do_fetch_derived_items else self.ITEM_LIST_URL
        items = _as_list(self._get_data_from_rest(url, projectId=project_id))
        if not items or "itemName" not in items[0]:
            return [{"itemId": None, "itemName": None}]
        if item_filter:
            if isinstance(item_filter, list):
                items = [c for c in items if c.get("itemName") in item_filter]
            else:
                needle = item_filter.lower()
                items = [c for c in items if needle in str(c.get("itemName", "")).lower()]
        return items

    def get_item_info(self, item_ids: Sequence[int] | list[int]):
        if not isinstance(item_ids, (list, tuple)) or not item_ids:
            raise ValueError("item_ids must be a non-empty list/tuple.")
        info = _as_list(self._get_data_from_rest(self.ITEM_LIST_URL, itemIds=list(item_ids)))
        return info if info and "itemName" in info[0] else [{"itemId": None, "itemName": None}]

    def get_derived_item_info(self, derived_item_ids: Sequence[int] | list[int]):
        if not isinstance(derived_item_ids, (list, tuple)) or not derived_item_ids:
            raise ValueError("derived_item_ids must be a non-empty list/tuple.")
        info = _as_list(self._get_data_from_rest(self.DERIVED_ITEM_LIST_URL, derivedItemIds=list(derived_item_ids)))
        return info if info and "itemName" in info[0] else [{"derivedItemId": None, "itemName": None}]

    def get_item_calibrations(self, item_ids):
        if not item_ids:
            raise ValueError("item_ids must be non-empty.")
        res = _as_list(self._get_data_from_rest(self.CALIBRATION_URL, itemIds=list(item_ids)))
        if not res:
            return []
        required = {"dataItemId", "calibrationDetailsResponseDto"}
        if not required.issubset(res[0].keys()):
            return []
        for item in res:
            details = item.get("calibrationDetailsResponseDto") or []
            for d in details:
                if _is_nan_or_none(d.get("lineScale")):
                    d["lineScale"] = 1
                if _is_nan_or_none(d.get("lineOffset")):
                    d["lineOffset"] = 0
        return res

    def get_raw_timeseries(self, item_ids, time_start: str, time_end: str):
        if not item_ids:
            raise ValueError("item_ids must be non-empty.")
        if not isinstance(time_start, str):
            raise TypeError("time_start must be a str in 'yyyy-mm-dd HH:MM:SS'.")
        if not isinstance(time_end, str):
            raise TypeError("time_end must be a str in 'yyyy-mm-dd HH:MM:SS'.")

        limit = 1000
        offset = 0
        rows: list[dict] = []
        while True:
            try:
                batch = _as_list(self._get_data_from_rest(self.TIMESERIES_URL, itemIds=list(item_ids), fromTime=time_start, toTime=time_end, limit=limit, offset=offset))
                rows.extend(batch)
                if len(batch) < limit:
                    break
                offset += limit
            except Exception as exc:
                logger.warning("SampleRest: %s", exc)
                break

        rows = [r for r in rows if r.get("timeSeriesData")]
        return rows if rows and {"itemId", "timeStampEnd", "timeSeriesData"}.issubset(rows[0].keys()) else [{"itemId": None, "timeStampEnd": None, "sampleRateHz": None, "timeSeriesData": None}]

    def get_raw_timeseries_derived(self, derived_item_info, time_start: datetime, time_end: datetime):
        if not isinstance(derived_item_info, list) or not derived_item_info:
            raise ValueError("derived_item_info must be a non-empty list of dicts.")
        if not all({"derivedItemId", "itemName"}.issubset(c.keys()) for c in derived_item_info):
            raise ValueError('Each record in derived_item_info must contain "derivedItemId" and "itemName".')
        if not isinstance(time_start, datetime) or not isinstance(time_end, datetime):
            raise TypeError("time_start and time_end must be datetime objects.")

        item_ids = [c["derivedItemId"] for c in derived_item_info]
        sample_rate_info = self.get_derived_item_sample_rate(item_ids, time_start, time_end)
        records: list[dict] = []

        for i, ch_id in enumerate(item_ids):
            rate = 25.0
            for s in sample_rate_info:
                if s.get("derivedItemId") == ch_id:
                    rate = float(s.get("sampleRateHz", 25.0))
                    break
            num_ten_min = min(10000.0 / rate, 1000.0)
            task_duration = timedelta(days=min(1.0, num_ten_min / 144.0))
            times = _generate_time_vector(time_start, time_end, task_duration)
            for stamp_index in range(len(times) - 1):
                try:
                    batch = _as_list(self._get_data_from_rest(self.DERIVED_TIMESERIES_URL, derivedItemIds=[ch_id], fromTime=times[stamp_index].strftime(self.TIME_FORMAT), toTime=times[stamp_index + 1].strftime(self.TIME_FORMAT)))
                    records.extend(batch)
                except Exception as exc:
                    logger.warning("SampleRest: %s for derived item - %s.", exc, derived_item_info[i]["itemName"])
        return records, sample_rate_info

    def get_raw_statistics(self, item_ids, time_start: str, time_end: str):
        if not item_ids:
            raise ValueError("item_ids must be non-empty.")
        limit = 1000
        offset = 0
        rows: list[dict] = []
        while True:
            try:
                batch = _as_list(self._get_data_from_rest(self.STATISTICS_URL, itemIds=list(item_ids), fromTime=time_start, toTime=time_end, limit=limit, offset=offset))
                rows.extend(batch)
                if len(batch) < limit:
                    break
                offset += limit
            except Exception as exc:
                logger.warning("SampleRest: %s", exc)
                break
        return rows if rows and {"itemId", "timeStampEnd", "meanValue", "minValue", "maxValue", "stdDev"}.issubset(rows[0].keys()) else [{"itemId": None, "timeStampEnd": None, "meanValue": None, "minValue": None, "maxValue": None, "stdDev": None, "sampleCount": None, "sampleRate": None, "gpsQuality": None}]

    def get_raw_statistics_derived(self, derived_item_ids, time_start: datetime, time_end: datetime):
        if not derived_item_ids:
            raise ValueError("derived_item_ids must be non-empty.")
        chunks = _chunks(derived_item_ids, 10)
        task_duration = timedelta(days=180)
        times = _generate_time_vector(time_start, time_end, task_duration)
        rows: list[dict] = []
        for chunk in chunks:
            for idx in range(len(times) - 1):
                try:
                    batch = _as_list(self._get_data_from_rest(self.DERIVED_STATISTICS_URL, derivedItemIds=list(chunk), fromTime=times[idx].strftime(self.TIME_FORMAT), toTime=times[idx + 1].strftime(self.TIME_FORMAT)))
                    rows.extend(batch)
                except Exception as exc:
                    logger.warning("SampleRest: %s", exc)
        if not rows:
            return [{"derivedItemId": None, "derivedData": None}]
        result = []
        for ch_id in derived_item_ids:
            combined = []
            for row in rows:
                if row.get("derivedItemId") == ch_id:
                    md = row.get("derivedData") or []
                    if isinstance(md, list):
                        combined.extend(md)
            result.append({"derivedItemId": ch_id, "derivedData": combined})
        return result

    def get_raw_stel(self, item_ids, derived_item_ids, rain_flow_setup_id, time_start: str, time_end: str):
        if not item_ids:
            raise ValueError("item_ids must be non-empty.")
        if not derived_item_ids:
            raise ValueError("derived_item_ids must be non-empty.")
        if not isinstance(rain_flow_setup_id, (int, float)):
            raise TypeError("rain_flow_setup_id must be numeric.")
        limit = 1000
        offset = 0
        rows: list[dict] = []
        while True:
            try:
                batch = _as_list(self._get_data_from_rest(self.STEL_URL, itemIds=list(item_ids), derivedItemIds=list(derived_item_ids), pkRainFlowSetupId=rain_flow_setup_id, startTime=time_start, stopTime=time_end, limit=limit, offset=offset))
                rows.extend(batch)
                if len(batch) < limit:
                    break
                offset += limit
            except Exception as exc:
                logger.warning("SampleRest: %s", exc)
                break
        return rows if rows and {"itemId", "data", "isDerivedItem", "pkRainFlowResultId"}.issubset(rows[0].keys()) else [{"itemId": None, "data": None, "isDerivedItem": None, "pkRainFlowResultId": None}]

    def get_derived_item_sample_rate(self, derived_item_ids, time_start: datetime, time_end: datetime):
        if not derived_item_ids:
            raise ValueError("derived_item_ids must be non-empty.")
        payload = self._get_data_from_rest(self.DERIVED_SAMPLE_RATE_URL, derivedItemIds=list(derived_item_ids), fromTime=time_start.strftime(self.TIME_FORMAT), toTime=time_end.strftime(self.TIME_FORMAT))
        return _as_list(payload)

    def make_timeseries_item_struct(self, time_series_data, item_name: str, item_id: int, sample_freq, interpolation_limit):
        if not isinstance(time_series_data, list) or not time_series_data:
            raise ValueError("time_series_data must be a non-empty list of dicts.")
        if not isinstance(item_name, str):
            raise TypeError("item_name must be str.")
        if not isinstance(item_id, (int, float)):
            raise TypeError("item_id must be numeric.")
        item_rows = [r for r in time_series_data if r.get("itemId") == item_id]
        if not item_rows:
            return {"time": [], "value": np.array([])}

        if sample_freq is None:
            freqs = []
            for row in item_rows:
                actual_count = len(row.get("timeSeriesData") or [])
                idx = next((i for i, hz in enumerate(self.SAMPLE_RATE_IN_HZ) if actual_count <= hz * self.SAMPLE_LENGTH_IN_SEC), len(self.SAMPLE_RATE_IN_HZ) - 1)
                freqs.append(float(self.SAMPLE_RATE_IN_HZ[idx]))
        elif isinstance(sample_freq, (int, float)):
            freqs = [float(sample_freq)] * len(item_rows)
        else:
            freqs = [float(f) for f in sample_freq]

        all_times = []
        all_values = []
        for row, freq in zip(item_rows, freqs):
            ts_end = datetime.strptime(str(row["timeStampEnd"]), self.TIME_FORMAT)
            time_vec = []
            step = timedelta(seconds=1.0 / float(freq))
            start = ts_end - timedelta(minutes=10) + step
            total = max(1, int(round(float(freq) * 600)))
            for i in range(total):
                time_vec.append(start + i * step)
            raw = list(row.get("timeSeriesData") or [])
            if interpolation_limit > 0:
                val = self.interpolate_time_series(item_name, time_vec, raw, interpolation_limit)
            else:
                val = np.array(raw, dtype=float)[: len(time_vec)]
                time_vec = time_vec[: len(val)]
            all_times.extend(time_vec)
            all_values.append(np.asarray(val, dtype=float))

        return {"time": all_times, "value": np.concatenate(all_values) if all_values else np.array([])}

    def interpolate_time_series(self, item_name: str, time, value, sample_difference_allowed):
        sample_count_nominal = len(time)
        sample_count_actual = len(value)
        sample_difference = abs(sample_count_nominal - sample_count_actual)
        if sample_difference == 0:
            return np.asarray(value, dtype=float)
        if sample_difference / sample_count_nominal > sample_difference_allowed:
            logger.warning("SampleRest: Sample count nominal = %d, actual = %d For ItemName: %s and TimeEnd: %s", sample_count_nominal, sample_count_actual, item_name, time[-1].strftime(self.TIME_FORMAT) if time else "unknown")
            return np.full(sample_count_nominal, np.nan)
        arr = np.asarray(value, dtype=float)
        x_old = np.arange(1, sample_count_actual + 1, dtype=float) * (sample_count_nominal / sample_count_actual)
        x_new = np.arange(1, sample_count_nominal + 1, dtype=float)
        result = np.interp(x_new, x_old, arr)
        return result

    def make_time_format_for_rest(self, ts: datetime):
        if not isinstance(ts, datetime):
            raise TypeError("ts must be a datetime object.")
        return ts.strftime(self.TIME_FORMAT)

    def merge_std_and_derived_item_info(self, std_item_info, derived_item_info):
        if not derived_item_info:
            return [{"itemName": c["itemName"], "itemId": c["itemId"]} for c in std_item_info]
        if not std_item_info:
            return [{"itemName": c["itemName"], "itemId": c["derivedItemId"]} for c in derived_item_info]
        out = [{"itemName": c["itemName"], "itemId": c["itemId"]} for c in std_item_info]
        out += [{"itemName": c["itemName"], "itemId": c["derivedItemId"]} for c in derived_item_info]
        return out

    @staticmethod
    def make_chunks(input_array, chunk_size):
        return [list(input_array[i:i + chunk_size]) for i in range(0, len(input_array), chunk_size)]

    def _resolve_environment_url(self, url: str) -> str:
        if self.environment == "dev" and self.PROD_HOST in url:
            return url.replace(self.PROD_HOST, self.DEV_HOST)
        if self.environment == "prod" and self.DEV_HOST in url:
            return url.replace(self.DEV_HOST, self.PROD_HOST)
        return url

    def _get_data_from_rest(self, request_url: str, **kwargs: Any):
        request_url = self._resolve_environment_url(request_url)
        payload = {k: v for k, v in kwargs.items() if v not in ("", None) and v != []}
        payload["userName"] = os.environ.get("SAMPLE_USERNAME", "example_user")
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        while True:
            try:
                response = requests.post(request_url, json=payload, headers=headers, timeout=20, verify=False)
                if response.status_code == 404:
                    logger.warning("SampleRest - Requested resource not found - retrying in %d sec", 30)
                    time.sleep(30)
                    continue
                if response.status_code == 502:
                    logger.warning("SampleRest - Server gateway error - retrying in %d sec", 30)
                    time.sleep(30)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                logger.warning("SampleRest - Request timed out - retrying in %d sec", 30)
                time.sleep(30)
            except requests.exceptions.ConnectionError:
                logger.warning("SampleRest - Unable to connect to server - retrying in %d sec", 30)
                time.sleep(30)


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _is_nan_or_none(value):
    if value is None:
        return True
    try:
        return bool(np.isnan(float(value)))
    except (TypeError, ValueError):
        return False


def _generate_time_vector(start: datetime, end: datetime, step: timedelta):
    out = []
    cur = start
    while cur < end:
        out.append(cur)
        cur += step
    if not out or out[-1] != end:
        out.append(end)
    return out


def _chunks(items, chunk_size):
    return [list(items[i:i + chunk_size]) for i in range(0, len(items), chunk_size)]
