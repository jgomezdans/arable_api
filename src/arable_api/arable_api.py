import logging
import requests
import json
import datetime as dt
import os
from pathlib import Path
import pandas as pd
import numpy as np
from pprint import pprint
import matplotlib.pyplot as plt
import sys

# Remove the default handler from the root logger
logging.getLogger().handlers = []

logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

# Set up a handler for the logger
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter for the handler
formatter = logging.Formatter(
    "%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s - %(message)s",
    "%Y-%m-%d %H:%M:%S",
)
console_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(console_handler)

ALL_FIELDS = [
    "aux_raw",
    "calibrated",
    "daily",
    "data_errors",
    "deployment",
    "dsd_calibrated",
    "dsd_raw",
    "health",
    "health_daily",
    "hourly",
    "irrigation_runtime_daily",
    "irrigation_runtime_hourly",
    "local_hourly",
    "location_irrigation_forecast_daily",
    "network",
    "sentek_daily",
    "sentek_hourly",
]


def get_response(
    service: str,
    parameters: dict | None = None,
    BASE_URL: str = "https://api.arable.cloud/api/v2",
    api_key: str | None = None,
) -> list | dict | str:
    """Function used to query the remote Arable API endpoint.
    This assumes APIkey authorisation, and you can stuff all
    parameters in a dictionary in `parameters`

    Args:
        service (str): What API service you want (e.g. the last bit
            of the URL).
        parameters (dict | None, optional): Dictionary of parameters.
            Defaults to None.
        BASE_URL (str, optional): Base URL. Defaults to
            "https://api.arable.cloud/api/v2".
        api_key (str, optional): API Key. Defaults to `None`. Will
            try to get the key from the environment variable
            `ARABLE_API`, or otherwise fail.

    Returns:
        list | dict | str: Returns the JSON decoded return
    """
    if api_key is None:
        try:
            api_key = os.environ["ARABLE_API"]
        except KeyError:
            print(
                "You did not provide an API key, "
                "and one isn't defined in the environment"
            )

    url = f"{BASE_URL}/{service}"
    r = requests.get(
        url,
        headers={"Authorization": f"apikey {api_key}"},
        params=parameters,
    )
    if r.status_code != 200:
        msg = (
            f"Code: {r.status_code}\n Response: {r.content}\n"
            + f"URL: {url}\n"
        )
        logger.error(msg)
        raise requests.HTTPError(
            "Problem accessing API endpoint\n" + msg
        )
    return json.loads(r.content)


def get_datasets() -> dict:
    """Queries API for available measurements"""
    meas = get_response("schemas/calibrated")
    ddatasets = pd.DataFrame(meas)
    datasets = dict(zip(ddatasets.column_name, ddatasets.description))
    return ddatasets


def get_devices() -> list:
    return [dev["name"] for dev in get_response("devices")["items"]]


def get_data(
    schema: str = "data/local_hourly",
    devices: None | list | str = None,
    start_time: None | str | dt.datetime = None,
    end_time: None | str | dt.datetime = None,
) -> pd.DataFrame | None:
    """Get data from the Arable API

    Args:
        schema (str, optional): Select a schema. Defaults to "data/local_hourly".
        devices (None | list | str, optional): Device or devices to gather data from.
             Defaults to `None`. If `None`, it will query all the devices and download data
             from all of them
        start_time (None | str | dt.datetime, optional): Start time. Defaults to None.
            If `None`, it will assume today.
        end_time (None | str | dt.datetime, optional): End time. Defaults to `None`.
            Same comment as `start_time`

    Returns:
        pd.DataFrame | None: A pandas DataFrame with the data for all the requrested
            devices, or `None` if there's no data available.
    """
    if not start_time:
        start_time = dt.datetime.now()
        end_time = dt.datetime.now()
    else:
        start_time = (
            dt.datetime.strptime(start_time, "%Y-%m-%d")
            if isinstance(start_time, str)
            else start_time
        )
        end_time = start_time if end_time is None else end_time

    if devices is None:
        devices = get_devices()
    elif isinstance(devices, str):
        devices = [
            devices,
        ]
    retvals = []
    for device in devices:
        query = {
            "device": device,
            "limit": "5012",
            "start_time": start_time.strftime("%Y-%m-%dT00:00:00Z"),
            "end_time": end_time.strftime("%Y-%m-%dT23:59:59Z"),
        }
        try:
            retval = get_response(f"{schema}", parameters=query)
        except requests.HTTPError:
            continue
        df = pd.DataFrame(retval)
        if len(df) == 0:
            logger.warning(f"{schema} returns 0 records")
            continue

        df["timex"] = pd.to_datetime(df.time)
        retvals.append(df.set_index("timex"))
    return pd.concat(retvals, axis=0) if retvals else None


def gather_data(
    output_folder: str | Path,
    start_time: None | dt.datetime | str = None,
) -> list[Path]:
    """Gathers data and dumps it to a set of CSV files
    in `output_folder`.

    Args:
        output_folder (str | Path): Location where CSV files will be 
            saved.
        start_time (None | dt.datetime | str, optional): Date on which
        to download data. Defaults to None, which means download all
        the data collected yesterday.

    Returns:
        list[Path]: List of filenames saved.
    """
    logger.info("Starting data gathering...")
    if start_time is None:
        start_time = dt.datetime.now() - dt.timedelta(
            days=1
        )  # yesterday
        logger.info(
            "Not given a date, so using yesterday "
            + start_time.strftime("%Y-%m-%d")
        )
    elif isinstance(start_time, str):
        start_time = dt.datetime.strptime(start_time, "%Y-%m-%d")
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)
    files = []
    for schema in ALL_FIELDS:
        df = get_data(schema=f"data/{schema}", start_time=start_time)
        if df is not None:
            loc = (
                output_folder
                / f"{start_time.strftime('%Y-%m-%d')}_{schema}.csv"
            )
            df.to_csv(loc)
            logger.info(f"Saved {schema} -> {loc}")
            files.append(loc)
    return files
