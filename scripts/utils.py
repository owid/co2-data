import datetime as dt
from typing import Any, Tuple

import pandas as pd
import requests
from dateutil.parser import parse

EPOCH_DATE = "2020-01-21"
API_URL = "http://127.0.0.1:8000"


def fetch_data_and_metadata_from_api(
    path: str, variables: list[str]
) -> Tuple[pd.DataFrame, dict[str, Any]]:
    r = requests.get(f"{API_URL}/v1/dataset/metadata/{path}")
    assert r.ok
    metadata = r.json()

    df = pd.read_feather(f"{API_URL}/v1/dataset/data/{path}.feather")
    df = df.rename(
        columns={
            "year": "Year",
            "entity_name": "Country",
        }
    ).drop(columns=["entity_id", "entity_code"])

    # use long title instead of short names
    df = df.rename(columns={v["short_name"]: v["title"] for v in metadata["variables"]})

    df = df[["Country", "Year"] + variables].dropna(subset=variables, how="all")

    # convert year is day variables to date
    year_is_day = [
        meta.get("display", {}).get("yearIsDay") for meta in metadata["variables"]
    ]

    if all(year_is_day):
        zero_day = parse(
            metadata["variables"][0]["display"].get("zeroDay", EPOCH_DATE)
        ).date()
        df["Date"] = df.pop("Year").apply(lambda y: zero_day + dt.timedelta(days=y))
    elif any(year_is_day):
        raise ValueError("Some variables have yearIsDay set, but not all.")

    return df, metadata
