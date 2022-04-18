import datetime as dt
import json
import os
from dateutil.parser import parse
from typing import Dict, Union, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

from scripts import INPUT_DIR

EPOCH_DATE = "2020-01-21"
# Get variables from local grapher if "HOST='http://localhost:3030'" is defined in local .env file.
# Otherwise load variables from from live grapher.
load_dotenv()
HOST = os.getenv("HOST", "https://ourworldindata.org")
ADDITIONAL_DATA_DIR = os.path.join(INPUT_DIR, "additional_data")


def _load_variable_from_additional_data_dir(variable_id):
    # Data for some variables cannot be fetched using the grapher/data/variables method; load from a local file instead.

    # List variable ids for which we have additional data.
    additional_data_variable_ids = [
        os.path.basename(file).split(".")[0]
        for file in os.scandir(ADDITIONAL_DATA_DIR)
        if file.name.endswith(".json")
    ]
    if str(variable_id) in additional_data_variable_ids:
        # If found, load data for this variable.
        additional_data_file = os.path.join(ADDITIONAL_DATA_DIR, f"{variable_id}.json")
        with open(additional_data_file, "r") as _file:
            data = json.loads(_file.read())
    else:
        print(f"ERROR: Data could not be loaded for variable {variable_id}.")
        data = None

    return data


def get_owid_variable(
    variable_id: Union[int, str], to_frame: bool = False
) -> Tuple[Union[pd.DataFrame, dict], dict]:
    data_keys = ["years", "entities", "values"]
    res = requests.get(f"{HOST}/grapher/data/variables/{variable_id}.json")
    if res.ok:
        result = json.loads(res.content)
    else:
        result = _load_variable_from_additional_data_dir(variable_id)
    meta = {
        k: v
        for k, v in result["variables"][str(variable_id)].items()
        if k not in data_keys
    }
    if to_frame:
        result = owid_variables_to_frame(result)
    return result, meta


def owid_variables_to_frame(owid_json: Dict[str, dict]) -> pd.DataFrame:
    entity_map = {int(k): v["name"] for k, v in owid_json["entityKey"].items()}
    frames = []
    for variable in owid_json["variables"].values():
        df = pd.DataFrame(
            {
                "year": variable["years"],
                "entity": [entity_map[e] for e in variable["entities"]],
                "variable": variable["name"],
                "value": variable["values"],
            }
        )
        if variable.get("display", {}).get("yearIsDay"):
            zero_day = parse(variable["display"].get("zeroDay", EPOCH_DATE)).date()
            df["date"] = df.pop("year").apply(lambda y: zero_day + dt.timedelta(days=y))
            df = df[["date", "entity", "variable", "value"]]
        frames.append(df)
    return pd.concat(frames)
