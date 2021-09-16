import json
import requests
import datetime as dt
from typing import Dict, Union, Tuple
from dateutil.parser import parse
import pandas as pd

EPOCH_DATE = "2020-01-21"


def get_owid_variable(
    variable_id: Union[int, str], to_frame: bool = False
) -> Tuple[Union[pd.DataFrame, dict], dict]:
    data_keys = ["years", "entities", "values"]
    res = requests.get(
        f"https://ourworldindata.org/grapher/data/variables/{variable_id}.json"
    )
    assert res.ok
    result = json.loads(res.content)
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
