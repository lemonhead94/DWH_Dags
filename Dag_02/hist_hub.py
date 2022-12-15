import json
import logging
import sys
import time
import traceback
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

with open("./config.json") as f:
    CONFIG = json.load(f)


def __setup_logging_to_file(filename: str) -> None:
    """Setup logging to log to a file."""
    logging.basicConfig(
        filename=filename,
        filemode="w",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def __extract_function_name() -> str:
    """Extracts failing function name from Traceback object."""
    tb = sys.exc_info()[-1]
    stk = traceback.extract_tb(tb, 1)
    fname = stk[0][3]
    return str(fname)


def __log_exception(e: Exception) -> None:
    """Logs exception with traceback."""
    logging.error(
        "Function {function_name} raised {exception_class} ({exception_docstring}): {exception_message}".format(
            function_name=__extract_function_name(),  # this is optional
            exception_class=e.__class__,
            exception_docstring=e.__doc__,
            exception_message=str(e),
        )
    )


def __get_hhb_ids(json: List[Dict[str, Any]]) -> Optional[str]:
    """Extracts hhb_ids from json data."""
    hhb_ids: List[int] = []
    for data in json:
        try:
            hhb_ids.append(data["hhb_id"])
        except (KeyError, TypeError):
            pass
    return ",".join([str(i) for i in hhb_ids]) if hhb_ids else None


def __get_forename(json: List[Dict[str, Any]]) -> Optional[str]:
    """Extracts forename from json data."""
    for data in json:
        try:
            return str(data["forename"])
        except (KeyError, TypeError):
            pass
    return None


def __get_surname(json: List[Dict[str, Any]]) -> Optional[str]:
    """Extracts surname from json data."""
    for data in json:
        try:
            return str(data["surname"])
        except (KeyError, TypeError):
            pass
    return None


def __get_sex(json: List[Dict[str, Any]]) -> Optional[str]:
    """Extracts sex from json data."""
    for data in json:
        try:
            if len(data["sex"]) > 0:
                return str(data["sex"][0]["term"]["labels"]["eng"])
        except (KeyError, TypeError):
            pass
    return None


def __get_birth_date(json: List[Dict[str, Any]]) -> Optional[str]:
    """Extracts birth data from json data."""
    for data in json:
        try:
            if len(data["existences"]) > 0:
                return str(data["existences"][0]["start"]["date"])
        except (KeyError, TypeError):
            pass
    return None


def __get_death_date(json: List[Dict[str, Any]]) -> Optional[str]:
    """Extracts death data from json data."""
    for data in json:
        try:
            if len(data["existences"]) > 0:
                return str(data["existences"][0]["end"]["date"])
        except (KeyError, TypeError):
            pass
    return None


def __get_relation_id(json: List[Dict[str, Any]]) -> Optional[int]:
    """Extracts relation id from json data."""
    for data in json:
        try:
            if len(data["relations"]) > 0:
                return int(data["relations"][0]["person"]["id"])
        except (KeyError, TypeError):
            pass
    return None


def __get_relation_name(json: List[Dict[str, Any]]) -> Optional[str]:
    """Extracts relation name from json data."""
    for data in json:
        try:
            if len(data["relations"]) > 0:
                return str(data["relations"][0]["person"]["name"])
        except (KeyError, TypeError):
            pass
    return None


def __get_relation_type(json: List[Dict[str, Any]]) -> Optional[str]:
    """Extracts relation type from json data."""
    for data in json:
        try:
            if len(data["relations"]) > 0:
                return str(data["relations"][0]["relation"]["labels"]["eng"])
        except KeyError:
            pass
    return None


def __retrieve_hhb_data(df: pd.DataFrame, type: str) -> pd.DataFrame:
    """Retrieves data from histhub.ch."""
    url = "https://data.histhub.ch/api/search/"
    headers = {"Content-Type": "application/json"}
    for index, row in df.iterrows():
        print(f"Processing {type} {index + 1} out of {len(df)}")
        data = {"version": 1, "external_ids.external_id": row.HLS_ID}
        response = requests.post(url, json=data, headers=headers)
        json = response.json()

        # backup in case there is a connection error
        if json is None:
            time.sleep(60)
            response = requests.post(url, json=data, headers=headers)

        try:
            df.loc[index, "hhb_ids"] = __get_hhb_ids(json)
            df.loc[index, "hhb_forename"] = __get_forename(json)
            df.loc[index, "hhb_surname"] = __get_surname(json)
            df.loc[index, "hhb_sex"] = __get_sex(json)
            df.loc[index, "hhb_birth_date"] = __get_birth_date(json)
            df.loc[index, "hhb_death_date"] = __get_death_date(json)
            df.loc[index, "hhb_relation_id"] = __get_relation_id(json)
            df.loc[index, "hhb_relation_name"] = __get_relation_name(json)
            df.loc[index, "hhb_relation_type"] = __get_relation_type(json)
        except Exception as e:
            __log_exception(e)
            time.sleep(60)
            pass

        time.sleep(1)


def main() -> None:
    __setup_logging_to_file("hist-hub.log")
    df_updates = pd.read_csv(CONFIG["HLS_UPDATE_CSV_PATH"], sep=";")
    df_new = pd.read_csv(CONFIG["HLS_NEW_CSV_PATH"], sep=";")

    __retrieve_hhb_data(df_updates, "Update Entries")
    __retrieve_hhb_data(df_new, "New Entries")

    df_updates.to_csv(CONFIG["HHB_UPDATE_CSV_PATH"], sep=";", index=False)
    df_new.to_csv(CONFIG["HHB_NEW_CSV_PATH"], sep=";", index=False)


if __name__ == "__main__":
    main()
