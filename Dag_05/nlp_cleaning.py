import json

import numpy as np
import pandas as pd

with open("./config.json") as f:
    CONFIG = json.load(f)


def __remove_duplicates(df: pd.DataFrame, column_name: str) -> None:
    """Remove duplicates in column"""
    df[column_name] = (
        df[column_name]
        .str.split(",")
        .apply(
            lambda x: np.nan
            if type(x) is float
            else [item for item in x if len(item) > 2]
        )
        .apply(lambda x: np.nan if type(x) is float else ",".join(x))
    )


def __only_keep_full_names(df: pd.DataFrame) -> None:
    """Remove all names that are not full names"""
    df["flair_person"] = (
        df["flair_person"]
        .str.split(",")
        .apply(
            lambda x: np.nan
            if type(x) is float
            else [item for item in x if len(item.split(" ")) > 1]
        )
        .apply(lambda x: np.nan if type(x) is float else ",".join(x))
    )


def main() -> None:
    df_updates = pd.read_csv(
        CONFIG["FLAIR_UPDATE_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    df_new = pd.read_csv(CONFIG["FLAIR_NEW_CSV_PATH"], dtype={"hhb_ids": str}, sep=";")

    __remove_duplicates(df_updates, "flair_locations")
    __remove_duplicates(df_new, "flair_locations")

    __remove_duplicates(df_updates, "flair_person")
    __remove_duplicates(df_new, "flair_person")

    __remove_duplicates(df_updates, "flair_organizations")
    __remove_duplicates(df_new, "flair_organizations")

    __remove_duplicates(df_updates, "flair_misc")
    __remove_duplicates(df_new, "flair_misc")

    __only_keep_full_names(df_updates)
    __only_keep_full_names(df_new)

    df_updates.to_csv(CONFIG["FLAIR_CLEANED_UPDATE_CSV_PATH"], sep=";", index=False)
    df_new.to_csv(CONFIG["FLAIR_CLEANED_NEW_CSV_PATH"], sep=";", index=False)


if __name__ == "__main__":
    main()
