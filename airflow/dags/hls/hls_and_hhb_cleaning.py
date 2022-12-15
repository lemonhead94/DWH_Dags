from typing import Dict

import numpy as np
import pandas as pd


def __replace_empty_strings_and_zero_width_spaces(df: pd.DataFrame) -> None:
    """Replace empty strings and zero width spaces with NaN"""
    df.replace(r"^\s*$", np.nan, regex=True, inplace=True)
    df.replace(r"^\u200b*$", np.nan, regex=True, inplace=True)


def __clean_birth_date(df: pd.DataFrame) -> None:
    """clean birth date to a single format"""
    df["cleaned_birth_date"] = df["birth_date"].str.extract(
        r"((\d{1,2}\.\d{1,2}\.\d{3,4})|(\d{3,4}))"
    )[0]
    # 8.8.1599 -> 1599-08-08
    df["cleaned_birth_date_formatted"] = df[
        df["cleaned_birth_date"].str.fullmatch(r"(\d{1}\.\d{1}\.\d{4})", na=False)
    ]["cleaned_birth_date"].str.replace(
        r"(\d{1})\.(\d{1})\.(\d{4})", r"\3-0\2-0\1", regex=True
    )
    # 8.8.900 -> 0900-08-08
    df["cleaned_birth_date_formatted"].fillna(
        df[df["cleaned_birth_date"].str.fullmatch(r"(\d{1}\.\d{1}\.\d{3})", na=False)][
            "cleaned_birth_date"
        ].str.replace(r"(\d{1})\.(\d{1})\.(\d{3})", r"0\3-0\2-0\1", regex=True),
        inplace=True,
    )
    # 14.5.1900 -> 1900-05-14
    df["cleaned_birth_date_formatted"].fillna(
        df[df["cleaned_birth_date"].str.fullmatch(r"(\d{2}\.\d{1}\.\d{4})", na=False)][
            "cleaned_birth_date"
        ].str.replace(r"(\d{2})\.(\d{1})\.(\d{4})", r"\3-0\2-\1", regex=True),
        inplace=True,
    )
    # 14.5.900 -> 0900-05-14
    df["cleaned_birth_date_formatted"].fillna(
        df[df["cleaned_birth_date"].str.fullmatch(r"(\d{2}\.\d{1}\.\d{3})", na=False)][
            "cleaned_birth_date"
        ].str.replace(r"(\d{2})\.(\d{1})\.(\d{3})", r"0\3-0\2-\1", regex=True),
        inplace=True,
    )
    # 4.05.1900 -> 1900-05-04
    df["cleaned_birth_date_formatted"].fillna(
        df[df["cleaned_birth_date"].str.fullmatch(r"(\d{1}\.\d{2}\.\d{4})", na=False)][
            "cleaned_birth_date"
        ].str.replace(r"(\d{1})\.(\d{2})\.(\d{4})", r"\3-\2-0\1", regex=True),
        inplace=True,
    )
    # 4.05.500 -> 0500-05-04
    df["cleaned_birth_date_formatted"].fillna(
        df[df["cleaned_birth_date"].str.fullmatch(r"(\d{1}\.\d{2}\.\d{3})", na=False)][
            "cleaned_birth_date"
        ].str.replace(r"(\d{1})\.(\d{2})\.(\d{3})", r"0\3-\2-0\1", regex=True),
        inplace=True,
    )
    # 18.08.1500 -> 1500-08-18
    df["cleaned_birth_date_formatted"].fillna(
        df[df["cleaned_birth_date"].str.fullmatch(r"(\d{2}\.\d{2}\.\d{4})", na=False)][
            "cleaned_birth_date"
        ].str.replace(r"(\d{2})\.(\d{2})\.(\d{4})", r"\3-\2-\1", regex=True),
        inplace=True,
    )
    # 18.08.900 -> 0900-08-18
    df["cleaned_birth_date_formatted"].fillna(
        df[df["cleaned_birth_date"].str.fullmatch(r"(\d{2}\.\d{2}\.\d{3})", na=False)][
            "cleaned_birth_date"
        ].str.replace(r"(\d{2})\.(\d{2})\.(\d{3})", r"0\3-\2-\1", regex=True),
        inplace=True,
    )
    # 900 to 0900-01-01
    df["cleaned_birth_date_formatted"].fillna(
        df[df["cleaned_birth_date"].str.fullmatch(r"(\d{3})", na=False)][
            "cleaned_birth_date"
        ].str.replace(r"(\d{3})", r"0\1-01-01", regex=True),
        inplace=True,
    )
    # 1500 to 1500-01-01
    df["cleaned_birth_date_formatted"].fillna(
        df[df["cleaned_birth_date"].str.fullmatch(r"(\d{4})", na=False)][
            "cleaned_birth_date"
        ].str.replace(r"(\d{4})", r"\1-01-01", regex=True),
        inplace=True,
    )
    # Adding Missing Birth Dates from HHB
    df["cleaned_birth_date_formatted"].fillna(df["hhb_birth_date"], inplace=True)
    df.drop("cleaned_birth_date", axis=1, inplace=True)


def __clean_death_date(df: pd.DataFrame) -> None:
    df["cleaned_death_date"] = df["death_date"].str.extract(
        r"((\d{1,2}\.\d{1,2}\.\d{3,4})|(\d{3,4}))"
    )[0]
    # 8.8.1599 -> 1599-08-08
    df["cleaned_death_date_formatted"] = df[
        df["cleaned_death_date"].str.fullmatch(r"(\d{1}\.\d{1}\.\d{4})", na=False)
    ]["cleaned_death_date"].str.replace(
        r"(\d{1})\.(\d{1})\.(\d{4})", r"\3-0\2-0\1", regex=True
    )
    # 8.8.900 -> 0900-08-08
    df["cleaned_death_date_formatted"].fillna(
        df[df["cleaned_death_date"].str.fullmatch(r"(\d{1}\.\d{1}\.\d{3})", na=False)][
            "cleaned_death_date"
        ].str.replace(r"(\d{1})\.(\d{1})\.(\d{3})", r"0\3-0\2-0\1", regex=True),
        inplace=True,
    )
    # 14.5.1900 -> 1900-05-14
    df["cleaned_death_date_formatted"].fillna(
        df[df["cleaned_death_date"].str.fullmatch(r"(\d{2}\.\d{1}\.\d{4})", na=False)][
            "cleaned_death_date"
        ].str.replace(r"(\d{2})\.(\d{1})\.(\d{4})", r"\3-0\2-\1", regex=True),
        inplace=True,
    )
    # 14.5.900 -> 0900-05-14
    df["cleaned_death_date_formatted"].fillna(
        df[df["cleaned_death_date"].str.fullmatch(r"(\d{2}\.\d{1}\.\d{3})", na=False)][
            "cleaned_death_date"
        ].str.replace(r"(\d{2})\.(\d{1})\.(\d{3})", r"0\3-0\2-\1", regex=True),
        inplace=True,
    )
    # 4.05.1900 -> 1900-05-04
    df["cleaned_death_date_formatted"].fillna(
        df[df["cleaned_death_date"].str.fullmatch(r"(\d{1}\.\d{2}\.\d{4})", na=False)][
            "cleaned_death_date"
        ].str.replace(r"(\d{1})\.(\d{2})\.(\d{4})", r"\3-\2-0\1", regex=True),
        inplace=True,
    )
    # 4.05.500 -> 0500-05-04
    df["cleaned_death_date_formatted"].fillna(
        df[df["cleaned_death_date"].str.fullmatch(r"(\d{1}\.\d{2}\.\d{3})", na=False)][
            "cleaned_death_date"
        ].str.replace(r"(\d{1})\.(\d{2})\.(\d{3})", r"0\3-\2-0\1", regex=True),
        inplace=True,
    )
    # 18.08.1500 -> 1500-08-18
    df["cleaned_death_date_formatted"].fillna(
        df[df["cleaned_death_date"].str.fullmatch(r"(\d{2}\.\d{2}\.\d{4})", na=False)][
            "cleaned_death_date"
        ].str.replace(r"(\d{2})\.(\d{2})\.(\d{4})", r"\3-\2-\1", regex=True),
        inplace=True,
    )
    # 18.08.900 -> 0900-08-18
    df["cleaned_death_date_formatted"].fillna(
        df[df["cleaned_death_date"].str.fullmatch(r"(\d{2}\.\d{2}\.\d{3})", na=False)][
            "cleaned_death_date"
        ].str.replace(r"(\d{2})\.(\d{2})\.(\d{3})", r"0\3-\2-\1", regex=True),
        inplace=True,
    )
    # 900 to 0900-01-01
    df["cleaned_death_date_formatted"].fillna(
        df[df["cleaned_death_date"].str.fullmatch(r"(\d{3})", na=False)][
            "cleaned_death_date"
        ].str.replace(r"(\d{3})", r"0\1-01-01", regex=True),
        inplace=True,
    )
    # 1500 to 1500-01-01
    df["cleaned_death_date_formatted"].fillna(
        df[df["cleaned_death_date"].str.fullmatch(r"(\d{4})", na=False)][
            "cleaned_death_date"
        ].str.replace(r"(\d{4})", r"\1-01-01", regex=True),
        inplace=True,
    )

    # fill missing dates with hhd_death_date
    df["cleaned_death_date_formatted"].fillna(df["hhb_death_date"], inplace=True)
    df.drop("cleaned_death_date", axis=1, inplace=True)


def clean_hls_and_hhb(config: Dict[str, str]) -> None:
    df_updates = pd.read_csv(
        config["HHB_UPDATE_CSV_PATH"], dtype={"hhb_ids": str}, sep=";"
    )
    df_new = pd.read_csv(config["HHB_NEW_CSV_PATH"], dtype={"hhb_ids": str}, sep=";")

    __replace_empty_strings_and_zero_width_spaces(df=df_updates)
    __replace_empty_strings_and_zero_width_spaces(df=df_new)

    __clean_birth_date(df=df_updates)
    __clean_birth_date(df=df_new)

    __clean_death_date(df=df_updates)
    __clean_death_date(df=df_new)

    df_updates.to_csv(config["CLEANED_UPDATE_CSV_PATH"], sep=";", index=False)
    df_new.to_csv(config["CLEANED_NEW_CSV_PATH"], sep=";", index=False)
