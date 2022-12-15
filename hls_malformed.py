# Description: correction of malformed entries in the HLS base scraping from 15.10.2022
import pandas as pd


def __malformed_hls_entries(df: pd.DataFrame) -> None:
    """correct malformed entry on HLS 11421"""
    df.loc[df["HLS_ID"] == 19242, "first_name"] = "Abraham"
    df.loc[df["HLS_ID"] == 19242, "last_name"] = "Keller"
    df.loc[df["HLS_ID"] == 19242, "published"] = "09.08.2007"


def __malformed_hls_dates(df: pd.DataFrame) -> None:
    """correct malformed entries on HLS base"""
    df.loc[df["HLS_ID"] == 15548, "cleaned_birth_date"] = "1.1.1375"
    df.loc[df["HLS_ID"] == 58876, "cleaned_birth_date"] = "15.6.1833"
    df.loc[df["HLS_ID"] == 12835, "cleaned_birth_date"] = "1.1.850"
    df.loc[df["HLS_ID"] == 48410, "cleaned_death_date"] = "1.1.1475"
    df.loc[df["HLS_ID"] == 12921, "cleaned_death_date"] = "1.1.400"
