from datetime import datetime, timedelta
from typing import Literal

import dask.dataframe as dd

from bdproject.paths import get_path_t1

_READ_FILTERS = {
    "yellow": [
        ("tpep_pickup_datetime", ">=", datetime(2012, 1, 1)),
        ("tpep_pickup_datetime", "<", datetime(2025, 2, 1)),
    ],
    "green": [
        ("lpep_pickup_datetime", ">=", datetime(2014, 1, 1)),
        ("lpep_pickup_datetime", "<", datetime(2025, 2, 1)),
    ],
    "fhv": [
        ("pickup_datetime", ">=", datetime(2015, 1, 1)),
        ("pickup_datetime", "<", datetime(2025, 2, 1)),
    ],
    "fhvhv": [
        ("pickup_datetime", ">=", datetime(2019, 2, 1)),
        ("pickup_datetime", "<", datetime(2025, 2, 1)),
    ],
}


def get_ddf_t1(dataset: Literal["yellow", "green", "fhv", "fhvhv"]) -> dd.DataFrame:
    return dd.read_parquet(
        get_path_t1(dataset),
        filters=_READ_FILTERS[dataset],
    )


_UNIVERSAL_COLS_MAPPER = {
    "yellow": {
        "tpep_pickup_datetime": "pu_dt",
        "tpep_dropoff_datetime": "do_dt",
        "pulocationid": "pu_locid",
        "dolocationid": "do_locid",
        "passenger_count": "passengers",
        "trip_distance": "distance",
        "fare_amount": "fee_fare",
        "tip_amount": "fee_tip",
        "total_amount": "fee_total",
        "payment_type": "payment_type",
        # Original
        "airport_fee": None,
        "ratecodeid": None,
        "vendorid": None,
        "congestion_surcharge": None,
        "extra": None,
        "improvement_surcharge": None,
        "mta_tax": None,
        "store_and_fwd_flag": None,
        "tolls_amount": None,
    },
    "green": {
        "lpep_pickup_datetime": "pu_dt",
        "lpep_dropoff_datetime": "do_dt",
        "pulocationid": "pu_locid",
        "dolocationid": "do_locid",
        "passenger_count": "passengers",
        "trip_distance": "distance",
        "fare_amount": "fee_fare",
        "tip_amount": "fee_tip",
        "total_amount": "fee_total",
        "payment_type": "payment_type",
        # Original
        "ratecodeid": None,
        "vendorid": None,
        "congestion_surcharge": None,
        "ehail_fee": None,
        "extra": None,
        "improvement_surcharge": None,
        "mta_tax": None,
        "store_and_fwd_flag": None,
        "tolls_amount": None,
        "trip_type": None,
    },
    "fhv": {
        "pickup_datetime": "pu_dt",
        "dropoff_datetime": "do_dt",
        "pulocationid": "pu_locid",
        "dolocationid": "do_locid",
        "dispatching_base_num": "dbasenum",
        # Original
        "affiliated_base_number": None,
        "sr_flag": None,
    },
    "fhvhv": {
        "pickup_datetime": "pu_dt",
        "dropoff_datetime": "do_dt",
        "pulocationid": "pu_locid",
        "dolocationid": "do_locid",
        "trip_miles": "distance",
        "base_passenger_fare": "fee_fare",
        "tips": "fee_tip",
        "dispatching_base_num": "dbasenum",
        "hvfhs_license_num": "licensenum",
        # Original
        "access_a_ride_flag": None,
        "airport_fee": None,
        "bcf": None,
        "congestion_surcharge": None,
        "driver_pay": None,
        "on_scene_datetime": None,
        "originating_base_num": None,
        "request_datetime": None,
        "sales_tax": None,
        "shared_match_flag": None,
        "shared_request_flag": None,
        "tolls": None,
        "trip_time": None,
        "wav_match_flag": None,
        "wav_request_flag": None,
    },
}


def _mask_negative(ddf: dd.DataFrame, col: str, cond: Literal["<0", "<=0"]):
    if col in ddf:
        ddf[col] = ddf[col].mask((ddf[col] < 0) if cond == "<0" else (ddf[col] <= 0))


def get_ddf_universal(ddf_original: dd.DataFrame) -> dd.DataFrame:
    """Takes a Dask dataframe from one of the four original datasets, and renames the columns to something more consistent."""
    t = (
        "yellow"
        if "tpep_pickup_datetime" in ddf_original
        else "green"
        if "lpep_pickup_datetime" in ddf_original
        else "fhvhv"
        if "hvfhs_license_num" in ddf_original
        else "fhv"
    )
    cols_mapper = _UNIVERSAL_COLS_MAPPER[t]
    ddf = ddf_original[list(cols_mapper)]  # Filter to only relevant columns
    ddf = ddf.rename(  # Rename dataset-specific names to predictable names
        columns={k: v if v is not None else f"x_{k}" for k, v in cols_mapper.items()}
    )
    # Set garbage values to NA
    ddf["pu_locid"] = ddf["pu_locid"].mask(
        (ddf["pu_locid"] < 1) | (263 < ddf["pu_locid"])
    )
    ddf["do_locid"] = ddf["do_locid"].mask(
        (ddf["do_locid"] < 1) | (263 < ddf["do_locid"])
    )
    ddf["do_dt"] = ddf["do_dt"].mask(
        (ddf["do_dt"] <= ddf["pu_dt"])
        | (ddf["pu_dt"] + timedelta(days=30) < ddf["do_dt"])
    )
    _mask_negative(ddf, "passengers", "<=0")
    _mask_negative(ddf, "distance", "<=0")
    _mask_negative(ddf, "fee_fare", "<0")
    _mask_negative(ddf, "fee_tip", "<0")
    _mask_negative(ddf, "fee_total", "<0")
    if "dbasenum" in ddf:
        ddf["dbasenum"] = ddf["dbasenum"].str.upper()
        ddf["dbasenum"] = ddf["dbasenum"].mask(ddf["dbasenum"] == "NULL")
    return ddf
