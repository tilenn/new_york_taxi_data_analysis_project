import re
from collections.abc import Iterable
from io import BytesIO
from pathlib import Path
from typing import Literal
from zipfile import ZipFile

import requests

PATH_HPC_ORIGINAL = Path("ext/src")
PATH_HPC_OUT = Path("ext/out")

_RE_ORIGINALFILE = re.compile(r"\w+_(\d{4})-(\d{2})\.parquet")


def get_ym_from_fname(fname: Path | str) -> tuple[int, int]:
    if not isinstance(fname, Path):
        fname = Path(fname)
    fname = fname.name
    match = _RE_ORIGINALFILE.fullmatch(fname)
    assert match is not None, f"cannot extract year and month from {fname!r}"
    return int(match[1]), int(match[2])


def list_original_files(
    category: str, starting_date: tuple[int, int]
) -> Iterable[Path]:
    for p in PATH_HPC_ORIGINAL.glob(f"{category}_????-??.parquet"):
        if get_ym_from_fname(p) < starting_date:
            continue
        yield p


def get_path_t1(dataset: Literal["yellow", "green", "fhv", "fhvhv"]) -> Path:
    return PATH_HPC_OUT / "t1" / f"{dataset}.parquet"


def get_paths_original_yellow() -> Iterable[Path]:
    return list_original_files("yellow_tripdata", (2012, 1))


def get_paths_original_green() -> Iterable[Path]:
    return list_original_files("green_tripdata", (2014, 1))


def get_paths_original_fhv() -> Iterable[Path]:
    return list_original_files("fhv_tripdata", (2015, 1))


def get_paths_original_fhvhv() -> Iterable[Path]:
    return list_original_files("fhvhv_tripdata", (2019, 2))


def get_path_shapefile(*, download: bool = True) -> Path:
    p = PATH_HPC_OUT / "external/taxi_zones/taxi_zones.shp"
    if not download or p.is_file():
        return p
    print("shapefile not found, downloading...")
    if not PATH_HPC_OUT.is_dir():
        raise RuntimeError("unable to download shapefile: ext/out is not a directory")
    with ZipFile(
        BytesIO(
            requests.get(
                "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
            ).content
        ),
        "r",
    ) as zf:
        p.parent.parent.mkdir(exist_ok=True)
        p.parent.mkdir(exist_ok=True)
        zf.extractall(p.parent)
    print("shapefile download complete.")
    return p
