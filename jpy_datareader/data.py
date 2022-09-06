"""
Module contains tools for collecting data from various remote sources
"""

import warnings

from jpy_datareader.estat import StatsListReader
from jpy_datareader.estat import MetaInfoReader
from jpy_datareader.estat import StatsDataReader
from jpy_datareader.estat import DataCatalogReader

__all__ = [
    "get_data_estat_statslist",
    "get_data_estat_metainfo",
    "get_data_estat_statsdata",
    "get_data_estat_datacatalog"
]

def get_data_estat_statslist(*args, **kwargs):
    return StatsListReader(*args, **kwargs).read()

def get_data_estat_metainfo(*args, **kwargs):
    return MetaInfoReader(*args, **kwargs).read()

def get_data_estat_statsdata(*args, **kwargs):
    return StatsDataReader(*args, **kwargs).read(normal=False)

def get_data_estat_datacatalog(*args, **kwargs):
    return DataCatalogReader(*args, **kwargs).read()


def DataReader(
    name=None,
    data_source=None,
    retry_count=3,
    pause=0.1,
    session=None,
    api_key=None,
):

    expected_source = [
        "estat",
        "estat-statslist",
        "estat-metainfo",
        "estat-datacatalog",
    ]

    if data_source not in expected_source:
        msg = f"data_source={data_source} is not implemented"
        raise NotImplementedError(msg)

    if data_source == "estat":
        return StatsDataReader(
            statsDataId=name,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read(normal=False)

    if data_source == "estat-statslist":
        return StatsDataReader(
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "estat-metainfo":
        return StatsDataReader(
            statsDataId=name,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "estat-datacatalog":
        return StatsDataReader(
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()
