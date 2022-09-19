"""
Module contains tools for collecting data from various remote sources
"""

import warnings

from jpy_datareader.estat import StatsListReader
from jpy_datareader.estat import MetaInfoReader
from jpy_datareader.estat import StatsDataReader
from jpy_datareader.estat import DataCatalogReader
from jpy_datareader.gbizinfo import hojinReader
from jpy_datareader.gbizinfo import corporate_naumberReader
from jpy_datareader.gbizinfo import certificationReader
from jpy_datareader.gbizinfo import commendationReader
from jpy_datareader.gbizinfo import financeReader
from jpy_datareader.gbizinfo import patentReader
from jpy_datareader.gbizinfo import procurementReader
from jpy_datareader.gbizinfo import subsidyReader
from jpy_datareader.gbizinfo import workplaceReader

__all__ = [
    "get_data_estat_statslist",
    "get_data_estat_metainfo",
    "get_data_estat_statsdata",
    "get_data_estat_datacatalog",
    "get_data_gbizinfo_hojin",
    "get_data_gbizinfo_corporate_naumber",
    "get_data_gbizinfo_certification",
    "get_data_gbizinfo_commendation",
    "get_data_gbizinfo_finance",
    "get_data_gbizinfo_patent",
    "get_data_gbizinfo_procurement",
    "get_data_gbizinfo_subsidy",
    "get_data_gbizinfo_workplace"
]

def get_data_estat_statslist(*args, **kwargs):
    return StatsListReader(*args, **kwargs).read()

def get_data_estat_metainfo(*args, **kwargs):
    return MetaInfoReader(*args, **kwargs).read()

def get_data_estat_statsdata(*args, **kwargs):
    return StatsDataReader(*args, **kwargs).read(normal=False)

def get_data_estat_datacatalog(*args, **kwargs):
    return DataCatalogReader(*args, **kwargs).read()

def get_data_gbizinfo_hojin(*args, **kwargs):
    return hojinReader(*args, **kwargs).read()

def get_data_gbizinfo_corporate_naumber(*args, **kwargs):
    return corporate_naumberReader(*args, **kwargs).read()

def get_data_gbizinfo_certification(*args, **kwargs):
    return certificationReader(*args, **kwargs).read()

def get_data_gbizinfo_commendation(*args, **kwargs):
    return commendationReader(*args, **kwargs).read()

def get_data_gbizinfo_finance(*args, **kwargs):
    return financeReader(*args, **kwargs).read()

def get_data_gbizinfo_patent(*args, **kwargs):
    return patentReader(*args, **kwargs).read()

def get_data_gbizinfo_procurement(*args, **kwargs):
    return procurementReader(*args, **kwargs).read()

def get_data_gbizinfo_subsidy(*args, **kwargs):
    return subsidyReader(*args, **kwargs).read()

def get_data_gbizinfo_workplace(*args, **kwargs):
    return workplaceReader(*args, **kwargs).read()

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
        "gbizinfo-hojin",
        "gbizinfo-corporate_naumber",
        "gbizinfo-certification",
        "gbizinfo-commendation",
        "gbizinfo-finance",
        "gbizinfo-patent",
        "gbizinfo-procurement",
        "gbizinfo-subsidy",
        "gbizinfo-workplace",
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

    if data_source == "gbizinfo-hojin":
        return hojinReader(
            corporate_number=name,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "gbizinfo-corporate_naumber":
        return corporate_naumberReader(
            corporate_number=name,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "gbizinfo-certification":
        return certificationReader(
            corporate_number=name,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "gbizinfo-commendation":
        return commendationReader(
            corporate_number=name,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "gbizinfo-finance":
        return financeReader(
            corporate_number=name,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "gbizinfo-patent":
        return patentReader(
            corporate_number=name,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "gbizinfo-procurement":
        return procurementReader(
            corporate_number=name,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "gbizinfo-subsidy":
        return subsidyReader(
            corporate_number=name,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "gbizinfo-workplace":
        return workplaceReader(
            corporate_number=name,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()