import os

from .data import (
    get_data_estat_statslist,
    get_data_estat_metainfo,
    get_data_estat_statsdata,
)

PKG = os.path.dirname(__file__)

__all__ = [
    "__version__",
    "get_data_estat_statslist",
    "get_data_estat_metainfo",
    "get_data_estat_statsdata",
]
