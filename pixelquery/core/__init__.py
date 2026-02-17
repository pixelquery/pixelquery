"""
PixelQuery Core Module

Core API, protocols, exceptions, and result types.
"""

from pixelquery.core.api import (
    compute_evi,
    compute_ndvi,
    list_tiles,
    open_dataset,
    open_mfdataset,
    open_xarray,
)
from pixelquery.core.dataarray import DataArray
from pixelquery.core.dataset import Dataset
from pixelquery.core.exceptions import (
    IngestionError,
    PixelQueryError,
    QueryError,
    TransactionError,
    ValidationError,
)
from pixelquery.core.interfaces import PixelQuery
from pixelquery.core.result import QueryResult

__all__ = [
    "DataArray",
    "Dataset",
    "IngestionError",
    "PixelQuery",
    "PixelQueryError",
    "QueryError",
    "QueryResult",
    "TransactionError",
    "ValidationError",
    "compute_evi",
    "compute_ndvi",
    "list_tiles",
    "open_dataset",
    "open_mfdataset",
    "open_xarray",
]
