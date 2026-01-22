"""
PixelQuery Core Module

Core API, protocols, exceptions, and result types.
"""

from pixelquery.core.interfaces import PixelQuery
from pixelquery.core.result import QueryResult
from pixelquery.core.exceptions import (
    PixelQueryError,
    TransactionError,
    IngestionError,
    QueryError,
    ValidationError,
)
from pixelquery.core.dataset import Dataset
from pixelquery.core.dataarray import DataArray
from pixelquery.core.api import (
    open_dataset,
    open_mfdataset,
    list_tiles,
    compute_ndvi,
    compute_evi,
)

__all__ = [
    # Protocols
    "PixelQuery",
    "QueryResult",
    # Classes
    "Dataset",
    "DataArray",
    # Functions
    "open_dataset",
    "open_mfdataset",
    "list_tiles",
    "compute_ndvi",
    "compute_evi",
    # Exceptions
    "PixelQueryError",
    "TransactionError",
    "IngestionError",
    "QueryError",
    "ValidationError",
]
