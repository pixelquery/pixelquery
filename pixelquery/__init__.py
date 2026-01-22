"""
PixelQuery - Apache Iceberg-based storage engine for satellite imagery

Multi-resolution satellite data as a queryable data lake.

Example usage:
    >>> import pixelquery as pq
    >>>
    >>> # Open a dataset (xarray-inspired API)
    >>> ds = pq.open_dataset("warehouse", tile_id="x0024_y0041")
    >>>
    >>> # Select bands and time range
    >>> subset = ds.sel(time=slice("2024-01", "2024-12"), bands=["red", "nir"])
    >>>
    >>> # Compute vegetation index
    >>> ndvi = pq.compute_ndvi(ds["red"], ds["nir"])
    >>>
    >>> # Convert to xarray for plotting
    >>> xr_ds = ds.to_xarray()
    >>> xr_ds["red"].plot()
"""

from pixelquery.core import (
    # Protocols
    PixelQuery,
    QueryResult,
    # Classes
    Dataset,
    DataArray,
    # Functions
    open_dataset,
    open_mfdataset,
    list_tiles,
    compute_ndvi,
    compute_evi,
    # Exceptions
    PixelQueryError,
    TransactionError,
    IngestionError,
    QueryError,
    ValidationError,
)

from pixelquery.products import BandInfo, ProductProfile
from pixelquery.grid import TileGrid

__version__ = "0.1.0"

__all__ = [
    # Main API Functions
    "open_dataset",
    "open_mfdataset",
    "list_tiles",
    # Classes
    "Dataset",
    "DataArray",
    # Utility Functions
    "compute_ndvi",
    "compute_evi",
    # Protocols (for advanced users)
    "PixelQuery",
    "QueryResult",
    "BandInfo",
    "ProductProfile",
    "TileGrid",
    # Exceptions
    "PixelQueryError",
    "TransactionError",
    "IngestionError",
    "QueryError",
    "ValidationError",
    # Metadata
    "__version__",
]
