"""
PixelQuery - Turn your COG files into an analysis-ready time-series data cube

Zero-copy virtual references to Cloud-Optimized GeoTIFFs via Icechunk.

Quick Start:
    >>> import pixelquery as pq
    >>>
    >>> # Ingest COGs from a directory
    >>> result = pq.ingest("./my_cogs/", band_names=["blue", "green", "red", "nir"])
    >>>
    >>> # Query as lazy xarray Dataset
    >>> ds = pq.open_xarray("./warehouse")
    >>> ndvi = ds.bandmath("(b3 - b2) / (b3 + b2)")  # by band index
    >>> ndvi = ds.bandmath("(nir - red) / (nir + red)")  # by name
    >>>
    >>> # Point time-series
    >>> ts = pq.timeseries("./warehouse", lon=127.05, lat=37.55)
"""

# Apply imagecodecs compatibility patch for Icechunk/VirtualTIFF
from pixelquery._internal.codecs import patch_imagecodecs

patch_imagecodecs()

from pixelquery.core import (
    DataArray,
    # Classes
    Dataset,
    IngestionError,
    # Protocols
    PixelQuery,
    # Exceptions
    PixelQueryError,
    QueryError,
    QueryResult,
    TransactionError,
    ValidationError,
    compute_evi,
    compute_ndvi,
    list_tiles,
    # Functions
    open_dataset,
    open_mfdataset,
    open_xarray,
)

# Legacy imports (may fail if deps not installed)
try:
    from pixelquery.products import BandInfo
except ImportError:
    BandInfo = None  # type: ignore[misc, assignment]

try:
    from pixelquery.grid import TileGrid
except ImportError:
    TileGrid = None  # type: ignore[misc, assignment]

# Register xarray BandMath accessor (ds.bandmath.ndvi(), etc.)
import pixelquery.core.bandmath

__version__ = "0.1.0"

__all__ = [
    "DataArray",
    "Dataset",
    "IngestionError",
    "PixelQueryError",
    "QueryError",
    "TransactionError",
    "ValidationError",
    "__version__",
    "catalog",
    "compute_evi",
    "compute_ndvi",
    "ingest",
    "inspect_cog",
    "inspect_directory",
    "list_tiles",
    "open_dataset",
    "open_mfdataset",
    "open_xarray",
    "register_product",
    "timeseries",
]


# Lazy imports for new Icechunk features (avoids heavy import at startup)
def __getattr__(name):
    if name == "ingest":
        from pixelquery.io.auto_ingest import ingest

        return ingest
    elif name == "timeseries":
        from pixelquery.core.timeseries import timeseries

        return timeseries
    elif name == "inspect_cog":
        from pixelquery.io.cog_metadata import inspect_cog

        return inspect_cog
    elif name == "inspect_directory":
        from pixelquery.io.cog_metadata import inspect_directory

        return inspect_directory
    elif name == "catalog":
        from pixelquery.catalog.icechunk_catalog import IcechunkCatalog

        return IcechunkCatalog
    elif name == "register_product":
        from pixelquery.catalog.product_profile import register_product

        return register_product
    elif name == "ProductProfile":
        from pixelquery.catalog.product_profile import ProductProfile

        return ProductProfile
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
