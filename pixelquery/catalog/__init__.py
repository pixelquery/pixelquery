"""
PixelQuery Catalog Module

Catalog implementations for metadata management.

Supports three backends:
- LocalCatalog: Arrow IPC + GeoParquet (legacy)
- IcebergCatalog: Apache Iceberg with SQLite catalog
- IcechunkCatalog: Icechunk virtual zarr references (recommended)

Use LocalCatalog.create() for automatic backend selection.
"""

__all__ = [
    "IcebergCatalog",
    "IcechunkCatalog",
    "LocalCatalog",
]


# Lazy imports — avoids top-level geopandas/pyiceberg/icechunk deps
def __getattr__(name):
    if name == "LocalCatalog":
        from pixelquery.catalog.local import LocalCatalog

        return LocalCatalog
    elif name == "IcebergCatalog":
        from pixelquery.catalog.iceberg import IcebergCatalog

        return IcebergCatalog
    elif name == "IcechunkCatalog":
        from pixelquery.catalog.icechunk_catalog import IcechunkCatalog

        return IcechunkCatalog
    elif name == "ProductProfile":
        from pixelquery.catalog.product_profile import ProductProfile

        return ProductProfile
    elif name == "register_product":
        from pixelquery.catalog.product_profile import register_product

        return register_product
    elif name == "get_registry":
        from pixelquery.catalog.product_profile import get_registry

        return get_registry
    elif name == "ProductRegistry":
        from pixelquery.catalog.product_profile import ProductRegistry

        return ProductRegistry
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
