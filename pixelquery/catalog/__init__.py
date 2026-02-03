"""
PixelQuery Catalog Module

Catalog implementations for metadata management.

Supports two backends:
- LocalCatalog: Arrow IPC + GeoParquet (legacy)
- IcebergCatalog: Apache Iceberg with SQLite catalog (recommended)

Use LocalCatalog.create() for automatic backend selection.
"""

from pixelquery.catalog.local import LocalCatalog
from pixelquery.catalog.iceberg import IcebergCatalog

__all__ = [
    'LocalCatalog',
    'IcebergCatalog',
]
