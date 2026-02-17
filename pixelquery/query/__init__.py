"""
PixelQuery Query Module

Query execution, spatial/temporal filtering, and resampling.
"""

from pixelquery.query.executor import QueryExecutor
from pixelquery.query.spatial import (
    SpatialQuery,
    geometry_to_bbox,
    query_tiles_by_geometry,
    query_tiles_by_point,
)

__all__ = [
    "QueryExecutor",
    "SpatialQuery",
    "geometry_to_bbox",
    "query_tiles_by_geometry",
    "query_tiles_by_point",
]
