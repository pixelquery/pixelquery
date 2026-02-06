"""
PixelQuery Query Module

Query execution, spatial/temporal filtering, and resampling.
"""

from pixelquery.query.executor import QueryExecutor
from pixelquery.query.spatial import (
    query_tiles_by_geometry,
    query_tiles_by_point,
    geometry_to_bbox,
    SpatialQuery,
)

__all__ = [
    'QueryExecutor',
    'query_tiles_by_geometry',
    'query_tiles_by_point',
    'geometry_to_bbox',
    'SpatialQuery',
]
