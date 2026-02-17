"""
Spatial query utilities for GeoJSON-based queries

Supports:
- GeoJSON polygon/multipolygon queries
- Shapely geometry support
- Point-in-tile queries
"""

import json
from pathlib import Path
from typing import Optional, Union

from pixelquery.grid.tile_grid import FixedTileGrid

# Try to import shapely for geometry operations
try:
    from shapely.geometry import MultiPolygon, Point, Polygon, box, shape  # noqa: F401
    from shapely.geometry.base import BaseGeometry

    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False
    BaseGeometry = None


def query_tiles_by_geometry(
    geometry: Union[dict, "BaseGeometry", str, Path],
    tile_grid: FixedTileGrid | None = None,
    available_tiles: list[str] | None = None,
) -> list[str]:
    """
    Find tiles that intersect with a GeoJSON geometry

    Args:
        geometry: GeoJSON dict, Shapely geometry, or path to GeoJSON file
        tile_grid: TileGrid instance (default: FixedTileGrid)
        available_tiles: Optional list of available tiles to filter

    Returns:
        List of tile IDs that intersect with the geometry

    Examples:
        >>> # GeoJSON dict
        >>> geojson = {
        ...     "type": "Polygon",
        ...     "coordinates": [[[126.9, 37.5], [127.1, 37.5], [127.1, 37.6], [126.9, 37.6], [126.9, 37.5]]]
        ... }
        >>> tiles = query_tiles_by_geometry(geojson)

        >>> # Shapely geometry
        >>> from shapely.geometry import Polygon
        >>> poly = Polygon([(126.9, 37.5), (127.1, 37.5), (127.1, 37.6), (126.9, 37.6)])
        >>> tiles = query_tiles_by_geometry(poly)

        >>> # GeoJSON file
        >>> tiles = query_tiles_by_geometry("study_area.geojson")
    """
    if not SHAPELY_AVAILABLE:
        raise ImportError(
            "shapely is required for geometry queries. Install with: pip install shapely"
        )

    # Initialize grid
    grid = tile_grid or FixedTileGrid()

    # Parse geometry
    geom = _parse_geometry(geometry)

    # Get bounding box
    minx, miny, maxx, maxy = geom.bounds

    # Get all tiles in bounding box
    bbox_tiles = grid.get_tiles_in_bounds((minx, miny, maxx, maxy))

    # Filter to tiles that actually intersect with geometry
    intersecting_tiles = []
    for tile_id in bbox_tiles:
        tile_bounds = grid.get_tile_bounds(tile_id)
        tile_box = box(*tile_bounds)

        if geom.intersects(tile_box):
            intersecting_tiles.append(tile_id)

    # Filter by available tiles if provided
    if available_tiles is not None:
        available_set = set(available_tiles)
        intersecting_tiles = [t for t in intersecting_tiles if t in available_set]

    return intersecting_tiles


def query_tiles_by_point(
    lon: float,
    lat: float,
    tile_grid: FixedTileGrid | None = None,
) -> str:
    """
    Get the tile containing a point

    Args:
        lon: Longitude in decimal degrees
        lat: Latitude in decimal degrees
        tile_grid: TileGrid instance (default: FixedTileGrid)

    Returns:
        Tile ID containing the point

    Examples:
        >>> tile = query_tiles_by_point(127.0, 37.5)
        >>> print(tile)
        'x4961_y1466'
    """
    grid = tile_grid or FixedTileGrid()
    return grid.get_tile_id(lon, lat)


def geometry_to_bbox(
    geometry: Union[dict, "BaseGeometry", str, Path],
) -> tuple[float, float, float, float]:
    """
    Get bounding box from geometry

    Args:
        geometry: GeoJSON dict, Shapely geometry, or path to GeoJSON file

    Returns:
        Bounding box as (minx, miny, maxx, maxy)

    Examples:
        >>> bbox = geometry_to_bbox(geojson)
        >>> print(bbox)
        (126.9, 37.5, 127.1, 37.6)
    """
    if not SHAPELY_AVAILABLE:
        raise ImportError("shapely is required. Install with: pip install shapely")

    geom = _parse_geometry(geometry)
    return geom.bounds  # type: ignore[no-any-return]


def clip_tile_to_geometry(
    tile_bounds: tuple[float, float, float, float],
    geometry: Union[dict, "BaseGeometry"],
) -> Optional["BaseGeometry"]:
    """
    Clip tile bounds to geometry

    Args:
        tile_bounds: Tile bounds (minx, miny, maxx, maxy)
        geometry: Clipping geometry

    Returns:
        Intersection geometry, or None if no intersection

    Examples:
        >>> clipped = clip_tile_to_geometry(tile_bounds, polygon)
    """
    if not SHAPELY_AVAILABLE:
        raise ImportError("shapely is required. Install with: pip install shapely")

    geom = _parse_geometry(geometry) if isinstance(geometry, dict) else geometry
    tile_box = box(*tile_bounds)

    if geom.intersects(tile_box):
        return geom.intersection(tile_box)
    return None


def _parse_geometry(
    geometry: Union[dict, "BaseGeometry", str, Path],
) -> "BaseGeometry":
    """
    Parse geometry from various input formats

    Args:
        geometry: GeoJSON dict, Shapely geometry, or path to GeoJSON file

    Returns:
        Shapely geometry object
    """
    if not SHAPELY_AVAILABLE:
        raise ImportError("shapely is required. Install with: pip install shapely")

    # Already a Shapely geometry
    if hasattr(geometry, "bounds") and hasattr(geometry, "intersects"):
        return geometry

    # Path to GeoJSON file
    if isinstance(geometry, (str, Path)):
        path = Path(geometry)
        if path.exists():
            with open(path) as f:
                geojson = json.load(f)
            return _geojson_to_geometry(geojson)
        else:
            raise FileNotFoundError(f"GeoJSON file not found: {geometry}")

    # GeoJSON dict
    if isinstance(geometry, dict):
        return _geojson_to_geometry(geometry)

    raise TypeError(f"Unsupported geometry type: {type(geometry)}")


def _geojson_to_geometry(geojson: dict) -> "BaseGeometry":
    """
    Convert GeoJSON dict to Shapely geometry

    Handles both Feature and raw geometry types.
    """
    # Handle FeatureCollection
    if geojson.get("type") == "FeatureCollection":
        features = geojson.get("features", [])
        if not features:
            raise ValueError("Empty FeatureCollection")
        # Use first feature or union all
        if len(features) == 1:
            return shape(features[0]["geometry"])
        else:
            from shapely.ops import unary_union

            geometries = [shape(f["geometry"]) for f in features]
            return unary_union(geometries)

    # Handle Feature
    if geojson.get("type") == "Feature":
        return shape(geojson["geometry"])

    # Handle raw geometry
    return shape(geojson)


# Convenience class for fluent API
class SpatialQuery:
    """
    Fluent API for spatial queries

    Examples:
        >>> from pixelquery.query.spatial import SpatialQuery
        >>>
        >>> # Query by GeoJSON
        >>> tiles = (SpatialQuery()
        ...     .from_geojson("study_area.geojson")
        ...     .filter_available(reader.list_tiles())
        ...     .get_tiles())
        >>>
        >>> # Query by bbox
        >>> tiles = (SpatialQuery()
        ...     .from_bbox(126.9, 37.5, 127.1, 37.6)
        ...     .get_tiles())
        >>>
        >>> # Query by point with buffer
        >>> tiles = (SpatialQuery()
        ...     .from_point(127.0, 37.5)
        ...     .buffer_km(5.0)
        ...     .get_tiles())
    """

    def __init__(self, tile_grid: FixedTileGrid | None = None):
        self.grid = tile_grid or FixedTileGrid()
        self._geometry = None
        self._available_tiles: list[str] | None = None

    def from_geojson(self, geojson: dict | str | Path) -> "SpatialQuery":
        """Set query geometry from GeoJSON"""
        if not SHAPELY_AVAILABLE:
            raise ImportError("shapely is required. Install with: pip install shapely")
        self._geometry = _parse_geometry(geojson)
        return self

    def from_bbox(self, minx: float, miny: float, maxx: float, maxy: float) -> "SpatialQuery":
        """Set query geometry from bounding box"""
        if not SHAPELY_AVAILABLE:
            raise ImportError("shapely is required. Install with: pip install shapely")
        self._geometry = box(minx, miny, maxx, maxy)
        return self

    def from_point(self, lon: float, lat: float) -> "SpatialQuery":
        """Set query geometry from point"""
        if not SHAPELY_AVAILABLE:
            raise ImportError("shapely is required. Install with: pip install shapely")
        self._geometry = Point(lon, lat)
        return self

    def buffer_km(self, radius_km: float) -> "SpatialQuery":
        """Buffer geometry by radius in kilometers"""
        if self._geometry is None:
            raise ValueError("Set geometry first with from_* methods")

        # Approximate degrees per km at equator (adjust for latitude if needed)
        degrees_per_km = 1 / 111.32
        buffer_deg = radius_km * degrees_per_km

        self._geometry = self._geometry.buffer(buffer_deg)
        return self

    def filter_available(self, available_tiles: list[str]) -> "SpatialQuery":
        """Filter results to available tiles only"""
        self._available_tiles = available_tiles
        return self

    def get_tiles(self) -> list[str]:
        """Execute query and return matching tiles"""
        if self._geometry is None:
            raise ValueError("Set geometry first with from_* methods")

        return query_tiles_by_geometry(
            self._geometry,
            tile_grid=self.grid,
            available_tiles=self._available_tiles,
        )

    def get_bbox(self) -> tuple[float, float, float, float]:
        """Get bounding box of current geometry"""
        if self._geometry is None:
            raise ValueError("Set geometry first with from_* methods")
        return self._geometry.bounds
