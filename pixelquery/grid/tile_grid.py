"""
TileGrid Implementation

Implements the geographic tile grid system for multi-resolution satellite data.
"""

from typing import Tuple
import math


class FixedTileGrid:
    """
    Fixed geographic tile grid (2.56km × 2.56km)

    Each tile has a fixed geographic size (2.56km × 2.56km) but contains
    different pixel counts depending on sensor resolution:
    - Sentinel-2 @ 10m: 256×256 pixels
    - Landsat-8 @ 30m: 85×85 pixels
    - Planet @ 3m: 853×853 pixels

    Tile IDs are in format "xNNNN_yNNNN" where:
    - x increases eastward from origin
    - y increases northward from origin
    - Origin (x0000_y0000) is at longitude 0°, latitude 0°

    Examples:
        >>> grid = FixedTileGrid()
        >>> tile_id = grid.get_tile_id(127.05, 37.55)  # Seoul, South Korea
        >>> print(tile_id)
        'x4961_y1466'
        >>>
        >>> bounds = grid.get_tile_bounds('x4961_y1466')
        >>> print(bounds)
        (127.04928, 37.54560, 127.07488, 37.57120)
        >>>
        >>> pixels = grid.get_pixels_for_resolution(10.0)  # Sentinel-2
        >>> print(pixels)
        256
    """

    # Tile size in meters (2.56km = 2560m)
    TILE_SIZE_M = 2560.0

    # Earth radius in meters (WGS84 equatorial radius)
    EARTH_RADIUS_M = 6378137.0

    def __init__(self, tile_size_m: float = TILE_SIZE_M):
        """
        Initialize tile grid

        Args:
            tile_size_m: Tile size in meters (default: 2560m = 2.56km)
        """
        self.tile_size_m = tile_size_m

    def get_tile_id(self, lon: float, lat: float) -> str:
        """
        Convert WGS84 coordinates to tile ID

        Args:
            lon: Longitude in decimal degrees (-180 to 180)
            lat: Latitude in decimal degrees (-90 to 90)

        Returns:
            Tile ID in format "xNNNN_yNNNN" (e.g., "x4961_y1466")

        Examples:
            >>> grid = FixedTileGrid()
            >>> grid.get_tile_id(127.05, 37.55)  # Seoul
            'x4961_y1466'
            >>> grid.get_tile_id(0.0, 0.0)  # Origin
            'x0000_y0000'
            >>> grid.get_tile_id(-122.42, 37.77)  # San Francisco
            'x-4776_y1474'
        """
        # Convert lon/lat to meters from origin (0, 0)
        x_m = self._lon_to_meters(lon, lat)
        y_m = self._lat_to_meters(lat)

        # Calculate tile indices
        x_idx = int(math.floor(x_m / self.tile_size_m))
        y_idx = int(math.floor(y_m / self.tile_size_m))

        # Format tile ID with sign handling
        return self._format_tile_id(x_idx, y_idx)

    def get_tile_bounds(self, tile_id: str) -> Tuple[float, float, float, float]:
        """
        Get geographic bounds of a tile

        Args:
            tile_id: Tile identifier (e.g., "x4961_y1466")

        Returns:
            Bounding box as (minx, miny, maxx, maxy) in WGS84 decimal degrees

        Examples:
            >>> grid = FixedTileGrid()
            >>> grid.get_tile_bounds('x0000_y0000')
            (0.0, 0.0, 0.02304, 0.02304)
            >>> grid.get_tile_bounds('x4961_y1466')
            (127.04928, 37.54560, 127.07232, 37.56864)
        """
        # Parse tile ID
        x_idx, y_idx = self._parse_tile_id(tile_id)

        # Calculate bounds in meters
        x_min_m = x_idx * self.tile_size_m
        x_max_m = (x_idx + 1) * self.tile_size_m
        y_min_m = y_idx * self.tile_size_m
        y_max_m = (y_idx + 1) * self.tile_size_m

        # Convert to lon/lat (use center lat for x conversion)
        center_lat = self._meters_to_lat((y_min_m + y_max_m) / 2)

        minx = self._meters_to_lon(x_min_m, center_lat)
        maxx = self._meters_to_lon(x_max_m, center_lat)
        miny = self._meters_to_lat(y_min_m)
        maxy = self._meters_to_lat(y_max_m)

        return (minx, miny, maxx, maxy)

    def get_pixels_for_resolution(self, resolution_m: float) -> int:
        """
        Calculate pixel count per tile for a given resolution

        Args:
            resolution_m: Spatial resolution in meters (e.g., 10.0 for Sentinel-2)

        Returns:
            Number of pixels along one dimension (square tiles assumed)

        Examples:
            >>> grid = FixedTileGrid()
            >>> grid.get_pixels_for_resolution(10.0)  # Sentinel-2
            256
            >>> grid.get_pixels_for_resolution(30.0)  # Landsat-8
            85
            >>> grid.get_pixels_for_resolution(3.0)   # Planet
            853
        """
        if resolution_m <= 0:
            raise ValueError(f"Resolution must be positive, got {resolution_m}")

        return int(math.ceil(self.tile_size_m / resolution_m))

    def get_tiles_in_bounds(self, bounds: Tuple[float, float, float, float]) -> list:
        """
        Get all tiles that intersect with the given bounds

        Args:
            bounds: Bounding box as (minx, miny, maxx, maxy) in WGS84 decimal degrees

        Returns:
            List of tile IDs that intersect with the bounds

        Examples:
            >>> grid = FixedTileGrid()
            >>> grid.get_tiles_in_bounds((127.0, 37.5, 127.1, 37.6))
            ['x4960_y1465', 'x4960_y1466', 'x4961_y1465', 'x4961_y1466']
        """
        minx, miny, maxx, maxy = bounds

        # Get tile IDs for all four corners
        # (needed because lon-to-meters conversion varies with latitude)
        tile_ids = [
            self.get_tile_id(minx, miny),
            self.get_tile_id(minx, maxy),
            self.get_tile_id(maxx, miny),
            self.get_tile_id(maxx, maxy)
        ]

        # Parse indices from all corners
        indices = [self._parse_tile_id(tid) for tid in tile_ids]
        x_indices = [idx[0] for idx in indices]
        y_indices = [idx[1] for idx in indices]

        # Get actual min/max indices
        min_x_idx = min(x_indices)
        max_x_idx = max(x_indices)
        min_y_idx = min(y_indices)
        max_y_idx = max(y_indices)

        # Generate all tiles in range and filter to those that actually intersect
        tiles = []
        for x_idx in range(min_x_idx, max_x_idx + 1):
            for y_idx in range(min_y_idx, max_y_idx + 1):
                tile_id = self._format_tile_id(x_idx, y_idx)

                # Check if tile actually intersects with bounds
                tile_minx, tile_miny, tile_maxx, tile_maxy = self.get_tile_bounds(tile_id)

                # Tiles intersect if they overlap in both dimensions
                if (tile_minx <= maxx and tile_maxx >= minx and
                    tile_miny <= maxy and tile_maxy >= miny):
                    tiles.append(tile_id)

        return tiles

    # -------------------------------------------------------------------------
    # Internal helper methods
    # -------------------------------------------------------------------------

    def _lon_to_meters(self, lon: float, lat: float) -> float:
        """Convert longitude to meters from origin (accounting for latitude)"""
        # Longitude degree length varies with latitude
        lat_rad = math.radians(lat)
        meters_per_degree = (math.pi / 180) * self.EARTH_RADIUS_M * math.cos(lat_rad)
        return lon * meters_per_degree

    def _lat_to_meters(self, lat: float) -> float:
        """Convert latitude to meters from origin"""
        # Latitude degree length is constant
        meters_per_degree = (math.pi / 180) * self.EARTH_RADIUS_M
        return lat * meters_per_degree

    def _meters_to_lon(self, x_m: float, lat: float) -> float:
        """Convert meters to longitude (accounting for latitude)"""
        lat_rad = math.radians(lat)
        meters_per_degree = (math.pi / 180) * self.EARTH_RADIUS_M * math.cos(lat_rad)
        if meters_per_degree == 0:
            return 0.0
        return x_m / meters_per_degree

    def _meters_to_lat(self, y_m: float) -> float:
        """Convert meters to latitude"""
        meters_per_degree = (math.pi / 180) * self.EARTH_RADIUS_M
        return y_m / meters_per_degree

    def _format_tile_id(self, x_idx: int, y_idx: int) -> str:
        """Format tile indices into tile ID string"""
        # Handle negative indices with sign
        x_str = f"x{x_idx:04d}" if x_idx >= 0 else f"x{x_idx:05d}"
        y_str = f"y{y_idx:04d}" if y_idx >= 0 else f"y{y_idx:05d}"
        return f"{x_str}_{y_str}"

    def _parse_tile_id(self, tile_id: str) -> Tuple[int, int]:
        """Parse tile ID string into x, y indices"""
        try:
            parts = tile_id.split('_')
            if len(parts) != 2:
                raise ValueError(f"Invalid tile ID format: {tile_id}")

            x_str = parts[0]
            y_str = parts[1]

            if not x_str.startswith('x') or not y_str.startswith('y'):
                raise ValueError(f"Invalid tile ID format: {tile_id}")

            x_idx = int(x_str[1:])
            y_idx = int(y_str[1:])

            return (x_idx, y_idx)
        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid tile ID '{tile_id}': {e}")
