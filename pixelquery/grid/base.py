"""
Tile Grid System Protocol

Geographic tile grid for multi-resolution satellite data.
"""

from typing import Protocol, Tuple


class TileGrid(Protocol):
    """
    Geographic tile grid system for multi-resolution data

    PixelQuery uses a fixed geographic grid (e.g., 2.56km × 2.56km tiles)
    where each tile contains variable pixel counts depending on product resolution:
    - Sentinel-2 @ 10m: 256×256 pixels
    - Landsat-8 @ 30m: 85×85 pixels
    - Planet @ 3m: 853×853 pixels
    """

    def get_tile_id(self, lon: float, lat: float) -> str:
        """
        Convert WGS84 coordinates to tile ID

        Args:
            lon: Longitude in decimal degrees
            lat: Latitude in decimal degrees

        Returns:
            Tile ID in format "xNNNN_yNNNN" (e.g., "x0024_y0041")
        """
        ...

    def get_tile_bounds(self, tile_id: str) -> Tuple[float, float, float, float]:
        """
        Get geographic bounds of a tile

        Args:
            tile_id: Tile identifier (e.g., "x0024_y0041")

        Returns:
            Bounding box as (minx, miny, maxx, maxy) in WGS84
        """
        ...

    def get_pixels_for_resolution(self, resolution_m: float) -> int:
        """
        Calculate pixel count per tile for a given resolution

        Args:
            resolution_m: Spatial resolution in meters

        Returns:
            Number of pixels along one dimension (square tiles assumed)

        Examples:
            10m resolution → 256 pixels (2560m / 10m)
            30m resolution → 85 pixels (2560m / 30m)
        """
        ...
