"""
Cloud-Optimized GeoTIFF (COG) reader using Rasterio
"""

from typing import Any

import numpy as np
import rasterio
from numpy.typing import NDArray
from rasterio.warp import transform_bounds
from rasterio.windows import Window


class COGReader:
    """
    Cloud-Optimized GeoTIFF reader using Rasterio

    Provides methods to read COG files, extract metadata, and handle
    coordinate transformations.

    Attributes:
        file_path: Path to the COG file
        dataset: Rasterio dataset handle

    Examples:
        >>> with COGReader("sentinel2.tif") as reader:
        ...     bounds = reader.get_bounds()
        ...     band_data = reader.read_band(1)
        ...     metadata = reader.get_metadata()
    """

    def __init__(self, file_path: str):
        """
        Open COG file with Rasterio

        Args:
            file_path: Path to COG file

        Raises:
            FileNotFoundError: If file doesn't exist
            rasterio.errors.RasterioIOError: If file can't be opened
        """
        self.file_path = file_path
        self.dataset = rasterio.open(file_path, "r")

    def read_band(self, band_index: int) -> NDArray:
        """
        Read single band as NumPy array

        Args:
            band_index: Band index (1-based, following GDAL convention)

        Returns:
            NumPy array with band data

        Examples:
            >>> reader = COGReader("image.tif")
            >>> red_band = reader.read_band(1)
            >>> red_band.shape
            (1024, 1024)
        """
        return self.dataset.read(band_index)

    def read_window(self, window: Window, band_index: int) -> NDArray:
        """
        Read specific window from a band

        Args:
            window: Rasterio Window object
            band_index: Band index (1-based)

        Returns:
            NumPy array with windowed data

        Examples:
            >>> from rasterio.windows import Window
            >>> reader = COGReader("image.tif")
            >>> window = Window(0, 0, 256, 256)  # col_off, row_off, width, height
            >>> data = reader.read_window(window, band_index=1)
        """
        return self.dataset.read(band_index, window=window)

    def get_metadata(self) -> dict[str, Any]:
        """
        Extract metadata from COG

        Returns:
            Dictionary with metadata:
            - crs: Coordinate Reference System
            - transform: Affine transform
            - bounds: Native bounds (minx, miny, maxx, maxy)
            - width: Image width in pixels
            - height: Image height in pixels
            - count: Number of bands
            - dtype: Data type
            - nodata: Nodata value

        Examples:
            >>> reader = COGReader("image.tif")
            >>> meta = reader.get_metadata()
            >>> meta['crs']
            CRS.from_epsg(32633)
        """
        return {
            "crs": self.dataset.crs,
            "transform": self.dataset.transform,
            "bounds": self.dataset.bounds,
            "width": self.dataset.width,
            "height": self.dataset.height,
            "count": self.dataset.count,
            "dtype": self.dataset.dtypes[0],
            "nodata": self.dataset.nodata,
        }

    def get_bounds(self, target_crs: str = "EPSG:4326") -> tuple[float, float, float, float]:
        """
        Get geographic bounds in target CRS

        Args:
            target_crs: Target coordinate system (default: WGS84)

        Returns:
            Tuple of (minx, miny, maxx, maxy) in target CRS

        Examples:
            >>> reader = COGReader("image.tif")
            >>> bounds = reader.get_bounds()  # WGS84 bounds
            >>> bounds
            (126.5, 37.0, 127.5, 38.0)
        """
        if self.dataset.crs is None:
            # Assume WGS84 if no CRS
            return self.dataset.bounds

        # Transform bounds to target CRS
        bounds = transform_bounds(self.dataset.crs, target_crs, *self.dataset.bounds)

        return bounds

    def get_resolution(self) -> float:
        """
        Get pixel resolution in meters

        Returns:
            Pixel resolution in meters (approximate)

        Notes:
            - For projected CRS: uses transform directly
            - For geographic CRS: converts degrees to meters at image center
            - Returns average of x and y resolutions

        Examples:
            >>> reader = COGReader("sentinel2.tif")
            >>> reader.get_resolution()
            10.0  # 10 meters
        """
        transform = self.dataset.transform

        # Get pixel size in native units
        pixel_size_x = abs(transform.a)
        pixel_size_y = abs(transform.e)

        # Check if CRS is projected (meters) or geographic (degrees)
        if self.dataset.crs is not None and self.dataset.crs.is_projected:
            # Already in meters
            return (pixel_size_x + pixel_size_y) / 2.0
        else:
            # Geographic CRS (degrees) - convert to meters
            # Use approximate conversion at image center
            # 1 degree ≈ 111,320 meters at equator
            center_lat = (self.dataset.bounds.bottom + self.dataset.bounds.top) / 2.0
            meters_per_degree = 111320.0 * np.cos(np.radians(center_lat))

            pixel_size_meters_x = pixel_size_x * meters_per_degree
            pixel_size_meters_y = pixel_size_y * 111320.0  # latitude constant

            return (pixel_size_meters_x + pixel_size_meters_y) / 2.0

    def get_mask(self, band_index: int) -> NDArray:
        """
        Get mask for nodata pixels

        Args:
            band_index: Band index (1-based)

        Returns:
            Boolean array where True = valid data, False = nodata

        Examples:
            >>> reader = COGReader("image.tif")
            >>> mask = reader.get_mask(1)
            >>> valid_pixels = data[mask]
        """
        data = self.read_band(band_index)

        if self.dataset.nodata is not None:
            # Create mask: True where data is valid
            mask = data != self.dataset.nodata
        else:
            # No nodata value - all pixels valid
            mask = np.ones(data.shape, dtype=bool)

        return mask

    def close(self):
        """
        Close file handle

        Examples:
            >>> reader = COGReader("image.tif")
            >>> reader.close()
        """
        if self.dataset is not None:
            self.dataset.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def __repr__(self) -> str:
        """String representation"""
        if self.dataset.closed:
            return f"<COGReader (closed): {self.file_path}>"
        else:
            return (
                f"<COGReader: {self.file_path}>\n"
                f"  Size: {self.dataset.width} x {self.dataset.height}\n"
                f"  Bands: {self.dataset.count}\n"
                f"  CRS: {self.dataset.crs}"
            )
