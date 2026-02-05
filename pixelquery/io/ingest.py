"""
Ingestion Pipeline for COG files

Converts Cloud-Optimized GeoTIFF files into PixelQuery's tiled storage format.

Supports two storage backends:
- Arrow IPC (legacy): Uses Arrow IPC files + GeoParquet metadata
- Iceberg (default): Uses Apache Iceberg tables with ACID transactions
"""

import logging
from typing import Dict, List, Optional, Tuple, Union, TYPE_CHECKING
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
from numpy.typing import NDArray
import rasterio
from rasterio.windows import Window, from_bounds
from rasterio.warp import reproject, Resampling
from rasterio.crs import CRS
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

# Module-level logger
logger = logging.getLogger(__name__)

from pixelquery.io.cog import COGReader
from pixelquery.grid.tile_grid import FixedTileGrid
from pixelquery.catalog.local import LocalCatalog
from pixelquery._internal.storage.geoparquet import TileMetadata
from pixelquery._internal.storage.arrow_chunk import ArrowChunkWriter

if TYPE_CHECKING:
    from pixelquery.io.iceberg_writer import IcebergPixelWriter
    from pixelquery.catalog.iceberg import IcebergCatalog

# Try to use Rust resampling (4-6x faster than scipy)
try:
    from pixelquery_core import resample_bilinear, resample_nearest_neighbor
    RUST_RESAMPLE_AVAILABLE = True
    logger.debug("Using Rust resampling (4-6x faster)")
except ImportError:
    RUST_RESAMPLE_AVAILABLE = False
    logger.debug("Rust resampling not available, using scipy fallback")

# Fallback to scipy if Rust not available
from scipy.ndimage import zoom


def _process_tile_band_worker(
    cog_path: str,
    tile_id: str,
    tile_bounds: Tuple[float, float, float, float],
    band_index: int,
    band_name: str,
    acquisition_time: datetime,
    product_id: str,
    warehouse_path: str,
    tile_size_m: float,
    resolution: float,
    nodata: Optional[float]
) -> Optional[TileMetadata]:
    """
    Worker function for parallel tile-band processing

    This function is designed to be pickle-able for use with ProcessPoolExecutor.
    It processes a single tile-band combination independently.

    Args:
        cog_path: Path to COG file
        tile_id: Tile identifier (e.g., 'x0024_y0041')
        tile_bounds: Tile bounds (minx, miny, maxx, maxy) in WGS84
        band_index: Band index in COG (1-based)
        band_name: Band name (e.g., 'red', 'nir')
        acquisition_time: Acquisition timestamp
        product_id: Product identifier
        warehouse_path: Warehouse root path
        tile_size_m: Tile size in meters
        resolution: Spatial resolution in meters
        nodata: Nodata value

    Returns:
        TileMetadata if successful, None if no valid data
    """
    # Import resampling functions
    try:
        from pixelquery_core import resample_bilinear, resample_nearest_neighbor
        use_rust = True
    except ImportError:
        from scipy.ndimage import zoom
        use_rust = False

    # Calculate target tile size in pixels
    tile_size_pixels = int(tile_size_m / resolution)

    # Open COG and extract tile data
    try:
        import rasterio
        with rasterio.open(cog_path) as src:
            cog_crs = src.crs
            cog_transform = src.transform

            # Convert tile bounds to COG CRS if needed
            if cog_crs and cog_crs != CRS.from_epsg(4326):
                from rasterio.warp import transform_bounds
                tile_bounds_cog = transform_bounds(
                    'EPSG:4326',
                    cog_crs,
                    *tile_bounds
                )
            else:
                tile_bounds_cog = tile_bounds

            # Calculate window in COG coordinates
            try:
                window = from_bounds(
                    *tile_bounds_cog,
                    transform=cog_transform
                )
            except ValueError:
                # Tile doesn't overlap COG
                return None

            # Read band data with window
            band_data = src.read(band_index, window=window)
            if band_data is None or band_data.size == 0:
                return None

            # Get mask (inverse of valid data mask)
            mask_data = src.read_masks(band_index, window=window) > 0

            # Resample to target resolution
            if use_rust:
                # Rust resampling (4-6x faster)
                pixels = resample_bilinear(
                    band_data.astype(np.uint16),
                    tile_size_pixels,
                    tile_size_pixels
                )
                mask = resample_nearest_neighbor(
                    mask_data,
                    tile_size_pixels,
                    tile_size_pixels
                )
            else:
                # Scipy fallback
                zoom_factors = (
                    tile_size_pixels / band_data.shape[0],
                    tile_size_pixels / band_data.shape[1]
                )
                pixels = zoom(band_data, zoom_factors, order=1).astype(np.uint16)
                mask = zoom(mask_data, zoom_factors, order=0).astype(bool)

            # Handle nodata
            if nodata is not None:
                mask = mask & (pixels != int(nodata))

            # Skip if no valid data
            if not mask.any():
                return None

            # Write chunk
            year_month = acquisition_time.strftime("%Y-%m")
            chunk_path = f"tiles/{tile_id}/{year_month}/{band_name}.arrow"
            full_chunk_path = Path(warehouse_path) / chunk_path

            # Ensure directory exists
            full_chunk_path.parent.mkdir(parents=True, exist_ok=True)

            # Write or append to chunk
            chunk_writer = ArrowChunkWriter()
            chunk_writer.append_to_chunk(
                str(full_chunk_path),
                data={
                    'time': [acquisition_time],
                    'pixels': [pixels.flatten()],
                    'mask': [mask.flatten()]
                },
                product_id=product_id,
                resolution=resolution,
                metadata={'band': band_name}
            )

            # Compute statistics
            valid_pixels = pixels[mask]
            min_value = float(valid_pixels.min()) if len(valid_pixels) > 0 else 0.0
            max_value = float(valid_pixels.max()) if len(valid_pixels) > 0 else 0.0
            mean_value = float(valid_pixels.mean()) if len(valid_pixels) > 0 else 0.0

            # Create metadata
            metadata = TileMetadata(
                tile_id=tile_id,
                year_month=year_month,
                band=band_name,
                bounds=tile_bounds,
                num_observations=1,
                min_value=min_value,
                max_value=max_value,
                mean_value=mean_value,
                cloud_cover=0.0,
                product_id=product_id,
                resolution=resolution,
                chunk_path=chunk_path
            )

            return metadata

    except Exception as e:
        # Log error but don't crash the worker
        logger.error(
            "Error processing tile-band: %s/%s - %s",
            tile_id, band_name, str(e),
            exc_info=True
        )
        return None


class IngestionPipeline:
    """
    Ingest COG files into PixelQuery warehouse

    Converts Cloud-Optimized GeoTIFF files into tiled storage format with
    automatic tiling, resampling, and metadata management.

    Supports two storage backends:
    - Arrow IPC (legacy): Uses Arrow IPC files + GeoParquet metadata
    - Iceberg (default): Uses Apache Iceberg tables with ACID transactions

    Attributes:
        warehouse_path: Path to warehouse directory
        tile_grid: TileGrid instance for geographic tiling
        catalog: Catalog instance for metadata management
        storage_backend: "auto", "arrow", or "iceberg"

    Examples:
        >>> # Recommended: Iceberg backend with auto-detect
        >>> pipeline = IngestionPipeline(
        ...     warehouse_path="./warehouse",
        ...     tile_grid=FixedTileGrid(),
        ...     storage_backend="iceberg"
        ... )
        >>> metadata = pipeline.ingest_cog(
        ...     cog_path="sentinel2.tif",
        ...     acquisition_time=datetime(2024, 6, 15),
        ...     product_id="sentinel2_l2a",
        ...     band_mapping={1: "red", 2: "green", 3: "blue", 4: "nir"}
        ... )
    """

    def __init__(
        self,
        warehouse_path: str,
        tile_grid: FixedTileGrid,
        catalog: Optional[Union[LocalCatalog, "IcebergCatalog"]] = None,
        max_workers: Optional[int] = None,
        storage_backend: str = "auto",
    ):
        """
        Initialize ingestion pipeline

        Args:
            warehouse_path: Path to warehouse directory
            tile_grid: TileGrid instance
            catalog: Optional catalog instance (auto-created if None)
            max_workers: Maximum number of parallel workers.
                        None = use CPU count, 1 = sequential processing
            storage_backend: Storage backend to use:
                - "auto": Uses Iceberg if catalog.db exists, else Iceberg for new
                - "arrow": Forces Arrow IPC + GeoParquet backend
                - "iceberg": Forces Apache Iceberg backend
        """
        self.warehouse_path = Path(warehouse_path)
        self.tile_grid = tile_grid
        self.storage_backend = storage_backend
        self._metadata_buffer = []  # Buffer for accumulating metadata
        self.max_workers = max_workers if max_workers is not None else mp.cpu_count()

        # Determine actual backend
        self._use_iceberg = self._should_use_iceberg()

        # Initialize catalog
        if catalog is not None:
            self.catalog = catalog
        else:
            # Auto-create catalog based on backend
            self.catalog = LocalCatalog.create(
                str(warehouse_path),
                backend="iceberg" if self._use_iceberg else "arrow"
            )

        # Initialize appropriate writer
        if self._use_iceberg:
            from pixelquery.io.iceberg_writer import IcebergPixelWriter
            self.iceberg_writer = IcebergPixelWriter(str(warehouse_path))
            self.chunk_writer = None  # Not used for Iceberg
            logger.info("Using Iceberg storage backend")
        else:
            self.chunk_writer = ArrowChunkWriter()
            self.iceberg_writer = None  # Not used for Arrow
            logger.info("Using Arrow IPC storage backend")

    def _should_use_iceberg(self) -> bool:
        """Determine whether to use Iceberg backend."""
        if self.storage_backend == "iceberg":
            return True
        elif self.storage_backend == "arrow":
            return False
        else:  # "auto"
            # Check for existing Iceberg catalog
            iceberg_db = self.warehouse_path / "catalog.db"
            if iceberg_db.exists():
                return True
            # Check for existing Arrow metadata
            arrow_metadata = self.warehouse_path / "metadata.parquet"
            if arrow_metadata.exists():
                return False
            # Default to Iceberg for new warehouses
            return True

    def ingest_cog(
        self,
        cog_path: str,
        acquisition_time: datetime,
        product_id: str,
        band_mapping: Dict[int, str],
        auto_commit: bool = True,
        parallel: bool = True
    ) -> List[TileMetadata]:
        """
        Ingest COG file into warehouse

        Workflow:
        1. Open COG and get bounds
        2. Find all overlapping tiles
        3. For each tile and band:
           - Extract pixel window from COG
           - Resample to tile resolution
           - Create nodata mask
           - Write to Arrow IPC file
           - Register metadata with catalog
        4. Return list of created metadata

        Args:
            cog_path: Path to COG file
            acquisition_time: Acquisition timestamp
            product_id: Product identifier (e.g., "sentinel2_l2a")
            band_mapping: Mapping from COG band index to band name
                         (e.g., {1: "red", 2: "green", 3: "blue", 4: "nir"})
            auto_commit: If False, metadata is buffered instead of written immediately.
                        Use flush_metadata() to write all buffered metadata at once.
            parallel: If True and max_workers > 1, process tiles in parallel.
                     Set to False for sequential processing.

        Returns:
            List of TileMetadata for created tiles

        Examples:
            >>> metadata_list = pipeline.ingest_cog(
            ...     cog_path="sentinel2.tif",
            ...     acquisition_time=datetime(2024, 6, 15, 10, 30),
            ...     product_id="sentinel2_l2a",
            ...     band_mapping={1: "blue", 2: "green", 3: "red", 4: "nir"}
            ... )
            >>> len(metadata_list)
            16  # 4 tiles × 4 bands
        """
        metadata_list = []

        # Get COG properties
        with COGReader(cog_path) as reader:
            cog_bounds = reader.get_bounds(target_crs='EPSG:4326')
            cog_metadata = reader.get_metadata()
            resolution = reader.get_resolution()
            nodata = cog_metadata.get('nodata')

        # Find overlapping tiles
        tiles = self.tile_grid.get_tiles_in_bounds(cog_bounds)

        # Process tiles in parallel or sequential
        # Note: Iceberg requires sequential processing for atomic batch writes
        use_parallel = parallel and self.max_workers > 1 and not self._use_iceberg
        if self._use_iceberg and parallel and self.max_workers > 1:
            logger.debug("Iceberg backend requires sequential processing; parallel disabled")
        if use_parallel:
            # PARALLEL PROCESSING
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []

                # Submit all tile-band combinations
                for tile_id in tiles:
                    tile_bounds = self.tile_grid.get_tile_bounds(tile_id)

                    for band_index, band_name in band_mapping.items():
                        future = executor.submit(
                            _process_tile_band_worker,
                            cog_path=cog_path,
                            tile_id=tile_id,
                            tile_bounds=tile_bounds,
                            band_index=band_index,
                            band_name=band_name,
                            acquisition_time=acquisition_time,
                            product_id=product_id,
                            warehouse_path=str(self.warehouse_path),
                            tile_size_m=self.tile_grid.tile_size_m,
                            resolution=resolution,
                            nodata=nodata
                        )
                        futures.append(future)

                # Collect results as they complete
                for future in as_completed(futures):
                    try:
                        metadata = future.result()
                        if metadata is not None:
                            metadata_list.append(metadata)
                    except Exception as e:
                        logger.error("Worker failed: %s", str(e), exc_info=True)
        else:
            # SEQUENTIAL PROCESSING
            observations_batch = []  # For Iceberg batch writes

            with COGReader(cog_path) as reader:
                for tile_id in tiles:
                    tile_bounds = self.tile_grid.get_tile_bounds(tile_id)

                    # Process each band
                    for band_index, band_name in band_mapping.items():
                        # Extract and process band data for this tile
                        pixels, mask = self._extract_tile_data(
                            reader=reader,
                            band_index=band_index,
                            tile_bounds=tile_bounds,
                            resolution=resolution,
                            nodata=nodata
                        )

                        # Skip if no valid data in this tile
                        if not mask.any():
                            continue

                        # Create year_month for path/partition
                        year_month = acquisition_time.strftime("%Y-%m")

                        if self._use_iceberg:
                            # ICEBERG BACKEND: Batch observations for atomic write
                            observations_batch.append({
                                "tile_id": tile_id,
                                "band": band_name,
                                "time": acquisition_time,
                                "pixels": pixels,
                                "mask": mask,
                                "product_id": product_id,
                                "resolution": resolution,
                                "bounds": tile_bounds,
                            })
                        else:
                            # ARROW BACKEND: Write to Arrow IPC file
                            chunk_path = f"tiles/{tile_id}/{year_month}/{band_name}.arrow"
                            full_chunk_path = self.warehouse_path / chunk_path

                            # Ensure directory exists
                            full_chunk_path.parent.mkdir(parents=True, exist_ok=True)

                            # Write or append to chunk
                            self.chunk_writer.append_to_chunk(
                                str(full_chunk_path),
                                data={
                                    'time': [acquisition_time],
                                    'pixels': [pixels.flatten()],
                                    'mask': [mask.flatten()]
                                },
                                product_id=product_id,
                                resolution=resolution,
                                metadata={'band': band_name}
                            )

                        # Compute statistics
                        valid_pixels = pixels[mask]
                        min_value = float(valid_pixels.min()) if len(valid_pixels) > 0 else 0.0
                        max_value = float(valid_pixels.max()) if len(valid_pixels) > 0 else 0.0
                        mean_value = float(valid_pixels.mean()) if len(valid_pixels) > 0 else 0.0

                        # Create metadata (for both backends)
                        if self._use_iceberg:
                            chunk_path = f"iceberg://{self.warehouse_path}/pixelquery/pixel_data"
                        else:
                            chunk_path = f"tiles/{tile_id}/{year_month}/{band_name}.arrow"

                        metadata = TileMetadata(
                            tile_id=tile_id,
                            year_month=year_month,
                            band=band_name,
                            bounds=tile_bounds,
                            num_observations=1,
                            min_value=min_value,
                            max_value=max_value,
                            mean_value=mean_value,
                            cloud_cover=0.0,  # TODO: Implement cloud mask detection
                            product_id=product_id,
                            resolution=resolution,
                            chunk_path=chunk_path
                        )

                        metadata_list.append(metadata)

            # For Iceberg, write all observations atomically
            if self._use_iceberg and observations_batch:
                self.iceberg_writer.write_observations(observations_batch)

        # Register all metadata with catalog
        if metadata_list:
            if self._use_iceberg:
                # For Iceberg, data is already written; no separate metadata registration needed
                # The metadata is embedded in the Iceberg table
                logger.debug(f"Iceberg: {len(metadata_list)} observations committed")
            else:
                # For Arrow, register metadata with GeoParquet catalog
                if auto_commit:
                    self.catalog.add_tile_metadata_batch(metadata_list)
                else:
                    self._metadata_buffer.extend(metadata_list)

        return metadata_list

    def flush_metadata(self) -> None:
        """
        Flush buffered metadata to catalog

        Writes all accumulated metadata from previous ingest_cog() calls
        with auto_commit=False to the catalog in a single batch operation.
        This is much more efficient than writing metadata after each file.

        Note: For Iceberg backend, metadata is committed atomically with data.
        This method is mainly for Arrow backend compatibility.

        Examples:
            >>> pipeline = IngestionPipeline(...)
            >>> for cog_path in cog_files:
            ...     pipeline.ingest_cog(..., auto_commit=False)
            >>> pipeline.flush_metadata()  # Write all metadata at once
        """
        if self._use_iceberg:
            # Iceberg commits are atomic; nothing to flush
            logger.debug("Iceberg backend: metadata committed atomically with data")
            self._metadata_buffer.clear()
        elif self._metadata_buffer:
            self.catalog.add_tile_metadata_batch(self._metadata_buffer)
            self._metadata_buffer.clear()

    def _extract_tile_data(
        self,
        reader: COGReader,
        band_index: int,
        tile_bounds: tuple,
        resolution: float,
        nodata: float = None
    ) -> tuple:
        """
        Extract and resample data for a tile

        Args:
            reader: COGReader instance
            band_index: Band index in COG (1-based)
            tile_bounds: Tile bounds (minx, miny, maxx, maxy) in WGS84
            resolution: Target resolution in meters
            nodata: Nodata value

        Returns:
            Tuple of (pixels, mask) where pixels is 2D array and mask is boolean array
        """
        # Calculate target tile size in pixels
        tile_size_pixels = self.tile_grid.get_pixels_for_resolution(resolution)

        # Get COG metadata
        cog_meta = reader.get_metadata()
        cog_crs = cog_meta['crs']
        cog_transform = cog_meta['transform']

        # Convert tile bounds to COG CRS if needed
        if cog_crs and cog_crs != CRS.from_epsg(4326):
            from rasterio.warp import transform_bounds
            tile_bounds_cog = transform_bounds(
                'EPSG:4326',
                cog_crs,
                *tile_bounds
            )
        else:
            tile_bounds_cog = tile_bounds

        # Calculate window in COG coordinates
        try:
            window = from_bounds(
                *tile_bounds_cog,
                transform=cog_transform
            )

            # Read data from window
            data = reader.read_window(window, band_index=band_index)

            # Check if data is empty or has zero dimensions
            if data.size == 0 or data.shape[0] == 0 or data.shape[1] == 0:
                # Empty data - return empty tile
                empty_pixels = np.zeros((tile_size_pixels, tile_size_pixels), dtype=np.uint16)
                empty_mask = np.zeros((tile_size_pixels, tile_size_pixels), dtype=bool)
                return empty_pixels, empty_mask

        except (ValueError, IndexError):
            # Window is outside COG bounds - return empty tile
            empty_pixels = np.zeros((tile_size_pixels, tile_size_pixels), dtype=np.uint16)
            empty_mask = np.zeros((tile_size_pixels, tile_size_pixels), dtype=bool)
            return empty_pixels, empty_mask

        # Resample to target tile size
        resampled_pixels = self._resample_array(
            data,
            target_shape=(tile_size_pixels, tile_size_pixels)
        )

        # Create mask
        if nodata is not None:
            mask = resampled_pixels != nodata
        else:
            mask = np.ones_like(resampled_pixels, dtype=bool)

        return resampled_pixels, mask

    def _resample_array(
        self,
        data: NDArray,
        target_shape: tuple
    ) -> NDArray:
        """
        Resample array to target shape using bilinear interpolation

        Uses Rust implementation when available (4-6x faster),
        falls back to scipy.ndimage.zoom otherwise.

        Args:
            data: Input array (2D)
            target_shape: Target shape (height, width)

        Returns:
            Resampled array
        """
        if data.shape == target_shape:
            return data

        target_h, target_w = target_shape

        if RUST_RESAMPLE_AVAILABLE:
            # Use Rust resampling (4-6x faster)
            resampled = resample_bilinear(
                data.astype(np.uint16),
                target_h,
                target_w
            )
        else:
            # Fallback to scipy bilinear interpolation
            zoom_factors = (
                target_h / data.shape[0],
                target_w / data.shape[1]
            )
            resampled = zoom(data, zoom_factors, order=1)
            if resampled.shape != target_shape:
                resampled = resampled[:target_h, :target_w]

        return resampled.astype(data.dtype)

    def __repr__(self) -> str:
        """String representation"""
        return (
            f"<IngestionPipeline>\\n"
            f"  Warehouse: {self.warehouse_path}\\n"
            f"  Tile Grid: {self.tile_grid}"
        )
