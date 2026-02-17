"""
PixelQuery Core API Protocol

Main API interface for ingesting and querying satellite imagery.

Design principle: "Contract-First Development"
- API stability through Protocol definitions
- Enables parallel development
- Easy mocking/testing
- Clear contracts

Note: This file now contains only the main PixelQuery API.
Other protocols have been moved to their respective modules:
- products/base.py: ProductProfile, BandInfo
- grid/base.py: TileGrid
- _internal/storage/base.py: StorageBackend
- _internal/transactions/base.py: Transaction, TransactionManager
- core/result.py: QueryResult
- core/exceptions.py: All exceptions
"""

from datetime import datetime
from typing import Any, Protocol

from pixelquery.core.result import QueryResult


class PixelQuery(Protocol):
    """
    Main PixelQuery API

    High-level interface for ingesting satellite imagery and querying
    multi-resolution time-series data.
    """

    def add_image(
        self, image_path: str, acquisition_date: datetime, product_id: str, **metadata: Any
    ) -> dict[str, Any]:
        """
        Ingest satellite image with ACID guarantees

        Process:
        1. Read COG using ProductProfile
        2. Split into geographic tiles
        3. Append to monthly Arrow chunks (multi-resolution)
        4. Update GeoParquet metadata
        5. Commit Iceberg transaction

        Args:
            image_path: Path or URL to COG file
            acquisition_date: Image acquisition timestamp
            product_id: Product identifier (e.g., "sentinel2_l2a")
            **metadata: Additional metadata (cloud_cover, etc.)

        Returns:
            Ingestion result with snapshot_id, tiles_written, chunk_paths

        Raises:
            IngestionError: If ingestion fails
        """
        ...

    def query_by_bounds(
        self,
        bounds: tuple[float, float, float, float],
        date_range: tuple[datetime, datetime],
        bands: list[str],
        target_resolution: float = 10.0,
        as_of_snapshot_id: int | None = None,
    ) -> QueryResult:
        """
        Query multi-resolution time-series data

        Process:
        1. Iceberg metadata scan (spatial + temporal filter)
        2. DuckDB GeoParquet query
        3. Parallel Arrow chunk reads
        4. Resample to target resolution (multi-resolution fusion!)
        5. Return unified result

        Args:
            bounds: Geographic bounding box (minx, miny, maxx, maxy) in WGS84
            date_range: Temporal range (start_date, end_date)
            bands: Band names to retrieve (e.g., ["red", "nir"])
            target_resolution: Target spatial resolution in meters (default 10m)
            as_of_snapshot_id: Iceberg snapshot for time travel (optional)

        Returns:
            QueryResult with multi-resolution data resampled to target resolution

        Examples:
            # 1-year NDVI time-series (multi-resolution!)
            result = pq.query_by_bounds(
                bounds=(127.0, 37.5, 127.1, 37.6),
                date_range=(datetime(2024,1,1), datetime(2024,12,31)),
                bands=["red", "nir"],
                target_resolution=10.0  # Sentinel-2 + Landsat-8 → unified 10m
            )

            df = result.to_pandas()
            df['ndvi'] = (df['band_nir'] - df['band_red']) / (df['band_nir'] + df['band_red'])
        """
        ...

    def query_time_series(
        self,
        tile_id: str,
        date_range: tuple[datetime, datetime],
        bands: list[str],
        target_resolution: float = 10.0,
    ) -> QueryResult:
        """
        Optimized time-series query for a single tile

        This is optimized for the primary use case: multi-year time-series analysis.
        Reads monthly Arrow chunks sequentially for maximum I/O efficiency.

        Args:
            tile_id: Single tile identifier (e.g., "x0024_y0041")
            date_range: Temporal range
            bands: Band names to retrieve
            target_resolution: Target resolution in meters

        Returns:
            QueryResult with time-series data

        Performance:
            - 1 year: ~4-11 seconds (120 Arrow chunks)
            - 5 years: ~5-18 seconds (600 Arrow chunks)
            - 10x-30x faster than COG+STAC for multi-year queries
        """
        ...
