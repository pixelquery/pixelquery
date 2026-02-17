"""
Local filesystem-based catalog implementation

Supports two backends:
- Arrow (default): Uses Arrow IPC files + GeoParquet metadata
- Iceberg: Uses Apache Iceberg tables with SQLite catalog
"""

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Union

from pixelquery._internal.storage.geoparquet import GeoParquetReader, GeoParquetWriter, TileMetadata

if TYPE_CHECKING:
    from pixelquery.catalog.iceberg import IcebergCatalog


class LocalCatalog:
    """
    Local filesystem-based catalog for PixelQuery warehouse

    Manages metadata using GeoParquet files stored in the warehouse.

    Attributes:
        warehouse_path: Path to the warehouse directory
        metadata_path: Path to the metadata.parquet file

    Examples:
        >>> from pixelquery.catalog import LocalCatalog
        >>>
        >>> # Auto-detect backend (Arrow or Iceberg)
        >>> catalog = LocalCatalog.create("warehouse")
        >>>
        >>> # Force specific backend
        >>> catalog = LocalCatalog.create("warehouse", backend="iceberg")
        >>>
        >>> # List all tiles
        >>> tiles = catalog.list_tiles()
        >>>
        >>> # Query tiles by spatial bounds
        >>> tiles = catalog.query_tiles(bounds=(-180, -90, 180, 90))
    """

    @classmethod
    def create(
        cls,
        warehouse_path: str,
        backend: str = "auto",
    ) -> Union["LocalCatalog", "IcebergCatalog"]:
        """
        Factory method to create catalog with specified backend

        Args:
            warehouse_path: Path to warehouse directory
            backend: Storage backend to use:
                - "auto": Uses Iceberg if catalog.db exists, else Arrow
                - "arrow": Forces Arrow IPC + GeoParquet backend
                - "iceberg": Forces Apache Iceberg backend

        Returns:
            Catalog instance with appropriate backend

        Examples:
            >>> # Auto-detect (recommended)
            >>> catalog = LocalCatalog.create("warehouse")

            >>> # Force Iceberg
            >>> catalog = LocalCatalog.create("warehouse", backend="iceberg")

            >>> # Force Arrow (legacy)
            >>> catalog = LocalCatalog.create("warehouse", backend="arrow")
        """
        warehouse = Path(warehouse_path)

        if backend == "auto":
            # Check for Icechunk repository (highest priority)
            icechunk_dir = warehouse / ".icechunk"
            if icechunk_dir.exists():
                from pixelquery.catalog.icechunk_catalog import IcechunkCatalog

                return IcechunkCatalog(str(warehouse_path))

            # Check for Iceberg catalog
            iceberg_db = warehouse / "catalog.db"
            if iceberg_db.exists():
                from pixelquery.catalog.iceberg import IcebergCatalog

                return IcebergCatalog(str(warehouse_path))

            # Check for Arrow metadata
            arrow_metadata = warehouse / "metadata.parquet"
            if arrow_metadata.exists():
                return cls(str(warehouse_path))

            # Default to Iceberg for new warehouses
            from pixelquery.catalog.iceberg import IcebergCatalog

            return IcebergCatalog(str(warehouse_path))

        elif backend == "icechunk":
            from pixelquery.catalog.icechunk_catalog import IcechunkCatalog

            return IcechunkCatalog(str(warehouse_path))

        elif backend == "iceberg":
            from pixelquery.catalog.iceberg import IcebergCatalog

            return IcebergCatalog(str(warehouse_path))

        elif backend == "arrow":
            return cls(str(warehouse_path))

        else:
            raise ValueError(
                f"Unknown backend: {backend}. Use 'auto', 'arrow', 'iceberg', or 'icechunk'"
            )

    def __init__(self, warehouse_path: str):
        """
        Initialize LocalCatalog

        Args:
            warehouse_path: Path to the warehouse directory
        """
        self.warehouse_path = Path(warehouse_path)
        self.metadata_path = self.warehouse_path / "metadata.parquet"

        # Create warehouse directory if it doesn't exist
        self.warehouse_path.mkdir(parents=True, exist_ok=True)

        self.reader = GeoParquetReader()
        self.writer = GeoParquetWriter()

    def exists(self) -> bool:
        """
        Check if the catalog metadata file exists

        Returns:
            True if metadata file exists, False otherwise
        """
        return self.metadata_path.exists()

    def list_tiles(
        self,
        bounds: tuple[float, float, float, float] | None = None,
        time_range: tuple[datetime, datetime] | None = None,
        product_id: str | None = None,
    ) -> list[str]:
        """
        List all unique tile IDs in the catalog

        Args:
            bounds: Optional bounding box (minx, miny, maxx, maxy)
            time_range: Optional time range (start, end)
            product_id: Optional product filter

        Returns:
            List of unique tile IDs

        Examples:
            >>> catalog.list_tiles()
            ['x0024_y0041', 'x0024_y0042', ...]

            >>> # Filter by bounds
            >>> catalog.list_tiles(bounds=(126.5, 37.0, 127.5, 38.0))
            ['x4961_y1466']
        """
        if not self.exists():
            return []

        # Query metadata
        if bounds:
            metadata_list = self.reader.query_by_bounds(str(self.metadata_path), bounds)
        else:
            metadata_list = self.reader.read_metadata(str(self.metadata_path))

        # Apply filters
        if time_range:
            start, end = time_range
            start_str = start.strftime("%Y-%m")
            end_str = end.strftime("%Y-%m")
            metadata_list = [m for m in metadata_list if start_str <= m.year_month <= end_str]

        if product_id:
            metadata_list = [m for m in metadata_list if m.product_id == product_id]

        # Extract unique tile IDs
        tile_ids = sorted({m.tile_id for m in metadata_list})
        return tile_ids

    def list_bands(self, tile_id: str | None = None, product_id: str | None = None) -> list[str]:
        """
        List all available bands

        Args:
            tile_id: Optional tile filter
            product_id: Optional product filter

        Returns:
            List of unique band names

        Examples:
            >>> catalog.list_bands()
            ['blue', 'green', 'red', 'nir', ...]

            >>> catalog.list_bands(tile_id="x0024_y0041")
            ['red', 'nir']
        """
        if not self.exists():
            return []

        metadata_list = self.reader.read_metadata(str(self.metadata_path))

        # Apply filters
        if tile_id:
            metadata_list = [m for m in metadata_list if m.tile_id == tile_id]

        if product_id:
            metadata_list = [m for m in metadata_list if m.product_id == product_id]

        # Extract unique bands
        bands = sorted({m.band for m in metadata_list})
        return bands

    def query_metadata(
        self, tile_id: str, year_month: str | None = None, band: str | None = None
    ) -> list[TileMetadata]:
        """
        Query metadata for a specific tile

        Args:
            tile_id: Tile identifier (e.g., "x0024_y0041")
            year_month: Optional year-month filter (e.g., "2024-01")
            band: Optional band filter (e.g., "red")

        Returns:
            List of TileMetadata matching the query

        Examples:
            >>> metadata = catalog.query_metadata("x0024_y0041", "2024-01", "red")
            >>> metadata[0].chunk_path
            'warehouse/tiles/x0024_y0041/2024-01/red.arrow'
        """
        if not self.exists():
            return []

        # Query by tile and time
        metadata_list = self.reader.query_by_tile_and_time(
            str(self.metadata_path), tile_id, year_month
        )

        # Filter by band
        if band:
            metadata_list = [m for m in metadata_list if m.band == band]

        return metadata_list

    def get_chunk_paths(
        self, tile_id: str, year_month: str, bands: list[str] | None = None
    ) -> dict[str, str]:
        """
        Get chunk file paths for a tile-month combination

        Args:
            tile_id: Tile identifier
            year_month: Year-month string (e.g., "2024-01")
            bands: Optional list of bands (default: all)

        Returns:
            Dictionary mapping band names to chunk paths

        Examples:
            >>> paths = catalog.get_chunk_paths("x0024_y0041", "2024-01", ["red", "nir"])
            >>> paths
            {'red': 'warehouse/tiles/x0024_y0041/2024-01/red.arrow',
             'nir': 'warehouse/tiles/x0024_y0041/2024-01/nir.arrow'}
        """
        metadata_list = self.query_metadata(tile_id, year_month)

        # Filter by bands
        if bands:
            metadata_list = [m for m in metadata_list if m.band in bands]

        # Build path dictionary
        chunk_paths = {m.band: m.chunk_path for m in metadata_list}
        return chunk_paths

    def get_tile_bounds(self, tile_id: str) -> tuple[float, float, float, float] | None:
        """
        Get geographic bounds for a tile

        Args:
            tile_id: Tile identifier

        Returns:
            Tuple of (minx, miny, maxx, maxy) or None if not found

        Examples:
            >>> catalog.get_tile_bounds("x0024_y0041")
            (126.5, 37.0, 127.5, 38.0)
        """
        if not self.exists():
            return None

        metadata_list = self.reader.read_metadata(str(self.metadata_path))

        # Find any metadata for this tile
        for m in metadata_list:
            if m.tile_id == tile_id:
                return m.bounds

        return None

    def add_tile_metadata(self, metadata: TileMetadata, mode: str = "append"):
        """
        Add tile metadata to the catalog

        Args:
            metadata: TileMetadata to add
            mode: 'append' or 'overwrite'

        Examples:
            >>> from pixelquery._internal.storage.geoparquet import TileMetadata
            >>> metadata = TileMetadata(
            ...     tile_id="x0024_y0041",
            ...     year_month="2024-01",
            ...     band="red",
            ...     bounds=(126.5, 37.0, 127.5, 38.0),
            ...     num_observations=10,
            ...     min_value=100.0,
            ...     max_value=5000.0,
            ...     mean_value=2500.0,
            ...     cloud_cover=0.1,
            ...     product_id="sentinel2_l2a",
            ...     resolution=10.0,
            ...     chunk_path="tiles/x0024_y0041/2024-01/red.arrow"
            ... )
            >>> catalog.add_tile_metadata(metadata)
        """
        self.writer.write_metadata([metadata], str(self.metadata_path), mode=mode)

    def add_tile_metadata_batch(self, metadata_list: list[TileMetadata], mode: str = "append"):
        """
        Add multiple tile metadata records in batch

        Args:
            metadata_list: List of TileMetadata to add
            mode: 'append' or 'overwrite'

        Examples:
            >>> metadata_list = [...]  # List of TileMetadata
            >>> catalog.add_tile_metadata_batch(metadata_list)
        """
        self.writer.write_metadata(metadata_list, str(self.metadata_path), mode=mode)

    def get_statistics(
        self, tile_id: str, band: str, time_range: tuple[datetime, datetime] | None = None
    ) -> dict[str, float]:
        """
        Get statistics for a tile-band combination

        Args:
            tile_id: Tile identifier
            band: Band name
            time_range: Optional time range filter

        Returns:
            Dictionary with min, max, mean, cloud_cover

        Examples:
            >>> stats = catalog.get_statistics("x0024_y0041", "red")
            >>> stats
            {'min': 100.0, 'max': 5000.0, 'mean': 2500.0, 'cloud_cover': 0.1}
        """
        metadata_list = self.query_metadata(tile_id, band=band)

        # Filter by time range
        if time_range:
            start, end = time_range
            start_str = start.strftime("%Y-%m")
            end_str = end.strftime("%Y-%m")
            metadata_list = [m for m in metadata_list if start_str <= m.year_month <= end_str]

        if not metadata_list:
            return {}

        # Aggregate statistics
        return {
            "min": min(m.min_value for m in metadata_list),
            "max": max(m.max_value for m in metadata_list),
            "mean": sum(m.mean_value for m in metadata_list) / len(metadata_list),
            "cloud_cover": sum(m.cloud_cover for m in metadata_list) / len(metadata_list),
            "num_observations": sum(m.num_observations for m in metadata_list),
        }

    def __repr__(self) -> str:
        """String representation"""
        exists_str = "exists" if self.exists() else "not initialized"
        return f"<LocalCatalog>\nWarehouse: {self.warehouse_path}\nMetadata: {exists_str}"
