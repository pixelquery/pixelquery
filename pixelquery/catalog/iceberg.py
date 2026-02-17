"""
Iceberg-based catalog implementation

Provides the same interface as LocalCatalog but uses Apache Iceberg
for ACID transactions and Time Travel support.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from pixelquery._internal.storage.iceberg_storage import IcebergStorageManager
from pixelquery.io.iceberg_reader import IcebergPixelReader

logger = logging.getLogger(__name__)


@dataclass
class TileMetadata:
    """
    Metadata for a tile-month-band combination

    Compatible with LocalCatalog's TileMetadata interface.
    """

    tile_id: str
    year_month: str
    band: str
    bounds: tuple[float, float, float, float] | None  # (minx, miny, maxx, maxy)
    num_observations: int
    min_value: float
    max_value: float
    mean_value: float
    cloud_cover: float
    product_id: str
    resolution: float
    chunk_path: str  # For compatibility, maps to Iceberg table path


class IcebergCatalog:
    """
    Iceberg-based catalog for PixelQuery warehouse

    Provides the same interface as LocalCatalog but uses Apache Iceberg
    for ACID transactions and Time Travel support.

    Key Features:
    - ACID transactions for all operations
    - Time Travel via snapshot_id parameter
    - Native partition pruning for efficient queries
    - Automatic metadata management

    Examples:
        >>> from pixelquery.catalog import IcebergCatalog
        >>> catalog = IcebergCatalog("warehouse")
        >>>
        >>> # List all tiles
        >>> tiles = catalog.list_tiles()
        >>>
        >>> # Time Travel: list tiles at specific snapshot
        >>> tiles = catalog.list_tiles(as_of_snapshot_id=12345)
        >>>
        >>> # Get snapshot history
        >>> snapshots = catalog.get_snapshot_history()
    """

    def __init__(self, warehouse_path: str, auto_initialize: bool = True):
        """
        Initialize IcebergCatalog

        Args:
            warehouse_path: Path to the warehouse directory
            auto_initialize: Whether to initialize storage immediately
        """
        self.warehouse_path = Path(warehouse_path)
        self.storage = IcebergStorageManager(str(warehouse_path))
        self.reader = IcebergPixelReader(str(warehouse_path), auto_initialize=False)

        if auto_initialize:
            self.storage.initialize()
            self.reader.storage = self.storage

        # For compatibility with LocalCatalog interface
        self.metadata_path = self.warehouse_path / "catalog.db"

    def exists(self) -> bool:
        """
        Check if the catalog exists

        Returns:
            True if catalog database exists, False otherwise
        """
        return self.metadata_path.exists()

    def list_tiles(
        self,
        bounds: tuple[float, float, float, float] | None = None,
        time_range: tuple[datetime, datetime] | None = None,
        product_id: str | None = None,
        as_of_snapshot_id: int | None = None,
    ) -> list[str]:
        """
        List all unique tile IDs in the catalog

        Args:
            bounds: Optional bounding box (minx, miny, maxx, maxy)
            time_range: Optional time range (start, end)
            product_id: Optional product filter
            as_of_snapshot_id: Query at specific snapshot (Time Travel)

        Returns:
            List of unique tile IDs

        Examples:
            >>> catalog.list_tiles()
            ['x0024_y0041', 'x0024_y0042', ...]

            >>> # Time Travel query
            >>> catalog.list_tiles(as_of_snapshot_id=12345)
        """
        if not self.exists():
            return []

        # Always refresh table to see latest snapshots and data
        # This is needed because separate writer instances may have committed new data
        self.storage.table.refresh()

        # Get tiles from reader
        tiles = self.reader.list_tiles(
            time_range=time_range,
            as_of_snapshot_id=as_of_snapshot_id,
        )

        # Filter by product_id if specified (requires full scan)
        if product_id:
            filtered_tiles = []
            for tile_id in tiles:
                # Check if any observation matches product_id
                try:
                    data = self.reader.read_tile(
                        tile_id,
                        time_range=time_range,
                        as_of_snapshot_id=as_of_snapshot_id,
                    )
                    for band_data in data.values():
                        if product_id in band_data.get("metadata", {}).get("product_ids", []):
                            filtered_tiles.append(tile_id)
                            break
                except Exception:
                    continue
            tiles = filtered_tiles

        # Filter by bounds if specified (requires checking geometry)
        if bounds:
            filtered_tiles = []
            for tile_id in tiles:
                tile_bounds = self.get_tile_bounds(tile_id, as_of_snapshot_id)
                if tile_bounds and self._bounds_intersect(bounds, tile_bounds):
                    filtered_tiles.append(tile_id)
            tiles = filtered_tiles

        return sorted(tiles)

    def _bounds_intersect(
        self,
        bounds1: tuple[float, float, float, float],
        bounds2: tuple[float, float, float, float],
    ) -> bool:
        """Check if two bounding boxes intersect."""
        minx1, miny1, maxx1, maxy1 = bounds1
        minx2, miny2, maxx2, maxy2 = bounds2

        return not (
            maxx1 < minx2  # bounds1 is left of bounds2
            or minx1 > maxx2  # bounds1 is right of bounds2
            or maxy1 < miny2  # bounds1 is below bounds2
            or miny1 > maxy2  # bounds1 is above bounds2
        )

    def list_bands(
        self,
        tile_id: str | None = None,
        product_id: str | None = None,
        as_of_snapshot_id: int | None = None,
    ) -> list[str]:
        """
        List all available bands

        Args:
            tile_id: Optional tile filter
            product_id: Optional product filter
            as_of_snapshot_id: Query at specific snapshot (Time Travel)

        Returns:
            List of unique band names

        Examples:
            >>> catalog.list_bands()
            ['blue', 'green', 'red', 'nir', ...]
        """
        if not self.exists():
            return []

        # Always refresh table to see latest snapshots and data
        # This is needed because separate writer instances may have committed new data
        self.storage.table.refresh()

        return self.reader.list_bands(
            tile_id=tile_id,
            as_of_snapshot_id=as_of_snapshot_id,
        )

    def query_metadata(
        self,
        tile_id: str,
        year_month: str | None = None,
        band: str | None = None,
        as_of_snapshot_id: int | None = None,
    ) -> list[TileMetadata]:
        """
        Query metadata for a specific tile

        Args:
            tile_id: Tile identifier (e.g., "x0024_y0041")
            year_month: Optional year-month filter (e.g., "2024-01")
            band: Optional band filter (e.g., "red")
            as_of_snapshot_id: Query at specific snapshot (Time Travel)

        Returns:
            List of TileMetadata matching the query

        Examples:
            >>> metadata = catalog.query_metadata("x0024_y0041", "2024-01", "red")
            >>> metadata[0].num_observations
            10
        """
        if not self.exists():
            return []

        from pyiceberg.expressions import And, EqualTo

        # Always refresh table to see latest snapshots and data
        # This is needed because separate writer instances may have committed new data
        self.storage.table.refresh()

        # Build filters
        filters = [EqualTo("tile_id", tile_id)]
        if year_month:
            filters.append(EqualTo("year_month", year_month))
        if band:
            filters.append(EqualTo("band", band))

        row_filter = And(*filters) if len(filters) > 1 else filters[0]

        # Execute scan
        if as_of_snapshot_id:
            scan = self.storage.table.scan(
                snapshot_id=as_of_snapshot_id,
                row_filter=row_filter,
            )
        else:
            scan = self.storage.table.scan(row_filter=row_filter)

        arrow_table = scan.to_arrow()

        # Group by tile_id, year_month, band and aggregate
        metadata_list = self._aggregate_metadata(arrow_table)

        return metadata_list

    def _aggregate_metadata(self, arrow_table) -> list[TileMetadata]:
        """Aggregate Arrow table rows into TileMetadata."""
        import pyarrow.compute as pc

        if arrow_table.num_rows == 0:
            return []

        metadata_list = []

        # Get unique combinations of tile_id, year_month, band
        tile_ids = arrow_table.column("tile_id").to_pylist()
        year_months = arrow_table.column("year_month").to_pylist()
        bands = arrow_table.column("band").to_pylist()

        unique_combinations = set(zip(tile_ids, year_months, bands, strict=False))

        for tile_id, year_month, band in unique_combinations:
            # Filter for this combination
            mask = pc.and_(
                pc.and_(
                    pc.equal(arrow_table.column("tile_id"), tile_id),
                    pc.equal(arrow_table.column("year_month"), year_month),
                ),
                pc.equal(arrow_table.column("band"), band),
            )
            subset = arrow_table.filter(mask)

            if subset.num_rows == 0:
                continue

            # Aggregate statistics
            min_values = subset.column("min_value").to_pylist()
            max_values = subset.column("max_value").to_pylist()
            mean_values = subset.column("mean_value").to_pylist()
            cloud_covers = subset.column("cloud_cover").to_pylist()

            # Filter out None values
            min_values = [v for v in min_values if v is not None]
            max_values = [v for v in max_values if v is not None]
            mean_values = [v for v in mean_values if v is not None]
            cloud_covers = [v for v in cloud_covers if v is not None]

            # Get first product_id and resolution (assuming consistent within group)
            product_id = subset.column("product_id")[0].as_py()
            resolution = subset.column("resolution")[0].as_py()

            # Get bounds from WKB if available
            bounds = None
            bounds_wkbs = subset.column("bounds_wkb").to_pylist()
            for wkb in bounds_wkbs:
                if wkb:
                    try:
                        from shapely import wkb as shapely_wkb

                        geom = shapely_wkb.loads(wkb)
                        bounds = geom.bounds  # (minx, miny, maxx, maxy)
                        break
                    except Exception:
                        pass

            metadata = TileMetadata(
                tile_id=tile_id,
                year_month=year_month,
                band=band,
                bounds=bounds,
                num_observations=subset.num_rows,
                min_value=min(min_values) if min_values else 0.0,
                max_value=max(max_values) if max_values else 0.0,
                mean_value=sum(mean_values) / len(mean_values) if mean_values else 0.0,
                cloud_cover=sum(cloud_covers) / len(cloud_covers) if cloud_covers else 0.0,
                product_id=product_id,
                resolution=resolution,
                chunk_path=f"iceberg://{self.warehouse_path}/pixelquery/pixel_data",
            )

            metadata_list.append(metadata)

        return metadata_list

    def get_chunk_paths(
        self,
        tile_id: str,
        year_month: str,
        bands: list[str] | None = None,
        as_of_snapshot_id: int | None = None,
    ) -> dict[str, str]:
        """
        Get chunk file paths for a tile-month combination

        Note: In Iceberg, data is stored in Parquet files managed by the table.
        This method returns a reference path for compatibility.

        Args:
            tile_id: Tile identifier
            year_month: Year-month string (e.g., "2024-01")
            bands: Optional list of bands (default: all)
            as_of_snapshot_id: Query at specific snapshot (Time Travel)

        Returns:
            Dictionary mapping band names to reference paths

        Examples:
            >>> paths = catalog.get_chunk_paths("x0024_y0041", "2024-01", ["red", "nir"])
        """
        metadata_list = self.query_metadata(
            tile_id,
            year_month,
            as_of_snapshot_id=as_of_snapshot_id,
        )

        if bands:
            metadata_list = [m for m in metadata_list if m.band in bands]

        # Return reference paths (actual data is in Iceberg Parquet files)
        chunk_paths = {m.band: m.chunk_path for m in metadata_list}
        return chunk_paths

    def get_tile_bounds(
        self,
        tile_id: str,
        as_of_snapshot_id: int | None = None,
    ) -> tuple[float, float, float, float] | None:
        """
        Get geographic bounds for a tile

        Args:
            tile_id: Tile identifier
            as_of_snapshot_id: Query at specific snapshot (Time Travel)

        Returns:
            Tuple of (minx, miny, maxx, maxy) or None if not found

        Examples:
            >>> catalog.get_tile_bounds("x0024_y0041")
            (126.5, 37.0, 127.5, 38.0)
        """
        metadata_list = self.query_metadata(
            tile_id,
            as_of_snapshot_id=as_of_snapshot_id,
        )

        for m in metadata_list:
            if m.bounds:
                return m.bounds

        return None

    def get_statistics(
        self,
        tile_id: str,
        band: str,
        time_range: tuple[datetime, datetime] | None = None,
        as_of_snapshot_id: int | None = None,
    ) -> dict[str, float]:
        """
        Get statistics for a tile-band combination

        Args:
            tile_id: Tile identifier
            band: Band name
            time_range: Optional time range filter
            as_of_snapshot_id: Query at specific snapshot (Time Travel)

        Returns:
            Dictionary with min, max, mean, cloud_cover

        Examples:
            >>> stats = catalog.get_statistics("x0024_y0041", "red")
            >>> stats
            {'min': 100.0, 'max': 5000.0, 'mean': 2500.0, 'cloud_cover': 0.1}
        """
        # Get metadata with optional time filter
        year_months = None
        if time_range:
            start, end = time_range
            # Generate all year_months in range
            year_months = []
            current = start.replace(day=1)
            while current <= end:
                year_months.append(current.strftime("%Y-%m"))
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)

        # Query metadata
        if year_months:
            all_metadata = []
            for ym in year_months:
                metadata = self.query_metadata(
                    tile_id,
                    year_month=ym,
                    band=band,
                    as_of_snapshot_id=as_of_snapshot_id,
                )
                all_metadata.extend(metadata)
            metadata_list = all_metadata
        else:
            metadata_list = self.query_metadata(
                tile_id,
                band=band,
                as_of_snapshot_id=as_of_snapshot_id,
            )

        if not metadata_list:
            return {}

        return {
            "min": min(m.min_value for m in metadata_list),
            "max": max(m.max_value for m in metadata_list),
            "mean": sum(m.mean_value for m in metadata_list) / len(metadata_list),
            "cloud_cover": sum(m.cloud_cover for m in metadata_list) / len(metadata_list),
            "num_observations": sum(m.num_observations for m in metadata_list),
        }

    def get_snapshot_history(self) -> list[dict[str, Any]]:
        """
        Get snapshot history for Time Travel

        Returns:
            List of snapshot dictionaries with:
            - snapshot_id: int
            - timestamp: datetime
            - operation: str (append, overwrite)
            - summary: dict

        Examples:
            >>> history = catalog.get_snapshot_history()
            >>> history[0]['snapshot_id']
            12345
        """
        # Refresh table to see latest snapshots
        self.storage.table.refresh()
        return self.storage.get_snapshot_history()

    def get_current_snapshot_id(self) -> int | None:
        """Get the current snapshot ID."""
        # Refresh table to see latest snapshot
        self.storage.table.refresh()
        return self.storage.get_current_snapshot_id()

    def get_table_stats(self) -> dict[str, Any]:
        """
        Get table statistics

        Returns:
            Dictionary with table info
        """
        # Refresh table to see latest stats
        self.storage.table.refresh()
        return self.storage.get_table_stats()

    def __repr__(self) -> str:
        """String representation"""
        exists_str = "exists" if self.exists() else "not initialized"
        snapshot_id = self.get_current_snapshot_id()
        return (
            f"<IcebergCatalog>\n"
            f"Warehouse: {self.warehouse_path}\n"
            f"Catalog: {exists_str}\n"
            f"Current Snapshot: {snapshot_id}"
        )
