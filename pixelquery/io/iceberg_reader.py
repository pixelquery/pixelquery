"""
Iceberg Pixel Data Reader

Reads pixel data from Iceberg tables with Time Travel support.

Features:
- Partition pruning for efficient queries
- Time Travel via snapshot_id
- Temporal and spatial filtering
- Band-specific queries
"""

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import (
    And,
    EqualTo,
    GreaterThanOrEqual,
    LessThanOrEqual,
)

from pixelquery._internal.storage.iceberg_storage import IcebergStorageManager

logger = logging.getLogger(__name__)


class IcebergPixelReader:
    """
    Reads pixel data from Iceberg tables

    Supports:
    - Partition pruning for efficient queries
    - Time Travel via snapshot_id
    - Temporal and spatial filtering
    - Multi-band queries

    Examples:
        >>> reader = IcebergPixelReader("warehouse")
        >>>
        >>> # Read current data
        >>> data = reader.read_tile("x0024_y0041", bands=["red", "nir"])
        >>>
        >>> # Time Travel query
        >>> historical = reader.read_tile(
        ...     "x0024_y0041",
        ...     as_of_snapshot_id=12345
        ... )
        >>>
        >>> # Time range query
        >>> data = reader.read_tile(
        ...     "x0024_y0041",
        ...     time_range=(datetime(2024, 1, 1), datetime(2024, 3, 31))
        ... )
    """

    def __init__(
        self,
        warehouse_path: str,
        auto_initialize: bool = True,
    ):
        """
        Initialize pixel reader

        Args:
            warehouse_path: Path to warehouse directory
            auto_initialize: Whether to initialize storage immediately
        """
        self.warehouse_path = Path(warehouse_path)
        self.storage = IcebergStorageManager(str(warehouse_path))

        if auto_initialize:
            self.storage.initialize()

    def read_tile(
        self,
        tile_id: str,
        bands: list[str] | None = None,
        time_range: tuple[datetime, datetime] | None = None,
        as_of_snapshot_id: int | None = None,
        reshape: tuple[int, int] | None = None,
    ) -> dict[str, Any]:
        """
        Read pixel data for a tile

        Args:
            tile_id: Tile identifier
            bands: List of bands to read (None = all)
            time_range: Optional (start, end) filter
            as_of_snapshot_id: Query at specific snapshot (Time Travel)
            reshape: Optional (height, width) to reshape pixel arrays

        Returns:
            Dictionary with band names as keys, containing:
            - times: List of datetime
            - pixels: List of pixel arrays
            - masks: List of mask arrays
            - metadata: Dict with product_id, resolution, etc.
        """
        # Always refresh table metadata to ensure snapshot history is current
        self.storage.table.refresh()

        # Build filter expression
        filters = [EqualTo("tile_id", tile_id)]

        if bands and len(bands) == 1:
            # Single band filter uses partition pruning
            filters.append(EqualTo("band", bands[0]))

        if time_range:
            start, end = time_range
            # Ensure timezone-aware
            if start.tzinfo is None:
                start = start.replace(tzinfo=UTC)
            if end.tzinfo is None:
                end = end.replace(tzinfo=UTC)

            # Use year_month for partition pruning
            start_month = start.strftime("%Y-%m")
            end_month = end.strftime("%Y-%m")
            filters.extend(
                [
                    GreaterThanOrEqual("year_month", start_month),
                    LessThanOrEqual("year_month", end_month),
                ]
            )

        # Combine filters
        row_filter = And(*filters) if len(filters) > 1 else filters[0]

        # Build scan with optional Time Travel
        if as_of_snapshot_id:
            logger.debug(f"Time Travel query at snapshot {as_of_snapshot_id}")
            scan = self.storage.table.scan(
                snapshot_id=as_of_snapshot_id,
                row_filter=row_filter,
            )
        else:
            scan = self.storage.table.scan(row_filter=row_filter)

        # Execute scan
        arrow_table = scan.to_arrow()
        logger.debug(f"Scanned {arrow_table.num_rows} rows")

        # Post-filter for multiple bands (if not handled by partition)
        if bands and len(bands) > 1:
            band_mask = pc.is_in(arrow_table.column("band"), pa.array(bands))
            arrow_table = arrow_table.filter(band_mask)

        # Post-filter for exact time range (partition is by month)
        if time_range:
            start, end = time_range
            time_col = arrow_table.column("time")

            # Filter by exact time range
            ge_start = pc.greater_equal(
                time_col, pa.scalar(start, type=pa.timestamp("us", tz="UTC"))
            )
            le_end = pc.less_equal(time_col, pa.scalar(end, type=pa.timestamp("us", tz="UTC")))
            time_mask = pc.and_(ge_start, le_end)
            arrow_table = arrow_table.filter(time_mask)

        # Process results
        return self._process_results(arrow_table, bands, reshape)

    def _process_results(
        self,
        arrow_table: pa.Table,
        bands: list[str] | None,
        reshape: tuple[int, int] | None,
    ) -> dict[str, Any]:
        """Process Arrow table into band-keyed results.

        OPTIMIZED: Uses pandas for fast bulk conversion (50x faster than row-by-row).
        """
        if arrow_table.num_rows == 0:
            logger.warning("No data found for query")
            return {}

        # Convert to pandas once (much faster than row-by-row Arrow access)
        df = arrow_table.to_pandas()

        result = {}
        unique_bands = sorted(df["band"].unique())

        for band in unique_bands:
            if bands and band not in bands:
                continue

            # Filter for this band
            band_df = df[df["band"] == band].copy()

            # Sort by time
            band_df = band_df.sort_values("time")

            # Extract times (already Python datetime from pandas)
            times = band_df["time"].tolist()

            # Convert pixels and masks to numpy arrays
            pixels_list = []
            masks_list = []

            for pixels_raw, mask_raw in zip(band_df["pixels"], band_df["mask"], strict=False):
                pixels_arr = np.array(pixels_raw, dtype=np.uint16)
                mask_arr = np.array(mask_raw, dtype=bool)

                if reshape:
                    pixels_arr = pixels_arr.reshape(reshape)
                    mask_arr = mask_arr.reshape(reshape)
                else:
                    # Try to reshape to square
                    size = int(np.sqrt(len(pixels_arr)))
                    if size * size == len(pixels_arr):
                        pixels_arr = pixels_arr.reshape((size, size))
                        mask_arr = mask_arr.reshape((size, size))

                pixels_list.append(pixels_arr)
                masks_list.append(mask_arr)

            # Collect metadata
            product_ids = band_df["product_id"].unique().tolist()
            resolutions = band_df["resolution"].unique().tolist()

            result[band] = {
                "times": times,
                "pixels": pixels_list,
                "masks": masks_list,
                "metadata": {
                    "product_ids": product_ids,
                    "resolutions": resolutions,
                },
            }

        return result

    def read_observation(
        self,
        tile_id: str,
        band: str,
        time: datetime,
        as_of_snapshot_id: int | None = None,
        reshape: tuple[int, int] | None = None,
    ) -> dict[str, Any] | None:
        """
        Read a single observation

        Args:
            tile_id: Tile identifier
            band: Band name
            time: Exact acquisition time
            as_of_snapshot_id: Optional snapshot for Time Travel
            reshape: Optional reshape dimensions

        Returns:
            Dict with pixels, mask, and metadata, or None if not found
        """
        # Query with narrow time window
        time_range = (time, time)
        result = self.read_tile(
            tile_id=tile_id,
            bands=[band],
            time_range=time_range,
            as_of_snapshot_id=as_of_snapshot_id,
            reshape=reshape,
        )

        if not result or band not in result:
            return None

        band_data = result[band]
        if not band_data["times"]:
            return None

        # Find exact match
        for i, t in enumerate(band_data["times"]):
            if t == time or (t.replace(tzinfo=None) == time.replace(tzinfo=None)):
                return {
                    "time": t,
                    "pixels": band_data["pixels"][i],
                    "mask": band_data["masks"][i],
                    "metadata": band_data["metadata"],
                }

        return None

    def list_tiles(
        self,
        time_range: tuple[datetime, datetime] | None = None,
        as_of_snapshot_id: int | None = None,
    ) -> list[str]:
        """
        List all unique tile IDs

        Args:
            time_range: Optional temporal filter
            as_of_snapshot_id: Query at specific snapshot

        Returns:
            Sorted list of unique tile IDs
        """
        # Always refresh table metadata to ensure snapshot history is current
        self.storage.table.refresh()

        filters = []

        if time_range:
            start, end = time_range
            filters.extend(
                [
                    GreaterThanOrEqual("year_month", start.strftime("%Y-%m")),
                    LessThanOrEqual("year_month", end.strftime("%Y-%m")),
                ]
            )

        # Build row filter
        if len(filters) == 1:
            row_filter = filters[0]
        elif len(filters) > 1:
            row_filter = And(*filters)
        else:
            row_filter = None

        # Scan with only tile_id column
        if as_of_snapshot_id:
            if row_filter is not None:
                scan = self.storage.table.scan(
                    snapshot_id=as_of_snapshot_id,
                    row_filter=row_filter,
                    selected_fields=["tile_id"],
                )
            else:
                scan = self.storage.table.scan(
                    snapshot_id=as_of_snapshot_id,
                    selected_fields=["tile_id"],
                )
        else:
            if row_filter is not None:
                scan = self.storage.table.scan(
                    row_filter=row_filter,
                    selected_fields=["tile_id"],
                )
            else:
                scan = self.storage.table.scan(
                    selected_fields=["tile_id"],
                )

        arrow_table = scan.to_arrow()
        if arrow_table.num_rows == 0:
            return []

        # Get unique tile IDs
        unique_tiles = pc.unique(arrow_table.column("tile_id")).to_pylist()
        return sorted(unique_tiles)

    def list_bands(
        self,
        tile_id: str | None = None,
        as_of_snapshot_id: int | None = None,
    ) -> list[str]:
        """
        List all unique band names

        Args:
            tile_id: Optional filter by tile
            as_of_snapshot_id: Query at specific snapshot

        Returns:
            Sorted list of unique band names
        """
        # Always refresh table metadata to ensure snapshot history is current
        self.storage.table.refresh()

        filters = []
        if tile_id:
            filters.append(EqualTo("tile_id", tile_id))

        # Build row filter
        if len(filters) == 1:
            row_filter = filters[0]
        elif len(filters) > 1:
            row_filter = And(*filters)
        else:
            row_filter = None

        if as_of_snapshot_id:
            if row_filter is not None:
                scan = self.storage.table.scan(
                    snapshot_id=as_of_snapshot_id,
                    row_filter=row_filter,
                    selected_fields=["band"],
                )
            else:
                scan = self.storage.table.scan(
                    snapshot_id=as_of_snapshot_id,
                    selected_fields=["band"],
                )
        else:
            if row_filter is not None:
                scan = self.storage.table.scan(
                    row_filter=row_filter,
                    selected_fields=["band"],
                )
            else:
                scan = self.storage.table.scan(
                    selected_fields=["band"],
                )

        arrow_table = scan.to_arrow()
        if arrow_table.num_rows == 0:
            return []

        unique_bands = pc.unique(arrow_table.column("band")).to_pylist()
        return sorted(unique_bands)

    def list_time_range(
        self,
        tile_id: str | None = None,
        band: str | None = None,
        as_of_snapshot_id: int | None = None,
    ) -> tuple[datetime, datetime] | None:
        """
        Get the time range of available data

        Args:
            tile_id: Optional filter by tile
            band: Optional filter by band
            as_of_snapshot_id: Query at specific snapshot

        Returns:
            Tuple of (earliest, latest) datetime, or None if no data
        """
        # Always refresh table metadata to ensure snapshot history is current
        self.storage.table.refresh()

        filters = []
        if tile_id:
            filters.append(EqualTo("tile_id", tile_id))
        if band:
            filters.append(EqualTo("band", band))

        # Build row filter
        if len(filters) == 1:
            row_filter = filters[0]
        elif len(filters) > 1:
            row_filter = And(*filters)
        else:
            row_filter = None

        if as_of_snapshot_id:
            if row_filter is not None:
                scan = self.storage.table.scan(
                    snapshot_id=as_of_snapshot_id,
                    row_filter=row_filter,
                    selected_fields=["time"],
                )
            else:
                scan = self.storage.table.scan(
                    snapshot_id=as_of_snapshot_id,
                    selected_fields=["time"],
                )
        else:
            if row_filter is not None:
                scan = self.storage.table.scan(
                    row_filter=row_filter,
                    selected_fields=["time"],
                )
            else:
                scan = self.storage.table.scan(
                    selected_fields=["time"],
                )

        arrow_table = scan.to_arrow()
        if arrow_table.num_rows == 0:
            return None

        time_col = arrow_table.column("time")
        min_time = pc.min(time_col).as_py()
        max_time = pc.max(time_col).as_py()

        return (min_time, max_time)

    def count_observations(
        self,
        tile_id: str | None = None,
        band: str | None = None,
        time_range: tuple[datetime, datetime] | None = None,
        as_of_snapshot_id: int | None = None,
    ) -> int:
        """
        Count observations matching criteria

        Args:
            tile_id: Optional filter by tile
            band: Optional filter by band
            time_range: Optional temporal filter
            as_of_snapshot_id: Query at specific snapshot

        Returns:
            Number of matching observations
        """
        # Always refresh table metadata to ensure snapshot history is current
        self.storage.table.refresh()

        filters = []
        if tile_id:
            filters.append(EqualTo("tile_id", tile_id))
        if band:
            filters.append(EqualTo("band", band))
        if time_range:
            start, end = time_range
            filters.extend(
                [
                    GreaterThanOrEqual("year_month", start.strftime("%Y-%m")),
                    LessThanOrEqual("year_month", end.strftime("%Y-%m")),
                ]
            )

        # Build row filter
        if len(filters) == 1:
            row_filter = filters[0]
        elif len(filters) > 1:
            row_filter = And(*filters)
        else:
            row_filter = None

        if as_of_snapshot_id:
            if row_filter is not None:
                scan = self.storage.table.scan(
                    snapshot_id=as_of_snapshot_id,
                    row_filter=row_filter,
                    selected_fields=["tile_id"],  # Minimal column
                )
            else:
                scan = self.storage.table.scan(
                    snapshot_id=as_of_snapshot_id,
                    selected_fields=["tile_id"],
                )
        else:
            if row_filter is not None:
                scan = self.storage.table.scan(
                    row_filter=row_filter,
                    selected_fields=["tile_id"],
                )
            else:
                scan = self.storage.table.scan(
                    selected_fields=["tile_id"],
                )

        return scan.to_arrow().num_rows

    def get_snapshot_history(self) -> list[dict[str, Any]]:
        """Get snapshot history for Time Travel."""
        return self.storage.get_snapshot_history()

    def get_current_snapshot_id(self) -> int | None:
        """Get the current snapshot ID."""
        return self.storage.get_current_snapshot_id()


def create_reader(warehouse_path: str) -> IcebergPixelReader:
    """
    Factory function to create pixel reader

    Args:
        warehouse_path: Path to warehouse directory

    Returns:
        Initialized IcebergPixelReader
    """
    return IcebergPixelReader(warehouse_path)
