"""
Iceberg Pixel Data Writer

Writes pixel observations to Iceberg tables with ACID guarantees.

Features:
- Atomic writes (single observation or batch)
- Automatic partitioning by tile_id, band, year_month
- Computed statistics for each observation
- Support for geometry bounds (WKB format)
"""

from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timezone
from pathlib import Path
import logging

import pyarrow as pa
import numpy as np
from numpy.typing import NDArray

from pixelquery._internal.storage.iceberg_storage import IcebergStorageManager
from pixelquery._internal.storage.iceberg_schema import TILE_SIZE_PIXELS

logger = logging.getLogger(__name__)


class IcebergPixelWriter:
    """
    Writes pixel data to Iceberg tables

    Each observation becomes a row in the Iceberg table.
    Partitioned by tile_id, band, year_month for efficient queries.

    Examples:
        >>> writer = IcebergPixelWriter("warehouse")
        >>>
        >>> # Single observation
        >>> writer.write_observation(
        ...     tile_id="x0024_y0041",
        ...     band="red",
        ...     time=datetime(2024, 1, 15),
        ...     pixels=np.array([...], dtype=np.uint16),
        ...     mask=np.array([...], dtype=bool),
        ...     product_id="sentinel2_l2a",
        ...     resolution=10.0
        ... )
        >>>
        >>> # Batch write
        >>> observations = [...]
        >>> writer.write_observations(observations)
    """

    def __init__(
        self,
        warehouse_path: str,
        auto_initialize: bool = True,
    ):
        """
        Initialize pixel writer

        Args:
            warehouse_path: Path to warehouse directory
            auto_initialize: Whether to initialize storage immediately
        """
        self.warehouse_path = Path(warehouse_path)
        self.storage = IcebergStorageManager(str(warehouse_path))

        if auto_initialize:
            self.storage.initialize()

    def write_observation(
        self,
        tile_id: str,
        band: str,
        time: datetime,
        pixels: NDArray,
        mask: NDArray,
        product_id: str,
        resolution: float,
        bounds: Optional[tuple] = None,
        cloud_cover: Optional[float] = None,
        crs: Optional[str] = None,
        transform: Optional[str] = None,
        source_file: Optional[str] = None,
    ) -> int:
        """
        Write a single observation

        Args:
            tile_id: Tile identifier (e.g., "x0024_y0041")
            band: Band name (e.g., "red", "nir")
            time: Acquisition timestamp
            pixels: Pixel data array (2D or flattened)
            mask: Valid pixel mask (same shape as pixels)
            product_id: Product identifier (e.g., "sentinel2_l2a")
            resolution: Spatial resolution in meters
            bounds: Optional bounding box (minx, miny, maxx, maxy)
            cloud_cover: Optional cloud cover percentage (0-100)
            crs: Optional CRS string (e.g., "EPSG:32632")
            transform: Optional affine transform as JSON string
            source_file: Optional source file path

        Returns:
            Snapshot ID of the commit
        """
        observation = {
            "tile_id": tile_id,
            "band": band,
            "time": time,
            "pixels": pixels,
            "mask": mask,
            "product_id": product_id,
            "resolution": resolution,
            "bounds": bounds,
            "cloud_cover": cloud_cover,
            "crs": crs,
            "transform": transform,
            "source_file": source_file,
        }

        return self.write_observations([observation])

    def write_observations(
        self,
        observations: List[Dict[str, Any]],
    ) -> int:
        """
        Write multiple observations atomically

        Args:
            observations: List of observation dictionaries with keys:
                - tile_id (str, required)
                - band (str, required)
                - time (datetime, required)
                - pixels (ndarray, required)
                - mask (ndarray, required)
                - product_id (str, required)
                - resolution (float, required)
                - bounds (tuple, optional)
                - cloud_cover (float, optional)
                - crs (str, optional)
                - transform (str, optional)
                - source_file (str, optional)

        Returns:
            Snapshot ID of the commit
        """
        if not observations:
            logger.warning("No observations to write")
            return 0

        logger.debug(f"Writing {len(observations)} observations")

        records = []
        ingestion_time = datetime.now(timezone.utc)

        for obs in observations:
            record = self._prepare_record(obs, ingestion_time)
            records.append(record)

        # Convert to PyArrow table
        arrow_table = self._create_arrow_table(records)

        # Append to Iceberg table (ACID commit)
        snapshot_id = self.storage.append_data(arrow_table)

        logger.info(f"Wrote {len(observations)} observations, snapshot_id={snapshot_id}")
        return snapshot_id

    def _prepare_record(
        self,
        obs: Dict[str, Any],
        ingestion_time: datetime,
    ) -> Dict[str, Any]:
        """Prepare a single observation record for storage."""
        pixels = obs["pixels"]
        mask = obs["mask"]
        time = obs["time"]

        # Ensure numpy arrays
        if not isinstance(pixels, np.ndarray):
            pixels = np.array(pixels)
        if not isinstance(mask, np.ndarray):
            mask = np.array(mask)

        # Flatten arrays for storage
        pixels_flat = pixels.flatten().astype(np.int32)
        mask_flat = mask.flatten().astype(bool)

        # Compute statistics on valid pixels
        valid_pixels = pixels_flat[mask_flat] if mask_flat.any() else pixels_flat
        valid_pixels = valid_pixels.astype(float)

        min_value = float(valid_pixels.min()) if len(valid_pixels) > 0 else None
        max_value = float(valid_pixels.max()) if len(valid_pixels) > 0 else None
        mean_value = float(valid_pixels.mean()) if len(valid_pixels) > 0 else None

        # Create WKB geometry if bounds provided
        bounds_wkb = None
        if obs.get("bounds"):
            try:
                from shapely.geometry import box
                bounds_wkb = box(*obs["bounds"]).wkb
            except ImportError:
                logger.warning("shapely not installed, skipping bounds geometry")

        # Ensure time is timezone-aware
        if time.tzinfo is None:
            time = time.replace(tzinfo=timezone.utc)

        # Build record
        record = {
            "tile_id": obs["tile_id"],
            "band": obs["band"],
            "year_month": time.strftime("%Y-%m"),
            "time": time,
            "pixels": pixels_flat.tolist(),
            "mask": mask_flat.tolist(),
            "product_id": obs["product_id"],
            "resolution": float(obs["resolution"]),
            "bounds_wkb": bounds_wkb,
            "num_pixels": len(pixels_flat),
            "min_value": min_value,
            "max_value": max_value,
            "mean_value": mean_value,
            "cloud_cover": obs.get("cloud_cover"),
            "crs": obs.get("crs"),
            "transform": obs.get("transform"),
            "source_file": obs.get("source_file"),
            "ingestion_time": ingestion_time,
        }

        return record

    def _create_arrow_table(self, records: List[Dict[str, Any]]) -> pa.Table:
        """Create PyArrow table from records."""
        # Define arrays
        tile_ids = [r["tile_id"] for r in records]
        bands = [r["band"] for r in records]
        year_months = [r["year_month"] for r in records]
        times = [r["time"] for r in records]
        pixels_list = [r["pixels"] for r in records]
        masks_list = [r["mask"] for r in records]
        product_ids = [r["product_id"] for r in records]
        resolutions = [r["resolution"] for r in records]
        bounds_wkbs = [r["bounds_wkb"] for r in records]
        num_pixels = [r["num_pixels"] for r in records]
        min_values = [r["min_value"] for r in records]
        max_values = [r["max_value"] for r in records]
        mean_values = [r["mean_value"] for r in records]
        cloud_covers = [r["cloud_cover"] for r in records]
        crss = [r["crs"] for r in records]
        transforms = [r["transform"] for r in records]
        source_files = [r["source_file"] for r in records]
        ingestion_times = [r["ingestion_time"] for r in records]

        # Create PyArrow arrays with explicit element nullability for lists
        # Must match Iceberg schema where list elements are required
        schema = pa.schema([
            pa.field("tile_id", pa.string(), nullable=False),
            pa.field("band", pa.string(), nullable=False),
            pa.field("year_month", pa.string(), nullable=False),
            pa.field("time", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("pixels", pa.list_(pa.field("element", pa.int32(), nullable=False)), nullable=False),
            pa.field("mask", pa.list_(pa.field("element", pa.bool_(), nullable=False)), nullable=False),
            pa.field("product_id", pa.string(), nullable=False),
            pa.field("resolution", pa.float32(), nullable=False),
            pa.field("bounds_wkb", pa.binary(), nullable=True),
            pa.field("num_pixels", pa.int32(), nullable=False),
            pa.field("min_value", pa.float32(), nullable=True),
            pa.field("max_value", pa.float32(), nullable=True),
            pa.field("mean_value", pa.float32(), nullable=True),
            pa.field("cloud_cover", pa.float32(), nullable=True),
            pa.field("crs", pa.string(), nullable=True),
            pa.field("transform", pa.string(), nullable=True),
            pa.field("source_file", pa.string(), nullable=True),
            pa.field("ingestion_time", pa.timestamp("us", tz="UTC"), nullable=True),
        ])

        arrays = [
            pa.array(tile_ids, type=pa.string()),
            pa.array(bands, type=pa.string()),
            pa.array(year_months, type=pa.string()),
            pa.array(times, type=pa.timestamp("us", tz="UTC")),
            pa.array(pixels_list, type=pa.list_(pa.int32())),
            pa.array(masks_list, type=pa.list_(pa.bool_())),
            pa.array(product_ids, type=pa.string()),
            pa.array(resolutions, type=pa.float32()),
            pa.array(bounds_wkbs, type=pa.binary()),
            pa.array(num_pixels, type=pa.int32()),
            pa.array(min_values, type=pa.float32()),
            pa.array(max_values, type=pa.float32()),
            pa.array(mean_values, type=pa.float32()),
            pa.array(cloud_covers, type=pa.float32()),
            pa.array(crss, type=pa.string()),
            pa.array(transforms, type=pa.string()),
            pa.array(source_files, type=pa.string()),
            pa.array(ingestion_times, type=pa.timestamp("us", tz="UTC")),
        ]

        return pa.Table.from_arrays(arrays, schema=schema)

    def get_snapshot_history(self) -> List[Dict[str, Any]]:
        """Get snapshot history for Time Travel."""
        return self.storage.get_snapshot_history()

    def get_current_snapshot_id(self) -> Optional[int]:
        """Get the current snapshot ID."""
        return self.storage.get_current_snapshot_id()


def create_writer(warehouse_path: str) -> IcebergPixelWriter:
    """
    Factory function to create pixel writer

    Args:
        warehouse_path: Path to warehouse directory

    Returns:
        Initialized IcebergPixelWriter
    """
    return IcebergPixelWriter(warehouse_path)
