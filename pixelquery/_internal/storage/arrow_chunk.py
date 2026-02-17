"""
Arrow IPC Chunk Storage

Implements monthly spatiotemporal chunk storage using Arrow IPC format.

NOTE: With Iceberg integration, this module is retained for backwards compatibility
with existing Arrow-based warehouses. New warehouses should use Iceberg storage
via IcebergStorageManager.

Performance: Uses Rust extensions when available for 2-35x faster I/O.
"""

from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
from numpy.typing import NDArray

# Try to use Rust Arrow functions (2-35x faster than Python)
try:
    from pixelquery_core import (  # type: ignore[attr-defined]
        arrow_append_to_chunk,
        arrow_write_chunk,
    )

    RUST_ARROW_AVAILABLE = True
except ImportError:
    RUST_ARROW_AVAILABLE = False


class ArrowChunkWriter:
    """
    Arrow IPC chunk writer for monthly spatiotemporal data

    Each chunk represents a single tile-month-band combination:
    - Tile: Geographic tile (e.g., x0024_y0041)
    - Month: Temporal partition (e.g., 2024-01)
    - Band: Spectral band (e.g., red, nir)

    File naming convention:
        {warehouse}/{table}/data/{tile_id}/{year}-{month:02d}/{band}.arrow

    Arrow Schema:
        - time: timestamp[ms] - Observation timestamps
        - pixels: list<uint16> - Variable-length pixel arrays (multi-resolution support)
        - mask: list<bool> - Cloud/invalid pixel masks
        - metadata: map<string, string> - Additional metadata

    Examples:
        >>> writer = ArrowChunkWriter()
        >>> data = {
        ...     'time': [datetime(2024, 1, 1), datetime(2024, 1, 15)],
        ...     'pixels': [np.array([1000, 1100, ...]), np.array([1050, 1150, ...])],
        ...     'mask': [np.array([False, False, ...]), np.array([True, False, ...])]
        ... }
        >>> writer.write_chunk(
        ...     path="warehouse/table/data/x0024_y0041/2024-01/red.arrow",
        ...     data=data,
        ...     product_id="sentinel2_l2a",
        ...     resolution=10.0
        ... )
    """

    # Arrow schema for spatiotemporal chunks
    SCHEMA = pa.schema(
        [
            ("time", pa.timestamp("ms", tz="UTC")),
            ("pixels", pa.list_(pa.uint16())),  # Variable-length arrays
            ("mask", pa.list_(pa.bool_())),  # Cloud/invalid mask
        ]
    )

    def write_chunk(
        self,
        path: str,
        data: dict[str, list],
        product_id: str,
        resolution: float,
        metadata: dict[str, str] | None = None,
    ) -> None:
        """
        Write spatiotemporal chunk to Arrow IPC file

        Args:
            path: Output file path
            data: Dictionary with keys 'time', 'pixels', 'mask'
            product_id: Product identifier (e.g., "sentinel2_l2a")
            resolution: Spatial resolution in meters
            metadata: Additional metadata

        Raises:
            ValueError: If data is invalid
            IOError: If write fails
        """
        # Validate input data
        self._validate_data(data)

        # Use Rust implementation if available (2-35x faster)
        if RUST_ARROW_AVAILABLE:
            self._write_chunk_rust(path, data, product_id, resolution, metadata)
            return

        # Fall back to Python implementation
        # Convert to Arrow arrays
        time_array = pa.array(data["time"], type=pa.timestamp("ms", tz="UTC"))
        pixels_array = self._convert_pixels_to_arrow(data["pixels"])
        mask_array = self._convert_mask_to_arrow(data["mask"])

        # Create record batch
        batch = pa.RecordBatch.from_arrays(
            [time_array, pixels_array, mask_array], schema=self.SCHEMA
        )

        # Prepare metadata
        chunk_metadata = {
            "product_id": product_id,
            "resolution": str(resolution),
            "num_observations": str(len(data["time"])),
            "creation_time": datetime.now(UTC).isoformat(),
        }
        if metadata:
            chunk_metadata.update(metadata)

        # Create schema with metadata
        schema_with_metadata = self.SCHEMA.with_metadata(chunk_metadata)

        # Write to file
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with pa.OSFile(path, "wb") as sink, ipc.new_file(sink, schema_with_metadata) as writer:
            writer.write_batch(batch)

    def _validate_data(self, data: dict[str, list]) -> None:
        """Validate input data structure"""
        required_keys = {"time", "pixels", "mask"}
        if not required_keys.issubset(data.keys()):
            missing = required_keys - data.keys()
            raise ValueError(f"Missing required keys: {missing}")

        n_obs = len(data["time"])
        if len(data["pixels"]) != n_obs:
            raise ValueError(f"pixels length ({len(data['pixels'])}) != time length ({n_obs})")
        if len(data["mask"]) != n_obs:
            raise ValueError(f"mask length ({len(data['mask'])}) != time length ({n_obs})")

    def _convert_pixels_to_arrow(self, pixels: list[NDArray]) -> pa.Array:
        """Convert list of numpy arrays to Arrow list array"""
        # Convert each numpy array to list
        pixels_list = [arr.astype(np.uint16).tolist() for arr in pixels]
        return pa.array(pixels_list, type=pa.list_(pa.uint16()))

    def _convert_mask_to_arrow(self, masks: list[NDArray]) -> pa.Array:
        """Convert list of mask arrays to Arrow list array"""
        masks_list = [arr.astype(bool).tolist() for arr in masks]
        return pa.array(masks_list, type=pa.list_(pa.bool_()))

    def _write_chunk_rust(
        self,
        path: str,
        data: dict[str, list],
        product_id: str,
        resolution: float,
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Write chunk using Rust implementation (2-35x faster)"""
        # Convert timestamps to milliseconds since epoch
        times_ms = [int(t.timestamp() * 1000) for t in data["time"]]

        # Convert numpy arrays to lists
        pixels_list = [arr.astype(np.uint16).flatten().tolist() for arr in data["pixels"]]
        masks_list = [arr.astype(bool).flatten().tolist() for arr in data["mask"]]

        # Prepare metadata
        rust_metadata = {
            "product_id": product_id,
            "resolution": str(resolution),
        }
        if metadata:
            rust_metadata.update(metadata)

        # Call Rust function
        arrow_write_chunk(path, times_ms, pixels_list, masks_list, rust_metadata)

    def _append_to_chunk_rust(
        self,
        path: str,
        data: dict[str, list],
        product_id: str,
        resolution: float,
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Append to chunk using Rust implementation (2-3x faster)"""
        # Convert timestamps to milliseconds since epoch
        times_ms = [int(t.timestamp() * 1000) for t in data["time"]]

        # Convert numpy arrays to lists
        pixels_list = [arr.astype(np.uint16).flatten().tolist() for arr in data["pixels"]]
        masks_list = [arr.astype(bool).flatten().tolist() for arr in data["mask"]]

        # Prepare metadata
        rust_metadata = {
            "product_id": product_id,
            "resolution": str(resolution),
        }
        if metadata:
            rust_metadata.update(metadata)

        # Call Rust function
        arrow_append_to_chunk(path, times_ms, pixels_list, masks_list, rust_metadata)

    def append_to_chunk(
        self,
        path: str,
        data: dict[str, list],
        product_id: str,
        resolution: float,
        metadata: dict[str, str] | None = None,
    ) -> None:
        """
        Append new observations to existing chunk file

        This is more efficient than reading the entire file, concatenating in Python,
        and rewriting. Uses Rust implementation when available (2-3x faster).

        Args:
            path: Chunk file path
            data: Dictionary with keys 'time', 'pixels', 'mask' (single observation each)
            product_id: Product identifier
            resolution: Spatial resolution in meters
            metadata: Additional metadata

        Raises:
            ValueError: If data is invalid
            IOError: If read/write fails
        """
        path_obj = Path(path)

        # Validate input data
        self._validate_data(data)

        # Use Rust implementation if available (2-3x faster)
        if RUST_ARROW_AVAILABLE:
            self._append_to_chunk_rust(path, data, product_id, resolution, metadata)
            return

        if not path_obj.exists():
            # First write - use regular write_chunk
            self.write_chunk(path, data, product_id, resolution, metadata)
            return

        # Read existing data as Arrow table (avoid Python conversion)
        with pa.OSFile(str(path), "rb") as source, ipc.open_file(source) as reader:
            existing_table = reader.read_all()
            # Preserve existing metadata
            existing_metadata = dict(reader.schema.metadata) if reader.schema.metadata else {}

        # Convert new data to Arrow arrays
        new_time_array = pa.array(data["time"], type=pa.timestamp("ms", tz="UTC"))
        new_pixels_array = self._convert_pixels_to_arrow(data["pixels"])
        new_mask_array = self._convert_mask_to_arrow(data["mask"])

        # Create record batch for new data
        new_batch = pa.RecordBatch.from_arrays(
            [new_time_array, new_pixels_array, new_mask_array], schema=self.SCHEMA
        )

        # Convert to table and concatenate with existing
        new_table = pa.Table.from_batches([new_batch])
        combined_table = pa.concat_tables([existing_table, new_table])

        # Update metadata
        chunk_metadata = {
            k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
            for k, v in existing_metadata.items()
        }
        chunk_metadata.update(
            {
                "product_id": product_id,
                "resolution": str(resolution),
                "num_observations": str(len(combined_table)),
                "last_updated": datetime.now(UTC).isoformat(),
            }
        )
        if metadata:
            chunk_metadata.update(metadata)

        # Create schema with updated metadata
        schema_with_metadata = self.SCHEMA.with_metadata(chunk_metadata)

        # Write combined data atomically
        # Use temporary file + rename for atomic write
        temp_path = str(path_obj) + ".tmp"
        try:
            with (
                pa.OSFile(temp_path, "wb") as sink,
                ipc.new_file(sink, schema_with_metadata) as writer,
            ):
                writer.write_table(combined_table)

            # Atomic rename
            import os

            os.replace(temp_path, str(path_obj))
        except Exception as e:
            # Clean up temp file on error
            if Path(temp_path).exists():
                Path(temp_path).unlink()
            raise e


class ArrowChunkReader:
    """
    Arrow IPC chunk reader for monthly spatiotemporal data

    Reads chunks written by ArrowChunkWriter and reconstructs
    spatiotemporal data.

    Examples:
        >>> reader = ArrowChunkReader()
        >>> data, metadata = reader.read_chunk(
        ...     "warehouse/table/data/x0024_y0041/2024-01/red.arrow"
        ... )
        >>> print(len(data['time']))  # Number of observations
        2
        >>> print(data['pixels'][0].shape)  # First observation pixel array
        (256, 256)
    """

    def read_chunk(
        self, path: str, reshape: tuple[int, int] | None = None
    ) -> tuple[dict[str, list], dict[str, str]]:
        """
        Read spatiotemporal chunk from Arrow IPC file

        Args:
            path: Input file path
            reshape: Optional (height, width) to reshape pixel arrays

        Returns:
            Tuple of (data, metadata):
            - data: Dictionary with 'time', 'pixels', 'mask'
            - metadata: Chunk metadata (product_id, resolution, etc.)

        Raises:
            FileNotFoundError: If file doesn't exist
            IOError: If read fails
        """
        if not Path(path).exists():
            raise FileNotFoundError(f"Chunk file not found: {path}")

        # Read Arrow file
        with pa.OSFile(path, "rb") as source, ipc.open_file(source) as reader:
            # Get metadata from schema
            metadata = dict(reader.schema.metadata) if reader.schema.metadata else {}
            # Decode bytes to strings
            metadata = {k.decode(): v.decode() for k, v in metadata.items()}

            # Read all batches (typically just one per chunk)
            table = reader.read_all()

        # Convert to Python objects
        data = {
            "time": table["time"].to_pylist(),
            "pixels": self._convert_pixels_from_arrow(table["pixels"], reshape),
            "mask": self._convert_mask_from_arrow(table["mask"], reshape),
        }

        return data, metadata

    def _convert_pixels_from_arrow(
        self, arrow_array: pa.Array, reshape: tuple[int, int] | None
    ) -> list[NDArray]:
        """Convert Arrow list array to list of numpy arrays"""
        pixels_list = arrow_array.to_pylist()
        arrays = [np.array(pixels, dtype=np.uint16) for pixels in pixels_list]

        if reshape:
            height, width = reshape
            arrays = [arr.reshape(height, width) for arr in arrays]

        return arrays

    def _convert_mask_from_arrow(
        self, arrow_array: pa.Array, reshape: tuple[int, int] | None
    ) -> list[NDArray]:
        """Convert Arrow list array to list of mask arrays"""
        mask_list = arrow_array.to_pylist()
        arrays = [np.array(mask, dtype=bool) for mask in mask_list]

        if reshape:
            height, width = reshape
            arrays = [arr.reshape(height, width) for arr in arrays]

        return arrays

    def read_chunk_metadata(self, path: str) -> dict[str, str]:
        """
        Read only chunk metadata without loading data

        Args:
            path: Input file path

        Returns:
            Chunk metadata dictionary
        """
        if not Path(path).exists():
            raise FileNotFoundError(f"Chunk file not found: {path}")

        with pa.OSFile(path, "rb") as source, ipc.open_file(source) as reader:
            metadata = dict(reader.schema.metadata) if reader.schema.metadata else {}
            return {k.decode(): v.decode() for k, v in metadata.items()}
