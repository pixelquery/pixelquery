"""
Arrow IPC Chunk Storage

Implements monthly spatiotemporal chunk storage using Arrow IPC format.
"""

from typing import Dict, List, Optional, Tuple
from pathlib import Path
from datetime import datetime
import pyarrow as pa
import pyarrow.ipc as ipc
import numpy as np
from numpy.typing import NDArray
import warnings

# Try to import Rust Arrow functions (10x faster)
try:
    from pixelquery_core import arrow_write_chunk, arrow_append_to_chunk
    RUST_ARROW_AVAILABLE = True
except ImportError:
    RUST_ARROW_AVAILABLE = False
    warnings.warn(
        "Rust Arrow I/O not available. Using pure Python (10x slower). "
        "Run 'pip install maturin && cd pixelquery_core && maturin develop --release' "
        "to enable Rust optimizations.",
        category=UserWarning,
        stacklevel=2
    )


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
    SCHEMA = pa.schema([
        ('time', pa.timestamp('ms', tz='UTC')),
        ('pixels', pa.list_(pa.uint16())),  # Variable-length arrays
        ('mask', pa.list_(pa.bool_())),      # Cloud/invalid mask
    ])

    def write_chunk(
        self,
        path: str,
        data: Dict[str, List],
        product_id: str,
        resolution: float,
        metadata: Optional[Dict[str, str]] = None
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

        if RUST_ARROW_AVAILABLE:
            # Rust path (10x faster)
            Path(path).parent.mkdir(parents=True, exist_ok=True)

            # Convert datetime to milliseconds
            times_ms = [int(dt.timestamp() * 1000) for dt in data['time']]

            # Convert numpy arrays to lists
            pixels_list = [arr.astype(np.uint16).tolist() for arr in data['pixels']]
            masks_list = [arr.astype(bool).tolist() for arr in data['mask']]

            # Prepare metadata
            from datetime import timezone
            chunk_metadata = {
                'product_id': product_id,
                'resolution': str(resolution),
                'num_observations': str(len(data['time'])),
                'creation_time': datetime.now(timezone.utc).isoformat(),
            }
            if metadata:
                chunk_metadata.update(metadata)

            # Call Rust function
            arrow_write_chunk(path, times_ms, pixels_list, masks_list, chunk_metadata)
        else:
            # Python fallback
            # Convert to Arrow arrays
            time_array = pa.array(data['time'], type=pa.timestamp('ms', tz='UTC'))
            pixels_array = self._convert_pixels_to_arrow(data['pixels'])
            mask_array = self._convert_mask_to_arrow(data['mask'])

            # Create record batch
            batch = pa.RecordBatch.from_arrays(
                [time_array, pixels_array, mask_array],
                schema=self.SCHEMA
            )

            # Prepare metadata
            from datetime import timezone
            chunk_metadata = {
                'product_id': product_id,
                'resolution': str(resolution),
                'num_observations': str(len(data['time'])),
                'creation_time': datetime.now(timezone.utc).isoformat(),
            }
            if metadata:
                chunk_metadata.update(metadata)

            # Create schema with metadata
            schema_with_metadata = self.SCHEMA.with_metadata(chunk_metadata)

            # Write to file
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            with pa.OSFile(path, 'wb') as sink:
                with ipc.new_file(sink, schema_with_metadata) as writer:
                    writer.write_batch(batch)

    def _validate_data(self, data: Dict[str, List]) -> None:
        """Validate input data structure"""
        required_keys = {'time', 'pixels', 'mask'}
        if not required_keys.issubset(data.keys()):
            missing = required_keys - data.keys()
            raise ValueError(f"Missing required keys: {missing}")

        n_obs = len(data['time'])
        if len(data['pixels']) != n_obs:
            raise ValueError(f"pixels length ({len(data['pixels'])}) != time length ({n_obs})")
        if len(data['mask']) != n_obs:
            raise ValueError(f"mask length ({len(data['mask'])}) != time length ({n_obs})")

    def _convert_pixels_to_arrow(self, pixels: List[NDArray]) -> pa.Array:
        """Convert list of numpy arrays to Arrow list array"""
        # Convert each numpy array to list
        pixels_list = [arr.astype(np.uint16).tolist() for arr in pixels]
        return pa.array(pixels_list, type=pa.list_(pa.uint16()))

    def _convert_mask_to_arrow(self, masks: List[NDArray]) -> pa.Array:
        """Convert list of mask arrays to Arrow list array"""
        masks_list = [arr.astype(bool).tolist() for arr in masks]
        return pa.array(masks_list, type=pa.list_(pa.bool_()))

    def append_to_chunk(
        self,
        path: str,
        data: Dict[str, List],
        product_id: str,
        resolution: float,
        metadata: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Append new observations to existing chunk file

        This is more efficient than reading the entire file, concatenating in Python,
        and rewriting. Instead, we work with Arrow tables directly and use Arrow's
        efficient concatenation.

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

        if RUST_ARROW_AVAILABLE:
            # Rust path (10x faster) - handles both new file and append
            Path(path).parent.mkdir(parents=True, exist_ok=True)

            # Convert datetime to milliseconds
            times_ms = [int(dt.timestamp() * 1000) for dt in data['time']]

            # Convert numpy arrays to lists
            pixels_list = [arr.astype(np.uint16).tolist() for arr in data['pixels']]
            masks_list = [arr.astype(bool).tolist() for arr in data['mask']]

            # Prepare metadata
            chunk_metadata = {
                'product_id': product_id,
                'resolution': str(resolution),
            }
            if metadata:
                chunk_metadata.update(metadata)

            # Call Rust function (handles both new and append)
            arrow_append_to_chunk(path, times_ms, pixels_list, masks_list, chunk_metadata)
            return  # Done - Rust handled everything

        # Python fallback
        if not path_obj.exists():
            # First write - use regular write_chunk
            self.write_chunk(path, data, product_id, resolution, metadata)
            return

        # Read existing data as Arrow table (avoid Python conversion)
        with pa.OSFile(str(path), 'rb') as source:
            with ipc.open_file(source) as reader:
                existing_table = reader.read_all()
                # Preserve existing metadata
                existing_metadata = dict(reader.schema.metadata) if reader.schema.metadata else {}

        # Convert new data to Arrow arrays
        new_time_array = pa.array(data['time'], type=pa.timestamp('ms', tz='UTC'))
        new_pixels_array = self._convert_pixels_to_arrow(data['pixels'])
        new_mask_array = self._convert_mask_to_arrow(data['mask'])

        # Create record batch for new data
        new_batch = pa.RecordBatch.from_arrays(
            [new_time_array, new_pixels_array, new_mask_array],
            schema=self.SCHEMA
        )

        # Convert to table and concatenate with existing
        new_table = pa.Table.from_batches([new_batch])
        combined_table = pa.concat_tables([existing_table, new_table])

        # Update metadata
        from datetime import timezone
        chunk_metadata = {k.decode() if isinstance(k, bytes) else k:
                         v.decode() if isinstance(v, bytes) else v
                         for k, v in existing_metadata.items()}
        chunk_metadata.update({
            'product_id': product_id,
            'resolution': str(resolution),
            'num_observations': str(len(combined_table)),
            'last_updated': datetime.now(timezone.utc).isoformat(),
        })
        if metadata:
            chunk_metadata.update(metadata)

        # Create schema with updated metadata
        schema_with_metadata = self.SCHEMA.with_metadata(chunk_metadata)

        # Write combined data atomically
        # Use temporary file + rename for atomic write
        temp_path = str(path_obj) + '.tmp'
        try:
            with pa.OSFile(temp_path, 'wb') as sink:
                with ipc.new_file(sink, schema_with_metadata) as writer:
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
        self,
        path: str,
        reshape: Optional[Tuple[int, int]] = None
    ) -> Tuple[Dict[str, List], Dict[str, str]]:
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
        with pa.OSFile(path, 'rb') as source:
            with ipc.open_file(source) as reader:
                # Get metadata from schema
                metadata = dict(reader.schema.metadata) if reader.schema.metadata else {}
                # Decode bytes to strings
                metadata = {k.decode(): v.decode() for k, v in metadata.items()}

                # Read all batches (typically just one per chunk)
                table = reader.read_all()

        # Convert to Python objects
        data = {
            'time': table['time'].to_pylist(),
            'pixels': self._convert_pixels_from_arrow(table['pixels'], reshape),
            'mask': self._convert_mask_from_arrow(table['mask'], reshape),
        }

        return data, metadata

    def _convert_pixels_from_arrow(
        self,
        arrow_array: pa.Array,
        reshape: Optional[Tuple[int, int]]
    ) -> List[NDArray]:
        """Convert Arrow list array to list of numpy arrays"""
        pixels_list = arrow_array.to_pylist()
        arrays = [np.array(pixels, dtype=np.uint16) for pixels in pixels_list]

        if reshape:
            height, width = reshape
            arrays = [arr.reshape(height, width) for arr in arrays]

        return arrays

    def _convert_mask_from_arrow(
        self,
        arrow_array: pa.Array,
        reshape: Optional[Tuple[int, int]]
    ) -> List[NDArray]:
        """Convert Arrow list array to list of mask arrays"""
        mask_list = arrow_array.to_pylist()
        arrays = [np.array(mask, dtype=bool) for mask in mask_list]

        if reshape:
            height, width = reshape
            arrays = [arr.reshape(height, width) for arr in arrays]

        return arrays

    def read_chunk_metadata(self, path: str) -> Dict[str, str]:
        """
        Read only chunk metadata without loading data

        Args:
            path: Input file path

        Returns:
            Chunk metadata dictionary
        """
        if not Path(path).exists():
            raise FileNotFoundError(f"Chunk file not found: {path}")

        with pa.OSFile(path, 'rb') as source:
            with ipc.open_file(source) as reader:
                metadata = dict(reader.schema.metadata) if reader.schema.metadata else {}
                return {k.decode(): v.decode() for k, v in metadata.items()}
