"""
PixelQuery Public API Functions

Top-level functions for opening datasets (xarray-inspired).

Supports two storage backends:
- Arrow IPC (legacy): Uses Arrow IPC files + GeoParquet metadata
- Iceberg (default): Uses Apache Iceberg tables with ACID transactions and Time Travel
"""

import logging
from datetime import datetime
from typing import Any

from pixelquery.core.dataarray import DataArray
from pixelquery.core.dataset import Dataset

logger = logging.getLogger(__name__)


def open_dataset(
    warehouse_path: str,
    tile_id: str,
    time_range: tuple[datetime, datetime] | None = None,
    bands: list[str] | None = None,
    as_of_snapshot_id: int | None = None,
    storage_backend: str = "auto",
    **kwargs,
) -> Dataset:
    """
    Open satellite imagery dataset for a tile (xarray.open_dataset-like)

    Args:
        warehouse_path: Path to Iceberg warehouse
        tile_id: Geographic tile identifier (e.g., "x0024_y0041")
        time_range: Optional time range filter (start_date, end_date)
        bands: Optional band filter (e.g., ["red", "nir"])
        as_of_snapshot_id: Query at specific snapshot (Time Travel)
                          Use get_snapshot_history() to list available snapshots
        storage_backend: "auto", "arrow", or "iceberg"
        **kwargs: Additional options (target_resolution, etc.)

    Returns:
        Dataset with multi-band satellite imagery

    Examples:
        >>> import pixelquery as pq
        >>>
        >>> # Open current state
        >>> ds = pq.open_dataset("warehouse", tile_id="x0024_y0041")
        >>>
        >>> # Open with filters
        >>> ds = pq.open_dataset(
        ...     "warehouse",
        ...     tile_id="x0024_y0041",
        ...     time_range=(datetime(2024,1,1), datetime(2024,12,31)),
        ...     bands=["red", "nir"]
        ... )
        >>>
        >>> # Time Travel: query historical state
        >>> snapshots = pq.get_snapshot_history("warehouse")
        >>> old_snapshot = snapshots[0]["snapshot_id"]
        >>> ds_historical = pq.open_dataset(
        ...     "warehouse", tile_id="x0024_y0041",
        ...     as_of_snapshot_id=old_snapshot
        ... )
        >>>
        >>> # Access bands
        >>> red = ds["red"]
        >>> nir = ds["nir"]
        >>>
        >>> # Compute NDVI
        >>> ndvi = (nir - red) / (nir + red)
    """
    from pathlib import Path

    from pixelquery.catalog import LocalCatalog

    # Check if Icechunk repository
    if (Path(warehouse_path) / ".icechunk").exists() and storage_backend in ("auto", "icechunk"):
        logger.info("Detected Icechunk repository, use open_xarray() for best experience")
        # Route to open_xarray and wrap result
        xr_ds = open_xarray(warehouse_path, time_range=time_range, bands=bands, **kwargs)
        return Dataset(
            tile_id=tile_id or "icechunk",
            time_range=time_range,
            bands=bands or [],
            data={"xarray": xr_ds},
            metadata={"warehouse_path": warehouse_path, "storage_backend": "icechunk"},
        )

    # Auto-detect or use specified backend
    catalog = LocalCatalog.create(warehouse_path, backend=storage_backend)

    # Determine if Iceberg backend
    use_iceberg = (Path(warehouse_path) / "catalog.db").exists() or storage_backend == "iceberg"

    if use_iceberg:
        # Use Iceberg reader with Time Travel support
        from pixelquery.io.iceberg_reader import IcebergPixelReader

        reader = IcebergPixelReader(warehouse_path)

        # Read data with optional Time Travel
        data = reader.read_tile(
            tile_id=tile_id,
            bands=bands,
            time_range=time_range,
            as_of_snapshot_id=as_of_snapshot_id,
        )

        # Convert to Dataset format
        dataset_data = {}
        for band_name, band_data in data.items():
            dataset_data[band_name] = band_data

        return Dataset(
            tile_id=tile_id,
            time_range=time_range,
            bands=list(data.keys()),
            data=dataset_data,
            metadata={
                "warehouse_path": warehouse_path,
                "storage_backend": "iceberg",
                "snapshot_id": as_of_snapshot_id or reader.get_current_snapshot_id(),
            },
        )
    else:
        # Use Arrow backend
        from pixelquery._internal.storage.arrow_chunk import ArrowChunkReader

        reader = ArrowChunkReader()

        # Query catalog for chunk paths
        all_bands = bands or catalog.list_bands(tile_id=tile_id)

        dataset_data = {}
        for band in all_bands:
            # Get all year_months for this tile/band
            metadata_list = catalog.query_metadata(tile_id, band=band)

            if time_range:
                start, end = time_range
                start_ym = start.strftime("%Y-%m")
                end_ym = end.strftime("%Y-%m")
                metadata_list = [m for m in metadata_list if start_ym <= m.year_month <= end_ym]

            # Read and merge chunks
            times_list = []
            pixels_list = []
            masks_list = []

            for meta in metadata_list:
                chunk_path = Path(warehouse_path) / meta.chunk_path
                if chunk_path.exists():
                    chunk_data, _chunk_meta = reader.read_chunk(str(chunk_path))
                    times_list.extend(chunk_data.get("time", []))
                    pixels_list.extend(chunk_data.get("pixels", []))
                    masks_list.extend(chunk_data.get("mask", []))

            if times_list:
                dataset_data[band] = {
                    "times": times_list,
                    "pixels": pixels_list,
                    "masks": masks_list,
                }

        return Dataset(
            tile_id=tile_id,
            time_range=time_range,
            bands=list(dataset_data.keys()),
            data=dataset_data,
            metadata={
                "warehouse_path": warehouse_path,
                "storage_backend": "arrow",
            },
        )


def open_xarray(
    repo_path: str,
    time_range: tuple[datetime, datetime] | None = None,
    bounds: tuple[float, float, float, float] | None = None,
    bands: list[str] | None = None,
    product_id: str | None = None,
    snapshot_id: str | None = None,
    cloud_mask: bool = False,
    **kwargs,
):
    """
    Open Icechunk virtual zarr datasets as a lazy xarray.Dataset.

    This is the primary query interface for Icechunk-backed warehouses.
    Returns a lazy xarray.Dataset with dimensions (time, band, y, x).
    Actual COG byte reads happen only when .compute() or .values is called.

    Args:
        repo_path: Path to the Icechunk repository root directory
        time_range: (start, end) datetime range filter
        bounds: (minx, miny, maxx, maxy) spatial intersection filter
        bands: Band name filter (e.g., ["red", "nir"])
        product_id: Product identifier filter
        snapshot_id: Icechunk snapshot ID for Time Travel queries
        **kwargs: Passed to IcechunkStorageManager (vcc_prefix, vcc_data_path, etc.)

    Returns:
        xr.Dataset with dims (time, band, y, x) if multiple scenes,
        or (band, y, x) if single scene

    Raises:
        ValueError: If no scenes match the query filters
        ImportError: If icechunk dependencies are not installed

    Examples:
        >>> import pixelquery as pq
        >>>
        >>> # Open all scenes
        >>> ds = pq.open_xarray("./warehouse")
        >>> print(ds)  # (time: 243, band: 4, y: 874, x: 3519)
        >>>
        >>> # NDVI computation
        >>> ndvi = (ds["data"].sel(band="nir") - ds["data"].sel(band="red")) / \\
        ...        (ds["data"].sel(band="nir") + ds["data"].sel(band="red"))
        >>>
        >>> # Time Travel
        >>> ds_old = pq.open_xarray("./warehouse", snapshot_id="abc123")
        >>>
        >>> # Filter by time range and bands
        >>> from datetime import datetime
        >>> ds = pq.open_xarray(
        ...     "./warehouse",
        ...     time_range=(datetime(2025, 1, 1), datetime(2025, 6, 30)),
        ...     bands=["red", "nir"],
        ... )
    """
    try:
        from pixelquery._internal.storage.icechunk_storage import IcechunkStorageManager
        from pixelquery.io.icechunk_reader import IcechunkVirtualReader
    except ImportError as e:
        raise ImportError(
            "Icechunk dependencies not installed. Install with: pip install pixelquery[icechunk]"
        ) from e

    # Extract storage manager kwargs
    sm_kwargs = {}
    for key in ("vcc_prefix", "vcc_data_path", "storage_type", "storage_config"):
        if key in kwargs:
            sm_kwargs[key] = kwargs.pop(key)

    storage = IcechunkStorageManager(repo_path, **sm_kwargs)
    storage.initialize()

    reader = IcechunkVirtualReader(storage)
    return reader.open_xarray(
        time_range=time_range,
        bounds=bounds,
        bands=bands,
        product_id=product_id,
        snapshot_id=snapshot_id,
        cloud_mask=cloud_mask,
    )


def open_mfdataset(
    warehouse_path: str,
    tile_ids: list[str],
    time_range: tuple[datetime, datetime] | None = None,
    bands: list[str] | None = None,
    **kwargs,
) -> Dataset:
    """
    Open multiple tiles as a single dataset (xarray.open_mfdataset-like)

    Args:
        warehouse_path: Path to Iceberg warehouse
        tile_ids: List of tile identifiers
        time_range: Optional time range filter
        bands: Optional band filter
        **kwargs: Additional options

    Returns:
        Dataset mosaicking multiple tiles

    Examples:
        >>> # Open multiple tiles
        >>> ds = pq.open_mfdataset(
        ...     "warehouse",
        ...     tile_ids=["x0024_y0041", "x0025_y0041"],
        ...     bands=["red", "nir"]
        ... )
    """
    raise NotImplementedError("open_mfdataset() will be implemented in Phase 3+")


def list_tiles(
    warehouse_path: str,
    bounds: tuple[float, float, float, float] | None = None,
    time_range: tuple[datetime, datetime] | None = None,
    as_of_snapshot_id: int | None = None,
    storage_backend: str = "auto",
) -> list[str]:
    """
    List available tiles

    Args:
        warehouse_path: Path to Iceberg warehouse
        bounds: Geographic bounding box (minx, miny, maxx, maxy)
        time_range: Temporal range filter
        as_of_snapshot_id: Query at specific snapshot (Time Travel)
        storage_backend: "auto", "arrow", or "iceberg"

    Returns:
        List of tile IDs

    Examples:
        >>> # List all tiles
        >>> tiles = pq.list_tiles("warehouse")
        >>> tiles
        ['x0024_y0041', 'x0024_y0042', ...]

        >>> # List tiles at historical snapshot
        >>> tiles = pq.list_tiles("warehouse", as_of_snapshot_id=12345)
    """
    from pixelquery.catalog import LocalCatalog

    catalog = LocalCatalog.create(warehouse_path, backend=storage_backend)

    # Check if Iceberg catalog with Time Travel support
    if hasattr(catalog, "list_tiles"):
        if as_of_snapshot_id and hasattr(catalog, "get_snapshot_history"):
            # IcebergCatalog with Time Travel
            return catalog.list_tiles(
                bounds=bounds,
                time_range=time_range,
                as_of_snapshot_id=as_of_snapshot_id,
            )
        else:
            return catalog.list_tiles(bounds=bounds, time_range=time_range)

    return []


def get_snapshot_history(
    warehouse_path: str,
    storage_backend: str = "auto",
) -> list[dict[str, Any]]:
    """
    Get snapshot history for Time Travel queries

    Only available for Iceberg backend.

    Args:
        warehouse_path: Path to warehouse
        storage_backend: "auto", "arrow", or "iceberg"

    Returns:
        List of snapshot dictionaries with:
        - snapshot_id: int
        - timestamp: datetime
        - operation: str (append, overwrite)
        - summary: dict

    Examples:
        >>> # Get snapshot history
        >>> snapshots = pq.get_snapshot_history("warehouse")
        >>> for snap in snapshots:
        ...     print(f"{snap['timestamp']}: {snap['snapshot_id']}")

        >>> # Use snapshot for Time Travel
        >>> old_snapshot = snapshots[0]["snapshot_id"]
        >>> ds = pq.open_dataset("warehouse", "x0024_y0041", as_of_snapshot_id=old_snapshot)
    """
    from pixelquery.catalog import LocalCatalog

    catalog = LocalCatalog.create(warehouse_path, backend=storage_backend)

    if hasattr(catalog, "get_snapshot_history"):
        return catalog.get_snapshot_history()
    else:
        logger.warning("Snapshot history only available for Iceberg backend")
        return []


def get_current_snapshot_id(
    warehouse_path: str,
    storage_backend: str = "auto",
) -> int | None:
    """
    Get the current snapshot ID

    Only available for Iceberg backend.

    Args:
        warehouse_path: Path to warehouse
        storage_backend: "auto", "arrow", or "iceberg"

    Returns:
        Current snapshot ID, or None if not available

    Examples:
        >>> current = pq.get_current_snapshot_id("warehouse")
        >>> print(f"Current snapshot: {current}")
    """
    from pixelquery.catalog import LocalCatalog

    catalog = LocalCatalog.create(warehouse_path, backend=storage_backend)

    if hasattr(catalog, "get_current_snapshot_id"):
        return catalog.get_current_snapshot_id()
    else:
        return None


# Utility functions
def compute_ndvi(red: DataArray, nir: DataArray) -> DataArray:
    """
    Compute NDVI (Normalized Difference Vegetation Index)

    Args:
        red: Red band DataArray
        nir: NIR band DataArray

    Returns:
        NDVI DataArray

    Examples:
        >>> ds = pq.open_dataset("warehouse", tile_id="x0024_y0041")
        >>> ndvi = pq.compute_ndvi(ds["red"], ds["nir"])
    """
    return (nir - red) / (nir + red)


def compute_evi(
    blue: DataArray,
    red: DataArray,
    nir: DataArray,
    G: float = 2.5,
    C1: float = 6.0,
    C2: float = 7.5,
    L: float = 1.0,
) -> DataArray:
    """
    Compute EVI (Enhanced Vegetation Index)

    Args:
        blue: Blue band DataArray
        red: Red band DataArray
        nir: NIR band DataArray
        G: Gain factor (default: 2.5)
        C1: Coefficient for aerosol resistance (default: 6.0)
        C2: Coefficient for aerosol resistance (default: 7.5)
        L: Canopy background adjustment (default: 1.0)

    Returns:
        EVI DataArray

    Formula:
        EVI = G * ((NIR - RED) / (NIR + C1*RED - C2*BLUE + L))
    """
    return G * ((nir - red) / (nir + C1 * red - C2 * blue + L))
