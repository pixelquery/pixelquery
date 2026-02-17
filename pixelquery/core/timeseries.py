"""
Point time-series extraction for PixelQuery.

Extracts pixel values at a single (lon, lat) point across all time steps.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def timeseries(
    warehouse: str,
    lon: float,
    lat: float,
    bands: list[str] | None = None,
    time_range: tuple[datetime, datetime] | None = None,
    product_id: str | None = None,
    snapshot_id: str | None = None,
    **kwargs,
):
    """
    Extract a time-series for a single point (lon, lat).

    Returns a 1D time-series xarray.Dataset at the nearest pixel.

    Args:
        warehouse: Path to PixelQuery warehouse
        lon: Longitude (or X coordinate in native CRS)
        lat: Latitude (or Y coordinate in native CRS)
        bands: Band filter (e.g., ["red", "nir"])
        time_range: (start, end) datetime range filter
        product_id: Product identifier filter
        snapshot_id: Icechunk snapshot ID for Time Travel
        **kwargs: Passed to open_xarray()

    Returns:
        xr.Dataset with dims (time,) and band values at the point

    Examples:
        >>> import pixelquery as pq
        >>> ts = pq.timeseries("./warehouse", lon=127.05, lat=37.55)
        >>> ts["data"].sel(band="nir").plot()

        >>> ts = pq.timeseries(
        ...     "./warehouse", lon=127.05, lat=37.55,
        ...     bands=["red", "nir"],
        ...     time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
        ... )
    """
    from pixelquery.core.api import open_xarray

    ds = open_xarray(
        warehouse,
        time_range=time_range,
        bands=bands,
        product_id=product_id,
        snapshot_id=snapshot_id,
        **kwargs,
    )

    # Select nearest pixel to (lon, lat)
    # In most satellite data, x corresponds to lon-like and y to lat-like coords
    if ("x" in ds.dims and "y" in ds.dims) or ("x" in ds.coords and "y" in ds.coords):
        point = ds.sel(x=lon, y=lat, method="nearest")
    else:
        # Fallback: use integer indexing with a warning
        logger.warning(
            "Dataset has no x/y coordinates; using center pixel. "
            "Consider providing georeferenced COGs."
        )
        if "y" in ds.dims and "x" in ds.dims:
            mid_y = ds.sizes["y"] // 2
            mid_x = ds.sizes["x"] // 2
            point = ds.isel(y=mid_y, x=mid_x)
        else:
            point = ds

    return point
