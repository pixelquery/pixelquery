"""
COG metadata extraction utilities.

Read metadata from Cloud-Optimized GeoTIFF files without loading pixel data.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class COGMetadata:
    """Metadata extracted from a COG file header."""

    path: str
    crs: str | None
    bounds: tuple[float, float, float, float] | None
    width: int
    height: int
    band_count: int
    dtype: str
    resolution: tuple[float, float] | None
    nodata: float | None
    driver: str

    def __repr__(self):
        return (
            f"<COGMetadata: {Path(self.path).name}>\n"
            f"  Size: {self.width}x{self.height}, {self.band_count} bands\n"
            f"  CRS: {self.crs}\n"
            f"  Bounds: {self.bounds}\n"
            f"  Resolution: {self.resolution}\n"
            f"  Dtype: {self.dtype}"
        )


def inspect_cog(path: str) -> COGMetadata:
    """
    Extract metadata from a COG file without reading pixel data.

    Args:
        path: Path to COG/GeoTIFF file

    Returns:
        COGMetadata with CRS, bounds, dimensions, band count, etc.

    Examples:
        >>> meta = pq.inspect_cog("./scene.tif")
        >>> print(meta.crs)       # "EPSG:32652"
        >>> print(meta.bounds)    # (127.0, 35.0, 127.5, 35.5)
        >>> print(meta.band_count) # 4
    """
    import rasterio

    with rasterio.open(path) as src:
        crs_str = str(src.crs) if src.crs else None
        bounds = tuple(src.bounds) if src.bounds else None
        res = src.res if src.res else None

        return COGMetadata(
            path=str(path),
            crs=crs_str,
            bounds=bounds,
            width=src.width,
            height=src.height,
            band_count=src.count,
            dtype=str(src.dtypes[0]),
            resolution=res,
            nodata=src.nodata,
            driver=src.driver,
        )


def inspect_directory(
    directory: str,
    glob_pattern: str = "**/*.tif*",
) -> list[COGMetadata]:
    """
    Scan a directory and return metadata for all COG files found.

    Args:
        directory: Directory path to scan
        glob_pattern: File pattern (default: "**/*.tif*")

    Returns:
        List of COGMetadata for each file found

    Examples:
        >>> metas = pq.inspect_directory("./cogs/")
        >>> for m in metas:
        ...     print(f"{m.path}: {m.band_count} bands, {m.crs}")
    """
    dir_path = Path(directory)
    if not dir_path.is_dir():
        raise NotADirectoryError(f"Not a directory: {directory}")

    results = []
    for f in sorted(dir_path.glob(glob_pattern)):
        if f.is_file():
            try:
                meta = inspect_cog(str(f))
                results.append(meta)
            except Exception as e:
                logger.warning("Failed to inspect %s: %s", f, e)

    return results
