"""
Sample data generator for PixelQuery tutorials.

Creates small synthetic COG files that simulate multi-temporal
satellite imagery for quick-start demonstrations.
"""

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Sample scene parameters
_SAMPLE_SCENES: list[dict[str, Any]] = [
    {
        "filename": "2024-01-15_sample_sr.tif",
        "date": "2024-01-15",
        "season": "winter",
        # Lower vegetation signal in winter
        "band_scales": {"blue": 0.12, "green": 0.10, "red": 0.09, "nir": 0.15},
    },
    {
        "filename": "2024-06-15_sample_sr.tif",
        "date": "2024-06-15",
        "season": "summer",
        # High vegetation signal in summer
        "band_scales": {"blue": 0.05, "green": 0.08, "red": 0.04, "nir": 0.35},
    },
    {
        "filename": "2024-10-15_sample_sr.tif",
        "date": "2024-10-15",
        "season": "autumn",
        # Medium vegetation signal
        "band_scales": {"blue": 0.08, "green": 0.09, "red": 0.07, "nir": 0.22},
    },
]


def create_sample_data(output_dir: str | None = None) -> str:
    """
    Create sample COG files for the quick-start tutorial.

    Generates 3 synthetic 4-band COGs (64x64 pixels) simulating a small area
    (near Seoul, South Korea) over 3 seasons. Files are ~50KB each.

    The synthetic data has realistic band ratios so NDVI calculations
    produce meaningful seasonal patterns.

    Args:
        output_dir: Directory to write COGs. If None, uses a temp directory.

    Returns:
        Path to the directory containing sample COGs.

    Examples:
        >>> from pixelquery.sample_data import create_sample_data
        >>> cog_dir = create_sample_data()
        >>> import os; os.listdir(cog_dir)
        ['2024-01-15_sample_sr.tif', '2024-06-15_sample_sr.tif', '2024-10-15_sample_sr.tif']
    """
    import numpy as np

    try:
        import rasterio
        from rasterio.crs import CRS
        from rasterio.transform import from_bounds
    except ImportError as e:
        raise ImportError(
            "rasterio is required for sample data generation. "
            "Install with: pip install pixelquery[icechunk]"
        ) from e

    if output_dir is None:
        import tempfile

        output_dir = tempfile.mkdtemp(prefix="pixelquery_sample_")

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # Sample area: small region near Seoul (EPSG:32652 - UTM 52N)
    # ~192m x 192m area (64 pixels * 3m resolution)
    west, south = 322000.0, 4150000.0
    east, north = west + 192.0, south + 192.0

    width, height = 64, 64
    transform = from_bounds(west, south, east, north, width, height)
    crs = CRS.from_epsg(32652)

    rng = np.random.default_rng(42)  # Reproducible

    for scene in _SAMPLE_SCENES:
        filepath: Path = out_path / scene["filename"]

        # Generate synthetic reflectance data
        bands = []
        for band_name in ["blue", "green", "red", "nir"]:
            scale = scene["band_scales"][band_name]
            # Base reflectance + spatial variation + noise
            base = np.full((height, width), scale * 10000, dtype=np.float64)
            # Add gentle spatial gradient (simulates field patterns)
            gradient_y = np.linspace(0.9, 1.1, height)[:, None]
            gradient_x = np.linspace(0.95, 1.05, width)[None, :]
            base *= gradient_y * gradient_x
            # Add noise
            noise = rng.normal(0, scale * 500, (height, width))
            band_data = np.clip(base + noise, 0, 10000).astype(np.uint16)
            bands.append(band_data)

        # Write as COG
        profile = {
            "driver": "GTiff",
            "dtype": "uint16",
            "width": width,
            "height": height,
            "count": 4,
            "crs": crs,
            "transform": transform,
            "compress": "deflate",
            "tiled": True,
            "blockxsize": 64,
            "blockysize": 64,
        }

        with rasterio.open(str(filepath), "w", **profile) as dst:
            for i, band_data in enumerate(bands, 1):
                dst.write(band_data, i)
            dst.update_tags(ns="rio_overview", resampling="nearest")

        logger.debug("Created sample COG: %s", filepath)

    logger.info("Created %d sample COGs in %s", len(_SAMPLE_SCENES), output_dir)
    return str(out_path)


def get_sample_data_path() -> str:
    """
    Get path to sample data, creating it if needed.

    Uses a cached location in the user's temp directory.
    Regenerates if the cache doesn't exist.

    Returns:
        Path to directory containing sample COGs.
    """
    import tempfile

    cache_dir = Path(tempfile.gettempdir()) / "pixelquery_sample_data"

    # Check if cache exists and has all files
    expected_files: list[str] = [s["filename"] for s in _SAMPLE_SCENES]
    if cache_dir.exists() and all((cache_dir / f).exists() for f in expected_files):
        return str(cache_dir)

    return create_sample_data(str(cache_dir))
