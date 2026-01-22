"""
PixelQuery Time-Series Demo

시계열 위성영상 데이터 ingestion 및 NDVI 분석 데모

Prerequisites:
- 여러 시점의 Sentinel-2 COG 파일들 (4 bands: blue, green, red, nir)
- COG 메타데이터에 acquisition time 정보 포함
- 파일들이 한 디렉토리에 모여있음

Usage:
    python examples/demo_timeseries.py --cog-dir ./sentinel2_data --warehouse ./warehouse
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Tuple
import numpy as np

from pixelquery.io.cog import COGReader
from pixelquery.io.ingest import IngestionPipeline
from pixelquery.catalog.local import LocalCatalog
from pixelquery.query.executor import QueryExecutor
from pixelquery.grid.tile_grid import FixedTileGrid


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="PixelQuery Time-Series Demo"
    )
    parser.add_argument(
        "--cog-dir",
        type=str,
        required=True,
        help="Directory containing COG files"
    )
    parser.add_argument(
        "--warehouse",
        type=str,
        default="./warehouse",
        help="Warehouse directory path (default: ./warehouse)"
    )
    parser.add_argument(
        "--product-id",
        type=str,
        default="sentinel2_l2a",
        help="Product ID (default: sentinel2_l2a)"
    )
    parser.add_argument(
        "--band-mapping",
        type=str,
        default="1:blue,2:green,3:red,4:nir",
        help="Band mapping (default: 1:blue,2:green,3:red,4:nir)"
    )
    parser.add_argument(
        "--file-pattern",
        type=str,
        default=None,
        help="Filter files by pattern (e.g., '*_sr.tiff' for Surface Reflectance only)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of files to ingest (for testing)"
    )
    return parser.parse_args()


def extract_acquisition_time(cog_path: str) -> datetime:
    """
    Extract acquisition time from COG metadata

    Priority:
    1. TIFFTAG_DATETIME from metadata
    2. Parse from filename (YYYYMMDD pattern)
    3. Use file modification time as fallback
    """
    with COGReader(cog_path) as reader:
        # Try to get from GDAL metadata tags
        if hasattr(reader.dataset, 'tags'):
            tags = reader.dataset.tags()
            if 'TIFFTAG_DATETIME' in tags:
                # Format: "YYYY:MM:DD HH:MM:SS"
                datetime_str = tags['TIFFTAG_DATETIME']
                try:
                    return datetime.strptime(datetime_str, "%Y:%m:%d %H:%M:%S")
                except ValueError:
                    pass

    # Fallback 1: Parse from filename
    import re
    filename = Path(cog_path).name

    # Try YYYY-MM-DD pattern first (e.g., 2025-01-17_analysis_ready.tiff)
    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    if date_match:
        date_str = date_match.group(1)
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            pass

    # Try YYYYMMDD pattern (e.g., S2A_20240115.tif)
    date_match = re.search(r'(\d{8})', filename)
    if date_match:
        date_str = date_match.group(1)
        try:
            return datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            pass

    # Fallback 2: Use file modification time
    import os
    mtime = os.path.getmtime(cog_path)
    return datetime.fromtimestamp(mtime)


def find_cog_files(cog_dir: str, file_pattern: str = None, limit: int = None) -> List[Tuple[str, datetime]]:
    """
    Find all COG files in directory and extract acquisition times

    Args:
        cog_dir: Directory path containing COG files
        file_pattern: Optional filter pattern (e.g., '*_sr.tiff')
        limit: Optional limit on number of files

    Returns:
        List of (cog_path, acquisition_time) tuples, sorted by time
    """
    cog_dir_path = Path(cog_dir)

    if not cog_dir_path.exists():
        raise FileNotFoundError(f"COG directory not found: {cog_dir}")

    # Find all .tif and .tiff files (including subdirectories)
    cog_files = []

    # Search patterns: direct files and files in subdirectories
    if file_pattern:
        patterns = [file_pattern, f"**/{file_pattern}"]
    else:
        patterns = ["*.tif", "*.tiff", "**/*.tif", "**/*.tiff"]

    seen_files = set()

    for pattern in patterns:
        for tif_file in cog_dir_path.glob(pattern):
            if tif_file in seen_files:
                continue
            seen_files.add(tif_file)

            print(f"  Found: {tif_file.name}")
            try:
                acq_time = extract_acquisition_time(str(tif_file))
                cog_files.append((str(tif_file), acq_time))
                print(f"    → Acquisition time: {acq_time}")
            except Exception as e:
                print(f"    ⚠️  Failed to extract time: {e}")
                continue

    # Sort by acquisition time
    cog_files.sort(key=lambda x: x[1])

    # Apply limit if specified
    if limit and len(cog_files) > limit:
        print(f"  (Limiting to first {limit} files)")
        cog_files = cog_files[:limit]

    return cog_files


def parse_band_mapping(band_mapping_str: str) -> dict:
    """
    Parse band mapping string to dict

    Example: "1:blue,2:green,3:red,4:nir" → {1: "blue", 2: "green", ...}
    """
    mapping = {}
    for pair in band_mapping_str.split(','):
        idx_str, name = pair.split(':')
        mapping[int(idx_str)] = name.strip()
    return mapping


def load_chunk_times(warehouse_path, tile_id, band):
    """
    Load time coordinates from chunk files

    Args:
        warehouse_path: Path to warehouse
        tile_id: Tile identifier
        band: Band name

    Returns:
        List of datetime objects
    """
    from pixelquery._internal.storage.arrow_chunk import ArrowChunkReader

    reader = ArrowChunkReader()
    times = []

    tile_dir = Path(warehouse_path) / "tiles" / tile_id
    if not tile_dir.exists():
        return times

    # Find all chunk files for this band
    for year_month_dir in sorted(tile_dir.glob("*")):
        chunk_file = year_month_dir / f"{band}.arrow"
        if chunk_file.exists():
            chunk_data, _ = reader.read_chunk(str(chunk_file))
            times.extend(chunk_data['time'])

    return times


def compute_ndvi_timeseries(dataset, times) -> List[Tuple[datetime, float]]:
    """
    Compute NDVI time-series for a dataset

    Args:
        dataset: Dataset object
        times: List of datetime objects corresponding to observations

    Returns:
        List of (time, mean_ndvi) tuples
    """
    ndvi_timeseries = []

    red_values = dataset["red"].values
    nir_values = dataset["nir"].values

    # Iterate over each time step
    num_observations = len(red_values) if isinstance(red_values, list) else 1
    if not isinstance(red_values, list):
        red_values = [red_values]
        nir_values = [nir_values]

    for i in range(min(num_observations, len(times))):
        time = times[i]
        red = red_values[i]
        nir = nir_values[i]

        # Compute NDVI
        ndvi = (nir.astype(float) - red.astype(float)) / (nir.astype(float) + red.astype(float) + 1e-8)

        # Calculate mean (excluding invalid values)
        mask = (ndvi >= -1.0) & (ndvi <= 1.0)
        mean_ndvi = ndvi[mask].mean()

        ndvi_timeseries.append((time, mean_ndvi))

    return ndvi_timeseries


def main():
    args = parse_args()

    print("=" * 60)
    print("PixelQuery Time-Series Demo")
    print("=" * 60)

    # ========================================
    # STEP 1: Find COG files
    # ========================================
    print("\n[Step 1] Scanning COG files...")
    if args.file_pattern:
        print(f"  File pattern filter: {args.file_pattern}")
    if args.limit:
        print(f"  Limit: {args.limit} files")
    cog_files = find_cog_files(args.cog_dir, file_pattern=args.file_pattern, limit=args.limit)

    if not cog_files:
        print("❌ No COG files found!")
        sys.exit(1)

    print(f"\n✓ Found {len(cog_files)} COG files")
    print(f"  Time range: {cog_files[0][1]} → {cog_files[-1][1]}")

    # ========================================
    # STEP 2: Setup warehouse
    # ========================================
    print(f"\n[Step 2] Setting up warehouse at {args.warehouse}...")
    warehouse_path = Path(args.warehouse)
    warehouse_path.mkdir(exist_ok=True, parents=True)

    catalog = LocalCatalog(str(warehouse_path))
    tile_grid = FixedTileGrid()
    executor = QueryExecutor(catalog)

    print("✓ Warehouse initialized")

    # ========================================
    # STEP 3: Ingest COG files
    # ========================================
    print(f"\n[Step 3] Ingesting {len(cog_files)} COG files...")

    pipeline = IngestionPipeline(
        warehouse_path=str(warehouse_path),
        tile_grid=tile_grid,
        catalog=catalog
    )

    band_mapping = parse_band_mapping(args.band_mapping)
    print(f"  Band mapping: {band_mapping}")

    total_metadata = []
    for i, (cog_path, acq_time) in enumerate(cog_files, 1):
        print(f"\n  [{i}/{len(cog_files)}] Ingesting {Path(cog_path).name}...")
        print(f"      Time: {acq_time}")

        try:
            metadata_list = pipeline.ingest_cog(
                cog_path=cog_path,
                acquisition_time=acq_time,
                product_id=args.product_id,
                band_mapping=band_mapping,
                auto_commit=False  # Buffer metadata for batch write
            )
            total_metadata.extend(metadata_list)
            print(f"      ✓ Created {len(metadata_list)} tile-band chunks")
        except Exception as e:
            print(f"      ❌ Failed: {e}")
            continue

    # Flush all buffered metadata to catalog in one batch
    print(f"\n[Step 3.1] Writing metadata to catalog...")
    pipeline.flush_metadata()
    print("✓ Metadata written")

    print(f"\n✓ Ingestion complete: {len(total_metadata)} total chunks")

    # ========================================
    # STEP 4: Query available data
    # ========================================
    print("\n[Step 4] Querying available data...")

    tiles = catalog.list_tiles()
    bands = catalog.list_bands()

    print(f"  Available tiles: {len(tiles)}")
    print(f"  Available bands: {bands}")

    if not tiles:
        print("❌ No tiles found!")
        sys.exit(1)

    # ========================================
    # STEP 5: Load time-series data
    # ========================================
    print("\n[Step 5] Loading time-series data...")

    # Load first tile with red and nir bands
    tile_id = tiles[0]
    print(f"  Loading tile: {tile_id}")

    datasets = executor.load_tiles(
        tile_ids=[tile_id],
        bands=["red", "nir"]
    )

    dataset = datasets[tile_id]

    # Load time coordinates from chunks
    times = load_chunk_times(str(warehouse_path), tile_id, "red")

    print(f"  ✓ Loaded {len(times)} time steps")
    if times:
        print(f"  Time range: {times[0]} → {times[-1]}")

    # ========================================
    # STEP 6: Compute NDVI time-series
    # ========================================
    print("\n[Step 6] Computing NDVI time-series...")

    ndvi_timeseries = compute_ndvi_timeseries(dataset, times)

    print("\n  NDVI Time-Series:")
    print("  " + "=" * 50)
    for time, mean_ndvi in ndvi_timeseries:
        print(f"  {time.strftime('%Y-%m-%d %H:%M:%S')}  |  NDVI: {mean_ndvi:.3f}")

    # ========================================
    # STEP 7: Summary statistics
    # ========================================
    print("\n[Step 7] Summary Statistics")
    print("  " + "=" * 50)

    ndvi_values = [ndvi for _, ndvi in ndvi_timeseries]
    print(f"  Number of observations: {len(ndvi_values)}")
    print(f"  Mean NDVI: {np.mean(ndvi_values):.3f}")
    print(f"  Min NDVI:  {np.min(ndvi_values):.3f}")
    print(f"  Max NDVI:  {np.max(ndvi_values):.3f}")
    print(f"  Std NDVI:  {np.std(ndvi_values):.3f}")

    # Calculate trend (simple linear regression)
    if len(ndvi_values) >= 2:
        time_indices = np.arange(len(ndvi_values))
        slope, intercept = np.polyfit(time_indices, ndvi_values, 1)
        trend = "increasing" if slope > 0 else "decreasing"
        print(f"  Trend: {trend} (slope: {slope:.6f})")

    print("\n" + "=" * 60)
    print("✓ Demo complete!")
    print("=" * 60)
    print(f"\nWarehouse location: {warehouse_path.absolute()}")
    print(f"Tiles stored: {len(tiles)}")
    print(f"Total observations: {len(total_metadata)}")


if __name__ == "__main__":
    main()
