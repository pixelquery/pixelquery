#!/usr/bin/env python3
"""
Real Data Test - PlanetScope Ingestion

Tests PixelQuery with actual PlanetScope satellite imagery.
"""

import sys
import time
from pathlib import Path
from datetime import datetime
import tempfile
import shutil

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from pixelquery.io.ingest import IngestionPipeline
from pixelquery.catalog import LocalCatalog
from pixelquery.grid.tile_grid import FixedTileGrid
from pixelquery.core.api import get_snapshot_history, get_current_snapshot_id
import pixelquery as pq


def find_planetscope_files(data_dir: str, limit: int = 5):
    """Find PlanetScope SR files with their dates"""
    data_path = Path(data_dir)
    files = []

    for tiff_file in data_path.rglob("*_sr.tiff"):
        # Extract date from filename: YYYY-MM-DD_analysis_ready_ps_sr.tiff
        filename = tiff_file.name
        date_str = filename.split("_")[0]  # YYYY-MM-DD
        try:
            acq_time = datetime.strptime(date_str, "%Y-%m-%d")
            files.append((str(tiff_file), acq_time))
        except ValueError:
            continue

    # Sort by date
    files.sort(key=lambda x: x[1])

    return files[:limit] if limit else files


def test_ingestion(data_dir: str, limit: int = 5):
    """Test ingestion with real PlanetScope data"""

    print("=" * 60)
    print("PixelQuery Real Data Test - PlanetScope")
    print("=" * 60)

    # Find files
    print(f"\n[1] Finding PlanetScope files in {data_dir}...")
    cog_files = find_planetscope_files(data_dir, limit=limit)
    print(f"    Found {len(cog_files)} files")

    if not cog_files:
        print("ERROR: No PlanetScope SR files found!")
        return False

    for path, acq_time in cog_files:
        print(f"    - {Path(path).name} ({acq_time.date()})")

    # Create temporary warehouse
    warehouse_dir = tempfile.mkdtemp(prefix="pq_test_")
    print(f"\n[2] Creating warehouse at: {warehouse_dir}")

    try:
        # PlanetScope band mapping: 1=Blue, 2=Green, 3=Red, 4=NIR
        band_mapping = {
            1: "blue",
            2: "green",
            3: "red",
            4: "nir"
        }

        # Initialize pipeline with Arrow backend for testing
        print("\n[3] Initializing ingestion pipeline...")

        # Create tile grid for the region (Korea area)
        # Default tile size is 2560m (2.56km), which at 3m resolution = 853 pixels
        tile_grid = FixedTileGrid(
            tile_size_m=2560.0,  # 2.56km tiles (default)
        )

        pipeline = IngestionPipeline(
            warehouse_path=warehouse_dir,
            tile_grid=tile_grid,
            storage_backend="arrow",  # Use Arrow backend
        )

        # Ingest files
        print(f"\n[4] Ingesting {len(cog_files)} COG files...")
        start_time = time.time()

        total_tiles = 0
        for i, (cog_path, acq_time) in enumerate(cog_files, 1):
            file_start = time.time()

            metadata_list = pipeline.ingest_cog(
                cog_path=cog_path,
                acquisition_time=acq_time,
                product_id="planetscope_sr",
                band_mapping=band_mapping,
            )

            file_time = time.time() - file_start
            total_tiles += len(metadata_list)
            print(f"    [{i}/{len(cog_files)}] {Path(cog_path).name}: "
                  f"{len(metadata_list)} tiles in {file_time:.2f}s")

        total_time = time.time() - start_time
        print(f"\n    Total: {total_tiles} tiles in {total_time:.2f}s "
              f"({total_time/len(cog_files):.2f}s/file)")

        # Verify catalog
        print("\n[5] Verifying catalog...")
        catalog = LocalCatalog(warehouse_dir)

        tiles = catalog.list_tiles()
        bands = catalog.list_bands()

        print(f"    Tiles in catalog: {len(tiles)}")
        print(f"    Bands in catalog: {bands}")

        if tiles:
            print(f"    Sample tiles: {tiles[:5]}")

        # Query data
        print("\n[6] Querying data...")
        if tiles:
            test_tile = tiles[0]
            ds = pq.open_dataset(
                warehouse_dir,
                tile_id=test_tile,
                storage_backend="arrow"
            )
            print(f"    Opened dataset for tile: {test_tile}")
            print(f"    Available bands: {ds.bands}")
            print(f"    Tile ID: {ds.tile_id}")

            # Try to access a band
            if ds.bands:
                band_name = ds.bands[0]
                print(f"    First band ({band_name}) data keys: {list(ds.data.get(band_name, {}).keys())}")

        print("\n" + "=" * 60)
        print("✅ TEST PASSED - Real data ingestion successful!")
        print("=" * 60)

        return True

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Cleanup
        print(f"\n[Cleanup] Removing temporary warehouse...")
        shutil.rmtree(warehouse_dir, ignore_errors=True)


def test_iceberg_ingestion(data_dir: str, limit: int = 5):
    """Test ingestion with Iceberg backend"""

    print("\n" + "=" * 60)
    print("PixelQuery Real Data Test - PlanetScope (ICEBERG Backend)")
    print("=" * 60)

    # Find files
    print(f"\n[1] Finding PlanetScope files in {data_dir}...")
    cog_files = find_planetscope_files(data_dir, limit=limit)
    print(f"    Found {len(cog_files)} files")

    if not cog_files:
        print("ERROR: No PlanetScope SR files found!")
        return False

    # Create temporary warehouse
    warehouse_dir = tempfile.mkdtemp(prefix="pq_iceberg_test_")
    print(f"\n[2] Creating Iceberg warehouse at: {warehouse_dir}")

    try:
        band_mapping = {1: "blue", 2: "green", 3: "red", 4: "nir"}

        print("\n[3] Initializing Iceberg ingestion pipeline...")
        tile_grid = FixedTileGrid(tile_size_m=2560.0)

        pipeline = IngestionPipeline(
            warehouse_path=warehouse_dir,
            tile_grid=tile_grid,
            storage_backend="iceberg",  # Use Iceberg backend
        )

        # Ingest files
        print(f"\n[4] Ingesting {len(cog_files)} COG files (Iceberg)...")
        start_time = time.time()

        total_tiles = 0
        for i, (cog_path, acq_time) in enumerate(cog_files, 1):
            file_start = time.time()

            # Iceberg automatically uses sequential processing for atomic batch writes
            metadata_list = pipeline.ingest_cog(
                cog_path=cog_path,
                acquisition_time=acq_time,
                product_id="planetscope_sr",
                band_mapping=band_mapping,
            )

            file_time = time.time() - file_start
            total_tiles += len(metadata_list)
            print(f"    [{i}/{len(cog_files)}] {Path(cog_path).name}: "
                  f"{len(metadata_list)} tiles in {file_time:.2f}s")

        total_time = time.time() - start_time
        print(f"\n    Total: {total_tiles} tiles in {total_time:.2f}s "
              f"({total_time/len(cog_files):.2f}s/file)")

        # Check what was written
        print("\n[5] Checking Iceberg storage...")
        from pixelquery._internal.storage.iceberg_storage import IcebergStorageManager
        storage = IcebergStorageManager(warehouse_dir)
        storage.initialize()

        stats = storage.get_table_stats()
        print(f"    Table stats: {stats}")

        # Test Time Travel feature
        print("\n[6] Testing Iceberg Time Travel...")
        snapshots = storage.get_snapshot_history()
        print(f"    Snapshots: {len(snapshots)}")

        if snapshots:
            latest = snapshots[0]
            print(f"    Latest snapshot: {latest['snapshot_id']} ({latest['operation']})")

        # Query data via reader
        print("\n[7] Querying Iceberg data via reader...")
        from pixelquery.io.iceberg_reader import IcebergPixelReader
        reader = IcebergPixelReader(warehouse_dir)

        tiles = reader.list_tiles()
        print(f"    Tiles from reader: {len(tiles)}")

        bands = reader.list_bands()
        print(f"    Bands from reader: {bands}")

        if tiles:
            test_tile = tiles[0]
            print(f"\n[8] Querying data for tile: {test_tile}")
            data = reader.read_tile(test_tile)
            print(f"    Data bands: {list(data.keys())}")
            for band, band_data in data.items():
                print(f"    {band}: {len(band_data.get('times', []))} observations")

        print("\n" + "=" * 60)
        print("✅ ICEBERG TEST PASSED!")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"\n❌ ICEBERG TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        print(f"\n[Cleanup] Removing temporary warehouse...")
        shutil.rmtree(warehouse_dir, ignore_errors=True)


if __name__ == "__main__":
    data_dir = "/Users/sonhoyoung/saefarm/arps"
    limit = 5  # Test with 5 files first

    if len(sys.argv) > 1:
        limit = int(sys.argv[1])

    # Test Arrow backend
    success1 = test_ingestion(data_dir, limit=limit)

    # Test Iceberg backend
    success2 = test_iceberg_ingestion(data_dir, limit=limit)

    sys.exit(0 if (success1 and success2) else 1)
