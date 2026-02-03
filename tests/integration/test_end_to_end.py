"""
End-to-End Integration Tests

Tests the complete workflow from COG ingestion to data query and analysis.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
import numpy as np
import rasterio
from rasterio.transform import from_bounds

from pixelquery.io.cog import COGReader
from pixelquery.io.ingest import IngestionPipeline
from pixelquery.grid.tile_grid import FixedTileGrid
from pixelquery.catalog.local import LocalCatalog
from pixelquery.query.executor import QueryExecutor
from pixelquery.core.dataset import Dataset


class TestEndToEndWorkflow:
    """Integration test for complete workflow"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def mock_cog_file(self):
        """Create small mock COG file with 4 bands"""
        tmpdir = tempfile.mkdtemp()
        cog_path = Path(tmpdir) / "sentinel2.tif"

        # Create 100x100 pixel COG covering small area in Seoul region
        width, height = 100, 100
        bounds = (126.95, 37.50, 126.96, 37.51)  # ~1km × 1km

        transform = from_bounds(*bounds, width, height)

        # Create 4-band array with reasonable sentinel-2 values
        # Band order: blue, green, red, nir
        data = np.zeros((4, height, width), dtype=np.uint16)

        # Simulate vegetation: low red, high nir
        data[0, :, :] = np.random.randint(500, 1500, (height, width))   # blue
        data[1, :, :] = np.random.randint(800, 2000, (height, width))   # green
        data[2, :, :] = np.random.randint(300, 1000, (height, width))   # red
        data[3, :, :] = np.random.randint(2000, 4000, (height, width))  # nir

        # Write COG
        with rasterio.open(
            cog_path, 'w',
            driver='GTiff',
            height=height, width=width,
            count=4,
            dtype=np.uint16,
            crs='EPSG:4326',
            transform=transform,
            nodata=0
        ) as dst:
            dst.write(data)

        yield str(cog_path)
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_full_workflow(self, temp_warehouse, mock_cog_file):
        """Test complete workflow: ingest → query → load → analyze"""

        # ====================
        # STEP 1: Setup
        # ====================
        catalog = LocalCatalog(temp_warehouse)
        tile_grid = FixedTileGrid()
        executor = QueryExecutor(catalog)

        # ====================
        # STEP 2: Ingest COG
        # ====================
        pipeline = IngestionPipeline(
            warehouse_path=temp_warehouse,
            tile_grid=tile_grid,
            catalog=catalog,
            storage_backend="arrow"  # Force Arrow backend for consistency
        )

        metadata_list = pipeline.ingest_cog(
            cog_path=mock_cog_file,
            acquisition_time=datetime(2024, 6, 15, 10, 30),
            product_id="sentinel2_l2a",
            band_mapping={1: "blue", 2: "green", 3: "red", 4: "nir"}
        )

        # Verify ingestion
        assert len(metadata_list) > 0
        print(f"✓ Ingested {len(metadata_list)} tile-band combinations")

        # ====================
        # STEP 3: Query Catalog
        # ====================
        tiles = catalog.list_tiles()
        assert len(tiles) > 0
        print(f"✓ Found {len(tiles)} tiles in catalog")

        bands = catalog.list_bands()
        assert len(bands) == 4
        assert "red" in bands
        assert "nir" in bands
        print(f"✓ Available bands: {bands}")

        # ====================
        # STEP 4: Load Data
        # ====================
        tile_id = tiles[0]
        dataset = executor.load_tile(
            tile_id=tile_id,
            bands=["red", "nir"]
        )

        # Verify dataset
        assert isinstance(dataset, Dataset)
        assert dataset.tile_id == tile_id
        assert "red" in dataset.bands
        assert "nir" in dataset.bands
        print(f"✓ Loaded dataset for tile {tile_id}")

        # ====================
        # STEP 5: Data Access
        # ====================
        red = dataset["red"]
        nir = dataset["nir"]

        # DataArray.values is a list of arrays (one per observation)
        assert isinstance(red.values, list)
        assert len(red.values) > 0
        assert len(nir.values) > 0
        print(f"✓ Accessed band data: red has {len(red.values)} observations")

        # ====================
        # STEP 6: Analysis (NDVI)
        # ====================
        # Compute NDVI for first observation: (NIR - Red) / (NIR + Red)
        red_values = red.values[0].astype(float)
        nir_values = nir.values[0].astype(float)

        ndvi = (nir_values - red_values) / (nir_values + red_values + 1e-8)

        # Verify NDVI is in valid range
        assert ndvi.min() >= -1.0
        assert ndvi.max() <= 1.0

        # For vegetation (high NIR, low Red), NDVI should be positive
        assert ndvi.mean() > 0
        print(f"✓ NDVI computed: min={ndvi.min():.3f}, max={ndvi.max():.3f}, mean={ndvi.mean():.3f}")

        # ====================
        # STEP 7: Statistics
        # ====================
        stats = executor.get_tile_statistics(tile_id, "red")

        assert 'min' in stats
        assert 'max' in stats
        assert 'mean' in stats
        assert stats['min'] < stats['max']
        print(f"✓ Retrieved statistics: min={stats['min']:.1f}, max={stats['max']:.1f}, mean={stats['mean']:.1f}")

        print("\\n=== End-to-End Test Complete! ===")

    def test_spatial_query_workflow(self, temp_warehouse, mock_cog_file):
        """Test spatial query workflow"""

        # Setup
        catalog = LocalCatalog(temp_warehouse)
        tile_grid = FixedTileGrid()
        executor = QueryExecutor(catalog)

        # Ingest
        pipeline = IngestionPipeline(
            warehouse_path=temp_warehouse,
            tile_grid=tile_grid,
            catalog=catalog,
            storage_backend="arrow"  # Force Arrow backend for consistency
        )

        pipeline.ingest_cog(
            cog_path=mock_cog_file,
            acquisition_time=datetime(2024, 6, 15),
            product_id="sentinel2_l2a",
            band_mapping={1: "red", 2: "nir"}
        )

        # Query by bounds
        bounds = (126.95, 37.50, 126.96, 37.51)
        datasets = executor.query_by_bounds(
            bounds=bounds,
            bands=["red"]
        )

        assert isinstance(datasets, dict)
        assert len(datasets) >= 1

        # Each dataset should be valid
        for tile_id, dataset in datasets.items():
            assert isinstance(dataset, Dataset)
            assert "red" in dataset.bands
            print(f"✓ Query returned tile {tile_id}")

    def test_multi_temporal_workflow(self, temp_warehouse, mock_cog_file):
        """Test workflow with multiple time steps"""

        # Setup
        catalog = LocalCatalog(temp_warehouse)
        tile_grid = FixedTileGrid()
        executor = QueryExecutor(catalog)

        pipeline = IngestionPipeline(
            warehouse_path=temp_warehouse,
            tile_grid=tile_grid,
            catalog=catalog,
            storage_backend="arrow"  # Force Arrow backend for consistency
        )

        # Ingest multiple time steps
        dates = [
            datetime(2024, 1, 15),
            datetime(2024, 2, 15),
            datetime(2024, 3, 15)
        ]

        for date in dates:
            pipeline.ingest_cog(
                cog_path=mock_cog_file,
                acquisition_time=date,
                product_id="sentinel2_l2a",
                band_mapping={1: "red"}
            )

        # Verify multiple months ingested
        tiles = catalog.list_tiles()
        assert len(tiles) > 0

        # Query should return data
        tile_id = tiles[0]
        dataset = executor.load_tile(
            tile_id=tile_id,
            bands=["red"]
        )

        assert dataset is not None
        print(f"✓ Multi-temporal ingestion successful: {len(dates)} dates")

    def test_warehouse_persistence(self, temp_warehouse, mock_cog_file):
        """Test that warehouse can be reopened and data persists"""

        # Initial setup and ingestion
        catalog1 = LocalCatalog(temp_warehouse)
        tile_grid = FixedTileGrid()

        pipeline = IngestionPipeline(
            warehouse_path=temp_warehouse,
            tile_grid=tile_grid,
            catalog=catalog1,
            storage_backend="arrow"  # Force Arrow backend for consistency
        )

        pipeline.ingest_cog(
            cog_path=mock_cog_file,
            acquisition_time=datetime(2024, 6, 15),
            product_id="sentinel2_l2a",
            band_mapping={1: "red"}
        )

        tiles1 = catalog1.list_tiles()
        assert len(tiles1) > 0

        # Close and reopen catalog
        catalog2 = LocalCatalog(temp_warehouse)
        tiles2 = catalog2.list_tiles()

        # Data should still be accessible
        assert tiles1 == tiles2
        print(f"✓ Warehouse persistence verified: {len(tiles2)} tiles")

        # Verify data can be loaded
        executor2 = QueryExecutor(catalog2)
        dataset = executor2.load_tile(
            tile_id=tiles2[0],
            bands=["red"]
        )

        assert dataset is not None
        assert "red" in dataset.bands
