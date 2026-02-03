"""
Tests for IcebergCatalog
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone

import numpy as np

from pixelquery.catalog.iceberg import IcebergCatalog, TileMetadata
from pixelquery.io.iceberg_writer import IcebergPixelWriter


class TestIcebergCatalog:
    """Test IcebergCatalog class"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def catalog(self, temp_warehouse):
        """Create IcebergCatalog instance"""
        return IcebergCatalog(temp_warehouse)

    @pytest.fixture
    def writer(self, temp_warehouse):
        """Create IcebergPixelWriter instance"""
        return IcebergPixelWriter(temp_warehouse)

    @pytest.fixture
    def sample_observation(self):
        """Create sample observation data"""
        return {
            "tile_id": "x0024_y0041",
            "band": "red",
            "time": datetime(2024, 1, 15, tzinfo=timezone.utc),
            "pixels": np.array([[100, 200], [300, 400]], dtype=np.uint16),
            "mask": np.array([[True, True], [False, True]], dtype=bool),
            "product_id": "sentinel2_l2a",
            "resolution": 10.0,
            "bounds": (126.5, 37.0, 127.5, 38.0),
            "cloud_cover": 0.1,
        }

    def test_init(self, catalog, temp_warehouse):
        """Test IcebergCatalog initialization"""
        assert catalog.warehouse_path == Path(temp_warehouse)
        assert catalog.metadata_path == Path(temp_warehouse) / "catalog.db"

    def test_exists_false_initially(self, temp_warehouse):
        """Test exists returns False for new catalog"""
        catalog = IcebergCatalog(temp_warehouse, auto_initialize=False)
        assert not catalog.exists()

    def test_exists_true_after_initialization(self, catalog):
        """Test exists returns True after initialization"""
        assert catalog.exists()

    def test_list_tiles_empty(self, catalog):
        """Test list_tiles on empty catalog"""
        tiles = catalog.list_tiles()
        assert tiles == []

    def test_list_tiles_after_write(self, catalog, writer, sample_observation):
        """Test list_tiles after writing data"""
        writer.write_observation(**sample_observation)

        tiles = catalog.list_tiles()
        assert len(tiles) == 1
        assert "x0024_y0041" in tiles

    def test_list_tiles_multiple(self, catalog, writer, sample_observation):
        """Test listing multiple tiles"""
        # Write to different tiles
        obs1 = sample_observation.copy()
        obs1["tile_id"] = "x0024_y0041"
        writer.write_observation(**obs1)

        obs2 = sample_observation.copy()
        obs2["tile_id"] = "x0024_y0042"
        writer.write_observation(**obs2)

        obs3 = sample_observation.copy()
        obs3["tile_id"] = "x0025_y0041"
        writer.write_observation(**obs3)

        tiles = catalog.list_tiles()
        assert len(tiles) == 3
        assert "x0024_y0041" in tiles
        assert "x0024_y0042" in tiles
        assert "x0025_y0041" in tiles

    def test_list_tiles_with_time_range(self, catalog, writer, sample_observation):
        """Test listing tiles filtered by time range"""
        # Write observations in different months
        obs1 = sample_observation.copy()
        obs1["time"] = datetime(2024, 1, 15, tzinfo=timezone.utc)
        writer.write_observation(**obs1)

        obs2 = sample_observation.copy()
        obs2["time"] = datetime(2024, 3, 15, tzinfo=timezone.utc)
        writer.write_observation(**obs2)

        # Query only January
        tiles = catalog.list_tiles(
            time_range=(datetime(2024, 1, 1), datetime(2024, 1, 31))
        )
        assert len(tiles) >= 1
        assert "x0024_y0041" in tiles

    def test_list_bands_empty(self, catalog):
        """Test list_bands on empty catalog"""
        bands = catalog.list_bands()
        assert bands == []

    def test_list_bands_after_write(self, catalog, writer, sample_observation):
        """Test list_bands after writing data"""
        writer.write_observation(**sample_observation)

        bands = catalog.list_bands()
        assert "red" in bands

    def test_list_bands_multiple(self, catalog, writer, sample_observation):
        """Test listing multiple bands"""
        for band in ["red", "green", "blue", "nir"]:
            obs = sample_observation.copy()
            obs["band"] = band
            writer.write_observation(**obs)

        bands = catalog.list_bands()
        assert len(bands) == 4
        assert "red" in bands
        assert "green" in bands
        assert "blue" in bands
        assert "nir" in bands

    def test_list_bands_by_tile(self, catalog, writer, sample_observation):
        """Test listing bands filtered by tile"""
        # Write different bands to different tiles
        obs1 = sample_observation.copy()
        obs1["tile_id"] = "x0024_y0041"
        obs1["band"] = "red"
        writer.write_observation(**obs1)

        obs2 = sample_observation.copy()
        obs2["tile_id"] = "x0024_y0041"
        obs2["band"] = "nir"
        writer.write_observation(**obs2)

        obs3 = sample_observation.copy()
        obs3["tile_id"] = "x0025_y0041"
        obs3["band"] = "blue"
        writer.write_observation(**obs3)

        bands = catalog.list_bands(tile_id="x0024_y0041")
        assert len(bands) == 2
        assert "red" in bands
        assert "nir" in bands
        assert "blue" not in bands

    def test_query_metadata(self, catalog, writer, sample_observation):
        """Test querying metadata"""
        writer.write_observation(**sample_observation)

        metadata_list = catalog.query_metadata("x0024_y0041", "2024-01", "red")

        assert len(metadata_list) >= 1
        metadata = metadata_list[0]
        assert metadata.tile_id == "x0024_y0041"
        assert metadata.year_month == "2024-01"
        assert metadata.band == "red"

    def test_query_metadata_empty(self, catalog):
        """Test querying metadata on empty catalog"""
        metadata_list = catalog.query_metadata("x0024_y0041")
        assert metadata_list == []

    def test_query_metadata_filters(self, catalog, writer, sample_observation):
        """Test metadata query filters"""
        # Write multiple observations
        for month in [1, 2, 3]:
            for band in ["red", "nir"]:
                obs = sample_observation.copy()
                obs["time"] = datetime(2024, month, 15, tzinfo=timezone.utc)
                obs["band"] = band
                writer.write_observation(**obs)

        # Query specific month and band
        metadata_list = catalog.query_metadata("x0024_y0041", "2024-02", "red")
        assert len(metadata_list) == 1
        assert metadata_list[0].year_month == "2024-02"
        assert metadata_list[0].band == "red"

        # Query all bands for a month
        metadata_list = catalog.query_metadata("x0024_y0041", "2024-02")
        assert len(metadata_list) == 2

        # Query all months for a band
        metadata_list = catalog.query_metadata("x0024_y0041", band="nir")
        assert len(metadata_list) == 3

    def test_query_metadata_with_snapshot_id(self, catalog, writer, sample_observation):
        """Test Time Travel query with snapshot_id"""
        # Write first observation
        snapshot_id1 = writer.write_observation(**sample_observation)

        # Query at first snapshot
        metadata_list = catalog.query_metadata(
            "x0024_y0041",
            "2024-01",
            "red",
            as_of_snapshot_id=snapshot_id1
        )
        assert len(metadata_list) >= 1

        # Write second observation with different band
        obs2 = sample_observation.copy()
        obs2["band"] = "nir"
        snapshot_id2 = writer.write_observation(**obs2)

        # Query at first snapshot should not see NIR
        metadata_list = catalog.query_metadata(
            "x0024_y0041",
            as_of_snapshot_id=snapshot_id1
        )
        bands = [m.band for m in metadata_list]
        assert "red" in bands
        assert "nir" not in bands

        # Query at second snapshot should see both
        metadata_list = catalog.query_metadata(
            "x0024_y0041",
            as_of_snapshot_id=snapshot_id2
        )
        bands = [m.band for m in metadata_list]
        assert "red" in bands
        assert "nir" in bands

    def test_get_chunk_paths(self, catalog, writer, sample_observation):
        """Test getting chunk paths"""
        writer.write_observation(**sample_observation)

        paths = catalog.get_chunk_paths("x0024_y0041", "2024-01", ["red"])

        assert "red" in paths
        assert "iceberg://" in paths["red"]

    def test_get_chunk_paths_multiple_bands(self, catalog, writer, sample_observation):
        """Test getting chunk paths for multiple bands"""
        for band in ["red", "green", "blue"]:
            obs = sample_observation.copy()
            obs["band"] = band
            writer.write_observation(**obs)

        paths = catalog.get_chunk_paths("x0024_y0041", "2024-01", ["red", "blue"])

        assert len(paths) == 2
        assert "red" in paths
        assert "blue" in paths
        assert "green" not in paths

    def test_get_tile_bounds(self, catalog, writer, sample_observation):
        """Test getting tile bounds"""
        writer.write_observation(**sample_observation)

        bounds = catalog.get_tile_bounds("x0024_y0041")

        assert bounds == (126.5, 37.0, 127.5, 38.0)

    def test_get_tile_bounds_not_found(self, catalog):
        """Test getting bounds for non-existent tile"""
        bounds = catalog.get_tile_bounds("x9999_y9999")
        assert bounds is None

    def test_get_statistics(self, catalog, writer, sample_observation):
        """Test getting statistics"""
        writer.write_observation(**sample_observation)

        stats = catalog.get_statistics("x0024_y0041", "red")

        assert "min" in stats
        assert "max" in stats
        assert "mean" in stats
        assert "cloud_cover" in stats
        assert "num_observations" in stats

    def test_get_statistics_aggregation(self, catalog, writer, sample_observation):
        """Test statistics aggregation across multiple observations"""
        # Write multiple months
        for month in range(1, 4):
            obs = sample_observation.copy()
            obs["time"] = datetime(2024, month, 15, tzinfo=timezone.utc)
            obs["pixels"] = np.array([[100 * month, 200 * month],
                                     [300 * month, 400 * month]], dtype=np.uint16)
            writer.write_observation(**obs)

        stats = catalog.get_statistics("x0024_y0041", "red")

        # Min should be from month 1 (100)
        assert stats["min"] == 100.0
        # Max should be from month 3 (400 * 3 = 1200)
        assert stats["max"] == 1200.0

    def test_get_snapshot_history(self, catalog, writer, sample_observation):
        """Test getting snapshot history"""
        # Initially empty
        history = catalog.get_snapshot_history()
        assert history == []

        # Write observations
        snapshot_id1 = writer.write_observation(**sample_observation)

        obs2 = sample_observation.copy()
        obs2["band"] = "nir"
        snapshot_id2 = writer.write_observation(**obs2)

        history = catalog.get_snapshot_history()

        assert len(history) == 2
        # Newest first
        assert history[0]["snapshot_id"] == snapshot_id2
        assert history[1]["snapshot_id"] == snapshot_id1

    def test_get_current_snapshot_id(self, catalog, writer, sample_observation):
        """Test getting current snapshot ID"""
        # Initially None
        snapshot_id = catalog.get_current_snapshot_id()
        assert snapshot_id is None

        # After write
        written_id = writer.write_observation(**sample_observation)
        current_id = catalog.get_current_snapshot_id()

        assert current_id == written_id

    def test_get_table_stats(self, catalog, writer, sample_observation):
        """Test getting table statistics"""
        writer.write_observation(**sample_observation)

        stats = catalog.get_table_stats()

        assert "total_snapshots" in stats
        assert "current_snapshot_id" in stats
        assert "schema_fields" in stats
        assert "partition_fields" in stats

    def test_repr(self, catalog):
        """Test string representation"""
        repr_str = repr(catalog)

        assert "IcebergCatalog" in repr_str
        assert "Warehouse" in repr_str


class TestIcebergCatalogIntegration:
    """Integration tests for IcebergCatalog"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_full_workflow(self, temp_warehouse):
        """Test complete workflow: write, query, retrieve"""
        catalog = IcebergCatalog(temp_warehouse)
        writer = IcebergPixelWriter(temp_warehouse)

        # Write data for multiple tiles, months, bands
        for tile_id in ["x0024_y0041", "x0024_y0042"]:
            for month in [1, 2]:
                for band in ["red", "nir"]:
                    obs = {
                        "tile_id": tile_id,
                        "band": band,
                        "time": datetime(2024, month, 15, tzinfo=timezone.utc),
                        "pixels": np.array([[100, 200], [300, 400]], dtype=np.uint16),
                        "mask": np.array([[True, True], [False, True]], dtype=bool),
                        "product_id": "sentinel2_l2a",
                        "resolution": 10.0,
                        "bounds": (126.5, 37.0, 127.5, 38.0),
                        "cloud_cover": 0.1,
                    }
                    writer.write_observation(**obs)

        # Query tiles
        tiles = catalog.list_tiles()
        assert len(tiles) == 2
        assert "x0024_y0041" in tiles
        assert "x0024_y0042" in tiles

        # Query bands
        bands = catalog.list_bands(tile_id="x0024_y0041")
        assert len(bands) == 2
        assert "red" in bands
        assert "nir" in bands

        # Get chunk paths
        paths = catalog.get_chunk_paths("x0024_y0041", "2024-01", ["red"])
        assert "red" in paths

        # Get statistics
        stats = catalog.get_statistics("x0024_y0041", "red")
        assert "min" in stats
        assert "max" in stats

    def test_time_travel_workflow(self, temp_warehouse):
        """Test Time Travel workflow"""
        catalog = IcebergCatalog(temp_warehouse)
        writer = IcebergPixelWriter(temp_warehouse)

        # Write initial observation
        obs1 = {
            "tile_id": "x0024_y0041",
            "band": "red",
            "time": datetime(2024, 1, 15, tzinfo=timezone.utc),
            "pixels": np.array([[100, 200], [300, 400]], dtype=np.uint16),
            "mask": np.array([[True, True], [False, True]], dtype=bool),
            "product_id": "sentinel2_l2a",
            "resolution": 10.0,
        }
        snapshot_id1 = writer.write_observation(**obs1)

        # Verify initial state
        tiles_at_snap1 = catalog.list_tiles(as_of_snapshot_id=snapshot_id1)
        assert len(tiles_at_snap1) == 1

        # Write more observations
        obs2 = obs1.copy()
        obs2["tile_id"] = "x0024_y0042"
        snapshot_id2 = writer.write_observation(**obs2)

        obs3 = obs1.copy()
        obs3["tile_id"] = "x0025_y0041"
        snapshot_id3 = writer.write_observation(**obs3)

        # Current state should have 3 tiles
        current_tiles = catalog.list_tiles()
        assert len(current_tiles) == 3

        # Time Travel to snapshot 1 should show only 1 tile
        historical_tiles = catalog.list_tiles(as_of_snapshot_id=snapshot_id1)
        assert len(historical_tiles) == 1
        assert "x0024_y0041" in historical_tiles

        # Time Travel to snapshot 2 should show 2 tiles
        historical_tiles = catalog.list_tiles(as_of_snapshot_id=snapshot_id2)
        assert len(historical_tiles) == 2

    def test_bounds_filtering(self, temp_warehouse):
        """Test spatial filtering by bounds"""
        catalog = IcebergCatalog(temp_warehouse)
        writer = IcebergPixelWriter(temp_warehouse)

        # Write observations with different bounds
        obs1 = {
            "tile_id": "x0024_y0041",
            "band": "red",
            "time": datetime(2024, 1, 15, tzinfo=timezone.utc),
            "pixels": np.array([[100, 200], [300, 400]], dtype=np.uint16),
            "mask": np.array([[True, True], [False, True]], dtype=bool),
            "product_id": "sentinel2_l2a",
            "resolution": 10.0,
            "bounds": (126.0, 37.0, 127.0, 38.0),
        }
        writer.write_observation(**obs1)

        obs2 = obs1.copy()
        obs2["tile_id"] = "x0025_y0041"
        obs2["bounds"] = (130.0, 37.0, 131.0, 38.0)
        writer.write_observation(**obs2)

        # Query within first tile bounds
        tiles = catalog.list_tiles(bounds=(126.0, 37.0, 127.0, 38.0))
        assert len(tiles) >= 1
        assert "x0024_y0041" in tiles
