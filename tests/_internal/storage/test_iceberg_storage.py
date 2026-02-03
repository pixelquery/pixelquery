"""
Tests for IcebergStorageManager
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone

import pyarrow as pa
import numpy as np

from pixelquery._internal.storage.iceberg_storage import IcebergStorageManager


class TestIcebergStorageManager:
    """Test IcebergStorageManager class"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def storage(self, temp_warehouse):
        """Create IcebergStorageManager instance"""
        return IcebergStorageManager(temp_warehouse)

    @pytest.fixture
    def sample_arrow_table(self, storage):
        """Create sample Arrow table with pixel data"""
        # Initialize storage to get the schema
        storage.initialize()

        # Use the schema from the Iceberg table
        iceberg_schema = storage.table.schema().as_arrow()

        # Create sample data
        data = {
            "tile_id": ["x0024_y0041"],
            "band": ["red"],
            "year_month": ["2024-01"],
            "time": [datetime(2024, 1, 15, tzinfo=timezone.utc)],
            "pixels": [[100, 200, 300, 400]],
            "mask": [[True, True, False, True]],
            "product_id": ["sentinel2_l2a"],
            "resolution": [10.0],
            "bounds_wkb": [None],
            "num_pixels": [4],
            "min_value": [100.0],
            "max_value": [300.0],
            "mean_value": [200.0],
            "cloud_cover": [0.1],
            "crs": ["EPSG:32632"],
            "transform": [None],
            "source_file": [None],
            "ingestion_time": [datetime.now(timezone.utc)],
        }

        return pa.Table.from_pydict(data, schema=iceberg_schema)

    def test_init(self, storage, temp_warehouse):
        """Test IcebergStorageManager initialization"""
        assert storage.warehouse_path == Path(temp_warehouse).resolve()
        assert storage.catalog_name == "pixelquery"
        assert not storage.is_initialized

    def test_initialize_creates_catalog_db(self, storage, temp_warehouse):
        """Test initialize() creates catalog.db"""
        storage.initialize()

        catalog_db = Path(temp_warehouse) / "catalog.db"
        assert catalog_db.exists()
        assert storage.is_initialized

    def test_initialize_creates_namespace(self, storage):
        """Test initialize() creates namespace"""
        storage.initialize()

        # Check namespace exists (catalog should have it)
        namespaces = list(storage.catalog.list_namespaces())
        assert ("pixelquery",) in namespaces

    def test_initialize_creates_table(self, storage):
        """Test initialize() creates pixel_data table"""
        storage.initialize()

        # Check table exists
        table_identifier = ("pixelquery", "pixel_data")
        table = storage.catalog.load_table(table_identifier)
        assert table is not None
        assert table.name() == table_identifier

    def test_initialize_idempotent(self, storage):
        """Test initialize() can be called multiple times"""
        storage.initialize()
        storage.initialize()  # Should not raise

        assert storage.is_initialized

    def test_table_property_auto_initializes(self, storage):
        """Test table property auto-initializes if needed"""
        assert not storage.is_initialized

        table = storage.table
        assert table is not None
        assert storage.is_initialized

    def test_catalog_property_auto_initializes(self, storage):
        """Test catalog property auto-initializes if needed"""
        assert not storage.is_initialized

        catalog = storage.catalog
        assert catalog is not None
        assert storage.is_initialized

    def test_append_data_writes_records(self, storage, sample_arrow_table):
        """Test append_data() writes records"""
        storage.initialize()

        snapshot_id = storage.append_data(sample_arrow_table)

        assert snapshot_id > 0
        assert storage.get_current_snapshot_id() == snapshot_id

    def test_append_data_returns_snapshot_id(self, storage, sample_arrow_table):
        """Test append_data() returns snapshot ID"""
        storage.initialize()

        snapshot_id1 = storage.append_data(sample_arrow_table)
        snapshot_id2 = storage.append_data(sample_arrow_table)

        assert snapshot_id1 > 0
        assert snapshot_id2 != snapshot_id1  # New snapshot (IDs are unique, not sequential)

    def test_append_empty_table(self, storage):
        """Test append_data() with empty table"""
        storage.initialize()

        # Create empty table with correct schema
        schema = storage.table.schema().as_arrow()

        # PyArrow requires all schema fields to have empty arrays
        empty_data = {field.name: [] for field in schema}
        empty_table = pa.Table.from_pydict(empty_data, schema=schema)

        snapshot_id = storage.append_data(empty_table)

        # Should return current snapshot ID (or 0 if none)
        current = storage.get_current_snapshot_id()
        if current:
            assert snapshot_id == current
        else:
            assert snapshot_id == 0

    def test_append_multiple_batches(self, storage, sample_arrow_table):
        """Test appending multiple batches"""
        storage.initialize()

        snapshot_id1 = storage.append_data(sample_arrow_table)
        snapshot_id2 = storage.append_data(sample_arrow_table)
        snapshot_id3 = storage.append_data(sample_arrow_table)

        # Each append creates a new snapshot (IDs are unique, not sequential)
        assert snapshot_id1 > 0
        assert snapshot_id2 != snapshot_id1
        assert snapshot_id3 != snapshot_id2
        assert snapshot_id3 != snapshot_id1

    def test_get_snapshot_history_returns_snapshots(self, storage, sample_arrow_table):
        """Test get_snapshot_history() returns snapshots"""
        storage.initialize()

        # Initially no snapshots
        snapshots = storage.get_snapshot_history()
        assert snapshots == []

        # Add data
        storage.append_data(sample_arrow_table)
        storage.append_data(sample_arrow_table)

        snapshots = storage.get_snapshot_history()
        assert len(snapshots) == 2

    def test_snapshot_history_sorted_newest_first(self, storage, sample_arrow_table):
        """Test snapshot history is sorted newest first"""
        storage.initialize()

        snapshot_id1 = storage.append_data(sample_arrow_table)
        snapshot_id2 = storage.append_data(sample_arrow_table)

        snapshots = storage.get_snapshot_history()

        assert snapshots[0]["snapshot_id"] == snapshot_id2
        assert snapshots[1]["snapshot_id"] == snapshot_id1

    def test_snapshot_history_contains_metadata(self, storage, sample_arrow_table):
        """Test snapshot history contains required metadata"""
        storage.initialize()

        storage.append_data(sample_arrow_table)

        snapshots = storage.get_snapshot_history()
        snapshot = snapshots[0]

        assert "snapshot_id" in snapshot
        assert "timestamp_ms" in snapshot
        assert "timestamp" in snapshot
        assert "operation" in snapshot
        assert "summary" in snapshot
        assert isinstance(snapshot["timestamp"], datetime)

    def test_get_current_snapshot_id_none_initially(self, storage):
        """Test get_current_snapshot_id() returns None initially"""
        storage.initialize()

        snapshot_id = storage.get_current_snapshot_id()
        assert snapshot_id is None

    def test_get_current_snapshot_id_after_append(self, storage, sample_arrow_table):
        """Test get_current_snapshot_id() after append"""
        storage.initialize()

        appended_id = storage.append_data(sample_arrow_table)
        current_id = storage.get_current_snapshot_id()

        assert current_id == appended_id

    def test_get_table_stats_returns_statistics(self, storage, sample_arrow_table):
        """Test get_table_stats() returns statistics"""
        storage.initialize()

        storage.append_data(sample_arrow_table)

        stats = storage.get_table_stats()

        assert "total_snapshots" in stats
        assert "current_snapshot_id" in stats
        assert "schema_fields" in stats
        assert "partition_fields" in stats
        assert "table_location" in stats

    def test_table_stats_schema_fields(self, storage):
        """Test get_table_stats() includes schema fields"""
        storage.initialize()

        stats = storage.get_table_stats()

        assert "tile_id" in stats["schema_fields"]
        assert "band" in stats["schema_fields"]
        assert "time" in stats["schema_fields"]
        assert "pixels" in stats["schema_fields"]

    def test_table_stats_partition_fields(self, storage):
        """Test get_table_stats() includes partition fields"""
        storage.initialize()

        stats = storage.get_table_stats()

        # Should be partitioned by tile_id, band, year_month
        assert "tile_id" in stats["partition_fields"]
        assert "band" in stats["partition_fields"]
        assert "year_month" in stats["partition_fields"]

    def test_table_stats_snapshot_count(self, storage, sample_arrow_table):
        """Test get_table_stats() tracks snapshot count"""
        storage.initialize()

        stats = storage.get_table_stats()
        assert stats["total_snapshots"] == 0

        storage.append_data(sample_arrow_table)
        stats = storage.get_table_stats()
        assert stats["total_snapshots"] == 1

        storage.append_data(sample_arrow_table)
        stats = storage.get_table_stats()
        assert stats["total_snapshots"] == 2

    def test_overwrite_data(self, storage, sample_arrow_table):
        """Test overwrite_data() operation"""
        storage.initialize()

        # Append initial data
        storage.append_data(sample_arrow_table)

        # Overwrite
        snapshot_id = storage.overwrite_data(sample_arrow_table)

        assert snapshot_id > 0
        assert storage.get_current_snapshot_id() == snapshot_id

    def test_expire_snapshots_with_few_snapshots(self, storage, sample_arrow_table):
        """Test expire_snapshots() retains minimum snapshots"""
        storage.initialize()

        # Create only 2 snapshots
        storage.append_data(sample_arrow_table)
        storage.append_data(sample_arrow_table)

        # Try to expire with retain_last=10
        expired_count = storage.expire_snapshots(retain_last=10)

        # Should not expire anything (only 2 snapshots < 10)
        assert expired_count == 0

    def test_expire_snapshots_calculation(self, storage, sample_arrow_table):
        """Test expire_snapshots() calculates expiration correctly"""
        storage.initialize()

        # Create 15 snapshots
        for _ in range(15):
            storage.append_data(sample_arrow_table)

        # Use a timestamp in the future to ensure all snapshots are "old enough"
        # This is a calculation-only method, not actual expiration
        from datetime import datetime, timezone
        future_timestamp_ms = int((datetime.now(timezone.utc).timestamp() + 3600) * 1000)

        expired_count = storage.expire_snapshots(
            older_than_ms=future_timestamp_ms,  # All current snapshots are older than this
            retain_last=10
        )

        # Should calculate 5 to expire (15 total - 10 to retain)
        assert expired_count == 5


class TestIcebergStorageManagerIntegration:
    """Integration tests for IcebergStorageManager"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_full_workflow(self, temp_warehouse):
        """Test complete workflow: initialize, write, read snapshots"""
        storage = IcebergStorageManager(temp_warehouse)
        storage.initialize()

        # Create sample data
        schema = storage.table.schema().as_arrow()

        data1 = pa.Table.from_pydict({
            "tile_id": ["x0024_y0041"],
            "band": ["red"],
            "year_month": ["2024-01"],
            "time": [datetime(2024, 1, 15, tzinfo=timezone.utc)],
            "pixels": [[100, 200, 300, 400]],
            "mask": [[True, True, False, True]],
            "product_id": ["sentinel2_l2a"],
            "resolution": [10.0],
            "bounds_wkb": [None],
            "num_pixels": [4],
            "min_value": [100.0],
            "max_value": [300.0],
            "mean_value": [200.0],
            "cloud_cover": [0.1],
            "crs": ["EPSG:32632"],
            "transform": [None],
            "source_file": [None],
            "ingestion_time": [datetime.now(timezone.utc)],
        }, schema=schema)

        data2 = pa.Table.from_pydict({
            "tile_id": ["x0024_y0041"],
            "band": ["nir"],
            "year_month": ["2024-01"],
            "time": [datetime(2024, 1, 15, tzinfo=timezone.utc)],
            "pixels": [[500, 600, 700, 800]],
            "mask": [[True, True, True, False]],
            "product_id": ["sentinel2_l2a"],
            "resolution": [10.0],
            "bounds_wkb": [None],
            "num_pixels": [4],
            "min_value": [500.0],
            "max_value": [700.0],
            "mean_value": [600.0],
            "cloud_cover": [0.1],
            "crs": ["EPSG:32632"],
            "transform": [None],
            "source_file": [None],
            "ingestion_time": [datetime.now(timezone.utc)],
        }, schema=schema)

        # Write data
        snapshot_id1 = storage.append_data(data1)
        snapshot_id2 = storage.append_data(data2)

        # Verify snapshots
        snapshots = storage.get_snapshot_history()
        assert len(snapshots) == 2
        assert snapshots[0]["snapshot_id"] == snapshot_id2
        assert snapshots[1]["snapshot_id"] == snapshot_id1

        # Verify stats
        stats = storage.get_table_stats()
        assert stats["total_snapshots"] == 2
        assert stats["current_snapshot_id"] == snapshot_id2

    def test_persistence_across_instances(self, temp_warehouse):
        """Test that data persists across storage instances"""
        # Create first instance and write data
        storage1 = IcebergStorageManager(temp_warehouse)
        storage1.initialize()

        schema = storage1.table.schema().as_arrow()
        data = pa.Table.from_pydict({
            "tile_id": ["x0024_y0041"],
            "band": ["red"],
            "year_month": ["2024-01"],
            "time": [datetime(2024, 1, 15, tzinfo=timezone.utc)],
            "pixels": [[100, 200, 300, 400]],
            "mask": [[True, True, False, True]],
            "product_id": ["sentinel2_l2a"],
            "resolution": [10.0],
            "bounds_wkb": [None],
            "num_pixels": [4],
            "min_value": [100.0],
            "max_value": [300.0],
            "mean_value": [200.0],
            "cloud_cover": [0.1],
            "crs": ["EPSG:32632"],
            "transform": [None],
            "source_file": [None],
            "ingestion_time": [datetime.now(timezone.utc)],
        }, schema=schema)

        snapshot_id = storage1.append_data(data)

        # Create second instance and verify data
        storage2 = IcebergStorageManager(temp_warehouse)
        storage2.initialize()

        assert storage2.get_current_snapshot_id() == snapshot_id

        snapshots = storage2.get_snapshot_history()
        assert len(snapshots) == 1
        assert snapshots[0]["snapshot_id"] == snapshot_id
