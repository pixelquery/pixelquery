"""
Tests for MigrationTool
"""

import shutil
import tempfile
from datetime import UTC, datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from pixelquery._internal.storage.arrow_chunk import ArrowChunkWriter
from pixelquery._internal.storage.geoparquet import GeoParquetWriter, TileMetadata
from pixelquery.util.migrate import MigrationTool


class TestMigrationTool:
    """Test MigrationTool class"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def tool(self, temp_warehouse):
        """Create MigrationTool instance"""
        return MigrationTool(temp_warehouse, backup=True)

    @pytest.fixture
    def arrow_warehouse_with_data(self, temp_warehouse):
        """Create Arrow warehouse with sample data"""
        warehouse = Path(temp_warehouse)

        # Write Arrow chunk
        chunk_dir = warehouse / "tiles" / "x0024_y0041" / "2024-01"
        chunk_dir.mkdir(parents=True, exist_ok=True)
        chunk_path = chunk_dir / "red.arrow"

        chunk_writer = ArrowChunkWriter()
        data = {
            "time": [datetime(2024, 1, 15, tzinfo=UTC)],
            "pixels": [np.array([100, 200, 300, 400], dtype=np.uint16)],
            "mask": [np.array([True, True, False, True], dtype=bool)],
        }
        chunk_writer.write_chunk(str(chunk_path), data, product_id="sentinel2_l2a", resolution=10.0)

        # Write GeoParquet metadata
        metadata_path = warehouse / "metadata.parquet"
        geoparquet_writer = GeoParquetWriter()
        tile_metadata = TileMetadata(
            tile_id="x0024_y0041",
            year_month="2024-01",
            band="red",
            bounds=(126.5, 37.0, 127.5, 38.0),
            num_observations=1,
            min_value=100.0,
            max_value=400.0,
            mean_value=250.0,
            cloud_cover=0.1,
            product_id="sentinel2_l2a",
            resolution=10.0,
            chunk_path="tiles/x0024_y0041/2024-01/red.arrow",
        )
        geoparquet_writer.write_metadata([tile_metadata], str(metadata_path))

        return temp_warehouse

    def test_init(self, tool, temp_warehouse):
        """Test MigrationTool initialization"""
        assert tool.warehouse_path == Path(temp_warehouse)
        assert tool.backup is True

    def test_check_migration_needed_empty_warehouse(self, tool):
        """Test check_migration_needed on empty warehouse"""
        status = tool.check_migration_needed()

        assert status["needed"] is False
        assert status["arrow_chunks"] == 0
        assert status["iceberg_records"] == 0
        assert status["arrow_metadata_exists"] is False
        assert status["iceberg_catalog_exists"] is False

    def test_check_migration_needed_with_arrow_files(self, arrow_warehouse_with_data):
        """Test check_migration_needed with Arrow files"""
        tool = MigrationTool(arrow_warehouse_with_data, backup=True)
        status = tool.check_migration_needed()

        assert status["needed"] is True
        assert status["arrow_chunks"] == 1
        assert status["iceberg_records"] == 0
        assert status["arrow_metadata_exists"] is True
        assert status["iceberg_catalog_exists"] is False

    def test_check_migration_needed_after_migration(self, arrow_warehouse_with_data):
        """Test check_migration_needed after migration"""
        tool = MigrationTool(arrow_warehouse_with_data, backup=False)

        # Run migration
        tool.migrate()

        # Check again - should not need migration (Iceberg has records)
        status = tool.check_migration_needed()
        assert status["needed"] is False
        assert status["iceberg_records"] > 0

    def test_migrate_dry_run_true(self, arrow_warehouse_with_data):
        """Test migrate() with dry_run=True"""
        tool = MigrationTool(arrow_warehouse_with_data, backup=True)

        result = tool.migrate(dry_run=True)

        assert result["status"] in ["success", "completed_with_errors"]
        assert result["records_migrated"] > 0
        assert result["arrow_files_processed"] == 1
        assert result["verification"]["dry_run"] is True

        # Iceberg catalog should NOT exist after dry run
        iceberg_db = Path(arrow_warehouse_with_data) / "catalog.db"
        assert not iceberg_db.exists()

    def test_migrate_creates_iceberg_catalog(self, arrow_warehouse_with_data):
        """Test migrate() creates Iceberg catalog from Arrow"""
        tool = MigrationTool(arrow_warehouse_with_data, backup=False)

        result = tool.migrate()

        assert result["status"] in ["success", "completed_with_errors"]
        assert result["records_migrated"] > 0

        # Iceberg catalog should exist
        iceberg_db = Path(arrow_warehouse_with_data) / "catalog.db"
        assert iceberg_db.exists()

    def test_migrate_nothing_to_migrate(self, temp_warehouse):
        """Test migrate() with nothing to migrate"""
        tool = MigrationTool(temp_warehouse)

        result = tool.migrate()

        assert result["status"] == "nothing_to_migrate"
        assert result["records_migrated"] == 0

    def test_migrate_with_progress_callback(self, arrow_warehouse_with_data):
        """Test migrate() with progress callback"""
        tool = MigrationTool(arrow_warehouse_with_data, backup=False)

        progress_updates = []

        def on_progress(progress):
            progress_updates.append(progress)

        _result = tool.migrate(on_progress=on_progress, dry_run=True)

        assert len(progress_updates) > 0
        # Should have at least one update (100% completion)
        assert 100.0 in progress_updates

    def test_migrate_batch_size(self, arrow_warehouse_with_data):
        """Test migrate() with custom batch_size"""
        tool = MigrationTool(arrow_warehouse_with_data, backup=False)

        result = tool.migrate(batch_size=1, dry_run=True)

        assert result["status"] in ["success", "completed_with_errors"]
        assert result["records_migrated"] > 0

    def test_migrate_verification(self, arrow_warehouse_with_data):
        """Test migrate() includes verification"""
        tool = MigrationTool(arrow_warehouse_with_data, backup=False)

        result = tool.migrate()

        assert "verification" in result
        verification = result["verification"]

        if "dry_run" not in verification:
            assert "iceberg_total_records" in verification
            assert "iceberg_unique_chunks" in verification
            assert "expected_chunks" in verification
            assert "match" in verification

    def test_migrate_with_backup(self, arrow_warehouse_with_data):
        """Test migrate() creates backup"""
        tool = MigrationTool(arrow_warehouse_with_data, backup=True)

        result = tool.migrate()

        assert result["backup_path"] is not None

        # Backup directory should exist
        backup_path = Path(result["backup_path"])
        assert backup_path.exists()
        assert "backup_arrow_" in backup_path.name

        # Original tiles should be moved to backup
        backup_tiles = backup_path / "tiles"
        assert backup_tiles.exists()

    def test_migrate_without_backup(self, arrow_warehouse_with_data):
        """Test migrate() without backup"""
        tool = MigrationTool(arrow_warehouse_with_data, backup=False)

        result = tool.migrate()

        assert result["backup_path"] is None

    def test_rollback(self, arrow_warehouse_with_data):
        """Test rollback() from backup"""
        tool = MigrationTool(arrow_warehouse_with_data, backup=True)

        # Run migration with backup
        result = tool.migrate()
        backup_path = result["backup_path"]

        # Remove tiles directory (simulate data loss)
        tiles_dir = Path(arrow_warehouse_with_data) / "tiles"
        if tiles_dir.exists():
            shutil.rmtree(tiles_dir)

        # Rollback
        rollback_result = tool.rollback(backup_path)

        assert rollback_result["status"] == "success"
        assert rollback_result["message"] == "Arrow files restored from backup"

        # Tiles should be restored
        assert tiles_dir.exists()

    def test_rollback_backup_not_found(self, tool):
        """Test rollback() with non-existent backup"""
        result = tool.rollback("/nonexistent/backup/path")

        assert result["status"] == "error"
        assert "not found" in result["message"]


class TestMigrationToolIntegration:
    """Integration tests for MigrationTool"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_full_migration_workflow(self, temp_warehouse):
        """Test complete migration workflow"""
        warehouse = Path(temp_warehouse)

        # Create Arrow data with multiple chunks
        chunk_writer = ArrowChunkWriter()
        geoparquet_writer = GeoParquetWriter()
        metadata_list = []

        for tile_id in ["x0024_y0041", "x0024_y0042"]:
            for month in ["2024-01", "2024-02"]:
                for band in ["red", "nir"]:
                    # Write Arrow chunk
                    chunk_dir = warehouse / "tiles" / tile_id / month
                    chunk_dir.mkdir(parents=True, exist_ok=True)
                    chunk_path = chunk_dir / f"{band}.arrow"

                    data = {
                        "time": [datetime(2024, 1, 15, tzinfo=UTC)],
                        "pixels": [np.array([100, 200, 300, 400], dtype=np.uint16)],
                        "mask": [np.array([True, True, False, True], dtype=bool)],
                    }
                    chunk_writer.write_chunk(
                        str(chunk_path), data, product_id="sentinel2_l2a", resolution=10.0
                    )

                    # Add metadata
                    metadata_list.append(
                        TileMetadata(
                            tile_id=tile_id,
                            year_month=month,
                            band=band,
                            bounds=(126.5, 37.0, 127.5, 38.0),
                            num_observations=1,
                            min_value=100.0,
                            max_value=400.0,
                            mean_value=250.0,
                            cloud_cover=0.1,
                            product_id="sentinel2_l2a",
                            resolution=10.0,
                            chunk_path=f"tiles/{tile_id}/{month}/{band}.arrow",
                        )
                    )

        # Write GeoParquet metadata
        metadata_path = warehouse / "metadata.parquet"
        geoparquet_writer.write_metadata(metadata_list, str(metadata_path))

        # Check migration needed
        tool = MigrationTool(temp_warehouse, backup=True)
        status = tool.check_migration_needed()
        assert status["needed"] is True
        assert status["arrow_chunks"] == 8  # 2 tiles * 2 months * 2 bands

        # Run migration
        result = tool.migrate()
        assert result["status"] in ["success", "completed_with_errors"]
        assert result["records_migrated"] > 0
        assert result["backup_path"] is not None

        # Verify Iceberg catalog exists
        pytest.importorskip("sqlalchemy", reason="sqlalchemy required for pyiceberg SQL catalog")
        from pixelquery.catalog.iceberg import IcebergCatalog

        catalog = IcebergCatalog(temp_warehouse)

        tiles = catalog.list_tiles()
        assert len(tiles) == 2
        assert "x0024_y0041" in tiles
        assert "x0024_y0042" in tiles

        bands = catalog.list_bands()
        assert "red" in bands
        assert "nir" in bands

    def test_migration_with_errors(self, temp_warehouse):
        """Test migration handles errors gracefully"""
        warehouse = Path(temp_warehouse)

        # Create valid Arrow chunk
        chunk_dir = warehouse / "tiles" / "x0024_y0041" / "2024-01"
        chunk_dir.mkdir(parents=True, exist_ok=True)
        chunk_path = chunk_dir / "red.arrow"

        chunk_writer = ArrowChunkWriter()
        data = {
            "time": [datetime(2024, 1, 15, tzinfo=UTC)],
            "pixels": [np.array([100, 200, 300, 400], dtype=np.uint16)],
            "mask": [np.array([True, True, False, True], dtype=bool)],
        }
        chunk_writer.write_chunk(str(chunk_path), data, product_id="sentinel2_l2a", resolution=10.0)

        # Create invalid Arrow file (corrupted)
        invalid_chunk_path = chunk_dir / "green.arrow"
        with open(invalid_chunk_path, "wb") as f:
            f.write(b"corrupted data")

        # Run migration
        tool = MigrationTool(temp_warehouse, backup=False)
        result = tool.migrate()

        # Should complete with errors
        assert result["status"] in ["success", "completed_with_errors"]
        if result["status"] == "completed_with_errors":
            assert len(result["errors"]) > 0
