"""
Tests for RecoveryTool
"""

import shutil
import tempfile
from datetime import UTC, datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from pixelquery.io.iceberg_writer import IcebergPixelWriter
from pixelquery.util.recovery import RecoveryTool


class TestRecoveryTool:
    """Test RecoveryTool class"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def tool(self, temp_warehouse):
        """Create RecoveryTool instance"""
        return RecoveryTool(temp_warehouse)

    def test_init(self, tool, temp_warehouse):
        """Test RecoveryTool initialization"""
        assert tool.warehouse_path == Path(temp_warehouse)

    def test_diagnose_empty_warehouse(self, tool):
        """Test diagnose() on empty warehouse"""
        issues = tool.diagnose()

        assert "orphaned_temp_files" in issues
        assert "storage_stats" in issues
        assert "backend_info" in issues
        assert "health_status" in issues
        assert "warnings" in issues

        assert issues["health_status"] == "healthy"
        assert len(issues["orphaned_temp_files"]) == 0

    def test_diagnose_returns_health_status(self, tool):
        """Test diagnose() returns health status"""
        issues = tool.diagnose()

        assert issues["health_status"] in ["healthy", "warning", "error"]

    def test_diagnose_with_temp_files(self, temp_warehouse, tool):
        """Test diagnose() detects orphaned temp files"""
        # Create temp files
        temp_file1 = Path(temp_warehouse) / "file1.tmp"
        temp_file2 = Path(temp_warehouse) / "subdir" / "file2.tmp"
        temp_file2.parent.mkdir(exist_ok=True)

        temp_file1.touch()
        temp_file2.touch()

        issues = tool.diagnose()

        assert len(issues["orphaned_temp_files"]) == 2
        assert issues["health_status"] == "warning"
        assert any("orphaned temp files" in w for w in issues["warnings"])

    def test_diagnose_storage_stats(self, temp_warehouse, tool):
        """Test diagnose() returns storage statistics"""
        # Create some files
        test_file = Path(temp_warehouse) / "test.parquet"
        test_file.write_bytes(b"x" * 1024 * 100)  # 100 KB

        issues = tool.diagnose()

        stats = issues["storage_stats"]
        assert "total_bytes" in stats
        assert "total_mb" in stats
        assert "parquet_bytes" in stats
        assert "arrow_bytes" in stats
        assert "file_count" in stats

        assert stats["total_bytes"] > 0
        assert stats["parquet_bytes"] > 0
        assert stats["file_count"] > 0

    def test_diagnose_backend_info_uninitialized(self, tool):
        """Test diagnose() detects uninitialized warehouse"""
        issues = tool.diagnose()

        assert issues["backend_info"]["type"] == "uninitialized"
        assert any("No catalog found" in w for w in issues["warnings"])

    def test_diagnose_backend_info_iceberg(self, temp_warehouse):
        """Test diagnose() detects Iceberg backend"""
        # Initialize Iceberg warehouse
        writer = IcebergPixelWriter(temp_warehouse)
        writer.write_observation(
            tile_id="x0024_y0041",
            band="red",
            time=datetime(2024, 1, 15, tzinfo=UTC),
            pixels=np.array([[100, 200], [300, 400]], dtype=np.uint16),
            mask=np.array([[True, True], [False, True]], dtype=bool),
            product_id="sentinel2_l2a",
            resolution=10.0,
        )

        tool = RecoveryTool(temp_warehouse)
        issues = tool.diagnose()

        assert issues["backend_info"]["type"] == "iceberg"
        assert "catalog_db_size_mb" in issues["backend_info"]
        assert "snapshot_count" in issues["backend_info"]
        assert issues["backend_info"]["snapshot_count"] > 0

    def test_diagnose_backend_info_arrow(self, temp_warehouse):
        """Test diagnose() detects Arrow backend"""
        # Create Arrow metadata file
        metadata_path = Path(temp_warehouse) / "metadata.parquet"
        metadata_path.write_bytes(b"dummy")

        tool = RecoveryTool(temp_warehouse)
        issues = tool.diagnose()

        assert issues["backend_info"]["type"] == "arrow"
        assert "metadata_size_mb" in issues["backend_info"]

    def test_repair_cleanup_temp_true(self, temp_warehouse, tool):
        """Test repair() cleans up temp files"""
        # Create temp files
        temp_file1 = Path(temp_warehouse) / "file1.tmp"
        temp_file2 = Path(temp_warehouse) / "file2.tmp"
        temp_file1.touch()
        temp_file2.touch()

        result = tool.repair(cleanup_temp=True)

        assert result["temp_files_removed"] == 2
        assert len(result["actions_taken"]) >= 2
        assert not temp_file1.exists()
        assert not temp_file2.exists()

    def test_repair_cleanup_temp_false(self, temp_warehouse, tool):
        """Test repair() skips cleanup when cleanup_temp=False"""
        # Create temp files
        temp_file = Path(temp_warehouse) / "file.tmp"
        temp_file.touch()

        result = tool.repair(cleanup_temp=False)

        assert result["temp_files_removed"] == 0
        assert temp_file.exists()

    def test_repair_orphaned_data(self, tool):
        """Test repair() with cleanup_orphaned_data"""
        result = tool.repair(cleanup_orphaned_data=True)

        # Currently not implemented, should skip
        assert result["orphaned_data_removed"] == 0
        assert any("skipped" in action for action in result["actions_taken"])

    def test_get_retry_info_iceberg(self, temp_warehouse):
        """Test get_retry_info() for Iceberg backend"""
        # Initialize Iceberg warehouse
        writer = IcebergPixelWriter(temp_warehouse)
        writer.write_observation(
            tile_id="x0024_y0041",
            band="red",
            time=datetime(2024, 1, 15, tzinfo=UTC),
            pixels=np.array([[100, 200], [300, 400]], dtype=np.uint16),
            mask=np.array([[True, True], [False, True]], dtype=bool),
            product_id="sentinel2_l2a",
            resolution=10.0,
        )

        tool = RecoveryTool(temp_warehouse)
        info = tool.get_retry_info()

        assert info["backend"] == "iceberg"
        assert "message" in info
        assert "automatically" in info["message"].lower()

    def test_get_retry_info_arrow(self, temp_warehouse):
        """Test get_retry_info() for Arrow backend"""
        # Create Arrow metadata
        metadata_path = Path(temp_warehouse) / "metadata.parquet"
        metadata_path.write_bytes(b"dummy")

        tool = RecoveryTool(temp_warehouse)
        info = tool.get_retry_info()

        assert info["backend"] == "arrow"
        assert "message" in info
        assert "does not support automatic recovery" in info["message"].lower()

    def test_verify_integrity_warehouse_not_exists(self):
        """Test verify_integrity() with non-existent warehouse"""
        tool = RecoveryTool("/nonexistent/warehouse")
        result = tool.verify_integrity()

        assert result["status"] == "error"
        assert len(result["errors"]) > 0
        assert "does not exist" in result["errors"][0]

    def test_verify_integrity_empty_warehouse(self, tool):
        """Test verify_integrity() on empty warehouse"""
        result = tool.verify_integrity()

        assert result["status"] == "warning"
        assert "Warehouse directory exists" in result["checks"]
        assert any("No catalog found" in check for check in result["checks"])

    def test_verify_integrity_iceberg(self, temp_warehouse):
        """Test verify_integrity() with Iceberg backend"""
        # Initialize Iceberg warehouse
        writer = IcebergPixelWriter(temp_warehouse)
        writer.write_observation(
            tile_id="x0024_y0041",
            band="red",
            time=datetime(2024, 1, 15, tzinfo=UTC),
            pixels=np.array([[100, 200], [300, 400]], dtype=np.uint16),
            mask=np.array([[True, True], [False, True]], dtype=bool),
            product_id="sentinel2_l2a",
            resolution=10.0,
        )

        tool = RecoveryTool(temp_warehouse)
        result = tool.verify_integrity()

        assert result["status"] == "healthy"
        assert "Warehouse directory exists" in result["checks"]
        assert any("Iceberg catalog found" in check for check in result["checks"])
        assert any("Iceberg table readable" in check for check in result["checks"])

    def test_verify_integrity_arrow(self, temp_warehouse):
        """Test verify_integrity() with Arrow backend"""
        # Create valid Arrow metadata
        from pixelquery._internal.storage.geoparquet import GeoParquetWriter, TileMetadata

        metadata_path = Path(temp_warehouse) / "metadata.parquet"
        writer = GeoParquetWriter()
        metadata = TileMetadata(
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
        writer.write_metadata([metadata], str(metadata_path))

        tool = RecoveryTool(temp_warehouse)
        result = tool.verify_integrity()

        assert result["status"] == "healthy"
        assert "Warehouse directory exists" in result["checks"]
        assert any("Arrow metadata found" in check for check in result["checks"])
        assert any("GeoParquet metadata readable" in check for check in result["checks"])


class TestRecoveryToolIntegration:
    """Integration tests for RecoveryTool"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_diagnose_and_repair_workflow(self, temp_warehouse):
        """Test complete diagnose and repair workflow"""
        # Create warehouse with temp files
        temp_file1 = Path(temp_warehouse) / "file1.tmp"
        temp_file2 = Path(temp_warehouse) / "subdir" / "file2.tmp"
        temp_file2.parent.mkdir(exist_ok=True)
        temp_file1.touch()
        temp_file2.touch()

        # Initialize Iceberg data
        writer = IcebergPixelWriter(temp_warehouse)
        writer.write_observation(
            tile_id="x0024_y0041",
            band="red",
            time=datetime(2024, 1, 15, tzinfo=UTC),
            pixels=np.array([[100, 200], [300, 400]], dtype=np.uint16),
            mask=np.array([[True, True], [False, True]], dtype=bool),
            product_id="sentinel2_l2a",
            resolution=10.0,
        )

        # Diagnose
        tool = RecoveryTool(temp_warehouse)
        issues = tool.diagnose()

        assert issues["health_status"] == "warning"
        assert len(issues["orphaned_temp_files"]) == 2
        assert issues["backend_info"]["type"] == "iceberg"

        # Repair
        result = tool.repair(cleanup_temp=True)
        assert result["temp_files_removed"] == 2

        # Re-diagnose
        issues = tool.diagnose()
        assert issues["health_status"] == "healthy"
        assert len(issues["orphaned_temp_files"]) == 0

    def test_verify_after_recovery(self, temp_warehouse):
        """Test integrity verification after recovery"""
        # Create warehouse with issues
        temp_file = Path(temp_warehouse) / "file.tmp"
        temp_file.touch()

        # Initialize Iceberg
        writer = IcebergPixelWriter(temp_warehouse)
        writer.write_observation(
            tile_id="x0024_y0041",
            band="red",
            time=datetime(2024, 1, 15, tzinfo=UTC),
            pixels=np.array([[100, 200], [300, 400]], dtype=np.uint16),
            mask=np.array([[True, True], [False, True]], dtype=bool),
            product_id="sentinel2_l2a",
            resolution=10.0,
        )

        tool = RecoveryTool(temp_warehouse)

        # Verify before repair
        result = tool.verify_integrity()
        assert result["status"] == "healthy"  # Temp files don't affect integrity

        # Repair
        tool.repair(cleanup_temp=True)

        # Verify after repair
        result = tool.verify_integrity()
        assert result["status"] == "healthy"
        assert len(result["errors"]) == 0
