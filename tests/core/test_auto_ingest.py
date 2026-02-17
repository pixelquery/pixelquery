"""Tests for the auto-ingest convenience function."""

from datetime import UTC, datetime, timezone
from pathlib import Path

import pytest


class TestParseDateFromFilename:
    """Test filename date parsing."""

    def test_iso_date(self):
        from pixelquery.io.auto_ingest import _parse_date_from_filename

        result = _parse_date_from_filename("2024-01-15_scene_sr.tiff")
        assert result == datetime(2024, 1, 15, tzinfo=UTC)

    def test_compact_date(self):
        from pixelquery.io.auto_ingest import _parse_date_from_filename

        result = _parse_date_from_filename("20240115_scene.tif")
        assert result == datetime(2024, 1, 15, tzinfo=UTC)

    def test_underscore_date(self):
        from pixelquery.io.auto_ingest import _parse_date_from_filename

        result = _parse_date_from_filename("2024_01_15_scene.tif")
        assert result == datetime(2024, 1, 15, tzinfo=UTC)

    def test_no_date(self):
        from pixelquery.io.auto_ingest import _parse_date_from_filename

        result = _parse_date_from_filename("scene_abc.tif")
        assert result is None

    def test_custom_pattern(self):
        from pixelquery.io.auto_ingest import _parse_date_from_filename

        result = _parse_date_from_filename("S2A_20240115T103021.tif", date_pattern=r"(\d{8})T")
        assert result == datetime(2024, 1, 15, tzinfo=UTC)

    def test_date_in_path(self):
        from pixelquery.io.auto_ingest import _parse_date_from_filename

        result = _parse_date_from_filename("2024-06-15_B04_10m.tiff")
        assert result == datetime(2024, 6, 15, tzinfo=UTC)


class TestIngestResult:
    """Test IngestResult dataclass."""

    def test_repr(self):
        from pixelquery.io.auto_ingest import IngestResult

        result = IngestResult(
            scene_count=10,
            group_names=["a", "b"],
            warehouse_path="/tmp/wh",
            elapsed=1.5,
        )
        assert "10 scenes" in repr(result)
        assert "1.5s" in repr(result)

    def test_repr_with_errors(self):
        from pixelquery.io.auto_ingest import IngestResult

        result = IngestResult(
            scene_count=8,
            group_names=[],
            warehouse_path="/tmp/wh",
            elapsed=2.0,
            errors=["err1", "err2"],
        )
        assert "2 errors" in repr(result)


@pytest.mark.integration
class TestIngestWithSampleData:
    """Integration tests using sample data."""

    def test_ingest_sample_data(self, tmp_path):
        """Test full ingest pipeline with synthetic COGs."""
        try:
            from pixelquery.io.auto_ingest import ingest
            from pixelquery.sample_data import create_sample_data
        except ImportError:
            pytest.skip("Icechunk dependencies not installed")

        # Create sample COGs
        cog_dir = create_sample_data(str(tmp_path / "cogs"))
        warehouse = str(tmp_path / "warehouse")

        # Ingest
        result = ingest(
            cog_dir,
            warehouse=warehouse,
            band_names=["blue", "green", "red", "nir"],
            product_id="sample",
        )

        assert result.scene_count == 3
        assert len(result.group_names) == 3
        assert result.elapsed > 0
        assert not result.errors

    def test_ingest_auto_band_names(self, tmp_path):
        """Test auto-detection of band names."""
        try:
            from pixelquery.io.auto_ingest import ingest
            from pixelquery.sample_data import create_sample_data
        except ImportError:
            pytest.skip("Icechunk dependencies not installed")

        cog_dir = create_sample_data(str(tmp_path / "cogs"))
        warehouse = str(tmp_path / "warehouse")

        # No band_names provided - should auto-detect
        result = ingest(cog_dir, warehouse=warehouse)
        assert result.scene_count == 3
