"""Tests for COG metadata extraction."""

from pathlib import Path

import pytest


class TestCOGMetadata:
    """Test COG metadata dataclass."""

    def test_repr(self):
        from pixelquery.io.cog_metadata import COGMetadata

        meta = COGMetadata(
            path="/tmp/test.tif",
            crs="EPSG:32652",
            bounds=(127.0, 35.0, 127.5, 35.5),
            width=1000,
            height=1000,
            band_count=4,
            dtype="uint16",
            resolution=(3.0, 3.0),
            nodata=0.0,
            driver="GTiff",
        )
        assert "test.tif" in repr(meta)
        assert "1000x1000" in repr(meta)
        assert "4 bands" in repr(meta)


@pytest.mark.integration
class TestInspectCOG:
    """Integration tests for COG inspection."""

    def test_inspect_sample_cog(self, tmp_path):
        try:
            from pixelquery.io.cog_metadata import inspect_cog
            from pixelquery.sample_data import create_sample_data
        except ImportError:
            pytest.skip("Dependencies not installed")

        cog_dir = create_sample_data(str(tmp_path / "cogs"))
        cog_path = str(Path(cog_dir) / "2024-01-15_sample_sr.tif")

        meta = inspect_cog(cog_path)
        assert meta.band_count == 4
        assert meta.width == 64
        assert meta.height == 64
        assert meta.crs is not None
        assert meta.dtype == "uint16"

    def test_inspect_directory(self, tmp_path):
        try:
            from pixelquery.io.cog_metadata import inspect_directory
            from pixelquery.sample_data import create_sample_data
        except ImportError:
            pytest.skip("Dependencies not installed")

        cog_dir = create_sample_data(str(tmp_path / "cogs"))

        metas = inspect_directory(cog_dir)
        assert len(metas) == 3
        for m in metas:
            assert m.band_count == 4
