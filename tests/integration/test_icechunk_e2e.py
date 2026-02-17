"""
End-to-end test for Icechunk virtual zarr integration.

Tests the full pipeline:
1. Ingest COGs as virtual references
2. Query with time/spatial/band filters
3. Verify data correctness against rasterio
4. Time Travel via snapshots
"""

import re
import shutil
import tempfile
from datetime import UTC, datetime, timezone
from pathlib import Path

import numpy as np
import pytest

# Skip all tests if saefarm data not available
SAEFARM_DIR = Path("/Users/sonhoyoung/saefarm/arps")
pytestmark = pytest.mark.skipif(
    not SAEFARM_DIR.exists(),
    reason="Saefarm data not available",
)


def _find_cog_files(limit=5):
    """Find COG files with acquisition dates."""
    cogs = []
    for date_dir in sorted(SAEFARM_DIR.iterdir()):
        if not date_dir.is_dir():
            continue
        for f in date_dir.glob("*_sr.tiff"):
            m = re.match(r"(\d{4}-\d{2}-\d{2})_", f.name)
            if m:
                acq_time = datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=UTC)
                cogs.append({"path": str(f), "time": acq_time})
                if len(cogs) >= limit:
                    return cogs
    return cogs


@pytest.fixture
def repo_dir():
    """Create a temporary directory for the Icechunk repo."""
    d = tempfile.mkdtemp(prefix="pq_icechunk_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


class TestIcechunkE2E:
    """End-to-end Icechunk integration tests."""

    def test_ingest_single_cog(self, repo_dir):
        """Test ingesting a single COG as virtual references."""
        from pixelquery.io.ingest import IcechunkIngestionPipeline

        cogs = _find_cog_files(limit=1)
        assert len(cogs) >= 1, "No COG files found"

        pipeline = IcechunkIngestionPipeline(repo_dir)
        group_name = pipeline.ingest_cog(
            cog_path=cogs[0]["path"],
            acquisition_time=cogs[0]["time"],
            product_id="planet_sr",
            band_names=["red", "green", "blue", "nir"],
        )

        assert group_name.startswith("scene_")
        assert (Path(repo_dir) / ".icechunk").exists()

    def test_ingest_batch_and_query(self, repo_dir):
        """Test batch ingest + query with open_xarray."""
        import pixelquery as pq
        from pixelquery.io.ingest import IcechunkIngestionPipeline

        cogs = _find_cog_files(limit=3)
        assert len(cogs) >= 2, "Need at least 2 COGs"

        pipeline = IcechunkIngestionPipeline(repo_dir)
        cog_infos = [
            {
                "cog_path": c["path"],
                "acquisition_time": c["time"],
                "product_id": "planet_sr",
                "band_names": ["red", "green", "blue", "nir"],
            }
            for c in cogs
        ]
        group_names = pipeline.ingest_cogs(cog_infos, message="Test batch")
        assert len(group_names) == len(cogs)

        # Query all scenes
        ds = pq.open_xarray(repo_dir)
        assert "data" in ds
        assert "time" in ds.dims
        assert ds.sizes["time"] == len(cogs)

    def test_data_correctness(self, repo_dir):
        """Verify virtual read matches rasterio direct read."""
        import rasterio

        import pixelquery as pq
        from pixelquery.io.ingest import IcechunkIngestionPipeline

        cogs = _find_cog_files(limit=1)
        assert len(cogs) >= 1

        pipeline = IcechunkIngestionPipeline(repo_dir)
        pipeline.ingest_cog(
            cog_path=cogs[0]["path"],
            acquisition_time=cogs[0]["time"],
            product_id="planet_sr",
            band_names=["red", "green", "blue", "nir"],
        )

        # Read via Icechunk
        ds = pq.open_xarray(repo_dir)
        icechunk_data = ds["data"].values  # shape: (band, y, x)

        # Read via rasterio
        with rasterio.open(cogs[0]["path"]) as src:
            rio_data = src.read()  # shape: (band, y, x)

        assert np.array_equal(icechunk_data, rio_data), "Data mismatch!"

    def test_time_travel(self, repo_dir):
        """Test snapshot-based Time Travel."""
        import pixelquery as pq
        from pixelquery._internal.storage.icechunk_storage import IcechunkStorageManager
        from pixelquery.io.icechunk_reader import IcechunkVirtualReader
        from pixelquery.io.ingest import IcechunkIngestionPipeline

        cogs = _find_cog_files(limit=2)
        assert len(cogs) >= 2

        # Ingest first COG
        pipeline = IcechunkIngestionPipeline(repo_dir)
        pipeline.ingest_cog(
            cog_path=cogs[0]["path"],
            acquisition_time=cogs[0]["time"],
            product_id="planet_sr",
            band_names=["red", "green", "blue", "nir"],
        )

        # Get snapshot after first ingest
        storage = IcechunkStorageManager(repo_dir)
        storage.initialize()
        history = storage.get_snapshot_history()
        first_snapshot = history[0]["snapshot_id"]

        # Ingest second COG
        pipeline.ingest_cog(
            cog_path=cogs[1]["path"],
            acquisition_time=cogs[1]["time"],
            product_id="planet_sr",
            band_names=["red", "green", "blue", "nir"],
        )

        # Current state should have 2 scenes
        reader = IcechunkVirtualReader(storage)
        current_scenes = reader.list_scenes()
        assert len(current_scenes) == 2

        # Time Travel to first snapshot should have 1 scene
        old_scenes = reader.list_scenes(snapshot_id=first_snapshot)
        assert len(old_scenes) == 1

    def test_band_filtering(self, repo_dir):
        """Test selecting specific bands."""
        import pixelquery as pq
        from pixelquery.io.ingest import IcechunkIngestionPipeline

        cogs = _find_cog_files(limit=1)
        assert len(cogs) >= 1

        pipeline = IcechunkIngestionPipeline(repo_dir)
        pipeline.ingest_cog(
            cog_path=cogs[0]["path"],
            acquisition_time=cogs[0]["time"],
            product_id="planet_sr",
            band_names=["red", "green", "blue", "nir"],
        )

        # Query with band filter
        ds = pq.open_xarray(repo_dir, bands=["red", "nir"])
        band_values = list(ds["data"].coords["band"].values)
        assert band_values == ["red", "nir"]

    def test_catalog_autodetect(self, repo_dir):
        """Test that LocalCatalog.create() auto-detects Icechunk."""
        from pixelquery.catalog.local import LocalCatalog
        from pixelquery.io.ingest import IcechunkIngestionPipeline

        cogs = _find_cog_files(limit=1)
        assert len(cogs) >= 1

        pipeline = IcechunkIngestionPipeline(repo_dir)
        pipeline.ingest_cog(
            cog_path=cogs[0]["path"],
            acquisition_time=cogs[0]["time"],
            product_id="planet_sr",
            band_names=["red", "green", "blue", "nir"],
        )

        catalog = LocalCatalog.create(repo_dir)
        from pixelquery.catalog.icechunk_catalog import IcechunkCatalog

        assert isinstance(catalog, IcechunkCatalog)


@pytest.mark.slow
class TestIcechunkFullScale:
    """Full-scale tests with all 243 COGs."""

    def test_full_243_cogs(self, repo_dir):
        """Ingest all 243 COGs and verify."""
        import time

        import pixelquery as pq
        from pixelquery.io.ingest import IcechunkIngestionPipeline

        cogs = _find_cog_files(limit=243)
        assert len(cogs) > 100, f"Expected 200+ COGs, got {len(cogs)}"

        pipeline = IcechunkIngestionPipeline(repo_dir)

        start = time.perf_counter()
        cog_infos = [
            {
                "cog_path": c["path"],
                "acquisition_time": c["time"],
                "product_id": "planet_sr",
                "band_names": ["red", "green", "blue", "nir"],
            }
            for c in cogs
        ]
        group_names = pipeline.ingest_cogs(cog_infos, message="Full scale test")
        elapsed = time.perf_counter() - start

        assert len(group_names) == len(cogs)
        print(
            f"\n  Ingested {len(cogs)} COGs in {elapsed:.2f}s ({elapsed / len(cogs) * 1000:.1f}ms/COG)"
        )

        # Query all
        ds = pq.open_xarray(repo_dir)
        assert ds.sizes["time"] == len(cogs)
        print(f"  Dataset: {dict(ds.sizes)}")
