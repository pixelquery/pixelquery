"""
Integration tests for geo-coordinate queries, bbox crop, and GeoJSON clip.

Verifies that:
1. open_scene() assigns real geo-coordinates (not integer indices)
2. .sel(x=lon, y=lat, method="nearest") returns correct pixel values
3. crop() reduces spatial extent
4. clip() masks pixels outside polygon to NaN
5. timeseries() works with geo-coordinates
6. Multi-scene concat aligns on real coordinates
"""

import re
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

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
def repo_with_scene():
    """Create a temp repo with one ingested COG scene."""
    from pixelquery.io.ingest import IcechunkIngestionPipeline

    d = tempfile.mkdtemp(prefix="pq_spatial_test_")
    cogs = _find_cog_files(limit=1)
    assert len(cogs) >= 1, "No COG files found"

    pipeline = IcechunkIngestionPipeline(d)
    pipeline.ingest_cog(
        cog_path=cogs[0]["path"],
        acquisition_time=cogs[0]["time"],
        product_id="planet_sr",
        band_names=["red", "green", "blue", "nir"],
    )
    yield d, cogs[0]
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def repo_with_scenes():
    """Create a temp repo with 3 ingested COG scenes."""
    from pixelquery.io.ingest import IcechunkIngestionPipeline

    d = tempfile.mkdtemp(prefix="pq_spatial_multi_")
    cogs = _find_cog_files(limit=3)
    assert len(cogs) >= 2, "Need at least 2 COGs"

    pipeline = IcechunkIngestionPipeline(d)
    cog_infos = [
        {
            "cog_path": c["path"],
            "acquisition_time": c["time"],
            "product_id": "planet_sr",
            "band_names": ["red", "green", "blue", "nir"],
        }
        for c in cogs
    ]
    pipeline.ingest_cogs(cog_infos, message="Spatial test batch")
    yield d, cogs
    shutil.rmtree(d, ignore_errors=True)


class TestGeoCoordinates:
    """Test that open_scene and open_xarray assign real geo-coordinates."""

    def test_geo_coordinates(self, repo_with_scene):
        """open_scene returns x/y as real geo-coordinates, not integer indices."""
        import rasterio

        import pixelquery as pq

        repo_dir, cog_info = repo_with_scene

        ds = pq.open_xarray(repo_dir)
        x_vals = ds.coords["x"].values
        y_vals = ds.coords["y"].values

        # Coordinates should NOT be integer sequences 0,1,2,...
        assert not np.array_equal(x_vals, np.arange(len(x_vals)))
        assert not np.array_equal(y_vals, np.arange(len(y_vals)))

        # Verify against rasterio bounds
        with rasterio.open(cog_info["path"]) as src:
            rb = src.bounds
            # x range should be within rasterio bounds
            assert x_vals.min() >= rb.left
            assert x_vals.max() <= rb.right
            # y range should be within rasterio bounds
            assert y_vals.min() >= rb.bottom
            assert y_vals.max() <= rb.top
            # y should be descending (top→bottom)
            assert y_vals[0] > y_vals[-1]

    def test_point_query(self, repo_with_scene):
        """.sel(x=lon, y=lat, method='nearest') returns correct pixel value."""
        import rasterio

        import pixelquery as pq

        repo_dir, cog_info = repo_with_scene

        ds = pq.open_xarray(repo_dir)

        # Pick center coordinate
        cx = float(ds.coords["x"].values[ds.sizes["x"] // 2])
        cy = float(ds.coords["y"].values[ds.sizes["y"] // 2])

        # Select band first, then use method="nearest" for spatial dims only
        val = ds["data"].sel(band="nir").sel(x=cx, y=cy, method="nearest").values
        assert val.shape == ()  # scalar
        assert np.isfinite(val) or val == 0  # valid pixel

        # Cross-check with rasterio at same approximate location
        with rasterio.open(cog_info["path"]) as src:
            row, col = src.index(cx, cy)
            # Clamp to valid range
            row = max(0, min(row, src.height - 1))
            col = max(0, min(col, src.width - 1))
            nir_band_idx = 4  # nir is 4th band (1-indexed in rasterio)
            rio_val = src.read(nir_band_idx, window=rasterio.windows.Window(col, row, 1, 1))[0, 0]

        assert val == rio_val, f"Point query mismatch: icechunk={val}, rasterio={rio_val}"

    def test_bbox_crop(self, repo_with_scene):
        """crop() reduces pixel count to the bounding box."""
        import pixelquery as pq

        repo_dir, _cog_info = repo_with_scene

        ds = pq.open_xarray(repo_dir)
        full_nx = ds.sizes["x"]
        full_ny = ds.sizes["y"]

        # Crop to center quarter
        x_vals = ds.coords["x"].values
        y_vals = ds.coords["y"].values
        x_quarter = (x_vals.max() - x_vals.min()) / 4
        y_quarter = (y_vals.max() - y_vals.min()) / 4
        center_bounds = (
            x_vals.min() + x_quarter,
            y_vals.min() + y_quarter,
            x_vals.max() - x_quarter,
            y_vals.max() - y_quarter,
        )

        cropped = pq.crop(ds, center_bounds)
        assert cropped.sizes["x"] < full_nx
        assert cropped.sizes["y"] < full_ny
        assert cropped.sizes["x"] > 0
        assert cropped.sizes["y"] > 0

    def test_geojson_clip(self, repo_with_scene):
        """clip() masks pixels outside polygon to NaN."""
        import pixelquery as pq

        repo_dir, _cog_info = repo_with_scene

        ds = pq.open_xarray(repo_dir)
        x_vals = ds.coords["x"].values
        y_vals = ds.coords["y"].values

        # Build a small polygon in the center
        cx = (x_vals.min() + x_vals.max()) / 2
        cy = (y_vals.min() + y_vals.max()) / 2
        dx = (x_vals.max() - x_vals.min()) / 8
        dy = (y_vals.max() - y_vals.min()) / 8

        # Use a triangle so corners of the bbox crop fall outside the polygon
        geojson = {
            "type": "Polygon",
            "coordinates": [[
                [cx - dx, cy - dy],
                [cx + dx, cy - dy],
                [cx, cy + dy],
                [cx - dx, cy - dy],
            ]],
        }

        clipped = pq.clip(ds, geojson)

        # Should have some NaN values (outside polygon) and some valid values (inside)
        data = clipped["data"].sel(band="nir").values
        has_nan = np.isnan(data).any()
        has_valid = np.isfinite(data).any()
        assert has_nan, "Expected NaN outside polygon"
        assert has_valid, "Expected valid values inside polygon"

    def test_timeseries_coords(self, repo_with_scene):
        """pq.timeseries(lon, lat) works with geo-coordinates."""
        import pixelquery as pq

        repo_dir, _cog_info = repo_with_scene

        # Get actual coordinate range
        ds = pq.open_xarray(repo_dir)
        cx = float(ds.coords["x"].values[ds.sizes["x"] // 2])
        cy = float(ds.coords["y"].values[ds.sizes["y"] // 2])

        ts = pq.timeseries(repo_dir, lon=cx, lat=cy, bands=["nir"])
        assert "data" in ts
        # Single scene: should be a scalar per band (no time dim)
        assert "x" not in ts.dims
        assert "y" not in ts.dims

    def test_multi_scene_alignment(self, repo_with_scenes):
        """Multiple scenes with same extent concat with aligned coordinates."""
        import pixelquery as pq

        repo_dir, cogs = repo_with_scenes

        ds = pq.open_xarray(repo_dir)
        assert "time" in ds.dims
        assert ds.sizes["time"] == len(cogs)

        # x/y should be real coordinates, not integers
        x_vals = ds.coords["x"].values
        y_vals = ds.coords["y"].values
        assert not np.array_equal(x_vals, np.arange(len(x_vals)))
        assert not np.array_equal(y_vals, np.arange(len(y_vals)))

        # All time slices should share the same coordinates
        for t in range(ds.sizes["time"]):
            slice_ds = ds.isel(time=t)
            np.testing.assert_array_equal(slice_ds.coords["x"].values, x_vals)
            np.testing.assert_array_equal(slice_ds.coords["y"].values, y_vals)


class TestCrsTransformIntegration:
    """Test CRS transformation in crop/clip with real data."""

    def test_crop_with_explicit_crs(self, repo_with_scene):
        """crop(crs='EPSG:4326') works when dataset is also EPSG:4326."""
        import pixelquery as pq

        repo_dir, _cog_info = repo_with_scene

        ds = pq.open_xarray(repo_dir)
        x_vals = ds.coords["x"].values
        y_vals = ds.coords["y"].values

        center_bounds = (
            float(x_vals.min() + (x_vals.max() - x_vals.min()) / 4),
            float(y_vals.min() + (y_vals.max() - y_vals.min()) / 4),
            float(x_vals.max() - (x_vals.max() - x_vals.min()) / 4),
            float(y_vals.max() - (y_vals.max() - y_vals.min()) / 4),
        )

        # crs="EPSG:4326" is the default, saefarm data is also EPSG:4326
        cropped = pq.crop(ds, center_bounds, crs="EPSG:4326")
        assert cropped.sizes["x"] < ds.sizes["x"]
        assert cropped.sizes["y"] < ds.sizes["y"]
        assert cropped.sizes["x"] > 0

    def test_clip_with_explicit_crs(self, repo_with_scene):
        """clip(crs='EPSG:4326') works when dataset is also EPSG:4326."""
        import pixelquery as pq

        repo_dir, _cog_info = repo_with_scene

        ds = pq.open_xarray(repo_dir)
        x_vals = ds.coords["x"].values
        y_vals = ds.coords["y"].values

        cx = (x_vals.min() + x_vals.max()) / 2
        cy = (y_vals.min() + y_vals.max()) / 2
        dx = (x_vals.max() - x_vals.min()) / 8
        dy = (y_vals.max() - y_vals.min()) / 8

        geojson = {
            "type": "Polygon",
            "coordinates": [[
                [cx - dx, cy - dy],
                [cx + dx, cy - dy],
                [cx, cy + dy],
                [cx - dx, cy - dy],
            ]],
        }

        clipped = pq.clip(ds, geojson, crs="EPSG:4326")
        data = clipped["data"].sel(band="nir").values
        assert np.isnan(data).any(), "Expected NaN outside polygon"
        assert np.isfinite(data).any(), "Expected valid inside polygon"

    def test_scenes_index_has_crs(self, repo_with_scene):
        """Verify that _scenes_index entries now include CRS."""
        import zarr

        from pixelquery._internal.storage.icechunk_storage import IcechunkStorageManager

        repo_dir, _cog_info = repo_with_scene
        storage = IcechunkStorageManager(repo_dir)
        storage.initialize()

        session = storage.readonly_session()
        root = zarr.open_group(session.store, mode="r")
        scenes = list(root["_scenes_index"].attrs["scenes"])

        assert len(scenes) >= 1
        assert "crs" in scenes[0], "scenes_index entry missing 'crs' field"
        assert scenes[0]["crs"] is not None
