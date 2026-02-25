"""
GDAL-Free Import Guard Tests

Verifies that the query path (open_xarray → crop → clip → bandmath → timeseries)
does NOT import rasterio/GDAL. Rasterio is only needed for the WRITE/INGEST path.
"""

import sys

import numpy as np
import pytest
import xarray as xr


def _make_geo_dataset(nx=20, ny=20, nbands=4):
    """Create a synthetic geo-referenced xr.Dataset matching Icechunk output format."""
    x_coords = np.linspace(127.0, 127.1, nx)
    y_coords = np.linspace(37.1, 37.0, ny)  # top→bottom
    band_names = ["blue", "green", "red", "nir"][:nbands]
    data = np.random.rand(nbands, ny, nx).astype(np.float32)
    da = xr.DataArray(
        data,
        dims=["band", "y", "x"],
        coords={"band": band_names, "y": y_coords, "x": x_coords},
    )
    return da.to_dataset(name="data")


class TestGdalFreeQueryPath:
    """Verify query operations work without rasterio installed."""

    def test_clip_no_rasterio(self, monkeypatch):
        """clip() works without rasterio."""
        monkeypatch.setitem(sys.modules, "rasterio", None)
        monkeypatch.setitem(sys.modules, "rasterio.features", None)
        monkeypatch.setitem(sys.modules, "rasterio.transform", None)

        from pixelquery.io.icechunk_reader import IcechunkVirtualReader

        ds = _make_geo_dataset()
        # Use a triangle so some pixels inside the bbox fall outside the polygon → NaN
        polygon = {
            "type": "Polygon",
            "coordinates": [[[127.05, 37.02], [127.09, 37.08], [127.01, 37.08], [127.05, 37.02]]],
        }
        result = IcechunkVirtualReader.clip(ds, polygon)
        assert "data" in result
        # Should have some NaN (outside polygon) and some valid values
        vals = result["data"].values
        assert np.any(np.isnan(vals))
        assert np.any(~np.isnan(vals))

    def test_crop_no_rasterio(self, monkeypatch):
        """crop() works without rasterio."""
        monkeypatch.setitem(sys.modules, "rasterio", None)
        monkeypatch.setitem(sys.modules, "rasterio.features", None)
        monkeypatch.setitem(sys.modules, "rasterio.transform", None)

        from pixelquery.io.icechunk_reader import IcechunkVirtualReader

        ds = _make_geo_dataset()
        bounds = (127.02, 37.02, 127.08, 37.08)
        result = IcechunkVirtualReader.crop(ds, bounds)
        assert "data" in result
        assert result["data"].sizes["x"] < ds["data"].sizes["x"]

    def test_bandmath_no_rasterio(self, monkeypatch):
        """bandmath accessor works without rasterio."""
        monkeypatch.setitem(sys.modules, "rasterio", None)
        monkeypatch.setitem(sys.modules, "rasterio.features", None)
        monkeypatch.setitem(sys.modules, "rasterio.transform", None)

        # Ensure the bandmath accessor is registered
        import pixelquery.core.bandmath

        ds = _make_geo_dataset()
        ndvi = ds.bandmath("(nir - red) / (nir + red)")
        assert isinstance(ndvi, xr.DataArray)
        assert ndvi.shape == (20, 20)

    def test_xarray_statistics_no_rasterio(self, monkeypatch):
        """xarray native statistics work without rasterio."""
        monkeypatch.setitem(sys.modules, "rasterio", None)

        ds = _make_geo_dataset()
        mean_val = ds["data"].mean()
        std_val = ds["data"].std()
        max_val = ds["data"].max()
        min_val = ds["data"].min()

        assert not np.isnan(mean_val.values)
        assert std_val.values > 0
        assert max_val.values > min_val.values

    def test_timeseries_module_no_rasterio_at_import(self, monkeypatch):
        """timeseries module can be imported without rasterio."""
        monkeypatch.setitem(sys.modules, "rasterio", None)
        monkeypatch.setitem(sys.modules, "rasterio.features", None)
        monkeypatch.setitem(sys.modules, "rasterio.transform", None)

        # Should import without error
        from pixelquery.core.timeseries import timeseries
