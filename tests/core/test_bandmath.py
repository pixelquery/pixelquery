"""Tests for BandMath xarray accessor."""

import numpy as np
import pytest
import xarray as xr

import pixelquery.core.bandmath


@pytest.fixture
def sample_ds():
    """Create a sample xarray Dataset mimicking PixelQuery output."""
    np.random.seed(42)
    data = np.random.rand(4, 8, 8).astype(np.float32)
    # Make NIR (b3) > Red (b2) for positive NDVI
    data[3] = data[2] + 0.3
    data = np.clip(data, 0.01, 1.0)

    da = xr.DataArray(
        data,
        dims=["band", "y", "x"],
        coords={"band": ["blue", "green", "red", "nir"]},
    )
    return da.to_dataset(name="data")


@pytest.fixture
def timeseries_ds():
    """Dataset with time dimension: (time, band, y, x)."""
    np.random.seed(42)
    data = np.random.rand(3, 4, 4, 4).astype(np.float32)
    data[:, 3] = data[:, 2] + 0.3
    data = np.clip(data, 0.01, 1.0)

    da = xr.DataArray(
        data,
        dims=["time", "band", "y", "x"],
        coords={
            "band": ["blue", "green", "red", "nir"],
            "time": [0, 1, 2],
        },
    )
    return da.to_dataset(name="data")


@pytest.fixture
def unnamed_ds():
    """Dataset without band name coordinates (index-only)."""
    np.random.seed(42)
    data = np.random.rand(4, 8, 8).astype(np.float32)
    data[3] = data[2] + 0.3
    data = np.clip(data, 0.01, 1.0)

    da = xr.DataArray(data, dims=["band", "y", "x"])
    return da.to_dataset(name="data")


class TestBandMathAccessor:
    def test_accessor_exists(self, sample_ds):
        assert hasattr(sample_ds, "bandmath")

    def test_bands_property(self, sample_ds):
        bands = sample_ds.bandmath.bands
        assert bands == {"b0": "blue", "b1": "green", "b2": "red", "b3": "nir"}

    def test_bands_unnamed(self, unnamed_ds):
        bands = unnamed_ds.bandmath.bands
        assert bands == {"b0": 0, "b1": 1, "b2": 2, "b3": 3}

    def test_repr(self, sample_ds):
        r = repr(sample_ds.bandmath)
        assert "BandMath" in r
        assert "b0: blue" in r
        assert "b3: nir" in r


class TestExpressionByIndex:
    def test_ndvi(self, sample_ds):
        ndvi = sample_ds.bandmath("(b3 - b2) / (b3 + b2)")
        assert ndvi.dims == ("y", "x")
        assert float(ndvi.min()) >= -1.0
        assert float(ndvi.max()) <= 1.0
        assert float(ndvi.mean()) > 0  # NIR > Red

    def test_ratio(self, sample_ds):
        ratio = sample_ds.bandmath("b3 / b2")
        assert float(ratio.mean()) > 1.0

    def test_evi(self, sample_ds):
        evi = sample_ds.bandmath("2.5 * (b3 - b2) / (b3 + 6*b2 - 7.5*b0 + 1)")
        assert evi.dims == ("y", "x")

    def test_with_numpy(self, sample_ds):
        result = sample_ds.bandmath("np.sqrt(b3 * b2)")
        assert result.shape == (8, 8)

    def test_unnamed_bands(self, unnamed_ds):
        ndvi = unnamed_ds.bandmath("(b3 - b2) / (b3 + b2)")
        assert ndvi.dims == ("y", "x")
        assert float(ndvi.mean()) > 0


class TestExpressionByName:
    def test_ndvi(self, sample_ds):
        ndvi = sample_ds.bandmath("(nir - red) / (nir + red)")
        assert float(ndvi.mean()) > 0

    def test_matches_index(self, sample_ds):
        by_name = sample_ds.bandmath("(nir - red) / (nir + red)")
        by_index = sample_ds.bandmath("(b3 - b2) / (b3 + b2)")
        np.testing.assert_allclose(by_name.values, by_index.values)


class TestWithTimeDimension:
    def test_preserves_time(self, timeseries_ds):
        ndvi = timeseries_ds.bandmath("(b3 - b2) / (b3 + b2)")
        assert "time" in ndvi.dims
        assert ndvi.shape == (3, 4, 4)
