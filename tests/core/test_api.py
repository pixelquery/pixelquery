"""
Tests for public API functions
"""

import numpy as np
import pytest
import xarray as xr

from pixelquery.core.api import compute_evi, compute_ndvi, open_dataset
from pixelquery.core.dataarray import DataArray
from pixelquery.core.dataset import Dataset


class TestOpenDataset:
    """Test open_dataset function"""

    def test_open_dataset_basic(self):
        """Test basic open_dataset call"""
        ds = open_dataset("warehouse", tile_id="x0024_y0041")

        assert isinstance(ds, Dataset)
        assert ds.tile_id == "x0024_y0041"

    def test_open_dataset_with_filters(self):
        """Test open_dataset with filters"""
        from datetime import datetime

        ds = open_dataset(
            "warehouse",
            tile_id="x0024_y0041",
            time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
            bands=["red", "nir"],
        )

        assert ds.tile_id == "x0024_y0041"
        # Note: ds.bands reflects data actually found, not just requested bands
        # With no actual data in the warehouse, bands will be empty
        assert isinstance(ds.bands, list)


class TestComputeNDVI:
    """Test compute_ndvi function"""

    def test_compute_ndvi_basic(self):
        """Test NDVI computation"""
        # Create sample data
        red_data = np.ones((10, 256, 256)) * 0.1
        nir_data = np.ones((10, 256, 256)) * 0.5

        red = DataArray(name="red", data=red_data)
        nir = DataArray(name="nir", data=nir_data)

        # Compute NDVI
        ndvi = compute_ndvi(red, nir)

        # Expected NDVI = (0.5 - 0.1) / (0.5 + 0.1) = 0.4 / 0.6 ≈ 0.667
        expected = (0.5 - 0.1) / (0.5 + 0.1)
        assert isinstance(ndvi, DataArray)
        assert np.allclose(ndvi.data, expected, rtol=1e-5)

    def test_compute_ndvi_zeros(self):
        """Test NDVI with zero values"""
        red_data = np.zeros((10, 256, 256))
        nir_data = np.zeros((10, 256, 256))

        red = DataArray(name="red", data=red_data)
        nir = DataArray(name="nir", data=nir_data)

        ndvi = compute_ndvi(red, nir)

        # Should handle division by zero (result will be nan)
        assert isinstance(ndvi, DataArray)


class TestComputeEVI:
    """Test compute_evi function"""

    def test_compute_evi_basic(self):
        """Test EVI computation"""
        blue_data = np.ones((10, 256, 256)) * 0.05
        red_data = np.ones((10, 256, 256)) * 0.1
        nir_data = np.ones((10, 256, 256)) * 0.5

        blue = DataArray(name="blue", data=blue_data)
        red = DataArray(name="red", data=red_data)
        nir = DataArray(name="nir", data=nir_data)

        # Compute EVI with default parameters
        evi = compute_evi(blue, red, nir)

        assert isinstance(evi, DataArray)
        # Just check it doesn't crash and returns reasonable values
        assert evi.shape == (10, 256, 256)

    def test_compute_evi_custom_params(self):
        """Test EVI with custom parameters"""
        blue_data = np.ones((10, 256, 256)) * 0.05
        red_data = np.ones((10, 256, 256)) * 0.1
        nir_data = np.ones((10, 256, 256)) * 0.5

        blue = DataArray(name="blue", data=blue_data)
        red = DataArray(name="red", data=red_data)
        nir = DataArray(name="nir", data=nir_data)

        # Test with custom parameters
        evi = compute_evi(blue, red, nir, G=3.0, C1=5.0, C2=8.0, L=2.0)

        assert isinstance(evi, DataArray)
        assert evi.shape == (10, 256, 256)


class TestComputeNdviXarrayInput:
    """Test compute_ndvi/evi with native xr.DataArray."""

    def test_ndvi_with_xr_dataarrays(self):
        red = xr.DataArray(np.full((10, 10), 0.1))
        nir = xr.DataArray(np.full((10, 10), 0.5))
        result = compute_ndvi(red, nir)
        expected = (0.5 - 0.1) / (0.5 + 0.1)
        np.testing.assert_allclose(result.values, expected, rtol=1e-5)

    def test_evi_with_xr_dataarrays(self):
        blue = xr.DataArray(np.full((10, 10), 0.05))
        red = xr.DataArray(np.full((10, 10), 0.1))
        nir = xr.DataArray(np.full((10, 10), 0.5))
        result = compute_evi(blue, red, nir)
        assert not np.any(np.isnan(result.values))
