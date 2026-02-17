"""
Tests for Dataset selection methods (sel, isel)
"""

from datetime import datetime

import numpy as np
import pytest

from pixelquery.core.dataset import Dataset


class TestDatasetSel:
    """Test Dataset.sel() method"""

    @pytest.fixture
    def sample_dataset(self):
        """Create sample dataset"""
        return Dataset(
            tile_id="x0024_y0041",
            time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
            bands=["red", "green", "blue", "nir"],
            data={
                "red": np.array([1, 2, 3]),
                "green": np.array([4, 5, 6]),
                "blue": np.array([7, 8, 9]),
                "nir": np.array([10, 11, 12]),
            },
        )

    def test_sel_bands_single(self, sample_dataset):
        """Test selecting single band"""
        result = sample_dataset.sel(bands=["red"])

        assert result.bands == ["red"]
        assert "red" in result.data
        assert len(result.data) == 1

    def test_sel_bands_multiple(self, sample_dataset):
        """Test selecting multiple bands"""
        result = sample_dataset.sel(bands=["red", "nir"])

        assert result.bands == ["red", "nir"]
        assert "red" in result.data
        assert "nir" in result.data
        assert len(result.data) == 2

    def test_sel_bands_invalid(self, sample_dataset):
        """Test selecting invalid band raises error"""
        with pytest.raises(ValueError, match="Invalid bands"):
            sample_dataset.sel(bands=["invalid_band"])

    def test_sel_time_month_string(self, sample_dataset):
        """Test selecting time with month string"""
        result = sample_dataset.sel(time="2024-01")

        assert result.time_range is not None
        start, end = result.time_range
        assert start == datetime(2024, 1, 1)
        assert end == datetime(2024, 2, 1)

    def test_sel_time_slice(self, sample_dataset):
        """Test selecting time with slice"""
        result = sample_dataset.sel(time=slice("2024-01", "2024-06"))

        assert result.time_range is not None
        start, end = result.time_range
        assert start == datetime(2024, 1, 1)
        assert end == datetime(2024, 6, 1)

    def test_sel_time_datetime(self, sample_dataset):
        """Test selecting time with datetime object"""
        dt = datetime(2024, 3, 15)
        result = sample_dataset.sel(time=dt)

        assert result.time_range is not None
        start, end = result.time_range
        assert start == dt
        assert end == dt

    def test_sel_combined(self, sample_dataset):
        """Test combined time and band selection"""
        result = sample_dataset.sel(time=slice("2024-01", "2024-06"), bands=["red", "nir"])

        assert result.bands == ["red", "nir"]
        assert len(result.data) == 2

        start, end = result.time_range
        assert start == datetime(2024, 1, 1)
        assert end == datetime(2024, 6, 1)

    def test_sel_no_parameters(self, sample_dataset):
        """Test sel with no parameters returns same dataset"""
        result = sample_dataset.sel()

        assert result.tile_id == sample_dataset.tile_id
        assert result.bands == sample_dataset.bands
        assert result.time_range == sample_dataset.time_range

    def test_sel_preserves_tile_id(self, sample_dataset):
        """Test that selection preserves tile_id"""
        result = sample_dataset.sel(bands=["red"])

        assert result.tile_id == sample_dataset.tile_id

    def test_sel_returns_new_dataset(self, sample_dataset):
        """Test that sel returns new Dataset instance"""
        result = sample_dataset.sel(bands=["red"])

        assert result is not sample_dataset
        assert isinstance(result, Dataset)

    def test_parse_time_string_month(self, sample_dataset):
        """Test parsing month string"""
        dt = sample_dataset._parse_time_string("2024-01")
        assert dt == datetime(2024, 1, 1)

    def test_parse_time_string_date(self, sample_dataset):
        """Test parsing date string"""
        dt = sample_dataset._parse_time_string("2024-01-15")
        assert dt == datetime(2024, 1, 15)

    def test_sel_time_december_month_end(self, sample_dataset):
        """Test selecting December calculates correct month end"""
        result = sample_dataset.sel(time="2024-12")

        start, end = result.time_range
        assert start == datetime(2024, 12, 1)
        assert end == datetime(2025, 1, 1)


class TestDatasetIsel:
    """Test Dataset.isel() method"""

    @pytest.fixture
    def sample_dataset(self):
        """Create sample dataset"""
        return Dataset(
            tile_id="x0024_y0041",
            time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
            bands=["red", "nir"],
            data={"red": np.array([1, 2, 3]), "nir": np.array([4, 5, 6])},
        )

    def test_isel_single_index(self, sample_dataset):
        """Test integer selection with single index"""
        result = sample_dataset.isel(time=0)

        assert isinstance(result, Dataset)
        assert "indexers" in result.metadata
        assert result.metadata["indexers"]["time"] == 0

    def test_isel_slice(self, sample_dataset):
        """Test integer selection with slice"""
        result = sample_dataset.isel(time=slice(0, 10))

        assert isinstance(result, Dataset)
        assert "indexers" in result.metadata
        assert isinstance(result.metadata["indexers"]["time"], slice)

    def test_isel_preserves_data(self, sample_dataset):
        """Test that isel preserves original data"""
        result = sample_dataset.isel(time=0)

        assert result.tile_id == sample_dataset.tile_id
        assert result.bands == sample_dataset.bands
        assert "red" in result.data
        assert "nir" in result.data


class TestDatasetChaining:
    """Test method chaining (xarray-like)"""

    @pytest.fixture
    def sample_dataset(self):
        """Create sample dataset"""
        return Dataset(
            tile_id="x0024_y0041",
            time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
            bands=["red", "green", "blue", "nir"],
            data={
                "red": np.array([1, 2, 3]),
                "green": np.array([4, 5, 6]),
                "blue": np.array([7, 8, 9]),
                "nir": np.array([10, 11, 12]),
            },
        )

    def test_chain_sel_sel(self, sample_dataset):
        """Test chaining multiple sel() calls"""
        result = sample_dataset.sel(time=slice("2024-01", "2024-06")).sel(bands=["red", "nir"])

        assert result.bands == ["red", "nir"]
        start, end = result.time_range
        assert start == datetime(2024, 1, 1)
        assert end == datetime(2024, 6, 1)

    def test_chain_sel_isel(self, sample_dataset):
        """Test chaining sel() and isel()"""
        result = sample_dataset.sel(bands=["red"]).isel(time=0)

        assert result.bands == ["red"]
        assert "indexers" in result.metadata
