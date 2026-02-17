"""
Tests for Dataset class
"""

from datetime import datetime

import numpy as np
import pytest

from pixelquery.core.dataset import Dataset, DatasetResampler


class TestDataset:
    """Test Dataset class"""

    def test_init(self):
        """Test Dataset initialization"""
        ds = Dataset(
            tile_id="x0024_y0041",
            time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
            bands=["red", "nir"],
        )

        assert ds.tile_id == "x0024_y0041"
        assert ds.bands == ["red", "nir"]
        assert len(ds.data) == 0

    def test_repr(self):
        """Test string representation"""
        ds = Dataset(
            tile_id="x0024_y0041",
            time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
            bands=["red", "nir", "green"],
        )

        repr_str = repr(ds)
        assert "x0024_y0041" in repr_str
        assert "red" in repr_str or "nir" in repr_str

    def test_getitem(self):
        """Test band access via indexing"""
        data = {"red": np.random.rand(10, 256, 256)}
        ds = Dataset(
            tile_id="x0024_y0041",
            bands=["red"],
            data=data,
            metadata={},
        )

        red_band = ds["red"]
        assert red_band.name == "red"

    def test_getitem_missing_band(self):
        """Test accessing non-existent band raises KeyError"""
        ds = Dataset(
            tile_id="x0024_y0041",
            bands=["red"],
            data={},
        )

        with pytest.raises(KeyError):
            _ = ds["nir"]

    def test_resample(self):
        """Test resample method returns resampler"""
        ds = Dataset(tile_id="x0024_y0041", bands=["red"])
        resampler = ds.resample(time="1M")

        assert isinstance(resampler, DatasetResampler)
        assert resampler.freq == "1M"

    def test_to_xarray(self):
        """Test to_xarray conversion"""
        pytest.importorskip("xarray")

        times = [datetime(2024, 1, i + 1) for i in range(5)]

        # Use list format (time series of 2D arrays)
        data = {
            "red": [np.random.rand(32, 32).astype(np.float32) for _ in range(5)],
            "nir": [np.random.rand(32, 32).astype(np.float32) for _ in range(5)],
        }

        ds = Dataset(
            tile_id="x0024_y0041",
            bands=["red", "nir"],
            data=data,
            metadata={
                "bounds": (126.9, 37.5, 127.0, 37.6),
                "times": times,
            },
        )

        xr_ds = ds.to_xarray()

        assert "red" in xr_ds.data_vars
        assert "nir" in xr_ds.data_vars
        assert xr_ds["red"].dims == ("time", "y", "x")
        assert xr_ds["red"].shape == (5, 32, 32)

    def test_to_pandas(self):
        """Test to_pandas conversion"""
        pytest.importorskip("pandas")

        times = [datetime(2024, 1, i + 1) for i in range(3)]

        # Use list format (time series of 2D arrays)
        data = {
            "red": [np.random.rand(4, 4).astype(np.float32) for _ in range(3)],
        }

        ds = Dataset(
            tile_id="x0024_y0041",
            bands=["red"],
            data=data,
            metadata={
                "times": times,
            },
        )

        df = ds.to_pandas()

        # Should have time, y, x, band, value columns
        assert "time" in df.columns
        assert "band" in df.columns
        assert "value" in df.columns
        # 3 times * 4 y * 4 x = 48 rows
        assert len(df) == 48

    def test_to_numpy(self):
        """Test to_numpy returns dict of arrays"""
        data = {"red": np.random.rand(10, 256, 256), "nir": np.random.rand(10, 256, 256)}
        ds = Dataset(tile_id="x0024_y0041", bands=["red", "nir"], data=data)

        result = ds.to_numpy()

        assert isinstance(result, dict)
        assert "red" in result
        assert "nir" in result
        assert result["red"].shape == (10, 256, 256)
        assert np.array_equal(result["red"], data["red"])

    def test_to_numpy_with_dataarray(self):
        """Test to_numpy with DataArray objects"""
        from pixelquery.core.dataarray import DataArray

        red_data = np.random.rand(5, 100, 100)
        nir_data = np.random.rand(5, 100, 100)

        data = {
            "red": DataArray(name="red", data=red_data, dims={"time": 5, "y": 100, "x": 100}),
            "nir": DataArray(name="nir", data=nir_data, dims={"time": 5, "y": 100, "x": 100}),
        }
        ds = Dataset(tile_id="x0024_y0041", bands=["red", "nir"], data=data)

        result = ds.to_numpy()

        assert isinstance(result, dict)
        assert result["red"].shape == (5, 100, 100)
        assert np.array_equal(result["red"], red_data)


class TestDatasetResampler:
    """Test DatasetResampler class"""

    def test_init(self):
        """Test DatasetResampler initialization"""
        ds = Dataset(tile_id="x0024_y0041", bands=["red"])
        resampler = DatasetResampler(ds, "1M")

        assert resampler.dataset == ds
        assert resampler.freq == "1M"

    def test_mean(self):
        """Test mean aggregation"""
        ds = Dataset(tile_id="x0024_y0041", bands=["red"], data={"red": np.array([1, 2, 3])})
        resampler = DatasetResampler(ds, "1M")

        result = resampler.mean()

        assert isinstance(result, Dataset)
        assert result.metadata["resampled"] is True
        assert result.metadata["freq"] == "1M"
        assert result.metadata["aggregation"] == "mean"

    def test_max(self):
        """Test max aggregation"""
        ds = Dataset(tile_id="x0024_y0041", bands=["red"], data={"red": np.array([1, 2, 3])})
        resampler = DatasetResampler(ds, "1M")

        result = resampler.max()

        assert isinstance(result, Dataset)
        assert result.metadata["resampled"] is True
        assert result.metadata["freq"] == "1M"
        assert result.metadata["aggregation"] == "max"

    def test_min(self):
        """Test min aggregation"""
        ds = Dataset(tile_id="x0024_y0041", bands=["red"], data={"red": np.array([1, 2, 3])})
        resampler = DatasetResampler(ds, "1M")

        result = resampler.min()

        assert isinstance(result, Dataset)
        assert result.metadata["resampled"] is True
        assert result.metadata["freq"] == "1M"
        assert result.metadata["aggregation"] == "min"

    def test_median(self):
        """Test median aggregation"""
        ds = Dataset(tile_id="x0024_y0041", bands=["red"], data={"red": np.array([1, 2, 3])})
        resampler = DatasetResampler(ds, "1M")

        result = resampler.median()

        assert isinstance(result, Dataset)
        assert result.metadata["resampled"] is True
        assert result.metadata["freq"] == "1M"
        assert result.metadata["aggregation"] == "median"

    def test_resampler_preserves_data(self):
        """Test that resampler preserves original data"""
        ds = Dataset(
            tile_id="x0024_y0041",
            bands=["red", "nir"],
            data={"red": np.array([1, 2, 3]), "nir": np.array([4, 5, 6])},
        )
        resampler = DatasetResampler(ds, "1M")

        result = resampler.mean()

        assert result.tile_id == ds.tile_id
        assert result.bands == ds.bands
        assert "red" in result.data
        assert "nir" in result.data
