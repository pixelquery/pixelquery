"""
Tests for DataArray class
"""

import numpy as np
import pytest

from pixelquery.core.dataarray import DataArray


class TestDataArray:
    """Test DataArray class"""

    def test_init(self):
        """Test DataArray initialization"""
        data = np.random.rand(10, 256, 256)
        da = DataArray(
            name="red",
            data=data,
            dims={"time": 10, "y": 256, "x": 256},
        )

        assert da.name == "red"
        assert da.shape == (10, 256, 256)
        assert da.size == 10 * 256 * 256

    def test_values_property(self):
        """Test values property returns underlying array"""
        data = np.random.rand(10, 256, 256)
        da = DataArray(name="red", data=data)

        assert np.array_equal(da.values, data)

    def test_shape_property(self):
        """Test shape property"""
        data = np.random.rand(10, 256, 256)
        da = DataArray(name="red", data=data)

        assert da.shape == (10, 256, 256)

    def test_dtype_property(self):
        """Test dtype property"""
        data = np.random.rand(10, 256, 256).astype(np.float32)
        da = DataArray(name="red", data=data)

        assert da.dtype == np.float32

    def test_repr(self):
        """Test string representation"""
        data = np.random.rand(10, 256, 256)
        da = DataArray(
            name="red",
            data=data,
            dims={"time": 10, "y": 256, "x": 256},
        )

        repr_str = repr(da)
        assert "red" in repr_str
        assert "10, 256, 256" in repr_str

    def test_add(self):
        """Test addition operation"""
        data1 = np.ones((10, 256, 256))
        data2 = np.ones((10, 256, 256)) * 2

        da1 = DataArray(name="a", data=data1)
        da2 = DataArray(name="b", data=data2)

        result = da1 + da2
        assert isinstance(result, DataArray)
        assert np.allclose(result.data, 3.0)

    def test_subtract(self):
        """Test subtraction operation"""
        data1 = np.ones((10, 256, 256)) * 5
        data2 = np.ones((10, 256, 256)) * 2

        da1 = DataArray(name="a", data=data1)
        da2 = DataArray(name="b", data=data2)

        result = da1 - da2
        assert isinstance(result, DataArray)
        assert np.allclose(result.data, 3.0)

    def test_multiply(self):
        """Test multiplication operation"""
        data1 = np.ones((10, 256, 256)) * 2
        da1 = DataArray(name="a", data=data1)

        result = da1 * 3
        assert isinstance(result, DataArray)
        assert np.allclose(result.data, 6.0)

    def test_divide(self):
        """Test division operation"""
        data1 = np.ones((10, 256, 256)) * 6
        da1 = DataArray(name="a", data=data1)

        result = da1 / 2
        assert isinstance(result, DataArray)
        assert np.allclose(result.data, 3.0)

    def test_to_numpy(self):
        """Test to_numpy method"""
        data = np.random.rand(10, 256, 256)
        da = DataArray(name="red", data=data)

        result = da.to_numpy()
        assert np.array_equal(result, data)

    def test_array_interface(self):
        """Test NumPy array interface"""
        data = np.random.rand(10, 256, 256)
        da = DataArray(name="red", data=data)

        array = np.asarray(da)
        assert np.array_equal(array, data)

    def test_sel_with_coords(self):
        """Test sel with coordinate labels"""
        data = np.arange(30).reshape(3, 2, 5).astype(float)
        times = np.array(["2024-01-01", "2024-02-01", "2024-03-01"], dtype="datetime64[D]")
        coords = {"time": times, "y": np.array([0, 1]), "x": np.array([0, 1, 2, 3, 4])}
        dims = {"time": 3, "y": 2, "x": 5}

        da = DataArray(name="red", data=data, dims=dims, coords=coords)

        # Select single time
        result = da.sel(time="2024-02-01")
        assert result.shape == (2, 5)

    def test_sel_slice(self):
        """Test sel with slice"""
        data = np.arange(30).reshape(3, 2, 5).astype(float)
        times = np.array(["2024-01-01", "2024-02-01", "2024-03-01"], dtype="datetime64[D]")
        coords = {"time": times}
        dims = {"time": 3, "y": 2, "x": 5}

        da = DataArray(name="red", data=data, dims=dims, coords=coords)

        # Select time range
        result = da.sel(time=slice("2024-01-01", "2024-02-01"))
        assert result.shape[0] == 2  # 2 time steps

    def test_isel_basic(self):
        """Test isel with integer indices"""
        data = np.arange(60).reshape(3, 4, 5).astype(float)
        dims = {"time": 3, "y": 4, "x": 5}

        da = DataArray(name="red", data=data, dims=dims)

        # Select single index
        result = da.isel(time=1)
        assert result.shape == (4, 5)
        assert "time" not in result.dims

        # Select slice
        result2 = da.isel(y=slice(1, 3))
        assert result2.shape == (3, 2, 5)
        assert result2.dims["y"] == 2

    def test_mean_overall(self):
        """Test mean over all dimensions"""
        data = np.ones((3, 4, 5)) * 10.0
        dims = {"time": 3, "y": 4, "x": 5}

        da = DataArray(name="red", data=data, dims=dims)
        result = da.mean()

        assert isinstance(result, float)
        assert result == 10.0

    def test_mean_single_dim(self):
        """Test mean over single dimension"""
        data = np.arange(60).reshape(3, 4, 5).astype(float)
        dims = {"time": 3, "y": 4, "x": 5}

        da = DataArray(name="red", data=data, dims=dims)
        result = da.mean(dim="time")

        assert isinstance(result, DataArray)
        assert result.shape == (4, 5)
        assert "time" not in result.dims

    def test_mean_multiple_dims(self):
        """Test mean over multiple dimensions"""
        data = np.ones((3, 4, 5)) * 5.0
        dims = {"time": 3, "y": 4, "x": 5}

        da = DataArray(name="red", data=data, dims=dims)
        result = da.mean(dim=["y", "x"])

        assert isinstance(result, DataArray)
        assert result.shape == (3,)
        assert result.dims == {"time": 3}

    def test_max_min(self):
        """Test max and min operations"""
        data = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]]).astype(float)
        dims = {"time": 2, "y": 2, "x": 2}

        da = DataArray(name="red", data=data, dims=dims)

        assert da.max() == 8.0
        assert da.min() == 1.0

        max_time = da.max(dim="time")
        assert max_time.shape == (2, 2)
        assert max_time.data[0, 0] == 5.0  # max of 1 and 5
