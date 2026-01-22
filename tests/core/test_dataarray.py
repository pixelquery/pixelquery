"""
Tests for DataArray class
"""

import pytest
import numpy as np

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

    def test_sel_not_implemented(self):
        """Test sel raises NotImplementedError"""
        da = DataArray(name="red", data=np.random.rand(10, 256, 256))

        with pytest.raises(NotImplementedError):
            da.sel(time="2024-01")

    def test_mean_not_implemented(self):
        """Test mean raises NotImplementedError"""
        da = DataArray(name="red", data=np.random.rand(10, 256, 256))

        with pytest.raises(NotImplementedError):
            da.mean(dim="time")
