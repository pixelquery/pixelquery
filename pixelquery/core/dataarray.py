"""
DataArray class - xarray-inspired API for single band data

Represents a single satellite band with labeled dimensions.
"""

from typing import Optional, Dict, Union, Any
import numpy as np
from numpy.typing import NDArray


class DataArray:
    """
    Single-band satellite imagery (xarray.DataArray-like)

    A DataArray represents a single band (e.g., "red", "nir") with
    labeled dimensions (time, y, x).

    Attributes:
        name: Band name (e.g., "red", "nir")
        data: NumPy array with shape (time, y, x)
        dims: Dimension names
        coords: Coordinate arrays for each dimension
        attrs: Additional attributes (metadata)

    Examples:
        >>> ds = pq.open_dataset("warehouse", tile_id="x0024_y0041")
        >>> red = ds["red"]  # Get single band
        >>>
        >>> # Select time range
        >>> subset = red.sel(time=slice("2024-01", "2024-12"))
        >>>
        >>> # Compute statistics
        >>> mean_red = red.mean(dim="time")
        >>>
        >>> # Convert to numpy
        >>> array = red.values  # or red.to_numpy()
    """

    def __init__(
        self,
        name: str,
        data: Optional[NDArray] = None,
        dims: Optional[Dict[str, int]] = None,
        coords: Optional[Dict[str, NDArray]] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize DataArray

        Args:
            name: Band name
            data: NumPy array
            dims: Dimension names and sizes
            coords: Coordinate arrays
            attrs: Additional attributes
        """
        self.name = name
        self.data = data if data is not None else np.array([])
        self.dims = dims or {}
        self.coords = coords or {}
        self.attrs = attrs or {}

    @property
    def values(self) -> NDArray:
        """Access underlying NumPy array (xarray-like)"""
        return self.data

    @property
    def shape(self) -> tuple:
        """Array shape"""
        return self.data.shape

    @property
    def size(self) -> int:
        """Total number of elements"""
        return self.data.size

    @property
    def dtype(self) -> np.dtype:
        """Data type"""
        return self.data.dtype

    def sel(self, **indexers: Any) -> "DataArray":
        """
        Select data by label (xarray.DataArray.sel-like)

        Args:
            **indexers: Dimension indexers (e.g., time="2024-01", y=slice(0, 100))

        Returns:
            New DataArray with selected data

        Examples:
            >>> # Select specific time
            >>> red.sel(time="2024-01-15")
            >>>
            >>> # Select time range
            >>> red.sel(time=slice("2024-01", "2024-12"))
        """
        raise NotImplementedError("sel() will be implemented in Phase 2+")

    def isel(self, **indexers: Any) -> "DataArray":
        """
        Select by integer index (xarray.DataArray.isel-like)

        Args:
            **indexers: Integer indexers

        Returns:
            New DataArray with selected data
        """
        raise NotImplementedError("isel() will be implemented in Phase 2+")

    def mean(self, dim: Optional[str] = None) -> Union["DataArray", float]:
        """
        Compute mean along dimension

        Args:
            dim: Dimension to reduce (e.g., "time", "y", "x")

        Returns:
            DataArray with reduced dimension or scalar if all dims reduced

        Examples:
            >>> # Temporal mean
            >>> red.mean(dim="time")
            >>>
            >>> # Spatial mean
            >>> red.mean(dim=["y", "x"])
            >>>
            >>> # Overall mean
            >>> red.mean()
        """
        raise NotImplementedError("mean() will be implemented in Phase 2+")

    def max(self, dim: Optional[str] = None) -> Union["DataArray", float]:
        """Compute maximum along dimension"""
        raise NotImplementedError("max() will be implemented in Phase 2+")

    def min(self, dim: Optional[str] = None) -> Union["DataArray", float]:
        """Compute minimum along dimension"""
        raise NotImplementedError("min() will be implemented in Phase 2+")

    def median(self, dim: Optional[str] = None) -> Union["DataArray", float]:
        """Compute median along dimension"""
        raise NotImplementedError("median() will be implemented in Phase 2+")

    def std(self, dim: Optional[str] = None) -> Union["DataArray", float]:
        """Compute standard deviation along dimension"""
        raise NotImplementedError("std() will be implemented in Phase 2+")

    def to_numpy(self) -> NDArray:
        """
        Convert to NumPy array

        Returns:
            NumPy array
        """
        return self.data

    def to_pandas(self) -> Any:  # pd.Series or pd.DataFrame
        """
        Convert to pandas Series or DataFrame

        Returns:
            Pandas object
        """
        raise NotImplementedError("to_pandas() will be implemented in Phase 3+")

    def plot(self, **kwargs: Any) -> Any:
        """
        Quick plot (delegates to matplotlib)

        Args:
            **kwargs: Plotting options

        Examples:
            >>> red.sel(time="2024-01-15").plot()  # Plot single timestep
            >>> red.mean(dim="time").plot()         # Plot temporal mean
        """
        raise NotImplementedError("plot() will be implemented in Phase 3+")

    def __repr__(self) -> str:
        """String representation"""
        dims_str = ", ".join(f"{k}: {v}" for k, v in self.dims.items())
        return (
            f"<PixelQuery.DataArray '{self.name}'>\n"
            f"Shape: {self.shape}\n"
            f"Dimensions: ({dims_str})\n"
            f"dtype: {self.dtype}"
        )

    def __array__(self) -> NDArray:
        """NumPy array interface"""
        return self.data

    # Arithmetic operations (xarray-like)
    def __add__(self, other: Union["DataArray", float, NDArray]) -> "DataArray":
        """Addition"""
        if isinstance(other, DataArray):
            result_data = self.data + other.data
        else:
            result_data = self.data + other

        return DataArray(
            name=f"{self.name}_add",
            data=result_data,
            dims=self.dims,
            coords=self.coords,
        )

    def __sub__(self, other: Union["DataArray", float, NDArray]) -> "DataArray":
        """Subtraction"""
        if isinstance(other, DataArray):
            result_data = self.data - other.data
        else:
            result_data = self.data - other

        return DataArray(
            name=f"{self.name}_sub",
            data=result_data,
            dims=self.dims,
            coords=self.coords,
        )

    def __mul__(self, other: Union["DataArray", float, NDArray]) -> "DataArray":
        """Multiplication"""
        if isinstance(other, DataArray):
            result_data = self.data * other.data
        else:
            result_data = self.data * other

        return DataArray(
            name=f"{self.name}_mul",
            data=result_data,
            dims=self.dims,
            coords=self.coords,
        )

    def __truediv__(self, other: Union["DataArray", float, NDArray]) -> "DataArray":
        """Division"""
        if isinstance(other, DataArray):
            result_data = self.data / other.data
        else:
            result_data = self.data / other

        return DataArray(
            name=f"{self.name}_div",
            data=result_data,
            dims=self.dims,
            coords=self.coords,
        )

    # Reverse operations (for scalar * DataArray)
    def __radd__(self, other: Union[float, NDArray]) -> "DataArray":
        """Reverse addition"""
        return self + other

    def __rsub__(self, other: Union[float, NDArray]) -> "DataArray":
        """Reverse subtraction"""
        result_data = other - self.data
        return DataArray(
            name=f"{self.name}_rsub",
            data=result_data,
            dims=self.dims,
            coords=self.coords,
        )

    def __rmul__(self, other: Union[float, NDArray]) -> "DataArray":
        """Reverse multiplication"""
        return self * other

    def __rtruediv__(self, other: Union[float, NDArray]) -> "DataArray":
        """Reverse division"""
        result_data = other / self.data
        return DataArray(
            name=f"{self.name}_rtruediv",
            data=result_data,
            dims=self.dims,
            coords=self.coords,
        )
