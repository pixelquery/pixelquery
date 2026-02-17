"""
DataArray class - xarray-inspired API for single band data

Represents a single satellite band with labeled dimensions.
"""

from typing import Any, Union

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
        data: NDArray | None = None,
        dims: dict[str, int] | None = None,
        coords: dict[str, NDArray] | None = None,
        attrs: dict[str, Any] | None = None,
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
        if not indexers:
            return self

        # Convert label-based indexers to integer indexers
        integer_indexers = {}
        for dim, value in indexers.items():
            if dim not in self.dims:
                raise KeyError(f"Dimension '{dim}' not found. Available: {list(self.dims.keys())}")

            if dim not in self.coords:
                raise KeyError(
                    f"No coordinates for dimension '{dim}'. Use isel() for integer indexing."
                )

            coord = self.coords[dim]

            if isinstance(value, slice):
                # Handle slice
                start_idx = (
                    self._find_nearest_index(coord, value.start)
                    if value.start is not None
                    else None
                )
                stop_idx = (
                    self._find_nearest_index(coord, value.stop) if value.stop is not None else None
                )
                if stop_idx is not None:
                    stop_idx += 1  # Include the stop value
                integer_indexers[dim] = slice(start_idx, stop_idx, value.step)
            else:
                # Single value - find nearest
                integer_indexers[dim] = self._find_nearest_index(coord, value)

        return self.isel(**integer_indexers)

    def _find_nearest_index(self, coord: NDArray, value: Any) -> int:
        """Find index of nearest coordinate value"""
        if coord.size == 0:
            raise ValueError("Empty coordinate array")

        # Handle datetime-like coordinates
        if np.issubdtype(coord.dtype, np.datetime64):
            if isinstance(value, str):
                value = np.datetime64(value)
            coord_numeric = coord.astype("datetime64[ns]").astype(np.int64)
            value_numeric = np.datetime64(value).astype("datetime64[ns]").astype(np.int64)
            return int(np.argmin(np.abs(coord_numeric - value_numeric)))

        # Numeric coordinates
        return int(np.argmin(np.abs(coord - value)))

    def isel(self, **indexers: Any) -> "DataArray":
        """
        Select by integer index (xarray.DataArray.isel-like)

        Args:
            **indexers: Integer indexers

        Returns:
            New DataArray with selected data
        """
        if not indexers:
            return self

        # Build index tuple and track new dimensions
        dim_names = list(self.dims.keys())
        index_tuple = []
        new_dims = {}
        new_coords = {}

        for _, dim in enumerate(dim_names):
            if dim in indexers:
                idx = indexers[dim]
                index_tuple.append(idx)

                if isinstance(idx, slice):
                    # Slice keeps dimension
                    start, stop, step = idx.start or 0, idx.stop or self.dims[dim], idx.step or 1
                    new_size = len(range(start, stop, step))
                    new_dims[dim] = new_size
                    if dim in self.coords:
                        new_coords[dim] = self.coords[dim][idx]
                # Single integer drops dimension
            else:
                # Keep full dimension
                index_tuple.append(slice(None))
                new_dims[dim] = self.dims[dim]
                if dim in self.coords:
                    new_coords[dim] = self.coords[dim]

        # Index the data
        new_data = self.data[tuple(index_tuple)]

        # If result is scalar, wrap in 0-d array for consistency
        if isinstance(new_data, (int, float, np.integer, np.floating)):
            new_data = np.array(new_data)

        return DataArray(
            name=self.name,
            data=new_data,
            dims=new_dims,
            coords=new_coords,
            attrs=self.attrs.copy(),
        )

    def mean(self, dim: str | list | None = None) -> Union["DataArray", float]:
        """
        Compute mean along dimension

        Args:
            dim: Dimension to reduce (e.g., "time", "y", "x"), or list of dims

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
        return self._reduce_op(np.mean, dim)

    def max(self, dim: str | list | None = None) -> Union["DataArray", float]:
        """Compute maximum along dimension"""
        return self._reduce_op(np.max, dim)

    def min(self, dim: str | list | None = None) -> Union["DataArray", float]:
        """Compute minimum along dimension"""
        return self._reduce_op(np.min, dim)

    def _reduce_op(self, op, dim: str | list | None = None) -> Union["DataArray", float]:
        """Apply reduction operation along dimension(s)"""
        if dim is None:
            # Reduce all dimensions
            result = op(self.data)
            return float(result)

        # Ensure dim is a list
        if isinstance(dim, str):
            dims_to_reduce = [dim]
        else:
            dims_to_reduce = list(dim)

        # Validate dimensions
        dim_names = list(self.dims.keys())
        for d in dims_to_reduce:
            if d not in dim_names:
                raise KeyError(f"Dimension '{d}' not found. Available: {dim_names}")

        # Get axis indices
        axes = tuple(dim_names.index(d) for d in dims_to_reduce)

        # Apply operation
        new_data = op(self.data, axis=axes)

        # Build new dimensions and coordinates (exclude reduced dims)
        new_dims = {d: s for d, s in self.dims.items() if d not in dims_to_reduce}
        new_coords = {d: c for d, c in self.coords.items() if d not in dims_to_reduce}

        if len(new_dims) == 0:
            # All dimensions reduced - return scalar
            return float(new_data)

        return DataArray(
            name=self.name,
            data=new_data,
            dims=new_dims,
            coords=new_coords,
            attrs=self.attrs.copy(),
        )

    def median(self, dim: str | None = None) -> Union["DataArray", float]:
        """Compute median along dimension"""
        raise NotImplementedError("median() will be implemented in Phase 2+")

    def std(self, dim: str | None = None) -> Union["DataArray", float]:
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
    def __radd__(self, other: float | NDArray) -> "DataArray":
        """Reverse addition"""
        return self + other

    def __rsub__(self, other: float | NDArray) -> "DataArray":
        """Reverse subtraction"""
        result_data = other - self.data
        return DataArray(
            name=f"{self.name}_rsub",
            data=result_data,
            dims=self.dims,
            coords=self.coords,
        )

    def __rmul__(self, other: float | NDArray) -> "DataArray":
        """Reverse multiplication"""
        return self * other

    def __rtruediv__(self, other: float | NDArray) -> "DataArray":
        """Reverse division"""
        result_data = other / self.data
        return DataArray(
            name=f"{self.name}_rtruediv",
            data=result_data,
            dims=self.dims,
            coords=self.coords,
        )
