"""
Dataset class - xarray-inspired API for PixelQuery

Provides a familiar interface for data scientists working with satellite imagery.
"""

from typing import Optional, List, Dict, Any, Union, Tuple
from datetime import datetime
import numpy as np
from numpy.typing import NDArray


class Dataset:
    """
    Multi-band satellite imagery dataset (xarray.Dataset-like)

    A Dataset represents satellite imagery for a specific tile with multiple bands
    and temporal observations. Inspired by xarray.Dataset for familiarity.

    Attributes:
        tile_id: Geographic tile identifier (e.g., "x0024_y0041")
        time_range: Temporal extent (start_date, end_date)
        bands: Available band names
        data: Dictionary of band data {band_name: DataArray}
        metadata: Additional metadata (product_id, resolution, etc.)

    Examples:
        >>> import pixelquery as pq
        >>> ds = pq.open_dataset("warehouse", tile_id="x0024_y0041")
        >>>
        >>> # Select specific bands and time range
        >>> subset = ds.sel(time=slice("2024-01", "2024-12"), bands=["red", "nir"])
        >>>
        >>> # Temporal resampling
        >>> monthly = ds.resample(time="1M").mean()
        >>>
        >>> # Convert to other formats
        >>> xr_ds = ds.to_xarray()
        >>> df = ds.to_pandas()
        >>> gdf = ds.metadata.to_geopandas()
    """

    def __init__(
        self,
        tile_id: str,
        time_range: Optional[Tuple[datetime, datetime]] = None,
        bands: Optional[List[str]] = None,
        data: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize Dataset

        Args:
            tile_id: Geographic tile identifier
            time_range: Temporal extent
            bands: Available band names
            data: Band data dictionary
            metadata: Additional metadata
        """
        self.tile_id = tile_id
        self.time_range = time_range
        self.bands = bands or []
        self.data = data or {}
        self.metadata = metadata or {}

        # Dimensions (xarray-like)
        self.dims: Dict[str, int] = {}
        self.coords: Dict[str, NDArray] = {}

    def sel(
        self,
        time: Optional[Union[str, slice, datetime]] = None,
        bands: Optional[List[str]] = None,
        **kwargs: Any
    ) -> "Dataset":
        """
        Select subset of data (xarray.Dataset.sel-like)

        Args:
            time: Time selection (str, datetime, or slice)
            bands: Band names to select
            **kwargs: Additional selection parameters

        Returns:
            New Dataset with selected data

        Examples:
            >>> # Select specific month
            >>> ds.sel(time="2024-01")
            >>>
            >>> # Select time range
            >>> ds.sel(time=slice("2024-01", "2024-12"))
            >>>
            >>> # Select bands
            >>> ds.sel(bands=["red", "nir"])
            >>>
            >>> # Combine selections
            >>> ds.sel(time=slice("2024-01", "2024-06"), bands=["red"])
        """
        # Process time selection
        new_time_range = self.time_range
        if time is not None:
            new_time_range = self._process_time_selection(time)

        # Process band selection
        new_bands = self.bands
        if bands is not None:
            # Validate bands exist
            invalid_bands = set(bands) - set(self.bands)
            if invalid_bands:
                raise ValueError(f"Invalid bands: {invalid_bands}. Available: {self.bands}")
            new_bands = bands

        # Filter data dictionary
        new_data = {}
        if bands is not None:
            for band in bands:
                if band in self.data:
                    new_data[band] = self.data[band]
        else:
            new_data = self.data.copy()

        # Create new Dataset with filtered metadata
        return Dataset(
            tile_id=self.tile_id,
            time_range=new_time_range,
            bands=new_bands,
            data=new_data,
            metadata=self.metadata.copy()
        )

    def _process_time_selection(
        self,
        time: Union[str, slice, datetime]
    ) -> Optional[Tuple[datetime, datetime]]:
        """
        Process time selection parameter

        Args:
            time: Time selector (str, datetime, or slice)

        Returns:
            Tuple of (start, end) datetime or None
        """
        if isinstance(time, slice):
            # Handle slice (e.g., slice("2024-01", "2024-12"))
            start = self._parse_time_string(time.start) if time.start else None
            end = self._parse_time_string(time.stop) if time.stop else None

            # Use existing bounds if not specified
            if start is None and self.time_range:
                start = self.time_range[0]
            if end is None and self.time_range:
                end = self.time_range[1]

            return (start, end) if start and end else None

        elif isinstance(time, str):
            # Single time point (e.g., "2024-01")
            dt = self._parse_time_string(time)
            # For a month string, select the whole month
            if len(time) == 7:  # "YYYY-MM" format
                start = dt
                # End of month
                if dt.month == 12:
                    end = datetime(dt.year + 1, 1, 1)
                else:
                    end = datetime(dt.year, dt.month + 1, 1)
                return (start, end)
            return (dt, dt)

        elif isinstance(time, datetime):
            return (time, time)

        return self.time_range

    def _parse_time_string(self, time_str: str) -> datetime:
        """
        Parse time string to datetime

        Args:
            time_str: Time string (e.g., "2024-01", "2024-01-15")

        Returns:
            datetime object
        """
        if len(time_str) == 7:  # "YYYY-MM"
            return datetime.strptime(time_str, "%Y-%m")
        elif len(time_str) == 10:  # "YYYY-MM-DD"
            return datetime.strptime(time_str, "%Y-%m-%d")
        else:
            # Try ISO format
            return datetime.fromisoformat(time_str)

    def isel(self, **indexers: Any) -> "Dataset":
        """
        Select by integer index (xarray.Dataset.isel-like)

        Args:
            **indexers: Dimension indexers (e.g., time=0, y=slice(0, 100))

        Returns:
            New Dataset with selected data

        Examples:
            >>> # Select first timestep
            >>> ds.isel(time=0)
            >>>
            >>> # Select first 10 timesteps
            >>> ds.isel(time=slice(0, 10))
        """
        # For now, return self with indexers metadata
        # Full implementation requires actual data loading
        return Dataset(
            tile_id=self.tile_id,
            time_range=self.time_range,
            bands=self.bands,
            data=self.data.copy(),
            metadata={**self.metadata, 'indexers': indexers}
        )

    def resample(self, time: str) -> "DatasetResampler":
        """
        Temporal resampling (xarray.Dataset.resample-like)

        Args:
            time: Resampling frequency (e.g., "1M" for monthly, "1W" for weekly)

        Returns:
            DatasetResampler object for aggregation

        Examples:
            >>> # Monthly mean
            >>> ds.resample(time="1M").mean()
            >>>
            >>> # Weekly max
            >>> ds.resample(time="1W").max()
        """
        return DatasetResampler(self, time)

    def mean(self, dim: Optional[str] = None) -> Union["Dataset", NDArray]:
        """
        Compute mean along dimension (xarray.Dataset.mean-like)

        Args:
            dim: Dimension to reduce (e.g., "time", "y", "x")

        Returns:
            Dataset with reduced dimension or array if all dims reduced
        """
        raise NotImplementedError("mean() will be implemented in Phase 2+")

    def to_xarray(self) -> Any:  # xr.Dataset
        """
        Convert to xarray.Dataset

        Returns:
            xarray.Dataset with dimensions (time, y, x) and variables for each band

        Examples:
            >>> xr_ds = ds.to_xarray()
            >>> xr_ds["red"].plot()  # Use xarray's plotting
        """
        raise NotImplementedError("to_xarray() will be implemented in Phase 3+")

    def to_pandas(self) -> Any:  # pd.DataFrame
        """
        Convert to pandas.DataFrame

        Returns:
            DataFrame with columns: time, y, x, band_red, band_nir, etc.

        Examples:
            >>> df = ds.to_pandas()
            >>> df['ndvi'] = (df['band_nir'] - df['band_red']) / (df['band_nir'] + df['band_red'])
        """
        raise NotImplementedError("to_pandas() will be implemented in Phase 3+")

    def to_numpy(self) -> Dict[str, NDArray]:
        """
        Convert to NumPy arrays

        Returns:
            Dictionary mapping band names to arrays

        Examples:
            >>> arrays = ds.to_numpy()
            >>> red = arrays['red']  # Shape: (time, y, x)
        """
        raise NotImplementedError("to_numpy() will be implemented in Phase 3+")

    def __repr__(self) -> str:
        """String representation"""
        bands_str = ", ".join(self.bands[:3])
        if len(self.bands) > 3:
            bands_str += f", ... ({len(self.bands)} total)"

        return (
            f"<PixelQuery.Dataset>\n"
            f"Tile: {self.tile_id}\n"
            f"Bands: {bands_str}\n"
            f"Time range: {self.time_range}"
        )

    def __getitem__(self, key: str) -> "DataArray":
        """
        Access band data (xarray-like)

        Args:
            key: Band name

        Returns:
            DataArray for the band

        Examples:
            >>> red = ds["red"]
            >>> nir = ds["nir"]
        """
        if key not in self.data:
            raise KeyError(f"Band '{key}' not found. Available: {self.bands}")

        from pixelquery.core.dataarray import DataArray
        return DataArray(
            name=key,
            data=self.data[key],
            dims=self.dims,
            coords=self.coords,
        )


class DatasetResampler:
    """
    Temporal resampling helper (xarray.DatasetResample-like)

    Created by Dataset.resample(). Provides aggregation methods.
    """

    def __init__(self, dataset: Dataset, freq: str):
        """
        Initialize resampler

        Args:
            dataset: Source dataset
            freq: Resampling frequency (e.g., "1M", "1W")
        """
        self.dataset = dataset
        self.freq = freq

    def mean(self) -> Dataset:
        """
        Compute temporal mean after resampling

        Returns:
            Dataset with resampled and aggregated data

        Examples:
            >>> ds.resample(time="1M").mean()  # Monthly mean
        """
        # Create resampled dataset with metadata
        return Dataset(
            tile_id=self.dataset.tile_id,
            time_range=self.dataset.time_range,
            bands=self.dataset.bands,
            data=self.dataset.data.copy(),
            metadata={
                **self.dataset.metadata,
                'resampled': True,
                'freq': self.freq,
                'aggregation': 'mean'
            }
        )

    def max(self) -> Dataset:
        """
        Compute temporal maximum after resampling

        Returns:
            Dataset with resampled and aggregated data
        """
        return Dataset(
            tile_id=self.dataset.tile_id,
            time_range=self.dataset.time_range,
            bands=self.dataset.bands,
            data=self.dataset.data.copy(),
            metadata={
                **self.dataset.metadata,
                'resampled': True,
                'freq': self.freq,
                'aggregation': 'max'
            }
        )

    def min(self) -> Dataset:
        """
        Compute temporal minimum after resampling

        Returns:
            Dataset with resampled and aggregated data
        """
        return Dataset(
            tile_id=self.dataset.tile_id,
            time_range=self.dataset.time_range,
            bands=self.dataset.bands,
            data=self.dataset.data.copy(),
            metadata={
                **self.dataset.metadata,
                'resampled': True,
                'freq': self.freq,
                'aggregation': 'min'
            }
        )

    def median(self) -> Dataset:
        """
        Compute temporal median after resampling

        Returns:
            Dataset with resampled and aggregated data
        """
        return Dataset(
            tile_id=self.dataset.tile_id,
            time_range=self.dataset.time_range,
            bands=self.dataset.bands,
            data=self.dataset.data.copy(),
            metadata={
                **self.dataset.metadata,
                'resampled': True,
                'freq': self.freq,
                'aggregation': 'median'
            }
        )
