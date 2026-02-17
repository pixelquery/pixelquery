"""
Dataset class - xarray-inspired API for PixelQuery

Provides a familiar interface for data scientists working with satellite imagery.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from numpy.typing import NDArray

    from pixelquery.core.dataarray import DataArray


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
        time_range: tuple[datetime, datetime] | None = None,
        bands: list[str] | None = None,
        data: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
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
        self.dims: dict[str, int] = {}
        self.coords: dict[str, NDArray] = {}

    def sel(
        self,
        time: str | slice | datetime | None = None,
        bands: list[str] | None = None,
        **kwargs: Any,
    ) -> Dataset:
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
            metadata=self.metadata.copy(),
        )

    def _process_time_selection(
        self, time: str | slice | datetime
    ) -> tuple[datetime, datetime] | None:
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

    def isel(self, **indexers: Any) -> Dataset:
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
            metadata={**self.metadata, "indexers": indexers},
        )

    def resample(self, time: str) -> DatasetResampler:
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

    def mean(self, dim: str | None = None) -> Dataset | NDArray:
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

            >>> # Access as DataArray
            >>> red = xr_ds["red"]
            >>> red.sel(time="2024-01").plot()
        """
        try:
            import xarray as xr
        except ImportError as e:
            raise ImportError(
                "xarray is required for to_xarray(). Install with: pip install xarray"
            ) from e

        # Build data variables
        data_vars = {}
        times = None
        y_coords = None
        x_coords = None

        for band_name in self.bands:
            if band_name not in self.data:
                continue

            band_data = self.data[band_name]

            # Handle list of arrays (time series)
            if isinstance(band_data, list):
                if len(band_data) == 0:
                    continue

                # Stack time series into 3D array (time, y, x)
                stacked = np.stack(band_data, axis=0)

                # Extract times from metadata if available
                if times is None and "times" in self.metadata:
                    times = self.metadata["times"]
                elif times is None:
                    times = list(range(len(band_data)))

                # Create coordinate arrays if not set
                if y_coords is None:
                    _, h, w = stacked.shape
                    y_coords = np.arange(h)
                    x_coords = np.arange(w)

                data_vars[band_name] = (["time", "y", "x"], stacked)

            # Handle single array
            elif isinstance(band_data, np.ndarray):
                if band_data.ndim == 2:
                    # Single observation (y, x)
                    h, w = band_data.shape
                    if y_coords is None:
                        y_coords = np.arange(h)
                        x_coords = np.arange(w)
                    data_vars[band_name] = (["y", "x"], band_data)
                elif band_data.ndim == 3:
                    # Time series (time, y, x)
                    t, h, w = band_data.shape
                    if times is None:
                        times = list(range(t))
                    if y_coords is None:
                        y_coords = np.arange(h)
                        x_coords = np.arange(w)
                    data_vars[band_name] = (["time", "y", "x"], band_data)

        # Build coordinates
        coords = {}
        if times is not None:
            coords["time"] = times
        if y_coords is not None:
            coords["y"] = y_coords
        if x_coords is not None:
            coords["x"] = x_coords

        # Add spatial coordinates if bounds available
        if "bounds" in self.metadata:
            minx, miny, maxx, maxy = self.metadata["bounds"]
            if y_coords is not None and x_coords is not None:
                # Calculate actual coordinates
                lon_coords = np.linspace(minx, maxx, len(x_coords))
                lat_coords = np.linspace(maxy, miny, len(y_coords))  # y is flipped
                coords["lon"] = (["x"], lon_coords)
                coords["lat"] = (["y"], lat_coords)

        # Build attributes
        attrs = {
            "tile_id": self.tile_id,
            "crs": "EPSG:4326",
        }
        if self.time_range:
            attrs["time_range"] = str(self.time_range)
        attrs.update({k: str(v) for k, v in self.metadata.items() if k != "bounds"})

        return xr.Dataset(data_vars, coords=coords, attrs=attrs)

    def to_pandas(self) -> Any:  # pd.DataFrame
        """
        Convert to pandas.DataFrame

        Returns:
            DataFrame with columns: time, y, x, band_red, band_nir, etc.

        Examples:
            >>> df = ds.to_pandas()
            >>> df['ndvi'] = (df['band_nir'] - df['band_red']) / (df['band_nir'] + df['band_red'])
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is required for to_pandas(). Install with: pip install pandas"
            ) from e

        records = []
        times = self.metadata.get("times", [])

        for band_name in self.bands:
            if band_name not in self.data:
                continue

            band_data = self.data[band_name]

            if isinstance(band_data, list):
                for t_idx, arr in enumerate(band_data):
                    time_val = times[t_idx] if t_idx < len(times) else t_idx
                    # Flatten spatial dimensions
                    for y_idx in range(arr.shape[0]):
                        for x_idx in range(arr.shape[1]):
                            records.append(
                                {
                                    "time": time_val,
                                    "y": y_idx,
                                    "x": x_idx,
                                    "band": band_name,
                                    "value": arr[y_idx, x_idx],
                                }
                            )

        return pd.DataFrame(records)

    def to_geotiff(
        self,
        output_path: str,
        band: str | None = None,
        time_index: int = 0,
        all_bands: bool = False,
        all_times: bool = False,
    ) -> str | list[str]:
        """
        Export to GeoTIFF file(s)

        Args:
            output_path: Output file path (or directory if all_times=True)
            band: Band name to export (required if not all_bands)
            time_index: Time index to export (ignored if all_times=True)
            all_bands: If True, export all bands as multi-band GeoTIFF
            all_times: If True, export each time step as separate file

        Returns:
            Path to created file(s)

        Examples:
            >>> # Single band, single time
            >>> ds.to_geotiff("output.tif", band="red", time_index=0)

            >>> # All bands as multi-band GeoTIFF
            >>> ds.to_geotiff("output.tif", all_bands=True, time_index=0)

            >>> # Export entire time series
            >>> paths = ds.to_geotiff("outputs/", band="red", all_times=True)
        """
        try:
            from rasterio.crs import CRS
            from rasterio.transform import from_bounds
        except ImportError as e:
            raise ImportError(
                "rasterio is required for to_geotiff(). Install with: pip install rasterio"
            ) from e

        from pathlib import Path

        # Get bounds
        bounds = self.metadata.get("bounds")
        if bounds is None:
            raise ValueError("Dataset has no bounds metadata. Cannot create GeoTIFF.")

        minx, miny, maxx, maxy = bounds

        # Determine what to export
        if all_bands:
            bands_to_export = self.bands
        elif band:
            if band not in self.bands:
                raise ValueError(f"Band '{band}' not found. Available: {self.bands}")
            bands_to_export = [band]
        else:
            raise ValueError("Specify 'band' or set all_bands=True")

        # Get data shape
        sample_data = None
        for b in bands_to_export:
            if b in self.data:
                d = self.data[b]
                if isinstance(d, list) and len(d) > 0:
                    sample_data = d[0]
                elif isinstance(d, np.ndarray):
                    sample_data = d if d.ndim == 2 else d[0]
                break

        if sample_data is None:
            raise ValueError("No data to export")

        height, width = sample_data.shape
        transform = from_bounds(minx, miny, maxx, maxy, width, height)
        crs = CRS.from_epsg(4326)

        # Determine time steps
        if all_times:
            # Export multiple files
            output_dir = Path(output_path)
            output_dir.mkdir(parents=True, exist_ok=True)

            output_files = []
            times = self.metadata.get("times", [])

            # Determine number of time steps
            n_times = 0
            for b in bands_to_export:
                if b in self.data:
                    d = self.data[b]
                    if isinstance(d, list):
                        n_times = max(n_times, len(d))
                    elif isinstance(d, np.ndarray) and d.ndim == 3:
                        n_times = max(n_times, d.shape[0])

            for t_idx in range(n_times):
                time_str = str(times[t_idx]) if t_idx < len(times) else f"t{t_idx:04d}"
                # Clean time string for filename
                time_str = time_str.replace(":", "-").replace(" ", "_")[:20]

                if all_bands:
                    out_file = output_dir / f"{self.tile_id}_{time_str}_allbands.tif"
                else:
                    out_file = output_dir / f"{self.tile_id}_{time_str}_{bands_to_export[0]}.tif"

                self._write_geotiff(
                    str(out_file), bands_to_export, t_idx, transform, crs, height, width
                )
                output_files.append(str(out_file))

            return output_files

        else:
            # Export single file
            self._write_geotiff(
                output_path, bands_to_export, time_index, transform, crs, height, width
            )
            return output_path

    def _write_geotiff(
        self,
        path: str,
        bands: list[str],
        time_index: int,
        transform,
        crs,
        height: int,
        width: int,
    ) -> None:
        """Write a single GeoTIFF file"""
        import rasterio

        # Collect band data
        band_arrays = []
        for band_name in bands:
            if band_name not in self.data:
                continue

            band_data = self.data[band_name]

            if isinstance(band_data, list):
                if time_index < len(band_data):
                    arr = band_data[time_index]
                else:
                    arr = np.zeros((height, width), dtype=np.uint16)
            elif isinstance(band_data, np.ndarray):
                if band_data.ndim == 3 and time_index < band_data.shape[0]:
                    arr = band_data[time_index]
                elif band_data.ndim == 2:
                    arr = band_data
                else:
                    arr = np.zeros((height, width), dtype=np.uint16)
            else:
                arr = np.zeros((height, width), dtype=np.uint16)

            band_arrays.append(arr.astype(np.uint16))

        # Write GeoTIFF
        profile = {
            "driver": "GTiff",
            "dtype": "uint16",
            "width": width,
            "height": height,
            "count": len(band_arrays),
            "crs": crs,
            "transform": transform,
            "compress": "lzw",
            "tiled": True,
            "blockxsize": 256,
            "blockysize": 256,
        }

        with rasterio.open(path, "w", **profile) as dst:
            for i, arr in enumerate(band_arrays, 1):
                dst.write(arr, i)

            # Set band descriptions
            dst.descriptions = tuple(bands)

    def to_numpy(self) -> dict[str, NDArray]:
        """
        Convert to NumPy arrays

        Returns:
            Dictionary mapping band names to arrays

        Examples:
            >>> arrays = ds.to_numpy()
            >>> red = arrays['red']  # Shape: (time, y, x)
        """
        result = {}
        for band_name in self.bands:
            if band_name in self.data:
                band_data = self.data[band_name]
                # Handle DataArray or raw numpy array
                if hasattr(band_data, "data"):
                    result[band_name] = np.asarray(band_data.data)
                else:
                    result[band_name] = np.asarray(band_data)
        return result

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

    def __getitem__(self, key: str) -> DataArray:
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
                "resampled": True,
                "freq": self.freq,
                "aggregation": "mean",
            },
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
                "resampled": True,
                "freq": self.freq,
                "aggregation": "max",
            },
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
                "resampled": True,
                "freq": self.freq,
                "aggregation": "min",
            },
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
                "resampled": True,
                "freq": self.freq,
                "aggregation": "median",
            },
        )
