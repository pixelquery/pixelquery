"""
Query Result Protocol

Container for query results with multiple output formats.
"""

from typing import Protocol, List, Dict, Any
from numpy.typing import NDArray


class QueryResult(Protocol):
    """
    Query result container

    Provides multiple output formats for different use cases:
    - Pandas: Statistical analysis, plotting
    - Xarray: Spatiotemporal cubes, NetCDF export
    - NumPy: Raw arrays for custom processing
    """

    tiles: List[Dict[str, Any]]  # List of {tile_id, date, bands: {red: ndarray, ...}}

    def to_pandas(self) -> Any:  # pd.DataFrame
        """
        Convert to Pandas DataFrame

        Returns:
            DataFrame with columns: tile_id, acquisition_date, product_id,
            band_red, band_nir, etc. (flattened view)
        """
        ...

    def to_xarray(self) -> Any:  # xr.Dataset
        """
        Convert to Xarray Dataset

        Returns:
            Dataset with dimensions (time, y, x) and variables for each band.
            Tiles are spatially mosaicked.
        """
        ...

    def to_numpy(self) -> Dict[str, NDArray]:
        """
        Convert to NumPy arrays

        Returns:
            Dictionary mapping band names to stacked arrays
        """
        ...
