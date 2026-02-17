"""
Band Math accessor for xarray Datasets.

Evaluates band math expressions using band index references (b0, b1, b2, ...)
or band name coordinates from the dataset.

Usage:
    >>> ds = pq.open_xarray("./warehouse")
    >>> ndvi = ds.bandmath("(b3 - b2) / (b3 + b2)")
    >>> ratio = ds.bandmath("nir / red")
"""

import numpy as np
import xarray as xr


@xr.register_dataset_accessor("bandmath")
class BandMathAccessor:
    """
    xarray Dataset accessor for band math expressions.

    Bands can be referenced by:
    - Index: b0, b1, b2, b3, ... (order matches the band dimension)
    - Name: blue, green, red, nir, ... (if band coordinates are assigned)

    All operations are lazy — actual COG reads happen only on .compute().
    """

    def __init__(self, ds: xr.Dataset):
        self._ds = ds
        self._var = self._detect_var()

    def _detect_var(self) -> str:
        """Find the primary data variable with a 'band' dimension."""
        for name, var in self._ds.data_vars.items():
            if "band" in var.dims:
                return name
        names = list(self._ds.data_vars)
        if names:
            return names[0]
        raise ValueError("Dataset has no data variables")

    @property
    def bands(self):
        """List available band references (names or indices)."""
        da = self._ds[self._var]
        if "band" in da.coords:
            names = list(da.coords["band"].values)
            return {f"b{i}": name for i, name in enumerate(names)}
        n = da.sizes.get("band", 0)
        return {f"b{i}": i for i in range(n)}

    def __call__(self, expr: str) -> xr.DataArray:
        """
        Evaluate a band math expression.

        Args:
            expr: Expression using b0/b1/b2... or band names.
                  numpy is available as 'np'.

        Returns:
            xr.DataArray with the computed result (lazy).

        Examples:
            >>> ds.bandmath("(b3 - b2) / (b3 + b2)")        # NDVI by index
            >>> ds.bandmath("(nir - red) / (nir + red)")     # NDVI by name
            >>> ds.bandmath("np.sqrt(b3 * b2)")              # custom
            >>> ds.bandmath("2.5 * (b3 - b2) / (b3 + 6*b2 - 7.5*b0 + 1)")  # EVI
        """
        namespace = {"np": np}
        da = self._ds[self._var]

        # Add b0, b1, b2, ... index references
        n_bands = da.sizes.get("band", 0)
        for i in range(n_bands):
            namespace[f"b{i}"] = da.isel(band=i)

        # Add band name references (if coordinates exist)
        if "band" in da.coords:
            for band_name in da.coords["band"].values:
                namespace[str(band_name)] = da.sel(band=band_name)

        result = eval(expr, {"__builtins__": {}}, namespace)
        if isinstance(result, xr.DataArray):
            result.name = "bandmath"
        return result

    def __repr__(self) -> str:
        band_map = self.bands
        lines = ["<BandMath>"]
        for idx, name in band_map.items():
            lines.append(f"  {idx}: {name}")
        return "\n".join(lines)
