"""
Icechunk Virtual Reader

Reads virtual zarr datasets from Icechunk repository.
Supports time-range, spatial, and band filtering.
Returns lazy xarray.Dataset objects backed by dask arrays.
"""

import logging
from datetime import UTC, datetime
from typing import Any

import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)


class IcechunkVirtualReader:
    """
    Reads virtual zarr datasets from an Icechunk repository.

    Data is loaded lazily -- actual COG byte reads happen only
    when .compute() or .values is called on the xarray objects.
    """

    def __init__(self, storage_manager):
        """
        Args:
            storage_manager: IcechunkStorageManager instance (initialized)
        """
        self.storage = storage_manager

    def list_scenes(
        self,
        time_range: tuple[datetime, datetime] | None = None,
        bounds: tuple[float, float, float, float] | None = None,
        product_id: str | None = None,
        snapshot_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        List scenes matching filter criteria.

        Uses _scenes_index for fast lookup without opening each group.

        Args:
            time_range: (start, end) datetime range filter
            bounds: (minx, miny, maxx, maxy) spatial intersection filter
            product_id: Filter by product identifier
            snapshot_id: Query at specific snapshot (Time Travel)

        Returns:
            List of scene metadata dicts with keys:
            group, acquisition_time, product_id, bounds, band_names
        """
        import zarr

        session = self.storage.readonly_session(snapshot_id=snapshot_id)
        store = session.store
        root = zarr.open_group(store, mode="r")

        if "_scenes_index" not in root:
            return []

        scenes = list(root["_scenes_index"].attrs.get("scenes", []))

        # Filter by time range
        if time_range:
            start, end = time_range
            # Ensure timezone-aware comparison
            if start.tzinfo is None:
                start = start.replace(tzinfo=UTC)
            if end.tzinfo is None:
                end = end.replace(tzinfo=UTC)

            filtered = []
            for s in scenes:
                acq = s.get("acquisition_time")
                if acq:
                    t = datetime.fromisoformat(acq)
                    if t.tzinfo is None:
                        t = t.replace(tzinfo=UTC)
                    if start <= t <= end:
                        filtered.append(s)
            scenes = filtered

        # Filter by spatial bounds (intersection test)
        if bounds:
            minx, miny, maxx, maxy = bounds
            filtered = []
            for s in scenes:
                sb = s.get("bounds")
                if sb is None:
                    # No bounds info -> include by default
                    filtered.append(s)
                elif not (sb[2] < minx or sb[0] > maxx or sb[3] < miny or sb[1] > maxy):
                    filtered.append(s)
            scenes = filtered

        # Filter by product_id
        if product_id:
            scenes = [s for s in scenes if s.get("product_id") == product_id]

        return scenes

    def _apply_cloud_mask(
        self,
        data: xr.DataArray,
        mask_path: str,
        product_id: str | None = None,
    ) -> xr.DataArray:
        """
        Apply cloud mask from a separate mask file.

        Reads the mask file with rasterio, extracts the cloud band,
        and masks out non-clear pixels based on ProductProfile settings.
        """
        import rasterio

        from pixelquery.catalog.product_profile import BUILTIN_PROFILES

        # Look up CloudMask config from ProductProfile
        profile = BUILTIN_PROFILES.get(product_id) if product_id else None
        if profile is None or profile.cloud_mask is None:
            logger.warning("No CloudMask config for product '%s', skipping", product_id)
            return data

        cm = profile.cloud_mask

        try:
            with rasterio.open(mask_path) as src:
                # Read the cloud mask band (1-indexed in rasterio)
                mask_data = src.read(cm.band_index + 1)

            # Build boolean mask: True where pixel is clear
            clear = np.isin(mask_data, cm.clear_values)

            # Broadcast to match data shape (data has band dim, mask doesn't)
            # mask_data shape: (y, x), data shape: (band, y, x) or similar
            clear_da = xr.DataArray(
                clear,
                dims=["y", "x"],
                coords={d: data.coords[d] for d in ["y", "x"] if d in data.coords},
            )

            return data.where(clear_da)
        except Exception as e:
            logger.warning("Failed to apply cloud mask from %s: %s", mask_path, e)
            return data

    def open_scene(
        self,
        group_name: str,
        bands: list[str] | None = None,
        snapshot_id: str | None = None,
        cloud_mask: bool = False,
    ) -> xr.Dataset:
        """
        Open a single scene as an xarray.Dataset.

        The returned dataset has dimensions (band, y, x) with band name
        coordinates assigned from scene metadata.

        Args:
            group_name: Zarr group name (e.g. "scene_20250101_a1b2c3d4")
            bands: Optional band name filter (e.g. ["red", "nir"])
            snapshot_id: Query at specific snapshot

        Returns:
            xr.Dataset with variable "data" having dims (band, y, x)
        """
        import zarr

        session = self.storage.readonly_session(snapshot_id=snapshot_id)
        store = session.store

        # Open zarr group as xarray dataset
        ds = xr.open_zarr(store, group=group_name, consolidated=False)

        # Read scene metadata from group attrs
        root = zarr.open_group(store, mode="r")
        attrs = dict(root[group_name].attrs)
        band_names = attrs.get("band_names", [])
        acq_time = attrs.get("acquisition_time")

        # VirtualTIFF stores all bands as variable "0" with shape (band, y, x)
        if "0" not in ds:
            return ds

        data = ds["0"]

        # Assign band name coordinates
        if band_names and len(band_names) == data.sizes.get("band", 0):
            data = data.assign_coords(band=band_names)

        # Filter bands if requested
        if bands and band_names:
            available = set(band_names)
            selected = [b for b in bands if b in available]
            if selected:
                data = data.sel(band=selected)

        # Apply cloud mask if requested and mask file is available
        if cloud_mask:
            mask_file = attrs.get("mask_source_file")
            if mask_file:
                data = self._apply_cloud_mask(
                    data,
                    mask_file,
                    product_id=attrs.get("product_id"),
                )

        # Build result dataset
        result = data.to_dataset(name="data")

        # Store metadata as dataset attrs
        result.attrs["group_name"] = group_name
        result.attrs["acquisition_time"] = acq_time or ""
        result.attrs["product_id"] = attrs.get("product_id", "")
        if attrs.get("bounds"):
            result.attrs["bounds"] = attrs["bounds"]
        if attrs.get("crs"):
            result.attrs["crs"] = attrs["crs"]

        return result

    def open_xarray(
        self,
        time_range: tuple[datetime, datetime] | None = None,
        bounds: tuple[float, float, float, float] | None = None,
        bands: list[str] | None = None,
        product_id: str | None = None,
        snapshot_id: str | None = None,
        cloud_mask: bool = False,
    ) -> xr.Dataset:
        """
        Open filtered scenes as a concatenated xarray.Dataset.

        This is the primary query interface. Returns a lazy dataset with
        dimensions (time, band, y, x). Data is only loaded from COG files
        when .compute() or .values is called.

        Args:
            time_range: (start, end) datetime range filter
            bounds: (minx, miny, maxx, maxy) spatial filter
            bands: Band name filter (e.g. ["red", "nir"])
            product_id: Product identifier filter
            snapshot_id: Icechunk snapshot for Time Travel

        Returns:
            xr.Dataset with dims (time, band, y, x) if multiple scenes,
            or (band, y, x) if single scene

        Raises:
            ValueError: If no scenes match the query filters
        """
        scenes = self.list_scenes(
            time_range=time_range,
            bounds=bounds,
            product_id=product_id,
            snapshot_id=snapshot_id,
        )

        if not scenes:
            raise ValueError("No scenes match the query filters")

        # Sort by acquisition time
        scenes.sort(key=lambda s: s.get("acquisition_time", ""))

        # Open each scene
        datasets = []
        times = []
        for scene_meta in scenes:
            try:
                ds = self.open_scene(
                    scene_meta["group"],
                    bands=bands,
                    snapshot_id=snapshot_id,
                    cloud_mask=cloud_mask,
                )

                # Parse acquisition time for the time coordinate
                acq_str = scene_meta.get("acquisition_time")
                if acq_str:
                    acq_time = datetime.fromisoformat(acq_str)
                    if acq_time.tzinfo is None:
                        acq_time = acq_time.replace(tzinfo=UTC)
                    times.append(acq_time)
                else:
                    times.append(None)

                datasets.append(ds)
            except Exception as e:
                logger.warning("Failed to open scene %s: %s", scene_meta["group"], e)

        if not datasets:
            raise ValueError("No scenes could be opened successfully")

        # Single scene - return as-is
        if len(datasets) == 1:
            return datasets[0]

        # Multiple scenes - concat along time dimension
        # Create time coordinate from acquisition times
        import pandas as pd

        _time_coords = pd.DatetimeIndex([t for t in times if t is not None])

        # Add time dimension and ensure spatial dims are indexed for alignment
        expanded = []
        for ds, t in zip(datasets, times, strict=False):
            # Assign integer coords to unindexed spatial dims so xr.concat
            # can do outer join when scenes have different extents
            for dim in ("y", "x"):
                if dim in ds.dims and dim not in ds.coords:
                    ds = ds.assign_coords({dim: np.arange(ds.sizes[dim])})
            if t is not None:
                ds = ds.expand_dims(time=[t])
            expanded.append(ds)

        # join="outer" pads shorter arrays with NaN (satellite scenes may vary in extent)
        combined = xr.concat(expanded, dim="time", join="outer")
        return combined

    def get_snapshot_history(self) -> list[dict[str, Any]]:
        """Get Icechunk snapshot history for Time Travel."""
        return self.storage.get_snapshot_history()
