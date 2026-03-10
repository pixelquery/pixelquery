"""
Icechunk Virtual Reader

Reads virtual zarr datasets from Icechunk repository.
Supports time-range, spatial, and band filtering.
Returns lazy xarray.Dataset objects backed by dask arrays.
"""

import bisect
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
        # Optimization 1: instance-level cache for the raw scenes index
        self._cached_scenes_index: list[dict[str, Any]] | None = None
        # Optimization 2: pre-sorted scenes and times for binary search
        self._sorted_scenes: list[dict[str, Any]] = []
        self._sorted_times: list[datetime] = []

    def _load_scenes_index(self) -> list[dict[str, Any]]:
        """Read the raw scenes index from zarr storage, populating the cache."""
        import zarr

        session = self.storage.readonly_session(snapshot_id=None)
        store = session.store
        root = zarr.open_group(store, mode="r")

        if "_scenes_index" not in root:
            return []

        raw: list[dict[str, Any]] = list(root["_scenes_index"].attrs.get("scenes", []))  # type: ignore[arg-type]

        # Pre-sort by acquisition_time for O(log N) binary search in list_scenes
        def _to_aware(acq: str) -> datetime:
            t = datetime.fromisoformat(acq)
            if t.tzinfo is None:
                t = t.replace(tzinfo=UTC)
            return t

        sortable = [s for s in raw if s.get("acquisition_time")]
        sortable.sort(key=lambda s: s["acquisition_time"])
        self._sorted_scenes = sortable
        self._sorted_times = [_to_aware(s["acquisition_time"]) for s in sortable]

        return raw

    def list_scenes(
        self,
        time_range: tuple[datetime, datetime] | None = None,
        bounds: tuple[float, float, float, float] | None = None,
        product_id: str | None = None,
        snapshot_id: str | None = None,
        bounds_crs: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        List scenes matching filter criteria.

        Uses _scenes_index for fast lookup without opening each group.
        The index is cached after the first read; time_range filtering uses
        binary search (O(log N)) on the pre-sorted list.

        Args:
            time_range: (start, end) datetime range filter
            bounds: (minx, miny, maxx, maxy) spatial intersection filter
            product_id: Filter by product identifier
            snapshot_id: Query at specific snapshot (Time Travel)
            bounds_crs: CRS of the *bounds* parameter (e.g. "EPSG:4326").
                        When provided and differs from a scene's CRS, the
                        query bounds are transformed before intersection test.

        Returns:
            List of scene metadata dicts with keys:
            group, acquisition_time, product_id, bounds, crs, band_names
        """
        # Snapshot queries bypass the cache (different store state)
        if snapshot_id is not None:
            import zarr

            session = self.storage.readonly_session(snapshot_id=snapshot_id)
            store = session.store
            root = zarr.open_group(store, mode="r")

            if "_scenes_index" not in root:
                return []

            scenes: list[dict[str, Any]] = list(root["_scenes_index"].attrs.get("scenes", []))  # type: ignore[arg-type]
        else:
            # Optimization 1: populate cache on first call only
            if self._cached_scenes_index is None:
                self._cached_scenes_index = self._load_scenes_index()
            scenes = list(self._cached_scenes_index)  # shallow copy to avoid mutation

        # Filter by time range
        if time_range:
            start, end = time_range
            # Ensure timezone-aware comparison
            if start.tzinfo is None:
                start = start.replace(tzinfo=UTC)
            if end.tzinfo is None:
                end = end.replace(tzinfo=UTC)

            if snapshot_id is None and self._sorted_times:
                # Optimization 2: O(log N) binary search on pre-sorted list
                left = bisect.bisect_left(self._sorted_times, start)
                right = bisect.bisect_right(self._sorted_times, end)
                scenes = self._sorted_scenes[left:right]
            else:
                # Fallback linear scan (snapshot queries or empty sorted list)
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
            from pixelquery.core.api import _needs_crs_transform, _transform_bounds

            minx, miny, maxx, maxy = bounds
            filtered = []
            for s in scenes:
                sb = s.get("bounds")
                if sb is None:
                    # No bounds info -> include by default
                    filtered.append(s)
                    continue
                # Transform query bounds to scene CRS if needed
                qb: tuple[float, float, float, float] = (minx, miny, maxx, maxy)
                scene_crs = s.get("crs")
                if bounds_crs and scene_crs and _needs_crs_transform(bounds_crs, scene_crs):
                    qb = _transform_bounds(qb, bounds_crs, scene_crs)
                if not (sb[2] < qb[0] or sb[0] > qb[2] or sb[3] < qb[1] or sb[1] > qb[3]):
                    filtered.append(s)
            scenes = filtered

        # Filter by product_id
        if product_id:
            scenes = [s for s in scenes if s.get("product_id") == product_id]

        return scenes

    def _apply_cloud_mask(
        self,
        data: xr.DataArray,
        attrs: dict,
        snapshot_id: str | None = None,
    ) -> xr.DataArray:
        """
        Apply cloud mask to data.

        Reads the mask from the Icechunk store (GDAL-free) if the scene was
        ingested with mask virtual references. Falls back to a warning for
        legacy scenes that only have mask_source_file.

        Args:
            data: Data array to mask
            attrs: Scene group attributes (contains mask_group or mask_source_file)
            snapshot_id: Icechunk snapshot for Time Travel
        """
        from pixelquery.catalog.product_profile import BUILTIN_PROFILES

        product_id = attrs.get("product_id")
        profile = BUILTIN_PROFILES.get(product_id) if product_id else None
        if profile is None or profile.cloud_mask is None:
            logger.warning("No CloudMask config for product '%s', skipping", product_id)
            return data

        cm = profile.cloud_mask

        # Prefer Icechunk-stored mask (GDAL-free)
        mask_group = attrs.get("mask_group")
        if mask_group:
            try:
                session = self.storage.readonly_session(snapshot_id=snapshot_id)
                store = session.store
                mask_ds = xr.open_zarr(store, group=mask_group, consolidated=False)

                if "0" not in mask_ds:
                    logger.warning("Mask group %s has no data variable, skipping", mask_group)
                    return data

                mask_var = mask_ds["0"]

                # Extract the cloud band
                if "band" in mask_var.dims and mask_var.sizes["band"] > cm.band_index:
                    cloud_band = mask_var.isel(band=cm.band_index).values
                elif mask_var.ndim == 2:
                    cloud_band = mask_var.values
                else:
                    cloud_band = (
                        mask_var.isel(band=0).values if "band" in mask_var.dims else mask_var.values
                    )

                # Build clear mask
                clear = np.isin(cloud_band, cm.clear_values)

                # Align to data grid if shapes differ
                if clear.shape != (data.sizes.get("y", 0), data.sizes.get("x", 0)):
                    clear_da = xr.DataArray(
                        clear,
                        dims=["y", "x"],
                    ).interp_like(
                        data.isel(band=0) if "band" in data.dims else data, method="nearest"
                    )
                    return data.where(clear_da > 0.5)

                clear_da = xr.DataArray(
                    clear,
                    dims=["y", "x"],
                    coords={d: data.coords[d] for d in ["y", "x"] if d in data.coords},
                )
                return data.where(clear_da)
            except Exception as e:
                logger.warning("Failed to apply cloud mask from %s: %s", mask_group, e)
                return data

        # Legacy fallback: mask_source_file (requires rasterio)
        mask_file = attrs.get("mask_source_file")
        if mask_file:
            logger.warning(
                "Legacy mask file reference '%s'; re-ingest with mask for GDAL-free cloud masking",
                mask_file,
            )

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
            return ds  # type: ignore[no-any-return]

        data = ds["0"]

        # Assign band name coordinates
        if band_names and len(band_names) == data.sizes.get("band", 0):  # type: ignore[arg-type]
            data = data.assign_coords(band=band_names)

        # Assign real geo-coordinates from bounds metadata
        scene_bounds = attrs.get("bounds")
        if scene_bounds and "y" in data.dims and "x" in data.dims:
            minx, miny, maxx, maxy = scene_bounds  # type: ignore[misc]
            ny, nx = data.sizes["y"], data.sizes["x"]
            x_res = (maxx - minx) / nx  # type: ignore[operator]
            y_res = (maxy - miny) / ny  # type: ignore[operator]
            x_coords = np.linspace(minx + x_res / 2, maxx - x_res / 2, nx)
            y_coords = np.linspace(maxy - y_res / 2, miny + y_res / 2, ny)  # top→bottom
            data = data.assign_coords(y=("y", y_coords), x=("x", x_coords))

        # Filter bands if requested
        if bands and band_names:
            available = set(band_names)  # type: ignore[arg-type]
            selected = [b for b in bands if b in available]
            if selected:
                data = data.sel(band=selected)

        # Apply cloud mask if requested
        if cloud_mask and (attrs.get("mask_group") or attrs.get("mask_source_file")):
            data = self._apply_cloud_mask(
                data,
                attrs,
                snapshot_id=snapshot_id,
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

        return result  # type: ignore[no-any-return]

    def open_xarray(
        self,
        time_range: tuple[datetime, datetime] | None = None,
        bounds: tuple[float, float, float, float] | None = None,
        bands: list[str] | None = None,
        product_id: str | None = None,
        snapshot_id: str | None = None,
        cloud_mask: bool = False,
        bounds_crs: str | None = None,
        latest_only: bool = False,
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
            bounds_crs: CRS of the *bounds* parameter. When provided,
                        bounds are transformed to the dataset CRS for
                        both scene filtering and pixel crop.
            latest_only: If True, open only the most recent scene.
                         Skips concat overhead — O(1) instead of O(N).

        Returns:
            xr.Dataset with dims (time, band, y, x) if multiple scenes,
            or (band, y, x) if single scene / latest_only

        Raises:
            ValueError: If no scenes match the query filters
        """
        scenes = self.list_scenes(
            time_range=time_range,
            bounds=bounds,
            product_id=product_id,
            snapshot_id=snapshot_id,
            bounds_crs=bounds_crs,
        )

        if not scenes:
            raise ValueError("No scenes match the query filters")

        # Sort by acquisition time
        scenes.sort(key=lambda s: s.get("acquisition_time", ""))

        # Optimization: only open the latest scene (skip O(N) concat)
        if latest_only:
            scenes = [scenes[-1]]

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
                    times.append(None)  # type: ignore[arg-type]

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

        # Add time dimension (geo-coordinates already assigned by open_scene)
        expanded = []
        for ds, t in zip(datasets, times, strict=False):
            if t is not None:
                ds = ds.expand_dims(time=[t])
            expanded.append(ds)

        # join="outer" pads shorter arrays with NaN (satellite scenes may vary in extent)
        combined = xr.concat(expanded, dim="time", join="outer")
        return combined

    @staticmethod
    def crop(ds: xr.Dataset, bounds: tuple[float, float, float, float]) -> xr.Dataset:
        """
        Crop dataset to a bounding box using geo-coordinates.

        Args:
            ds: Dataset with geo-coordinate x/y dims
            bounds: (minx, miny, maxx, maxy)

        Returns:
            Cropped dataset
        """
        minx, miny, maxx, maxy = bounds
        return ds.sel(x=slice(minx, maxx), y=slice(maxy, miny))

    @staticmethod
    def clip(ds: xr.Dataset, geometry) -> xr.Dataset:
        """
        Clip dataset to a GeoJSON polygon. Pixels outside → NaN.

        Args:
            ds: Dataset with geo-coordinate x/y dims
            geometry: GeoJSON dict or shapely geometry

        Returns:
            Clipped dataset (outside polygon = NaN)
        """
        from shapely import contains_xy, prepare
        from shapely.geometry import shape

        geom = shape(geometry) if isinstance(geometry, dict) else geometry

        # First bbox crop for efficiency
        gminx, gminy, gmaxx, gmaxy = geom.bounds
        cropped = ds.sel(x=slice(gminx, gmaxx), y=slice(gmaxy, gminy))

        # Build pixel-center coordinate grids
        x_vals = cropped.coords["x"].values
        y_vals = cropped.coords["y"].values

        if len(x_vals) == 0 or len(y_vals) == 0:
            return cropped

        xx, yy = np.meshgrid(x_vals, y_vals)

        # Vectorized containment test (shapely 2.0 C-level operation)
        prepare(geom)
        inside = contains_xy(geom, xx.ravel(), yy.ravel()).reshape(xx.shape)

        mask_da = xr.DataArray(~inside, dims=["y", "x"], coords={"y": y_vals, "x": x_vals})
        return cropped.where(~mask_da)

    @staticmethod
    def to_cog(
        ds: xr.Dataset,
        output_path: str,
        *,
        geometry=None,
        crs: str = "EPSG:4326",
        nodata: float = -999.0,
        overview_levels: list[int] | None = None,
        compress: str = "DEFLATE",
        data_var: str = "data",
    ) -> str:
        """
        Export dataset (or polygon clip) to Cloud-Optimized GeoTIFF.

        Args:
            ds: xr.Dataset with dims (band, y, x) or (y, x)
            output_path: Output file path (.tif)
            geometry: Optional GeoJSON dict or shapely geometry for polygon clip.
                      If provided, pixels outside polygon become nodata.
            crs: Coordinate reference system string
            nodata: Nodata value for output
            overview_levels: Overview pyramid levels (default: [2, 4, 8, 16])
            compress: Compression method (DEFLATE, LZW, ZSTD)
            data_var: Name of the data variable in the dataset

        Returns:
            Output file path
        """
        import rasterio
        from rasterio.transform import from_bounds

        # Apply polygon clip if geometry provided
        if geometry is not None:
            ds = IcechunkVirtualReader.clip(ds, geometry)

        if data_var not in ds:
            raise ValueError(f"Variable '{data_var}' not found in dataset")

        da = ds[data_var]
        data = da.values.copy().astype(np.float32)

        # Replace NaN with nodata
        data[np.isnan(data)] = nodata

        # Determine shape
        if data.ndim == 3:
            nbands, ny, nx = data.shape
        elif data.ndim == 2:
            ny, nx = data.shape
            nbands = 1
            data = data[np.newaxis, :, :]
        else:
            raise ValueError(f"Unexpected data dimensions: {data.ndim}")

        # Build affine transform from coordinates
        x_coords = da.coords["x"].values
        y_coords = da.coords["y"].values
        x_res = abs(x_coords[1] - x_coords[0]) if len(x_coords) > 1 else 1e-4
        y_res = abs(y_coords[0] - y_coords[1]) if len(y_coords) > 1 else 1e-4
        minx = float(x_coords.min()) - x_res / 2
        maxx = float(x_coords.max()) + x_res / 2
        miny = float(y_coords.min()) - y_res / 2
        maxy = float(y_coords.max()) + y_res / 2
        transform = from_bounds(minx, miny, maxx, maxy, nx, ny)

        if overview_levels is None:
            overview_levels = [2, 4, 8, 16]

        # Write COG
        profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "width": nx,
            "height": ny,
            "count": nbands,
            "crs": crs,
            "transform": transform,
            "nodata": nodata,
            "tiled": True,
            "blockxsize": 256,
            "blockysize": 256,
            "compress": compress,
        }

        with rasterio.open(output_path, "w", **profile) as dst:
            for i in range(nbands):
                dst.write(data[i], i + 1)

            # Build overviews for COG
            if overview_levels:
                dst.build_overviews(overview_levels, rasterio.enums.Resampling.nearest)
                dst.update_tags(ns="rio_overview", resampling="nearest")

        logger.info("Exported COG: %s (%d bands, %dx%d)", output_path, nbands, nx, ny)
        return output_path

    def point_timeseries(
        self,
        lon: float,
        lat: float,
        time_range=None,
        bands: list[str] | None = None,
        product_id: str | None = None,
        snapshot_id: str | None = None,
        bounds_crs: str | None = None,
        cloud_mask: bool = False,
    ) -> dict:
        """Extract pixel values at (lon, lat) across all time steps using direct zarr reads.

        Instead of opening full raster datasets for each scene, this method:
        1. Computes pixel coordinates from scene bounds metadata
        2. Reads only the single pixel (band, 1, 1) from each scene's zarr array

        Returns:
            dict with keys:
            - "timestamps": list of ISO date strings
            - "values": dict mapping band_name -> list of float values
        """
        import zarr

        scenes = self.list_scenes(
            time_range=time_range,
            product_id=product_id,
            snapshot_id=snapshot_id,
            bounds_crs=bounds_crs,
        )

        if not scenes:
            raise ValueError("No scenes match the query filters")

        scenes.sort(key=lambda s: s.get("acquisition_time", ""))

        session = self.storage.readonly_session(snapshot_id=snapshot_id)
        store = session.store
        root = zarr.open_group(store, mode="r")

        timestamps = []
        # band_values: {band_name: [val_t0, val_t1, ...]}
        band_values: dict[str, list[float]] = {}

        for scene_meta in scenes:
            group_name = scene_meta["group"]
            scene_bounds = scene_meta.get("bounds")
            scene_band_names = scene_meta.get("band_names", [])
            acq_time = scene_meta.get("acquisition_time", "")

            if not scene_bounds:
                continue

            try:
                import zarr as _zarr

                grp = root[group_name]
                if not isinstance(grp, _zarr.Group) or "0" not in grp:
                    continue
                zarr_arr = grp["0"]
                if not isinstance(zarr_arr, _zarr.Array):
                    continue

                _n_bands, ny, nx = zarr_arr.shape
                minx, miny, maxx, maxy = scene_bounds

                # Compute pixel indices from bounds (same logic as open_scene coord assignment)
                x_res = (maxx - minx) / nx
                y_res = (maxy - miny) / ny

                # x coords go left to right: minx + x_res/2 ... maxx - x_res/2
                x_idx = round((lon - (minx + x_res / 2)) / x_res)
                x_idx = max(0, min(nx - 1, x_idx))

                # y coords go top to bottom: maxy - y_res/2 ... miny + y_res/2
                y_idx = round(((maxy - y_res / 2) - lat) / y_res)
                y_idx = max(0, min(ny - 1, y_idx))

                # Direct zarr read — only 1 pixel across all bands
                pixel: np.ndarray = np.asarray(zarr_arr[:, y_idx, x_idx])

                # Cloud mask filtering
                if cloud_mask:
                    scene_product_id = scene_meta.get("product_id")
                    mask_group_name = scene_meta.get("mask_group")
                    if scene_product_id and mask_group_name and mask_group_name in root:
                        from pixelquery.catalog.product_profile import BUILTIN_PROFILES
                        profile = BUILTIN_PROFILES.get(scene_product_id)
                        if profile is not None and profile.cloud_mask is not None:
                            cm = profile.cloud_mask
                            mask_grp = root[mask_group_name]
                            if isinstance(mask_grp, _zarr.Group) and "0" in mask_grp:
                                mask_arr = mask_grp["0"]
                                if isinstance(mask_arr, _zarr.Array):
                                    mask_pixel = int(np.asarray(mask_arr[cm.band_index, y_idx, x_idx]))
                                    if mask_pixel not in cm.clear_values:
                                        continue  # skip cloudy scene

                timestamps.append(acq_time[:10] if acq_time else "")

                # Map band values
                for b_idx, band_name in enumerate(scene_band_names):
                    if bands and band_name not in bands:
                        continue
                    if band_name not in band_values:
                        band_values[band_name] = []
                    band_values[band_name].append(float(pixel[b_idx]))

            except Exception as e:
                logger.warning("Failed to read pixel from scene %s: %s", group_name, e)

        if not timestamps:
            raise ValueError("No pixel data could be extracted")

        return {"timestamps": timestamps, "values": band_values}

    def get_snapshot_history(self) -> list[dict[str, Any]]:
        """Get Icechunk snapshot history for Time Travel."""
        return self.storage.get_snapshot_history()  # type: ignore[no-any-return]
