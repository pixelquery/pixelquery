"""
PixelQuery S3 Client

High-level convenience API for S3-based COG ingestion and spatial query.
Wraps Icechunk, obstore, VirtualiZarr boilerplate into a clean interface.

Usage:
    >>> from pixelquery.io.s3_client import PixelQueryS3
    >>>
    >>> pq = PixelQueryS3(
    ...     bucket="my-bucket",
    ...     endpoint_url="http://localhost:9000",  # MinIO
    ...     access_key_id="minioadmin",
    ...     secret_access_key="minioadmin",
    ... )
    >>>
    >>> # Ingest COGs
    >>> pq.ingest_cogs("arps/", band_names=["band1", "band2"])
    >>>
    >>> # Query
    >>> scenes = pq.list_scenes(time_range=("2025-01-01", "2025-06-01"))
    >>> ds = pq.open_scene(scenes[0])
    >>>
    >>> # Clip by polygon + export
    >>> pq.clip_to_cog(ds, polygon, "output.tif")
"""

import logging
import re
import tempfile
from datetime import UTC, datetime
from typing import Any

import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)


class PixelQueryS3:
    """
    S3 기반 위성영상 시공간 쿼리 클라이언트.

    COG → Icechunk 인제스트 → xarray lazy 쿼리 → crop/clip → 통계/내보내기
    """

    def __init__(
        self,
        bucket: str,
        *,
        endpoint_url: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        region: str = "us-east-1",
        repo_path: str | None = None,
        allow_http: bool = True,
        force_path_style: bool = True,
    ):
        """
        Args:
            bucket: S3 bucket name
            endpoint_url: S3 endpoint (e.g. "http://localhost:9000" for MinIO)
            access_key_id: AWS access key
            secret_access_key: AWS secret key
            region: AWS region
            repo_path: Local path for Icechunk repo. Auto-generated if None.
            allow_http: Allow HTTP (non-HTTPS) connections
            force_path_style: Use path-style S3 URLs (required for MinIO)
        """
        import os

        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.region = region

        # Set env vars for obstore S3Store
        if access_key_id:
            os.environ.setdefault("AWS_ACCESS_KEY_ID", access_key_id)
        if secret_access_key:
            os.environ.setdefault("AWS_SECRET_ACCESS_KEY", secret_access_key)
        if region:
            os.environ.setdefault("AWS_REGION", region)
        if endpoint_url:
            os.environ.setdefault("AWS_ENDPOINT_URL", endpoint_url)
        if allow_http:
            os.environ.setdefault("AWS_ALLOW_HTTP", "true")

        # Apply codecs patch
        from pixelquery._internal.codecs import patch_imagecodecs

        patch_imagecodecs()

        # Setup Icechunk
        import icechunk

        self._repo_path = repo_path or tempfile.mkdtemp(prefix="pixelquery_")
        self._prefix = f"s3://{bucket}/"

        config = icechunk.RepositoryConfig.default()
        vcc_store = icechunk.s3_store(
            endpoint_url=endpoint_url,
            allow_http=allow_http,
            force_path_style=force_path_style,
        )
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(self._prefix, vcc_store)
        )

        cred_kwargs: dict[str, Any] = {}
        if access_key_id and secret_access_key:
            cred_kwargs = {
                "access_key_id": access_key_id,
                "secret_access_key": secret_access_key,
            }
        raw_cred = icechunk.s3_credentials(**cred_kwargs) if cred_kwargs else None
        vcc_auth = icechunk.containers_credentials({self._prefix: raw_cred})

        storage = icechunk.local_filesystem_storage(self._repo_path)

        try:
            self._repo = icechunk.Repository.open(
                storage=storage,
                config=config,
                authorize_virtual_chunk_access=vcc_auth,
            )
            logger.info("Opened existing repo at %s", self._repo_path)
        except Exception:
            self._repo = icechunk.Repository.create(
                storage=storage,
                config=config,
                authorize_virtual_chunk_access=vcc_auth,
            )
            logger.info("Created new repo at %s", self._repo_path)

        # Setup obstore registry
        import obstore
        from obspec_utils.registry import ObjectStoreRegistry

        self._s3_store = obstore.store.S3Store(bucket=bucket)
        self._registry = ObjectStoreRegistry()
        self._registry.register(self._prefix, self._s3_store)

        # Reader
        from pixelquery.io.icechunk_reader import IcechunkVirtualReader

        self._reader = IcechunkVirtualReader(self)

    # ── StorageManager interface (for IcechunkVirtualReader) ──

    def readonly_session(self, snapshot_id: str | None = None, branch: str = "main"):
        if snapshot_id:
            return self._repo.readonly_session(snapshot_id=snapshot_id)
        return self._repo.readonly_session(branch=branch)

    # ── Ingestion ──

    def list_cogs(self, prefix: str = "") -> list[str]:
        """List COG files in S3 bucket.

        Args:
            prefix: S3 key prefix (e.g. "arps/")

        Returns:
            List of S3 URLs (s3://bucket/key)
        """
        import obstore

        paths = []
        for chunk in obstore.list(self._s3_store, prefix=prefix):
            for entry in chunk if isinstance(chunk, list) else [chunk]:
                p = entry["path"] if isinstance(entry, dict) else entry.path
                if p.endswith((".tif", ".tiff")):
                    paths.append(f"{self._prefix}{p}")
        return sorted(paths)

    def ingest_cogs(
        self,
        prefix: str = "",
        *,
        cog_urls: list[str] | None = None,
        band_names: list[str] | None = None,
        product_id: str = "default",
        bounds: list[float] | None = None,
        crs: str = "EPSG:4326",
        filename_pattern: str = r"(\d{4}-\d{2}-\d{2})",
    ) -> list[str]:
        """Ingest COGs from S3 into Icechunk.

        Args:
            prefix: S3 prefix to scan for COGs (e.g. "arps/")
            cog_urls: Explicit list of S3 URLs. Overrides prefix scan.
            band_names: Band name list. Auto-detected if None.
            product_id: Product identifier
            bounds: [minx, miny, maxx, maxy]. None = unknown.
            crs: Coordinate reference system
            filename_pattern: Regex to extract date from filename

        Returns:
            List of ingested group names
        """
        import zarr
        from virtual_tiff import VirtualTIFF
        from virtualizarr import open_virtual_dataset

        urls = cog_urls or self.list_cogs(prefix)
        if not urls:
            raise ValueError(f"No COGs found at prefix '{prefix}'")

        session = self._repo.writable_session("main")
        store = session.store

        # Dedup: collect already-ingested source URLs
        root = zarr.open_group(store, mode="a")
        if "_scenes_index" not in root:
            idx = root.create_group("_scenes_index")
        else:
            idx = root["_scenes_index"]

        existing = list(idx.attrs.get("scenes", []))
        ingested_sources: set[str] = set()
        for s in existing:
            src = s.get("source_file", "")
            if src:
                ingested_sources.add(src)

        # Global offset for group naming (avoid index collision across batches)
        offset = len(existing)

        group_names = []
        skipped = 0

        for i, url in enumerate(urls):
            # Skip already-ingested COGs (dedup by source_file URL)
            if url in ingested_sources:
                logger.info("Skip duplicate: %s", url.split("/")[-1])
                skipped += 1
                continue

            fname = url.split("/")[-1]
            date_match = re.search(filename_pattern, fname)
            if date_match:
                date_str = date_match.group(1)
                acq_time = datetime.fromisoformat(date_str).replace(tzinfo=UTC)
            else:
                acq_time = datetime.now(UTC)
                date_str = acq_time.strftime("%Y%m%d")

            vds = open_virtual_dataset(
                url, registry=self._registry, parser=VirtualTIFF(ifd=0)
            )
            gn = f"scene_{date_str.replace('-', '')}_{offset + len(group_names):04d}"
            vds.virtualize.to_icechunk(store, group=gn)

            root = zarr.open_group(store, mode="a")
            shape = list(vds["0"].shape) if "0" in vds else []

            # Auto-detect band names from shape
            if band_names is None and shape:
                n = shape[0] if len(shape) == 3 else 1
                auto_bands = [f"band{j + 1}" for j in range(n)]
            else:
                auto_bands = band_names or []

            root[gn].attrs.update(
                {
                    "acquisition_time": acq_time.isoformat(),
                    "product_id": product_id,
                    "band_names": auto_bands,
                    "source_file": url,
                    "crs": crs,
                    "bounds": bounds or [],
                    "shape": shape,
                }
            )
            group_names.append(gn)

        if not group_names:
            if skipped:
                logger.info("All %d COGs already ingested, nothing to do", skipped)
            return []

        # Update scenes index
        for gn in group_names:
            a = dict(root[gn].attrs)
            existing.append(
                {
                    "group": gn,
                    "acquisition_time": a.get("acquisition_time"),
                    "product_id": a.get("product_id"),
                    "bounds": a.get("bounds"),
                    "crs": a.get("crs"),
                    "band_names": a.get("band_names"),
                    "source_file": a.get("source_file"),
                }
            )
        idx.attrs["scenes"] = existing
        session.commit(f"Ingest {len(group_names)} COGs")

        logger.info(
            "Ingested %d COGs (%d skipped as duplicates)", len(group_names), skipped
        )
        return group_names

    # ── Query ──

    def list_scenes(
        self,
        *,
        time_range: tuple | None = None,
        bounds: tuple[float, float, float, float] | None = None,
        product_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search scenes by time range and/or spatial bounds.

        Args:
            time_range: (start, end) — str or datetime. e.g. ("2025-01-01", "2025-06-01")
            bounds: (minx, miny, maxx, maxy)
            product_id: Filter by product

        Returns:
            List of scene metadata dicts
        """
        parsed_range = None
        if time_range:
            start, end = time_range
            if isinstance(start, str):
                start = datetime.fromisoformat(start).replace(tzinfo=UTC)
            if isinstance(end, str):
                end = datetime.fromisoformat(end).replace(tzinfo=UTC)
            parsed_range = (start, end)

        return self._reader.list_scenes(
            time_range=parsed_range,
            bounds=bounds,
            product_id=product_id,
        )

    def open_scene(
        self,
        scene: str | dict,
        *,
        bands: list[str] | None = None,
    ) -> xr.Dataset:
        """Open a scene as xarray Dataset.

        Args:
            scene: Group name (str) or scene dict from list_scenes()
            bands: Optional band filter

        Returns:
            xr.Dataset with dims (band, y, x) and geo-coordinates
        """
        group = scene["group"] if isinstance(scene, dict) else scene
        return self._reader.open_scene(group, bands=bands)

    # ── Spatial operations ──

    @staticmethod
    def crop(
        ds: xr.Dataset,
        bounds: tuple[float, float, float, float],
    ) -> xr.Dataset:
        """Crop to bounding box. (minx, miny, maxx, maxy)"""
        from pixelquery.io.icechunk_reader import IcechunkVirtualReader

        return IcechunkVirtualReader.crop(ds, bounds)

    @staticmethod
    def clip(ds: xr.Dataset, geometry) -> xr.Dataset:
        """Clip to GeoJSON polygon. Pixels outside → NaN."""
        from pixelquery.io.icechunk_reader import IcechunkVirtualReader

        return IcechunkVirtualReader.clip(ds, geometry)

    # ── Export ──

    @staticmethod
    def to_cog(
        ds: xr.Dataset,
        output_path: str,
        *,
        geometry=None,
        crs: str = "EPSG:4326",
        nodata: float = -999.0,
        compress: str = "DEFLATE",
    ) -> str:
        """Export to Cloud-Optimized GeoTIFF.

        Args:
            ds: Dataset to export
            output_path: Output .tif path
            geometry: Optional GeoJSON polygon for clipping
            crs: Output CRS
            nodata: Nodata value
            compress: Compression (DEFLATE, LZW, ZSTD)

        Returns:
            Output file path
        """
        from pixelquery.io.icechunk_reader import IcechunkVirtualReader

        return IcechunkVirtualReader.to_cog(
            ds,
            output_path,
            geometry=geometry,
            crs=crs,
            nodata=nodata,
            compress=compress,
        )

    # ── Statistics ──

    @staticmethod
    def stats(
        ds: xr.Dataset,
        *,
        nodata: float = -999.0,
        data_var: str = "data",
    ) -> dict[str, float]:
        """Compute statistics on dataset.

        Returns:
            Dict with mean, std, min, max, median, p25, p75
        """
        data = ds[data_var].values.astype(float)
        data[data == nodata] = np.nan
        return {
            "mean": float(np.nanmean(data)),
            "std": float(np.nanstd(data)),
            "min": float(np.nanmin(data)),
            "max": float(np.nanmax(data)),
            "median": float(np.nanmedian(data)),
            "p25": float(np.nanpercentile(data, 25)),
            "p75": float(np.nanpercentile(data, 75)),
        }

    # ── Convenience ──

    def clip_to_cog(
        self,
        scene: str | dict | xr.Dataset,
        geometry,
        output_path: str,
        *,
        nodata: float = -999.0,
        compress: str = "DEFLATE",
    ) -> str:
        """Open scene → clip by polygon → export COG. One call.

        Args:
            scene: Group name, scene dict, or already-opened Dataset
            geometry: GeoJSON polygon dict
            output_path: Output .tif path

        Returns:
            Output file path
        """
        if isinstance(scene, xr.Dataset):
            ds = scene
        else:
            ds = self.open_scene(scene)

        # Optimize: crop to bbox first, then clip
        from shapely.geometry import shape

        geom = shape(geometry) if isinstance(geometry, dict) else geometry
        bbox = geom.bounds
        ds = self.crop(ds, bbox)

        return self.to_cog(
            ds,
            output_path,
            geometry=geometry,
            nodata=nodata,
            compress=compress,
        )

    def timeseries(
        self,
        geometry,
        *,
        time_range: tuple | None = None,
        nodata: float = -999.0,
    ) -> list[dict[str, Any]]:
        """Compute per-scene statistics for a polygon over time.

        Args:
            geometry: GeoJSON polygon
            time_range: Optional time filter (str or datetime)
            nodata: Nodata value

        Returns:
            List of dicts: [{date, band1_mean, band2_mean, ...}, ...]
        """
        from shapely.geometry import shape

        scenes = self.list_scenes(time_range=time_range)
        scenes.sort(key=lambda s: s.get("acquisition_time", ""))

        geom = shape(geometry) if isinstance(geometry, dict) else geometry
        bbox = geom.bounds

        results = []
        for s in scenes:
            ds = self.open_scene(s)
            cropped = self.crop(ds, bbox)
            clipped = self.clip(cropped, geometry)
            vals = clipped["data"].values.astype(float)
            vals[vals == nodata] = np.nan

            row: dict[str, Any] = {"date": s["acquisition_time"][:10]}

            if vals.ndim == 3:
                band_names = s.get("band_names") or [
                    f"band{i + 1}" for i in range(vals.shape[0])
                ]
                for bi, bname in enumerate(band_names):
                    if bi < vals.shape[0]:
                        row[f"{bname}_mean"] = float(np.nanmean(vals[bi]))
                row["mean"] = float(np.nanmean(vals))
            else:
                row["mean"] = float(np.nanmean(vals))

            results.append(row)

        return results

    # ── Rendering ──

    _COLORMAPS: dict[str, np.ndarray] = {}

    @staticmethod
    def _build_lut(stops: list[tuple[float, tuple[int, ...]]]) -> np.ndarray:
        lut = np.zeros((256, 4), dtype=np.uint8)
        for i in range(256):
            t = i / 255.0
            for j in range(len(stops) - 1):
                if stops[j][0] <= t <= stops[j + 1][0]:
                    frac = (t - stops[j][0]) / (stops[j + 1][0] - stops[j][0] + 1e-10)
                    c0 = np.array(stops[j][1])
                    c1 = np.array(stops[j + 1][1])
                    lut[i] = (c0 + frac * (c1 - c0)).astype(np.uint8)
                    break
        return lut

    @classmethod
    def _get_lut(cls, name: str) -> np.ndarray:
        if name not in cls._COLORMAPS:
            luts = {
                "rdylgn": [
                    (0.0, (165, 0, 38, 255)),
                    (0.1, (215, 48, 39, 255)),
                    (0.25, (244, 109, 67, 255)),
                    (0.4, (253, 174, 97, 255)),
                    (0.5, (255, 255, 191, 255)),
                    (0.6, (166, 217, 106, 255)),
                    (0.75, (102, 189, 99, 255)),
                    (0.9, (26, 152, 80, 255)),
                    (1.0, (0, 104, 55, 255)),
                ],
                "viridis": [
                    (0.0, (68, 1, 84, 255)),
                    (0.25, (59, 82, 139, 255)),
                    (0.5, (33, 145, 140, 255)),
                    (0.75, (94, 201, 98, 255)),
                    (1.0, (253, 231, 37, 255)),
                ],
                "inferno": [
                    (0.0, (0, 0, 4, 255)),
                    (0.25, (87, 16, 110, 255)),
                    (0.5, (188, 55, 84, 255)),
                    (0.75, (249, 142, 9, 255)),
                    (1.0, (252, 255, 164, 255)),
                ],
            }
            if name not in luts:
                raise ValueError(f"Unknown colormap: {name}. Choose from: {list(luts)}")
            cls._COLORMAPS[name] = cls._build_lut(luts[name])
        return cls._COLORMAPS[name]

    @staticmethod
    def ndvi(
        ds: xr.Dataset,
        *,
        band_red: int = 2,
        band_nir: int = 3,
        nodata: float = -999.0,
        data_var: str = "data",
    ) -> np.ndarray:
        """Compute NDVI from dataset.

        Args:
            ds: Dataset with band dimension
            band_red: Red band index (0-based)
            band_nir: NIR band index (0-based)

        Returns:
            2D float32 array with NDVI values (-1 to 1), NaN for invalid
        """
        vals = ds[data_var].values.astype(np.float32)
        vals[vals == nodata] = np.nan
        red = vals[band_red]
        nir = vals[band_nir]
        denom = nir + red
        return np.where(denom > 0, (nir - red) / denom, np.nan)

    @classmethod
    def render_png(
        cls,
        data_2d: np.ndarray,
        *,
        colormap: str = "rdylgn",
        vmin: float | None = None,
        vmax: float | None = None,
    ) -> bytes:
        """Render 2D array to PNG with colormap. NaN → transparent.

        Args:
            data_2d: 2D float array (e.g. NDVI, single band)
            colormap: "rdylgn" (vegetation), "viridis", "inferno"
            vmin/vmax: Value range. Auto-detected (p2/p98) if None.

        Returns:
            PNG image as bytes
        """
        from PIL import Image
        import io

        valid = data_2d[~np.isnan(data_2d)]
        if len(valid) == 0:
            return b""

        if vmin is None:
            vmin = float(np.percentile(valid, 2))
        if vmax is None:
            vmax = float(np.percentile(valid, 98))

        norm = np.clip((data_2d - vmin) / (vmax - vmin + 1e-10), 0, 1)
        indices = (norm * 255).astype(np.uint8)

        lut = cls._get_lut(colormap)
        rgba = lut[indices]
        rgba[np.isnan(data_2d)] = [0, 0, 0, 0]

        img = Image.fromarray(rgba, mode="RGBA")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return buf.getvalue()

    def clip_to_png(
        self,
        scene: str | dict | xr.Dataset,
        geometry,
        *,
        expression: str = "ndvi",
        band: int | None = None,
        colormap: str = "rdylgn",
        vmin: float | None = None,
        vmax: float | None = None,
        nodata: float = -999.0,
    ) -> bytes:
        """Open scene → clip → compute index → colormap → PNG. One call.

        Args:
            scene: Group name, scene dict, or Dataset
            geometry: GeoJSON polygon
            expression: "ndvi", "evi", or "band" (single band rendering)
            band: Band index for single-band rendering (0-based)
            colormap: Colormap name
            vmin/vmax: Value range

        Returns:
            PNG bytes (transparent outside polygon)
        """
        from shapely.geometry import shape

        if isinstance(scene, xr.Dataset):
            ds = scene
        else:
            ds = self.open_scene(scene)

        geom = shape(geometry) if isinstance(geometry, dict) else geometry
        bbox = geom.bounds
        ds = self.crop(ds, bbox)
        ds = self.clip(ds, geometry)

        vals = ds["data"].values.astype(np.float32)
        vals[vals == nodata] = np.nan

        if expression == "ndvi":
            data_2d = self.ndvi(ds, nodata=nodata)
            if vmin is None:
                vmin = -0.2
            if vmax is None:
                vmax = 0.8
        elif expression == "band":
            idx = band if band is not None else 0
            data_2d = vals[idx]
            if colormap == "rdylgn":
                colormap = "viridis"
        else:
            raise ValueError(f"Unknown expression: {expression}")

        return self.render_png(data_2d, colormap=colormap, vmin=vmin, vmax=vmax)

    def multi_field_stats(
        self,
        feature_collection: dict,
        scene: str | dict | xr.Dataset | None = None,
        *,
        nodata: float = -999.0,
    ) -> list[dict[str, Any]]:
        """Compute stats for each feature in a GeoJSON FeatureCollection.

        Args:
            feature_collection: GeoJSON FeatureCollection dict
            scene: Scene to analyze. If None, uses first available scene.

        Returns:
            List of dicts per feature with id, name, pixels, mean, std, min, max
        """
        from shapely.geometry import shape

        if scene is None:
            scenes = self.list_scenes()
            if not scenes:
                raise ValueError("No scenes available")
            ds = self.open_scene(scenes[0])
        elif isinstance(scene, xr.Dataset):
            ds = scene
        else:
            ds = self.open_scene(scene)

        results = []
        for feat in feature_collection["features"]:
            props = feat.get("properties", {})
            geom = feat["geometry"]
            bbox = shape(geom).bounds

            cropped = self.crop(ds, bbox)
            clipped = self.clip(cropped, geom)
            vals = clipped["data"].values.astype(float)
            vals[vals == nodata] = np.nan
            valid = ~np.isnan(vals)

            results.append(
                {
                    "id": props.get("id", ""),
                    "name": props.get("name", ""),
                    "pixels": int(valid.sum()),
                    "mean": float(np.nanmean(vals)),
                    "std": float(np.nanstd(vals)),
                    "min": float(np.nanmin(vals)) if valid.any() else float("nan"),
                    "max": float(np.nanmax(vals)) if valid.any() else float("nan"),
                }
            )
        return results
