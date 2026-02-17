"""
Icechunk Catalog

Catalog implementation backed by Icechunk repository.
Provides scene listing, band discovery, and snapshot history.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pixelquery.catalog.product_profile import ProductProfile

logger = logging.getLogger(__name__)


class IcechunkCatalog:
    """
    Catalog backed by an Icechunk repository.

    Uses the _scenes_index for fast metadata lookups without
    opening individual zarr groups.
    """

    def __init__(
        self,
        warehouse_path: str,
        vcc_prefix: str | None = None,
        vcc_data_path: str | None = None,
    ):
        self.warehouse_path = warehouse_path
        self._storage = None
        self._reader = None
        self._vcc_prefix = vcc_prefix
        self._vcc_data_path = vcc_data_path

    def _ensure_initialized(self):
        if self._storage is None:
            from pixelquery._internal.storage.icechunk_storage import IcechunkStorageManager
            from pixelquery.io.icechunk_reader import IcechunkVirtualReader

            self._storage = IcechunkStorageManager(
                self.warehouse_path,
                vcc_prefix=self._vcc_prefix,
                vcc_data_path=self._vcc_data_path,
            )
            self._storage.initialize()
            self._reader = IcechunkVirtualReader(self._storage)

    def list_tiles(
        self,
        bounds: tuple[float, float, float, float] | None = None,
        time_range: tuple[datetime, datetime] | None = None,
        **kwargs,
    ) -> list[str]:
        """List scene group names (analogous to tile IDs)."""
        self._ensure_initialized()
        scenes = self._reader.list_scenes(
            time_range=time_range,
            bounds=bounds,
        )
        return [s["group"] for s in scenes]

    def list_bands(self, tile_id: str | None = None) -> list[str]:
        """List available band names."""
        self._ensure_initialized()
        scenes = self._reader.list_scenes()
        all_bands = set()
        for s in scenes:
            if tile_id and s["group"] != tile_id:
                continue
            bands = s.get("band_names", [])
            if bands:
                all_bands.update(bands)
        return sorted(all_bands)

    def get_snapshot_history(self) -> list[dict[str, Any]]:
        """Get Icechunk snapshot history for Time Travel."""
        self._ensure_initialized()
        return self._storage.get_snapshot_history()

    def get_current_snapshot_id(self) -> str | None:
        """Get the current snapshot ID."""
        self._ensure_initialized()
        history = self._storage.get_snapshot_history()
        return history[0]["snapshot_id"] if history else None

    def exists(self) -> bool:
        """Check if the Icechunk repo exists."""
        from pathlib import Path

        return (Path(self.warehouse_path) / ".icechunk").exists()

    def products(self) -> list[str]:
        """List product_ids found in the warehouse."""
        self._ensure_initialized()
        scenes = self._reader.list_scenes()
        product_ids = set()
        for s in scenes:
            product_id = s.get("product_id")
            if product_id:
                product_ids.add(product_id)
        return sorted(product_ids)

    def product_profiles(self) -> dict[str, ProductProfile]:
        """
        Get product profiles stored in the repository.

        Returns:
            Dict mapping product_id to ProductProfile
        """
        from pixelquery.catalog.product_profile import ProductProfile

        self._ensure_initialized()

        try:
            import zarr

            session = self._storage.readonly_session()
            store = session.store
            root = zarr.open_group(store, mode="r")

            if "_products_registry" in root:
                profiles_data = dict(root["_products_registry"].attrs.get("profiles", {}))
                profiles = {}
                for pid, pdata in profiles_data.items():
                    profiles[pid] = ProductProfile.from_dict(pdata)
                return profiles
        except Exception as e:
            logger.debug("Could not load product profiles from repo: %s", e)

        return {}

    def register_product(self, profile: ProductProfile) -> None:
        """
        Persist a product profile to the repository.

        Args:
            profile: ProductProfile to register
        """
        import zarr

        self._ensure_initialized()

        # Read existing profiles
        existing_profiles = self.product_profiles()
        existing_profiles[profile.product_id] = profile

        # Serialize all profiles
        profiles_data = {pid: p.to_dict() for pid, p in existing_profiles.items()}

        # Write to _products_registry group attrs
        session = self._storage.writable_session()
        store = session.store
        root = zarr.open_group(store, mode="a")

        if "_products_registry" not in root:
            registry_group = root.create_group("_products_registry")
        else:
            registry_group = root["_products_registry"]

        registry_group.attrs["profiles"] = profiles_data

        # Commit the change
        self._storage.commit(session, f"Register product profile: {profile.product_id}")
        logger.info("Registered product profile to repo: %s", profile.product_id)

    def scenes(
        self,
        product_id: str | None = None,
        time_range: tuple[datetime, datetime] | None = None,
        bounds: tuple[float, float, float, float] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Enhanced scene listing with full metadata.

        Args:
            product_id: Filter by product_id
            time_range: Filter by time range (start, end)
            bounds: Filter by spatial bounds (minx, miny, maxx, maxy)

        Returns:
            List of scene metadata dicts
        """
        self._ensure_initialized()
        scenes = self._reader.list_scenes(time_range=time_range, bounds=bounds)

        if product_id:
            scenes = [s for s in scenes if s.get("product_id") == product_id]

        return scenes

    def summary(self) -> str:
        """
        Formatted summary of all products with scene counts, time ranges, band names.

        Returns:
            Multi-line summary string
        """
        self._ensure_initialized()

        # Get all scenes
        all_scenes = self._reader.list_scenes()

        # Group by product_id
        products_data = {}
        for scene in all_scenes:
            product_id = scene.get("product_id", "unknown")
            if product_id not in products_data:
                products_data[product_id] = {
                    "scenes": [],
                    "bands": set(),
                }
            products_data[product_id]["scenes"].append(scene)

            # Collect band names
            band_names = scene.get("band_names", [])
            if band_names:
                products_data[product_id]["bands"].update(band_names)

        # Get registered profiles
        profiles = self.product_profiles()

        # Build summary
        lines = ["=== PixelQuery Warehouse Summary ==="]
        lines.append(f"Products: {len(products_data)}")
        lines.append("")

        for product_id in sorted(products_data.keys()):
            data = products_data[product_id]
            profile = profiles.get(product_id)

            # Header
            provider = profile.provider if profile else "Unknown"
            lines.append(f"{product_id} ({provider})")

            # Scene count
            scene_count = len(data["scenes"])
            lines.append(f"  Scenes: {scene_count}")

            # Time range
            times = []
            for s in data["scenes"]:
                acq = s.get("acquisition_time")
                if acq:
                    try:
                        if isinstance(acq, str):
                            times.append(datetime.fromisoformat(acq))
                        elif isinstance(acq, datetime):
                            times.append(acq)
                    except Exception:
                        pass

            if times:
                min_time = min(times).strftime("%Y-%m-%d")
                max_time = max(times).strftime("%Y-%m-%d")
                lines.append(f"  Time range: {min_time} ~ {max_time}")

            # Bands
            bands_list = sorted(data["bands"])
            if bands_list:
                bands_str = ", ".join(bands_list)
                lines.append(f"  Bands: {bands_str}")

            # Resolution
            if profile:
                lines.append(f"  Resolution: {profile.resolution}m")

            lines.append("")

        return "\n".join(lines)

    def __repr__(self) -> str:
        exists_str = "exists" if self.exists() else "not initialized"
        product_count = 0
        if self.exists():
            try:
                self._ensure_initialized()
                product_count = len(self.products())
            except Exception:
                pass

        return (
            f"<IcechunkCatalog>\n"
            f"Warehouse: {self.warehouse_path}\n"
            f"Status: {exists_str}\n"
            f"Products: {product_count}"
        )
