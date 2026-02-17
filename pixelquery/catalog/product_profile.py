"""
Product Profile system for multi-satellite support.

Defines band mappings, resolution, and metadata for different satellite products.
Profiles are stored in the Icechunk repository alongside scene data.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class CloudMask:
    """
    Defines how to interpret a cloud mask file for a satellite product.

    Attributes:
        band_index: Band index in the mask file (0-based)
        clear_values: Pixel values considered "clear" (keep these, mask the rest)
        file_suffix: Suffix pattern for matching mask files to data files
                     e.g. "_udm2" matches 2024-01-15_scene_udm2.tif

    Examples:
        >>> # Planet UDM2: band 6 is cloud, 0 = clear
        >>> CloudMask(band_index=5, clear_values=[0], file_suffix="_udm2")
        >>> # Sentinel-2 SCL: single band, 4-7 = clear vegetation/soil/water
        >>> CloudMask(band_index=0, clear_values=[4, 5, 6, 7, 11], file_suffix="_SCL")
    """

    band_index: int
    clear_values: list[int] = field(default_factory=lambda: [0])
    file_suffix: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "band_index": self.band_index,
            "clear_values": self.clear_values,
            "file_suffix": self.file_suffix,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CloudMask":
        return cls(
            band_index=data["band_index"],
            clear_values=data.get("clear_values", [0]),
            file_suffix=data.get("file_suffix", ""),
        )


@dataclass
class ProductProfile:
    """
    Defines a satellite product's band mapping and metadata.

    Attributes:
        product_id: Unique identifier (e.g. "planet_sr", "sentinel2_l2a")
        bands: Band name to band index mapping (e.g. {"red": 2, "nir": 3})
        resolution: Spatial resolution in meters
        provider: Data provider (e.g. "Planet", "ESA", "USGS")
        description: Human-readable description
        extra_fields: Additional metadata field names tracked per scene

    Examples:
        >>> profile = ProductProfile(
        ...     product_id="planet_sr",
        ...     bands={"blue": 0, "green": 1, "red": 2, "nir": 3},
        ...     resolution=3.0,
        ...     provider="Planet",
        ... )
    """

    product_id: str
    bands: dict[str, int]
    resolution: float
    provider: str = ""
    description: str = ""
    cloud_mask: CloudMask | None = None
    extra_fields: list[str] = field(default_factory=list)

    @property
    def band_names(self) -> list[str]:
        """Band names sorted by index."""
        return [k for k, _ in sorted(self.bands.items(), key=lambda x: x[1])]

    @property
    def band_count(self) -> int:
        return len(self.bands)

    def to_dict(self) -> dict[str, Any]:
        d = {
            "product_id": self.product_id,
            "bands": self.bands,
            "resolution": self.resolution,
            "provider": self.provider,
            "description": self.description,
            "extra_fields": self.extra_fields,
        }
        if self.cloud_mask:
            d["cloud_mask"] = self.cloud_mask.to_dict()
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProductProfile":
        cloud_mask = None
        if data.get("cloud_mask"):
            cloud_mask = CloudMask.from_dict(data["cloud_mask"])
        return cls(
            product_id=data["product_id"],
            bands=data["bands"],
            resolution=data["resolution"],
            provider=data.get("provider", ""),
            description=data.get("description", ""),
            cloud_mask=cloud_mask,
            extra_fields=data.get("extra_fields", []),
        )

    def __repr__(self):
        bands_str = ", ".join(self.band_names)
        return (
            f"<ProductProfile: {self.product_id}>\n"
            f"  Provider: {self.provider}\n"
            f"  Bands: [{bands_str}]\n"
            f"  Resolution: {self.resolution}m"
        )


# Built-in profiles for common satellite products
BUILTIN_PROFILES = {
    "planet_sr": ProductProfile(
        product_id="planet_sr",
        bands={"blue": 0, "green": 1, "red": 2, "nir": 3},
        resolution=3.0,
        provider="Planet",
        description="PlanetScope Surface Reflectance (4-band)",
        cloud_mask=CloudMask(
            band_index=5,  # UDM2 band 6 = cloud
            clear_values=[0],  # 0 = no cloud
            file_suffix="_udm2",
        ),
    ),
    "sentinel2_l2a": ProductProfile(
        product_id="sentinel2_l2a",
        bands={
            "coastal_aerosol": 0,
            "blue": 1,
            "green": 2,
            "red": 3,
            "vegetation_red_edge_1": 4,
            "vegetation_red_edge_2": 5,
            "vegetation_red_edge_3": 6,
            "nir": 7,
            "narrow_nir": 8,
            "water_vapour": 9,
            "swir_cirrus": 10,
            "swir_1": 11,
            "swir_2": 12,
        },
        resolution=10.0,
        provider="ESA",
        description="Sentinel-2 Level-2A Surface Reflectance",
        cloud_mask=CloudMask(
            band_index=0,  # SCL is single-band
            clear_values=[4, 5, 6, 7, 11],  # vegetation, bare soil, water, unclassified, snow/ice
            file_suffix="_SCL",
        ),
    ),
    "landsat8_sr": ProductProfile(
        product_id="landsat8_sr",
        bands={
            "coastal_aerosol": 0,
            "blue": 1,
            "green": 2,
            "red": 3,
            "nir": 4,
            "swir_1": 5,
            "swir_2": 6,
        },
        resolution=30.0,
        provider="USGS",
        description="Landsat 8 Collection 2 Surface Reflectance",
        cloud_mask=CloudMask(
            band_index=0,  # QA_PIXEL single band
            clear_values=[21824],  # bit-packed: clear + low cloud confidence
            file_suffix="_QA_PIXEL",
        ),
    ),
    "landsat9_sr": ProductProfile(
        product_id="landsat9_sr",
        bands={
            "coastal_aerosol": 0,
            "blue": 1,
            "green": 2,
            "red": 3,
            "nir": 4,
            "swir_1": 5,
            "swir_2": 6,
        },
        resolution=30.0,
        provider="USGS",
        description="Landsat 9 Collection 2 Surface Reflectance",
        cloud_mask=CloudMask(
            band_index=0,
            clear_values=[21824],
            file_suffix="_QA_PIXEL",
        ),
    ),
}


class ProductRegistry:
    """
    In-memory registry of product profiles.

    Includes built-in profiles and user-registered profiles.
    Profiles are also persisted to Icechunk repo when a warehouse is used.
    """

    def __init__(self):
        self._profiles: dict[str, ProductProfile] = dict(BUILTIN_PROFILES)

    def register(self, profile: ProductProfile) -> None:
        """Register a product profile."""
        self._profiles[profile.product_id] = profile
        logger.info("Registered product profile: %s", profile.product_id)

    def get(self, product_id: str) -> ProductProfile | None:
        """Get a profile by product_id."""
        return self._profiles.get(product_id)

    def list_products(self) -> list[str]:
        """List all registered product IDs."""
        return sorted(self._profiles.keys())

    def list_profiles(self) -> list[ProductProfile]:
        """List all registered profiles."""
        return [self._profiles[k] for k in sorted(self._profiles)]

    def to_dict(self) -> dict[str, Any]:
        """Serialize all profiles to dict (for zarr attrs storage)."""
        return {pid: p.to_dict() for pid, p in self._profiles.items()}

    def load_from_dict(self, data: dict[str, Any]) -> None:
        """Load profiles from dict (from zarr attrs)."""
        for pid, pdata in data.items():
            self._profiles[pid] = ProductProfile.from_dict(pdata)


# Global registry instance
_global_registry = ProductRegistry()


def register_product(
    product_id: str,
    bands: dict[str, int],
    resolution: float,
    provider: str = "",
    description: str = "",
    cloud_mask: CloudMask | None = None,
    extra_fields: list[str] | None = None,
) -> ProductProfile:
    """
    Register a product profile in the global registry.

    Args:
        product_id: Unique identifier
        bands: Band name to index mapping
        resolution: Spatial resolution in meters
        provider: Data provider name
        description: Human-readable description
        cloud_mask: Cloud mask configuration
        extra_fields: Additional metadata fields

    Returns:
        The created ProductProfile

    Examples:
        >>> pq.register_product(
        ...     "my_drone",
        ...     bands={"red": 0, "green": 1, "blue": 2},
        ...     resolution=0.05,
        ...     provider="DJI",
        ... )
    """
    profile = ProductProfile(
        product_id=product_id,
        bands=bands,
        resolution=resolution,
        provider=provider,
        description=description,
        cloud_mask=cloud_mask,
        extra_fields=extra_fields or [],
    )
    _global_registry.register(profile)
    return profile


def get_registry() -> ProductRegistry:
    """Get the global product registry."""
    return _global_registry
