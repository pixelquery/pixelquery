"""
PixelQuery Products Module

Satellite/sensor product profiles and band information.
"""

from pixelquery.products.base import (
    Band,
    BandInfo,
    ProductProfile,
    get_profile,
    list_profiles,
    register_profile,
)

__all__ = [
    "Band",
    "BandInfo",
    "ProductProfile",
    "get_profile",
    "list_profiles",
    "register_profile",
]


def _register_builtins() -> None:
    """Register built-in satellite profiles."""
    from pixelquery.products.profiles.landsat8 import Landsat8L2
    from pixelquery.products.profiles.sentinel2 import Sentinel2L2A

    # Convert legacy dataclass profiles to new ProductProfile
    _legacy_to_profile(Landsat8L2())
    _legacy_to_profile(Sentinel2L2A())


def _legacy_to_profile(legacy) -> None:
    """Convert a legacy frozen-dataclass profile to ProductProfile and register."""
    bands = {}
    if legacy.bands:
        for name, b in legacy.bands.items():
            bands[name] = Band(
                standard_name=b.standard_name,
                wavelength=b.wavelength,
                resolution=b.resolution,
                native_name=b.native_name,
                bandwidth=b.bandwidth,
            )
    profile = ProductProfile(
        product_id=legacy.product_id,
        bands=bands,
        provider=legacy.provider,
        sensor=legacy.sensor,
        native_resolution=legacy.native_resolution,
        scale_factor=legacy.scale_factor,
        offset=legacy.offset,
        nodata=legacy.nodata,
    )
    register_profile(profile)


_register_builtins()
