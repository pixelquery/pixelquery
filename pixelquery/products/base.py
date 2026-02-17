"""
Product Profile and Band Information Protocols

Defines satellite product specifications to enable multi-resolution fusion.
"""

from typing import Protocol


class BandInfo(Protocol):
    """
    Satellite band metadata

    Attributes:
        native_name: Product-specific band name (e.g., "B04" for Sentinel-2)
        standard_name: Standardized name (e.g., "red")
        wavelength: Center wavelength in nanometers
        resolution: Native spatial resolution in meters
        bandwidth: Spectral bandwidth in nanometers (optional)
    """

    native_name: str
    standard_name: str
    wavelength: float
    resolution: float
    bandwidth: float | None


class ProductProfile(Protocol):
    """
    Satellite product specification

    Defines the characteristics of a satellite product (e.g., Sentinel-2 L2A)
    to enable multi-resolution fusion.

    Attributes:
        product_id: Unique identifier (e.g., "sentinel2_l2a")
        provider: Data provider (e.g., "ESA")
        sensor: Sensor name (e.g., "MSI")
        native_resolution: Primary spatial resolution in meters
        bands: Dictionary mapping standard band names to BandInfo
        scale_factor: DN to reflectance conversion factor
        offset: Additive offset for conversion
        nodata: No-data pixel value
    """

    product_id: str
    provider: str
    sensor: str
    native_resolution: float
    bands: dict[str, BandInfo]
    scale_factor: float
    offset: float
    nodata: int
