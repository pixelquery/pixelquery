"""
Product Profiles

Satellite product profile implementations (Sentinel-2, Landsat-8, Planet, etc.)
"""

from pixelquery.products.profiles.sentinel2 import Sentinel2L2A, Sentinel2BandInfo
from pixelquery.products.profiles.landsat8 import Landsat8L2, Landsat8BandInfo

__all__ = [
    "Sentinel2L2A",
    "Sentinel2BandInfo",
    "Landsat8L2",
    "Landsat8BandInfo",
]
