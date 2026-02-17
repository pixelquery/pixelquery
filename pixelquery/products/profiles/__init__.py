"""
Product Profiles

Satellite product profile implementations (Sentinel-2, Landsat-8, Planet, etc.)
"""

from pixelquery.products.profiles.landsat8 import Landsat8BandInfo, Landsat8L2
from pixelquery.products.profiles.sentinel2 import Sentinel2BandInfo, Sentinel2L2A

__all__ = [
    "Landsat8BandInfo",
    "Landsat8L2",
    "Sentinel2BandInfo",
    "Sentinel2L2A",
]
