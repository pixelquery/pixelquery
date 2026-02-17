"""
Sentinel-2 Product Profile

Implements ProductProfile for Sentinel-2 L2A (Surface Reflectance) data.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Sentinel2BandInfo:
    """
    Sentinel-2 band metadata

    Attributes:
        native_name: Sentinel-2 band name (e.g., "B04")
        standard_name: Standardized name (e.g., "red")
        wavelength: Center wavelength in nanometers
        resolution: Native spatial resolution in meters (10m or 20m)
        bandwidth: Spectral bandwidth in nanometers
    """

    native_name: str
    standard_name: str
    wavelength: float
    resolution: float
    bandwidth: float


@dataclass(frozen=True)
class Sentinel2L2A:
    """
    Sentinel-2 Level-2A Product Profile

    Surface Reflectance data with atmospheric correction.
    Source: ESA Copernicus program

    Specifications:
    - Sensor: MultiSpectral Instrument (MSI)
    - Provider: European Space Agency (ESA)
    - Native Resolution: 10m (for primary bands)
    - Revisit Time: 5 days (2 satellites)
    - Swath Width: 290 km
    - Radiometric Resolution: 12-bit

    Band Configuration:
    - 10m bands: Blue, Green, Red, NIR
    - 20m bands: Red Edge (3 bands), Narrow NIR, SWIR (2 bands)

    Data Format:
    - DN to Reflectance: DN * 0.0001
    - No-data value: 0
    - Valid range: 1 - 10000 (0.0001 - 1.0 reflectance)

    Examples:
        >>> from pixelquery.products.profiles import Sentinel2L2A
        >>> profile = Sentinel2L2A()
        >>> print(profile.product_id)
        'sentinel2_l2a'
        >>> print(profile.bands['red'].wavelength)
        665.0
        >>> print(profile.bands['red'].resolution)
        10.0
    """

    # Product identification
    product_id: str = "sentinel2_l2a"
    provider: str = "ESA"
    sensor: str = "MSI"
    native_resolution: float = 10.0  # Primary band resolution

    # Radiometric conversion
    scale_factor: float = 0.0001  # DN to reflectance
    offset: float = 0.0
    nodata: int = 0

    # Band specifications (immutable)
    bands: dict[str, Sentinel2BandInfo] = None

    def __post_init__(self):
        """Initialize band definitions"""
        if self.bands is None:
            # Use object.__setattr__ because dataclass is frozen
            object.__setattr__(
                self,
                "bands",
                {
                    # 10m resolution bands
                    "blue": Sentinel2BandInfo(
                        native_name="B02",
                        standard_name="blue",
                        wavelength=490.0,
                        resolution=10.0,
                        bandwidth=65.0,
                    ),
                    "green": Sentinel2BandInfo(
                        native_name="B03",
                        standard_name="green",
                        wavelength=560.0,
                        resolution=10.0,
                        bandwidth=35.0,
                    ),
                    "red": Sentinel2BandInfo(
                        native_name="B04",
                        standard_name="red",
                        wavelength=665.0,
                        resolution=10.0,
                        bandwidth=30.0,
                    ),
                    "nir": Sentinel2BandInfo(
                        native_name="B08",
                        standard_name="nir",
                        wavelength=842.0,
                        resolution=10.0,
                        bandwidth=115.0,
                    ),
                    # 20m resolution bands
                    "red_edge_1": Sentinel2BandInfo(
                        native_name="B05",
                        standard_name="red_edge_1",
                        wavelength=705.0,
                        resolution=20.0,
                        bandwidth=15.0,
                    ),
                    "red_edge_2": Sentinel2BandInfo(
                        native_name="B06",
                        standard_name="red_edge_2",
                        wavelength=740.0,
                        resolution=20.0,
                        bandwidth=15.0,
                    ),
                    "red_edge_3": Sentinel2BandInfo(
                        native_name="B07",
                        standard_name="red_edge_3",
                        wavelength=783.0,
                        resolution=20.0,
                        bandwidth=20.0,
                    ),
                    "nir_narrow": Sentinel2BandInfo(
                        native_name="B8A",
                        standard_name="nir_narrow",
                        wavelength=865.0,
                        resolution=20.0,
                        bandwidth=20.0,
                    ),
                    "swir_1": Sentinel2BandInfo(
                        native_name="B11",
                        standard_name="swir_1",
                        wavelength=1610.0,
                        resolution=20.0,
                        bandwidth=90.0,
                    ),
                    "swir_2": Sentinel2BandInfo(
                        native_name="B12",
                        standard_name="swir_2",
                        wavelength=2190.0,
                        resolution=20.0,
                        bandwidth=180.0,
                    ),
                },
            )

    def get_band_by_native_name(self, native_name: str) -> Sentinel2BandInfo:
        """
        Get band info by Sentinel-2 native name (e.g., "B04")

        Args:
            native_name: Sentinel-2 band name (e.g., "B04", "B08")

        Returns:
            BandInfo for the specified band

        Raises:
            KeyError: If band name not found

        Examples:
            >>> profile = Sentinel2L2A()
            >>> band = profile.get_band_by_native_name('B04')
            >>> print(band.standard_name)
            'red'
        """
        for band_info in self.bands.values():
            if band_info.native_name == native_name:
                return band_info
        raise KeyError(f"Band '{native_name}' not found in Sentinel-2 profile")

    def get_10m_bands(self) -> dict[str, Sentinel2BandInfo]:
        """
        Get all 10m resolution bands

        Returns:
            Dictionary of 10m bands (blue, green, red, nir)
        """
        return {name: band for name, band in self.bands.items() if band.resolution == 10.0}

    def get_20m_bands(self) -> dict[str, Sentinel2BandInfo]:
        """
        Get all 20m resolution bands

        Returns:
            Dictionary of 20m bands (red edge, narrow NIR, SWIR)
        """
        return {name: band for name, band in self.bands.items() if band.resolution == 20.0}

    def __repr__(self) -> str:
        """String representation"""
        return (
            f"<Sentinel2L2A>\n"
            f"Provider: {self.provider}\n"
            f"Sensor: {self.sensor}\n"
            f"Native Resolution: {self.native_resolution}m\n"
            f"Bands: {len(self.bands)} ({len(self.get_10m_bands())} @ 10m, {len(self.get_20m_bands())} @ 20m)"
        )
