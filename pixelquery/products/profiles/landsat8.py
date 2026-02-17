"""
Landsat-8 Product Profile

Implements ProductProfile for Landsat-8 Collection 2 Level-2 (Surface Reflectance) data.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Landsat8BandInfo:
    """
    Landsat-8 band metadata

    Attributes:
        native_name: Landsat-8 band name (e.g., "B4")
        standard_name: Standardized name (e.g., "red")
        wavelength: Center wavelength in nanometers
        resolution: Native spatial resolution in meters (30m)
        bandwidth: Spectral bandwidth in nanometers
    """

    native_name: str
    standard_name: str
    wavelength: float
    resolution: float
    bandwidth: float


@dataclass(frozen=True)
class Landsat8L2:
    """
    Landsat-8 Collection 2 Level-2 Product Profile

    Surface Reflectance data with atmospheric correction.
    Source: USGS Landsat program

    Specifications:
    - Sensor: Operational Land Imager (OLI)
    - Provider: United States Geological Survey (USGS)
    - Native Resolution: 30m (for all bands)
    - Revisit Time: 16 days
    - Swath Width: 185 km
    - Radiometric Resolution: 16-bit

    Band Configuration:
    - 30m bands: Coastal/Aerosol, Blue, Green, Red, NIR, SWIR-1, SWIR-2

    Data Format (Collection 2 Level-2):
    - DN to Reflectance: (DN * 0.0000275) - 0.2
    - No-data value: 0
    - Valid range: 7273 - 43636 (0.0 - 1.0 reflectance after scaling)

    Examples:
        >>> from pixelquery.products.profiles import Landsat8L2
        >>> profile = Landsat8L2()
        >>> print(profile.product_id)
        'landsat8_l2'
        >>> print(profile.bands['red'].wavelength)
        655.0
        >>> print(profile.bands['red'].resolution)
        30.0
    """

    # Product identification
    product_id: str = "landsat8_l2"
    provider: str = "USGS"
    sensor: str = "OLI"
    native_resolution: float = 30.0  # All bands are 30m

    # Radiometric conversion (Collection 2 Level-2)
    scale_factor: float = 0.0000275  # DN to reflectance
    offset: float = -0.2
    nodata: int = 0

    # Band specifications (immutable)
    bands: dict[str, Landsat8BandInfo] | None = None

    def __post_init__(self):
        """Initialize band definitions"""
        if self.bands is None:
            # Use object.__setattr__ because dataclass is frozen
            object.__setattr__(
                self,
                "bands",
                {
                    # All bands at 30m resolution
                    "coastal": Landsat8BandInfo(
                        native_name="B1",
                        standard_name="coastal",
                        wavelength=443.0,
                        resolution=30.0,
                        bandwidth=16.0,
                    ),
                    "blue": Landsat8BandInfo(
                        native_name="B2",
                        standard_name="blue",
                        wavelength=482.0,
                        resolution=30.0,
                        bandwidth=60.0,
                    ),
                    "green": Landsat8BandInfo(
                        native_name="B3",
                        standard_name="green",
                        wavelength=562.0,
                        resolution=30.0,
                        bandwidth=57.0,
                    ),
                    "red": Landsat8BandInfo(
                        native_name="B4",
                        standard_name="red",
                        wavelength=655.0,
                        resolution=30.0,
                        bandwidth=37.0,
                    ),
                    "nir": Landsat8BandInfo(
                        native_name="B5",
                        standard_name="nir",
                        wavelength=865.0,
                        resolution=30.0,
                        bandwidth=28.0,
                    ),
                    "swir_1": Landsat8BandInfo(
                        native_name="B6",
                        standard_name="swir_1",
                        wavelength=1610.0,
                        resolution=30.0,
                        bandwidth=85.0,
                    ),
                    "swir_2": Landsat8BandInfo(
                        native_name="B7",
                        standard_name="swir_2",
                        wavelength=2200.0,
                        resolution=30.0,
                        bandwidth=187.0,
                    ),
                },
            )

    def get_band_by_native_name(self, native_name: str) -> Landsat8BandInfo:
        """
        Get band info by Landsat-8 native name (e.g., "B4")

        Args:
            native_name: Landsat-8 band name (e.g., "B4", "B5")

        Returns:
            BandInfo for the specified band

        Raises:
            KeyError: If band name not found

        Examples:
            >>> profile = Landsat8L2()
            >>> band = profile.get_band_by_native_name('B4')
            >>> print(band.standard_name)
            'red'
        """
        assert self.bands is not None
        for band_info in self.bands.values():
            if band_info.native_name == native_name:
                return band_info
        raise KeyError(f"Band '{native_name}' not found in Landsat-8 profile")

    def get_common_bands(self) -> dict[str, Landsat8BandInfo]:
        """
        Get common bands for vegetation analysis (blue, green, red, nir)

        Returns:
            Dictionary of common bands
        """
        assert self.bands is not None
        return {
            name: band
            for name, band in self.bands.items()
            if name in ["blue", "green", "red", "nir"]
        }

    def __repr__(self) -> str:
        """String representation"""
        assert self.bands is not None
        return (
            f"<Landsat8L2>\n"
            f"Provider: {self.provider}\n"
            f"Sensor: {self.sensor}\n"
            f"Native Resolution: {self.native_resolution}m\n"
            f"Bands: {len(self.bands)} @ 30m"
        )
