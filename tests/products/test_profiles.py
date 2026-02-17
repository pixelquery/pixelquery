"""
Tests for Product Profiles
"""

import pytest

from pixelquery.products.profiles import (
    Landsat8BandInfo,
    Landsat8L2,
    Sentinel2BandInfo,
    Sentinel2L2A,
)


class TestSentinel2L2A:
    """Test Sentinel-2 L2A product profile"""

    @pytest.fixture
    def profile(self):
        """Create Sentinel-2 profile"""
        return Sentinel2L2A()

    def test_product_metadata(self, profile):
        """Test product identification metadata"""
        assert profile.product_id == "sentinel2_l2a"
        assert profile.provider == "ESA"
        assert profile.sensor == "MSI"
        assert profile.native_resolution == 10.0

    def test_radiometric_conversion(self, profile):
        """Test radiometric conversion parameters"""
        assert profile.scale_factor == 0.0001
        assert profile.offset == 0.0
        assert profile.nodata == 0

    def test_bands_exist(self, profile):
        """Test that all expected bands exist"""
        expected_bands = [
            "blue",
            "green",
            "red",
            "nir",  # 10m bands
            "red_edge_1",
            "red_edge_2",
            "red_edge_3",  # 20m red edge
            "nir_narrow",
            "swir_1",
            "swir_2",  # 20m NIR/SWIR
        ]
        for band_name in expected_bands:
            assert band_name in profile.bands

    def test_10m_bands(self, profile):
        """Test 10m resolution bands"""
        bands_10m = profile.get_10m_bands()
        assert len(bands_10m) == 4
        assert "blue" in bands_10m
        assert "green" in bands_10m
        assert "red" in bands_10m
        assert "nir" in bands_10m

        # Check resolution
        for band in bands_10m.values():
            assert band.resolution == 10.0

    def test_20m_bands(self, profile):
        """Test 20m resolution bands"""
        bands_20m = profile.get_20m_bands()
        assert len(bands_20m) == 6  # 3 red edge + narrow NIR + 2 SWIR

        # Check resolution
        for band in bands_20m.values():
            assert band.resolution == 20.0

    def test_blue_band_specs(self, profile):
        """Test blue band specifications"""
        blue = profile.bands["blue"]
        assert blue.native_name == "B02"
        assert blue.standard_name == "blue"
        assert blue.wavelength == 490.0
        assert blue.resolution == 10.0
        assert blue.bandwidth == 65.0

    def test_red_band_specs(self, profile):
        """Test red band specifications"""
        red = profile.bands["red"]
        assert red.native_name == "B04"
        assert red.standard_name == "red"
        assert red.wavelength == 665.0
        assert red.resolution == 10.0
        assert red.bandwidth == 30.0

    def test_nir_band_specs(self, profile):
        """Test NIR band specifications"""
        nir = profile.bands["nir"]
        assert nir.native_name == "B08"
        assert nir.standard_name == "nir"
        assert nir.wavelength == 842.0
        assert nir.resolution == 10.0
        assert nir.bandwidth == 115.0

    def test_swir_band_specs(self, profile):
        """Test SWIR band specifications"""
        swir1 = profile.bands["swir_1"]
        assert swir1.native_name == "B11"
        assert swir1.resolution == 20.0
        assert swir1.wavelength == 1610.0

        swir2 = profile.bands["swir_2"]
        assert swir2.native_name == "B12"
        assert swir2.resolution == 20.0
        assert swir2.wavelength == 2190.0

    def test_get_band_by_native_name(self, profile):
        """Test getting band by Sentinel-2 native name"""
        band = profile.get_band_by_native_name("B04")
        assert band.standard_name == "red"

        band = profile.get_band_by_native_name("B08")
        assert band.standard_name == "nir"

    def test_get_band_by_native_name_not_found(self, profile):
        """Test error when native name not found"""
        with pytest.raises(KeyError):
            profile.get_band_by_native_name("B99")

    def test_band_info_immutable(self, profile):
        """Test that BandInfo is immutable"""
        band = profile.bands["red"]
        with pytest.raises(AttributeError):  # dataclass frozen
            band.wavelength = 999.0

    def test_repr(self, profile):
        """Test string representation"""
        repr_str = repr(profile)
        assert "Sentinel2L2A" in repr_str
        assert "ESA" in repr_str
        assert "MSI" in repr_str
        assert "10m" in repr_str


class TestLandsat8L2:
    """Test Landsat-8 L2 product profile"""

    @pytest.fixture
    def profile(self):
        """Create Landsat-8 profile"""
        return Landsat8L2()

    def test_product_metadata(self, profile):
        """Test product identification metadata"""
        assert profile.product_id == "landsat8_l2"
        assert profile.provider == "USGS"
        assert profile.sensor == "OLI"
        assert profile.native_resolution == 30.0

    def test_radiometric_conversion(self, profile):
        """Test radiometric conversion parameters (Collection 2)"""
        assert profile.scale_factor == 0.0000275
        assert profile.offset == -0.2
        assert profile.nodata == 0

    def test_bands_exist(self, profile):
        """Test that all expected bands exist"""
        expected_bands = ["coastal", "blue", "green", "red", "nir", "swir_1", "swir_2"]
        for band_name in expected_bands:
            assert band_name in profile.bands

    def test_all_bands_30m(self, profile):
        """Test that all bands are 30m resolution"""
        for band in profile.bands.values():
            assert band.resolution == 30.0

    def test_blue_band_specs(self, profile):
        """Test blue band specifications"""
        blue = profile.bands["blue"]
        assert blue.native_name == "B2"
        assert blue.standard_name == "blue"
        assert blue.wavelength == 482.0
        assert blue.resolution == 30.0
        assert blue.bandwidth == 60.0

    def test_red_band_specs(self, profile):
        """Test red band specifications"""
        red = profile.bands["red"]
        assert red.native_name == "B4"
        assert red.standard_name == "red"
        assert red.wavelength == 655.0
        assert red.resolution == 30.0
        assert red.bandwidth == 37.0

    def test_nir_band_specs(self, profile):
        """Test NIR band specifications"""
        nir = profile.bands["nir"]
        assert nir.native_name == "B5"
        assert nir.standard_name == "nir"
        assert nir.wavelength == 865.0
        assert nir.resolution == 30.0
        assert nir.bandwidth == 28.0

    def test_swir_band_specs(self, profile):
        """Test SWIR band specifications"""
        swir1 = profile.bands["swir_1"]
        assert swir1.native_name == "B6"
        assert swir1.wavelength == 1610.0

        swir2 = profile.bands["swir_2"]
        assert swir2.native_name == "B7"
        assert swir2.wavelength == 2200.0

    def test_coastal_band_specs(self, profile):
        """Test coastal/aerosol band"""
        coastal = profile.bands["coastal"]
        assert coastal.native_name == "B1"
        assert coastal.wavelength == 443.0
        assert coastal.resolution == 30.0

    def test_get_band_by_native_name(self, profile):
        """Test getting band by Landsat-8 native name"""
        band = profile.get_band_by_native_name("B4")
        assert band.standard_name == "red"

        band = profile.get_band_by_native_name("B5")
        assert band.standard_name == "nir"

    def test_get_band_by_native_name_not_found(self, profile):
        """Test error when native name not found"""
        with pytest.raises(KeyError):
            profile.get_band_by_native_name("B99")

    def test_get_common_bands(self, profile):
        """Test getting common bands for vegetation analysis"""
        common = profile.get_common_bands()
        assert len(common) == 4
        assert "blue" in common
        assert "green" in common
        assert "red" in common
        assert "nir" in common

    def test_band_info_immutable(self, profile):
        """Test that BandInfo is immutable"""
        band = profile.bands["red"]
        with pytest.raises(AttributeError):  # dataclass frozen
            band.wavelength = 999.0

    def test_repr(self, profile):
        """Test string representation"""
        repr_str = repr(profile)
        assert "Landsat8L2" in repr_str
        assert "USGS" in repr_str
        assert "OLI" in repr_str
        assert "30m" in repr_str


class TestCrossProductComparison:
    """Test comparisons between different products"""

    def test_common_band_wavelengths(self):
        """Test that common bands have similar wavelengths"""
        s2 = Sentinel2L2A()
        l8 = Landsat8L2()

        # Blue bands should be similar (within 10nm)
        assert abs(s2.bands["blue"].wavelength - l8.bands["blue"].wavelength) < 10

        # Red bands should be similar
        assert abs(s2.bands["red"].wavelength - l8.bands["red"].wavelength) < 15

        # NIR bands should be similar
        assert abs(s2.bands["nir"].wavelength - l8.bands["nir"].wavelength) < 25

    def test_resolution_differences(self):
        """Test resolution differences between products"""
        s2 = Sentinel2L2A()
        l8 = Landsat8L2()

        # Sentinel-2 has finer native resolution for RGB+NIR
        assert s2.native_resolution < l8.native_resolution
        assert s2.bands["red"].resolution == 10.0
        assert l8.bands["red"].resolution == 30.0

    def test_scale_factor_differences(self):
        """Test radiometric conversion differences"""
        s2 = Sentinel2L2A()
        l8 = Landsat8L2()

        # Different scale factors
        assert s2.scale_factor != l8.scale_factor

        # Sentinel-2 has no offset, Landsat-8 does
        assert s2.offset == 0.0
        assert l8.offset == -0.2
