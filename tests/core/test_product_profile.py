"""Tests for ProductProfile system."""

import pytest


class TestProductProfile:
    """Test ProductProfile dataclass."""

    def test_create_profile(self):
        from pixelquery.catalog.product_profile import ProductProfile

        profile = ProductProfile(
            product_id="test_product",
            bands={"red": 0, "green": 1, "blue": 2, "nir": 3},
            resolution=3.0,
            provider="Test",
        )
        assert profile.product_id == "test_product"
        assert profile.band_count == 4
        assert profile.band_names == ["red", "green", "blue", "nir"]

    def test_to_dict_from_dict(self):
        from pixelquery.catalog.product_profile import ProductProfile

        profile = ProductProfile(
            product_id="test",
            bands={"red": 0, "nir": 1},
            resolution=10.0,
            provider="Test",
            description="A test product",
        )
        data = profile.to_dict()
        restored = ProductProfile.from_dict(data)

        assert restored.product_id == profile.product_id
        assert restored.bands == profile.bands
        assert restored.resolution == profile.resolution
        assert restored.provider == profile.provider
        assert restored.description == profile.description

    def test_band_names_sorted_by_index(self):
        from pixelquery.catalog.product_profile import ProductProfile

        profile = ProductProfile(
            product_id="test",
            bands={"nir": 3, "red": 2, "green": 1, "blue": 0},
            resolution=3.0,
        )
        assert profile.band_names == ["blue", "green", "red", "nir"]

    def test_repr(self):
        from pixelquery.catalog.product_profile import ProductProfile

        profile = ProductProfile(
            product_id="planet_sr",
            bands={"red": 0, "nir": 1},
            resolution=3.0,
            provider="Planet",
        )
        text = repr(profile)
        assert "planet_sr" in text
        assert "Planet" in text
        assert "3.0m" in text


class TestProductRegistry:
    """Test ProductRegistry."""

    def test_builtin_profiles(self):
        from pixelquery.catalog.product_profile import ProductRegistry

        registry = ProductRegistry()
        products = registry.list_products()
        assert "planet_sr" in products
        assert "sentinel2_l2a" in products
        assert "landsat8_sr" in products

    def test_register_custom(self):
        from pixelquery.catalog.product_profile import ProductProfile, ProductRegistry

        registry = ProductRegistry()
        custom = ProductProfile(
            product_id="my_drone",
            bands={"r": 0, "g": 1, "b": 2},
            resolution=0.05,
            provider="DJI",
        )
        registry.register(custom)
        assert "my_drone" in registry.list_products()
        assert registry.get("my_drone") == custom

    def test_get_nonexistent(self):
        from pixelquery.catalog.product_profile import ProductRegistry

        registry = ProductRegistry()
        assert registry.get("nonexistent") is None

    def test_to_dict(self):
        from pixelquery.catalog.product_profile import ProductRegistry

        registry = ProductRegistry()
        data = registry.to_dict()
        assert "planet_sr" in data
        assert "bands" in data["planet_sr"]


class TestRegisterProductFunction:
    """Test the register_product convenience function."""

    def test_register_product(self):
        from pixelquery.catalog.product_profile import get_registry, register_product

        profile = register_product(
            "test_satellite",
            bands={"red": 0, "nir": 1},
            resolution=5.0,
            provider="TestCo",
        )
        assert profile.product_id == "test_satellite"

        registry = get_registry()
        assert registry.get("test_satellite") is not None
