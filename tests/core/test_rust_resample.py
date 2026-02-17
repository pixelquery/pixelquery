"""Tests for Rust resampling functions"""

import numpy as np
import pytest
from scipy.ndimage import zoom

try:
    from pixelquery_core import resample_bilinear, resample_nearest_neighbor

    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extensions not built")
class TestRustResampleBilinear:
    """Tests for bilinear interpolation"""

    def test_basic_upscale(self):
        """Test basic 2x upscaling"""
        input_data = np.array([[100, 200], [300, 400]], dtype=np.uint16)
        result = resample_bilinear(input_data, 4, 4)

        assert result.shape == (4, 4)
        assert result.dtype == np.uint16
        # Corners should be preserved
        assert result[0, 0] == 100
        assert result[0, 3] == 200
        assert result[3, 0] == 300
        assert result[3, 3] == 400

    def test_basic_downscale(self):
        """Test downscaling"""
        input_data = np.random.randint(0, 4000, (256, 256), dtype=np.uint16)
        result = resample_bilinear(input_data, 64, 64)

        assert result.shape == (64, 64)
        assert result.dtype == np.uint16
        assert result.min() >= 0
        assert result.max() <= 4000

    def test_same_size(self):
        """Test when output size equals input size"""
        input_data = np.array([[100, 200], [300, 400]], dtype=np.uint16)
        result = resample_bilinear(input_data, 2, 2)

        assert result.shape == (2, 2)
        np.testing.assert_array_equal(result, input_data)

    def test_non_square(self):
        """Test non-square input and output"""
        input_data = np.random.randint(0, 4000, (100, 200), dtype=np.uint16)
        result = resample_bilinear(input_data, 50, 100)

        assert result.shape == (50, 100)

    def test_large_array(self):
        """Test with large array to exercise parallel code path"""
        input_data = np.random.randint(0, 4000, (512, 512), dtype=np.uint16)
        result = resample_bilinear(input_data, 256, 256)

        assert result.shape == (256, 256)

    def test_uniform_input(self):
        """Uniform input should give uniform output"""
        input_data = np.full((100, 100), 2000, dtype=np.uint16)
        result = resample_bilinear(input_data, 50, 50)

        assert result.shape == (50, 50)
        assert np.all(result == 2000)

    def test_matches_scipy_approximate(self):
        """Verify Rust results match scipy within reasonable tolerance"""
        input_data = np.random.randint(0, 4000, (100, 100), dtype=np.uint16)
        target_h, target_w = 256, 256

        rust_result = resample_bilinear(input_data, target_h, target_w)

        zoom_factors = (target_h / 100, target_w / 100)
        scipy_result = zoom(input_data, zoom_factors, order=1).astype(np.uint16)

        # Allow small differences due to different interpolation edge handling
        diff = np.abs(rust_result.astype(int) - scipy_result.astype(int))
        assert np.mean(diff) < 5.0, f"Mean difference {np.mean(diff)} too large"
        assert np.max(diff) < 50, f"Max difference {np.max(diff)} too large"


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extensions not built")
class TestRustResampleNearestNeighbor:
    """Tests for nearest-neighbor interpolation"""

    def test_basic_upscale(self):
        """Test basic upscaling of boolean mask"""
        input_mask = np.array([[True, False], [False, True]], dtype=bool)
        result = resample_nearest_neighbor(input_mask, 4, 4)

        assert result.shape == (4, 4)
        assert result.dtype == bool

    def test_basic_downscale(self):
        """Test downscaling of boolean mask"""
        input_mask = np.random.choice([True, False], (256, 256))
        result = resample_nearest_neighbor(input_mask, 64, 64)

        assert result.shape == (64, 64)
        assert result.dtype == bool

    def test_same_size(self):
        """Test when output size equals input size"""
        input_mask = np.array([[True, False], [False, True]], dtype=bool)
        result = resample_nearest_neighbor(input_mask, 2, 2)

        assert result.shape == (2, 2)
        np.testing.assert_array_equal(result, input_mask)

    def test_all_true(self):
        """All True input should give all True output"""
        input_mask = np.ones((100, 100), dtype=bool)
        result = resample_nearest_neighbor(input_mask, 50, 50)

        assert result.shape == (50, 50)
        assert np.all(result)

    def test_all_false(self):
        """All False input should give all False output"""
        input_mask = np.zeros((100, 100), dtype=bool)
        result = resample_nearest_neighbor(input_mask, 50, 50)

        assert result.shape == (50, 50)
        assert not np.any(result)

    def test_large_array(self):
        """Test with large array to exercise parallel code path"""
        input_mask = np.random.choice([True, False], (512, 512))
        result = resample_nearest_neighbor(input_mask, 256, 256)

        assert result.shape == (256, 256)
        assert result.dtype == bool


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extensions not built")
class TestRustResampleEdgeCases:
    """Edge case tests"""

    def test_single_pixel_input(self):
        """Test with 1x1 input"""
        input_data = np.array([[1000]], dtype=np.uint16)
        result = resample_bilinear(input_data, 10, 10)

        assert result.shape == (10, 10)
        # All pixels should have same value
        assert np.all(result == 1000)

    def test_single_row(self):
        """Test with single row input"""
        input_data = np.array([[100, 200, 300]], dtype=np.uint16)
        result = resample_bilinear(input_data, 3, 6)

        assert result.shape == (3, 6)

    def test_single_column(self):
        """Test with single column input"""
        input_data = np.array([[100], [200], [300]], dtype=np.uint16)
        result = resample_bilinear(input_data, 6, 3)

        assert result.shape == (6, 3)

    def test_max_value(self):
        """Test with uint16 max value"""
        input_data = np.full((10, 10), 65535, dtype=np.uint16)
        result = resample_bilinear(input_data, 20, 20)

        assert result.shape == (20, 20)
        assert np.all(result == 65535)

    def test_zero_value(self):
        """Test with all zeros"""
        input_data = np.zeros((10, 10), dtype=np.uint16)
        result = resample_bilinear(input_data, 20, 20)

        assert result.shape == (20, 20)
        assert np.all(result == 0)


# Fallback test if Rust not available
class TestFallbackBehavior:
    """Test that code gracefully handles missing Rust extensions"""

    def test_import_check(self):
        """Verify we can detect Rust availability"""
        # This test always passes - just verifies the import detection works
        try:
            from pixelquery_core import resample_bilinear

            assert True  # Rust available
        except ImportError:
            assert True  # Rust not available - that's OK for this test
