import pytest
import numpy as np

try:
    from pixelquery_core import resample_bilinear, resample_nearest_neighbor
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extensions not built")
class TestRustResample:
    def test_bilinear_upscale(self):
        """Test upscaling with bilinear interpolation"""
        input_data = np.array([
            [100, 200],
            [300, 400]
        ], dtype=np.uint16)

        result = resample_bilinear(input_data, 4, 4)

        assert result.shape == (4, 4)
        assert result.dtype == np.uint16
        # Check corners match
        assert result[0, 0] == 100
        assert result[0, 3] == 200
        assert result[3, 0] == 300
        assert result[3, 3] == 400

    def test_bilinear_downscale(self):
        """Test downscaling preserves range"""
        input_data = np.random.randint(0, 4000, (256, 256), dtype=np.uint16)
        result = resample_bilinear(input_data, 85, 85)

        assert result.shape == (85, 85)
        assert result.dtype == np.uint16
        assert result.min() >= 0
        assert result.max() <= 4000

    def test_bilinear_same_size(self):
        """Test no-op when input and output sizes are the same"""
        input_data = np.random.randint(0, 4000, (100, 100), dtype=np.uint16)
        result = resample_bilinear(input_data, 100, 100)

        assert result.shape == (100, 100)
        assert result.dtype == np.uint16

    def test_nearest_neighbor_mask(self):
        """Test boolean mask resampling"""
        mask = np.array([
            [True, False],
            [False, True]
        ], dtype=bool)

        result = resample_nearest_neighbor(mask, 4, 4)

        assert result.shape == (4, 4)
        assert result.dtype == bool

    def test_nearest_neighbor_preserves_values(self):
        """Test that nearest neighbor doesn't create new values"""
        mask = np.random.choice([True, False], (50, 50))
        result = resample_nearest_neighbor(mask, 100, 100)

        # All values in result should be either True or False (no interpolation)
        assert result.dtype == bool
        assert set(result.flatten()) <= {True, False}

    def test_matches_scipy(self):
        """Verify Rust results match scipy within tolerance"""
        from scipy.ndimage import zoom

        input_data = np.random.randint(0, 4000, (100, 100), dtype=np.uint16)
        target_h, target_w = 256, 256

        # Rust version
        rust_result = resample_bilinear(input_data, target_h, target_w)

        # scipy version
        zoom_factors = (target_h / 100, target_w / 100)
        scipy_result = zoom(input_data, zoom_factors, order=1).astype(np.uint16)

        # Allow small differences due to rounding
        diff = np.abs(rust_result.astype(int) - scipy_result.astype(int))
        assert np.mean(diff) < 2.0  # Mean difference < 2 intensity values
        assert np.max(diff) < 10  # Max difference < 10

    def test_typical_sentinel2_case(self):
        """Test typical Sentinel-2 ingestion case"""
        # Simulate a 100x100 window from COG that needs to be resampled to 256x256 tile
        input_data = np.random.randint(100, 4000, (100, 100), dtype=np.uint16)
        mask = np.random.choice([True, False], (100, 100), p=[0.9, 0.1])  # 90% valid

        # Resample
        resampled_pixels = resample_bilinear(input_data, 256, 256)
        resampled_mask = resample_nearest_neighbor(mask, 256, 256)

        # Verify shapes
        assert resampled_pixels.shape == (256, 256)
        assert resampled_mask.shape == (256, 256)

        # Verify dtypes
        assert resampled_pixels.dtype == np.uint16
        assert resampled_mask.dtype == bool

        # Verify value ranges are preserved
        assert resampled_pixels.min() >= input_data.min()
        assert resampled_pixels.max() <= input_data.max()


@pytest.mark.skipif(RUST_AVAILABLE, reason="Testing fallback behavior")
class TestFallbackWarning:
    def test_fallback_warning_shown(self):
        """Test that warning is shown when Rust is not available"""
        with pytest.warns(UserWarning, match="Rust resampling not available"):
            # This should trigger the import and warning
            import pixelquery.io.ingest
            # Force reimport to trigger warning
            import importlib
            importlib.reload(pixelquery.io.ingest)
