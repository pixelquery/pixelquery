"""
Tests for COGReader
"""

import shutil
import tempfile
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds
from rasterio.windows import Window

from pixelquery.io import COGReader


class TestCOGReader:
    """Test COGReader class"""

    @pytest.fixture
    def mock_cog_file(self):
        """Create small mock COG file"""
        tmpdir = tempfile.mkdtemp()
        cog_path = Path(tmpdir) / "test.tif"

        # Create 100x100 pixel COG
        width, height = 100, 100
        bounds = (126.5, 37.0, 126.6, 37.1)

        transform = from_bounds(*bounds, width, height)

        # Create 4-band array
        data = np.random.randint(0, 4000, (4, height, width), dtype=np.uint16)

        # Write COG
        with rasterio.open(
            cog_path,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=4,
            dtype=np.uint16,
            crs="EPSG:4326",
            transform=transform,
            nodata=0,
        ) as dst:
            dst.write(data)

        yield str(cog_path)

        # Cleanup
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_init(self, mock_cog_file):
        """Test COGReader initialization"""
        reader = COGReader(mock_cog_file)

        assert reader.file_path == mock_cog_file
        assert reader.dataset is not None
        assert not reader.dataset.closed

        reader.close()

    def test_context_manager(self, mock_cog_file):
        """Test context manager support"""
        with COGReader(mock_cog_file) as reader:
            assert not reader.dataset.closed

        assert reader.dataset.closed

    def test_read_band(self, mock_cog_file):
        """Test reading a single band"""
        with COGReader(mock_cog_file) as reader:
            band_data = reader.read_band(1)

            assert isinstance(band_data, np.ndarray)
            assert band_data.shape == (100, 100)
            assert band_data.dtype == np.uint16

    def test_read_multiple_bands(self, mock_cog_file):
        """Test reading multiple bands"""
        with COGReader(mock_cog_file) as reader:
            band1 = reader.read_band(1)
            band2 = reader.read_band(2)
            band3 = reader.read_band(3)
            band4 = reader.read_band(4)

            assert band1.shape == band2.shape == band3.shape == band4.shape
            assert band1.shape == (100, 100)

    def test_read_window(self, mock_cog_file):
        """Test reading a window"""
        with COGReader(mock_cog_file) as reader:
            window = Window(0, 0, 50, 50)  # col_off, row_off, width, height
            data = reader.read_window(window, band_index=1)

            assert data.shape == (50, 50)
            assert data.dtype == np.uint16

    def test_get_metadata(self, mock_cog_file):
        """Test metadata extraction"""
        with COGReader(mock_cog_file) as reader:
            metadata = reader.get_metadata()

            assert "crs" in metadata
            assert "transform" in metadata
            assert "bounds" in metadata
            assert "width" in metadata
            assert "height" in metadata
            assert "count" in metadata
            assert "dtype" in metadata
            assert "nodata" in metadata

            assert metadata["width"] == 100
            assert metadata["height"] == 100
            assert metadata["count"] == 4
            assert metadata["nodata"] == 0

    def test_get_bounds(self, mock_cog_file):
        """Test bounds extraction"""
        with COGReader(mock_cog_file) as reader:
            bounds = reader.get_bounds()

            assert len(bounds) == 4
            minx, miny, maxx, maxy = bounds

            # Check bounds are reasonable (WGS84)
            assert 126.5 <= minx < maxx <= 126.6
            assert 37.0 <= miny < maxy <= 37.1

    def test_get_bounds_wgs84(self, mock_cog_file):
        """Test bounds in WGS84"""
        with COGReader(mock_cog_file) as reader:
            bounds = reader.get_bounds(target_crs="EPSG:4326")

            minx, miny, maxx, maxy = bounds
            assert 126.5 <= minx < maxx <= 126.6
            assert 37.0 <= miny < maxy <= 37.1

    def test_get_resolution(self, mock_cog_file):
        """Test resolution extraction"""
        with COGReader(mock_cog_file) as reader:
            resolution = reader.get_resolution()

            assert isinstance(resolution, float)
            assert resolution > 0

            # For 0.1 degree extent over 100 pixels
            # 0.1 degree ≈ 11,132 meters at 37° latitude
            # Resolution ≈ 11,132 / 100 ≈ 111 meters
            assert 50 < resolution < 200  # Reasonable range

    def test_get_mask(self, mock_cog_file):
        """Test mask generation"""
        with COGReader(mock_cog_file) as reader:
            mask = reader.get_mask(1)

            assert isinstance(mask, np.ndarray)
            assert mask.shape == (100, 100)
            assert mask.dtype == bool

            # Most pixels should be valid (nodata=0, data is 0-4000)
            assert mask.sum() > 0

    def test_get_mask_nodata(self):
        """Test mask with actual nodata values"""
        tmpdir = tempfile.mkdtemp()
        cog_path = Path(tmpdir) / "test_nodata.tif"

        try:
            # Create COG with nodata values
            width, height = 50, 50
            bounds = (126.5, 37.0, 126.6, 37.1)
            transform = from_bounds(*bounds, width, height)

            # Create data with some nodata (0) pixels
            data = np.random.randint(1, 1000, (1, height, width), dtype=np.uint16)
            data[0, :10, :10] = 0  # Set corner to nodata

            with rasterio.open(
                cog_path,
                "w",
                driver="GTiff",
                height=height,
                width=width,
                count=1,
                dtype=np.uint16,
                crs="EPSG:4326",
                transform=transform,
                nodata=0,
            ) as dst:
                dst.write(data)

            # Read and check mask
            with COGReader(str(cog_path)) as reader:
                mask = reader.get_mask(1)

                # Corner should be masked out
                assert not mask[0, 0]
                assert not mask[5, 5]

                # Other areas should be valid
                assert mask[20, 20]
                assert mask[40, 40]

        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_repr(self, mock_cog_file):
        """Test string representation"""
        reader = COGReader(mock_cog_file)
        repr_str = repr(reader)

        assert "COGReader" in repr_str
        assert mock_cog_file in repr_str
        assert "100 x 100" in repr_str
        assert "Bands: 4" in repr_str

        reader.close()

        repr_str_closed = repr(reader)
        assert "closed" in repr_str_closed


class TestCOGReaderProjected:
    """Test COGReader with projected CRS"""

    @pytest.fixture
    def projected_cog_file(self):
        """Create COG in projected CRS (UTM)"""
        tmpdir = tempfile.mkdtemp()
        cog_path = Path(tmpdir) / "projected.tif"

        # UTM zone 33N bounds (in meters)
        width, height = 100, 100
        bounds = (500000, 4100000, 510000, 4110000)  # 10km x 10km

        transform = from_bounds(*bounds, width, height)

        data = np.random.randint(0, 1000, (1, height, width), dtype=np.uint16)

        with rasterio.open(
            cog_path,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=1,
            dtype=np.uint16,
            crs="EPSG:32633",  # UTM 33N
            transform=transform,
        ) as dst:
            dst.write(data)

        yield str(cog_path)
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_projected_resolution(self, projected_cog_file):
        """Test resolution for projected CRS"""
        with COGReader(projected_cog_file) as reader:
            resolution = reader.get_resolution()

            # 10,000 meters / 100 pixels = 100 meters/pixel
            assert 99 < resolution < 101

    def test_projected_to_wgs84_bounds(self, projected_cog_file):
        """Test bounds transformation to WGS84"""
        with COGReader(projected_cog_file) as reader:
            bounds = reader.get_bounds(target_crs="EPSG:4326")

            minx, miny, maxx, maxy = bounds

            # Should be in WGS84 range
            assert -180 <= minx < maxx <= 180
            assert -90 <= miny < maxy <= 90


class TestCOGReaderErrors:
    """Test error handling"""

    def test_file_not_found(self):
        """Test error when file doesn't exist"""
        with pytest.raises(rasterio.errors.RasterioIOError):
            COGReader("nonexistent.tif")

    def test_invalid_band_index(self, mock_cog_file=None):
        """Test error when reading invalid band"""
        tmpdir = tempfile.mkdtemp()
        cog_path = Path(tmpdir) / "test.tif"

        try:
            # Create 1-band COG
            width, height = 10, 10
            bounds = (126.5, 37.0, 126.6, 37.1)
            transform = from_bounds(*bounds, width, height)
            data = np.random.randint(0, 100, (1, height, width), dtype=np.uint16)

            with rasterio.open(
                cog_path,
                "w",
                driver="GTiff",
                height=height,
                width=width,
                count=1,
                dtype=np.uint16,
                crs="EPSG:4326",
                transform=transform,
            ) as dst:
                dst.write(data)

            with COGReader(str(cog_path)) as reader, pytest.raises(IndexError):
                # Try to read band 2 (doesn't exist)
                reader.read_band(2)

        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
