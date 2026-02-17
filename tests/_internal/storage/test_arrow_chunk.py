"""
Tests for Arrow IPC chunk storage
"""

import shutil
import tempfile
from datetime import UTC, datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from pixelquery._internal.storage.arrow_chunk import ArrowChunkReader, ArrowChunkWriter


class TestArrowChunkWriter:
    """Test Arrow IPC chunk writer"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir)

    @pytest.fixture
    def sample_data(self):
        """Create sample spatiotemporal data"""
        return {
            "time": [
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 15, tzinfo=UTC),
            ],
            "pixels": [
                np.array([1000, 1100, 1200], dtype=np.uint16),
                np.array([1050, 1150, 1250], dtype=np.uint16),
            ],
            "mask": [
                np.array([False, False, True], dtype=bool),
                np.array([True, False, False], dtype=bool),
            ],
        }

    @pytest.fixture
    def writer(self):
        """Create writer instance"""
        return ArrowChunkWriter()

    def test_init(self, writer):
        """Test writer initialization"""
        assert writer is not None
        assert writer.SCHEMA is not None

    def test_write_chunk_basic(self, writer, temp_dir, sample_data):
        """Test basic chunk writing"""
        path = str(Path(temp_dir) / "test_chunk.arrow")

        writer.write_chunk(path=path, data=sample_data, product_id="sentinel2_l2a", resolution=10.0)

        assert Path(path).exists()
        assert Path(path).stat().st_size > 0

    def test_write_chunk_with_metadata(self, writer, temp_dir, sample_data):
        """Test chunk writing with custom metadata"""
        path = str(Path(temp_dir) / "test_chunk.arrow")

        writer.write_chunk(
            path=path,
            data=sample_data,
            product_id="sentinel2_l2a",
            resolution=10.0,
            metadata={"tile_id": "x0024_y0041", "year_month": "2024-01"},
        )

        assert Path(path).exists()

    def test_write_chunk_creates_directories(self, writer, temp_dir, sample_data):
        """Test that writer creates parent directories"""
        path = str(Path(temp_dir) / "nested" / "path" / "chunk.arrow")

        writer.write_chunk(path=path, data=sample_data, product_id="sentinel2_l2a", resolution=10.0)

        assert Path(path).exists()
        assert Path(path).parent.exists()

    def test_validate_data_missing_keys(self, writer):
        """Test validation catches missing keys"""
        invalid_data = {"time": [], "pixels": []}  # Missing 'mask'

        with pytest.raises(ValueError, match="Missing required keys"):
            writer._validate_data(invalid_data)

    def test_validate_data_length_mismatch(self, writer):
        """Test validation catches length mismatches"""
        invalid_data = {
            "time": [datetime(2024, 1, 1, tzinfo=UTC)],
            "pixels": [np.array([1000])],
            "mask": [np.array([False]), np.array([True])],  # Too many masks
        }

        with pytest.raises(ValueError, match="mask length"):
            writer._validate_data(invalid_data)


class TestArrowChunkReader:
    """Test Arrow IPC chunk reader"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir)

    @pytest.fixture
    def sample_chunk(self, temp_dir):
        """Create a sample chunk file"""
        writer = ArrowChunkWriter()
        path = str(Path(temp_dir) / "test_chunk.arrow")

        data = {
            "time": [
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 15, tzinfo=UTC),
            ],
            "pixels": [
                np.array([1000, 1100, 1200, 1300], dtype=np.uint16),
                np.array([1050, 1150, 1250, 1350], dtype=np.uint16),
            ],
            "mask": [
                np.array([False, False, True, False], dtype=bool),
                np.array([True, False, False, True], dtype=bool),
            ],
        }

        writer.write_chunk(
            path=path,
            data=data,
            product_id="sentinel2_l2a",
            resolution=10.0,
            metadata={"tile_id": "x0024_y0041"},
        )

        return path

    @pytest.fixture
    def reader(self):
        """Create reader instance"""
        return ArrowChunkReader()

    def test_init(self, reader):
        """Test reader initialization"""
        assert reader is not None

    def test_read_chunk_basic(self, reader, sample_chunk):
        """Test basic chunk reading"""
        data, _metadata = reader.read_chunk(sample_chunk)

        assert "time" in data
        assert "pixels" in data
        assert "mask" in data

        assert len(data["time"]) == 2
        assert len(data["pixels"]) == 2
        assert len(data["mask"]) == 2

    def test_read_chunk_metadata(self, reader, sample_chunk):
        """Test reading chunk metadata"""
        _data, metadata = reader.read_chunk(sample_chunk)

        assert "product_id" in metadata
        assert metadata["product_id"] == "sentinel2_l2a"
        assert metadata["resolution"] == "10.0"
        assert "tile_id" in metadata
        assert metadata["tile_id"] == "x0024_y0041"

    def test_read_chunk_pixel_values(self, reader, sample_chunk):
        """Test pixel values are preserved"""
        data, _metadata = reader.read_chunk(sample_chunk)

        # Check first observation
        assert np.array_equal(data["pixels"][0], np.array([1000, 1100, 1200, 1300]))

        # Check second observation
        assert np.array_equal(data["pixels"][1], np.array([1050, 1150, 1250, 1350]))

    def test_read_chunk_mask_values(self, reader, sample_chunk):
        """Test mask values are preserved"""
        data, _metadata = reader.read_chunk(sample_chunk)

        # Check first mask
        assert np.array_equal(data["mask"][0], np.array([False, False, True, False]))

        # Check second mask
        assert np.array_equal(data["mask"][1], np.array([True, False, False, True]))

    def test_read_chunk_with_reshape(self, reader, temp_dir):
        """Test reading with reshape to 2D arrays"""
        writer = ArrowChunkWriter()
        path = str(Path(temp_dir) / "reshape_test.arrow")

        # Create 2x2 pixel data (flattened)
        data = {
            "time": [datetime(2024, 1, 1, tzinfo=UTC)],
            "pixels": [np.array([100, 200, 300, 400], dtype=np.uint16)],
            "mask": [np.array([False, False, True, True], dtype=bool)],
        }

        writer.write_chunk(path=path, data=data, product_id="test", resolution=10.0)

        # Read with reshape
        data_read, _ = reader.read_chunk(path, reshape=(2, 2))

        assert data_read["pixels"][0].shape == (2, 2)
        assert data_read["mask"][0].shape == (2, 2)

        # Check values after reshape
        expected_pixels = np.array([[100, 200], [300, 400]], dtype=np.uint16)
        assert np.array_equal(data_read["pixels"][0], expected_pixels)

    def test_read_chunk_not_found(self, reader):
        """Test error when chunk file not found"""
        with pytest.raises(FileNotFoundError):
            reader.read_chunk("/nonexistent/path.arrow")

    def test_read_chunk_metadata_only(self, reader, sample_chunk):
        """Test reading only metadata without data"""
        metadata = reader.read_chunk_metadata(sample_chunk)

        assert "product_id" in metadata
        assert metadata["product_id"] == "sentinel2_l2a"
        assert metadata["resolution"] == "10.0"


class TestArrowChunkAppend:
    """Test append_to_chunk optimization"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir)

    def test_append_to_new_chunk(self, temp_dir):
        """Test appending creates new file if it doesn't exist"""
        writer = ArrowChunkWriter()
        reader = ArrowChunkReader()
        path = str(Path(temp_dir) / "append_new.arrow")

        # Append to non-existent file (should create it)
        data = {
            "time": [datetime(2024, 1, 1, tzinfo=UTC)],
            "pixels": [np.array([1000, 2000], dtype=np.uint16)],
            "mask": [np.array([False, True], dtype=bool)],
        }

        writer.append_to_chunk(path=path, data=data, product_id="test", resolution=10.0)

        assert Path(path).exists()

        # Verify data
        read_data, _metadata = reader.read_chunk(path)
        assert len(read_data["time"]) == 1
        assert np.array_equal(read_data["pixels"][0], data["pixels"][0])

    def test_append_to_existing_chunk(self, temp_dir):
        """Test appending to existing chunk"""
        writer = ArrowChunkWriter()
        reader = ArrowChunkReader()
        path = str(Path(temp_dir) / "append_existing.arrow")

        # First write
        data1 = {
            "time": [datetime(2024, 1, 1, tzinfo=UTC)],
            "pixels": [np.array([1000, 2000], dtype=np.uint16)],
            "mask": [np.array([False, True], dtype=bool)],
        }

        writer.write_chunk(path=path, data=data1, product_id="test", resolution=10.0)

        # Append second observation
        data2 = {
            "time": [datetime(2024, 1, 15, tzinfo=UTC)],
            "pixels": [np.array([1500, 2500], dtype=np.uint16)],
            "mask": [np.array([True, False], dtype=bool)],
        }

        writer.append_to_chunk(path=path, data=data2, product_id="test", resolution=10.0)

        # Verify both observations exist
        read_data, _metadata = reader.read_chunk(path)
        assert len(read_data["time"]) == 2
        assert np.array_equal(read_data["pixels"][0], data1["pixels"][0])
        assert np.array_equal(read_data["pixels"][1], data2["pixels"][0])
        assert np.array_equal(read_data["mask"][0], data1["mask"][0])
        assert np.array_equal(read_data["mask"][1], data2["mask"][0])

    def test_append_multiple_times(self, temp_dir):
        """Test multiple appends to same chunk"""
        writer = ArrowChunkWriter()
        reader = ArrowChunkReader()
        path = str(Path(temp_dir) / "append_multiple.arrow")

        # Append 5 observations one by one
        n_obs = 5
        for i in range(n_obs):
            data = {
                "time": [datetime(2024, 1, i + 1, tzinfo=UTC)],
                "pixels": [np.array([i * 100, i * 200], dtype=np.uint16)],
                "mask": [np.array([i % 2 == 0, i % 2 == 1], dtype=bool)],
            }

            writer.append_to_chunk(path=path, data=data, product_id="test", resolution=10.0)

        # Verify all observations
        read_data, metadata = reader.read_chunk(path)
        assert len(read_data["time"]) == n_obs

        # Check metadata was updated
        assert metadata["num_observations"] == str(n_obs)
        assert "last_updated" in metadata

    def test_append_preserves_metadata(self, temp_dir):
        """Test that append preserves existing metadata"""
        writer = ArrowChunkWriter()
        reader = ArrowChunkReader()
        path = str(Path(temp_dir) / "append_metadata.arrow")

        # First write with custom metadata
        data1 = {
            "time": [datetime(2024, 1, 1, tzinfo=UTC)],
            "pixels": [np.array([1000], dtype=np.uint16)],
            "mask": [np.array([False], dtype=bool)],
        }

        writer.write_chunk(
            path=path,
            data=data1,
            product_id="sentinel2_l2a",
            resolution=10.0,
            metadata={"band": "red", "tile_id": "x0024_y0041"},
        )

        # Append second observation
        data2 = {
            "time": [datetime(2024, 1, 15, tzinfo=UTC)],
            "pixels": [np.array([2000], dtype=np.uint16)],
            "mask": [np.array([True], dtype=bool)],
        }

        writer.append_to_chunk(
            path=path,
            data=data2,
            product_id="sentinel2_l2a",
            resolution=10.0,
            metadata={"band": "red"},
        )

        # Verify metadata is preserved
        _, metadata = reader.read_chunk(path)
        assert metadata["product_id"] == "sentinel2_l2a"
        assert metadata["band"] == "red"
        assert metadata["tile_id"] == "x0024_y0041"  # Original metadata preserved


class TestArrowChunkRoundtrip:
    """Test roundtrip: write then read"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir)

    def test_roundtrip_simple(self, temp_dir):
        """Test simple write-read roundtrip"""
        writer = ArrowChunkWriter()
        reader = ArrowChunkReader()
        path = str(Path(temp_dir) / "roundtrip.arrow")

        # Original data
        original_data = {
            "time": [datetime(2024, 1, 1, tzinfo=UTC)],
            "pixels": [np.array([1000, 2000, 3000], dtype=np.uint16)],
            "mask": [np.array([False, True, False], dtype=bool)],
        }

        # Write
        writer.write_chunk(path=path, data=original_data, product_id="test", resolution=10.0)

        # Read
        read_data, _metadata = reader.read_chunk(path)

        # Verify
        assert len(read_data["time"]) == len(original_data["time"])
        assert np.array_equal(read_data["pixels"][0], original_data["pixels"][0])
        assert np.array_equal(read_data["mask"][0], original_data["mask"][0])

    def test_roundtrip_multiple_observations(self, temp_dir):
        """Test roundtrip with multiple observations"""
        writer = ArrowChunkWriter()
        reader = ArrowChunkReader()
        path = str(Path(temp_dir) / "roundtrip_multi.arrow")

        # Original data with 5 observations
        n_obs = 5
        original_data = {
            "time": [datetime(2024, 1, i + 1, tzinfo=UTC) for i in range(n_obs)],
            "pixels": [
                np.random.randint(0, 10000, size=100, dtype=np.uint16) for _ in range(n_obs)
            ],
            "mask": [np.random.rand(100) > 0.5 for _ in range(n_obs)],
        }

        # Write
        writer.write_chunk(path=path, data=original_data, product_id="test", resolution=10.0)

        # Read
        read_data, _metadata = reader.read_chunk(path)

        # Verify all observations
        assert len(read_data["time"]) == n_obs
        for i in range(n_obs):
            assert np.array_equal(read_data["pixels"][i], original_data["pixels"][i])
            assert np.array_equal(read_data["mask"][i], original_data["mask"][i])
