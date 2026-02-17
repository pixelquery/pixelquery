"""
Tests for Rust Arrow chunk I/O integration

Tests the pixelquery_core.arrow_* functions and their integration
with ArrowChunkWriter/ArrowChunkReader.
"""

import os
import tempfile
from datetime import UTC, datetime, timezone
from pathlib import Path

import numpy as np
import pytest

# Check if Rust extensions are available
try:
    from pixelquery_core import arrow_append_to_chunk, arrow_read_chunk, arrow_write_chunk

    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extensions not built")
class TestRustArrowWrite:
    """Test Rust arrow_write_chunk function"""

    def test_write_basic(self, tmp_path):
        """Test basic write operation"""
        path = str(tmp_path / "test.arrow")
        times_ms = [1704067200000]  # 2024-01-01
        pixels = [[1000, 1100, 1200, 1300]]
        masks = [[False, False, True, False]]
        metadata = {"product_id": "test", "resolution": "10.0"}

        arrow_write_chunk(path, times_ms, pixels, masks, metadata)

        assert Path(path).exists()
        assert Path(path).stat().st_size > 0

    def test_write_multiple_observations(self, tmp_path):
        """Test write with multiple observations"""
        path = str(tmp_path / "test.arrow")
        times_ms = [1704067200000, 1704153600000, 1704240000000]
        pixels = [[1000, 1100], [2000, 2100], [3000, 3100]]
        masks = [[False, True], [True, False], [False, False]]
        metadata = {"product_id": "test", "resolution": "10.0"}

        arrow_write_chunk(path, times_ms, pixels, masks, metadata)

        # Verify by reading back
        read_times, read_pixels, read_masks, _read_meta = arrow_read_chunk(path)
        assert len(read_times) == 3
        assert read_times == times_ms
        assert read_pixels == pixels
        assert read_masks == masks

    def test_write_creates_directories(self, tmp_path):
        """Test that write creates parent directories"""
        path = str(tmp_path / "nested" / "dirs" / "test.arrow")
        times_ms = [1704067200000]
        pixels = [[1000]]
        masks = [[False]]
        metadata = {"product_id": "test"}

        arrow_write_chunk(path, times_ms, pixels, masks, metadata)

        assert Path(path).exists()


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extensions not built")
class TestRustArrowAppend:
    """Test Rust arrow_append_to_chunk function"""

    def test_append_to_existing(self, tmp_path):
        """Test append to existing file"""
        path = str(tmp_path / "test.arrow")
        metadata = {"product_id": "test", "resolution": "10.0"}

        # Initial write
        arrow_write_chunk(path, [1704067200000], [[1000, 1100]], [[False, False]], metadata)

        # Append
        arrow_append_to_chunk(path, [1704153600000], [[2000, 2100]], [[True, False]], metadata)

        # Verify
        read_times, read_pixels, _read_masks, _read_meta = arrow_read_chunk(path)
        assert len(read_times) == 2
        assert read_times[0] == 1704067200000
        assert read_times[1] == 1704153600000
        assert read_pixels[0] == [1000, 1100]
        assert read_pixels[1] == [2000, 2100]

    def test_append_creates_new_file(self, tmp_path):
        """Test append creates file if not exists"""
        path = str(tmp_path / "new.arrow")
        metadata = {"product_id": "test", "resolution": "10.0"}

        # Append to non-existent file
        arrow_append_to_chunk(path, [1704067200000], [[1000, 1100]], [[False, False]], metadata)

        assert Path(path).exists()
        read_times, _, _, _ = arrow_read_chunk(path)
        assert len(read_times) == 1

    def test_multiple_appends(self, tmp_path):
        """Test multiple consecutive appends"""
        path = str(tmp_path / "test.arrow")
        metadata = {"product_id": "test", "resolution": "10.0"}

        # Initial write
        arrow_write_chunk(path, [1704067200000], [[1000]], [[False]], metadata)

        # Multiple appends
        for i in range(1, 10):
            arrow_append_to_chunk(
                path, [1704067200000 + i * 86400000], [[1000 + i * 100]], [[i % 2 == 0]], metadata
            )

        # Verify
        read_times, _read_pixels, _read_masks, _read_meta = arrow_read_chunk(path)
        assert len(read_times) == 10


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extensions not built")
class TestRustArrowRead:
    """Test Rust arrow_read_chunk function"""

    def test_read_basic(self, tmp_path):
        """Test basic read operation"""
        path = str(tmp_path / "test.arrow")
        times_ms = [1704067200000, 1704153600000]
        pixels = [[1000, 1100], [2000, 2100]]
        masks = [[False, True], [True, False]]
        metadata = {"product_id": "test", "resolution": "10.0"}

        arrow_write_chunk(path, times_ms, pixels, masks, metadata)

        read_times, read_pixels, read_masks, read_meta = arrow_read_chunk(path)

        assert read_times == times_ms
        assert read_pixels == pixels
        assert read_masks == masks
        assert read_meta["product_id"] == "test"
        assert read_meta["resolution"] == "10.0"
        assert "num_observations" in read_meta
        assert read_meta["num_observations"] == "2"

    def test_read_metadata_preserved(self, tmp_path):
        """Test that metadata is preserved through write/read cycle"""
        path = str(tmp_path / "test.arrow")
        metadata = {
            "product_id": "sentinel2",
            "resolution": "10.0",
            "custom_key": "custom_value",
        }

        arrow_write_chunk(path, [1704067200000], [[1000]], [[False]], metadata)

        _, _, _, read_meta = arrow_read_chunk(path)

        assert read_meta["product_id"] == "sentinel2"
        assert read_meta["resolution"] == "10.0"
        assert read_meta["custom_key"] == "custom_value"


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extensions not built")
class TestRustPythonInteroperability:
    """Test that Rust-written files can be read by Python and vice versa"""

    def test_rust_write_python_read(self, tmp_path):
        """Test that Python can read Rust-written files"""
        from pixelquery._internal.storage.arrow_chunk import ArrowChunkReader

        path = str(tmp_path / "rust_written.arrow")
        times_ms = [1704067200000, 1704153600000]
        pixels = [[1000, 1100, 1200, 1300], [2000, 2100, 2200, 2300]]
        masks = [[False, False, True, False], [True, False, False, False]]
        metadata = {"product_id": "test", "resolution": "10.0"}

        # Write with Rust
        arrow_write_chunk(path, times_ms, pixels, masks, metadata)

        # Read with Python
        reader = ArrowChunkReader()
        data, meta = reader.read_chunk(path)

        assert len(data["time"]) == 2
        assert meta["product_id"] == "test"

    def test_python_write_rust_read(self, tmp_path):
        """Test that Rust can read Python-written files"""
        import pixelquery._internal.storage.arrow_chunk as arrow_module
        from pixelquery._internal.storage.arrow_chunk import RUST_ARROW_AVAILABLE, ArrowChunkWriter

        # Temporarily disable Rust for write
        original_flag = arrow_module.RUST_ARROW_AVAILABLE
        arrow_module.RUST_ARROW_AVAILABLE = False

        try:
            path = str(tmp_path / "python_written.arrow")
            writer = ArrowChunkWriter()

            data = {
                "time": [datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 2, tzinfo=UTC)],
                "pixels": [
                    np.array([1000, 1100, 1200, 1300], dtype=np.uint16),
                    np.array([2000, 2100, 2200, 2300], dtype=np.uint16),
                ],
                "mask": [
                    np.array([False, False, True, False]),
                    np.array([True, False, False, False]),
                ],
            }

            writer.write_chunk(path, data, product_id="test", resolution=10.0)

            # Read with Rust
            read_times, read_pixels, _read_masks, _read_meta = arrow_read_chunk(path)

            assert len(read_times) == 2
            assert read_pixels[0] == [1000, 1100, 1200, 1300]
        finally:
            # Restore original flag
            arrow_module.RUST_ARROW_AVAILABLE = original_flag


@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extensions not built")
class TestRustArrowPerformance:
    """Performance sanity checks for Rust Arrow functions"""

    def test_large_array_performance(self, tmp_path):
        """Test that large arrays can be written efficiently"""
        import time

        path = str(tmp_path / "large.arrow")
        pixel_size = 256 * 256  # 65536 pixels

        times_ms = [1704067200000 + i * 86400000 for i in range(5)]
        pixels = [list(range(pixel_size)) for _ in range(5)]
        masks = [[i % 2 == 0 for i in range(pixel_size)] for _ in range(5)]
        metadata = {"product_id": "test", "resolution": "10.0"}

        start = time.perf_counter()
        arrow_write_chunk(path, times_ms, pixels, masks, metadata)
        elapsed = time.perf_counter() - start

        # Should complete in reasonable time (< 1 second for this size)
        assert elapsed < 1.0, f"Write took too long: {elapsed:.2f}s"

        # Verify file was written
        assert Path(path).exists()
        file_size = Path(path).stat().st_size
        assert file_size > 0, "File is empty"
