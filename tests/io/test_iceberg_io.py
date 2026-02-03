"""
Tests for IcebergPixelWriter and IcebergPixelReader
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone

import numpy as np

from pixelquery.io.iceberg_writer import IcebergPixelWriter
from pixelquery.io.iceberg_reader import IcebergPixelReader


class TestIcebergPixelWriter:
    """Test IcebergPixelWriter class"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def writer(self, temp_warehouse):
        """Create IcebergPixelWriter instance"""
        return IcebergPixelWriter(temp_warehouse)

    @pytest.fixture
    def sample_observation(self):
        """Create sample observation data"""
        return {
            "tile_id": "x0024_y0041",
            "band": "red",
            "time": datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            "pixels": np.array([[100, 200], [300, 400]], dtype=np.uint16),
            "mask": np.array([[True, True], [False, True]], dtype=bool),
            "product_id": "sentinel2_l2a",
            "resolution": 10.0,
            "bounds": (126.5, 37.0, 127.5, 38.0),
            "cloud_cover": 0.1,
            "crs": "EPSG:32632",
        }

    def test_init(self, writer, temp_warehouse):
        """Test IcebergPixelWriter initialization"""
        assert writer.warehouse_path == Path(temp_warehouse)
        assert writer.storage is not None

    def test_write_observation_single(self, writer, sample_observation):
        """Test write_observation() for single observation"""
        snapshot_id = writer.write_observation(**sample_observation)

        assert snapshot_id > 0

    def test_write_observation_returns_snapshot_id(self, writer, sample_observation):
        """Test write_observation() returns snapshot ID"""
        snapshot_id1 = writer.write_observation(**sample_observation)
        snapshot_id2 = writer.write_observation(**sample_observation)

        assert snapshot_id1 > 0
        assert snapshot_id2 > snapshot_id1

    def test_write_observation_with_all_fields(self, writer):
        """Test write_observation() with all optional fields"""
        snapshot_id = writer.write_observation(
            tile_id="x0024_y0041",
            band="red",
            time=datetime(2024, 1, 15, tzinfo=timezone.utc),
            pixels=np.array([[100, 200], [300, 400]], dtype=np.uint16),
            mask=np.array([[True, True], [False, True]], dtype=bool),
            product_id="sentinel2_l2a",
            resolution=10.0,
            bounds=(126.5, 37.0, 127.5, 38.0),
            cloud_cover=0.1,
            crs="EPSG:32632",
            transform='{"a": 10.0, "b": 0.0, "c": 126.0, "d": 0.0, "e": -10.0, "f": 38.0}',
            source_file="/path/to/source.tif",
        )

        assert snapshot_id > 0

    def test_write_observation_minimal_fields(self, writer):
        """Test write_observation() with minimal required fields"""
        snapshot_id = writer.write_observation(
            tile_id="x0024_y0041",
            band="red",
            time=datetime(2024, 1, 15, tzinfo=timezone.utc),
            pixels=np.array([[100, 200], [300, 400]], dtype=np.uint16),
            mask=np.array([[True, True], [False, True]], dtype=bool),
            product_id="sentinel2_l2a",
            resolution=10.0,
        )

        assert snapshot_id > 0

    def test_write_observation_handles_naive_datetime(self, writer):
        """Test write_observation() handles naive datetime"""
        snapshot_id = writer.write_observation(
            tile_id="x0024_y0041",
            band="red",
            time=datetime(2024, 1, 15, 12, 0, 0),  # Naive datetime
            pixels=np.array([[100, 200], [300, 400]], dtype=np.uint16),
            mask=np.array([[True, True], [False, True]], dtype=bool),
            product_id="sentinel2_l2a",
            resolution=10.0,
        )

        assert snapshot_id > 0

    def test_write_observations_batch(self, writer, sample_observation):
        """Test write_observations() for batch write"""
        observations = [sample_observation.copy() for _ in range(3)]

        snapshot_id = writer.write_observations(observations)

        assert snapshot_id > 0

    def test_write_observations_empty_list(self, writer):
        """Test write_observations() with empty list"""
        snapshot_id = writer.write_observations([])

        assert snapshot_id == 0

    def test_write_observations_multiple_tiles(self, writer, sample_observation):
        """Test write_observations() with multiple tiles"""
        observations = []

        for tile_id in ["x0024_y0041", "x0024_y0042", "x0025_y0041"]:
            obs = sample_observation.copy()
            obs["tile_id"] = tile_id
            observations.append(obs)

        snapshot_id = writer.write_observations(observations)

        assert snapshot_id > 0

    def test_write_observations_multiple_bands(self, writer, sample_observation):
        """Test write_observations() with multiple bands"""
        observations = []

        for band in ["red", "green", "blue", "nir"]:
            obs = sample_observation.copy()
            obs["band"] = band
            observations.append(obs)

        snapshot_id = writer.write_observations(observations)

        assert snapshot_id > 0

    def test_get_snapshot_history(self, writer, sample_observation):
        """Test get_snapshot_history()"""
        # Initially empty
        history = writer.get_snapshot_history()
        assert history == []

        # After writes
        writer.write_observation(**sample_observation)
        writer.write_observation(**sample_observation)

        history = writer.get_snapshot_history()
        assert len(history) == 2

    def test_get_current_snapshot_id(self, writer, sample_observation):
        """Test get_current_snapshot_id()"""
        # Initially None
        snapshot_id = writer.get_current_snapshot_id()
        assert snapshot_id is None

        # After write
        written_id = writer.write_observation(**sample_observation)
        current_id = writer.get_current_snapshot_id()

        assert current_id == written_id


class TestIcebergPixelReader:
    """Test IcebergPixelReader class"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def reader(self, temp_warehouse):
        """Create IcebergPixelReader instance"""
        return IcebergPixelReader(temp_warehouse)

    @pytest.fixture
    def writer(self, temp_warehouse):
        """Create IcebergPixelWriter instance"""
        return IcebergPixelWriter(temp_warehouse)

    @pytest.fixture
    def sample_observation(self):
        """Create sample observation data"""
        return {
            "tile_id": "x0024_y0041",
            "band": "red",
            "time": datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            "pixels": np.array([[100, 200], [300, 400]], dtype=np.uint16),
            "mask": np.array([[True, True], [False, True]], dtype=bool),
            "product_id": "sentinel2_l2a",
            "resolution": 10.0,
            "bounds": (126.5, 37.0, 127.5, 38.0),
            "cloud_cover": 0.1,
        }

    def test_init(self, reader, temp_warehouse):
        """Test IcebergPixelReader initialization"""
        assert reader.warehouse_path == Path(temp_warehouse)
        assert reader.storage is not None

    def test_read_tile_empty(self, reader):
        """Test read_tile() on empty warehouse"""
        result = reader.read_tile("x0024_y0041")

        assert result == {}

    def test_read_tile_single_observation(self, reader, writer, sample_observation):
        """Test read_tile() for single observation"""
        writer.write_observation(**sample_observation)

        result = reader.read_tile("x0024_y0041")

        assert "red" in result
        band_data = result["red"]
        assert "times" in band_data
        assert "pixels" in band_data
        assert "masks" in band_data
        assert "metadata" in band_data

    def test_read_tile_multiple_bands(self, reader, writer, sample_observation):
        """Test read_tile() with multiple bands"""
        for band in ["red", "green", "blue"]:
            obs = sample_observation.copy()
            obs["band"] = band
            writer.write_observation(**obs)

        result = reader.read_tile("x0024_y0041")

        assert "red" in result
        assert "green" in result
        assert "blue" in result

    def test_read_tile_filter_bands(self, reader, writer, sample_observation):
        """Test read_tile() with bands filter"""
        for band in ["red", "green", "blue", "nir"]:
            obs = sample_observation.copy()
            obs["band"] = band
            writer.write_observation(**obs)

        result = reader.read_tile("x0024_y0041", bands=["red", "nir"])

        assert "red" in result
        assert "nir" in result
        assert "green" not in result
        assert "blue" not in result

    def test_read_tile_time_range(self, reader, writer, sample_observation):
        """Test read_tile() with time range filter"""
        # Write observations in different months
        for month in [1, 2, 3]:
            obs = sample_observation.copy()
            obs["time"] = datetime(2024, month, 15, tzinfo=timezone.utc)
            writer.write_observation(**obs)

        # Query only January
        result = reader.read_tile(
            "x0024_y0041",
            time_range=(
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 31, tzinfo=timezone.utc)
            )
        )

        assert "red" in result
        band_data = result["red"]
        assert len(band_data["times"]) == 1

    def test_read_tile_time_travel(self, reader, writer, sample_observation):
        """Test Time Travel query with snapshot_id"""
        # Write first observation
        obs1 = sample_observation.copy()
        snapshot_id1 = writer.write_observation(**obs1)

        # Write second observation with different band
        obs2 = sample_observation.copy()
        obs2["band"] = "nir"
        snapshot_id2 = writer.write_observation(**obs2)

        # Read at first snapshot - should only see red
        result = reader.read_tile("x0024_y0041", as_of_snapshot_id=snapshot_id1)
        assert "red" in result
        assert "nir" not in result

        # Read at second snapshot - should see both
        result = reader.read_tile("x0024_y0041", as_of_snapshot_id=snapshot_id2)
        assert "red" in result
        assert "nir" in result

    def test_read_tile_reshape(self, reader, writer, sample_observation):
        """Test read_tile() with reshape parameter"""
        writer.write_observation(**sample_observation)

        result = reader.read_tile("x0024_y0041", reshape=(2, 2))

        band_data = result["red"]
        pixels = band_data["pixels"][0]
        assert pixels.shape == (2, 2)

    def test_read_observation_single(self, reader, writer, sample_observation):
        """Test read_observation() for single observation"""
        time = sample_observation["time"]
        writer.write_observation(**sample_observation)

        result = reader.read_observation("x0024_y0041", "red", time)

        assert result is not None
        assert "pixels" in result
        assert "mask" in result
        assert "metadata" in result
        assert result["time"] == time

    def test_read_observation_not_found(self, reader):
        """Test read_observation() for non-existent observation"""
        result = reader.read_observation(
            "x0024_y0041",
            "red",
            datetime(2024, 1, 15, tzinfo=timezone.utc)
        )

        assert result is None

    def test_list_tiles_empty(self, reader):
        """Test list_tiles() on empty warehouse"""
        tiles = reader.list_tiles()

        assert tiles == []

    def test_list_tiles_multiple(self, reader, writer, sample_observation):
        """Test list_tiles() with multiple tiles"""
        for tile_id in ["x0024_y0041", "x0024_y0042", "x0025_y0041"]:
            obs = sample_observation.copy()
            obs["tile_id"] = tile_id
            writer.write_observation(**obs)

        tiles = reader.list_tiles()

        assert len(tiles) == 3
        assert "x0024_y0041" in tiles
        assert "x0024_y0042" in tiles
        assert "x0025_y0041" in tiles

    def test_list_tiles_time_range(self, reader, writer, sample_observation):
        """Test list_tiles() with time range filter"""
        # Write observations in different months
        obs1 = sample_observation.copy()
        obs1["time"] = datetime(2024, 1, 15, tzinfo=timezone.utc)
        writer.write_observation(**obs1)

        obs2 = sample_observation.copy()
        obs2["time"] = datetime(2024, 3, 15, tzinfo=timezone.utc)
        writer.write_observation(**obs2)

        # Query only January
        tiles = reader.list_tiles(
            time_range=(
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 31, tzinfo=timezone.utc)
            )
        )

        assert len(tiles) >= 1

    def test_list_tiles_time_travel(self, reader, writer, sample_observation):
        """Test list_tiles() with Time Travel"""
        # Write first tile
        obs1 = sample_observation.copy()
        obs1["tile_id"] = "x0024_y0041"
        snapshot_id1 = writer.write_observation(**obs1)

        # Write second tile
        obs2 = sample_observation.copy()
        obs2["tile_id"] = "x0024_y0042"
        snapshot_id2 = writer.write_observation(**obs2)

        # At first snapshot, only one tile
        tiles = reader.list_tiles(as_of_snapshot_id=snapshot_id1)
        assert len(tiles) == 1

        # At second snapshot, two tiles
        tiles = reader.list_tiles(as_of_snapshot_id=snapshot_id2)
        assert len(tiles) == 2

    def test_list_bands_empty(self, reader):
        """Test list_bands() on empty warehouse"""
        bands = reader.list_bands()

        assert bands == []

    def test_list_bands_multiple(self, reader, writer, sample_observation):
        """Test list_bands() with multiple bands"""
        for band in ["red", "green", "blue", "nir"]:
            obs = sample_observation.copy()
            obs["band"] = band
            writer.write_observation(**obs)

        bands = reader.list_bands()

        assert len(bands) == 4
        assert "red" in bands
        assert "nir" in bands

    def test_list_bands_by_tile(self, reader, writer, sample_observation):
        """Test list_bands() filtered by tile"""
        # Write different bands to different tiles
        obs1 = sample_observation.copy()
        obs1["tile_id"] = "x0024_y0041"
        obs1["band"] = "red"
        writer.write_observation(**obs1)

        obs2 = sample_observation.copy()
        obs2["tile_id"] = "x0025_y0041"
        obs2["band"] = "nir"
        writer.write_observation(**obs2)

        bands = reader.list_bands(tile_id="x0024_y0041")

        assert "red" in bands
        assert "nir" not in bands

    def test_list_time_range(self, reader, writer, sample_observation):
        """Test list_time_range()"""
        # Write observations at different times
        for day in [1, 15, 28]:
            obs = sample_observation.copy()
            obs["time"] = datetime(2024, 1, day, tzinfo=timezone.utc)
            writer.write_observation(**obs)

        time_range = reader.list_time_range("x0024_y0041", "red")

        assert time_range is not None
        start, end = time_range
        assert start.day == 1
        assert end.day == 28

    def test_list_time_range_not_found(self, reader):
        """Test list_time_range() for non-existent data"""
        time_range = reader.list_time_range("x0024_y0041", "red")

        assert time_range is None

    def test_count_observations(self, reader, writer, sample_observation):
        """Test count_observations()"""
        # Write multiple observations
        for _ in range(5):
            writer.write_observation(**sample_observation)

        count = reader.count_observations("x0024_y0041", "red")

        assert count == 5

    def test_count_observations_filters(self, reader, writer, sample_observation):
        """Test count_observations() with filters"""
        # Write observations in different months
        for month in [1, 2, 3]:
            obs = sample_observation.copy()
            obs["time"] = datetime(2024, month, 15, tzinfo=timezone.utc)
            writer.write_observation(**obs)

        # Count only January
        count = reader.count_observations(
            "x0024_y0041",
            "red",
            time_range=(
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 31, tzinfo=timezone.utc)
            )
        )

        assert count == 1


class TestIcebergIOIntegration:
    """Integration tests for IcebergPixelWriter and IcebergPixelReader"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_write_and_read_workflow(self, temp_warehouse):
        """Test complete write and read workflow"""
        writer = IcebergPixelWriter(temp_warehouse)
        reader = IcebergPixelReader(temp_warehouse)

        # Write observations
        observations = []
        for tile_id in ["x0024_y0041", "x0024_y0042"]:
            for month in [1, 2]:
                for band in ["red", "nir"]:
                    obs = {
                        "tile_id": tile_id,
                        "band": band,
                        "time": datetime(2024, month, 15, tzinfo=timezone.utc),
                        "pixels": np.array([[100, 200], [300, 400]], dtype=np.uint16),
                        "mask": np.array([[True, True], [False, True]], dtype=bool),
                        "product_id": "sentinel2_l2a",
                        "resolution": 10.0,
                    }
                    observations.append(obs)

        writer.write_observations(observations)

        # Read tiles
        tiles = reader.list_tiles()
        assert len(tiles) == 2

        # Read bands
        bands = reader.list_bands()
        assert len(bands) == 2

        # Read tile data
        result = reader.read_tile("x0024_y0041", bands=["red"])
        assert "red" in result
        assert len(result["red"]["times"]) == 2  # Two months

    def test_time_travel_workflow(self, temp_warehouse):
        """Test Time Travel workflow with writer and reader"""
        writer = IcebergPixelWriter(temp_warehouse)
        reader = IcebergPixelReader(temp_warehouse)

        # Write observations at different snapshots
        obs1 = {
            "tile_id": "x0024_y0041",
            "band": "red",
            "time": datetime(2024, 1, 15, tzinfo=timezone.utc),
            "pixels": np.array([[100, 200], [300, 400]], dtype=np.uint16),
            "mask": np.array([[True, True], [False, True]], dtype=bool),
            "product_id": "sentinel2_l2a",
            "resolution": 10.0,
        }
        snapshot_id1 = writer.write_observation(**obs1)

        obs2 = obs1.copy()
        obs2["band"] = "nir"
        snapshot_id2 = writer.write_observation(**obs2)

        obs3 = obs1.copy()
        obs3["tile_id"] = "x0024_y0042"
        snapshot_id3 = writer.write_observation(**obs3)

        # Time Travel queries
        tiles_at_snap1 = reader.list_tiles(as_of_snapshot_id=snapshot_id1)
        assert len(tiles_at_snap1) == 1

        tiles_at_snap2 = reader.list_tiles(as_of_snapshot_id=snapshot_id2)
        assert len(tiles_at_snap2) == 1

        tiles_at_snap3 = reader.list_tiles(as_of_snapshot_id=snapshot_id3)
        assert len(tiles_at_snap3) == 2

        # Read data at specific snapshot
        result = reader.read_tile("x0024_y0041", as_of_snapshot_id=snapshot_id1)
        assert "red" in result
        assert "nir" not in result

        result = reader.read_tile("x0024_y0041", as_of_snapshot_id=snapshot_id2)
        assert "red" in result
        assert "nir" in result
