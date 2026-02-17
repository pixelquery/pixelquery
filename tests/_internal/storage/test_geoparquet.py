"""
Tests for GeoParquet tile metadata storage
"""

import shutil
import tempfile
from pathlib import Path

import pytest

from pixelquery._internal.storage.geoparquet import GeoParquetReader, GeoParquetWriter, TileMetadata


class TestTileMetadata:
    """Test TileMetadata dataclass"""

    def test_create_metadata(self):
        """Test creating TileMetadata"""
        meta = TileMetadata(
            tile_id="x0024_y0041",
            year_month="2024-01",
            band="red",
            bounds=(127.049, 37.546, 127.072, 37.569),
            num_observations=2,
            min_value=100.0,
            max_value=10000.0,
            mean_value=2500.0,
            cloud_cover=5.0,
            product_id="sentinel2_l2a",
            resolution=10.0,
            chunk_path="data/x0024_y0041/2024-01/red.arrow",
        )

        assert meta.tile_id == "x0024_y0041"
        assert meta.year_month == "2024-01"
        assert meta.band == "red"
        assert meta.num_observations == 2


class TestGeoParquetWriter:
    """Test GeoParquet writer"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir)

    @pytest.fixture
    def sample_metadata(self):
        """Create sample metadata"""
        return [
            TileMetadata(
                tile_id="x0024_y0041",
                year_month="2024-01",
                band="red",
                bounds=(127.049, 37.546, 127.072, 37.569),
                num_observations=2,
                min_value=100.0,
                max_value=10000.0,
                mean_value=2500.0,
                cloud_cover=5.0,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path="data/x0024_y0041/2024-01/red.arrow",
            ),
            TileMetadata(
                tile_id="x0024_y0041",
                year_month="2024-01",
                band="nir",
                bounds=(127.049, 37.546, 127.072, 37.569),
                num_observations=2,
                min_value=200.0,
                max_value=9000.0,
                mean_value=4500.0,
                cloud_cover=5.0,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path="data/x0024_y0041/2024-01/nir.arrow",
            ),
        ]

    @pytest.fixture
    def writer(self):
        """Create writer instance"""
        return GeoParquetWriter()

    def test_init(self, writer):
        """Test writer initialization"""
        assert writer is not None

    def test_write_metadata_basic(self, writer, temp_dir, sample_metadata):
        """Test basic metadata writing"""
        path = str(Path(temp_dir) / "tiles.parquet")

        writer.write_metadata(sample_metadata, path, mode="overwrite")

        assert Path(path).exists()
        assert Path(path).stat().st_size > 0

    def test_write_metadata_creates_directories(self, writer, temp_dir, sample_metadata):
        """Test that writer creates parent directories"""
        path = str(Path(temp_dir) / "nested" / "path" / "tiles.parquet")

        writer.write_metadata(sample_metadata, path, mode="overwrite")

        assert Path(path).exists()

    def test_write_metadata_empty_list(self, writer, temp_dir):
        """Test error on empty metadata list"""
        path = str(Path(temp_dir) / "tiles.parquet")

        with pytest.raises(ValueError, match="cannot be empty"):
            writer.write_metadata([], path)

    def test_write_metadata_invalid_mode(self, writer, temp_dir, sample_metadata):
        """Test error on invalid write mode"""
        path = str(Path(temp_dir) / "tiles.parquet")

        with pytest.raises(ValueError, match="Invalid mode"):
            writer.write_metadata(sample_metadata, path, mode="invalid")

    def test_write_metadata_append(self, writer, temp_dir, sample_metadata):
        """Test append mode"""
        path = str(Path(temp_dir) / "tiles.parquet")

        # Write initial data
        writer.write_metadata([sample_metadata[0]], path, mode="overwrite")

        # Append more data
        writer.write_metadata([sample_metadata[1]], path, mode="append")

        # Verify both entries exist
        reader = GeoParquetReader()
        all_metadata = reader.read_metadata(path)
        assert len(all_metadata) == 2


class TestGeoParquetReader:
    """Test GeoParquet reader"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir)

    @pytest.fixture
    def sample_file(self, temp_dir):
        """Create sample GeoParquet file"""
        writer = GeoParquetWriter()
        path = str(Path(temp_dir) / "tiles.parquet")

        metadata_list = [
            TileMetadata(
                tile_id="x0024_y0041",
                year_month="2024-01",
                band="red",
                bounds=(127.049, 37.546, 127.072, 37.569),
                num_observations=2,
                min_value=100.0,
                max_value=10000.0,
                mean_value=2500.0,
                cloud_cover=5.0,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path="data/x0024_y0041/2024-01/red.arrow",
            ),
            TileMetadata(
                tile_id="x0024_y0041",
                year_month="2024-02",
                band="red",
                bounds=(127.049, 37.546, 127.072, 37.569),
                num_observations=3,
                min_value=150.0,
                max_value=9500.0,
                mean_value=2800.0,
                cloud_cover=10.0,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path="data/x0024_y0041/2024-02/red.arrow",
            ),
            TileMetadata(
                tile_id="x0025_y0041",
                year_month="2024-01",
                band="red",
                bounds=(127.072, 37.546, 127.095, 37.569),
                num_observations=2,
                min_value=120.0,
                max_value=9800.0,
                mean_value=2600.0,
                cloud_cover=8.0,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path="data/x0025_y0041/2024-01/red.arrow",
            ),
        ]

        writer.write_metadata(metadata_list, path, mode="overwrite")
        return path

    @pytest.fixture
    def reader(self):
        """Create reader instance"""
        return GeoParquetReader()

    def test_init(self, reader):
        """Test reader initialization"""
        assert reader is not None

    def test_read_metadata_basic(self, reader, sample_file):
        """Test basic metadata reading"""
        metadata_list = reader.read_metadata(sample_file)

        assert len(metadata_list) == 3
        assert all(isinstance(m, TileMetadata) for m in metadata_list)

    def test_read_metadata_values(self, reader, sample_file):
        """Test metadata values are preserved"""
        metadata_list = reader.read_metadata(sample_file)

        # Find first metadata entry
        meta = metadata_list[0]
        assert meta.tile_id == "x0024_y0041"
        assert meta.year_month == "2024-01"
        assert meta.band == "red"
        assert meta.num_observations == 2
        assert meta.min_value == 100.0
        assert meta.max_value == 10000.0

    def test_read_metadata_bounds(self, reader, sample_file):
        """Test bounds are preserved"""
        metadata_list = reader.read_metadata(sample_file)

        meta = metadata_list[0]
        assert len(meta.bounds) == 4
        minx, miny, maxx, maxy = meta.bounds
        assert 127.0 < minx < 128.0
        assert 37.0 < miny < 38.0
        assert maxx > minx
        assert maxy > miny

    def test_read_metadata_not_found(self, reader):
        """Test error when file not found"""
        with pytest.raises(FileNotFoundError):
            reader.read_metadata("/nonexistent/tiles.parquet")

    def test_query_by_bounds(self, reader, sample_file):
        """Test spatial query by bounds"""
        # Query bounds that cover first two tiles
        metadata_list = reader.query_by_bounds(sample_file, bounds=(127.0, 37.5, 127.08, 37.6))

        # Should get tiles x0024_y0041 and x0025_y0041
        assert len(metadata_list) >= 2

    def test_query_by_bounds_no_match(self, reader, sample_file):
        """Test spatial query with no matches"""
        # Query bounds far away
        metadata_list = reader.query_by_bounds(sample_file, bounds=(0.0, 0.0, 0.1, 0.1))

        assert len(metadata_list) == 0

    def test_query_by_tile_and_time(self, reader, sample_file):
        """Test query by tile ID and time"""
        # Query specific tile and month
        metadata_list = reader.query_by_tile_and_time(
            sample_file, tile_id="x0024_y0041", year_month="2024-01"
        )

        assert len(metadata_list) == 1
        assert metadata_list[0].tile_id == "x0024_y0041"
        assert metadata_list[0].year_month == "2024-01"

    def test_query_by_tile_only(self, reader, sample_file):
        """Test query by tile ID without time filter"""
        # Query all months for a tile
        metadata_list = reader.query_by_tile_and_time(sample_file, tile_id="x0024_y0041")

        assert len(metadata_list) == 2  # 2024-01 and 2024-02
        assert all(m.tile_id == "x0024_y0041" for m in metadata_list)

    def test_query_by_tile_no_match(self, reader, sample_file):
        """Test query with non-existent tile"""
        metadata_list = reader.query_by_tile_and_time(sample_file, tile_id="x9999_y9999")

        assert len(metadata_list) == 0


class TestGeoParquetRoundtrip:
    """Test roundtrip: write then read"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir)

    def test_roundtrip_single(self, temp_dir):
        """Test simple write-read roundtrip"""
        writer = GeoParquetWriter()
        reader = GeoParquetReader()
        path = str(Path(temp_dir) / "tiles.parquet")

        # Original metadata
        original = TileMetadata(
            tile_id="x0024_y0041",
            year_month="2024-01",
            band="red",
            bounds=(127.049, 37.546, 127.072, 37.569),
            num_observations=2,
            min_value=100.0,
            max_value=10000.0,
            mean_value=2500.0,
            cloud_cover=5.0,
            product_id="sentinel2_l2a",
            resolution=10.0,
            chunk_path="data/x0024_y0041/2024-01/red.arrow",
        )

        # Write
        writer.write_metadata([original], path, mode="overwrite")

        # Read
        read_metadata = reader.read_metadata(path)

        # Verify
        assert len(read_metadata) == 1
        meta = read_metadata[0]
        assert meta.tile_id == original.tile_id
        assert meta.year_month == original.year_month
        assert meta.band == original.band
        assert meta.num_observations == original.num_observations
        assert meta.min_value == original.min_value
        assert meta.max_value == original.max_value
        assert meta.cloud_cover == original.cloud_cover

    def test_roundtrip_multiple(self, temp_dir):
        """Test roundtrip with multiple entries"""
        writer = GeoParquetWriter()
        reader = GeoParquetReader()
        path = str(Path(temp_dir) / "tiles.parquet")

        # Create multiple metadata entries
        metadata_list = []
        for i in range(10):
            meta = TileMetadata(
                tile_id=f"x{i:04d}_y0041",
                year_month="2024-01",
                band="red",
                bounds=(127.0 + i * 0.01, 37.5, 127.01 + i * 0.01, 37.51),
                num_observations=i + 1,
                min_value=100.0 * i,
                max_value=10000.0 * i,
                mean_value=2500.0 * i,
                cloud_cover=float(i),
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path=f"data/x{i:04d}_y0041/2024-01/red.arrow",
            )
            metadata_list.append(meta)

        # Write
        writer.write_metadata(metadata_list, path, mode="overwrite")

        # Read
        read_metadata = reader.read_metadata(path)

        # Verify
        assert len(read_metadata) == 10
