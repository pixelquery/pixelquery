"""
Tests for IngestionPipeline
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
import numpy as np
import rasterio
from rasterio.transform import from_bounds

from pixelquery.io.ingest import IngestionPipeline
from pixelquery.io.cog import COGReader
from pixelquery.grid.tile_grid import FixedTileGrid
from pixelquery.catalog.local import LocalCatalog
from pixelquery._internal.storage.arrow_chunk import ArrowChunkReader


class TestIngestionPipeline:
    """Test IngestionPipeline class"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def mock_cog_file(self):
        """Create small mock COG file"""
        tmpdir = tempfile.mkdtemp()
        cog_path = Path(tmpdir) / "test.tif"

        # Create 100x100 pixel COG covering small area
        width, height = 100, 100
        bounds = (126.5, 37.0, 126.6, 37.1)  # ~11km × 11km

        transform = from_bounds(*bounds, width, height)

        # Create 4-band array (blue, green, red, nir)
        data = np.random.randint(100, 4000, (4, height, width), dtype=np.uint16)

        # Write COG
        with rasterio.open(
            cog_path, 'w',
            driver='GTiff',
            height=height, width=width,
            count=4,
            dtype=np.uint16,
            crs='EPSG:4326',
            transform=transform,
            nodata=0
        ) as dst:
            dst.write(data)

        yield str(cog_path)

        # Cleanup
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def pipeline(self, temp_warehouse):
        """Create IngestionPipeline instance"""
        catalog = LocalCatalog(temp_warehouse)
        tile_grid = FixedTileGrid()

        return IngestionPipeline(
            warehouse_path=temp_warehouse,
            tile_grid=tile_grid,
            catalog=catalog,
            storage_backend="arrow"  # Force Arrow backend for consistency
        )

    def test_init(self, pipeline, temp_warehouse):
        """Test IngestionPipeline initialization"""
        assert pipeline.warehouse_path == Path(temp_warehouse)
        assert isinstance(pipeline.tile_grid, FixedTileGrid)
        assert isinstance(pipeline.catalog, LocalCatalog)
        assert pipeline.chunk_writer is not None

    def test_ingest_cog_basic(self, pipeline, mock_cog_file):
        """Test basic COG ingestion"""
        metadata_list = pipeline.ingest_cog(
            cog_path=mock_cog_file,
            acquisition_time=datetime(2024, 6, 15, 10, 30),
            product_id="sentinel2_l2a",
            band_mapping={1: "blue", 2: "green", 3: "red", 4: "nir"}
        )

        # Should create metadata for each tile-band combination
        assert len(metadata_list) > 0

        # Check metadata fields
        for metadata in metadata_list:
            assert metadata.tile_id is not None
            assert metadata.year_month == "2024-06"
            assert metadata.band in ["blue", "green", "red", "nir"]
            assert metadata.product_id == "sentinel2_l2a"
            assert metadata.chunk_path is not None
            assert metadata.num_observations == 1

    def test_ingest_cog_creates_files(self, pipeline, mock_cog_file, temp_warehouse):
        """Test that ingestion creates Arrow IPC files"""
        metadata_list = pipeline.ingest_cog(
            cog_path=mock_cog_file,
            acquisition_time=datetime(2024, 6, 15),
            product_id="sentinel2_l2a",
            band_mapping={1: "red"}
        )

        # Check that files were created
        for metadata in metadata_list:
            chunk_path = Path(temp_warehouse) / metadata.chunk_path
            assert chunk_path.exists()
            assert chunk_path.suffix == ".arrow"

    def test_ingest_cog_registers_metadata(self, pipeline, mock_cog_file):
        """Test that ingestion registers metadata with catalog"""
        metadata_list = pipeline.ingest_cog(
            cog_path=mock_cog_file,
            acquisition_time=datetime(2024, 6, 15),
            product_id="sentinel2_l2a",
            band_mapping={1: "red", 2: "nir"}
        )

        # Query catalog for registered tiles
        tiles = pipeline.catalog.list_tiles()
        assert len(tiles) > 0

        # Check that ingested tiles are in catalog
        tile_ids = {m.tile_id for m in metadata_list}
        for tile_id in tile_ids:
            assert tile_id in tiles

    def test_ingest_cog_multiple_bands(self, pipeline, mock_cog_file):
        """Test ingestion with multiple bands"""
        metadata_list = pipeline.ingest_cog(
            cog_path=mock_cog_file,
            acquisition_time=datetime(2024, 6, 15),
            product_id="sentinel2_l2a",
            band_mapping={1: "blue", 2: "green", 3: "red", 4: "nir"}
        )

        # Should have metadata for each band
        bands = {m.band for m in metadata_list}
        assert "blue" in bands
        assert "green" in bands
        assert "red" in bands
        assert "nir" in bands

    def test_ingest_cog_statistics(self, pipeline, mock_cog_file):
        """Test that statistics are computed correctly"""
        metadata_list = pipeline.ingest_cog(
            cog_path=mock_cog_file,
            acquisition_time=datetime(2024, 6, 15),
            product_id="sentinel2_l2a",
            band_mapping={1: "red"}
        )

        # Check statistics are reasonable
        for metadata in metadata_list:
            assert metadata.min_value >= 0
            assert metadata.max_value > metadata.min_value
            assert metadata.min_value <= metadata.mean_value <= metadata.max_value

    def test_ingest_cog_chunk_data(self, pipeline, mock_cog_file, temp_warehouse):
        """Test that chunk data can be read back"""
        metadata_list = pipeline.ingest_cog(
            cog_path=mock_cog_file,
            acquisition_time=datetime(2024, 6, 15),
            product_id="sentinel2_l2a",
            band_mapping={1: "red"}
        )

        # Read back chunk data
        reader = ArrowChunkReader()
        for metadata in metadata_list:
            chunk_path = Path(temp_warehouse) / metadata.chunk_path
            data, chunk_metadata = reader.read_chunk(str(chunk_path))

            # Check data structure
            assert 'time' in data
            assert 'pixels' in data
            assert 'mask' in data

            # Check data types
            assert len(data['time']) > 0
            assert len(data['pixels']) > 0
            assert len(data['mask']) > 0

            # Check pixel data
            pixels = data['pixels'][0]
            assert isinstance(pixels, np.ndarray)
            assert pixels.dtype == np.uint16

            # Check metadata
            assert chunk_metadata is not None
            assert 'product_id' in chunk_metadata

    def test_ingest_cog_year_month(self, pipeline, mock_cog_file):
        """Test year-month formatting"""
        # January
        metadata_list = pipeline.ingest_cog(
            cog_path=mock_cog_file,
            acquisition_time=datetime(2024, 1, 15),
            product_id="sentinel2_l2a",
            band_mapping={1: "red"}
        )
        assert all(m.year_month == "2024-01" for m in metadata_list)

        # December
        metadata_list = pipeline.ingest_cog(
            cog_path=mock_cog_file,
            acquisition_time=datetime(2024, 12, 25),
            product_id="sentinel2_l2a",
            band_mapping={1: "red"}
        )
        assert all(m.year_month == "2024-12" for m in metadata_list)

    def test_repr(self, pipeline, temp_warehouse):
        """Test string representation"""
        repr_str = repr(pipeline)
        assert "IngestionPipeline" in repr_str
        assert "Warehouse" in repr_str


class TestIngestionPipelineEdgeCases:
    """Test edge cases and error handling"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def pipeline(self, temp_warehouse):
        """Create IngestionPipeline instance"""
        catalog = LocalCatalog(temp_warehouse)
        tile_grid = FixedTileGrid()

        return IngestionPipeline(
            warehouse_path=temp_warehouse,
            tile_grid=tile_grid,
            catalog=catalog,
            storage_backend="arrow"  # Force Arrow backend for consistency
        )

    def test_ingest_nonexistent_file(self, pipeline):
        """Test error handling for nonexistent file"""
        with pytest.raises(rasterio.errors.RasterioIOError):
            pipeline.ingest_cog(
                cog_path="nonexistent.tif",
                acquisition_time=datetime(2024, 6, 15),
                product_id="sentinel2_l2a",
                band_mapping={1: "red"}
            )

    def test_ingest_empty_band_mapping(self, pipeline):
        """Test ingestion with empty band mapping"""
        tmpdir = tempfile.mkdtemp()
        cog_path = Path(tmpdir) / "test.tif"

        try:
            # Create minimal COG
            width, height = 10, 10
            bounds = (0, 0, 0.1, 0.1)
            transform = from_bounds(*bounds, width, height)
            data = np.ones((1, height, width), dtype=np.uint16)

            with rasterio.open(
                cog_path, 'w',
                driver='GTiff',
                height=height, width=width,
                count=1,
                dtype=np.uint16,
                crs='EPSG:4326',
                transform=transform
            ) as dst:
                dst.write(data)

            # Ingest with empty band mapping
            metadata_list = pipeline.ingest_cog(
                cog_path=str(cog_path),
                acquisition_time=datetime(2024, 6, 15),
                product_id="test",
                band_mapping={}
            )

            # Should return empty list
            assert len(metadata_list) == 0

        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
