"""
Tests for QueryExecutor
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
import numpy as np

from pixelquery.query import QueryExecutor
from pixelquery.catalog import LocalCatalog
from pixelquery._internal.storage.geoparquet import TileMetadata
from pixelquery._internal.storage.arrow_chunk import ArrowChunkWriter
from pixelquery.core.dataset import Dataset


class TestQueryExecutor:
    """Test QueryExecutor class"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def catalog(self, temp_warehouse):
        """Create LocalCatalog with sample data"""
        catalog = LocalCatalog(temp_warehouse)

        # Add sample metadata
        metadata_list = [
            TileMetadata(
                tile_id="x0024_y0041",
                year_month="2024-01",
                band="red",
                bounds=(126.5, 37.0, 127.5, 38.0),
                num_observations=10,
                min_value=100.0,
                max_value=5000.0,
                mean_value=2500.0,
                cloud_cover=0.1,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path="tiles/x0024_y0041/2024-01/red.arrow"
            ),
            TileMetadata(
                tile_id="x0024_y0041",
                year_month="2024-01",
                band="nir",
                bounds=(126.5, 37.0, 127.5, 38.0),
                num_observations=10,
                min_value=100.0,
                max_value=5000.0,
                mean_value=2500.0,
                cloud_cover=0.1,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path="tiles/x0024_y0041/2024-01/nir.arrow"
            )
        ]

        catalog.add_tile_metadata_batch(metadata_list)

        # Create dummy chunk files
        chunk_writer = ArrowChunkWriter()
        for metadata in metadata_list:
            chunk_path = Path(temp_warehouse) / metadata.chunk_path
            chunk_path.parent.mkdir(parents=True, exist_ok=True)

            # Create dummy data
            data = {
                'time': [datetime(2024, 1, i+1) for i in range(10)],
                'pixels': [np.array([i] * 100, dtype=np.uint16) for i in range(10)],
                'mask': [np.array([True] * 100) for i in range(10)]
            }

            chunk_writer.write_chunk(
                str(chunk_path),
                data,
                product_id=metadata.product_id,
                resolution=metadata.resolution,
                metadata={'band': metadata.band}
            )

        return catalog

    @pytest.fixture
    def executor(self, catalog):
        """Create QueryExecutor instance"""
        return QueryExecutor(catalog)

    def test_init(self, executor, catalog):
        """Test QueryExecutor initialization"""
        assert executor.catalog == catalog
        assert executor.warehouse_path == catalog.warehouse_path
        assert executor.chunk_reader is not None

    def test_load_tile(self, executor):
        """Test loading a single tile"""
        dataset = executor.load_tile(
            tile_id="x0024_y0041",
            bands=["red", "nir"]
        )

        assert isinstance(dataset, Dataset)
        assert dataset.tile_id == "x0024_y0041"
        assert dataset.bands == ["red", "nir"]
        assert "red" in dataset.data
        assert "nir" in dataset.data

    def test_load_tile_with_time_range(self, executor):
        """Test loading tile with time range filter"""
        dataset = executor.load_tile(
            tile_id="x0024_y0041",
            time_range=(datetime(2024, 1, 1), datetime(2024, 1, 31)),
            bands=["red"]
        )

        assert isinstance(dataset, Dataset)
        assert dataset.time_range is not None

    def test_load_tile_auto_bands(self, executor):
        """Test loading tile with automatic band detection"""
        dataset = executor.load_tile(
            tile_id="x0024_y0041"
        )

        assert isinstance(dataset, Dataset)
        assert len(dataset.bands) >= 2  # Should have red and nir

    def test_load_tiles(self, executor):
        """Test loading multiple tiles"""
        datasets = executor.load_tiles(
            tile_ids=["x0024_y0041"],
            bands=["red"]
        )

        assert isinstance(datasets, dict)
        assert "x0024_y0041" in datasets
        assert isinstance(datasets["x0024_y0041"], Dataset)

    def test_query_by_bounds(self, executor):
        """Test querying by spatial bounds"""
        datasets = executor.query_by_bounds(
            bounds=(126.5, 37.0, 127.5, 38.0),
            bands=["red"]
        )

        assert isinstance(datasets, dict)
        assert len(datasets) >= 1

    def test_query_by_bounds_no_match(self, executor):
        """Test querying with bounds that don't match any tiles"""
        datasets = executor.query_by_bounds(
            bounds=(0.0, 0.0, 1.0, 1.0),
            bands=["red"]
        )

        assert isinstance(datasets, dict)
        assert len(datasets) == 0

    def test_get_available_tiles(self, executor):
        """Test getting available tiles"""
        tiles = executor.get_available_tiles()

        assert isinstance(tiles, list)
        assert "x0024_y0041" in tiles

    def test_get_available_tiles_with_bounds(self, executor):
        """Test getting available tiles filtered by bounds"""
        tiles = executor.get_available_tiles(
            bounds=(126.5, 37.0, 127.5, 38.0)
        )

        assert isinstance(tiles, list)
        assert "x0024_y0041" in tiles

    def test_get_tile_statistics(self, executor):
        """Test getting tile statistics"""
        stats = executor.get_tile_statistics("x0024_y0041", "red")

        assert isinstance(stats, dict)
        assert 'min' in stats
        assert 'max' in stats
        assert 'mean' in stats
        assert 'cloud_cover' in stats

    def test_repr(self, executor):
        """Test string representation"""
        repr_str = repr(executor)

        assert "QueryExecutor" in repr_str
        assert "Warehouse" in repr_str


class TestQueryExecutorIntegration:
    """Integration tests for QueryExecutor"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_full_workflow(self, temp_warehouse):
        """Test complete workflow: setup catalog, add data, query"""
        # Setup catalog
        catalog = LocalCatalog(temp_warehouse)

        # Add metadata for multiple tiles and months
        metadata_list = []
        chunk_writer = ArrowChunkWriter()

        for tile_id in ["x0024_y0041", "x0024_y0042"]:
            for month in [1, 2]:
                for band in ["red", "nir"]:
                    metadata = TileMetadata(
                        tile_id=tile_id,
                        year_month=f"2024-{month:02d}",
                        band=band,
                        bounds=(126.5, 37.0, 127.5, 38.0),
                        num_observations=10,
                        min_value=100.0,
                        max_value=5000.0,
                        mean_value=2500.0,
                        cloud_cover=0.1,
                        product_id="sentinel2_l2a",
                        resolution=10.0,
                        chunk_path=f"tiles/{tile_id}/2024-{month:02d}/{band}.arrow"
                    )
                    metadata_list.append(metadata)

                    # Create chunk file
                    chunk_path = Path(temp_warehouse) / metadata.chunk_path
                    chunk_path.parent.mkdir(parents=True, exist_ok=True)

                    data = {
                        'time': [datetime(2024, month, i+1) for i in range(5)],
                        'pixels': [np.array([i] * 100, dtype=np.uint16) for i in range(5)],
                        'mask': [np.array([True] * 100) for i in range(5)]
                    }

                    chunk_writer.write_chunk(
                        str(chunk_path),
                        data,
                        product_id=metadata.product_id,
                        resolution=metadata.resolution,
                        metadata={'band': band}
                    )

        catalog.add_tile_metadata_batch(metadata_list)

        # Create executor
        executor = QueryExecutor(catalog)

        # Query tiles
        tiles = executor.get_available_tiles()
        assert len(tiles) == 2

        # Load single tile
        dataset = executor.load_tile("x0024_y0041", bands=["red"])
        assert dataset.tile_id == "x0024_y0041"
        assert "red" in dataset.data

        # Query by bounds
        datasets = executor.query_by_bounds(
            bounds=(126.5, 37.0, 127.5, 38.0),
            bands=["red"]
        )
        assert len(datasets) == 2

        # Get statistics
        stats = executor.get_tile_statistics("x0024_y0041", "red")
        assert stats['min'] == 100.0
        assert stats['max'] == 5000.0

    def test_time_range_filtering(self, temp_warehouse):
        """Test time range filtering"""
        catalog = LocalCatalog(temp_warehouse)

        # Add metadata for multiple months
        metadata_list = []
        chunk_writer = ArrowChunkWriter()

        for month in range(1, 7):  # 6 months
            metadata = TileMetadata(
                tile_id="x0024_y0041",
                year_month=f"2024-{month:02d}",
                band="red",
                bounds=(126.5, 37.0, 127.5, 38.0),
                num_observations=10,
                min_value=100.0,
                max_value=5000.0,
                mean_value=2500.0,
                cloud_cover=0.1,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path=f"tiles/x0024_y0041/2024-{month:02d}/red.arrow"
            )
            metadata_list.append(metadata)

            # Create chunk file
            chunk_path = Path(temp_warehouse) / metadata.chunk_path
            chunk_path.parent.mkdir(parents=True, exist_ok=True)

            data = {
                'time': [datetime(2024, month, i+1) for i in range(5)],
                'pixels': [np.array([i] * 100, dtype=np.uint16) for i in range(5)],
                'mask': [np.array([True] * 100) for i in range(5)]
            }

            chunk_writer.write_chunk(
                str(chunk_path),
                data,
                product_id=metadata.product_id,
                resolution=metadata.resolution,
                metadata={'band': 'red'}
            )

        catalog.add_tile_metadata_batch(metadata_list)

        # Create executor and query with time range
        executor = QueryExecutor(catalog)

        dataset = executor.load_tile(
            tile_id="x0024_y0041",
            time_range=(datetime(2024, 1, 1), datetime(2024, 3, 31)),
            bands=["red"]
        )

        assert isinstance(dataset, Dataset)
        assert dataset.time_range is not None
