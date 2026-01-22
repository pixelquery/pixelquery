"""
Tests for LocalCatalog
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

from pixelquery.catalog import LocalCatalog
from pixelquery._internal.storage.geoparquet import TileMetadata


class TestLocalCatalog:
    """Test LocalCatalog class"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def catalog(self, temp_warehouse):
        """Create LocalCatalog instance"""
        return LocalCatalog(temp_warehouse)

    @pytest.fixture
    def sample_metadata(self):
        """Create sample TileMetadata"""
        return TileMetadata(
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
        )

    def test_init(self, catalog, temp_warehouse):
        """Test LocalCatalog initialization"""
        assert catalog.warehouse_path == Path(temp_warehouse)
        assert catalog.metadata_path == Path(temp_warehouse) / "metadata.parquet"
        assert catalog.warehouse_path.exists()

    def test_exists_false_initially(self, catalog):
        """Test exists returns False for new catalog"""
        assert not catalog.exists()

    def test_add_tile_metadata(self, catalog, sample_metadata):
        """Test adding single metadata record"""
        catalog.add_tile_metadata(sample_metadata)

        assert catalog.exists()

    def test_add_tile_metadata_batch(self, catalog):
        """Test adding multiple metadata records"""
        metadata_list = [
            TileMetadata(
                tile_id=f"x{i:04d}_y0041",
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
                chunk_path=f"tiles/x{i:04d}_y0041/2024-01/red.arrow"
            )
            for i in range(3)
        ]

        catalog.add_tile_metadata_batch(metadata_list)

        assert catalog.exists()

    def test_list_tiles_empty(self, catalog):
        """Test list_tiles on empty catalog"""
        tiles = catalog.list_tiles()
        assert tiles == []

    def test_list_tiles(self, catalog):
        """Test listing tiles"""
        # Add metadata for multiple tiles
        metadata_list = [
            TileMetadata(
                tile_id=f"x{i:04d}_y0041",
                year_month="2024-01",
                band="red",
                bounds=(126.0 + i, 37.0, 127.0 + i, 38.0),
                num_observations=10,
                min_value=100.0,
                max_value=5000.0,
                mean_value=2500.0,
                cloud_cover=0.1,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path=f"tiles/x{i:04d}_y0041/2024-01/red.arrow"
            )
            for i in range(3)
        ]

        catalog.add_tile_metadata_batch(metadata_list)

        tiles = catalog.list_tiles()
        assert len(tiles) == 3
        assert "x0000_y0041" in tiles
        assert "x0001_y0041" in tiles
        assert "x0002_y0041" in tiles

    def test_list_tiles_by_bounds(self, catalog):
        """Test listing tiles filtered by bounds"""
        metadata_list = [
            TileMetadata(
                tile_id="x0000_y0041",
                year_month="2024-01",
                band="red",
                bounds=(126.0, 37.0, 127.0, 38.0),
                num_observations=10,
                min_value=100.0,
                max_value=5000.0,
                mean_value=2500.0,
                cloud_cover=0.1,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path="tiles/x0000_y0041/2024-01/red.arrow"
            ),
            TileMetadata(
                tile_id="x0001_y0041",
                year_month="2024-01",
                band="red",
                bounds=(130.0, 37.0, 131.0, 38.0),
                num_observations=10,
                min_value=100.0,
                max_value=5000.0,
                mean_value=2500.0,
                cloud_cover=0.1,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path="tiles/x0001_y0041/2024-01/red.arrow"
            )
        ]

        catalog.add_tile_metadata_batch(metadata_list)

        # Query within first tile bounds
        tiles = catalog.list_tiles(bounds=(126.0, 37.0, 127.0, 38.0))
        assert len(tiles) == 1
        assert "x0000_y0041" in tiles

    def test_list_bands_empty(self, catalog):
        """Test list_bands on empty catalog"""
        bands = catalog.list_bands()
        assert bands == []

    def test_list_bands(self, catalog):
        """Test listing bands"""
        metadata_list = [
            TileMetadata(
                tile_id="x0024_y0041",
                year_month="2024-01",
                band=band,
                bounds=(126.5, 37.0, 127.5, 38.0),
                num_observations=10,
                min_value=100.0,
                max_value=5000.0,
                mean_value=2500.0,
                cloud_cover=0.1,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path=f"tiles/x0024_y0041/2024-01/{band}.arrow"
            )
            for band in ["red", "green", "blue", "nir"]
        ]

        catalog.add_tile_metadata_batch(metadata_list)

        bands = catalog.list_bands()
        assert len(bands) == 4
        assert "red" in bands
        assert "nir" in bands

    def test_list_bands_by_tile(self, catalog):
        """Test listing bands filtered by tile"""
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
                tile_id="x0025_y0041",
                year_month="2024-01",
                band="nir",
                bounds=(127.5, 37.0, 128.5, 38.0),
                num_observations=10,
                min_value=100.0,
                max_value=5000.0,
                mean_value=2500.0,
                cloud_cover=0.1,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path="tiles/x0025_y0041/2024-01/nir.arrow"
            )
        ]

        catalog.add_tile_metadata_batch(metadata_list)

        bands = catalog.list_bands(tile_id="x0024_y0041")
        assert len(bands) == 1
        assert "red" in bands

    def test_query_metadata(self, catalog, sample_metadata):
        """Test querying metadata"""
        catalog.add_tile_metadata(sample_metadata)

        metadata_list = catalog.query_metadata("x0024_y0041", "2024-01", "red")

        assert len(metadata_list) == 1
        assert metadata_list[0].tile_id == "x0024_y0041"
        assert metadata_list[0].band == "red"

    def test_query_metadata_empty(self, catalog):
        """Test querying metadata on empty catalog"""
        metadata_list = catalog.query_metadata("x0024_y0041")
        assert metadata_list == []

    def test_get_chunk_paths(self, catalog):
        """Test getting chunk paths"""
        metadata_list = [
            TileMetadata(
                tile_id="x0024_y0041",
                year_month="2024-01",
                band=band,
                bounds=(126.5, 37.0, 127.5, 38.0),
                num_observations=10,
                min_value=100.0,
                max_value=5000.0,
                mean_value=2500.0,
                cloud_cover=0.1,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path=f"tiles/x0024_y0041/2024-01/{band}.arrow"
            )
            for band in ["red", "nir"]
        ]

        catalog.add_tile_metadata_batch(metadata_list)

        paths = catalog.get_chunk_paths("x0024_y0041", "2024-01", ["red", "nir"])

        assert len(paths) == 2
        assert "red" in paths
        assert "nir" in paths
        assert paths["red"] == "tiles/x0024_y0041/2024-01/red.arrow"

    def test_get_tile_bounds(self, catalog, sample_metadata):
        """Test getting tile bounds"""
        catalog.add_tile_metadata(sample_metadata)

        bounds = catalog.get_tile_bounds("x0024_y0041")

        assert bounds == (126.5, 37.0, 127.5, 38.0)

    def test_get_tile_bounds_not_found(self, catalog):
        """Test getting bounds for non-existent tile"""
        bounds = catalog.get_tile_bounds("x9999_y9999")
        assert bounds is None

    def test_get_statistics(self, catalog):
        """Test getting statistics"""
        # Add multiple months of data
        metadata_list = [
            TileMetadata(
                tile_id="x0024_y0041",
                year_month=f"2024-{month:02d}",
                band="red",
                bounds=(126.5, 37.0, 127.5, 38.0),
                num_observations=10,
                min_value=100.0 + month * 10,
                max_value=5000.0 + month * 10,
                mean_value=2500.0 + month * 10,
                cloud_cover=0.1,
                product_id="sentinel2_l2a",
                resolution=10.0,
                chunk_path=f"tiles/x0024_y0041/2024-{month:02d}/red.arrow"
            )
            for month in range(1, 4)
        ]

        catalog.add_tile_metadata_batch(metadata_list)

        stats = catalog.get_statistics("x0024_y0041", "red")

        assert stats['min'] == 110.0  # min of all months
        assert stats['max'] == 5030.0  # max of all months
        assert 'mean' in stats
        assert 'cloud_cover' in stats

    def test_repr(self, catalog):
        """Test string representation"""
        repr_str = repr(catalog)

        assert "LocalCatalog" in repr_str
        assert "Warehouse" in repr_str


class TestLocalCatalogIntegration:
    """Integration tests for LocalCatalog"""

    @pytest.fixture
    def temp_warehouse(self):
        """Create temporary warehouse directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_full_workflow(self, temp_warehouse):
        """Test complete workflow: add, query, retrieve"""
        catalog = LocalCatalog(temp_warehouse)

        # Add metadata for multiple tiles, months, bands
        metadata_list = []
        for tile_id in ["x0024_y0041", "x0024_y0042"]:
            for month in [1, 2]:
                for band in ["red", "nir"]:
                    metadata_list.append(
                        TileMetadata(
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
                    )

        catalog.add_tile_metadata_batch(metadata_list)

        # Query tiles
        tiles = catalog.list_tiles()
        assert len(tiles) == 2

        # Query bands
        bands = catalog.list_bands(tile_id="x0024_y0041")
        assert len(bands) == 2

        # Get chunk paths
        paths = catalog.get_chunk_paths("x0024_y0041", "2024-01", ["red"])
        assert "red" in paths

        # Get statistics
        stats = catalog.get_statistics("x0024_y0041", "red")
        assert 'min' in stats
        assert 'max' in stats
