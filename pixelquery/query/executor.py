"""
Query executor - orchestrates data loading and query execution
"""

from datetime import datetime

from pixelquery._internal.storage.arrow_chunk import ArrowChunkReader
from pixelquery.catalog.local import LocalCatalog
from pixelquery.core.dataset import Dataset


class QueryExecutor:
    """
    Executes queries against the PixelQuery warehouse

    Orchestrates metadata queries (via Catalog) and data loading (via storage backends).

    Attributes:
        catalog: LocalCatalog instance for metadata queries
        warehouse_path: Path to the warehouse directory

    Examples:
        >>> from pixelquery.query import QueryExecutor
        >>> from pixelquery.catalog import LocalCatalog
        >>>
        >>> catalog = LocalCatalog("warehouse")
        >>> executor = QueryExecutor(catalog)
        >>>
        >>> # Load data for a tile
        >>> dataset = executor.load_tile(
        ...     tile_id="x0024_y0041",
        ...     time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
        ...     bands=["red", "nir"]
        ... )
    """

    def __init__(self, catalog: LocalCatalog):
        """
        Initialize QueryExecutor

        Args:
            catalog: LocalCatalog instance
        """
        self.catalog = catalog
        self.warehouse_path = catalog.warehouse_path
        self.chunk_reader = ArrowChunkReader()

    def load_tile(
        self,
        tile_id: str,
        time_range: tuple[datetime, datetime] | None = None,
        bands: list[str] | None = None,
        product_id: str | None = None,
    ) -> Dataset:
        """
        Load data for a specific tile

        Args:
            tile_id: Tile identifier (e.g., "x0024_y0041")
            time_range: Optional time range filter (start, end)
            bands: Optional list of bands to load (default: all)
            product_id: Optional product filter

        Returns:
            Dataset with loaded data

        Examples:
            >>> dataset = executor.load_tile(
            ...     tile_id="x0024_y0041",
            ...     time_range=(datetime(2024, 1, 1), datetime(2024, 6, 30)),
            ...     bands=["red", "nir"]
            ... )
            >>> dataset.bands
            ['red', 'nir']
        """
        # Get available bands if not specified
        if bands is None:
            bands = self.catalog.list_bands(tile_id=tile_id, product_id=product_id)

        # Query metadata for this tile
        metadata_list = []
        if time_range:
            start, end = time_range
            # Query for each month in range
            current = datetime(start.year, start.month, 1)
            while current <= end:
                year_month = current.strftime("%Y-%m")
                for band in bands:
                    tile_metadata = self.catalog.query_metadata(tile_id, year_month, band)
                    metadata_list.extend(tile_metadata)

                # Move to next month
                if current.month == 12:
                    current = datetime(current.year + 1, 1, 1)
                else:
                    current = datetime(current.year, current.month + 1, 1)
        else:
            # Load all available data
            for band in bands:
                tile_metadata = self.catalog.query_metadata(tile_id, band=band)
                metadata_list.extend(tile_metadata)

        # Load data from chunks
        data = {}
        for band in bands:
            band_data_list = []

            # Find all chunks for this band
            band_metadata = [m for m in metadata_list if m.band == band]

            for metadata in band_metadata:
                chunk_path = self.warehouse_path / metadata.chunk_path

                if chunk_path.exists():
                    chunk_data, _chunk_meta = self.chunk_reader.read_chunk(str(chunk_path))
                    # Extend to collect all time observations
                    band_data_list.extend(chunk_data["pixels"])

            # Store list of time observations
            if band_data_list:
                # Keep as list for time-series access
                data[band] = band_data_list

        # Get tile bounds
        bounds = self.catalog.get_tile_bounds(tile_id)

        # Create Dataset
        dataset = Dataset(
            tile_id=tile_id,
            time_range=time_range,
            bands=bands,
            data=data,
            metadata={"bounds": bounds, "product_id": product_id, "num_chunks": len(metadata_list)},
        )

        return dataset

    def load_tiles(
        self,
        tile_ids: list[str],
        time_range: tuple[datetime, datetime] | None = None,
        bands: list[str] | None = None,
        product_id: str | None = None,
    ) -> dict[str, Dataset]:
        """
        Load data for multiple tiles

        Args:
            tile_ids: List of tile identifiers
            time_range: Optional time range filter
            bands: Optional list of bands to load
            product_id: Optional product filter

        Returns:
            Dictionary mapping tile IDs to Datasets

        Examples:
            >>> datasets = executor.load_tiles(
            ...     tile_ids=["x0024_y0041", "x0024_y0042"],
            ...     bands=["red", "nir"]
            ... )
            >>> datasets["x0024_y0041"].bands
            ['red', 'nir']
        """
        datasets = {}
        for tile_id in tile_ids:
            datasets[tile_id] = self.load_tile(
                tile_id=tile_id, time_range=time_range, bands=bands, product_id=product_id
            )
        return datasets

    def query_by_bounds(
        self,
        bounds: tuple[float, float, float, float],
        time_range: tuple[datetime, datetime] | None = None,
        bands: list[str] | None = None,
        product_id: str | None = None,
    ) -> dict[str, Dataset]:
        """
        Query data by spatial bounds

        Args:
            bounds: Bounding box (minx, miny, maxx, maxy)
            time_range: Optional time range filter
            bands: Optional list of bands to load
            product_id: Optional product filter

        Returns:
            Dictionary mapping tile IDs to Datasets

        Examples:
            >>> datasets = executor.query_by_bounds(
            ...     bounds=(126.5, 37.0, 127.5, 38.0),
            ...     time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
            ...     bands=["red", "nir"]
            ... )
        """
        # Find tiles that intersect with bounds
        tile_ids = self.catalog.list_tiles(
            bounds=bounds, time_range=time_range, product_id=product_id
        )

        # Load data for each tile
        return self.load_tiles(
            tile_ids=tile_ids, time_range=time_range, bands=bands, product_id=product_id
        )

    def get_available_tiles(
        self,
        bounds: tuple[float, float, float, float] | None = None,
        time_range: tuple[datetime, datetime] | None = None,
        product_id: str | None = None,
    ) -> list[str]:
        """
        Get list of available tiles (without loading data)

        Args:
            bounds: Optional bounding box filter
            time_range: Optional time range filter
            product_id: Optional product filter

        Returns:
            List of tile IDs

        Examples:
            >>> tiles = executor.get_available_tiles(
            ...     bounds=(126.0, 37.0, 128.0, 38.0)
            ... )
            >>> tiles
            ['x0024_y0041', 'x0024_y0042']
        """
        return self.catalog.list_tiles(bounds=bounds, time_range=time_range, product_id=product_id)

    def get_tile_statistics(
        self, tile_id: str, band: str, time_range: tuple[datetime, datetime] | None = None
    ) -> dict[str, float]:
        """
        Get pre-computed statistics for a tile-band

        Args:
            tile_id: Tile identifier
            band: Band name
            time_range: Optional time range filter

        Returns:
            Dictionary with min, max, mean, cloud_cover

        Examples:
            >>> stats = executor.get_tile_statistics("x0024_y0041", "red")
            >>> stats['mean']
            2500.0
        """
        return self.catalog.get_statistics(tile_id, band, time_range)

    def __repr__(self) -> str:
        """String representation"""
        return (
            f"<QueryExecutor>\n"
            f"Warehouse: {self.warehouse_path}\n"
            f"Catalog: {self.catalog.__class__.__name__}"
        )
