"""
GeoParquet Tile Metadata Storage

Implements tile metadata storage using GeoParquet format for spatial indexing.
"""

from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box


@dataclass
class TileMetadata:
    """
    Tile metadata for a single tile-month-band

    Attributes:
        tile_id: Tile identifier (e.g., "x0024_y0041")
        year_month: Year-month string (e.g., "2024-01")
        band: Band name (e.g., "red", "nir")
        bounds: Geographic bounds (minx, miny, maxx, maxy)
        num_observations: Number of observations in this chunk
        min_value: Minimum pixel value
        max_value: Maximum pixel value
        mean_value: Mean pixel value
        cloud_cover: Cloud cover percentage (0-100)
        product_id: Product identifier (e.g., "sentinel2_l2a")
        resolution: Spatial resolution in meters
        chunk_path: Path to Arrow IPC chunk file
    """

    tile_id: str
    year_month: str
    band: str
    bounds: tuple[float, float, float, float]
    num_observations: int
    min_value: float
    max_value: float
    mean_value: float
    cloud_cover: float
    product_id: str
    resolution: float
    chunk_path: str


class GeoParquetWriter:
    """
    GeoParquet writer for tile metadata

    Writes tile metadata to GeoParquet format with spatial indexing
    for efficient spatial and temporal queries via DuckDB.

    File naming convention:
        {warehouse}/{table}/metadata/tiles.parquet

    Schema:
        - tile_id: string
        - year_month: string (partition key)
        - band: string
        - geometry: geometry (tile bounds as polygon)
        - num_observations: int64
        - min_value: float64
        - max_value: float64
        - mean_value: float64
        - cloud_cover: float64
        - product_id: string
        - resolution: float64
        - chunk_path: string

    Examples:
        >>> writer = GeoParquetWriter()
        >>> metadata = TileMetadata(
        ...     tile_id="x0024_y0041",
        ...     year_month="2024-01",
        ...     band="red",
        ...     bounds=(127.049, 37.546, 127.072, 37.569),
        ...     num_observations=2,
        ...     min_value=100.0,
        ...     max_value=10000.0,
        ...     mean_value=2500.0,
        ...     cloud_cover=5.0,
        ...     product_id="sentinel2_l2a",
        ...     resolution=10.0,
        ...     chunk_path="data/x0024_y0041/2024-01/red.arrow"
        ... )
        >>> writer.write_metadata([metadata], "warehouse/table/metadata/tiles.parquet")
    """

    def write_metadata(
        self, metadata_list: list[TileMetadata], path: str, mode: str = "append"
    ) -> None:
        """
        Write tile metadata to GeoParquet

        Args:
            metadata_list: List of tile metadata to write
            path: Output GeoParquet file path
            mode: Write mode ('append' or 'overwrite')

        Raises:
            ValueError: If metadata list is empty or invalid
            IOError: If write fails
        """
        if not metadata_list:
            raise ValueError("metadata_list cannot be empty")

        # Validate mode
        if mode not in ("append", "overwrite"):
            raise ValueError(f"Invalid mode: {mode}. Must be 'append' or 'overwrite'")

        # Convert to GeoDataFrame
        gdf = self._create_geodataframe(metadata_list)

        # Ensure parent directory exists
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        # Write to GeoParquet
        if mode == "overwrite" or not Path(path).exists():
            gdf.to_parquet(path, compression="zstd", index=False)
        elif mode == "append":
            # Append to existing file
            self._append_to_parquet(gdf, path)

    def _create_geodataframe(self, metadata_list: list[TileMetadata]) -> gpd.GeoDataFrame:
        """Convert metadata list to GeoDataFrame"""
        records = []
        for meta in metadata_list:
            # Create polygon from bounds
            minx, miny, maxx, maxy = meta.bounds
            geometry = box(minx, miny, maxx, maxy)

            records.append(
                {
                    "tile_id": meta.tile_id,
                    "year_month": meta.year_month,
                    "band": meta.band,
                    "geometry": geometry,
                    "num_observations": meta.num_observations,
                    "min_value": meta.min_value,
                    "max_value": meta.max_value,
                    "mean_value": meta.mean_value,
                    "cloud_cover": meta.cloud_cover,
                    "product_id": meta.product_id,
                    "resolution": meta.resolution,
                    "chunk_path": meta.chunk_path,
                }
            )

        # Create GeoDataFrame with WGS84 CRS
        gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
        return gdf

    def _append_to_parquet(self, new_gdf: gpd.GeoDataFrame, path: str) -> None:
        """Append new data to existing GeoParquet file"""
        # Read existing data
        existing_gdf = gpd.read_parquet(path)

        # Concatenate
        combined_gdf = gpd.GeoDataFrame(
            pd.concat([existing_gdf, new_gdf], ignore_index=True), crs=existing_gdf.crs
        )

        # Write back
        combined_gdf.to_parquet(path, compression="zstd", index=False)


class GeoParquetReader:
    """
    GeoParquet reader for tile metadata

    Reads tile metadata from GeoParquet format with spatial and temporal
    filtering capabilities.

    Examples:
        >>> reader = GeoParquetReader()
        >>> metadata = reader.read_metadata("warehouse/table/metadata/tiles.parquet")
        >>> print(len(metadata))
        10
        >>>
        >>> # Spatial query
        >>> filtered = reader.query_by_bounds(
        ...     path="warehouse/table/metadata/tiles.parquet",
        ...     bounds=(127.0, 37.5, 127.1, 37.6)
        ... )
    """

    def read_metadata(self, path: str) -> list[TileMetadata]:
        """
        Read all tile metadata from GeoParquet

        Args:
            path: GeoParquet file path

        Returns:
            List of TileMetadata objects

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        if not Path(path).exists():
            raise FileNotFoundError(f"GeoParquet file not found: {path}")

        # Read GeoParquet
        gdf = gpd.read_parquet(path)

        # Convert to TileMetadata objects
        metadata_list = []
        for _, row in gdf.iterrows():
            metadata = TileMetadata(
                tile_id=row["tile_id"],
                year_month=row["year_month"],
                band=row["band"],
                bounds=tuple(row["geometry"].bounds),  # (minx, miny, maxx, maxy)
                num_observations=int(row["num_observations"]),
                min_value=float(row["min_value"]),
                max_value=float(row["max_value"]),
                mean_value=float(row["mean_value"]),
                cloud_cover=float(row["cloud_cover"]),
                product_id=row["product_id"],
                resolution=float(row["resolution"]),
                chunk_path=row["chunk_path"],
            )
            metadata_list.append(metadata)

        return metadata_list

    def query_by_bounds(
        self, path: str, bounds: tuple[float, float, float, float]
    ) -> list[TileMetadata]:
        """
        Query metadata by spatial bounds

        Args:
            path: GeoParquet file path
            bounds: Query bounds (minx, miny, maxx, maxy)

        Returns:
            List of TileMetadata that intersect with bounds
        """
        if not Path(path).exists():
            raise FileNotFoundError(f"GeoParquet file not found: {path}")

        # Read GeoParquet
        gdf = gpd.read_parquet(path)

        # Create query box
        minx, miny, maxx, maxy = bounds
        query_box = box(minx, miny, maxx, maxy)

        # Spatial filter
        gdf_filtered = gdf[gdf.intersects(query_box)]

        # Convert to TileMetadata
        metadata_list = []
        for _, row in gdf_filtered.iterrows():
            metadata = TileMetadata(
                tile_id=row["tile_id"],
                year_month=row["year_month"],
                band=row["band"],
                bounds=tuple(row["geometry"].bounds),
                num_observations=int(row["num_observations"]),
                min_value=float(row["min_value"]),
                max_value=float(row["max_value"]),
                mean_value=float(row["mean_value"]),
                cloud_cover=float(row["cloud_cover"]),
                product_id=row["product_id"],
                resolution=float(row["resolution"]),
                chunk_path=row["chunk_path"],
            )
            metadata_list.append(metadata)

        return metadata_list

    def query_by_tile_and_time(
        self, path: str, tile_id: str, year_month: str | None = None
    ) -> list[TileMetadata]:
        """
        Query metadata by tile ID and optional time

        Args:
            path: GeoParquet file path
            tile_id: Tile identifier
            year_month: Optional year-month filter (e.g., "2024-01")

        Returns:
            List of matching TileMetadata
        """
        if not Path(path).exists():
            raise FileNotFoundError(f"GeoParquet file not found: {path}")

        # Read GeoParquet
        gdf = gpd.read_parquet(path)

        # Filter by tile_id
        gdf_filtered = gdf[gdf["tile_id"] == tile_id]

        # Optional time filter
        if year_month:
            gdf_filtered = gdf_filtered[gdf_filtered["year_month"] == year_month]

        # Convert to TileMetadata
        metadata_list = []
        for _, row in gdf_filtered.iterrows():
            metadata = TileMetadata(
                tile_id=row["tile_id"],
                year_month=row["year_month"],
                band=row["band"],
                bounds=tuple(row["geometry"].bounds),
                num_observations=int(row["num_observations"]),
                min_value=float(row["min_value"]),
                max_value=float(row["max_value"]),
                mean_value=float(row["mean_value"]),
                cloud_cover=float(row["cloud_cover"]),
                product_id=row["product_id"],
                resolution=float(row["resolution"]),
                chunk_path=row["chunk_path"],
            )
            metadata_list.append(metadata)

        return metadata_list
