"""
Iceberg Schema Definitions for PixelQuery

Defines the schema and partition specification for pixel data storage
in Apache Iceberg format.

Schema Design:
- Row-per-observation: Each row represents one acquisition of a tile/band
- Partitioned by tile_id, band, year_month for efficient query pruning
- Supports Time Travel via Iceberg snapshots
"""

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    FloatType,
    IntegerType,
    ListType,
    NestedField,
    StringType,
    TimestamptzType,
)

# Default tile size in pixels (256x256)
TILE_SIZE_PIXELS = 256

# Pixel data schema
# Each row represents one observation (one tile, one band, one timestamp)
PIXEL_DATA_SCHEMA = Schema(
    # Partition keys
    NestedField(1, "tile_id", StringType(), required=True),
    NestedField(2, "band", StringType(), required=True),
    NestedField(3, "year_month", StringType(), required=True),  # Format: "2024-01"
    # Temporal data
    NestedField(4, "time", TimestamptzType(), required=True),
    # Pixel data (flattened arrays)
    NestedField(
        5,
        "pixels",
        ListType(element_id=6, element_type=IntegerType(), element_required=True),
        required=True,
    ),
    NestedField(
        7,
        "mask",
        ListType(element_id=8, element_type=BooleanType(), element_required=True),
        required=True,
    ),
    # Metadata columns
    NestedField(9, "product_id", StringType(), required=True),
    NestedField(10, "resolution", FloatType(), required=True),
    NestedField(11, "bounds_wkb", BinaryType(), required=False),  # WKB geometry
    NestedField(12, "num_pixels", IntegerType(), required=True),
    # Statistics (computed from valid pixels)
    NestedField(13, "min_value", FloatType(), required=False),
    NestedField(14, "max_value", FloatType(), required=False),
    NestedField(15, "mean_value", FloatType(), required=False),
    NestedField(16, "cloud_cover", FloatType(), required=False),
    # Optional metadata
    NestedField(17, "crs", StringType(), required=False),  # Coordinate reference system
    NestedField(18, "transform", StringType(), required=False),  # Affine transform JSON
    NestedField(19, "source_file", StringType(), required=False),  # Original COG path
    NestedField(20, "ingestion_time", TimestamptzType(), required=False),
)

# Partition specification
# Partitions by tile_id, band, and year_month for efficient query pruning
PIXEL_DATA_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="tile_id"),
    PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="band"),
    PartitionField(source_id=3, field_id=1002, transform=IdentityTransform(), name="year_month"),
)

# Table properties for optimal performance
PIXEL_DATA_TABLE_PROPERTIES = {
    # File format
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.parquet.compression-level": "3",
    # Target file size (64MB for good compression vs query performance)
    "write.target-file-size-bytes": str(64 * 1024 * 1024),
    # Snapshot retention (keep last 10 snapshots for Time Travel)
    "history.expire.max-snapshot-age-ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
    "history.expire.min-snapshots-to-keep": "10",
    # Metadata
    "table.type": "pixel_data",
    "table.version": "1.0.0",
}


def get_pixel_data_schema() -> Schema:
    """Get the pixel data schema."""
    return PIXEL_DATA_SCHEMA


def get_pixel_data_partition_spec() -> PartitionSpec:
    """Get the pixel data partition specification."""
    return PIXEL_DATA_PARTITION_SPEC


def get_pixel_data_table_properties() -> dict:
    """Get the recommended table properties."""
    return PIXEL_DATA_TABLE_PROPERTIES.copy()
