# Apache Iceberg Integration

PixelQuery uses **Apache Iceberg** as its storage layer, providing enterprise-grade ACID transactions, Time Travel, and schema evolution capabilities for satellite imagery data.

## Why Iceberg?

### ACID Transactions

Iceberg ensures data consistency through atomic transactions:

- **Atomicity**: Writes are all-or-nothing. No partial updates.
- **Consistency**: Metadata and data always in sync. No orphaned files.
- **Isolation**: Concurrent readers see consistent snapshots.
- **Durability**: Committed data persists across failures.

This is critical for scientific data where integrity is non-negotiable.

### Time Travel

Query historical states of your data without keeping multiple copies:

```python
from pixelquery.core import api

# List all snapshots
snapshots = api.get_snapshot_history("./warehouse")
for snapshot in snapshots:
    print(f"ID: {snapshot['snapshot_id']}, Created: {snapshot['timestamp_ms']}")

# Query as of a specific snapshot
dataset_v1 = api.open_dataset(
    "./warehouse",
    tile_id="x0024_y0041",
    as_of_snapshot_id=snapshots[0]["snapshot_id"]
)

# Query as of a timestamp
dataset_v2 = api.open_dataset(
    "./warehouse",
    tile_id="x0024_y0041",
    as_of_timestamp_ms=1704067200000  # January 1, 2024
)
```

**Use cases:**
- Audit data changes and track who modified what
- Reproduce results from past analyses
- Compare datasets across different processing versions
- Debug data quality issues by examining historical states

### Schema Evolution

Add or modify columns without breaking existing code:

```python
# Original schema: (time, y, x, red, nir)

# Later, add NDVI computed band
warehouse.add_column("ndvi", "float32")

# Existing queries still work - NDVI computed on-the-fly
# New queries can directly access NDVI
```

### Partitioning for Performance

Iceberg automatically manages hidden partitioning:

- **Time-based**: Monthly partitions for efficient time-range queries
- **Spatial**: Tile-based partitions for geographic queries
- **Compression**: Column-level compression with Zstandard (Zstd)

No need to manually organize data into directories.

## Storage Format

PixelQuery stores data in **Apache Parquet** format within Iceberg tables:

### Schema

Each observation (pixel at a time) is stored as a row:

```
observations/
├── timestamp (int64)          # Unix timestamp, milliseconds
├── tile_id (string)           # e.g., "x0024_y0041"
├── band (string)              # e.g., "red", "nir", "blue"
├── pixel_y (int32)            # Y coordinate (0-255 for 256x256 tile)
├── pixel_x (int32)            # X coordinate
├── value (float32)            # Pixel value
├── quality (int8)             # Quality flag (-1=nodata, 0=low, 1=high)
└── metadata (map<string, string>)  # Satellite ID, processing version, etc
```

### Advantages of Row-Per-Observation

- **Flexible**: Handle variable-length time series for each pixel
- **Queryable**: Use SQL to filter by band, quality, timestamp
- **Compressible**: Column-level compression (Zstd) for efficient storage
- **Append-friendly**: New observations added as new rows

### Metadata Structure

Iceberg metadata hierarchy:

```
warehouse/
├── metadata/
│   ├── 00000-<uuid>.metadata.json          # Current metadata
│   ├── snap-<id>-<uuid>.avro              # Snapshot manifest
│   └── ...
├── data/
│   ├── 00000-<uuid>.parquet               # Parquet files
│   ├── 00001-<uuid>.parquet
│   └── ...
└── metadata.sqlite                         # Iceberg catalog (local SQLite)
```

## Using Time Travel

### Common Patterns

#### 1. Compare Two Versions

```python
from pixelquery.core import api

snapshots = api.get_snapshot_history("./warehouse")

# Get two consecutive snapshots
v1_id = snapshots[0]["snapshot_id"]
v2_id = snapshots[1]["snapshot_id"]

# Load both versions
data_v1 = api.open_dataset("./warehouse", tile_id="x0024_y0041",
                           as_of_snapshot_id=v1_id)
data_v2 = api.open_dataset("./warehouse", tile_id="x0024_y0041",
                           as_of_snapshot_id=v2_id)

# Compute differences
import numpy as np
red_v1 = data_v1["red"].to_numpy()
red_v2 = data_v2["red"].to_numpy()
diff = red_v2 - red_v1

print(f"Max change: {np.max(np.abs(diff))}")
```

#### 2. Audit Changes

```python
snapshots = api.get_snapshot_history("./warehouse")

for i, snapshot in enumerate(snapshots):
    print(f"\nSnapshot {i}")
    print(f"  ID: {snapshot['snapshot_id']}")
    print(f"  Timestamp: {snapshot['timestamp_ms']}")

    # Metadata about what changed
    if 'summary' in snapshot:
        print(f"  Summary: {snapshot['summary']}")
```

#### 3. Rollback to Previous Version

```python
from pixelquery.core import api

snapshots = api.get_snapshot_history("./warehouse")
previous_version_id = snapshots[1]["snapshot_id"]

# Explicitly query old version
old_data = api.open_dataset(
    "./warehouse",
    tile_id="x0024_y0041",
    as_of_snapshot_id=previous_version_id
)

# Or, for true rollback (reset current table to old version):
api.rollback_to_snapshot("./warehouse", previous_version_id)
```

#### 4. Time-Based Queries

```python
import datetime
from pixelquery.core import api

# Query as of a specific date
target_date = datetime.datetime(2024, 6, 15)
target_ms = int(target_date.timestamp() * 1000)

data = api.open_dataset(
    "./warehouse",
    tile_id="x0024_y0041",
    as_of_timestamp_ms=target_ms
)
```

## Migration from Arrow Backend

If you're migrating from PixelQuery's previous Arrow IPC + custom 2PC implementation:

### Using the CLI

```bash
# Migrate entire warehouse
pixelquery migrate ./warehouse

# Check migration status
pixelquery info ./warehouse
```

### Using the API

```python
from pixelquery.io import migrate_to_iceberg

# Migrate a single tile
migrate_to_iceberg(
    warehouse_path="./warehouse",
    tile_ids=["x0024_y0041"],
    delete_source=False  # Keep Arrow files as backup
)

# Migrate all tiles
migrate_to_iceberg(
    warehouse_path="./warehouse",
    tile_ids="all",
    delete_source=True  # Delete Arrow files after successful migration
)
```

### What Happens During Migration

1. **Read** existing Arrow chunks by tile and timestamp
2. **Convert** to Parquet format with new schema
3. **Create** Iceberg table with metadata
4. **Verify** data integrity (checksums, row counts)
5. **Clean up** Arrow files (optional)

### Verification

After migration, verify everything works:

```python
from pixelquery.core import api

# Check warehouse health
health = api.check_warehouse_health("./warehouse")
print(f"Total tiles: {health['num_tiles']}")
print(f"Total snapshots: {health['num_snapshots']}")
print(f"Storage size: {health['storage_bytes'] / 1e9:.2f} GB")

# Spot-check a tile
data = api.open_dataset("./warehouse", tile_id="x0024_y0041")
print(f"Time range: {data['red'].time.min()} to {data['red'].time.max()}")
print(f"Shape: {data['red'].shape}")
```

## Recovery and Diagnostics

### Check Warehouse Health

```bash
pixelquery info ./warehouse

# Output:
# Warehouse: ./warehouse
# Format: Iceberg
# Catalog: SQLite (metadata.sqlite)
# Tables: 1
#
# observations table:
#   - Snapshots: 5
#   - Latest snapshot ID: 4567890123
#   - Rows: 524,288,000
#   - Parquet files: 12
#   - Size on disk: 45.3 GB
```

### Diagnose Issues

```bash
pixelquery recovery diagnose ./warehouse

# Checks for:
# - Missing Parquet files referenced in metadata
# - Orphaned Parquet files not in any snapshot
# - Corrupted metadata files
# - Integrity violations (duplicate rows, etc)
```

### Repair Warehouse

```bash
# Dry run - see what would be fixed
pixelquery recovery repair ./warehouse --dry-run

# Actual repair - removes orphans, fixes metadata inconsistencies
pixelquery recovery repair ./warehouse
```

## Performance Characteristics

### Write Performance

- **Append**: ~0.5-2s per tile per month (depending on resolution)
- **Schema changes**: Instant (only metadata updated)
- **Concurrent writes**: Safe (ACID isolation)

### Query Performance

- **Time-series queries** (full tile, many months): 5-30x faster than COG+STAC
- **Spatial queries** (bounding box): Efficient with tile partitioning
- **Time Travel queries**: Same speed as current version (no overhead)

### Storage Efficiency

- **Compression**: Parquet Zstd typically achieves 8-12x compression
- **No duplication**: Time Travel doesn't require multiple copies
- **Deduplication**: Identical pixels across timestamps stored once via compression

## Best Practices

### For Data Integrity

1. **Always use transactions** when writing multiple tiles
2. **Enable checksums** in Iceberg configuration
3. **Regular backups** - Keep snapshots for at least 30 days
4. **Audit snapshots** - Review who created each snapshot

### For Performance

1. **Partition by tile** - Queries on single tiles are fastest
2. **Use time ranges** - Avoid scanning entire table
3. **Batch ingestion** - Write multiple observations per transaction
4. **Monitor snapshot count** - Expire old snapshots periodically

### For Data Quality

1. **Set quality flags** - Mark nodata, low-quality observations
2. **Validate on ingestion** - Check for NaN, out-of-range values
3. **Time Travel inspection** - Compare versions to detect anomalies
4. **Metadata tracking** - Record processing version, satellite ID, etc

## Advanced: Custom Iceberg Configuration

For large-scale deployments, customize Iceberg settings:

```python
from pixelquery.core import api

config = {
    "iceberg.parquet.compression": "zstd",           # Compression type
    "iceberg.parquet.compression-level": 6,          # Compression level
    "iceberg.split.open-file-cost": 4 * 1024 * 1024, # 4MB file cost
    "iceberg.table.delete.mode": "copy-on-write",     # Full data copy
}

api.create_warehouse("./warehouse", config=config)
```

## References

- **[Apache Iceberg Docs](https://iceberg.apache.org/)** - Official documentation
- **[Iceberg Format Spec](https://iceberg.apache.org/docs/latest/spec/)** - Technical specification
- **[PyIceberg](https://py.iceberg.apache.org/)** - Python Iceberg client
- **[Parquet Format](https://parquet.apache.org/)** - Columnar storage format
