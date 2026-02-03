"""
Info CLI command

Shows warehouse information including storage backend, statistics, and snapshots.
"""

import argparse
from pathlib import Path


def run_info(args: argparse.Namespace) -> None:
    """Run the info command"""
    warehouse_path = Path(args.warehouse)

    if not warehouse_path.exists():
        print(f"Error: Warehouse not found: {warehouse_path}")
        return

    print(f"Warehouse: {warehouse_path.resolve()}")
    print()

    iceberg_db = warehouse_path / "catalog.db"
    arrow_metadata = warehouse_path / "metadata.parquet"

    if iceberg_db.exists():
        _show_iceberg_info(warehouse_path, args.snapshots)
    elif arrow_metadata.exists():
        _show_arrow_info(warehouse_path)
    else:
        print("Status: Empty or uninitialized warehouse")


def _show_iceberg_info(warehouse_path: Path, show_snapshots: bool) -> None:
    """Show Iceberg warehouse information"""
    print("Backend: Apache Iceberg")
    print()

    try:
        from pixelquery._internal.storage.iceberg_storage import IcebergStorageManager

        storage = IcebergStorageManager(str(warehouse_path))
        storage.initialize()

        stats = storage.get_table_stats()

        print("Table Statistics:")
        print(f"  Snapshots: {stats['total_snapshots']}")
        print(f"  Current Snapshot: {stats['current_snapshot_id']}")
        print(f"  Schema Fields: {len(stats['schema_fields'])}")
        print(f"  Partition Fields: {', '.join(stats['partition_fields'])}")
        print(f"  Location: {stats['table_location']}")
        print()

        scan = storage.table.scan(selected_fields=["tile_id"])
        record_count = scan.to_arrow().num_rows
        print(f"Total Records: {record_count:,}")

        import pyarrow.compute as pc
        scan = storage.table.scan(selected_fields=["tile_id"])
        df = scan.to_arrow()
        unique_tiles = len(pc.unique(df.column("tile_id")).to_pylist()) if df.num_rows > 0 else 0
        print(f"Unique Tiles: {unique_tiles:,}")

        scan = storage.table.scan(selected_fields=["band"])
        df = scan.to_arrow()
        unique_bands = pc.unique(df.column("band")).to_pylist() if df.num_rows > 0 else []
        print(f"Bands: {', '.join(sorted(unique_bands))}")
        print()

        if show_snapshots:
            snapshots = storage.get_snapshot_history()
            print("Snapshot History:")
            print("-" * 60)
            for snap in snapshots[:10]:
                ts = snap["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                op = snap["operation"]
                sid = snap["snapshot_id"]
                summary = snap.get("summary", {})
                added = summary.get("added-data-files", "?")
                print(f"  {ts}  {op:10}  ID: {sid}  (+{added} files)")

            if len(snapshots) > 10:
                print(f"  ... and {len(snapshots) - 10} more")
            print()

    except Exception as e:
        print(f"Error reading Iceberg metadata: {e}")


def _show_arrow_info(warehouse_path: Path) -> None:
    """Show Arrow warehouse information"""
    print("Backend: Arrow IPC + GeoParquet")
    print()

    try:
        from pixelquery._internal.storage.geoparquet import GeoParquetReader

        reader = GeoParquetReader()
        metadata_path = warehouse_path / "metadata.parquet"
        metadata_list = reader.read_metadata(str(metadata_path))

        print(f"Total Metadata Records: {len(metadata_list):,}")

        unique_tiles = set(m.tile_id for m in metadata_list)
        print(f"Unique Tiles: {len(unique_tiles):,}")

        unique_bands = sorted(set(m.band for m in metadata_list))
        print(f"Bands: {', '.join(unique_bands)}")

        year_months = sorted(set(m.year_month for m in metadata_list))
        if year_months:
            print(f"Time Range: {year_months[0]} to {year_months[-1]}")

        arrow_files = list(warehouse_path.glob("tiles/**/*.arrow"))
        print(f"Arrow Chunk Files: {len(arrow_files):,}")

    except Exception as e:
        print(f"Error reading Arrow metadata: {e}")
