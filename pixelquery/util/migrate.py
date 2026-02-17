"""
Arrow to Iceberg Migration Utility

Migrates existing Arrow IPC storage to Iceberg tables.

Features:
- Incremental migration (batch by batch)
- Validation (record count comparison)
- Backup (preserves original files)
- Progress callbacks
"""

import logging
import shutil
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class MigrationTool:
    """
    Migrates Arrow IPC data to Iceberg tables

    Migration process:
    1. Scan all Arrow chunk files
    2. Read each chunk and convert to Iceberg format
    3. Append to Iceberg table in batches
    4. Verify record counts match
    5. Optionally archive old Arrow files

    Examples:
        >>> tool = MigrationTool("warehouse")
        >>>
        >>> # Check if migration is needed
        >>> status = tool.check_migration_needed()
        >>> print(status)
        {'needed': True, 'arrow_chunks': 42, 'iceberg_records': 0}
        >>>
        >>> # Run migration with progress callback
        >>> result = tool.migrate(
        ...     batch_size=1000,
        ...     on_progress=lambda p: print(f"{p:.1f}% complete")
        ... )
        >>> print(result)
        {'status': 'success', 'records_migrated': 4200}
    """

    def __init__(self, warehouse_path: str, backup: bool = True):
        """
        Initialize migration tool

        Args:
            warehouse_path: Path to warehouse directory
            backup: If True, backup Arrow files before removal
        """
        self.warehouse_path = Path(warehouse_path)
        self.backup = backup
        self._arrow_reader = None
        self._geoparquet_reader = None
        self._iceberg_writer = None

    def check_migration_needed(self) -> dict[str, Any]:
        """
        Check migration status

        Returns:
            Dictionary with:
            - needed: bool (True if Arrow files exist and no Iceberg records)
            - arrow_chunks: int (count of Arrow files)
            - iceberg_records: int (count of Iceberg records)
            - arrow_metadata_exists: bool
            - iceberg_catalog_exists: bool
        """
        arrow_chunks = list(self.warehouse_path.glob("tiles/**/*.arrow"))
        arrow_metadata = self.warehouse_path / "metadata.parquet"
        iceberg_db = self.warehouse_path / "catalog.db"

        iceberg_records = 0
        if iceberg_db.exists():
            try:
                from pixelquery._internal.storage.iceberg_storage import IcebergStorageManager

                storage = IcebergStorageManager(str(self.warehouse_path))
                storage.initialize()
                scan = storage.table.scan(selected_fields=["tile_id"])
                iceberg_records = scan.to_arrow().num_rows
            except Exception as e:
                logger.warning(f"Could not count Iceberg records: {e}")

        return {
            "needed": len(arrow_chunks) > 0 and iceberg_records == 0,
            "arrow_chunks": len(arrow_chunks),
            "iceberg_records": iceberg_records,
            "arrow_metadata_exists": arrow_metadata.exists(),
            "iceberg_catalog_exists": iceberg_db.exists(),
        }

    def migrate(
        self,
        batch_size: int = 100,
        on_progress: Callable[[float], None] | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """
        Execute migration

        Args:
            batch_size: Number of observations per Iceberg commit
            on_progress: Callback for progress updates (0-100)
            dry_run: If True, only simulate migration without writing

        Returns:
            Dictionary with migration results
        """
        status = self.check_migration_needed()

        if not status["needed"]:
            return {
                "status": "nothing_to_migrate",
                "records_migrated": 0,
                "arrow_files_processed": 0,
                "message": "No Arrow files to migrate or Iceberg already populated",
            }

        from pixelquery._internal.storage.arrow_chunk import ArrowChunkReader
        from pixelquery._internal.storage.geoparquet import GeoParquetReader
        from pixelquery.io.iceberg_writer import IcebergPixelWriter

        arrow_reader = ArrowChunkReader()
        geoparquet_reader = GeoParquetReader()

        if not dry_run:
            iceberg_writer = IcebergPixelWriter(str(self.warehouse_path))
        else:
            iceberg_writer = None

        arrow_files = list(self.warehouse_path.glob("tiles/**/*.arrow"))

        if not arrow_files:
            return {"status": "nothing_to_migrate", "records_migrated": 0}

        logger.info(f"Found {len(arrow_files)} Arrow chunk files to migrate")

        metadata_map = {}
        metadata_path = self.warehouse_path / "metadata.parquet"
        if metadata_path.exists():
            try:
                metadata_list = geoparquet_reader.read_metadata(str(metadata_path))
                for m in metadata_list:
                    key = (m.tile_id, m.year_month, m.band)
                    metadata_map[key] = m
            except Exception as e:
                logger.warning(f"Could not read GeoParquet metadata: {e}")

        records_migrated = 0
        observations_batch = []
        errors = []

        for i, arrow_file in enumerate(arrow_files):
            try:
                parts = arrow_file.relative_to(self.warehouse_path / "tiles").parts
                if len(parts) != 3:
                    logger.warning(f"Skipping unexpected path: {arrow_file}")
                    continue

                tile_id = parts[0]
                year_month = parts[1]
                band = parts[2].replace(".arrow", "")

                data, chunk_metadata = arrow_reader.read_chunk(str(arrow_file))

                meta_key = (tile_id, year_month, band)
                extra_meta = metadata_map.get(meta_key)

                for j, time in enumerate(data.get("time", [])):
                    if isinstance(time, str):
                        time = datetime.fromisoformat(time)
                    elif not isinstance(time, datetime):
                        time = datetime.fromisoformat(str(time))

                    if time.tzinfo is None:
                        time = time.replace(tzinfo=UTC)

                    obs = {
                        "tile_id": tile_id,
                        "band": band,
                        "time": time,
                        "pixels": data["pixels"][j],
                        "mask": data["mask"][j],
                        "product_id": chunk_metadata.get("product_id", "unknown"),
                        "resolution": float(chunk_metadata.get("resolution", 10.0)),
                        "bounds": extra_meta.bounds if extra_meta else None,
                        "cloud_cover": extra_meta.cloud_cover if extra_meta else None,
                    }
                    observations_batch.append(obs)

                    if len(observations_batch) >= batch_size:
                        if not dry_run and iceberg_writer:
                            iceberg_writer.write_observations(observations_batch)
                        records_migrated += len(observations_batch)
                        observations_batch = []

            except Exception as e:
                error_msg = f"Error processing {arrow_file}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)

            if on_progress:
                progress = (i + 1) / len(arrow_files) * 100
                on_progress(progress)

        if observations_batch:
            if not dry_run and iceberg_writer:
                iceberg_writer.write_observations(observations_batch)
            records_migrated += len(observations_batch)

        if not dry_run:
            verification = self._verify_migration(len(arrow_files))
        else:
            verification = {"dry_run": True}

        backup_path = None
        if self.backup and not dry_run:
            backup_path = self._backup_arrow_files()

        return {
            "status": "success" if not errors else "completed_with_errors",
            "records_migrated": records_migrated,
            "arrow_files_processed": len(arrow_files),
            "verification": verification,
            "backup_path": str(backup_path) if backup_path else None,
            "errors": errors,
        }

    def _verify_migration(self, expected_files: int) -> dict[str, Any]:
        """Verify migration integrity"""
        try:
            from pixelquery._internal.storage.iceberg_storage import IcebergStorageManager

            storage = IcebergStorageManager(str(self.warehouse_path))
            storage.initialize()

            scan = storage.table.scan(selected_fields=["tile_id", "band", "year_month"])
            df = scan.to_pandas()

            unique_chunks = len(df.groupby(["tile_id", "band", "year_month"]))

            return {
                "iceberg_total_records": len(df),
                "iceberg_unique_chunks": unique_chunks,
                "expected_chunks": expected_files,
                "match": unique_chunks == expected_files,
            }
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return {"error": str(e)}

    def _backup_arrow_files(self) -> Path | None:
        """Move Arrow files to backup directory"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = self.warehouse_path / f"backup_arrow_{timestamp}"
        backup_dir.mkdir(exist_ok=True)

        tiles_dir = self.warehouse_path / "tiles"
        if tiles_dir.exists():
            shutil.move(str(tiles_dir), str(backup_dir / "tiles"))
            logger.info(f"Backed up tiles to {backup_dir / 'tiles'}")

        metadata_path = self.warehouse_path / "metadata.parquet"
        if metadata_path.exists():
            shutil.move(str(metadata_path), str(backup_dir / "metadata.parquet"))
            logger.info(f"Backed up metadata.parquet to {backup_dir}")

        return backup_dir

    def rollback(self, backup_path: str) -> dict[str, Any]:
        """Rollback migration from backup"""
        backup_dir = Path(backup_path)

        if not backup_dir.exists():
            return {"status": "error", "message": f"Backup not found: {backup_path}"}

        backup_tiles = backup_dir / "tiles"
        if backup_tiles.exists():
            target_tiles = self.warehouse_path / "tiles"
            if target_tiles.exists():
                shutil.rmtree(str(target_tiles))
            shutil.move(str(backup_tiles), str(target_tiles))

        backup_metadata = backup_dir / "metadata.parquet"
        if backup_metadata.exists():
            target_metadata = self.warehouse_path / "metadata.parquet"
            if target_metadata.exists():
                target_metadata.unlink()
            shutil.move(str(backup_metadata), str(target_metadata))

        return {"status": "success", "message": "Arrow files restored from backup"}
