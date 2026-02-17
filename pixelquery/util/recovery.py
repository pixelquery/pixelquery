"""
Recovery CLI Utilities

Level 2 Recovery: Query + Retry

Features:
- Diagnose warehouse issues
- Cleanup orphaned temp files
- Storage statistics
"""

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class RecoveryTool:
    """
    Recovery utilities for PixelQuery warehouse

    Level 2 capabilities:
    - Query incomplete operations
    - Cleanup orphaned temp files
    - Storage statistics

    Examples:
        >>> recovery = RecoveryTool("warehouse")
        >>>
        >>> # Diagnose issues
        >>> issues = recovery.diagnose()
        >>> print(issues)
        {'orphaned_temp_files': [...], 'storage_stats': {...}}
        >>>
        >>> # Fix issues
        >>> result = recovery.repair()
        >>> print(result)
        {'temp_files_removed': 3}
    """

    def __init__(self, warehouse_path: str):
        """Initialize recovery tool"""
        self.warehouse_path = Path(warehouse_path)

    def diagnose(self) -> dict[str, Any]:
        """Diagnose warehouse for issues"""
        issues: dict[str, Any] = {
            "orphaned_temp_files": [],
            "storage_stats": {},
            "backend_info": {},
            "health_status": "healthy",
            "warnings": [],
        }

        temp_files = list(self.warehouse_path.glob("**/*.tmp"))
        issues["orphaned_temp_files"] = [str(f) for f in temp_files]
        if temp_files:
            issues["warnings"].append(f"Found {len(temp_files)} orphaned temp files")
            issues["health_status"] = "warning"

        total_size = 0
        parquet_size = 0
        arrow_size = 0
        file_count = 0

        for f in self.warehouse_path.rglob("*"):
            if f.is_file():
                try:
                    file_count += 1
                    size = f.stat().st_size
                    total_size += size

                    if f.suffix == ".parquet":
                        parquet_size += size
                    elif f.suffix == ".arrow":
                        arrow_size += size
                except OSError:
                    pass

        issues["storage_stats"] = {
            "total_bytes": total_size,
            "total_mb": round(total_size / 1024 / 1024, 2),
            "parquet_bytes": parquet_size,
            "parquet_mb": round(parquet_size / 1024 / 1024, 2),
            "arrow_bytes": arrow_size,
            "arrow_mb": round(arrow_size / 1024 / 1024, 2),
            "file_count": file_count,
        }

        iceberg_db = self.warehouse_path / "catalog.db"
        arrow_metadata = self.warehouse_path / "metadata.parquet"

        if iceberg_db.exists():
            issues["backend_info"]["type"] = "iceberg"
            issues["backend_info"]["catalog_db_size_mb"] = round(
                iceberg_db.stat().st_size / 1024 / 1024, 2
            )

            try:
                from pixelquery._internal.storage.iceberg_storage import IcebergStorageManager

                storage = IcebergStorageManager(str(self.warehouse_path))
                storage.initialize()
                snapshots = storage.get_snapshot_history()
                issues["backend_info"]["snapshot_count"] = len(snapshots)
                if snapshots:
                    issues["backend_info"]["latest_snapshot"] = snapshots[0]
            except Exception as e:
                issues["warnings"].append(f"Could not read Iceberg metadata: {e}")
                issues["health_status"] = "warning"

        elif arrow_metadata.exists():
            issues["backend_info"]["type"] = "arrow"
            issues["backend_info"]["metadata_size_mb"] = round(
                arrow_metadata.stat().st_size / 1024 / 1024, 2
            )
        else:
            issues["backend_info"]["type"] = "uninitialized"
            issues["warnings"].append("No catalog found - warehouse may be empty or corrupted")

        return issues

    def repair(
        self,
        cleanup_temp: bool = True,
        cleanup_orphaned_data: bool = False,
    ) -> dict[str, Any]:
        """Repair warehouse issues"""
        results: dict[str, Any] = {
            "temp_files_removed": 0,
            "orphaned_data_removed": 0,
            "actions_taken": [],
        }

        if cleanup_temp:
            temp_files = list(self.warehouse_path.glob("**/*.tmp"))
            for f in temp_files:
                try:
                    f.unlink()
                    results["temp_files_removed"] += 1
                    results["actions_taken"].append(f"Removed: {f}")
                except Exception as e:
                    logger.warning(f"Failed to remove {f}: {e}")

        if cleanup_orphaned_data:
            logger.warning("Orphaned data cleanup is experimental and not fully implemented")
            results["actions_taken"].append("Orphaned data cleanup skipped (not implemented)")

        return results

    def get_retry_info(self) -> dict[str, Any]:
        """Get information about operations that can be retried"""
        info = {
            "note": "Iceberg provides automatic rollback for failed operations",
            "recommendation": "Re-run failed ingestion with same parameters",
            "failed_operations": [],
        }

        iceberg_db = self.warehouse_path / "catalog.db"
        if iceberg_db.exists():
            info["backend"] = "iceberg"
            info["message"] = (
                "Iceberg handles transaction recovery automatically. "
                "Any failed write operations have been rolled back. "
                "Simply retry the operation that failed."
            )
        else:
            info["backend"] = "arrow"
            info["message"] = (
                "Arrow backend does not support automatic recovery. "
                "Check for incomplete .tmp files and manually clean up if needed."
            )

        return info

    def verify_integrity(self) -> dict[str, Any]:
        """Verify warehouse integrity"""
        results: dict[str, Any] = {
            "status": "healthy",
            "checks": [],
            "errors": [],
        }

        if not self.warehouse_path.exists():
            results["status"] = "error"
            results["errors"].append("Warehouse directory does not exist")
            return results
        results["checks"].append("Warehouse directory exists")

        iceberg_db = self.warehouse_path / "catalog.db"
        arrow_metadata = self.warehouse_path / "metadata.parquet"

        if iceberg_db.exists():
            results["checks"].append("Iceberg catalog found")

            try:
                from pixelquery._internal.storage.iceberg_storage import IcebergStorageManager

                storage = IcebergStorageManager(str(self.warehouse_path))
                storage.initialize()
                stats = storage.get_table_stats()
                results["checks"].append(
                    f"Iceberg table readable ({stats['total_snapshots']} snapshots)"
                )
            except Exception as e:
                results["status"] = "error"
                results["errors"].append(f"Cannot read Iceberg table: {e}")

        elif arrow_metadata.exists():
            results["checks"].append("Arrow metadata found")

            try:
                from pixelquery._internal.storage.geoparquet import GeoParquetReader

                reader = GeoParquetReader()
                metadata = reader.read_metadata(str(arrow_metadata))
                results["checks"].append(f"GeoParquet metadata readable ({len(metadata)} records)")
            except Exception as e:
                results["status"] = "error"
                results["errors"].append(f"Cannot read GeoParquet metadata: {e}")
        else:
            results["status"] = "warning"
            results["checks"].append("No catalog found (warehouse may be empty)")

        return results
