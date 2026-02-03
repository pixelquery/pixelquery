"""
Recovery CLI command

Diagnose and repair warehouse issues.
"""

import argparse
from pathlib import Path


def run_recovery(args: argparse.Namespace) -> None:
    """Run the recovery command"""
    warehouse_path = Path(args.warehouse)

    if not warehouse_path.exists():
        print(f"Error: Warehouse not found: {warehouse_path}")
        return

    from pixelquery.util.recovery import RecoveryTool

    tool = RecoveryTool(str(warehouse_path))

    if args.recovery_command == "diagnose":
        _run_diagnose(tool)
    elif args.recovery_command == "repair":
        _run_repair(tool, args)
    elif args.recovery_command == "verify":
        _run_verify(tool)
    else:
        _run_diagnose(tool)


def _run_diagnose(tool) -> None:
    """Run diagnostics"""
    print("Running warehouse diagnostics...")
    print()

    issues = tool.diagnose()

    status = issues["health_status"]
    status_symbol = {"healthy": "OK", "warning": "WARNING", "error": "ERROR"}
    print(f"Health Status: {status_symbol.get(status, status)}")
    print()

    backend = issues["backend_info"]
    print("Backend Information:")
    print(f"  Type: {backend.get('type', 'unknown')}")
    if backend.get("type") == "iceberg":
        print(f"  Catalog size: {backend.get('catalog_db_size_mb', 'N/A')} MB")
        print(f"  Snapshots: {backend.get('snapshot_count', 'N/A')}")
    elif backend.get("type") == "arrow":
        print(f"  Metadata size: {backend.get('metadata_size_mb', 'N/A')} MB")
    print()

    stats = issues["storage_stats"]
    print("Storage Statistics:")
    print(f"  Total size: {stats['total_mb']} MB")
    print(f"  Parquet files: {stats['parquet_mb']} MB")
    print(f"  Arrow files: {stats['arrow_mb']} MB")
    print(f"  Total files: {stats['file_count']}")
    print()

    if issues["orphaned_temp_files"]:
        print(f"Orphaned Temp Files: {len(issues['orphaned_temp_files'])}")
        for f in issues["orphaned_temp_files"][:5]:
            print(f"  - {f}")
        if len(issues["orphaned_temp_files"]) > 5:
            print(f"  ... and {len(issues['orphaned_temp_files']) - 5} more")
        print()

    if issues["warnings"]:
        print("Warnings:")
        for w in issues["warnings"]:
            print(f"  - {w}")
        print()

    if issues["orphaned_temp_files"]:
        print("Recommendations:")
        print("  - Run 'pixelquery recovery repair' to clean up temp files")
        print()


def _run_repair(tool, args: argparse.Namespace) -> None:
    """Run repairs"""
    print("Running warehouse repairs...")
    print()

    result = tool.repair(
        cleanup_temp=True,
        cleanup_orphaned_data=getattr(args, 'cleanup_orphaned', False),
    )

    print("Repair Results:")
    print(f"  Temp files removed: {result['temp_files_removed']}")
    print(f"  Orphaned data removed: {result['orphaned_data_removed']}")
    print()

    if result["actions_taken"]:
        print("Actions taken:")
        for action in result["actions_taken"][:10]:
            print(f"  - {action}")
        if len(result["actions_taken"]) > 10:
            print(f"  ... and {len(result['actions_taken']) - 10} more")


def _run_verify(tool) -> None:
    """Run integrity verification"""
    print("Verifying warehouse integrity...")
    print()

    result = tool.verify_integrity()

    status = result["status"]
    status_symbol = {"healthy": "PASS", "warning": "WARN", "error": "FAIL"}
    print(f"Integrity Status: {status_symbol.get(status, status)}")
    print()

    print("Checks performed:")
    for check in result["checks"]:
        print(f"  [PASS] {check}")
    print()

    if result["errors"]:
        print("Errors found:")
        for err in result["errors"]:
            print(f"  [FAIL] {err}")
