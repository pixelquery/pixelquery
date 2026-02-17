"""
Migrate CLI command

Migrates Arrow IPC storage to Apache Iceberg.
"""

import argparse
import sys
from pathlib import Path


def run_migrate(args: argparse.Namespace) -> None:
    """Run the migrate command"""
    warehouse_path = Path(args.warehouse)

    if not warehouse_path.exists():
        print(f"Error: Warehouse not found: {warehouse_path}")
        sys.exit(1)

    from pixelquery.util.migrate import MigrationTool

    tool = MigrationTool(str(warehouse_path), backup=not args.no_backup)

    status = tool.check_migration_needed()

    print(f"Warehouse: {warehouse_path.resolve()}")
    print()
    print("Migration Status:")
    print(f"  Arrow chunk files: {status['arrow_chunks']}")
    print(f"  Iceberg records: {status['iceberg_records']}")
    print(f"  Arrow metadata exists: {status['arrow_metadata_exists']}")
    print(f"  Iceberg catalog exists: {status['iceberg_catalog_exists']}")
    print()

    if not status["needed"]:
        print("No migration needed.")
        if status["iceberg_records"] > 0:
            print("Iceberg catalog already populated.")
        elif status["arrow_chunks"] == 0:
            print("No Arrow files to migrate.")
        return

    if args.dry_run:
        print("DRY RUN - No changes will be made")
        print()
    else:
        print(f"Ready to migrate {status['arrow_chunks']} Arrow chunk files to Iceberg.")
        if not args.no_backup:
            print("Original files will be backed up.")
        else:
            print("WARNING: Original files will NOT be backed up!")

        try:
            response = input("\nProceed with migration? [y/N] ")
            if response.lower() != "y":
                print("Migration cancelled.")
                return
        except (EOFError, KeyboardInterrupt):
            print("\nMigration cancelled.")
            return

    def progress_callback(percent: float) -> None:
        bar_width = 40
        filled = int(bar_width * percent / 100)
        bar = "=" * filled + "-" * (bar_width - filled)
        print(f"\rMigrating: [{bar}] {percent:.1f}%", end="", flush=True)

    print()

    result = tool.migrate(
        batch_size=args.batch_size,
        on_progress=progress_callback,
        dry_run=args.dry_run,
    )

    print()
    print()

    print("Migration Results:")
    print(f"  Status: {result['status']}")
    print(f"  Records migrated: {result['records_migrated']:,}")
    print(f"  Arrow files processed: {result['arrow_files_processed']}")

    if result.get("verification"):
        v = result["verification"]
        if "dry_run" not in v:
            print()
            print("Verification:")
            print(f"  Iceberg records: {v.get('iceberg_total_records', 'N/A')}")
            print(f"  Chunks match: {v.get('match', 'N/A')}")

    if result.get("backup_path"):
        print()
        print(f"Backup created at: {result['backup_path']}")

    if result.get("errors"):
        print()
        print("Errors:")
        for err in result["errors"][:5]:
            print(f"  - {err}")
        if len(result["errors"]) > 5:
            print(f"  ... and {len(result['errors']) - 5} more")
