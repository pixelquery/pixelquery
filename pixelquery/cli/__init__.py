"""
PixelQuery CLI Entry Points

Provides command-line interface for:
- migrate: Migrate Arrow storage to Iceberg
- recovery: Diagnose and repair warehouse issues
- info: Show warehouse information
"""

import argparse
import logging
import sys


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="PixelQuery - Satellite imagery storage engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  pixelquery info ./warehouse              Show warehouse information
  pixelquery migrate ./warehouse           Migrate Arrow to Iceberg
  pixelquery recovery diagnose ./warehouse Diagnose warehouse issues
  pixelquery recovery repair ./warehouse   Repair warehouse issues
        """,
    )

    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Info command
    info_parser = subparsers.add_parser("info", help="Show warehouse information")
    info_parser.add_argument("warehouse", help="Warehouse path")
    info_parser.add_argument(
        "--snapshots", action="store_true", help="Show snapshot history (Time Travel)"
    )

    # Migrate command
    migrate_parser = subparsers.add_parser("migrate", help="Migrate Arrow storage to Iceberg")
    migrate_parser.add_argument("warehouse", help="Warehouse path")
    migrate_parser.add_argument(
        "--batch-size", type=int, default=100, help="Records per batch (default: 100)"
    )
    migrate_parser.add_argument(
        "--no-backup", action="store_true", help="Skip backup of original files"
    )
    migrate_parser.add_argument(
        "--dry-run", action="store_true", help="Simulate migration without writing"
    )

    # Recovery command
    recovery_parser = subparsers.add_parser("recovery", help="Diagnose and repair warehouse issues")
    recovery_parser.add_argument("warehouse", help="Warehouse path")
    recovery_subparsers = recovery_parser.add_subparsers(dest="recovery_command")

    _diagnose_parser = recovery_subparsers.add_parser("diagnose", help="Diagnose warehouse issues")

    repair_parser = recovery_subparsers.add_parser("repair", help="Repair warehouse issues")
    repair_parser.add_argument(
        "--cleanup-orphaned", action="store_true", help="Remove unreferenced data files (CAUTION)"
    )

    _verify_parser = recovery_subparsers.add_parser("verify", help="Verify warehouse integrity")

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level, format="%(message)s" if not args.verbose else "%(levelname)s: %(message)s"
    )

    if args.command == "info":
        from pixelquery.cli.info import run_info

        run_info(args)
    elif args.command == "migrate":
        from pixelquery.cli.migrate import run_migrate

        run_migrate(args)
    elif args.command == "recovery":
        from pixelquery.cli.recovery import run_recovery

        run_recovery(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
