"""
Transaction Management Protocols

ACID transaction protocols for coordinating data writes.

NOTE: With Iceberg as the primary storage backend, transactions are handled
natively by PyIceberg. These protocols are retained for:
1. Documentation of transaction semantics
2. Potential future extension for non-Iceberg backends
3. Testing/mocking purposes

Iceberg provides built-in:
- Optimistic concurrency control
- Snapshot isolation
- Automatic rollback on failure
- Time Travel via snapshots
"""

from typing import Any, Protocol


class Transaction(Protocol):
    """
    ACID transaction for coordinating Arrow + GeoParquet + Iceberg writes

    Two-Phase Commit Protocol:
    1. PREPARE: Write all data to temporary paths (.tmp files)
    2. COMMIT: Iceberg optimistic concurrency commit
    3. FINALIZE: Atomically rename .tmp → final (if Iceberg succeeds)
    4. ROLLBACK: Delete .tmp files (if Iceberg fails)

    This ensures metadata (Iceberg) and pixel data (Arrow) stay consistent.
    """

    def stage_arrow_chunk(self, tile_id: str, year_month: str, data: dict[str, Any]) -> str:
        """
        Stage Arrow chunk to temporary path

        Args:
            tile_id: Geographic tile identifier
            year_month: Temporal partition (e.g., "2024-01")
            data: Pixel data + metadata to write

        Returns:
            Temporary file path
        """
        ...

    def stage_geoparquet_metadata(self, records: list[dict[str, Any]]) -> str:
        """
        Stage GeoParquet metadata to temporary path

        Args:
            records: Tile metadata records (geometry, stats, paths)

        Returns:
            Temporary file path
        """
        ...

    def commit(self) -> dict[str, Any]:
        """
        Commit transaction (Iceberg + finalize files)

        Returns:
            Commit result with snapshot_id and file paths

        Raises:
            TransactionError: If commit fails (triggers rollback)
        """
        ...

    def rollback(self) -> None:
        """
        Rollback transaction (delete temporary files)

        Called automatically on commit failure.
        """
        ...


class TransactionManager(Protocol):
    """Transaction factory"""

    def begin(self) -> Transaction:
        """Start a new transaction"""
        ...
