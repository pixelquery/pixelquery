"""
Internal Transactions Module

⚠️ PRIVATE API - Do not use directly!

NOTE: With Iceberg integration, ACID transactions are handled natively by PyIceberg.
The custom 2PC implementation has been removed. Iceberg provides:
- Optimistic concurrency control
- Snapshot isolation
- Automatic rollback on failure
- Time Travel via snapshots
"""

__all__ = []
