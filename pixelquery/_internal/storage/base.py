"""
Storage Backend Protocol

Abstract storage interface for multiple backends (local, S3, Azure, GCS).
"""

from typing import Protocol, List


class StorageBackend(Protocol):
    """
    Abstract storage interface

    Enables multiple storage backends (local filesystem, S3, Azure, GCS)
    while maintaining consistent API.
    """

    def read_bytes(self, path: str) -> bytes:
        """Read file contents as bytes"""
        ...

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write bytes to file"""
        ...

    def atomic_rename(self, src: str, dest: str) -> None:
        """
        Atomically rename file (critical for transactions)

        This is used for two-phase commit: write to .tmp, then rename.
        """
        ...

    def delete(self, path: str) -> None:
        """Delete file"""
        ...

    def exists(self, path: str) -> bool:
        """Check if file exists"""
        ...

    def list_files(self, prefix: str) -> List[str]:
        """List files matching prefix (for discovery)"""
        ...
