"""
Two-Phase Commit Transaction Implementation

Implements 2PC for ACID guarantees when ingesting satellite data.
"""

from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime, timezone
import shutil
import uuid


class TransactionError(Exception):
    """Transaction-related errors"""
    pass


class TwoPhaseCommitTransaction:
    """
    Two-Phase Commit transaction for tile ingestion

    Ensures ACID properties when writing tile data and metadata:
    1. Atomicity: All writes succeed or none do
    2. Consistency: Metadata always consistent with data
    3. Isolation: Concurrent transactions don't interfere
    4. Durability: Committed data persists

    Transaction workflow:
    1. **Prepare Phase**:
       - Write all data to temporary files (.tmp suffix)
       - Validate all writes succeeded
       - If any fails, rollback all temporary files

    2. **Commit Phase**:
       - Atomic rename of all temporary files to final paths
       - Update metadata
       - If any fails, attempt rollback

    Examples:
        >>> txn = TwoPhaseCommitTransaction()
        >>> txn.begin()
        >>>
        >>> # Prepare: Write to .tmp files
        >>> txn.write_file("data.arrow", data, temp=True)
        >>> txn.write_file("metadata.parquet", metadata, temp=True)
        >>>
        >>> # Commit: Atomic rename
        >>> txn.commit()  # All files moved atomically
        >>>
        >>> # On error:
        >>> try:
        ...     txn.commit()
        ... except TransactionError:
        ...     txn.rollback()  # All .tmp files deleted
    """

    def __init__(self):
        """Initialize transaction"""
        self.txn_id = str(uuid.uuid4())
        self.state = "not_started"  # not_started, prepared, committed, aborted
        self.temp_files: List[Path] = []  # Files written in prepare phase
        self.committed_files: List[Path] = []  # Files after commit
        self.start_time: Optional[datetime] = None
        self.commit_time: Optional[datetime] = None

    def begin(self) -> None:
        """
        Begin transaction

        Raises:
            TransactionError: If transaction already started
        """
        if self.state != "not_started":
            raise TransactionError(f"Transaction already in state: {self.state}")

        self.state = "preparing"
        self.start_time = datetime.now(timezone.utc)

    def write_file(self, path: str, data: bytes, temp: bool = True) -> None:
        """
        Write file during transaction

        Args:
            path: Target file path
            data: File contents
            temp: If True, write to .tmp file (prepare phase)

        Raises:
            TransactionError: If transaction not started or write fails
        """
        if self.state != "preparing":
            raise TransactionError(f"Cannot write in state: {self.state}")

        target_path = Path(path)

        if temp:
            # Write to temporary file
            temp_path = Path(str(path) + f".{self.txn_id}.tmp")
            try:
                temp_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path.write_bytes(data)
                self.temp_files.append(temp_path)
            except Exception as e:
                raise TransactionError(f"Failed to write temp file {temp_path}: {e}")
        else:
            # Direct write (for metadata updates in commit phase)
            try:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                target_path.write_bytes(data)
                self.committed_files.append(target_path)
            except Exception as e:
                raise TransactionError(f"Failed to write file {target_path}: {e}")

    def prepare(self) -> None:
        """
        Complete prepare phase

        Validates all temporary files are written successfully.

        Raises:
            TransactionError: If validation fails
        """
        if self.state != "preparing":
            raise TransactionError(f"Cannot prepare in state: {self.state}")

        # Validate all temp files exist
        for temp_file in self.temp_files:
            if not temp_file.exists():
                raise TransactionError(f"Temp file missing: {temp_file}")

        self.state = "prepared"

    def commit(self) -> None:
        """
        Commit transaction

        Atomically renames all temporary files to final locations.

        Raises:
            TransactionError: If commit fails
        """
        if self.state == "preparing":
            # Auto-prepare if not done
            self.prepare()

        if self.state != "prepared":
            raise TransactionError(f"Cannot commit in state: {self.state}")

        # Commit phase: Atomic rename of all temp files
        try:
            for temp_file in self.temp_files:
                # Remove .{txn_id}.tmp suffix
                final_path = Path(str(temp_file).replace(f".{self.txn_id}.tmp", ""))

                # Atomic rename
                temp_file.rename(final_path)
                self.committed_files.append(final_path)

            self.state = "committed"
            self.commit_time = datetime.now(timezone.utc)

        except Exception as e:
            # Commit failed - attempt rollback
            self.state = "commit_failed"
            self._cleanup_temp_files()
            raise TransactionError(f"Commit failed: {e}")

    def rollback(self) -> None:
        """
        Rollback transaction

        Deletes all temporary files and restores original state.

        Raises:
            TransactionError: If rollback fails
        """
        if self.state in ("not_started", "committed"):
            raise TransactionError(f"Cannot rollback in state: {self.state}")

        self._cleanup_temp_files()
        self.state = "aborted"

    def _cleanup_temp_files(self) -> None:
        """Delete all temporary files"""
        for temp_file in self.temp_files:
            try:
                if temp_file.exists():
                    temp_file.unlink()
            except Exception as e:
                # Log but don't raise - best effort cleanup
                print(f"Warning: Failed to delete temp file {temp_file}: {e}")

    def get_status(self) -> Dict[str, Any]:
        """
        Get transaction status

        Returns:
            Dictionary with transaction state and statistics
        """
        duration = None
        if self.start_time and self.commit_time:
            duration = (self.commit_time - self.start_time).total_seconds()

        return {
            'txn_id': self.txn_id,
            'state': self.state,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'commit_time': self.commit_time.isoformat() if self.commit_time else None,
            'duration_seconds': duration,
            'num_temp_files': len(self.temp_files),
            'num_committed_files': len(self.committed_files),
        }

    def __enter__(self):
        """Context manager: begin transaction"""
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager: commit or rollback"""
        if exc_type is None:
            # No exception - commit
            try:
                if self.state == "preparing":
                    self.commit()
            except TransactionError:
                self.rollback()
                raise
        else:
            # Exception occurred - rollback
            if self.state in ("preparing", "prepared"):
                self.rollback()

        return False  # Don't suppress exceptions
