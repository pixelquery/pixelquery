"""
Tests for Two-Phase Commit transaction
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from pixelquery._internal.transactions.two_phase import (
    TwoPhaseCommitTransaction,
    TransactionError
)


class TestTwoPhaseCommitTransaction:
    """Test 2PC transaction"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def txn(self):
        """Create transaction instance"""
        return TwoPhaseCommitTransaction()

    def test_init(self, txn):
        """Test transaction initialization"""
        assert txn.state == "not_started"
        assert txn.txn_id is not None
        assert len(txn.temp_files) == 0

    def test_begin(self, txn):
        """Test beginning transaction"""
        txn.begin()

        assert txn.state == "preparing"
        assert txn.start_time is not None

    def test_begin_twice_fails(self, txn):
        """Test cannot begin transaction twice"""
        txn.begin()

        with pytest.raises(TransactionError, match="already in state"):
            txn.begin()

    def test_write_file_temp(self, txn, temp_dir):
        """Test writing file in prepare phase"""
        txn.begin()

        path = str(Path(temp_dir) / "test.txt")
        data = b"test data"

        txn.write_file(path, data, temp=True)

        # File should be written to .tmp path
        assert len(txn.temp_files) == 1
        assert txn.temp_files[0].exists()
        assert ".tmp" in str(txn.temp_files[0])

    def test_write_file_without_begin_fails(self, txn, temp_dir):
        """Test writing without beginning transaction fails"""
        path = str(Path(temp_dir) / "test.txt")
        data = b"test data"

        with pytest.raises(TransactionError, match="Cannot write"):
            txn.write_file(path, data)

    def test_prepare(self, txn, temp_dir):
        """Test prepare phase"""
        txn.begin()

        # Write some files
        for i in range(3):
            path = str(Path(temp_dir) / f"file{i}.txt")
            txn.write_file(path, f"data{i}".encode(), temp=True)

        # Prepare
        txn.prepare()

        assert txn.state == "prepared"
        assert len(txn.temp_files) == 3

    def test_commit_success(self, txn, temp_dir):
        """Test successful commit"""
        txn.begin()

        # Write files
        paths = []
        for i in range(3):
            path = str(Path(temp_dir) / f"file{i}.txt")
            paths.append(path)
            txn.write_file(path, f"data{i}".encode(), temp=True)

        # Commit
        txn.commit()

        # Check state
        assert txn.state == "committed"
        assert txn.commit_time is not None

        # All final files should exist
        for path in paths:
            assert Path(path).exists()

        # No temp files should remain
        for temp_file in txn.temp_files:
            assert not temp_file.exists()

    def test_commit_without_prepare(self, txn, temp_dir):
        """Test commit auto-prepares if needed"""
        txn.begin()

        path = str(Path(temp_dir) / "file.txt")
        txn.write_file(path, b"data", temp=True)

        # Commit without explicit prepare
        txn.commit()

        assert txn.state == "committed"
        assert Path(path).exists()

    def test_rollback(self, txn, temp_dir):
        """Test rollback deletes temp files"""
        txn.begin()

        # Write files
        paths = []
        for i in range(3):
            path = str(Path(temp_dir) / f"file{i}.txt")
            paths.append(path)
            txn.write_file(path, f"data{i}".encode(), temp=True)

        # Rollback
        txn.rollback()

        # Check state
        assert txn.state == "aborted"

        # No files should exist (neither temp nor final)
        for path in paths:
            assert not Path(path).exists()

        for temp_file in txn.temp_files:
            assert not temp_file.exists()

    def test_rollback_committed_fails(self, txn, temp_dir):
        """Test cannot rollback committed transaction"""
        txn.begin()

        path = str(Path(temp_dir) / "file.txt")
        txn.write_file(path, b"data", temp=True)
        txn.commit()

        with pytest.raises(TransactionError, match="Cannot rollback"):
            txn.rollback()

    def test_context_manager_success(self, temp_dir):
        """Test context manager with successful transaction"""
        path = str(Path(temp_dir) / "file.txt")

        with TwoPhaseCommitTransaction() as txn:
            txn.write_file(path, b"data", temp=True)

        # Should auto-commit
        assert Path(path).exists()

    def test_context_manager_exception_rollback(self, temp_dir):
        """Test context manager rolls back on exception"""
        path = str(Path(temp_dir) / "file.txt")

        try:
            with TwoPhaseCommitTransaction() as txn:
                txn.write_file(path, b"data", temp=True)
                raise ValueError("Test error")
        except ValueError:
            pass

        # Should auto-rollback
        assert not Path(path).exists()

    def test_get_status(self, txn):
        """Test getting transaction status"""
        status = txn.get_status()

        assert 'txn_id' in status
        assert status['state'] == 'not_started'
        assert status['num_temp_files'] == 0

    def test_get_status_after_commit(self, txn, temp_dir):
        """Test status after commit"""
        txn.begin()

        path = str(Path(temp_dir) / "file.txt")
        txn.write_file(path, b"data", temp=True)
        txn.commit()

        status = txn.get_status()

        assert status['state'] == 'committed'
        assert status['duration_seconds'] is not None
        assert status['num_temp_files'] == 1
        assert status['num_committed_files'] == 1

    def test_concurrent_transactions_isolated(self, temp_dir):
        """Test multiple transactions are isolated"""
        path = str(Path(temp_dir) / "file.txt")

        txn1 = TwoPhaseCommitTransaction()
        txn2 = TwoPhaseCommitTransaction()

        # Both write to same path
        txn1.begin()
        txn1.write_file(path, b"data1", temp=True)

        txn2.begin()
        txn2.write_file(path, b"data2", temp=True)

        # Different temp files
        assert len(txn1.temp_files) == 1
        assert len(txn2.temp_files) == 1
        assert txn1.temp_files[0] != txn2.temp_files[0]

    def test_nested_directories(self, txn, temp_dir):
        """Test transaction creates nested directories"""
        path = str(Path(temp_dir) / "a" / "b" / "c" / "file.txt")

        txn.begin()
        txn.write_file(path, b"data", temp=True)
        txn.commit()

        assert Path(path).exists()
        assert Path(path).parent.exists()

    def test_write_file_direct(self, txn, temp_dir):
        """Test writing file directly (not temp)"""
        txn.begin()

        path = str(Path(temp_dir) / "direct.txt")
        txn.write_file(path, b"data", temp=False)

        # File should exist immediately (not temp)
        assert Path(path).exists()
        assert len(txn.committed_files) == 1

    def test_transaction_id_unique(self):
        """Test each transaction has unique ID"""
        txn1 = TwoPhaseCommitTransaction()
        txn2 = TwoPhaseCommitTransaction()

        assert txn1.txn_id != txn2.txn_id


class TestTransactionRobustness:
    """Test transaction error handling and robustness"""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory"""
        tmpdir = tempfile.mkdtemp()
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    def test_large_file_transaction(self, temp_dir):
        """Test transaction with large file"""
        path = str(Path(temp_dir) / "large.bin")
        large_data = b"x" * (10 * 1024 * 1024)  # 10 MB

        with TwoPhaseCommitTransaction() as txn:
            txn.write_file(path, large_data, temp=True)

        assert Path(path).exists()
        assert Path(path).stat().st_size == len(large_data)

    def test_many_files_transaction(self, temp_dir):
        """Test transaction with many files"""
        num_files = 100

        with TwoPhaseCommitTransaction() as txn:
            for i in range(num_files):
                path = str(Path(temp_dir) / f"file{i:04d}.txt")
                txn.write_file(path, f"data{i}".encode(), temp=True)

        # All files should be committed
        for i in range(num_files):
            path = Path(temp_dir) / f"file{i:04d}.txt"
            assert path.exists()
