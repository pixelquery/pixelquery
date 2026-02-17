"""
Tests for exceptions
"""

import pytest

from pixelquery.core.exceptions import (
    IngestionError,
    PixelQueryError,
    QueryError,
    TransactionError,
    ValidationError,
)


class TestExceptions:
    """Test exception hierarchy"""

    def test_base_exception(self):
        """Test PixelQueryError"""
        with pytest.raises(PixelQueryError):
            raise PixelQueryError("Test error")

    def test_transaction_error(self):
        """Test TransactionError inherits from PixelQueryError"""
        with pytest.raises(PixelQueryError):
            raise TransactionError("Transaction failed")

        with pytest.raises(TransactionError):
            raise TransactionError("Transaction failed")

    def test_ingestion_error(self):
        """Test IngestionError inherits from PixelQueryError"""
        with pytest.raises(PixelQueryError):
            raise IngestionError("Ingestion failed")

    def test_query_error(self):
        """Test QueryError inherits from PixelQueryError"""
        with pytest.raises(PixelQueryError):
            raise QueryError("Query failed")

    def test_validation_error(self):
        """Test ValidationError inherits from PixelQueryError"""
        with pytest.raises(PixelQueryError):
            raise ValidationError("Validation failed")

    def test_exception_messages(self):
        """Test exception messages are preserved"""
        msg = "Custom error message"

        try:
            raise TransactionError(msg)
        except TransactionError as e:
            assert str(e) == msg
