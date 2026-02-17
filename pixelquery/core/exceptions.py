"""
PixelQuery Exceptions

Exception hierarchy for error handling.
"""


class PixelQueryError(Exception):
    """Base exception for PixelQuery"""

    pass


class TransactionError(PixelQueryError):
    """Transaction commit/rollback failed"""

    pass


class IngestionError(PixelQueryError):
    """Image ingestion failed"""

    pass


class QueryError(PixelQueryError):
    """Query execution failed"""

    pass


class ValidationError(PixelQueryError):
    """Data validation failed"""

    pass
