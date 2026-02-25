"""
PixelQuery I/O Module

COG reading and I/O backend abstraction.
"""

__all__ = ["COGReader", "IngestionPipeline", "PixelQueryS3"]


def __getattr__(name):
    if name == "COGReader":
        from pixelquery.io.cog import COGReader

        return COGReader
    if name == "IngestionPipeline":
        from pixelquery.io.ingest import IngestionPipeline

        return IngestionPipeline
    if name == "PixelQueryS3":
        from pixelquery.io.s3_client import PixelQueryS3

        return PixelQueryS3
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
