"""
PixelQuery I/O Module

COG reading and I/O backend abstraction.
"""

__all__ = ["COGReader", "IngestionPipeline"]


def __getattr__(name):
    if name == "COGReader":
        from pixelquery.io.cog import COGReader
        return COGReader
    if name == "IngestionPipeline":
        from pixelquery.io.ingest import IngestionPipeline
        return IngestionPipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
