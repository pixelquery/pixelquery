"""
PixelQuery I/O Module

COG reading and I/O backend abstraction.
"""

from pixelquery.io.cog import COGReader
from pixelquery.io.ingest import IngestionPipeline

__all__ = ["COGReader", "IngestionPipeline"]
