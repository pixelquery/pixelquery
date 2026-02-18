"""
Auto-ingest convenience function for PixelQuery.

Scans a directory of COG files, auto-detects dates from filenames,
extracts metadata from COG headers, and ingests into an Icechunk warehouse.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class IngestResult:
    """Result of an ingest operation."""

    scene_count: int
    group_names: list[str]
    warehouse_path: str
    elapsed: float
    errors: list[str] = field(default_factory=list)

    def __repr__(self):
        status = f"{self.scene_count} scenes in {self.elapsed:.1f}s"
        if self.errors:
            status += f" ({len(self.errors)} errors)"
        return f"<IngestResult: {status}>"


# Date parsing patterns (tried in order)
_DATE_PATTERNS = [
    (r"(\d{4}-\d{2}-\d{2})", "%Y-%m-%d"),  # 2024-01-15
    (r"(\d{8})", "%Y%m%d"),  # 20240115
    (r"(\d{4}_\d{2}_\d{2})", "%Y_%m_%d"),  # 2024_01_15
]


def _parse_date_from_filename(filename: str, date_pattern: str | None = None) -> datetime | None:
    """
    Extract acquisition date from filename.

    Tries common patterns in order:
    1. Custom regex (if provided via date_pattern)
    2. YYYY-MM-DD
    3. YYYYMMDD
    4. YYYY_MM_DD
    5. Falls back to None if no date found
    """
    if date_pattern:
        m = re.search(date_pattern, filename)
        if m:
            date_str = m.group(1) if m.lastindex else m.group(0)
            # Try parsing the matched string
            for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y_%m_%d", "%Y/%m/%d"):
                try:
                    return datetime.strptime(date_str, fmt).replace(tzinfo=UTC)
                except ValueError:
                    continue

    for pattern, fmt in _DATE_PATTERNS:
        m = re.search(pattern, filename)
        if m:
            try:
                return datetime.strptime(m.group(1), fmt).replace(tzinfo=UTC)
            except ValueError:
                continue

    return None


def _extract_band_names_from_cog(cog_path: str) -> list[str]:
    """Auto-generate band names from COG metadata."""
    try:
        import rasterio

        with rasterio.open(cog_path) as src:
            band_count = src.count
            # Try to get band descriptions
            descriptions = src.descriptions
            if descriptions and any(d for d in descriptions):
                return [d or f"band_{i + 1}" for i, d in enumerate(descriptions)]
            # Default: band_1, band_2, ...
            return [f"band_{i + 1}" for i in range(band_count)]
    except Exception:
        return ["band_1"]


def _match_mask_file(
    cog_file: Path,
    mask_dir: Path,
    date_pattern: str | None = None,
) -> Path | None:
    """
    Find matching mask file for a data COG by date.

    Tries to find a file in mask_dir with the same date as cog_file.
    """
    cog_date = _parse_date_from_filename(cog_file.name, date_pattern)
    if cog_date is None:
        return None

    for mask_file in sorted(mask_dir.glob("**/*.tif*")):
        mask_date = _parse_date_from_filename(mask_file.name, date_pattern)
        if mask_date and mask_date.date() == cog_date.date():
            return mask_file
    return None


def ingest(
    source: str,
    warehouse: str = "./warehouse",
    band_names: list[str] | None = None,
    product_id: str | None = None,
    date_pattern: str | None = None,
    glob_pattern: str = "**/*.tif*",
    mask_path: str | None = None,
    storage_type: str = "local",
    storage_config: dict[str, Any] | None = None,
    vcc_prefix: str | None = None,
    vcc_data_path: str | None = None,
    **kwargs,
) -> IngestResult:
    """
    Auto-ingest COG files from a directory into a PixelQuery warehouse.

    Automatically:
    - Scans directory for COG/TIFF files
    - Extracts acquisition date from filenames
    - Reads CRS, bounds, and band count from COG headers
    - Creates Icechunk virtual references (zero-copy)

    Args:
        source: Directory path or single COG file path
        warehouse: Output warehouse path (created if needed)
        band_names: Band names. If None, auto-generates from COG metadata
        product_id: Product identifier. If None, derived from directory name
        date_pattern: Regex for date extraction from filename.
                     If None, tries common patterns automatically.
        glob_pattern: File pattern for directory scan (default: "**/*.tif*")
        mask_path: Directory containing cloud mask files (e.g. UDM2).
                   Files are matched to data COGs by date.
        storage_type: "local", "s3", or "gcs"
        storage_config: Cloud storage config
        vcc_prefix: Virtual Chunk Container URL prefix
        vcc_data_path: Local filesystem path for VCC store

    Returns:
        IngestResult with scene_count, elapsed_time, warehouse_path

    Examples:
        >>> import pixelquery as pq
        >>> result = pq.ingest("./my_cogs/", warehouse="./warehouse")
        >>> print(f"Ingested {result.scene_count} scenes in {result.elapsed:.1f}s")

        >>> # With cloud mask
        >>> pq.ingest("./analytic/", mask_path="./udm2/",
        ...           band_names=["blue", "green", "red", "nir"])

        >>> # Single file
        >>> pq.ingest("./scene.tif", warehouse="./warehouse")
    """
    import time

    start = time.perf_counter()

    errors = []

    # Check if source is a remote URI (s3://, gs://, http://, https://)
    is_remote = isinstance(source, str) and re.match(r"^(s3|gs|https?)://", source)

    if is_remote:
        # Remote file: pass URI directly to rasterio (no Path operations)
        cog_files = [source]
        source_path = Path(source.split("/")[-1])  # for product_id derivation only
    else:
        source_path = Path(source)
        # Discover COG files
        if source_path.is_file():
            cog_files = [str(source_path)]
        elif source_path.is_dir():
            cog_files_paths = sorted(source_path.glob(glob_pattern))
            if not cog_files_paths:
                raise FileNotFoundError(f"No files matching '{glob_pattern}' found in {source}")
            cog_files = [str(f) for f in cog_files_paths]
        else:
            raise FileNotFoundError(f"Source path does not exist: {source}")

    # Derive product_id from directory name if not provided
    if product_id is None:
        if is_remote:
            # Extract a meaningful name from the URI path
            uri_parts = source.rstrip("/").split("/")
            product_id = uri_parts[-2] if len(uri_parts) >= 3 else "default"
        elif source_path.is_dir():
            product_id = source_path.name
        else:
            product_id = source_path.parent.name or "default"

    # Auto-detect band names from first COG if not provided
    if band_names is None:
        band_names = _extract_band_names_from_cog(str(cog_files[0]))
        logger.info("Auto-detected band names: %s", band_names)

    # Auto-derive VCC from source path when not provided (ensures COG files are reachable)
    if vcc_prefix is None and storage_type == "local":
        resolved = source_path.resolve()
        # VCC prefix must include a path beyond root "/"
        # Use the first path component (e.g. /Users/, /private/, /data/)
        parts = resolved.parts  # ('/', 'private', 'var', ...)
        if len(parts) >= 2:
            base = "/" + parts[1] + "/"
        else:
            base = str(resolved.parent) + "/"
        vcc_data_path = base
        vcc_prefix = f"file://{base}"
    elif vcc_prefix is None and is_remote and storage_type == "s3":
        # Extract bucket from S3 URI: s3://bucket/path/... -> s3://bucket/
        bucket_name = source.replace("s3://", "").split("/")[0]
        vcc_prefix = f"s3://{bucket_name}/"

    # Initialize pipeline
    from pixelquery.io.ingest import IcechunkIngestionPipeline

    pipeline = IcechunkIngestionPipeline(
        repo_path=warehouse,
        storage_type=storage_type,
        storage_config=storage_config,
        vcc_prefix=vcc_prefix,
        vcc_data_path=vcc_data_path,
    )

    # Resolve mask directory if provided
    mask_dir = Path(mask_path) if mask_path else None

    # Build batch ingest info
    cog_infos = []
    for cog_file in cog_files:
        # Extract filename for date parsing (works for both local paths and URIs)
        if is_remote:
            filename = cog_file.rstrip("/").split("/")[-1]
        else:
            filename = Path(cog_file).name

        # Parse date from filename
        acq_time = _parse_date_from_filename(filename, date_pattern)
        if acq_time is None:
            if is_remote:
                # Cannot stat remote files; use current time as fallback
                acq_time = datetime.now(tz=UTC)
                logger.warning(
                    "No date found in remote filename '%s', using current time",
                    filename,
                )
            else:
                # Fallback to file modification time
                mtime = Path(cog_file).stat().st_mtime
                acq_time = datetime.fromtimestamp(mtime, tz=UTC)
                logger.warning(
                    "No date found in filename '%s', using file mtime: %s",
                    filename,
                    acq_time.date(),
                )

        info = {
            "cog_path": cog_file if is_remote else str(Path(cog_file).resolve()),
            "acquisition_time": acq_time,
            "product_id": product_id,
            "band_names": band_names,
        }

        # Match mask file by date (local files only)
        if not is_remote and mask_dir and mask_dir.is_dir():
            matched = _match_mask_file(Path(cog_file), mask_dir, date_pattern)
            if matched:
                info["mask_path"] = str(matched.resolve())
            else:
                logger.warning("No mask file found for %s", filename)

        cog_infos.append(info)

    # Batch ingest (single atomic commit)
    try:
        group_names = pipeline.ingest_cogs(
            cog_infos,
            message=f"Ingest {len(cog_infos)} COGs from {source}",
        )
    except Exception as e:
        # If batch fails, try one by one
        logger.warning("Batch ingest failed (%s), trying individual ingest", e)
        group_names = []
        for info in cog_infos:
            try:
                name = pipeline.ingest_cog(**info)  # type: ignore[arg-type]
                group_names.append(name)
            except Exception as e2:
                errors.append(f"{info['cog_path']}: {e2}")
                logger.error("Failed to ingest %s: %s", info["cog_path"], e2)

    elapsed = time.perf_counter() - start

    # For remote warehouses, keep URI as-is; for local, resolve to absolute path
    if re.match(r"^(s3|gs|https?)://", warehouse):
        resolved_warehouse = warehouse
    else:
        resolved_warehouse = str(Path(warehouse).resolve())

    result = IngestResult(
        scene_count=len(group_names),
        group_names=group_names,
        warehouse_path=resolved_warehouse,
        elapsed=elapsed,
        errors=errors,
    )

    logger.info("Ingest complete: %s", result)
    return result
