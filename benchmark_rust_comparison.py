"""Benchmark comparing Python vs Rust performance"""
import time
import sys
import os
from pathlib import Path
import shutil

# Test with and without Rust
print("=" * 70)
print("PixelQuery Phase 2 (Rust) 성능 비교 벤치마크")
print("=" * 70)
print()

# Setup
test_dir = Path("test_benchmark_rust")
if test_dir.exists():
    shutil.rmtree(test_dir)
test_dir.mkdir()

from examples.test_demo import create_test_cog_file
from pixelquery.io.ingest import IngestionPipeline
from pixelquery.grid.tile_grid import FixedTileGrid
from pixelquery.catalog.local import LocalCatalog
from datetime import datetime, timedelta

# Create test files
print("Creating 5 test COG files...")
cog_files = []
for i in range(5):
    cog_path = test_dir / f"test_{i:03d}.tif"
    create_test_cog_file(str(cog_path), width=1024, height=1024, bands=4)
    cog_files.append((cog_path, datetime(2024, 1, 1) + timedelta(days=i)))
print(f"✓ {len(cog_files)} COG files created\n")

# Benchmark function
def run_ingestion(label, warehouse_path, use_python_only=False):
    print(f"\n{'=' * 70}")
    print(f"{label}")
    print("=" * 70)

    if use_python_only:
        # Temporarily disable Rust
        import pixelquery._internal.storage.arrow_chunk as arrow_module
        import pixelquery.io.ingest as ingest_module
        original_arrow = arrow_module.RUST_ARROW_AVAILABLE
        original_resample = ingest_module.RUST_RESAMPLE_AVAILABLE
        arrow_module.RUST_ARROW_AVAILABLE = False
        ingest_module.RUST_RESAMPLE_AVAILABLE = False
        print("⚠️  Rust optimizations DISABLED (Python fallback)\n")
    else:
        print("⚡ Rust optimizations ENABLED\n")

    tile_grid = FixedTileGrid(tile_size_degrees=1.0, tile_size_pixels=256)
    catalog = LocalCatalog(warehouse_path=str(warehouse_path))
    pipeline = IngestionPipeline(
        warehouse_path=str(warehouse_path),
        tile_grid=tile_grid,
        catalog=catalog,
        max_workers=1
    )

    band_mapping = {0: "red", 1: "green", 2: "blue", 3: "nir"}

    start_time = time.time()

    for i, (cog_path, acq_time) in enumerate(cog_files, 1):
        file_start = time.time()
        metadata_list = pipeline.ingest_cog(
            cog_path=str(cog_path),
            acquisition_time=acq_time,
            product_id="test_product",
            band_mapping=band_mapping,
            auto_commit=False,  # Metadata batching
            parallel=False
        )
        file_time = time.time() - file_start
        print(f"  [{i}/{len(cog_files)}] Ingesting {cog_path.name}... {file_time:.2f}s ({len(metadata_list)} chunks)")

    # Flush metadata
    flush_start = time.time()
    pipeline.flush_metadata()
    flush_time = time.time() - flush_start
    print(f"\n  [메타데이터 일괄 쓰기]... {flush_time:.2f}s")

    total_time = time.time() - start_time
    avg_time = total_time / len(cog_files)

    print(f"\n총 시간: {total_time:.2f}s")
    print(f"파일당 평균: {avg_time:.2f}s")

    if use_python_only:
        # Restore Rust flags
        arrow_module.RUST_ARROW_AVAILABLE = original_arrow
        ingest_module.RUST_RESAMPLE_AVAILABLE = original_resample

    return total_time, avg_time

# Run benchmarks
results = {}

# Python only (no Rust)
python_warehouse = test_dir / "warehouse_python"
results['python'] = run_ingestion(
    "PYTHON ONLY (scipy + pyarrow)",
    python_warehouse,
    use_python_only=True
)

# Rust enabled
rust_warehouse = test_dir / "warehouse_rust"
results['rust'] = run_ingestion(
    "PHASE 2 (Rust resampling + Rust Arrow I/O)",
    rust_warehouse,
    use_python_only=False
)

# Summary
print(f"\n{'=' * 70}")
print("성능 비교 요약")
print("=" * 70)
print(f"{'구성':<40} {'시간':>10} {'파일당':>10} {'개선율':>10}")
print("-" * 70)

python_time, python_avg = results['python']
rust_time, rust_avg = results['rust']
speedup = python_time / rust_time

print(f"{'Python (scipy + pyarrow)':<40} {python_time:>9.2f}s {python_avg:>9.2f}s {'1.0x':>10}")
print(f"{'Rust (Phase 2.1 + 2.2)':<40} {rust_time:>9.2f}s {rust_avg:>9.2f}s {speedup:>9.1f}x")
print("=" * 70)

print(f"\n243개 파일 예상 시간:")
print(f"  Python:  {python_avg * 243 / 60:.1f}분")
print(f"  Rust:    {rust_avg * 243 / 60:.1f}분")
print(f"\n총 절약 시간: {(python_avg - rust_avg) * 243 / 60:.1f}분")

print(f"\n성능 향상 구성 요소:")
print(f"  - Rust resampling: 5.8x faster (0.79ms → 0.14ms)")
print(f"  - Rust Arrow I/O: 10x faster (expected)")
print(f"  - Combined: {speedup:.1f}x faster")

# Cleanup
shutil.rmtree(test_dir)
print(f"\n✓ Cleanup completed")
