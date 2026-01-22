"""Direct comparison: Rust vs Python performance"""
import time
import shutil
from pathlib import Path
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from datetime import datetime, timedelta, timezone

print("=" * 70)
print("PixelQuery: Rust vs Python 직접 비교")
print("=" * 70)
print()

# Setup
test_dir = Path("test_comparison")
if test_dir.exists():
    shutil.rmtree(test_dir)
test_dir.mkdir()

# Create ONE test COG file
print("📦 테스트 파일 생성 중...")
cog_path = test_dir / "test.tif"

bounds = (126.95, 37.50, 126.96, 37.51)
width, height = 1024, 1024
transform = from_bounds(*bounds, width, height)

data = np.zeros((4, height, width), dtype=np.uint16)
data[0, :, :] = np.random.randint(500, 1500, (height, width))
data[1, :, :] = np.random.randint(800, 2000, (height, width))
data[2, :, :] = np.random.randint(300, 1000, (height, width))
data[3, :, :] = np.random.randint(2000, 4000, (height, width))

with rasterio.open(
    cog_path, 'w',
    driver='GTiff',
    height=height,
    width=width,
    count=4,
    dtype=np.uint16,
    crs='EPSG:4326',
    transform=transform,
    compress='lzw',
    tiled=True,
    blockxsize=256,
    blockysize=256
) as dst:
    dst.write(data)

print(f"✅ COG 파일 생성 완료 (1024x1024px, 4 bands)\n")

# Import modules
from pixelquery.io.ingest import IngestionPipeline
from pixelquery.grid.tile_grid import FixedTileGrid
from pixelquery.catalog.local import LocalCatalog
import pixelquery._internal.storage.arrow_chunk as arrow_module
import pixelquery.io.ingest as ingest_module

def run_benchmark(name, use_rust=True):
    """Run benchmark with or without Rust"""
    print("=" * 70)
    print(f"{name}")
    print("=" * 70)

    if not use_rust:
        # Disable Rust
        original_arrow = arrow_module.RUST_ARROW_AVAILABLE
        original_resample = ingest_module.RUST_RESAMPLE_AVAILABLE
        arrow_module.RUST_ARROW_AVAILABLE = False
        ingest_module.RUST_RESAMPLE_AVAILABLE = False
        print("⚠️  Rust DISABLED - Python fallback\n")
    else:
        print("⚡ Rust ENABLED\n")

    warehouse_path = test_dir / f"warehouse_{'rust' if use_rust else 'python'}"
    tile_grid = FixedTileGrid(tile_size_m=2560.0)
    catalog = LocalCatalog(warehouse_path=str(warehouse_path))
    pipeline = IngestionPipeline(
        warehouse_path=str(warehouse_path),
        tile_grid=tile_grid,
        catalog=catalog,
        max_workers=1
    )

    band_mapping = {0: "blue", 1: "green", 2: "red", 3: "nir"}

    # Warmup
    pipeline.ingest_cog(
        cog_path=str(cog_path),
        acquisition_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        product_id="warmup",
        band_mapping=band_mapping,
        auto_commit=True,
        parallel=False
    )

    # Actual benchmark (3 runs)
    times = []
    for i in range(3):
        start = time.time()
        pipeline.ingest_cog(
            cog_path=str(cog_path),
            acquisition_time=datetime(2024, 1, i+2, tzinfo=timezone.utc),
            product_id="test",
            band_mapping=band_mapping,
            auto_commit=True,
            parallel=False
        )
        elapsed = time.time() - start
        times.append(elapsed)
        print(f"  Run {i+1}: {elapsed:.2f}s")

    avg_time = sum(times) / len(times)
    min_time = min(times)
    print(f"\n평균: {avg_time:.2f}s")
    print(f"최소: {min_time:.2f}s")

    if not use_rust:
        # Restore Rust
        arrow_module.RUST_ARROW_AVAILABLE = original_arrow
        ingest_module.RUST_RESAMPLE_AVAILABLE = original_resample

    return avg_time

# Run benchmarks
results = {}
results['python'] = run_benchmark("PYTHON ONLY (scipy + pyarrow)", use_rust=False)
print()
results['rust'] = run_benchmark("RUST (Resampling + Arrow I/O)", use_rust=True)

# Summary
print()
print("=" * 70)
print("📊 최종 비교")
print("=" * 70)
print(f"Python:  {results['python']:.2f}s/file")
print(f"Rust:    {results['rust']:.2f}s/file")
print(f"Speedup: {results['python'] / results['rust']:.2f}x")
print()
print(f"절약된 시간 (파일당): {results['python'] - results['rust']:.2f}s")
print(f"절약된 시간 (243개):  {(results['python'] - results['rust']) * 243:.1f}s ({(results['python'] - results['rust']) * 243 / 60:.1f}분)")
print("=" * 70)

# Cleanup
shutil.rmtree(test_dir)
print("\n✅ 완료!")
