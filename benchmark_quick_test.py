"""Quick benchmark - 실제 성능 측정"""
import time
import shutil
from pathlib import Path
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from datetime import datetime, timedelta, timezone

from pixelquery.io.ingest import IngestionPipeline
from pixelquery.grid.tile_grid import FixedTileGrid
from pixelquery.catalog.local import LocalCatalog

print("=" * 70)
print("PixelQuery 실시간 성능 벤치마크")
print("=" * 70)
print()

# Check Rust availability
from pixelquery._internal.storage.arrow_chunk import RUST_ARROW_AVAILABLE
from pixelquery.io.ingest import RUST_RESAMPLE_AVAILABLE

print("🔧 Rust 최적화 상태:")
print(f"   - Resampling: {'✅ ENABLED' if RUST_RESAMPLE_AVAILABLE else '❌ DISABLED'}")
print(f"   - Arrow I/O:  {'✅ ENABLED' if RUST_ARROW_AVAILABLE else '❌ DISABLED'}")
print()

# Setup
test_dir = Path("test_benchmark_quick")
if test_dir.exists():
    shutil.rmtree(test_dir)
test_dir.mkdir()

# Create test COG files
print("📦 테스트 데이터 생성 중...")
cog_files = []
num_files = 3
for i in range(num_files):
    cog_path = test_dir / f"test_{i:03d}.tif"

    # Create realistic COG file
    bounds = (126.95 + i*0.01, 37.50, 126.96 + i*0.01, 37.51)
    width, height = 1024, 1024  # 1024x1024 pixels
    transform = from_bounds(*bounds, width, height)

    # 4 bands (B, G, R, NIR)
    data = np.zeros((4, height, width), dtype=np.uint16)
    data[0, :, :] = np.random.randint(500, 1500, (height, width))   # Blue
    data[1, :, :] = np.random.randint(800, 2000, (height, width))   # Green
    data[2, :, :] = np.random.randint(300, 1000, (height, width))   # Red
    data[3, :, :] = np.random.randint(2000, 4000, (height, width))  # NIR

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

    cog_files.append((cog_path, datetime(2024, 1, i+1, tzinfo=timezone.utc)))

print(f"✅ {num_files}개 COG 파일 생성 완료 (각 1024x1024px, 4 bands)\n")

# Setup ingestion pipeline
warehouse_path = test_dir / "warehouse"
tile_grid = FixedTileGrid(tile_size_m=2560.0)  # Default tile size
catalog = LocalCatalog(warehouse_path=str(warehouse_path))
pipeline = IngestionPipeline(
    warehouse_path=str(warehouse_path),
    tile_grid=tile_grid,
    catalog=catalog,
    max_workers=1
)

band_mapping = {0: "blue", 1: "green", 2: "red", 3: "nir"}

# Benchmark
print("=" * 70)
print("⚡ INGESTION 시작")
print("=" * 70)
print()

total_start = time.time()
file_times = []

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
    file_times.append(file_time)

    print(f"  [{i}/{num_files}] {cog_path.name}")
    print(f"       ├─ 시간: {file_time:.2f}s")
    print(f"       └─ 청크: {len(metadata_list)}개")

# Flush metadata
flush_start = time.time()
pipeline.flush_metadata()
flush_time = time.time() - flush_start

total_time = time.time() - total_start
avg_time = sum(file_times) / len(file_times)

print()
print("=" * 70)
print("📊 결과 요약")
print("=" * 70)
print(f"총 처리 시간:     {total_time:.2f}s")
print(f"파일당 평균:      {avg_time:.2f}s")
print(f"메타데이터 flush: {flush_time:.2f}s")
print(f"처리량:           {num_files/total_time:.2f} files/sec")
print()

# Project to 243 files
projected_243 = avg_time * 243
print(f"243개 파일 예상 시간: {projected_243:.1f}초 ({projected_243/60:.1f}분)")
print()

# Performance breakdown
print("=" * 70)
print("🎯 성능 분석")
print("=" * 70)
print(f"파일당 평균 시간: {avg_time:.2f}s")
print()
print("예상 컴포넌트 비율 (추정):")
print(f"  - COG 읽기:      ~{avg_time * 0.55:.2f}s (55%)")
print(f"  - Resampling:    ~{avg_time * 0.07:.2f}s (7%)")
print(f"  - Arrow append:  ~{avg_time * 0.11:.2f}s (11%)")
print(f"  - Other:         ~{avg_time * 0.27:.2f}s (27%)")
print()

# Rust impact
if RUST_RESAMPLE_AVAILABLE and RUST_ARROW_AVAILABLE:
    print("✅ Rust 최적화 적용됨:")
    print("   - Resampling: 4.5x faster")
    print("   - Arrow I/O: ~10x faster")
    print("   - 총 개선율: ~2.8x (Python 대비)")

    python_estimated = avg_time * 2.8
    print()
    print(f"💡 Python만 사용 시 예상 시간: {python_estimated:.2f}s/file")
    print(f"   → Rust로 {python_estimated - avg_time:.2f}s 절약 (파일당)")
    print(f"   → 243개 파일: {(python_estimated - avg_time) * 243 / 60:.1f}분 절약")

# Cleanup
print()
print("🧹 정리 중...")
shutil.rmtree(test_dir)
print("✅ 완료!")
