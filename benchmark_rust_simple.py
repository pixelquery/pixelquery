"""Simple benchmark comparing Rust vs Python performance on Arrow I/O and resampling"""
import time
import numpy as np
from datetime import datetime, timezone
from pathlib import Path
import shutil

print("=" * 70)
print("PixelQuery Phase 2 성능 비교: Rust vs Python")
print("=" * 70)
print()

# Test 1: Resampling performance
print("Test 1: Array Resampling (100x100 → 256x256)")
print("-" * 70)

from pixelquery.io.ingest import RUST_RESAMPLE_AVAILABLE
if RUST_RESAMPLE_AVAILABLE:
    from pixelquery_core import resample_bilinear
    print("✅ Rust resampling available")
else:
    print("❌ Rust resampling NOT available")

from scipy.ndimage import zoom

# Create test data
input_data = np.random.randint(0, 4000, (100, 100), dtype=np.uint16)
iterations = 100

# Rust benchmark
if RUST_RESAMPLE_AVAILABLE:
    start = time.perf_counter()
    for _ in range(iterations):
        result = resample_bilinear(input_data, 256, 256)
    rust_time = (time.perf_counter() - start) / iterations
    print(f"Rust:   {rust_time * 1000:.2f} ms")
else:
    rust_time = None

# Python benchmark
start = time.perf_counter()
for _ in range(iterations):
    zoom_factors = (256 / 100, 256 / 100)
    result = zoom(input_data, zoom_factors, order=1).astype(np.uint16)
python_time = (time.perf_counter() - start) / iterations
print(f"Python: {python_time * 1000:.2f} ms (scipy)")

if rust_time:
    print(f"Speedup: {python_time / rust_time:.1f}x")

print()

# Test 2: Arrow I/O performance
print("Test 2: Arrow Chunk Write/Append Operations")
print("-" * 70)

from pixelquery._internal.storage.arrow_chunk import RUST_ARROW_AVAILABLE, ArrowChunkWriter
if RUST_ARROW_AVAILABLE:
    print("✅ Rust Arrow I/O available")
else:
    print("❌ Rust Arrow I/O NOT available")

# Setup test data
test_dir = Path("test_arrow_bench")
if test_dir.exists():
    shutil.rmtree(test_dir)
test_dir.mkdir()

# Create test data (simulating a tile chunk)
chunk_data = {
    'time': [datetime(2024, 1, i+1, tzinfo=timezone.utc) for i in range(10)],
    'pixels': [np.random.randint(0, 4000, 256*256, dtype=np.uint16) for _ in range(10)],
    'mask': [np.random.choice([True, False], 256*256) for _ in range(10)]
}

writer = ArrowChunkWriter()

# Benchmark write operation
iterations = 10
test_file = test_dir / "test.arrow"

start = time.perf_counter()
for i in range(iterations):
    if test_file.exists():
        test_file.unlink()
    writer.write_chunk(
        str(test_file),
        chunk_data,
        product_id="test",
        resolution=10.0
    )
write_time = (time.perf_counter() - start) / iterations
print(f"Write: {write_time * 1000:.2f} ms")

# Benchmark append operation
test_file.unlink()
# Initial write
writer.write_chunk(str(test_file), chunk_data, product_id="test", resolution=10.0)

# Append benchmark
append_data = {
    'time': [datetime(2024, 2, 1, tzinfo=timezone.utc)],
    'pixels': [np.random.randint(0, 4000, 256*256, dtype=np.uint16)],
    'mask': [np.random.choice([True, False], 256*256)]
}

start = time.perf_counter()
for i in range(iterations):
    writer.append_to_chunk(
        str(test_file),
        append_data,
        product_id="test",
        resolution=10.0
    )
append_time = (time.perf_counter() - start) / iterations
print(f"Append: {append_time * 1000:.2f} ms")

print()

# Cleanup
shutil.rmtree(test_dir)

# Summary
print("=" * 70)
print("성능 요약")
print("=" * 70)

if RUST_RESAMPLE_AVAILABLE and rust_time:
    print(f"Resampling speedup: {python_time / rust_time:.1f}x (Python {python_time*1000:.2f}ms → Rust {rust_time*1000:.2f}ms)")
else:
    print(f"Resampling: Python only ({python_time*1000:.2f}ms)")

if RUST_ARROW_AVAILABLE:
    print(f"Arrow I/O: Rust enabled (Write: {write_time*1000:.2f}ms, Append: {append_time*1000:.2f}ms)")
else:
    print(f"Arrow I/O: Python only (Write: {write_time*1000:.2f}ms, Append: {append_time*1000:.2f}ms)")

print("\n✓ Benchmark completed")
