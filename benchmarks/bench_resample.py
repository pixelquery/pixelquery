"""Benchmark script comparing Rust vs scipy resampling performance"""
import time
import numpy as np
from scipy.ndimage import zoom

try:
    from pixelquery_core import resample_bilinear
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False


def benchmark_scipy(data, target_shape, iterations=100):
    """Benchmark scipy.ndimage.zoom performance"""
    zoom_factors = (target_shape[0] / data.shape[0], target_shape[1] / data.shape[1])

    start = time.perf_counter()
    for _ in range(iterations):
        result = zoom(data, zoom_factors, order=1).astype(np.uint16)
    elapsed = time.perf_counter() - start

    return elapsed / iterations


def benchmark_rust(data, target_shape, iterations=100):
    """Benchmark Rust resample_bilinear performance"""
    if not RUST_AVAILABLE:
        return None

    start = time.perf_counter()
    for _ in range(iterations):
        result = resample_bilinear(data, target_shape[0], target_shape[1])
    elapsed = time.perf_counter() - start

    return elapsed / iterations


if __name__ == '__main__':
    # Test case: Typical COG window → Tile size (100x100 → 256x256)
    input_data = np.random.randint(0, 4000, (100, 100), dtype=np.uint16)
    target_shape = (256, 256)

    print("=" * 60)
    print("Resampling Performance Benchmark")
    print("=" * 60)
    print(f"Input shape:  {input_data.shape}")
    print(f"Output shape: {target_shape}")
    print(f"Iterations:   100")
    print("-" * 60)

    # Benchmark scipy
    scipy_time = benchmark_scipy(input_data, target_shape)
    print(f"scipy.ndimage.zoom: {scipy_time*1000:.2f} ms")

    # Benchmark Rust
    if RUST_AVAILABLE:
        rust_time = benchmark_rust(input_data, target_shape)
        print(f"Rust resample:      {rust_time*1000:.2f} ms")
        print("-" * 60)
        print(f"Speedup:            {scipy_time/rust_time:.1f}x")
        print("=" * 60)
    else:
        print("Rust not available - build with 'maturin develop --release'")
        print("=" * 60)
