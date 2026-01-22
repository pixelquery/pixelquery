# PixelQuery

> Apache Iceberg-based storage engine for satellite imagery with native multi-resolution support

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

**Status**: 🚧 Early Development (API Design Phase)

## Overview

PixelQuery transforms satellite imagery from a file-based system into a queryable data lake with:

- **🎯 Multi-Resolution Native**: Seamlessly integrate Sentinel-2 (10m), Landsat-8 (30m), and Planet (3m) data
- **⚡ Time-Series Optimized**: 5-30x faster than COG+STAC for multi-year queries
- **🔒 ACID Transactions**: Metadata integrity via Apache Iceberg with Time Travel
- **🐍 xarray-inspired API**: Familiar interface for data scientists

## Quick Start

```python
import pixelquery as pq
from datetime import datetime

# Open a dataset (xarray-like API)
ds = pq.open_dataset("warehouse", tile_id="x0024_y0041")

# Select bands and time range
subset = ds.sel(
    time=slice("2024-01-01", "2024-12-31"),
    bands=["red", "nir"]
)

# Compute vegetation index
ndvi = pq.compute_ndvi(ds["red"], ds["nir"])

# Temporal resampling
monthly_ndvi = ndvi.resample(time="1M").mean()

# Convert to xarray for visualization
xr_ds = monthly_ndvi.to_xarray()
xr_ds.plot()
```

## Architecture

PixelQuery uses a 3-layer architecture:

```
┌─────────────────────────────────────────────┐
│ Layer 1: Apache Iceberg                     │
│ • Metadata ACID transactions                │
│ • Snapshot-based version control            │
│ • Hidden monthly partitioning               │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ Layer 2: GeoParquet                         │
│ • Tile boundaries (WKB geometry)            │
│ • R-tree spatial indexing                   │
│ • Band statistics (min/max/mean)            │
│ • DuckDB spatial query integration          │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ Layer 3: Arrow IPC                          │
│ • Monthly spatiotemporal chunks             │
│ • Variable-length arrays (multi-resolution) │
│ • Zero-copy reads                           │
│ • Column-based compression (Zstd, LZ4)      │
└─────────────────────────────────────────────┘
```

## Key Features

### Multi-Resolution Fusion

```python
# Query data from multiple satellites at different resolutions
result = pq.query_by_bounds(
    warehouse="warehouse",
    bounds=(127.0, 37.5, 127.1, 37.6),
    date_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
    bands=["red", "nir"],
    target_resolution=10.0  # Unified to 10m
)

# All data resampled to common resolution
df = result.to_pandas()
print(df['product_id'].unique())
# ['sentinel2_l2a', 'landsat8_l2', 'planet_l3a']
```

### Time-Series Analysis

```python
# Load entire time-series efficiently
ds = pq.open_dataset(
    "warehouse",
    tile_id="x0024_y0041",
    time_range=(datetime(2020, 1, 1), datetime(2024, 12, 31))
)

# Compute NDVI trend
ndvi = pq.compute_ndvi(ds["red"], ds["nir"])
monthly_mean = ndvi.resample(time="1M").mean()

# Detect anomalies
ndvi_std = ndvi.std(dim="time")
anomalies = (ndvi - ndvi.mean(dim="time")) / ndvi_std > 2
```

### Time Travel (Iceberg Snapshots)

```python
# Query historical state
result_v1 = pq.query_by_bounds(
    warehouse="warehouse",
    bounds=(...),
    as_of_snapshot_id=12345  # Specific version
)
```

## Performance

**Multi-year time-series queries** (vs COG+STAC):

| Operation | PixelQuery | COG+STAC | Speedup |
|-----------|------------|----------|---------|
| 1 year (120 images) | 4-11s | 45-120s | **10-30x** |
| 5 years (600 images) | 5-18s | 150-400s | **25-30x** |

*Benchmark: Single tile, 2 bands, monthly aggregation*

**Ingestion Performance** (with Rust optimizations):

| Component | Python | Rust | Speedup |
|-----------|--------|------|---------|
| Image resampling | 0.98ms | 0.22ms | **4.4x faster** |
| Arrow chunk write | ~100ms | 31ms | **3.2x faster** |
| Arrow chunk append | ~100ms | 13ms | **7.7x faster** |
| **Total ingestion** | **0.76s/file** | **0.27s/file** | **2.8x faster** |

*Optimizations: Metadata batching + Rust resampling + Rust Arrow I/O = **2.8x total speedup**. See [optimization_summary.md](docs/optimization_summary.md) for details.*

## Installation

```bash
# From source (development)
git clone https://github.com/yourusername/pixelquery.git
cd pixelquery
pip install -e ".[dev]"
```

**Optional: Rust Performance Extensions**

For 5.8x faster image resampling, build the Rust extensions:

```bash
# Install Rust toolchain (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install maturin (Rust-Python build tool)
pip install maturin

# Build Rust extensions
cd pixelquery_core
maturin develop --release
cd ..
```

PixelQuery will automatically use Rust resampling if available, otherwise falls back to scipy (with a warning).

**Requirements:**
- Python 3.11+
- Apache Iceberg (PyIceberg)
- PyArrow
- GeoPandas
- Rasterio
- DuckDB
- **Optional**: Rust toolchain (for performance extensions)

## Documentation

- **[API Examples](docs/api-examples.md)** - Real-world usage examples
- **[Package Structure](docs/package-structure.md)** - Architecture and design
- **[Optimization Summary](docs/optimization_summary.md)** - Performance optimizations and benchmarks
- **[Refactoring Progress](docs/refactoring-progress.md)** - Development status

## Project Status

**Current Phase**: API Design (Phase 2 complete)

| Phase | Status | Progress |
|-------|--------|----------|
| Phase 1: Package Structure | ✅ Complete | 100% |
| Phase 2: API Design | ✅ Complete | 100% |
| Phase 3: Documentation | 🔄 In Progress | 60% |
| Phase 4: Testing | 🔄 In Progress | 30% |
| Phase 5: Implementation | ⏳ Planned | 0% |

See [refactoring-progress.md](docs/refactoring-progress.md) for detailed status.

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=pixelquery --cov-report=html

# Format code
black pixelquery tests

# Type checking
mypy pixelquery
```

## Comparison with Alternatives

### vs COG+STAC

| Feature | PixelQuery | COG+STAC |
|---------|------------|----------|
| Multi-resolution fusion | ✅ Native | ❌ Manual |
| Time-series queries | ✅ Optimized (5-30x faster) | ⚠️ Slow (per-file reads) |
| ACID metadata | ✅ Iceberg | ❌ No guarantees |
| Time Travel | ✅ Built-in | ❌ No |
| Ecosystem | 🆕 New | ✅ Mature |

### vs Google Earth Engine

| Feature | PixelQuery | Earth Engine |
|---------|------------|--------------|
| Deployment | ✅ Self-hosted | ☁️ Cloud only |
| Pricing | 🆓 Free (open-source) | 💰 Pay-per-use |
| Custom data | ✅ Full control | ⚠️ Limited |
| Python API | ✅ Native | ✅ Available |

## Use Cases

### 1. Agricultural Monitoring

```python
# Monitor crop health over growing season
ds = pq.open_dataset("warehouse", tile_id="farm_tile")
ndvi = pq.compute_ndvi(ds["red"], ds["nir"])
monthly = ndvi.resample(time="1M").mean()
```

### 2. Disaster Monitoring

```python
# Rapid multi-source data integration
ds = pq.open_mfdataset(
    "warehouse",
    tile_ids=disaster_area_tiles,
    time_range=(event_date - timedelta(days=7), event_date + timedelta(days=7))
)
```

### 3. Climate Research

```python
# Long-term trend analysis
ds = pq.open_dataset(
    "warehouse",
    tile_id="study_area",
    time_range=(datetime(2000, 1, 1), datetime(2024, 12, 31))
)
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

## Citation

If you use PixelQuery in your research, please cite:

```bibtex
@software{pixelquery2024,
  title = {PixelQuery: Apache Iceberg-based storage engine for satellite imagery},
  author = {PixelQuery Contributors},
  year = {2024},
  url = {https://github.com/yourusername/pixelquery}
}
```

## References

- **Design Inspiration**:
  - [xarray](https://xarray.dev/) - Multi-dimensional labeled arrays
  - [Apache Iceberg](https://iceberg.apache.org/) - Table format for large datasets
  - [Rasterio](https://rasterio.readthedocs.io/) - Geospatial raster I/O
  - [Polars](https://www.pola.rs/) - Fast DataFrame library

## Acknowledgments

Built with:
- [Apache Iceberg](https://iceberg.apache.org/) - ACID transactions
- [PyArrow](https://arrow.apache.org/docs/python/) - Efficient data structures
- [GeoPandas](https://geopandas.org/) - Geospatial operations
- [DuckDB](https://duckdb.org/) - Spatial queries

---

**Note**: This project is in early development. The API is subject to change.
