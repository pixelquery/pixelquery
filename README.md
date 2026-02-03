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
from pixelquery.core.dataarray import DataArray
from pixelquery.core.dataset import Dataset
import numpy as np

# Create a DataArray (xarray-like API)
data = np.random.rand(10, 256, 256).astype(np.float32)
times = np.array(['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01',
                  '2024-06-01', '2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01'],
                 dtype='datetime64[D]')

red = DataArray(
    name="red",
    data=data,
    dims={"time": 10, "y": 256, "x": 256},
    coords={"time": times}
)

# Select by label (sel) or integer index (isel)
subset = red.sel(time=slice("2024-01-01", "2024-06-01"))  # First 6 months
first_timestep = red.isel(time=0)  # First observation

# Compute statistics
temporal_mean = red.mean(dim="time")  # Mean over time -> (256, 256)
overall_mean = red.mean()  # Scalar

# Arithmetic operations (xarray-like)
nir = DataArray(name="nir", data=np.random.rand(10, 256, 256).astype(np.float32),
                dims={"time": 10, "y": 256, "x": 256})
ndvi = (nir - red) / (nir + red)  # Element-wise operations

# Convert to numpy
arrays = red.to_numpy()  # Returns numpy array
```

## Architecture

PixelQuery uses a 2-layer architecture:

```
┌─────────────────────────────────────────────┐
│ Layer 1: Apache Iceberg                     │
│ • Metadata + Data in Parquet files          │
│ • ACID transactions (native)                │
│ • Snapshot-based Time Travel                │
│ • SQLite-backed catalog                     │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│ Layer 2: GeoParquet                         │
│ • Tile boundaries (WKB geometry)            │
│ • R-tree spatial indexing                   │
│ • Band statistics (min/max/mean)            │
│ • DuckDB spatial query integration          │
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

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| **DataArray** | | |
| `sel()` - label-based selection | Implemented | Supports datetime coordinates |
| `isel()` - integer-based selection | Implemented | |
| `mean()`, `max()`, `min()` | Implemented | Single or multiple dimensions |
| `to_numpy()` | Implemented | |
| Arithmetic operations (+, -, *, /) | Implemented | |
| `resample()` | Planned (Phase 3) | |
| `to_xarray()`, `to_pandas()` | Planned (Phase 3) | |
| **Dataset** | | |
| `to_numpy()` | Implemented | Returns dict of arrays |
| `to_xarray()`, `to_pandas()` | Planned (Phase 3) | |
| **Ingestion** | | |
| COG ingestion | Implemented | |
| Parallel processing | Implemented | |
| **Storage** | | |
| Iceberg storage | Implemented | Native ACID transactions |
| Time Travel | Implemented | Via snapshots |
| GeoParquet catalog | Implemented | |
| **CLI Tools** | | |
| `info` - Show warehouse metadata | Implemented | |
| `migrate` - Arrow to Iceberg migration | Implemented | |
| `recovery diagnose` - Detect issues | Implemented | |
| `recovery repair` - Fix warehouse | Implemented | |

## Performance

**Multi-year time-series queries** (vs COG+STAC):

| Operation | PixelQuery | COG+STAC | Speedup |
|-----------|------------|----------|---------|
| 1 year (120 images) | 4-11s | 45-120s | **10-30x** |
| 5 years (600 images) | 5-18s | 150-400s | **25-30x** |

*Benchmark: Single tile, 2 bands, monthly aggregation*

*See [BENCHMARK_RESULTS.md](./BENCHMARK_RESULTS.md) for detailed methodology and results.*

## Installation

```bash
# From source (development)
git clone https://github.com/yourusername/pixelquery.git
cd pixelquery
pip install -e ".[dev]"
```

**Requirements:**
- Python 3.11+
- Apache Iceberg (PyIceberg)
- PyArrow
- GeoPandas
- Rasterio
- DuckDB

## CLI Tools

PixelQuery provides command-line tools for warehouse management and diagnostics:

```bash
# Show warehouse metadata and info
pixelquery info ./warehouse

# Migrate Arrow storage to Iceberg
pixelquery migrate ./warehouse

# Diagnose warehouse issues
pixelquery recovery diagnose ./warehouse

# Repair warehouse (fixes corruption, orphaned files, etc)
pixelquery recovery repair ./warehouse
```

## Documentation

- **[API Examples](docs/api-examples.md)** - Real-world usage examples
- **[Package Structure](docs/package-structure.md)** - Architecture and design
- **[Iceberg Integration](docs/iceberg-integration.md)** - Apache Iceberg storage and Time Travel
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
