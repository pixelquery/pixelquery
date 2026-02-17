# PixelQuery

> Turn your COG files into an analysis-ready time-series data cube. No infrastructure required.

[![PyPI](https://img.shields.io/pypi/v/pixelquery.svg)](https://pypi.org/project/pixelquery/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://python.org)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## What is PixelQuery?

PixelQuery converts a directory of Cloud-Optimized GeoTIFFs (COGs) into a
queryable time-series data cube backed by [Icechunk](https://icechunk.io/) virtual Zarr storage.

- **Zero data copy**: Virtual references to original COGs (no duplication)
- **Fast ingestion**: ~9ms per COG (243 files in 2 seconds)
- **Lazy loading**: Data reads from COGs only when you call `.compute()`
- **pip install**: No STAC server, no database, no infrastructure
- **Multi-satellite**: Built-in product profiles for Planet, Sentinel-2, Landsat

## Quick Start

```bash
pip install pixelquery[icechunk]
```

```python
import pixelquery as pq

# Ingest COGs from a directory
result = pq.ingest("./my_cogs/", band_names=["blue", "green", "red", "nir"])
print(f"Ingested {result.scene_count} scenes in {result.elapsed:.1f}s")

# Query as lazy xarray Dataset
ds = pq.open_xarray("./warehouse")
print(ds)  # Dimensions: (time: 243, band: 4, y: 874, x: 3519)
```

## 5-Minute Tutorial

### 1. Inspect your COG files

```python
import pixelquery as pq

# Check what you have
meta = pq.inspect_cog("./scene.tif")
print(meta)  # CRS, bounds, bands, resolution
```

### 2. Ingest

```python
result = pq.ingest(
    "./planet_cogs/",
    warehouse="./warehouse",
    band_names=["blue", "green", "red", "nir"],
    product_id="planet_sr",
)
```

### 3. Query

```python
ds = pq.open_xarray("./warehouse")

# Filter by time range
from datetime import datetime
ds = pq.open_xarray(
    "./warehouse",
    time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
    bands=["red", "nir"],
)
```

### 4. Compute NDVI

```python
nir = ds["data"].sel(band="nir")
red = ds["data"].sel(band="red")
ndvi = (nir - red) / (nir + red)
ndvi.mean(dim="time").compute()  # Actual COG reads happen here
```

### 5. Point time-series

```python
ts = pq.timeseries("./warehouse", lon=127.05, lat=37.55)
ts["data"].sel(band="nir").plot()  # Plot NIR time-series
```

## Product Profiles

Register satellite product definitions for multi-product warehouses:

```python
pq.register_product(
    "sentinel2_l2a",
    bands={"blue": 1, "green": 2, "red": 3, "nir": 7},
    resolution=10.0,
    provider="ESA",
)

# Browse warehouse contents
cat = pq.catalog("./warehouse")
print(cat.summary())
# === PixelQuery Warehouse Summary ===
# Products: 2
#
# planet_sr (Planet)
#   Scenes: 243
#   Bands: blue, green, red, nir
#   Resolution: 3.0m
```

## How It Works

```
COG files (on disk/S3)
    |
    v
VirtualTIFF parser (reads byte offsets, ~3ms/file)
    |
    v
Icechunk repository (stores virtual chunk references)
    |
    v
xarray.open_zarr() (lazy loading)
    |
    v
.compute() → reads actual pixel data from original COGs
```

No data is copied during ingestion. Icechunk stores only the byte-range
references to the original COG files. Actual pixel data is read on-demand
when you call `.compute()` or `.values`.

## Performance

| Operation | Result |
|-----------|--------|
| Single COG ingest | ~3ms (virtual reference) |
| 243 COG batch | 2.1s (8.6ms/COG) |
| Storage overhead | 0.2MB for 4.4GB data |
| Metadata query | 59ms |
| Compute 6 scenes | 255ms |

## Time Travel

Icechunk provides built-in versioning. Every ingest creates a snapshot.

```python
# View history
history = pq.open_xarray("./warehouse", snapshot_id=None)

# Query at a specific point in time
cat = pq.catalog("./warehouse")
snapshots = cat.get_snapshot_history()
old_ds = pq.open_xarray("./warehouse", snapshot_id=snapshots[-1]["snapshot_id"])
```

## API Reference

### Core Functions

| Function | Description |
|----------|-------------|
| `pq.ingest(source, warehouse, ...)` | Auto-scan and ingest COGs |
| `pq.open_xarray(warehouse, ...)` | Query as lazy xarray Dataset |
| `pq.timeseries(warehouse, lon, lat, ...)` | Extract point time-series |

### Inspection

| Function | Description |
|----------|-------------|
| `pq.inspect_cog(path)` | Read COG metadata (CRS, bounds, bands) |
| `pq.inspect_directory(dir)` | Scan directory for COGs |

### Catalog

| Function | Description |
|----------|-------------|
| `pq.catalog(warehouse)` | Get catalog for warehouse |
| `pq.register_product(...)` | Register a product profile |
| `catalog.summary()` | Formatted warehouse summary |
| `catalog.products()` | List product IDs |
| `catalog.scenes(...)` | List scenes with filters |

## Installation

### From PyPI

```bash
pip install pixelquery[icechunk]
```

### From Source

```bash
git clone https://github.com/yourusername/pixelquery.git
cd pixelquery
pip install -e ".[icechunk,dev]"
```

## When to Use PixelQuery

| Scenario | Best Tool |
|----------|-----------|
| Private COGs -> time-series analysis | **PixelQuery** |
| Public satellite data catalog | STAC + stackstac |
| Enterprise cloud data platform | Arraylake |
| Planetary-scale analysis | Google Earth Engine |

PixelQuery is designed for researchers and developers who have their own COG
files and want to query them as a time-series data cube without setting up
any infrastructure.

## Contributing

Contributions are welcome! Please open an issue or PR.

## License

Apache 2.0
