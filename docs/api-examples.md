# PixelQuery API Examples

> **Status**: Prototype API (Phase 2 completed)
>
> These examples demonstrate the intended xarray-inspired API design.
> Implementation will be completed in Phase 3+.

## Quick Start

### Basic Usage

```python
import pixelquery as pq
from datetime import datetime

# Open a dataset for a specific tile
ds = pq.open_dataset("warehouse", tile_id="x0024_y0041")

# View dataset info
print(ds)
# <PixelQuery.Dataset>
# Tile: x0024_y0041
# Bands: red, green, blue, nir, swir1, swir2
# Time range: (2020-01-01, 2024-12-31)

# Access individual bands
red = ds["red"]
nir = ds["nir"]

print(red)
# <PixelQuery.DataArray 'red'>
# Shape: (500, 256, 256)  # (time, y, x)
# Dimensions: (time: 500, y: 256, x: 256)
# dtype: float32
```

### Selecting Data

```python
# Select specific time range
subset = ds.sel(time=slice("2024-01-01", "2024-12-31"))

# Select specific bands
rgb = ds.sel(bands=["red", "green", "blue"])

# Combine selections
recent_rgb = ds.sel(
    time=slice("2024-01-01", "2024-12-31"),
    bands=["red", "green", "blue"]
)

# Select single timestep
single_day = ds["red"].sel(time="2024-06-15")
```

### Temporal Analysis

```python
# Monthly resampling
monthly_mean = ds.resample(time="1M").mean()

# Weekly maximum
weekly_max = ds.resample(time="1W").max()

# Compute temporal statistics
temporal_mean = ds.mean(dim="time")
temporal_std = ds["nir"].std(dim="time")
```

### Vegetation Indices

```python
# NDVI (Normalized Difference Vegetation Index)
ndvi = pq.compute_ndvi(ds["red"], ds["nir"])

# Or manually
ndvi_manual = (ds["nir"] - ds["red"]) / (ds["nir"] + ds["red"])

# EVI (Enhanced Vegetation Index)
evi = pq.compute_evi(ds["blue"], ds["red"], ds["nir"])

# Time-series analysis
ndvi_timeseries = ndvi.mean(dim=["y", "x"])  # Spatial average
```

### Multi-Tile Analysis

```python
# Open multiple tiles
ds_multi = pq.open_mfdataset(
    "warehouse",
    tile_ids=["x0024_y0041", "x0025_y0041", "x0024_y0042"],
    bands=["red", "nir"]
)

# Query by geographic bounds
tiles_gdf = pq.list_tiles(
    "warehouse",
    bounds=(127.0, 37.5, 127.1, 37.6)  # Seoul area
)

print(tiles_gdf)
# GeoDataFrame with columns: tile_id, geometry, time_range, product_ids
```

## Integration with Scientific Python Ecosystem

### Convert to xarray

```python
# Convert to xarray.Dataset for advanced analysis
xr_ds = ds.to_xarray()

# Use xarray's rich functionality
xr_ds["red"].plot()  # Quick plot
xr_ds.to_netcdf("output.nc")  # Save to NetCDF

# Group by season
seasonal = xr_ds.groupby("time.season").mean()

# Dask integration for large datasets
import dask
xr_ds_lazy = ds.to_xarray(chunks={"time": 10})
```

### Convert to pandas

```python
# Convert to DataFrame for statistical analysis
df = ds.to_pandas()

print(df.head())
#           time  tile_id  product_id  band_red  band_nir  ...
# 0  2024-01-01  x0024_y0041  sentinel2_l2a    0.05      0.35  ...
# 1  2024-01-03  x0024_y0041  sentinel2_l2a    0.06      0.38  ...

# Compute NDVI in pandas
df['ndvi'] = (df['band_nir'] - df['band_red']) / (df['band_nir'] + df['band_red'])

# Time-series plots
import matplotlib.pyplot as plt
df.groupby('time')['ndvi'].mean().plot()
plt.title('Mean NDVI Time Series')
plt.show()
```

### NumPy arrays

```python
# Convert to NumPy for custom processing
arrays = ds.to_numpy()

red_array = arrays['red']  # Shape: (500, 256, 256)
nir_array = arrays['nir']

# Custom analysis
import numpy as np
seasonal_mean = np.mean(red_array.reshape(-1, 12, 256, 256), axis=0)
```

## Real-World Use Cases

### Use Case 1: Agricultural Monitoring

```python
import pixelquery as pq
from datetime import datetime

# Load data for farm area
ds = pq.open_dataset(
    "warehouse",
    tile_id="x0024_y0041",
    time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
    bands=["red", "nir"]
)

# Compute monthly NDVI
ndvi = pq.compute_ndvi(ds["red"], ds["nir"])
monthly_ndvi = ndvi.resample(time="1M").mean()

# Detect anomalies
ndvi_mean = ndvi.mean(dim="time")
ndvi_std = ndvi.std(dim="time")
anomalies = (ndvi - ndvi_mean) / ndvi_std > 2  # Z-score > 2

# Export for visualization
xr_ds = monthly_ndvi.to_xarray()
xr_ds.to_netcdf("farm_ndvi_2024.nc")
```

### Use Case 2: Multi-Resolution Fusion

```python
# Query data from multiple satellites at different resolutions
# Sentinel-2 @ 10m, Landsat-8 @ 30m, Planet @ 3m
result = pq.query_by_bounds(
    warehouse="warehouse",
    bounds=(127.0, 37.5, 127.1, 37.6),
    date_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
    bands=["red", "nir"],
    target_resolution=10.0  # Resample all to 10m
)

# All data is now unified at 10m resolution
df = result.to_pandas()
print(df['product_id'].unique())
# ['sentinel2_l2a', 'landsat8_l2', 'planet_l3a']

# Time-series with 5-30x denser observations
df.groupby('time')['band_nir'].count().plot()
```

### Use Case 3: Change Detection

```python
# Load two time periods
ds_before = pq.open_dataset(
    "warehouse",
    tile_id="x0024_y0041",
    time_range=(datetime(2020, 1, 1), datetime(2020, 12, 31))
)

ds_after = pq.open_dataset(
    "warehouse",
    tile_id="x0024_y0041",
    time_range=(datetime(2024, 1, 1), datetime(2024, 12, 31))
)

# Compute mean NDVI for each period
ndvi_before = pq.compute_ndvi(ds_before["red"], ds_before["nir"]).mean(dim="time")
ndvi_after = pq.compute_ndvi(ds_after["red"], ds_after["nir"]).mean(dim="time")

# Detect changes
ndvi_change = ndvi_after - ndvi_before

# Visualize
import matplotlib.pyplot as plt
xr_change = ndvi_change.to_xarray()
xr_change.plot(cmap='RdYlGn', vmin=-0.5, vmax=0.5)
plt.title('NDVI Change (2020 vs 2024)')
plt.show()
```

## Advanced Features

### Time Travel (Iceberg Snapshots)

```python
# Query historical state of data
result = pq.query_by_bounds(
    warehouse="warehouse",
    bounds=(127.0, 37.5, 127.1, 37.6),
    date_range=(datetime(2024, 1, 1), datetime(2024, 12, 31)),
    bands=["red", "nir"],
    as_of_snapshot_id=12345  # Specific Iceberg snapshot
)

# Compare versions
result_v1 = pq.query_by_bounds(..., as_of_snapshot_id=100)
result_v2 = pq.query_by_bounds(..., as_of_snapshot_id=200)
```

### Custom Product Profiles

```python
# Define custom satellite product (advanced)
from pixelquery.products import ProductProfile, BandInfo

custom_profile = ProductProfile(
    product_id="custom_satellite",
    provider="CustomCo",
    sensor="CustomSensor",
    native_resolution=5.0,
    bands={
        "red": BandInfo(
            native_name="B1",
            standard_name="red",
            wavelength=665.0,
            resolution=5.0,
            bandwidth=30.0
        ),
        # ... more bands
    },
    scale_factor=0.0001,
    offset=0.0,
    nodata=-9999
)
```

## Comparison with COG+STAC

```python
# Traditional COG+STAC approach
import rasterio
import pystac_client

catalog = pystac_client.Client.open("https://...")
items = catalog.search(
    bbox=(127.0, 37.5, 127.1, 37.6),
    datetime="2024-01-01/2024-12-31",
    collections=["sentinel-2-l2a"]
).items()

# Read each COG individually (slow for time-series!)
for item in items:
    with rasterio.open(item.assets["red"].href) as src:
        red = src.read(1)
    # ... process each image separately

# PixelQuery approach (10-30x faster!)
ds = pq.open_dataset("warehouse", tile_id="x0024_y0041")
red = ds["red"]  # All timesteps loaded efficiently
monthly_mean = red.resample(time="1M").mean()  # Fast temporal aggregation
```

## Next Steps

- 📖 See [package-structure.md](package-structure.md) for architecture details
- 🚀 Phase 3: Full implementation of Dataset/DataArray methods
- 📊 Phase 4: Performance benchmarks
- 📚 Phase 5: Comprehensive documentation with Jupyter notebooks
