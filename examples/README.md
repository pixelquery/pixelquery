# PixelQuery Examples

This directory contains example scripts demonstrating PixelQuery usage.

## Time-Series Demo

`demo_timeseries.py` demonstrates how to:
- Ingest multiple time-series COG files
- Automatically extract acquisition times from metadata
- Compute NDVI time-series
- Analyze vegetation trends over time

### Usage

```bash
python examples/demo_timeseries.py --cog-dir <path-to-cogs> --warehouse <warehouse-path>
```

### Arguments

- `--cog-dir`: Directory containing COG files (required)
- `--warehouse`: Warehouse directory path (default: ./warehouse)
- `--product-id`: Product identifier (default: sentinel2_l2a)
- `--band-mapping`: Band mapping (default: 1:blue,2:green,3:red,4:nir)

### Example

```bash
# Using Sentinel-2 COG files
python examples/demo_timeseries.py \
    --cog-dir ./sentinel2_data \
    --warehouse ./my_warehouse

# Using custom band mapping
python examples/demo_timeseries.py \
    --cog-dir ./landsat_data \
    --warehouse ./landsat_warehouse \
    --product-id "landsat8_l2" \
    --band-mapping "1:blue,2:green,3:red,4:nir"
```

### COG File Requirements

- Files should be Cloud-Optimized GeoTIFF format
- Must contain at least Red and NIR bands for NDVI calculation
- Acquisition time should be in metadata (TIFFTAG_DATETIME tag)
- If metadata missing, time will be extracted from filename (YYYYMMDD pattern) or file modification time

### Output

The script will:
1. Scan and identify all COG files
2. Extract acquisition times
3. Ingest data into PixelQuery warehouse
4. Load time-series data
5. Compute NDVI for each time step
6. Display NDVI time-series and trend analysis
