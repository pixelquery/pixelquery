# PixelQuery

> Query S3-hosted satellite imagery without GDAL — clip, NDVI, and serve PNG in milliseconds.

🇰🇷 [한국어 README](README.ko.md)

[![PyPI](https://img.shields.io/pypi/v/pixelquery.svg)](https://pypi.org/project/pixelquery/)
[![Python](https://img.shields.io/pypi/pyversions/pixelquery.svg)](https://pypi.org/project/pixelquery/)
[![License: Apache 2.0](https://img.shields.io/pypi/l/pixelquery.svg)](LICENSE)
[![CI](https://github.com/pixelquery/pixelquery/actions/workflows/ci.yml/badge.svg)](https://github.com/pixelquery/pixelquery/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/pixelquery/pixelquery/branch/main/graph/badge.svg)](https://codecov.io/gh/pixelquery/pixelquery)
[![Downloads](https://img.shields.io/pypi/dm/pixelquery.svg)](https://pypi.org/project/pixelquery/)

PixelQuery turns Cloud-Optimized GeoTIFFs (COGs) on S3-compatible storage into a real-time, GDAL-free analysis cube. It uses [Icechunk](https://icechunk.io/) virtual zarr stores so the original COGs are never copied — only their byte offsets are catalogued. Queries pull only the bytes they actually need.

## Quick start

```bash
pip install "pixelquery[icechunk]"
```

```python
from pixelquery.io.s3_client import PixelQueryS3

pq = PixelQueryS3(
    bucket="my-bucket",
    endpoint_url="http://localhost:9000",  # MinIO; omit for AWS S3
    access_key_id="minioadmin",
    secret_access_key="minioadmin",
)

# 1. Ingest COGs (virtual references — no data copy)
pq.ingest_cogs("arps/", band_names=["blue", "green", "red", "nir"])

# 2. Spatiotemporal search
scenes = pq.list_scenes(
    time_range=("2025-01-01", "2025-06-01"),
    bounds=(128.70, 36.31, 128.74, 36.33),
)

# 3. Clip a field polygon (GeoJSON dict), compute NDVI, render PNG
field_polygon = {"type": "Polygon", "coordinates": [...]}
png_bytes = pq.clip_to_png(scenes[0], field_polygon, expression="ndvi")
```

## Why PixelQuery

- **Zero data copy.** Ingestion stores virtual chunk references, not pixels. Original COGs stay in S3 untouched.
- **GDAL-free query path.** `crop`, `clip`, `ndvi`, `stats`, and `timeseries` use `xarray` + `numpy` + `shapely` — no `rasterio` import on the hot path. (`rasterio` is needed only for COG export.)
- **S3-native.** Works with MinIO, AWS S3, and any S3-compatible store. No GDAL VFS configuration to babysit.
- **Real-time serving.** A polygon clip → NDVI → PNG round-trip takes ~80 ms on a 4-band int16 COG, fast enough to serve from a request handler without a precomputed cache.

## Core capabilities

### Spatiotemporal search
Filter scenes by time range and bounding box without reading any pixels:
```python
scenes = pq.list_scenes(
    time_range=("2025-04-01", "2025-10-01"),
    bounds=(128.70, 36.31, 128.74, 36.33),
)
```

### BBox + GeoJSON polygon clip
Read only the byte ranges that overlap your area of interest. Polygon clipping uses shapely's C-level `contains_xy` for sub-millisecond per-pixel masking:
```python
ds      = pq.open_scene(scenes[0])
cropped = pq.crop(ds, bbox)
clipped = pq.clip(cropped, field_polygon)
stats   = pq.stats(clipped)  # mean, std, min, max, median, p25, p75
```

### NDVI computation and PNG rendering
Compute NDVI and emit a colormapped PNG in one call, with no `matplotlib` dependency (PIL only):
```python
png_bytes = pq.clip_to_png(scenes[0], field_polygon, expression="ndvi")
```
Built-in colormaps: `rdylgn` (vegetation), `viridis`, `inferno`.

### Timeseries and multi-field analysis
Track polygon-level statistics over time, or compare many fields in one call:
```python
ts      = pq.timeseries(field_polygon, time_range=("2025-01-01", "2025-12-31"))
results = pq.multi_field_stats(feature_collection)
```

### STAC 1.0.0 export
Convert search results into STAC Items or a STAC Collection — no `pystac` dependency:
```python
item       = pq.to_stac_item(scenes[0])
collection = pq.to_stac_collection(collection_id="my-farm-2025")
```

## Performance

Benchmark on MinIO (localhost), 4-band int16 874×3519 COGs:

| Operation | Time |
|---|---|
| COG ingest (per file) | ~100 ms |
| `list_scenes` (metadata only) | < 1 ms |
| BBox crop | 39 ms |
| Polygon clip | 58 ms |
| Statistics (7 metrics) | 174 ms |
| Timeseries (7 scenes) | 378 ms (54 ms/scene) |
| Multi-field (3 polygons) | 140 ms |
| **Clip → NDVI → PNG** | **80 ms** |
| Clip → COG export | 8.7 s |

At ~80 ms per polygon clip → NDVI → PNG, ~38 requests/second per process is achievable without any precomputed cache.

## When to use PixelQuery

| Scenario | Best tool |
|---|---|
| Private S3 COGs → real-time field-level NDVI / stats | **PixelQuery** |
| Time-series analysis over a private COG archive | **PixelQuery** |
| Public satellite catalogs (Sentinel, Landsat) | STAC + [stackstac](https://github.com/gjoseph92/stackstac) |
| Enterprise managed cloud platform | Arraylake |
| Planetary-scale processing | Google Earth Engine |

## Documentation

- [Changelog](CHANGELOG.md) — what shipped in each release
- [Contributing guide](CONTRIBUTING.md) — development setup, tests, and pull requests
- [Security policy](SECURITY.md) — how to report a vulnerability
- Full documentation site — *coming soon*

## Contributing

Bug reports, feature requests, and pull requests are all welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) first — it covers the local development setup, the test and lint commands, and the pull request workflow.

## License

PixelQuery is licensed under the [Apache License 2.0](LICENSE).
