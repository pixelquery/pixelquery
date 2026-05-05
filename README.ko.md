# PixelQuery

> 🇬🇧 [English version](README.md) — 메인 문서는 영어입니다. 이 페이지는 한국어 사용자를 위한 보존본이며 일부 섹션이 최신이 아닐 수 있습니다.

> S3 위성영상을 GDAL 없이 시공간 검색, Polygon Clip, NDVI, 시계열, PNG 렌더링까지.

[![PyPI](https://img.shields.io/pypi/v/pixelquery.svg)](https://pypi.org/project/pixelquery/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://python.org)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## What is PixelQuery?

PixelQuery는 S3(또는 로컬)의 Cloud-Optimized GeoTIFF(COG)를 [Icechunk](https://icechunk.io/) 가상 Zarr 저장소로 변환하여, **GDAL/rasterio 없이** 시공간 쿼리를 수행하는 Python 라이브러리입니다.

- **Zero data copy** — Virtual reference만 저장 (원본 COG 복제 없음)
- **GDAL-free query** — 쿼리 경로에서 rasterio/GDAL 불필요 (shapely + numpy만 사용)
- **S3 native** — MinIO, AWS S3 등 S3 호환 스토리지 직접 지원
- **한 클래스** — `PixelQueryS3` 하나로 인제스트부터 렌더링까지
- **실시간 서빙** — 필지 Clip → NDVI → PNG가 80ms (DB 불필요)

## Architecture

```
COG files (S3/MinIO)
    │
    ▼
VirtualTIFF parser (byte offset만 읽기, ~3ms/file)
    │
    ▼
Icechunk repository (가상 청크 참조 저장)
    │
    ▼
xarray.open_zarr() (lazy loading, S3 접근 없음)
    │
    ▼
crop / clip / stats / NDVI / PNG  ← 이 시점에 필요한 byte range만 S3에서 fetch
```

## Quick Start

```bash
pip install pixelquery[icechunk]
```

```python
from pixelquery.io.s3_client import PixelQueryS3

# 1. 클라이언트 생성 (Icechunk + VCC + Registry 자동 설정)
pq = PixelQueryS3(
    bucket="my-bucket",
    endpoint_url="http://localhost:9000",  # MinIO
    access_key_id="minioadmin",
    secret_access_key="minioadmin",
)

# 2. COG 인제스트 (중복 자동 스킵)
pq.ingest_cogs("arps/", band_names=["blue", "green", "red", "nir"])

# 3. 시공간 검색
scenes = pq.list_scenes(
    time_range=("2025-01-01", "2025-06-01"),
    bounds=(128.70, 36.31, 128.74, 36.33),
)

# 4. 씬 열기 → Crop → Clip → 통계
ds = pq.open_scene(scenes[0])
cropped = pq.crop(ds, (128.70, 36.31, 128.74, 36.33))
clipped = pq.clip(cropped, geojson_polygon)
stats = pq.stats(clipped)  # {mean, std, min, max, median, p25, p75}
```

## Core Features

### Spatiotemporal Search

메타데이터 기반 검색으로 픽셀 읽기 없이 씬을 필터링합니다.

```python
# 시간 범위
scenes = pq.list_scenes(time_range=("2025-04-01", "2025-10-01"))

# 공간 범위 (BBox)
scenes = pq.list_scenes(bounds=(128.70, 36.31, 128.74, 36.33))

# 시공간 AND 조건
scenes = pq.list_scenes(
    time_range=("2025-04-01", "2025-10-01"),
    bounds=(128.70, 36.31, 128.74, 36.33),
)
```

### BBox Crop

관심 영역만 잘라냅니다. Icechunk가 필요한 byte range만 S3에서 읽습니다.

```python
ds = pq.open_scene(scenes[0])
cropped = pq.crop(ds, (128.70, 36.31, 128.74, 36.33))
```

### GeoJSON Polygon Clip

필지 경계(Polygon)로 마스킹합니다. 폴리곤 외부 픽셀은 NaN. **shapely 2.0 C-level 연산** 사용.

```python
field_polygon = {
    "type": "Polygon",
    "coordinates": [[[128.700, 36.310], [128.720, 36.308],
                     [128.725, 36.320], [128.710, 36.325],
                     [128.695, 36.318], [128.700, 36.310]]]
}

ds = pq.open_scene(scenes[0])
clipped = pq.clip(ds, field_polygon)
```

### NDVI Calculation + PNG Rendering

matplotlib 없이 PIL만으로 NDVI 컬러맵 PNG를 생성합니다. 폴리곤 외부는 투명(alpha=0).

```python
# 한 줄로 clip → NDVI → PNG
png_bytes = pq.clip_to_png(scenes[0], field_polygon, expression="ndvi")

# 단일 밴드 렌더링
png_bytes = pq.clip_to_png(scenes[0], field_polygon,
                           expression="band", band=0, colormap="viridis")

# 단계별 제어
clipped = pq.clip(pq.crop(pq.open_scene(scenes[0]), bbox), field_polygon)
ndvi = pq.ndvi(clipped)                                    # 2D float32 array
png = pq.render_png(ndvi, colormap="rdylgn", vmin=-0.2, vmax=0.8)  # PNG bytes
```

Built-in colormaps: `rdylgn` (vegetation), `viridis`, `inferno`

### Timeseries Analysis

폴리곤 영역의 시간별 밴드 평균을 추적합니다.

```python
ts = pq.timeseries(field_polygon, time_range=("2025-01-01", "2025-12-31"))
# [{"date": "2025-01-01", "mean": 720.0, "band1_mean": ...}, ...]
```

### Multi-Field Comparison

GeoJSON FeatureCollection으로 다중 필지를 한번에 비교합니다.

```python
results = pq.multi_field_stats(feature_collection)
# [{"id": "field_A", "name": "논 A", "pixels": 5120, "mean": 849.6, ...}, ...]
```

### COG Export

클립 결과를 Cloud-Optimized GeoTIFF로 내보냅니다. (Tiled + Overviews + DEFLATE)

```python
pq.clip_to_cog(scenes[0], field_polygon, "output.tif")
```

### Duplicate Ingestion Prevention

동일한 COG를 중복 인제스트하면 자동으로 건너뜁니다. `source_file` URL을 기준으로 dedup.

```python
pq.ingest_cogs("arps/", band_names=["b1", "b2", "b3", "b4"])  # 7 ingested
pq.ingest_cogs("arps/", band_names=["b1", "b2", "b3", "b4"])  # 0 ingested (all skipped)
```

### Error Handling

커스텀 예외로 에러 원인을 명확하게 전달합니다. 깨진 COG가 있어도 나머지는 정상 인제스트됩니다.

```python
from pixelquery.core.exceptions import IngestionError, QueryError, ValidationError

# 1. 인제스트: 깨진 COG는 건너뛰고 계속 진행 (graceful degradation)
groups = pq.ingest_cogs("mixed/", band_names=["b1", "b2", "b3", "b4"])
if pq.last_ingest_errors:
    for err in pq.last_ingest_errors:
        print(f"  Failed: {err['url']} — {err['error']}")

# 2. S3 접근 실패 → IngestionError
try:
    pq.list_cogs("nonexistent/")
except IngestionError as e:
    print(e)  # "S3 list failed (bucket=my-bucket, prefix='nonexistent/'): ..."

# 3. 씬 열기 실패 → QueryError
try:
    pq.open_scene("invalid_group")
except QueryError as e:
    print(e)  # "Failed to open scene 'invalid_group': ..."

# 4. 잘못된 GeoJSON → ValidationError
try:
    pq.clip(ds, invalid_polygon)
except ValidationError as e:
    print(e)  # "Invalid geometry: Self-intersection[128.71 36.32]"

# 5. 밴드 인덱스 범위 초과 → ValidationError
try:
    pq.ndvi(ds, band_red=10, band_nir=11)  # 4-band 데이터에 10, 11 요청
except ValidationError as e:
    print(e)  # "Band index out of range: red=10, nir=11, data has 4 bands"
```

| Exception | Raised by | Meaning |
|-----------|-----------|---------|
| `IngestionError` | `list_cogs()` | S3 접근 실패 |
| `QueryError` | `open_scene()` | 씬 열기/zarr 파싱 실패 |
| `ValidationError` | `clip()`, `clip_to_png()`, `ndvi()` | 잘못된 geometry 또는 밴드 인덱스 |

### Scene Caching

`open_scene()`은 결과를 자동 캐싱합니다. xarray Dataset은 lazy(dask-backed)이므로 메모리 증가 없이 반복 호출 성능이 크게 향상됩니다.

```python
# 첫 호출: ~350ms (zarr metadata 파싱 + 좌표 계산)
ds = pq.open_scene(scenes[0])

# 두 번째 호출: ~0ms (캐시 히트)
ds = pq.open_scene(scenes[0])

# timeseries()에서 자동 혜택: 7개 씬 × 350ms → 캐시 후 ~0ms
ts = pq.timeseries(polygon)  # 첫 실행 후 재호출 시 즉시 완료

# 캐시 수동 비우기 (인제스트 후 새 데이터 반영 시)
pq.clear_cache()
```

- 최대 64개 씬 캐시 (FIFO eviction)
- lazy Dataset이라 추가 메모리 미미
- 캐시 키: `group:band1,band2,...` (밴드 조합별 분리)

### STAC Compatibility

`list_scenes()` 결과를 [STAC 1.0.0](https://stacspec.org/) 포맷으로 변환할 수 있습니다. **pystac 의존 없이** 순수 dict로 생성됩니다.

```python
# 개별 씬 → STAC Item
scenes = pq.list_scenes()
item = pq.to_stac_item(scenes[0])
print(item["type"])          # "Feature"
print(item["stac_version"])  # "1.0.0"
print(item["properties"]["datetime"])  # "2025-04-15T00:00:00+00:00"
print(item["properties"]["eo:bands"])  # [{"name": "blue"}, {"name": "green"}, ...]
print(item["bbox"])          # [128.70, 36.31, 128.74, 36.33]

# 전체 컬렉션 → STAC Collection
collection = pq.to_stac_collection(collection_id="my-farm-2025")
print(collection["type"])    # "Collection"
print(len(collection["items"]))  # 씬 수
print(collection["extent"]["spatial"]["bbox"])     # [[minx, miny, maxx, maxy]]
print(collection["extent"]["temporal"]["interval"])  # [["2025-01-01T...", "2025-12-31T..."]]

# JSON 파일로 저장 (다른 STAC 도구와 연동)
import json
with open("collection.json", "w") as f:
    json.dump(collection, f, indent=2)
```

STAC Item 구조:

```json
{
  "type": "Feature",
  "stac_version": "1.0.0",
  "id": "scene_20250415_0003",
  "geometry": { "type": "Polygon", "coordinates": [...] },
  "bbox": [128.70, 36.31, 128.74, 36.33],
  "properties": {
    "datetime": "2025-04-15T00:00:00+00:00",
    "product_id": "dji_mavic3m",
    "eo:bands": [{"name": "blue"}, {"name": "green"}, {"name": "red"}, {"name": "nir"}]
  },
  "assets": {
    "data": { "href": "s3://my-bucket/arps/2025-04-15.tif", "type": "image/tiff; application=geotiff" }
  }
}
```

## Performance

MinIO S3 (localhost) 기준, 4-band int16 874x3519 COG 벤치마크:

| Operation | Time |
|-----------|------|
| COG ingest (per file) | ~100ms |
| list_scenes (메타데이터) | < 1ms |
| Crop (BBox) | 39ms |
| Clip (polygon) | 58ms |
| Statistics (7 metrics) | 174ms |
| Timeseries (7 scenes) | 378ms (54ms/scene) |
| Multi-field (3 polygons) | 140ms |
| **Clip → NDVI → PNG** | **80ms** |
| Clip → COG export | 8.7s |

**Real-time serving**: 필지 단위 Clip → NDVI → PNG를 80ms에 수행하므로, 중간 결과를 DB에 저장하지 않고 요청마다 계산해도 충분합니다. 동시 8 요청 시 ~38 req/s 처리 가능.

## GDAL-Free Query Path

PixelQuery의 쿼리 경로는 **rasterio/GDAL에 의존하지 않습니다**.

| Operation | GDAL-free? | How |
|-----------|-----------|-----|
| open_scene | Yes | xarray + zarr + icechunk |
| crop | Yes | xarray .sel() |
| clip | Yes | shapely.contains_xy() + numpy meshgrid |
| bandmath / NDVI | Yes | numpy arithmetic |
| stats | Yes | numpy nanmean/nanstd/nanpercentile |
| timeseries | Yes | 위 조합 |
| render_png | Yes | PIL + numpy LUT |
| to_stac_item / to_stac_collection | Yes | 순수 dict 생성 (no pystac) |
| **to_cog (export)** | **No** | rasterio (write path only) |

rasterio는 **COG 쓰기(export)에만** 필요합니다. 쿼리/분석 경로는 순수 Python + numpy + shapely로 동작합니다.

## Local File Support

S3 없이 로컬 COG 파일도 지원합니다:

```python
import pixelquery as pq

# 로컬 디렉토리에서 인제스트
result = pq.ingest("./my_cogs/", band_names=["blue", "green", "red", "nir"])

# 쿼리
ds = pq.open_xarray("./warehouse")
```

## Product Profiles

위성별 프로파일을 등록하면 밴드 매핑이 자동으로 설정됩니다:

```python
pq.register_product(
    "sentinel2_l2a",
    bands={"blue": 1, "green": 2, "red": 3, "nir": 7},
    resolution=10.0,
    provider="ESA",
)
```

## Time Travel

Icechunk의 스냅샷 기능으로 과거 상태를 조회할 수 있습니다:

```python
cat = pq.catalog("./warehouse")
snapshots = cat.get_snapshot_history()
old_ds = pq.open_xarray("./warehouse", snapshot_id=snapshots[-1]["snapshot_id"])
```

## API Reference

### PixelQueryS3 (S3 High-Level API)

```python
from pixelquery.io.s3_client import PixelQueryS3
```

| Method | Description |
|--------|-------------|
| `PixelQueryS3(bucket, ...)` | 클라이언트 생성 (Icechunk + VCC 자동 설정) |
| `pq.list_cogs(prefix)` | S3 COG 파일 목록 |
| `pq.ingest_cogs(prefix, ...)` | COG 인제스트 (중복 자동 스킵) |
| `pq.list_scenes(time_range, bounds)` | 시공간 메타데이터 검색 |
| `pq.open_scene(scene)` | 씬 열기 (lazy xarray Dataset) |
| `pq.crop(ds, bounds)` | BBox crop |
| `pq.clip(ds, geometry)` | GeoJSON polygon clip |
| `pq.stats(ds)` | 통계 (mean, std, min, max, median, p25, p75) |
| `pq.timeseries(polygon, ...)` | 폴리곤 시계열 분석 |
| `pq.multi_field_stats(fc)` | 다중 필지 통계 |
| `pq.clip_to_cog(scene, polygon, path)` | Clip → COG export |
| `pq.ndvi(ds)` | NDVI 계산 → 2D array |
| `pq.render_png(data_2d, colormap)` | 2D array → PNG bytes |
| `pq.clip_to_png(scene, polygon, ...)` | Clip → NDVI/Band → PNG (one call) |
| `pq.clear_cache()` | 씬 캐시 비우기 |
| `pq.to_stac_item(scene)` | 씬 메타데이터 → STAC 1.0.0 Item dict |
| `pq.to_stac_collection(...)` | 전체 씬 → STAC 1.0.0 Collection dict |
| `pq.last_ingest_errors` | 마지막 인제스트의 실패 목록 (list[dict]) |

### Core Functions (Local)

| Function | Description |
|----------|-------------|
| `pq.ingest(source, warehouse, ...)` | 로컬 COG 인제스트 |
| `pq.open_xarray(warehouse, ...)` | lazy xarray Dataset |
| `pq.timeseries(warehouse, lon, lat)` | 포인트 시계열 |
| `pq.inspect_cog(path)` | COG 메타데이터 |
| `pq.catalog(warehouse)` | 카탈로그 조회 |

## Installation

### From PyPI

```bash
pip install pixelquery[icechunk]
```

### From Source

```bash
git clone https://github.com/pixelquery/pixelquery.git
cd pixelquery
pip install -e ".[icechunk,dev]"
```

### S3 Rendering (optional)

```bash
pip install Pillow  # PNG rendering (no matplotlib needed)
```

## When to Use PixelQuery

| Scenario | Best Tool |
|----------|-----------|
| S3 COG → 필지 NDVI/통계 실시간 서빙 | **PixelQuery** |
| Private COGs → time-series analysis | **PixelQuery** |
| Public satellite data catalog | STAC + stackstac |
| Enterprise cloud data platform | Arraylake |
| Planetary-scale analysis | Google Earth Engine |

PixelQuery는 S3에 저장된 위성영상을 GDAL 없이 빠르게 시공간 검색하고, 필지 단위 분석 결과를 실시간으로 서빙하려는 개발자를 위해 설계되었습니다.

## Contributing

Contributions are welcome! Please open an issue or PR.

## License

Apache 2.0
