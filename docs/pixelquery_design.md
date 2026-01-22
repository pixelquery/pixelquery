# PixelQuery: The Apache Iceberg for Satellite Imagery

> Multi-resolution satellite imagery storage and query engine powered by Apache Iceberg

**Version:** 1.0.0  
**Author:** 호영  
**Status:** Implementation Ready

---

## 📋 Table of Contents

1. [Executive Summary](#executive-summary)
2. [Vision & Motivation](#vision--motivation)
3. [Core Architecture](#core-architecture)
4. [Data Model](#data-model)
5. [Multi-Resolution System](#multi-resolution-system)
6. [Query Engine](#query-engine)
7. [Implementation Plan](#implementation-plan)
8. [API Reference](#api-reference)
9. [Performance Benchmarks](#performance-benchmarks)
10. [Deployment Guide](#deployment-guide)
11. [Commercialization Strategy](#commercialization-strategy)

---

## Executive Summary

### What is PixelQuery?

**PixelQuery = "Apache Iceberg for Satellite Imagery"**

위성영상을 SQL로 쿼리 가능한 데이터 레이크로 변환하는 오픈소스 스토리지 엔진.

### Key Features

```
✅ Multi-Resolution Native
   - Sentinel-2 (10m), Landsat-8 (30m), Planet (3m) 통합
   - 원본 해상도 보존, 정보 손실 없음

✅ ACID Transactions (Iceberg)
   - 동시 쓰기 안전
   - 스냅샷 기반 버전 관리
   - Time Travel 지원

✅ SQL Queryable
   - 공간 쿼리 (ST_Intersects)
   - 시계열 쿼리
   - 밴드 연산 (NDVI, EVI)

✅ Production Ready
   - Spark/Flink/Trino 통합
   - AWS/GCP/Azure 지원
   - 확장 가능 (PB-scale)
```

### Quick Start

```python
from pixelquery import PixelQuery
from datetime import datetime

# 1. 초기화
pq = PixelQuery("s3://my-bucket/pixelquery")

# 2. 이미지 추가 (다양한 해상도)
pq.add_image("sentinel2.tif", datetime(2024, 1, 5), product_id="sentinel2_l2a")
pq.add_image("landsat8.tif", datetime(2024, 1, 7), product_id="landsat8_l2")
pq.add_image("planet.tif", datetime(2024, 1, 10), product_id="planet_ps2")

# 3. 쿼리 (10m 해상도로 통일)
result = pq.query_by_bounds(
    bounds=(127.0, 37.5, 127.1, 37.6),
    date_range=("2024-01-01", "2024-01-31"),
    bands=["red", "nir"],
    target_resolution=10.0
)

# 4. NDVI 계산
df = result.to_pandas()
df['ndvi'] = (df['nir'] - df['red']) / (df['nir'] + df['red'])
```

---

## Vision & Motivation

### The Problem

**현재 위성영상 관리의 문제점:**

```
1. 파일 기반 관리
   - COG 파일을 S3에 dump
   - 파일명으로 관리 (s2_20240105_T52SCG.tif)
   - 메타데이터 분산
   - 중복 데이터

2. 데이터 무결성 없음
   - 동시 쓰기 충돌
   - 덮어쓰기 시 복구 불가
   - 버전 관리 없음

3. 쿼리 어려움
   - "이 영역의 1월 데이터" → 모든 파일 확인
   - 시계열 분석 비효율
   - 제품 간 통합 어려움

4. 비용
   - 중복 스토리지
   - 불필요한 다운로드
   - 수동 전처리
```

### The Solution: PixelQuery

**PixelQuery = 위성영상을 위한 데이터 웨어하우스**

```
Before (파일 시스템):
s3://bucket/
├── sentinel2_20240105_T52SCG.tif
├── sentinel2_20240110_T52SCG.tif
├── landsat8_20240107_LC08_116034.tif
└── planet_20240110_1234_5678.tif

→ 메타데이터 없음
→ 쿼리 불가
→ 무결성 없음

After (PixelQuery):
s3://bucket/pixelquery/
├── warehouse/
│   └── tile_catalog/          # Iceberg
│       ├── metadata/
│       └── data/*.parquet     # GeoParquet
└── tiles/
    └── x0024_y0041/
        └── 2024-01.arrow      # Pixel data

→ 중앙 집중식 메타데이터
→ SQL 쿼리 가능
→ ACID 보장
```

### Target Users

```
1. 위성 데이터 서비스 회사
   - Planet, Airbus, 국내 스타트업
   - 내부 데이터 관리 비용 절감
   - 고객 서빙 파이프라인 간소화

2. 국방/정보기관
   - 시계열 분석 (군사 작전)
   - Time Travel (과거 시점 복원)
   - On-premise 폐쇄망 지원

3. AgriTech / 재난 모니터링
   - 빠른 시계열 쿼리
   - 밴드 연산 (NDVI, EVI)
   - 대규모 ROI 처리
```

---

## Core Architecture

### Three-Layer Design

```
┌─────────────────────────────────────────────────────┐
│           Layer 1: Apache Iceberg                    │
│           (Transaction & Metadata Layer)             │
│                                                      │
│  역할: "데이터베이스 엔진"                              │
│  - ACID 트랜잭션                                      │
│  - 스냅샷 관리 (Time Travel)                          │
│  - 스키마 진화                                        │
│  - 파티션 관리 (월별 Hidden Partitioning)              │
│                                                      │
│  Technology: PyIceberg 0.6.0+                        │
│  Storage: Avro/JSON manifests (~10MB)                │
└─────────────────────────────────────────────────────┘
                        ↓ manages
┌─────────────────────────────────────────────────────┐
│           Layer 2: GeoParquet                        │
│           (Spatial Metadata Layer)                   │
│                                                      │
│  역할: "공간 검색 엔진"                                │
│  - WKB geometry (타일 경계)                           │
│  - R-tree 공간 인덱스                                 │
│  - DuckDB spatial 통합                               │
│  - 타일별 통계 (min/max/mean)                         │
│                                                      │
│  Technology: GeoPandas + DuckDB Spatial              │
│  Storage: Parquet (~1MB per partition)               │
└─────────────────────────────────────────────────────┘
                        ↓ references
┌─────────────────────────────────────────────────────┐
│           Layer 3: Arrow IPC                         │
│           (Pixel Data Layer)                         │
│                                                      │
│  역할: "픽셀 저장소"                                   │
│  - 월별 시공간 큐브 [256, 256, N] (가변 크기!)         │
│  - 컬럼 기반 압축 (Zstd, LZ4)                         │
│  - Zero-copy 읽기                                    │
│  - NumPy 직접 변환                                    │
│                                                      │
│  Technology: PyArrow 14.0+                           │
│  Storage: Arrow IPC (~1.5MB per chunk)               │
└─────────────────────────────────────────────────────┘
```

### File System Layout

```
pixelquery_data/
├── warehouse/                          # Iceberg warehouse
│   └── tile_catalog/                   # Iceberg table
│       ├── metadata/
│       │   ├── v1.metadata.json        # Table metadata
│       │   ├── v2.metadata.json
│       │   ├── snap-001.avro           # Snapshot manifests
│       │   ├── snap-002.avro
│       │   └── version-hint.text       # Current version
│       └── data/
│           ├── month_partition=2024-01/
│           │   ├── 00000-0.parquet     # GeoParquet
│           │   └── 00001-0.parquet
│           └── month_partition=2024-02/
│               └── 00000-0.parquet
└── tiles/                              # Arrow chunks
    ├── x0000_y0000/
    │   ├── 2024-01.arrow               # 1월 청크
    │   └── 2024-02.arrow               # 2월 청크
    ├── x0000_y0001/
    │   └── 2024-01.arrow
    └── x0001_y0000/
        └── 2024-01.arrow
```

### Query Execution Flow

```
User Query:
  pq.query_by_bounds(
      bounds=(127.0, 37.5, 127.1, 37.6),
      date_range=("2024-01-01", "2024-01-31")
  )

┌─────────────────────────────────────────┐
│ Phase 0: Iceberg Snapshot Selection     │ ~1-5ms
│ - Load current snapshot                 │
│ - Partition pruning (2024-01)           │
│ - Schema version check                  │
└─────────────────────────────────────────┘
              ↓ manifest files
┌─────────────────────────────────────────┐
│ Phase 1: GeoParquet Spatial Scan        │ ~5-10ms
│ - DuckDB: ST_Intersects(geometry, ROI)  │
│ - Filter by date range                  │
│ - Prune by band statistics              │
│ Result: 10-30 tile paths                │
└─────────────────────────────────────────┘
              ↓ chunk file paths
┌─────────────────────────────────────────┐
│ Phase 2: Arrow Pixel Processing         │ ~50-100ms
│ - Parallel read Arrow chunks            │
│ - Extract by date index (offset calc)   │
│ - Resample to target resolution         │
│ - NumPy vectorized operations           │
└─────────────────────────────────────────┘
              ↓
         QueryResult
```

---

## Data Model

### Iceberg Table Schema

```python
from pyiceberg.schema import Schema
from pyiceberg.types import *

tile_catalog_schema = Schema(
    # === 타일 식별 ===
    NestedField(1, "tile_id", StringType(), required=True),
    NestedField(2, "tile_x", IntegerType(), required=True),
    NestedField(3, "tile_y", IntegerType(), required=True),
    
    # === 공간 정보 (GeoParquet) ===
    NestedField(4, "geometry", BinaryType(), required=True),  # WKB
    NestedField(5, "crs", StringType()),  # "EPSG:4326"
    
    # === 시간 정보 ===
    NestedField(7, "acquisition_date", TimestampType(), required=True),
    NestedField(8, "year_month", StringType(), required=True),  # "2024-01"
    NestedField(9, "observation_count", IntegerType()),
    
    # === 청크 파일 정보 ===
    NestedField(10, "chunk_file_path", StringType(), required=True),
    NestedField(11, "chunk_file_size_bytes", LongType()),
    
    # === 밴드 통계 ===
    NestedField(12, "band_red_min", FloatType()),
    NestedField(13, "band_red_max", FloatType()),
    NestedField(14, "band_red_mean", FloatType()),
    NestedField(15, "band_red_std", FloatType()),
    
    NestedField(16, "band_nir_min", FloatType()),
    NestedField(17, "band_nir_max", FloatType()),
    NestedField(18, "band_nir_mean", FloatType()),
    NestedField(19, "band_nir_std", FloatType()),
    
    # === 제품 정보 ===
    NestedField(20, "product_id", StringType()),  # "sentinel2_l2a"
    NestedField(21, "provider", StringType()),    # "ESA"
    NestedField(22, "sensor", StringType()),      # "MSI"
    
    # === 해상도 정보 (Multi-Resolution) ===
    NestedField(24, "native_resolution", FloatType()),  # 원본 해상도 (m)
    NestedField(25, "pixel_count", IntegerType()),  # 픽셀 수
    
    # === 품질 정보 ===
    NestedField(27, "cloud_cover_percent", FloatType()),
    NestedField(28, "valid_pixel_percent", FloatType()),
    
    # === 감사 정보 ===
    NestedField(30, "created_by", StringType()),
    NestedField(31, "created_at", TimestampType()),
    NestedField(32, "image_id", StringType()),
)

# Partition Spec (Hidden Partitioning)
partition_spec = PartitionSpec(
    PartitionField(
        source_id=8,  # year_month
        field_id=1000,
        transform=IdentityTransform(),
        name="month_partition"
    )
)

# Sort Order (공간 정렬)
sort_order = SortOrder(
    SortField(source_id=2, direction=SortDirection.ASC),  # tile_x
    SortField(source_id=3, direction=SortDirection.ASC),  # tile_y
)
```

### Arrow Chunk Schema (Multi-Resolution)

```python
import pyarrow as pa

monthly_chunk_schema = pa.schema([
    ("tile_id", pa.string()),
    ("year_month", pa.string()),
    
    # === 시계열 정보 ===
    ("acquisition_dates", pa.list_(pa.timestamp("ms"))),
    
    # === 제품별 메타데이터 (Multi-Resolution 핵심!) ===
    ("product_ids", pa.list_(pa.string())),
    ("resolutions", pa.list_(pa.float32())),  # [10.0, 30.0, 3.0]
    ("pixel_shapes", pa.list_(pa.list_(pa.int32()))),  # [[256,256], [85,85], [853,853]]
    
    # === 밴드 데이터 (Flat array, 가변 크기!) ===
    ("band_blue", pa.list_(pa.float32())),
    ("band_green", pa.list_(pa.float32())),
    ("band_red", pa.list_(pa.float32())),
    ("band_nir", pa.list_(pa.float32())),
    ("band_swir1", pa.list_(pa.float32())),
    ("band_swir2", pa.list_(pa.float32())),
    
    # === 메타데이터 ===
    ("observation_count", pa.int32()),
])
```

### Example Data

```python
# tiles/x0024_y0041/2024-01.arrow
{
    "tile_id": "x0024_y0041",
    "year_month": "2024-01",
    
    # 3개 관측
    "acquisition_dates": [
        datetime(2024, 1, 5, 10, 0, 0),   # Sentinel-2
        datetime(2024, 1, 7, 10, 30, 0),  # Landsat-8
        datetime(2024, 1, 10, 9, 45, 0),  # Planet
    ],
    
    # 제품 정보
    "product_ids": ["sentinel2_l2a", "landsat8_l2", "planet_ps2"],
    "resolutions": [10.0, 30.0, 3.0],
    "pixel_shapes": [[256, 256], [85, 85], [853, 853]],
    
    # 밴드 데이터 (flat)
    "band_red": [
        # [Sentinel-2: 65,536개][Landsat-8: 7,225개][Planet: 727,609개]
        # 총: 800,370 개 float32 값
    ],
    
    "observation_count": 3,
}
```

---

## Multi-Resolution System

### Problem Statement

위성 제품마다 픽셀 크기(해상도)가 다름:

| Product | Resolution | Pixels per 2.56km tile |
|---------|-----------|------------------------|
| Sentinel-2 | 10m | 256 × 256 |
| Landsat-8 | 30m | 85 × 85 |
| Planet PlanetScope | 3m | 853 × 853 |
| 드론 영상 | 0.05m | 51,200 × 51,200 |

### Solution: Geographic Tiles + Native Resolution

#### 1. TileGrid System

```python
class TileGrid:
    """
    지리적 기준 타일 그리드
    
    타일 = 고정된 지리적 크기 (2.56km × 2.56km)
    제품마다 다른 픽셀 수로 표현
    """
    
    def __init__(self, origin=(124.0, 33.0), tile_size_meters=2560):
        self.origin = origin  # 원점 (경도, 위도)
        self.tile_size_meters = tile_size_meters
    
    def get_tile_id(self, lon, lat):
        """지리 좌표 → 타일 ID"""
        # 1도 ≈ 111.32km
        tile_x = int((lon - self.origin[0]) * 111320 / self.tile_size_meters)
        tile_y = int((lat - self.origin[1]) * 111320 / self.tile_size_meters)
        return f"x{tile_x:04d}_y{tile_y:04d}"
    
    def get_tile_bounds(self, tile_id):
        """타일 ID → 지리 경계 (WGS84)"""
        tile_x, tile_y = self._parse_tile_id(tile_id)
        
        minx = self.origin[0] + (tile_x * self.tile_size_meters / 111320)
        miny = self.origin[1] + (tile_y * self.tile_size_meters / 111320)
        maxx = minx + (self.tile_size_meters / 111320)
        maxy = miny + (self.tile_size_meters / 111320)
        
        return (minx, miny, maxx, maxy)
    
    def get_pixels_for_resolution(self, resolution_meters):
        """해상도 → 픽셀 수 계산"""
        return int(self.tile_size_meters / resolution_meters)
    
    @staticmethod
    def _parse_tile_id(tile_id):
        """타일 ID 파싱: 'x0024_y0041' → (24, 41)"""
        parts = tile_id.split('_')
        tile_x = int(parts[0][1:])
        tile_y = int(parts[1][1:])
        return tile_x, tile_y
```

#### 2. ProductProfile (다중 해상도 지원)

```python
from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class BandInfo:
    """밴드 정보"""
    native_name: str           # 원본 밴드 이름 (e.g., "B04", "SR_B4")
    standard_name: str         # 표준 밴드 이름 (e.g., "red")
    wavelength: float          # 중심 파장 (nm)
    resolution: float          # 밴드별 해상도 (m)
    bandwidth: Optional[float] = None  # 밴드폭 (nm)

@dataclass
class ProductProfile:
    """위성 제품 프로필"""
    product_id: str            # "sentinel2_l2a"
    provider: str              # "ESA", "USGS", "Planet"
    sensor: str                # "MSI", "OLI", "PS2"
    product_level: str         # "L2A", "L2", "3B"
    native_resolution: float   # 제품 기본 해상도 (m) - 중요!
    
    bands: Dict[str, BandInfo] # 밴드 매핑
    scale_factor: float = 1.0
    offset: float = 0.0
    nodata: int = 0
    
    cloud_band: Optional[str] = None
    native_crs: str = "EPSG:4326"


# === Sentinel-2 L2A ===
SENTINEL2_L2A = ProductProfile(
    product_id="sentinel2_l2a",
    provider="ESA",
    sensor="MSI",
    product_level="L2A",
    native_resolution=10.0,  # 10m
    bands={
        "blue": BandInfo("B02", "blue", 490, 10, 65),
        "green": BandInfo("B03", "green", 560, 10, 35),
        "red": BandInfo("B04", "red", 665, 10, 30),
        "nir": BandInfo("B08", "nir", 842, 10, 115),
        "swir1": BandInfo("B11", "swir1", 1610, 20, 90),
        "swir2": BandInfo("B12", "swir2", 2190, 20, 180),
    },
    scale_factor=0.0001,
    cloud_band="SCL",
)

# === Landsat-8 L2 ===
LANDSAT8_L2 = ProductProfile(
    product_id="landsat8_l2",
    provider="USGS",
    sensor="OLI",
    product_level="L2",
    native_resolution=30.0,  # 30m
    bands={
        "blue": BandInfo("SR_B2", "blue", 482, 30, 65),
        "green": BandInfo("SR_B3", "green", 562, 30, 85),
        "red": BandInfo("SR_B4", "red", 655, 30, 40),
        "nir": BandInfo("SR_B5", "nir", 865, 30, 30),
        "swir1": BandInfo("SR_B6", "swir1", 1610, 30, 90),
        "swir2": BandInfo("SR_B7", "swir2", 2200, 30, 180),
    },
    scale_factor=0.0000275,
    offset=-0.2,
)

# === Planet PlanetScope ===
PLANET_PS2 = ProductProfile(
    product_id="planet_ps2",
    provider="Planet",
    sensor="PS2",
    product_level="3B",
    native_resolution=3.0,  # 3m
    bands={
        "blue": BandInfo("blue", "blue", 485, 3, 70),
        "green": BandInfo("green", "green", 545, 3, 80),
        "red": BandInfo("red", "red", 630, 3, 60),
        "nir": BandInfo("nir", "nir", 820, 3, 90),
    },
    scale_factor=0.0001,
)
```

#### 3. 데이터 저장 (원본 해상도)

```python
def save_tile_native_resolution(
    tile_data,
    product_profile,
    grid,
    output_dir,
    year_month
):
    """
    타일을 원본 해상도로 저장
    
    Parameters:
        tile_data: 타일 픽셀 데이터
        product_profile: ProductProfile 인스턴스
        grid: TileGrid 인스턴스
        output_dir: 출력 디렉토리
        year_month: "2024-01"
    """
    # 1. 픽셀 수 계산
    pixels_per_tile = grid.get_pixels_for_resolution(
        product_profile.native_resolution
    )
    # Sentinel-2: 256
    # Landsat-8: 85
    # Planet: 853
    
    # 2. 청크 경로
    chunk_path = output_dir / tile_data.tile_id / f"{year_month}.arrow"
    
    # 3. 기존 청크 읽기 (있으면)
    if chunk_path.exists():
        existing = pa.ipc.open_file(chunk_path).read_all()
        
        # 기존 데이터
        dates = existing["acquisition_dates"][0].to_pylist()
        product_ids = existing["product_ids"][0].to_pylist()
        resolutions = existing["resolutions"][0].to_pylist()
        shapes = existing["pixel_shapes"][0].to_pylist()
        band_red_flat = existing["band_red"][0].to_numpy()
        
        # 4. 새 데이터 추가 (시간순 정렬)
        import bisect
        insert_idx = bisect.bisect_left(dates, tile_data.acquisition_date)
        
        dates.insert(insert_idx, tile_data.acquisition_date)
        product_ids.insert(insert_idx, product_profile.product_id)
        resolutions.insert(insert_idx, product_profile.native_resolution)
        shapes.insert(insert_idx, [pixels_per_tile, pixels_per_tile])
        
        # 5. 배열 삽입 (offset 계산)
        offset = sum(s[0] * s[1] for s in shapes[:insert_idx])
        new_data_flat = tile_data.band_red.flatten()
        band_red_flat = np.insert(band_red_flat, offset, new_data_flat)
    else:
        # 새 청크 생성
        dates = [tile_data.acquisition_date]
        product_ids = [product_profile.product_id]
        resolutions = [product_profile.native_resolution]
        shapes = [[pixels_per_tile, pixels_per_tile]]
        band_red_flat = tile_data.band_red.flatten()
    
    # 6. Arrow 테이블 생성
    table = pa.table({
        "tile_id": [tile_data.tile_id],
        "year_month": [year_month],
        "acquisition_dates": [dates],
        "product_ids": [product_ids],
        "resolutions": [resolutions],
        "pixel_shapes": [shapes],
        "band_red": [band_red_flat.tolist()],
        "observation_count": [len(dates)],
    })
    
    # 7. 원자적 쓰기
    temp_path = chunk_path.with_suffix(".arrow.tmp")
    chunk_path.parent.mkdir(parents=True, exist_ok=True)
    
    with pa.ipc.new_file(temp_path, table.schema) as writer:
        writer.write_table(table)
    
    temp_path.replace(chunk_path)  # Atomic rename
```

#### 4. 데이터 추출 (Offset 계산)

```python
def extract_observation(chunk, date_idx):
    """
    Arrow 청크에서 특정 시점 데이터 추출
    
    Parameters:
        chunk: Arrow Table
        date_idx: 날짜 인덱스 (0-based)
    
    Returns:
        {
            "data": np.ndarray (2D),
            "resolution": float,
            "shape": [int, int],
            "product_id": str
        }
    """
    # 메타데이터 추출
    product_ids = chunk["product_ids"][0].to_pylist()
    resolutions = chunk["resolutions"][0].to_pylist()
    shapes = chunk["pixel_shapes"][0].to_pylist()
    band_flat = chunk["band_red"][0].to_numpy()
    
    # Offset 계산
    offset = sum(s[0] * s[1] for s in shapes[:date_idx])
    size = shapes[date_idx][0] * shapes[date_idx][1]
    
    # 데이터 추출
    data_1d = band_flat[offset:offset+size]
    data_2d = data_1d.reshape(shapes[date_idx])
    
    return {
        "data": data_2d,
        "resolution": resolutions[date_idx],
        "shape": shapes[date_idx],
        "product_id": product_ids[date_idx],
    }


# 사용 예시
chunk = pa.ipc.open_file("tiles/x0024_y0041/2024-01.arrow").read_all()

# Sentinel-2 데이터 (idx=0)
s2 = extract_observation(chunk, 0)
# s2["data"].shape = (256, 256)
# s2["resolution"] = 10.0

# Landsat-8 데이터 (idx=1)
l8 = extract_observation(chunk, 1)
# l8["data"].shape = (85, 85)
# l8["resolution"] = 30.0

# Planet 데이터 (idx=2)
planet = extract_observation(chunk, 2)
# planet["data"].shape = (853, 853)
# planet["resolution"] = 3.0
```

---

## Query Engine

### Query API

```python
def query_by_bounds(
    self,
    bounds: tuple,
    date: datetime = None,
    date_range: tuple = None,
    bands: list = ["red", "nir"],
    target_resolution: float = 10.0,
    as_of_timestamp: datetime = None,
    as_of_snapshot_id: int = None
) -> QueryResult:
    """
    공간 + 시간 쿼리
    
    Parameters:
        bounds: (minx, miny, maxx, maxy) in EPSG:4326
        date: 특정 날짜
        date_range: (start, end) 날짜 범위
        bands: 밴드 리스트
        target_resolution: 결과 해상도 (m)
        as_of_timestamp: Time Travel (타임스탬프)
        as_of_snapshot_id: Time Travel (스냅샷 ID)
    
    Returns:
        QueryResult with:
            - tiles: List[TileResult]
            - to_pandas() method
            - to_xarray() method
    """
```

### Resampling Logic

```python
def resample_to_target(data, src_resolution, target_resolution):
    """
    데이터를 목표 해상도로 리샘플링
    
    Parameters:
        data: np.ndarray (2D)
        src_resolution: 원본 해상도 (m)
        target_resolution: 목표 해상도 (m)
    
    Returns:
        np.ndarray (2D) resampled
    """
    if src_resolution == target_resolution:
        return data  # 리샘플링 불필요
    
    from scipy.ndimage import zoom
    
    scale_factor = src_resolution / target_resolution
    resampled = zoom(data, scale_factor, order=1)  # bilinear
    
    return resampled


# 예시
# Landsat-8 (30m, 85×85) → 10m (256×256)
l8_10m = resample_to_target(l8_data, 30.0, 10.0)

# Planet (3m, 853×853) → 10m (256×256)
planet_10m = resample_to_target(planet_data, 3.0, 10.0)

# Sentinel-2 (10m, 256×256) → 3m (853×853)
s2_3m = resample_to_target(s2_data, 10.0, 3.0)
```

### Query Execution Example

```python
from pixelquery import PixelQuery
from datetime import datetime

pq = PixelQuery("s3://my-bucket/pixelquery")

# === 기본 쿼리 (10m 해상도) ===
result = pq.query_by_bounds(
    bounds=(127.0, 37.5, 127.1, 37.6),
    date_range=("2024-01-01", "2024-01-31"),
    bands=["red", "nir"],
    target_resolution=10.0  # 기본값
)

# 내부 동작:
# 1. Iceberg: month_partition=2024-01 선택
# 2. GeoParquet: ST_Intersects(geometry, ROI)
#    → 10개 타일 선택
# 3. Arrow: 각 청크에서 날짜 필터링
#    - Sentinel-2 (10m): 그대로 → 256×256
#    - Landsat-8 (30m): 업샘플링 → 256×256
#    - Planet (3m): 다운샘플링 → 256×256
# 4. 결과 반환 (모두 10m 해상도로 통일)

# Pandas로 변환
df = result.to_pandas()
# Columns: tile_id, acquisition_date, product_id, band_red, band_nir

# NDVI 계산
df['ndvi'] = (df['band_nir'] - df['band_red']) / (df['band_nir'] + df['band_red'])


# === 고해상도 쿼리 (3m, Planet 활용) ===
result_hires = pq.query_by_bounds(
    bounds=(127.0, 37.5, 127.01, 37.51),  # 작은 영역
    date="2024-01-10",
    bands=["red", "nir"],
    target_resolution=3.0  # Planet 해상도
)

# 내부 동작:
# - Planet (3m): 그대로 → 853×853
# - Sentinel-2 (10m): 업샘플링 → 853×853
# - Landsat-8 (30m): 업샘플링 → 853×853


# === Time Travel ===
# 2주 전 상태로
past_result = pq.query_by_bounds(
    bounds=(127.0, 37.5, 127.1, 37.6),
    date="2024-01-05",
    as_of_timestamp=datetime(2024, 1, 10, 0, 0, 0)
)

# 또는 스냅샷 ID로
snapshot_result = pq.query_by_bounds(
    bounds=(127.0, 37.5, 127.1, 37.6),
    as_of_snapshot_id=3490349304
)
```

---

## Implementation Plan

### Phase 1: Foundation (Week 1-2)

**목표: Iceberg + ProductProfile + TileGrid**

```
pixelquery/
├── __init__.py
├── core.py
├── iceberg/
│   ├── __init__.py
│   ├── catalog.py        # Iceberg 카탈로그 관리
│   ├── schema.py         # 테이블 스키마 정의
│   └── transactions.py   # ACID 트랜잭션
├── products/
│   ├── __init__.py
│   ├── base.py           # ProductProfile, BandInfo
│   ├── registry.py       # 제품 레지스트리
│   └── profiles/
│       ├── __init__.py
│       ├── sentinel2.py
│       ├── landsat8.py
│       └── planet.py
└── grid/
    ├── __init__.py
    └── tile_grid.py      # TileGrid 클래스
```

**체크리스트:**
- [ ] TileGrid 구현 및 테스트
- [ ] ProductProfile 시스템
- [ ] Iceberg 카탈로그 초기화 (SQLite, Glue)
- [ ] 테이블 스키마 생성
- [ ] 기본 CRUD 작업
- [ ] Snapshot 생성/조회

**산출물:**
```python
from pixelquery import PixelQuery
from pixelquery.products import SENTINEL2_L2A

pq = PixelQuery("./data")
table = pq.catalog.create_table("tile_catalog", schema)
print(f"Created: {table}")
```

### Phase 2: Multi-Resolution Tiling (Week 3-4)

**목표: COG → 다중 해상도 타일 → Arrow 청크**

```
pixelquery/
├── storage/
│   ├── __init__.py
│   ├── tile_writer.py    # COG → 타일 분할
│   ├── chunk_manager.py  # Arrow 청크 관리 (가변 크기)
│   └── tile_reader.py    # Arrow 청크 읽기
└── tests/
    ├── test_tile_writer.py
    ├── test_multi_resolution.py
    └── test_chunk_append.py
```

**체크리스트:**
- [ ] COG → 원본 해상도 타일 분할
- [ ] Rasterio window 읽기
- [ ] 가변 크기 Arrow 청크 생성
- [ ] Offset 계산 로직
- [ ] 자동 시간순 정렬 (bisect)
- [ ] GeoParquet 메타데이터 생성

**산출물:**
```python
from pixelquery.storage import TileWriter

writer = TileWriter(grid, product_profile)
tiles = writer.split_image("sentinel2.tif", acquisition_date)
# tiles: List[Tile] (256×256 for Sentinel-2)

writer.save_chunks(tiles, output_dir, year_month="2024-01")
# → tiles/x0024_y0041/2024-01.arrow
```

### Phase 3: Query Engine (Week 5-6)

**목표: 공간/시간 쿼리 + 리샘플링**

```
pixelquery/
├── query/
│   ├── __init__.py
│   ├── executor.py       # 쿼리 실행기
│   ├── spatial.py        # 공간 쿼리 (DuckDB)
│   ├── temporal.py       # 시간 쿼리
│   ├── resampling.py     # 리샘플링 로직
│   └── iceberg_scan.py   # Iceberg 스캔
└── tests/
    ├── test_query.py
    └── test_resampling.py
```

**체크리스트:**
- [ ] Iceberg snapshot 선택
- [ ] GeoParquet 공간 스캔 (DuckDB)
- [ ] Arrow 청크 읽기 (병렬)
- [ ] Offset 계산으로 데이터 추출
- [ ] 리샘플링 구현 (scipy.ndimage.zoom)
- [ ] target_resolution 파라미터
- [ ] QueryResult 클래스
- [ ] to_pandas(), to_xarray() 변환

**산출물:**
```python
result = pq.query_by_bounds(
    bounds=(127.0, 37.5, 127.1, 37.6),
    date_range=("2024-01-01", "2024-01-31"),
    bands=["red", "nir"],
    target_resolution=10.0
)

df = result.to_pandas()
# tile_id, acquisition_date, product_id, band_red, band_nir
```

### Phase 4: Integration & Testing (Week 7-8)

**목표: End-to-End 통합 테스트**

```
pixelquery/
├── cli/
│   ├── __init__.py
│   └── main.py           # CLI 인터페이스
└── tests/
    ├── integration/
    │   ├── test_e2e.py
    │   └── test_saepam_data.py
    └── benchmarks/
        └── test_performance.py
```

**체크리스트:**
- [ ] CLI 구현 (add-image, query)
- [ ] Saepam 실제 데이터로 테스트
- [ ] 성능 벤치마크
- [ ] 문서화 (README, API docs)
- [ ] PyPI 패키지 준비

**산출물:**
```bash
# CLI 사용
pixelquery init --warehouse ./data
pixelquery add-image sentinel2.tif --date 2024-01-05 --product sentinel2_l2a
pixelquery query --bounds "127.0,37.5,127.1,37.6" --date-range "2024-01-01,2024-01-31"
```

---

## API Reference

### PixelQuery Class

```python
class PixelQuery:
    """Main PixelQuery interface"""
    
    def __init__(
        self,
        warehouse: str,
        catalog_type: str = "sql",
        catalog_config: dict = None
    ):
        """
        초기화
        
        Parameters:
            warehouse: S3/Local path
            catalog_type: "sql", "glue", "rest"
            catalog_config: 카탈로그 설정
        """
    
    def add_image(
        self,
        image_path: str,
        image_id: str,
        acquisition_date: datetime,
        product_id: str,
        created_by: str = None,
        commit_message: str = None
    ) -> dict:
        """이미지 추가 (ACID)"""
    
    def query_by_bounds(
        self,
        bounds: tuple,
        date: datetime = None,
        date_range: tuple = None,
        bands: list = None,
        target_resolution: float = 10.0,
        as_of_timestamp: datetime = None,
        as_of_snapshot_id: int = None
    ) -> QueryResult:
        """공간/시간 쿼리"""
    
    def query_timeseries(
        self,
        location: tuple,
        date_range: tuple,
        bands: list,
        target_resolution: float = 10.0
    ) -> TimeSeriesResult:
        """시계열 쿼리"""
    
    def rollback_to_snapshot(
        self,
        snapshot_id: int
    ):
        """스냅샷 롤백"""
    
    def list_snapshots(self) -> list:
        """스냅샷 목록"""
```

### QueryResult Class

```python
class QueryResult:
    """쿼리 결과"""
    
    def to_pandas(self) -> pd.DataFrame:
        """Pandas DataFrame으로 변환"""
    
    def to_xarray(self) -> xr.Dataset:
        """Xarray Dataset으로 변환"""
    
    def to_numpy(self) -> dict:
        """NumPy 배열로 변환"""
    
    def save(self, path: str, format: str = "parquet"):
        """결과 저장"""
```

---

## Performance Benchmarks

### Target Performance

| Operation | Target | Notes |
|-----------|--------|-------|
| 이미지 추가 (Sentinel-2) | < 10s | COG → 타일 → Arrow |
| 공간 쿼리 (10 타일) | < 100ms | Phase 0+1+2 |
| 시계열 쿼리 (1년) | < 50ms | 12개월 청크 |
| 리샘플링 (30m→10m) | < 20ms | scipy.ndimage.zoom |
| Time Travel | < 5ms | Iceberg 스냅샷 |

### Storage Efficiency

```
타일당 스토리지 (1년, 월 6회 관측):

Sentinel-2만:
- 256×256×12×6 = 4,718,592 픽셀
- float32 = 18,874,368 bytes ≈ 18 MB

Sentinel-2 + Landsat-8:
- (256²×6 + 85²×6)×12 = 5,237,880 픽셀
- float32 = 20,951,520 bytes ≈ 20 MB

Sentinel-2 + Landsat-8 + Planet:
- (256²×6 + 85²×6 + 853²×6)×12 = 57,683,640 픽셀
- float32 = 230,734,560 bytes ≈ 220 MB

압축 후 (Zstd, level 3):
- 약 0.6-0.7x 압축률
- 220 MB → 130-150 MB
```

### Scalability

```
ROI: 서울 (30km × 30km)
타일 수: 약 144개 (12×12)

1년 데이터:
- Sentinel-2만: 144 × 18 MB = 2.6 GB
- All products: 144 × 220 MB = 31.7 GB

10년 데이터:
- All products: 317 GB (압축 후 ~190 GB)

전국 (500km × 1000km):
- 타일 수: 약 76,000개
- 10년 All products: 16.7 TB (압축 후 ~10 TB)
```

---

## Deployment Guide

### Local Development

```bash
# 1. 설치
pip install pixelquery

# 2. 초기화
pixelquery init --warehouse ./pixelquery_data

# 3. 데이터 추가
pixelquery add-image \
    sentinel2_20240105.tif \
    --date 2024-01-05 \
    --product sentinel2_l2a

# 4. 쿼리
pixelquery query \
    --bounds "127.0,37.5,127.1,37.6" \
    --date-range "2024-01-01,2024-01-31" \
    --output result.parquet
```

### AWS Deployment

```python
from pixelquery import PixelQuery

# S3 + Glue Catalog
pq = PixelQuery(
    warehouse="s3://my-bucket/pixelquery",
    catalog_type="glue",
    catalog_config={
        "database": "pixelquery_db",
        "region": "ap-northeast-2"
    }
)

# Lambda로 자동 처리
# S3 PutObject → Lambda → pq.add_image()
```

### Spark Integration

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.pixelquery", 
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.pixelquery.type", "glue") \
    .config("spark.sql.catalog.pixelquery.warehouse", 
            "s3://bucket/pixelquery") \
    .getOrCreate()

# PixelQuery 테이블을 Spark로
df = spark.table("pixelquery.tile_catalog")

# 집계
df.groupBy("product_id", "year_month") \
    .agg(
        count("*").alias("tile_count"),
        avg("band_red_mean").alias("avg_red")
    ) \
    .show()
```

---

## Commercialization Strategy

### Open Core Model

```
Open Source (Apache 2.0):
├── Core library
├── Local catalog (SQLite)
├── Basic queries
├── Python API
└── CLI

Commercial (Enterprise):
├── Distributed catalog (Glue, Hive, REST)
├── Query optimizer
├── RBAC & audit logs
├── 24/7 support
└── SLA guarantees

SaaS (Managed):
├── Cloud hosting
├── Auto-scaling
├── Visualization dashboard
├── API gateway
└── Monitoring
```

### Target Markets

1. **위성 데이터 서비스 회사** (B2B)
   - Planet, Airbus, 국내 스타트업
   - 내부 데이터 관리 비용 절감
   - 월 $99-999 (프로 플랜)

2. **국방/정보기관** (B2G)
   - 시계열 분석, Time Travel
   - On-premise 폐쇄망
   - 초기 $100K-500K, 연간 $50K-200K

3. **AgriTech / 재난 모니터링** (B2B)
   - 빠른 시계열 쿼리
   - 대규모 ROI 처리
   - 월 $299-2,999

### Revenue Projections

```
Year 1: $50K-100K
- 컨설팅: $30K
- 연구 과제: $20K-50K
- 초기 고객: $0-20K

Year 2: $300K-500K
- SaaS 구독: $50K (50개 회사)
- 엔터프라이즈: $200K-400K (2-3개)
- 컨설팅: $50K

Year 3: $1M-2M
- SaaS: $200K (200개 회사)
- 엔터프라이즈: $600K-1.5M (5-10개)
- 파트너십: $200K-300K
- Series A 투자 가능
```

---

## Conclusion

**PixelQuery = The Apache Iceberg for Satellite Imagery**

```
✅ Multi-Resolution Native
✅ ACID Transactions
✅ SQL Queryable
✅ Production Ready
✅ Open Source

→ "위성영상의 Iceberg 표준을 만들자"
```

### Next Steps

1. **Week 1-2**: Foundation (Iceberg + ProductProfile)
2. **Week 3-4**: Multi-Resolution Tiling
3. **Week 5-6**: Query Engine
4. **Week 7-8**: Integration & Testing
5. **Month 3**: Open Source Release
6. **Month 6**: First Commercial Customer

---

**Ready to implement! 🚀**

Contact: 호영
Repository: https://github.com/pixelquery/pixelquery (TBD)  
Documentation: https://docs.pixelquery.io (TBD)