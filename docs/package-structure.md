# PixelQuery 프로젝트 구조 리팩토링 계획

> **작성일**: 2026-01-12
> **목적**: 오픈소스화를 위한 best practice 패키지 구조 설계

## 현재 상황
- **프로젝트**: Apache Iceberg 기반 위성 이미지 저장 엔진
- **목적**: Multi-resolution 위성 데이터를 쿼리 가능한 데이터 레이크로 변환
- **현재 상태**: 초기 설계 단계 (interfaces만 구현됨)
- **구조**: 3-layer 아키텍처 (Iceberg + GeoParquet + Arrow IPC)

## 참고할 오픈소스 프로젝트 추천

**타겟 사용자**: 데이터 과학자/연구원
**중요 측면**: 패키지 구조, API 설계, 테스트, 문서화 모두 중요

데이터 과학자를 위한 라이브러리이므로, **측면별로** 참고할 프로젝트를 분류합니다.

---

### 📦 패키지 구조 (Backend/Internal)

#### 1순위: Apache Iceberg (Python) ⭐
**추천 이유:**
- PixelQuery가 Iceberg를 기반으로 하므로 가장 직접적으로 관련됨
- 테이블 포맷/데이터 엔진 라이브러리로서 유사한 추상화 레벨
- 명확한 계층 구조 (API → Core → I/O → Internals)

**패키지 구조 특징:**
```
pyiceberg/
├── catalog/          # 카탈로그 추상화 (REST, Hive, Glue 등)
├── table/            # 테이블 API와 메타데이터
├── io/              # 파일 I/O (Parquet, Avro)
├── expressions/     # 쿼리 표현식
├── types/           # 데이터 타입 시스템
└── utils/           # 유틸리티
```

**참고 포인트:**
- Protocol-based 설계 (타입 안정성)
- 명확한 public vs private API 분리 (`_internal/` 사용)

#### 2순위: Polars
**추천 이유:**
- Rust 백엔드 + Python API 구조
- 성능 중심 설계 (PixelQuery와 동일한 목표)

**패키지 구조:**
```
polars/
├── dataframe/       # DataFrame API
├── lazyframe/       # 지연 평가 API
├── io/              # I/O (Parquet, CSV, Arrow 등)
├── functions/       # 함수/표현식
└── _internal/       # 내부 구현 (사용자에게 숨김)
```

**참고 포인트:**
- Public API와 internals 엄격 분리
- Lazy evaluation 패턴

---

### 🎨 API 설계 (User-Facing)

#### 1순위: xarray ⭐⭐⭐ (가장 중요!)
**추천 이유:**
- **데이터 과학자들이 이미 위성 이미지/기후 데이터에 사용하는 표준 라이브러리**
- Multi-dimensional labeled data (시간 × 공간 × 밴드)
- PixelQuery의 use case와 정확히 일치

**API 패턴:**
```python
# xarray 스타일
ds = xr.open_dataset("data.nc")
ds.sel(time="2024-01", lat=slice(30, 40))
ds.resample(time="1M").mean()

# PixelQuery에 적용 가능한 패턴
pq = PixelQuery("warehouse")
result = pq.query_time_series(tile_id, time_range, bands)
result.to_xarray()  # xarray로 변환
result.sel(time="2024-01")  # xarray-like 메서드 체이닝
```

**참고 포인트:**
- Labeled dimensions (time, x, y, band)
- Method chaining API
- Pandas/Numpy 통합
- `.to_pandas()`, `.to_numpy()` 변환 메서드

#### 2순위: pandas / GeoPandas
**추천 이유:**
- 데이터 과학자가 가장 익숙한 API
- DataFrame 기반 메타데이터 조회에 적합

**API 패턴:**
```python
# GeoPandas 스타일
gdf = gpd.read_file("data.geojson")
gdf[gdf.within(bbox)].plot()

# PixelQuery에 적용
tiles_gdf = pq.list_tiles(bbox=bbox, time_range=(...))
tiles_gdf.plot()  # GeoDataFrame 반환
```

**참고 포인트:**
- `.to_geopandas()` 메서드 제공
- 메타데이터 조회는 DataFrame으로 반환

#### 3순위: Rasterio
**추천 이유:**
- 위성 이미지 도메인 라이브러리
- GDAL 위 Python 래퍼 (저수준 ↔ 고수준 분리 패턴)

**API 패턴:**
```python
# Rasterio 스타일
with rasterio.open("image.tif") as src:
    data = src.read([1, 2, 3])  # 밴드 읽기
    meta = src.meta

# PixelQuery에 적용 가능
with pq.open_tile(tile_id) as tile:
    data = tile.read(bands=["red", "nir"], time_range=(...))
```

**참고 포인트:**
- Context manager 패턴 (`with` 문)
- 도메인 용어 (band, window, resampling)

---

### 🧪 테스트/CI/CD 구조

#### 1순위: pandas
**추천 이유:**
- 매우 포괄적인 테스트 스위트
- CI/CD 파이프라인 잘 구성됨

**구조:**
```
pandas/
├── pandas/tests/               # 테스트 코드
│   ├── frame/                 # DataFrame 테스트
│   ├── series/                # Series 테스트
│   └── io/                    # I/O 테스트
├── .github/workflows/          # GitHub Actions
└── pyproject.toml             # pytest 설정
```

**참고 포인트:**
- 모듈별 테스트 디렉토리 분리
- Fixture 활용 (`conftest.py`)
- Parametrized tests (`@pytest.mark.parametrize`)

#### 2순위: xarray
**참고 포인트:**
- Integration tests (NetCDF, Zarr, Dask)
- Hypothesis-based property testing

---

### 📚 문서화 방식

#### 1순위: xarray
**추천 이유:**
- 데이터 과학자를 위한 최고의 문서화
- Jupyter notebook 예제 풍부

**구조:**
```
docs/
├── getting-started/            # 빠른 시작 가이드
├── user-guide/                 # 사용자 가이드
├── examples/                   # Jupyter notebook 예제
├── api-reference/              # API 레퍼런스
└── tutorials/                  # 튜토리얼
```

**참고 포인트:**
- Gallery of examples (실행 가능한 예제)
- Sphinx + nbsphinx (notebook을 문서로 변환)

#### 2순위: GeoPandas
**참고 포인트:**
- 시각화 중심 예제 (맵 플롯)
- Real-world use cases

---

## 최종 추천: Multi-Reference Approach

PixelQuery는 **측면별로 다른 프로젝트를 참고**해야 합니다:

| 측면 | 참고 프로젝트 | 이유 |
|------|--------------|------|
| **Backend 구조** | Apache Iceberg | Iceberg 기반, 명확한 계층 |
| **사용자 API** | xarray ⭐ | 데이터 과학자 표준, spatiotemporal data |
| **메타데이터 API** | pandas/GeoPandas | DataFrame API 친숙도 |
| **도메인 용어** | Rasterio | 위성 이미지 도메인 |
| **내부 구현** | Polars | 성능 최적화, internals 분리 |
| **테스트** | pandas | 포괄적인 테스트 스위트 |
| **문서화** | xarray | 데이터 과학자 친화적 |

### 제안하는 패키지 구조

```
pixelquery/
├── __init__.py                    # Public API (PixelQuery, Dataset 클래스)
│
├── core/                          # [xarray 참고]
│   ├── __init__.py
│   ├── dataset.py                # Dataset class (xarray-like API)
│   ├── dataarray.py              # DataArray class (single variable)
│   └── interfaces.py             # Protocol 정의
│
├── catalog/                       # [Iceberg 참고]
│   ├── __init__.py
│   ├── base.py                   # Catalog protocol
│   └── local.py                  # LocalCatalog implementation
│
├── products/                      # [도메인 특화]
│   ├── __init__.py
│   ├── base.py                   # ProductProfile protocol
│   ├── registry.py               # Product registry
│   └── profiles/
│       ├── __init__.py
│       ├── sentinel2.py
│       ├── landsat8.py
│       └── planet.py
│
├── grid/                          # [도메인 특화]
│   ├── __init__.py
│   └── tile_grid.py              # Geographic tile system
│
├── io/                            # [Rasterio + xarray 참고]
│   ├── __init__.py
│   ├── cog.py                    # COG reading
│   └── backends.py               # I/O backend abstraction
│
├── query/                         # [Polars + xarray 참고]
│   ├── __init__.py
│   ├── executor.py               # Query orchestration
│   ├── spatial.py                # Spatial queries
│   ├── temporal.py               # Temporal queries
│   └── resampling.py             # Multi-resolution resampling
│
├── _internal/                     # [Polars 참고] ⚠️ Private
│   ├── storage/                  # Storage implementation
│   │   ├── arrow_chunk.py       # Arrow IPC backend
│   │   ├── geoparquet.py        # GeoParquet backend
│   │   └── writers.py           # Tile writers
│   └── transactions/             # Transaction implementation
│       └── two_phase.py          # 2PC implementation
│
├── testing/                       # [pandas 참고]
│   ├── __init__.py
│   └── fixtures.py               # Test fixtures (for users)
│
└── util/                          # [xarray 참고]
    ├── __init__.py
    └── geometry.py               # Spatial utilities

tests/                             # [pandas 참고]
├── conftest.py                    # Shared fixtures
├── core/
│   ├── test_dataset.py
│   └── test_dataarray.py
├── io/
│   └── test_cog.py
├── query/
│   └── test_executor.py
└── integration/                   # End-to-end tests
    └── test_workflows.py
```

### 주요 설계 원칙

#### 1. **xarray-inspired API** (데이터 과학자 친화적) ⭐
```python
# 사용자 API 예시
import pixelquery as pq

# Dataset 생성 (xarray-like)
ds = pq.open_dataset("warehouse", tile_id="x0024_y0041")

# Method chaining (xarray-like)
ds.sel(time=slice("2024-01", "2024-12"), bands=["red", "nir"])
ds.resample(time="1M").mean()

# 상호 운용성 (pandas/xarray ecosystem)
df = ds.to_pandas()           # Pandas DataFrame
xr_ds = ds.to_xarray()        # xarray Dataset
gdf = ds.metadata.to_geopandas()  # GeoDataFrame
```

#### 2. **명확한 Public vs Private 분리** (Polars 방식)
- **Public API**: `pixelquery.*` (사용자가 import)
- **Private API**: `pixelquery._internal.*` (내부 구현, **문서화 X**)
- 사용자는 `_internal`을 직접 import하지 않음

#### 3. **Protocol-based 설계** (Iceberg 방식, 현재 유지)
```python
# pixelquery/core/interfaces.py
from typing import Protocol

class CatalogProtocol(Protocol):
    def create_table(self, ...): ...
    def load_table(self, ...): ...

class ProductProfile(Protocol):
    def get_bands(self) -> list[str]: ...
    def get_resolution(self) -> float: ...
```

#### 4. **도메인 용어 명확화** (Rasterio 방식)
- **Tile**: 2.56km × 2.56km 지리적 단위
- **Chunk**: 월별 spatiotemporal chunk (Arrow IPC)
- **Band**: 위성 밴드 (red, nir 등)
- **Resolution**: 공간 해상도 (10m, 30m)

#### 5. **Flat is better than nested**
- Top-level 모듈: 최대 2-3 depth
- `_internal/` 아래만 더 깊은 구조 허용 (사용자에게 안 보임)

#### 6. **테스트 구조** (pandas 방식)
- `tests/` 디렉토리는 `pixelquery/` 구조를 미러링
- `conftest.py`에 공통 fixtures
- `pixelquery/testing/` 모듈로 사용자에게 test utilities 제공

---

## 구체적인 실행 계획

### Phase 1: 패키지 구조 리팩토링 (1-2일)

**목표**: 현재 flat 구조를 multi-reference 기반 구조로 재구성

**작업:**
1. 새로운 디렉토리 구조 생성
2. 기존 `pixelquery/core/interfaces.py` 내용 분산
3. `__init__.py` 파일 생성 (각 모듈)
4. `tests/` 디렉토리 구조 생성

**참고 프로젝트:**
- Apache Iceberg의 디렉토리 구조
- Polars의 `_internal/` 패턴

### Phase 2: API 설계 (3-5일)

**목표**: xarray-inspired public API 설계 및 프로토타입

**참고 프로젝트:**
- **xarray**: `xarray/core/dataset.py`, `xarray/core/dataarray.py`
- **pandas**: `pandas/core/frame.py` (DataFrame API)

### Phase 3: 문서화 구조 (2-3일)

**목표**: 데이터 과학자를 위한 문서 템플릿 구축

**참고 프로젝트:**
- **xarray**: https://docs.xarray.dev
- **GeoPandas**: https://geopandas.org

### Phase 4: 테스트 구조 (1-2일)

**목표**: 포괄적인 테스트 프레임워크 구축

**참고 프로젝트:**
- **pandas**: `pandas/tests/`
- **xarray**: `xarray/tests/`

### Phase 5: pyproject.toml 업데이트

**작업:**
1. 프로젝트 메타데이터 업데이트 ("satlake" → "pixelquery")
2. 의존성 정의
3. Optional dependencies (xarray, dev tools)

---

## Apache Spark와의 비교

**Spark를 참고하지 않는 이유:**
- **규모의 차이**: Spark는 매우 큰 프로젝트 (50+ 모듈)
- **복잡도**: 분산 컴퓨팅 프레임워크로서 PixelQuery보다 훨씬 복잡
- **추상화 레벨**: Spark는 범용 데이터 처리, PixelQuery는 위성 이미지 특화
- **사용자 타겟**: Spark는 데이터 엔지니어, PixelQuery는 데이터 과학자/연구원

---

## 핵심 결론

### 🎯 가장 중요한 권장사항

**xarray를 최우선 참고하세요!**

이유:
1. 데이터 과학자들이 **이미 위성 이미지 처리에 사용**하는 도구
2. Spatiotemporal data의 **사실상 표준 API**
3. pandas/numpy ecosystem과 **완벽한 통합**
4. PixelQuery의 use case (multi-dimensional time-series)와 **정확히 일치**

### 📚 각 측면별 참고 프로젝트

| 측면 | 1순위 | 2순위 | 3순위 |
|------|-------|-------|-------|
| **API 설계** | **xarray** | pandas/GeoPandas | Rasterio |
| **Backend 구조** | **Apache Iceberg** | Polars | - |
| **테스트** | **pandas** | xarray | pytest |
| **문서화** | **xarray** | GeoPandas | - |

### 📊 예상 효과

이 접근법을 따를 경우:
- ✅ 데이터 과학자들의 **빠른 adoption** (익숙한 xarray API)
- ✅ **생산성 향상** (pandas/xarray ecosystem 통합)
- ✅ **유지보수 용이** (명확한 public/private 분리)
- ✅ **오픈소스 성공** (포괄적인 문서화)
