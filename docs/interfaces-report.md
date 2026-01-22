# PixelQuery Interfaces Report

> **생성일**: 2026-01-12
> **상태**: Phase 1-5 완료 후 생성된 모든 Protocol/Interface 정리

---

## 📋 Executive Summary

**총 Protocol 수**: 9개
**카테고리**: 5개 (Core API, Products, Grid, Storage, Transactions)

| 카테고리 | Protocol 수 | 파일 위치 |
|---------|------------|----------|
| **Core API** | 2 | `core/interfaces.py`, `core/result.py` |
| **Products** | 2 | `products/base.py` |
| **Grid** | 1 | `grid/base.py` |
| **Storage** | 1 | `_internal/storage/base.py` |
| **Transactions** | 2 | `_internal/transactions/base.py` |
| **Classes** | 2 | `core/dataset.py`, `core/dataarray.py` |

**상태**:
- ✅ **Protocol 정의 완료**: 9개 모두
- 🔄 **구현 대기**: 9개 (NotImplementedError 상태)
- ✅ **Classes 구현**: Dataset, DataArray (프로토타입 완료)

---

## 1. Core API Protocols

### 1.1 PixelQuery

**파일**: `pixelquery/core/interfaces.py`
**목적**: 메인 API - 위성 이미지 ingestion 및 쿼리

```python
class PixelQuery(Protocol):
    """Main PixelQuery API"""

    def add_image(
        self,
        image_path: str,
        acquisition_date: datetime,
        product_id: str,
        **metadata: Any
    ) -> Dict[str, Any]:
        """
        Ingest satellite image with ACID guarantees

        Process:
        1. Read COG using ProductProfile
        2. Split into geographic tiles
        3. Append to monthly Arrow chunks
        4. Update GeoParquet metadata
        5. Commit Iceberg transaction
        """
        ...

    def query_by_bounds(
        self,
        bounds: Tuple[float, float, float, float],
        date_range: Tuple[datetime, datetime],
        bands: List[str],
        target_resolution: float = 10.0,
        as_of_snapshot_id: Optional[int] = None
    ) -> QueryResult:
        """Query multi-resolution time-series data"""
        ...

    def query_time_series(
        self,
        tile_id: str,
        date_range: Tuple[datetime, datetime],
        bands: List[str],
        target_resolution: float = 10.0
    ) -> QueryResult:
        """Optimized time-series query for a single tile"""
        ...
```

**주요 메서드**:
- `add_image()` - 위성 이미지 추가 (ACID 보장)
- `query_by_bounds()` - Geographic bounds로 쿼리
- `query_time_series()` - 단일 타일 시계열 쿼리 (최적화)

**설계 원칙**:
- ACID transactions (Iceberg)
- Multi-resolution fusion
- Time Travel 지원 (snapshot_id)

**구현 상태**: ⏳ 미구현 (Phase 6+)

---

### 1.2 QueryResult

**파일**: `pixelquery/core/result.py`
**목적**: 쿼리 결과 컨테이너 (다양한 출력 형식 지원)

```python
class QueryResult(Protocol):
    """Query result container"""

    tiles: List[Dict[str, Any]]  # List of {tile_id, date, bands: {...}}

    def to_pandas(self) -> Any:  # pd.DataFrame
        """
        Convert to Pandas DataFrame

        Returns:
            DataFrame with columns: tile_id, acquisition_date,
            product_id, band_red, band_nir, etc.
        """
        ...

    def to_xarray(self) -> Any:  # xr.Dataset
        """
        Convert to Xarray Dataset

        Returns:
            Dataset with dimensions (time, y, x) and
            variables for each band
        """
        ...

    def to_numpy(self) -> Dict[str, NDArray]:
        """Convert to NumPy arrays"""
        ...
```

**주요 메서드**:
- `to_pandas()` - DataFrame 변환
- `to_xarray()` - xarray Dataset 변환
- `to_numpy()` - NumPy arrays 변환

**설계 원칙**:
- Scientific Python ecosystem 통합
- Multiple output formats

**구현 상태**: ⏳ 미구현 (Phase 9)

---

## 2. Products Protocols

### 2.1 BandInfo

**파일**: `pixelquery/products/base.py`
**목적**: 위성 밴드 메타데이터

```python
class BandInfo(Protocol):
    """Satellite band metadata"""

    native_name: str           # e.g., "B04" for Sentinel-2
    standard_name: str         # e.g., "red"
    wavelength: float          # Center wavelength (nm)
    resolution: float          # Native resolution (meters)
    bandwidth: Optional[float] # Spectral bandwidth (nm)
```

**속성**:
- `native_name` - 제품별 밴드 이름 (예: Sentinel-2의 "B04")
- `standard_name` - 표준화된 이름 (예: "red", "nir")
- `wavelength` - 중심 파장 (nm)
- `resolution` - 공간 해상도 (m)
- `bandwidth` - 대역폭 (nm, optional)

**설계 원칙**:
- 제품 간 밴드 매핑 표준화
- Multi-resolution 지원

**구현 상태**: ⏳ 미구현 (Phase 6)

---

### 2.2 ProductProfile

**파일**: `pixelquery/products/base.py`
**목적**: 위성 제품 specification (예: Sentinel-2 L2A, Landsat-8 L2)

```python
class ProductProfile(Protocol):
    """Satellite product specification"""

    product_id: str                    # e.g., "sentinel2_l2a"
    provider: str                      # e.g., "ESA"
    sensor: str                        # e.g., "MSI"
    native_resolution: float           # Primary resolution (m)
    bands: Dict[str, BandInfo]         # Band name → BandInfo
    scale_factor: float                # DN to reflectance
    offset: float                      # Additive offset
    nodata: int                        # No-data value
```

**속성**:
- `product_id` - 고유 식별자 (예: "sentinel2_l2a")
- `provider` - 데이터 제공자 (예: "ESA", "USGS")
- `sensor` - 센서 이름 (예: "MSI", "OLI")
- `native_resolution` - 주요 해상도 (m)
- `bands` - 밴드 사전 (standard name → BandInfo)
- `scale_factor`, `offset` - DN → reflectance 변환
- `nodata` - No-data 픽셀 값

**설계 원칙**:
- Multi-resolution fusion을 위한 제품 간 호환성
- COG 읽기 시 메타데이터 활용

**구현 예정**:
- Sentinel-2 L2A (10m, 20m, 60m bands)
- Landsat-8 L2 (30m, 15m pan)
- Planet SkySat (3m)

**구현 상태**: ⏳ 미구현 (Phase 6)

---

## 3. Grid Protocol

### 3.1 TileGrid

**파일**: `pixelquery/grid/base.py`
**목적**: Geographic tile grid system (고정 그리드, 가변 픽셀 수)

```python
class TileGrid(Protocol):
    """
    Geographic tile grid system for multi-resolution data

    Fixed geographic grid (e.g., 2.56km × 2.56km tiles)
    Variable pixel counts:
    - Sentinel-2 @ 10m: 256×256 pixels
    - Landsat-8 @ 30m: 85×85 pixels
    - Planet @ 3m: 853×853 pixels
    """

    def get_tile_id(self, lon: float, lat: float) -> str:
        """
        Convert WGS84 coordinates to tile ID

        Returns:
            Tile ID in format "xNNNN_yNNNN" (e.g., "x0024_y0041")
        """
        ...

    def get_tile_bounds(self, tile_id: str) -> Tuple[float, float, float, float]:
        """
        Get geographic bounds of a tile

        Returns:
            Bounding box as (minx, miny, maxx, maxy) in WGS84
        """
        ...

    def get_pixels_for_resolution(self, resolution_m: float) -> int:
        """
        Calculate pixel count per tile for a given resolution

        Examples:
            10m resolution → 256 pixels (2560m / 10m)
            30m resolution → 85 pixels (2560m / 30m)
        """
        ...
```

**주요 메서드**:
- `get_tile_id()` - 좌표 → 타일 ID 변환
- `get_tile_bounds()` - 타일 ID → bounds 변환
- `get_pixels_for_resolution()` - 해상도별 픽셀 수 계산

**설계 원칙**:
- **고정 지리적 타일** (예: 2.56km × 2.56km)
- **가변 픽셀 수** (해상도에 따라)
- Multi-resolution 데이터를 동일 타일에 저장

**예시**:
```
타일 "x0024_y0041" (2.56km × 2.56km):
- Sentinel-2 @ 10m:  256 × 256 = 65,536 pixels
- Landsat-8 @ 30m:   85 × 85 = 7,225 pixels
- Planet @ 3m:       853 × 853 = 727,609 pixels
```

**구현 상태**: ⏳ 미구현 (Phase 6)

---

## 4. Storage Protocol (Private)

### 4.1 StorageBackend

**파일**: `pixelquery/_internal/storage/base.py`
**카테고리**: ⚠️ Private API
**목적**: 추상 스토리지 인터페이스 (local, S3, Azure, GCS 지원)

```python
class StorageBackend(Protocol):
    """Abstract storage interface"""

    def read_bytes(self, path: str) -> bytes:
        """Read file contents as bytes"""
        ...

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write bytes to file"""
        ...

    def atomic_rename(self, src: str, dest: str) -> None:
        """
        Atomically rename file (critical for transactions)

        Used for two-phase commit: write to .tmp, then rename
        """
        ...

    def delete(self, path: str) -> None:
        """Delete file"""
        ...

    def exists(self, path: str) -> bool:
        """Check if file exists"""
        ...

    def list_files(self, prefix: str) -> List[str]:
        """List files matching prefix"""
        ...
```

**주요 메서드**:
- `read_bytes()`, `write_bytes()` - 파일 I/O
- `atomic_rename()` - 원자적 rename (트랜잭션용)
- `delete()`, `exists()`, `list_files()` - 파일 관리

**설계 원칙**:
- Multiple backends 지원 (local, S3, Azure, GCS)
- Atomic operations for transactions

**구현 예정**:
- `LocalStorageBackend` (filesystem)
- `S3StorageBackend` (AWS S3)
- `AzureStorageBackend` (Azure Blob)

**구현 상태**: ⏳ 미구현 (Phase 7)

---

## 5. Transactions Protocols (Private)

### 5.1 Transaction

**파일**: `pixelquery/_internal/transactions/base.py`
**카테고리**: ⚠️ Private API
**목적**: ACID transaction for Arrow + GeoParquet + Iceberg

```python
class Transaction(Protocol):
    """
    ACID transaction for coordinating writes

    Two-Phase Commit Protocol:
    1. PREPARE: Write all data to temporary paths (.tmp files)
    2. COMMIT: Iceberg optimistic concurrency commit
    3. FINALIZE: Atomically rename .tmp → final (if Iceberg succeeds)
    4. ROLLBACK: Delete .tmp files (if Iceberg fails)
    """

    def stage_arrow_chunk(
        self,
        tile_id: str,
        year_month: str,
        data: Dict[str, Any]
    ) -> str:
        """
        Stage Arrow chunk to temporary path

        Returns:
            Temporary file path
        """
        ...

    def stage_geoparquet_metadata(
        self,
        records: List[Dict[str, Any]]
    ) -> str:
        """
        Stage GeoParquet metadata to temporary path

        Returns:
            Temporary file path
        """
        ...

    def commit(self) -> Dict[str, Any]:
        """
        Commit transaction (Iceberg + finalize files)

        Returns:
            Commit result with snapshot_id and file paths

        Raises:
            TransactionError: If commit fails (triggers rollback)
        """
        ...

    def rollback(self) -> None:
        """Rollback transaction (delete temporary files)"""
        ...
```

**주요 메서드**:
- `stage_arrow_chunk()` - Arrow chunk를 임시 경로에 작성
- `stage_geoparquet_metadata()` - GeoParquet 메타데이터 staging
- `commit()` - 트랜잭션 커밋 (Iceberg + file rename)
- `rollback()` - 롤백 (임시 파일 삭제)

**설계 원칙**:
- **Two-Phase Commit** (2PC)
- Iceberg 메타데이터와 픽셀 데이터의 일관성 보장
- Optimistic concurrency control

**2PC 흐름**:
```
1. PREPARE:
   data.arrow.tmp
   metadata.parquet.tmp

2. COMMIT (Iceberg):
   ✅ Success → proceed to FINALIZE
   ❌ Failure → ROLLBACK

3. FINALIZE:
   data.arrow.tmp → data.arrow
   metadata.parquet.tmp → metadata.parquet

4. ROLLBACK (on failure):
   rm data.arrow.tmp
   rm metadata.parquet.tmp
```

**구현 상태**: ⏳ 미구현 (Phase 7)

---

### 5.2 TransactionManager

**파일**: `pixelquery/_internal/transactions/base.py`
**카테고리**: ⚠️ Private API
**목적**: Transaction factory

```python
class TransactionManager(Protocol):
    """Transaction factory"""

    def begin(self) -> Transaction:
        """Start a new transaction"""
        ...
```

**주요 메서드**:
- `begin()` - 새 트랜잭션 시작

**설계 원칙**:
- Factory pattern for transaction creation
- Context manager 지원 예정 (`with txn_manager.begin() as txn:`)

**구현 상태**: ⏳ 미구현 (Phase 7)

---

## 6. Classes (Implemented)

### 6.1 Dataset

**파일**: `pixelquery/core/dataset.py`
**카테고리**: Public API
**목적**: xarray.Dataset-like API for multi-band imagery

```python
class Dataset:
    """Multi-band satellite imagery dataset (xarray.Dataset-like)"""

    # Attributes
    tile_id: str
    time_range: Optional[Tuple[datetime, datetime]]
    bands: List[str]
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    dims: Dict[str, int]
    coords: Dict[str, NDArray]

    # Methods (현재 프로토타입)
    def sel(time, bands, **kwargs) -> Dataset
    def isel(**indexers) -> Dataset
    def resample(time: str) -> DatasetResampler
    def mean(dim) -> Dataset | NDArray

    # Conversion
    def to_xarray() -> xr.Dataset
    def to_pandas() -> pd.DataFrame
    def to_numpy() -> Dict[str, NDArray]

    # Indexing
    def __getitem__(key: str) -> DataArray
    def __repr__() -> str
```

**구현 상태**:
- ✅ 기본 구조 완료
- ⏳ sel(), resample(), to_xarray() 등은 NotImplementedError (Phase 8-9)

**테스트**: 11 tests ✅

---

### 6.2 DataArray

**파일**: `pixelquery/core/dataarray.py`
**카테고리**: Public API
**목적**: xarray.DataArray-like API for single-band data

```python
class DataArray:
    """Single-band satellite imagery (xarray.DataArray-like)"""

    # Attributes
    name: str
    data: NDArray
    dims: Dict[str, int]
    coords: Dict[str, NDArray]
    attrs: Dict[str, Any]

    # Properties
    values: NDArray
    shape: tuple
    size: int
    dtype: np.dtype

    # Selection (프로토타입)
    def sel(**indexers) -> DataArray
    def isel(**indexers) -> DataArray

    # Aggregation (프로토타입)
    def mean(dim) -> DataArray | float
    def max(dim) -> DataArray | float
    def min(dim) -> DataArray | float
    def median(dim) -> DataArray | float
    def std(dim) -> DataArray | float

    # Conversion
    def to_numpy() -> NDArray
    def to_pandas() -> pd.Series | pd.DataFrame

    # Arithmetic (구현 완료) ✅
    def __add__, __sub__, __mul__, __truediv__
    def __radd__, __rsub__, __rmul__, __rtruediv__
```

**구현 상태**:
- ✅ Arithmetic operations 완료
- ✅ Properties 완료
- ⏳ sel(), aggregations은 NotImplementedError (Phase 8)

**테스트**: 13 tests ✅

---

## 7. Interface 의존성 그래프

```
┌─────────────────────────────────────┐
│ Public API (User-Facing)            │
├─────────────────────────────────────┤
│ PixelQuery (main API)               │
│   ↓ uses                            │
│ QueryResult                         │
│ Dataset (implemented)               │
│ DataArray (implemented)             │
└─────────────────────────────────────┘
         ↓ depends on
┌─────────────────────────────────────┐
│ Domain Protocols                    │
├─────────────────────────────────────┤
│ ProductProfile                      │
│   ↓ has                             │
│ BandInfo                            │
│                                     │
│ TileGrid                            │
└─────────────────────────────────────┘
         ↓ depends on
┌─────────────────────────────────────┐
│ Internal Protocols (Private)        │
├─────────────────────────────────────┤
│ StorageBackend                      │
│                                     │
│ TransactionManager                  │
│   ↓ creates                         │
│ Transaction                         │
└─────────────────────────────────────┘
```

---

## 8. 구현 우선순위

### Phase 6: 기본 구현
1. ✅ **TileGrid** - 좌표 ↔ 타일 ID 변환
2. ✅ **ProductProfile** implementations
   - Sentinel2L2AProfile
   - Landsat8L2Profile
3. ✅ **BandInfo** - 밴드 메타데이터

### Phase 7: 스토리지 계층
1. ✅ **StorageBackend** implementations
   - LocalStorageBackend
   - (Optional) S3StorageBackend
2. ✅ **TransactionManager** & **Transaction**
   - 2-Phase Commit 구현
3. ✅ Arrow IPC writer/reader
4. ✅ GeoParquet writer

### Phase 8: 쿼리 엔진
1. ✅ **Dataset.sel()** 구현
2. ✅ **Dataset.resample()** 구현
3. ✅ **DataArray** aggregations (mean, max, etc.)
4. ✅ Multi-resolution resampling

### Phase 9: 상호운용성
1. ✅ **QueryResult** 구현
2. ✅ **Dataset.to_xarray()** 구현
3. ✅ **Dataset.to_pandas()** 구현
4. ✅ **DataArray.to_pandas()** 구현

### Phase 10: PixelQuery 메인 API
1. ✅ **PixelQuery.add_image()** 구현
2. ✅ **PixelQuery.query_by_bounds()** 구현
3. ✅ **PixelQuery.query_time_series()** 구현

---

## 9. 참고 프로젝트 매핑

| Protocol | 참고 프로젝트 | 이유 |
|----------|--------------|------|
| **Dataset, DataArray** | xarray | 데이터 과학자 친화적 API |
| **PixelQuery, QueryResult** | xarray | open_dataset 패턴 |
| **ProductProfile, BandInfo** | Rasterio | 도메인 특화 (GIS) |
| **TileGrid** | - | PixelQuery 고유 (multi-resolution) |
| **StorageBackend** | PyIceberg | 추상 스토리지 패턴 |
| **Transaction** | DBMS 2PC | 분산 트랜잭션 표준 |

---

## 10. 요약

### Protocol 현황
- **정의 완료**: 9개 ✅
- **구현 완료**: 2개 (Dataset, DataArray - 프로토타입)
- **구현 대기**: 7개

### 주요 특징
1. ✅ **Protocol-based 설계** - 타입 안정성
2. ✅ **Public/Private 분리** - `_internal/` 디렉토리
3. ✅ **xarray-inspired** - 데이터 과학자 친화적
4. ✅ **Multi-resolution native** - 고유 차별점
5. ✅ **ACID transactions** - 데이터 일관성

### 다음 단계
Phase 6부터 실제 구현 시작:
1. TileGrid 구현
2. ProductProfile 구현 (Sentinel-2, Landsat-8)
3. StorageBackend 구현
4. Transaction 구현
5. Dataset/DataArray 메서드 구현
6. PixelQuery 메인 API 구현

---

**문서 생성일**: 2026-01-12
**마지막 업데이트**: 2026-01-12
**상태**: Phase 1-5 완료 후 생성
