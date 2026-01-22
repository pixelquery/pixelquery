# PixelQuery 구현 계획: 상용화 관점의 냉정한 평가와 실행 전략

## Executive Summary

**결론**: 진행 가능하나, **중대한 위험 요소**들을 인지하고 단계별 검증이 필수입니다.

**핵심 발견**:
- 디자인 문서의 기본 가정("Iceberg for Raster")에 **기술적 오류** 존재
- 실제 구현 난이도는 문서 추정(8주)의 **2-2.5배** (14-19주)
- 시장은 이미 COG+STAC으로 표준화되어 있어 **전환 비용이 매우 높음**
- 하지만 **멀티-레졸루션 통합 쿼리**는 독특한 가치 제안

**상용화 리스크**: 🔴 HIGH
- 기술 리스크: MEDIUM-HIGH (트랜잭션, 성능)
- 시장 리스크: HIGH (전환 비용, 기존 솔루션)
- PMF 불확실성: VERY HIGH

**권장 전략**: "Lean Startup" 방식
1. **Phase 0-1**: 인터페이스 + PoC (4주) → **첫 검증 포인트**
2. **Phase 2**: MVP (6주) → **시장 검증 (파일럿 고객 확보)**
3. **Phase 3-4**: 전체 구현 (9주) → 파일럿 성공 시에만 진행

---

## 1. 냉정한 평가: 상용화 관점

### 1.1 기술적 결함 분석

#### ❌ **Critical Flaw**: "Iceberg for Raster"는 기술적으로 부정확

**발견된 문제**:
```
디자인 문서 주장: "Apache Iceberg for Satellite Imagery"
실제:
- Iceberg는 래스터 데이터를 네이티브로 지원하지 않음 (메타데이터만)
- 픽셀 데이터는 Arrow 파일에 별도 저장 → Iceberg ACID 보장 범위 밖
- 진짜 래스터용 Iceberg는 "Havasu" (경쟁자)가 이미 존재
```

**실제 아키텍처**:
```
현재 디자인:
  Iceberg Table (metadata only, ACID ✓)
       ↓ references
  Arrow Files (pixel data, ACID ✗)  ← 여기가 문제!
```

**픽셀 데이터는 ACID 보장이 안 됨** → Two-phase commit 직접 구현 필요

#### 🟡 **성능 우려**

**목표 vs 현실**:
| 목표 (디자인 문서) | MVP 예상치 | 프로덕션 목표 |
|-------------------|-----------|--------------|
| 10 타일 쿼리 < 100ms | 300-500ms | 150ms |
| 이미지 추가 < 10s | 15-20s | 10s |
| 리샘플링 오버헤드 | 미측정 | 병목 가능 |

초기 성능은 느릴 것 → 최적화 반복 필수

### 1.2 시장 분석: 레드오션

#### 경쟁 구도

**직접 경쟁자**:
1. **COG + STAC + DuckDB** (무료, 오픈소스)
   - 현재 업계 표준
   - Planet, NASA, ESA 모두 사용
   - "충분히 좋음" (Good Enough)

2. **Havasu** (Iceberg 기반 래스터 포맷)
   - Wherobots 지원
   - 이미 동일 문제 해결 중
   - 더 많은 리소스

3. **Google Earth Engine** (상용)
   - 압도적 시장 점유율
   - 연구자들 사이 표준

**PixelQuery 차별점**:
- ✅ **멀티-레졸루션 네이티브** (독특함!)
- ✅ Python-first, 오픈소스
- ✅ ACID 메타데이터 (Iceberg)
- ❌ 픽셀 ACID는 복잡/불확실

**시장 포지셔닝**:
```
"Apache Iceberg for Raster" (X)
          ↓
"Multi-Resolution Satellite Data Lake" (O)
```

#### 타겟 고객 현실 체크

| 고객군 | 가능성 | 이유 |
|--------|--------|------|
| **Planet/Airbus** | 🔴 Very Low | 이미 COG+STAC 인프라 투자, 전환 비용 높음 |
| **국방/정보기관** | 🟡 Low-Medium | Time Travel 매력적이나 검증된 기술만 사용 (1-2년 후) |
| **AgriTech 스타트업** | 🟢 Medium-High | 시계열 쿼리 필요, 예산 민감, 새 도구 시도 의향 ⭐ |
| **재난 모니터링** | 🟢 Medium | 빠른 멀티소스 통합 필요 |
| **연구기관** | 🟢 Medium | 오픈소스 선호, 새로운 접근법 시도 |

**1년차 현실적 목표**:
- 파일럿 고객: 3-5개 (AgriTech, 연구소)
- 유료 전환: 1-2개
- 수익: $10K-30K (연간)
- **창업 목표라면 매우 부족** → Side Project나 기술 검증 단계로 적합

### 1.3 리소스 요구사항

#### 타임라인 (1인 개발자)

| Phase | 기간 | 누적 | 위험도 |
|-------|------|------|--------|
| Phase 0: 인터페이스 + 트랜잭션 PoC | 2주 | 2주 | 🔴 HIGH |
| Phase 1: Foundation | 3주 | 5주 | 🟡 MEDIUM |
| Phase 2: Storage & Ingestion | 6주 | 11주 | 🔴 HIGH |
| Phase 3: Query Engine | 5주 | 16주 | 🟡 MEDIUM |
| Phase 4: Testing & Optimization | 3주 | 19주 | 🟢 LOW |

**총 19주 ≈ 4.5개월** (풀타임 기준)

#### 필요 스킬셋 (현실 체크)

- [ ] Geospatial (CRS, 투영법, 리샘플링) - **필수**
- [ ] 분산 시스템 (ACID, Iceberg, 동시성) - **필수**
- [ ] Python 성능 최적화 (NumPy, Cython) - **중요**
- [ ] Data Engineering (Parquet, Arrow) - **필수**
- [ ] DevOps (S3, AWS Glue, 배포) - **중요**

**부족한 스킬이 있다면 학습 시간 +2-4주**

#### 비용 (1년차)

```
개발 인프라:
- S3 버킷 ($50/월 × 12) = $600
- AWS Glue 테스트 ($100/월 × 6) = $600
- 위성 데이터 다운로드 = $200-500 (일회성)

운영 (파일럿 고객):
- S3 스토리지 (10TB) = $230/월 × 12 = $2,760
- 데이터 전송 = $500-1,000/년

Total Year 1: $4,650 - $5,500

수익 예상: $10K-30K
손익: +$5K - $25K (낙관적 시나리오)
```

### 1.4 킬러 리스크

#### 🚨 프로젝트를 죽일 수 있는 요인들

**1. 트랜잭션 원자성 문제 해결 실패** (확률: 40%)
- Two-phase commit이 너무 복잡하거나 느림
- → **대응**: Phase 0에서 PoC 검증, 실패 시 "메타데이터만 ACID" 로 피벗

**2. 성능이 목표치에 못 미침** (확률: 60%)
- 리샘플링 병목, S3 레이턴시
- → **대응**: Rust extension, 캐싱, 배치 처리

**3. "Good Enough" 문제** (확률: 70%)
- COG+STAC이 대부분 사용 사례 커버
- 고객이 전환할 이유 부족
- → **대응**: 킬러 기능 개발 (실시간 시계열 분석 대시보드?)

**4. Havasu가 대세가 됨** (확률: 50%)
- Wherobots의 리소스가 압도
- → **대응**: "Python 생태계를 위한 경량 대안" 포지셔닝

**5. 파일럿 고객 확보 실패** (확률: 50%)
- 실제 페인포인트가 아님
- → **대응**: Phase 2 완료 후 즉시 고객 인터뷰, 없으면 중단

---

## 2. 인터페이스 우선 구현 전략

### 2.1 핵심 원칙

**"Contract-First Development"**:
1. 모든 구현 전에 인터페이스(Protocol) 정의
2. 인터페이스로 먼저 통합 테스트 작성
3. Mock 구현으로 사용성 검증
4. 실제 구현은 마지막

**이점**:
- API 안정성 확보
- 병렬 개발 가능 (여러 팀/기여자)
- 조기 피드백
- 리팩토링 용이

### 2.2 Phase 0: 인터페이스 정의 + 위험 검증 (2주) 🔴 CRITICAL

#### 목표

1. **모든 핵심 인터페이스 정의** (Protocol 기반)
2. **트랜잭션 PoC** - ACID 가능성 검증
3. **통합 테스트 스켈레톤** - Mock으로 E2E 플로우

#### 핵심 파일

##### 1. `/pixelquery/core/interfaces.py`

모든 추상 인터페이스 정의:

```python
from typing import Protocol, List, Tuple, Optional, Dict
from datetime import datetime
import numpy as np

# === Product 관련 ===

class BandInfo(Protocol):
    """밴드 메타데이터"""
    native_name: str       # "B04"
    standard_name: str     # "red"
    wavelength: float      # 665 (nm)
    resolution: float      # 10.0 (m)

class ProductProfile(Protocol):
    """위성 제품 프로필"""
    product_id: str
    provider: str
    sensor: str
    native_resolution: float
    bands: Dict[str, BandInfo]
    scale_factor: float
    nodata: int

# === Tile Grid ===

class TileGrid(Protocol):
    """지리적 타일 그리드"""
    def get_tile_id(self, lon: float, lat: float) -> str:
        """좌표 → tile_id (e.g., 'x0024_y0041')"""
        ...

    def get_tile_bounds(self, tile_id: str) -> Tuple[float, float, float, float]:
        """tile_id → (minx, miny, maxx, maxy)"""
        ...

    def get_pixels_for_resolution(self, resolution_m: float) -> int:
        """해상도 → 타일당 픽셀 수"""
        ...

# === Storage ===

class StorageBackend(Protocol):
    """스토리지 추상화"""
    def read_bytes(self, path: str) -> bytes: ...
    def write_bytes(self, path: str, data: bytes) -> None: ...
    def atomic_rename(self, src: str, dest: str) -> None: ...
    def delete(self, path: str) -> None: ...
    def exists(self, path: str) -> bool: ...

# === Transaction ===

class Transaction(Protocol):
    """ACID 트랜잭션"""
    def stage_arrow_chunk(self, tile_id: str, year_month: str, data: dict) -> str:
        """Arrow 청크를 임시 경로에 저장"""
        ...

    def stage_geoparquet_metadata(self, records: List[dict]) -> str:
        """GeoParquet 메타데이터를 임시 경로에 저장"""
        ...

    def commit(self) -> dict:
        """Iceberg 커밋 + 임시 파일 최종화 (atomic)
        Returns: {"snapshot_id": int, "files_written": List[str]}
        """
        ...

    def rollback(self) -> None:
        """실패 시 임시 파일 삭제"""
        ...

class TransactionManager(Protocol):
    """트랜잭션 관리자"""
    def begin(self) -> Transaction: ...

# === Query ===

class QueryResult(Protocol):
    """쿼리 결과"""
    tiles: List[dict]  # [{tile_id, date, bands: {red: ndarray, ...}}]

    def to_pandas(self) -> "pd.DataFrame": ...
    def to_xarray(self) -> "xr.Dataset": ...
    def to_numpy(self) -> Dict[str, np.ndarray]: ...

# === Main API ===

class PixelQuery(Protocol):
    """메인 API"""
    def add_image(
        self,
        image_path: str,
        acquisition_date: datetime,
        product_id: str,
        **metadata
    ) -> dict:
        """이미지 추가 (ACID)"""
        ...

    def query_by_bounds(
        self,
        bounds: Tuple[float, float, float, float],
        date_range: Tuple[datetime, datetime],
        bands: List[str],
        target_resolution: float = 10.0,
        as_of_snapshot_id: Optional[int] = None
    ) -> QueryResult:
        """공간+시간 쿼리"""
        ...
```

##### 2. `/pixelquery/transactions/two_phase_commit.py`

**🔴 MOST CRITICAL - 트랜잭션 PoC**

```python
class TwoPhaseCommitTransaction:
    """
    Two-Phase Commit 프로토타입

    목표: Arrow + GeoParquet + Iceberg를 원자적으로 커밋 가능한가?

    전략:
    1. PREPARE: 모든 데이터를 .tmp 파일로 저장
    2. COMMIT: Iceberg 낙관적 동시성 커밋 시도
    3. FINALIZE: Iceberg 성공 시 .tmp → 최종 경로 (atomic rename)
    4. ROLLBACK: 실패 시 .tmp 파일 삭제
    """

    def __init__(self, storage: StorageBackend, iceberg_table):
        self._storage = storage
        self._iceberg_table = iceberg_table
        self._staged_files: List[Tuple[str, str]] = []  # (temp, final)
        self._committed = False

    def stage_arrow_chunk(self, tile_id: str, year_month: str, data: dict) -> str:
        """1단계: Arrow → .tmp"""
        temp_path = f"tiles/{tile_id}/{year_month}.arrow.tmp"
        final_path = f"tiles/{tile_id}/{year_month}.arrow"

        # Arrow 테이블 생성
        import pyarrow as pa
        table = pa.table(data)

        # 임시 파일에 쓰기
        with pa.ipc.new_file(temp_path, table.schema) as writer:
            writer.write_table(table)

        self._staged_files.append((temp_path, final_path))
        return temp_path

    def commit(self) -> dict:
        """2-3단계: Iceberg 커밋 + 파일 최종화"""
        try:
            # 2. Iceberg 낙관적 동시성 커밋
            snapshot = self._iceberg_table.append(...)  # GeoParquet 레코드

            # 3. 성공 시 모든 .tmp → 최종 (atomic!)
            for temp, final in self._staged_files:
                self._storage.atomic_rename(temp, final)

            self._committed = True
            return {"snapshot_id": snapshot.snapshot_id, "files": len(self._staged_files)}

        except Exception as e:
            # 4. 실패 시 롤백
            self.rollback()
            raise TransactionError(f"Commit failed: {e}")

    def rollback(self):
        """임시 파일 삭제"""
        for temp, _ in self._staged_files:
            if self._storage.exists(temp):
                self._storage.delete(temp)
```

**⚠️ Week 2 검증 포인트**:
```python
# 테스트 케이스:
# 1. 정상 커밋
# 2. Iceberg 충돌 시 롤백
# 3. 파일 시스템 오류 시 롤백
# 4. 성능 측정 (오버헤드 < 100ms?)

# 실패 시: ACID 포기하고 "best-effort consistency"로 피벗
```

##### 3. `/pixelquery/core/pixelquery.py`

메인 API 스켈레톤:

```python
class PixelQueryImpl:
    """PixelQuery 구현 (초기엔 Mock)"""

    def __init__(
        self,
        warehouse: str,
        catalog_type: str = "sql",
        storage_backend: Optional[StorageBackend] = None
    ):
        self.warehouse = warehouse
        self._catalog = None  # Lazy init
        self._storage = storage_backend or LocalStorage(warehouse)
        self._tx_manager = None  # Phase 0에서 초기화

    def add_image(
        self,
        image_path: str,
        acquisition_date: datetime,
        product_id: str,
        **metadata
    ) -> dict:
        """Phase 2에서 구현"""
        raise NotImplementedError("Phase 2")

    def query_by_bounds(
        self,
        bounds: Tuple[float, float, float, float],
        date_range: Tuple[datetime, datetime],
        bands: List[str],
        target_resolution: float = 10.0,
        as_of_snapshot_id: Optional[int] = None
    ) -> QueryResult:
        """Phase 3에서 구현"""
        raise NotImplementedError("Phase 3")
```

#### Week 2 마일스톤

```python
# 이게 작동하면 Phase 0 성공:

from pixelquery import PixelQuery
from pixelquery.transactions import TwoPhaseCommitTransaction

pq = PixelQuery("./test_warehouse")

# 1. 트랜잭션 PoC 테스트
tx = pq._tx_manager.begin()
tx.stage_arrow_chunk("x0000_y0000", "2024-01", dummy_data)
result = tx.commit()
print(f"✓ Transaction committed: {result}")

# 2. 충돌 테스트 (동시 쓰기)
# ... 병렬 트랜잭션 실행 ...
# 하나는 성공, 하나는 재시도 확인

# 3. 성능 측정
# 트랜잭션 오버헤드 < 100ms?
```

**GO/NO-GO 결정**:
- ✅ GO: 트랜잭션 작동, 오버헤드 허용 가능 → Phase 1 진행
- ❌ PIVOT: 트랜잭션 너무 복잡/느림 → "메타데이터만 ACID" 로 변경

---

### 2.3 Phase 1: Foundation (3주)

#### 목표

Iceberg + ProductProfile + TileGrid 구현 (픽셀 데이터 없음)

#### 구현 파일

##### 1. `/pixelquery/grid/tile_grid.py`

```python
class TileGridImpl:
    """지리적 타일 그리드 구현"""

    def __init__(
        self,
        origin: Tuple[float, float] = (124.0, 33.0),  # 한국 기준
        tile_size_meters: float = 2560.0
    ):
        self.origin = origin
        self.tile_size_meters = tile_size_meters

    def get_tile_id(self, lon: float, lat: float) -> str:
        """WGS84 좌표 → tile_id"""
        # 1도 ≈ 111.32km
        meters_per_degree = 111320.0

        tile_x = int((lon - self.origin[0]) * meters_per_degree / self.tile_size_meters)
        tile_y = int((lat - self.origin[1]) * meters_per_degree / self.tile_size_meters)

        return f"x{tile_x:04d}_y{tile_y:04d}"

    def get_tile_bounds(self, tile_id: str) -> Tuple[float, float, float, float]:
        """tile_id → WGS84 bbox"""
        tile_x, tile_y = self._parse_tile_id(tile_id)

        meters_per_degree = 111320.0
        minx = self.origin[0] + (tile_x * self.tile_size_meters / meters_per_degree)
        miny = self.origin[1] + (tile_y * self.tile_size_meters / meters_per_degree)
        maxx = minx + (self.tile_size_meters / meters_per_degree)
        maxy = miny + (self.tile_size_meters / meters_per_degree)

        return (minx, miny, maxx, maxy)

    def get_pixels_for_resolution(self, resolution_m: float) -> int:
        """해상도 → 타일당 픽셀 수

        예:
        - 10m: 2560m / 10m = 256 픽셀
        - 30m: 2560m / 30m = 85 픽셀
        - 3m: 2560m / 3m = 853 픽셀
        """
        return int(self.tile_size_meters / resolution_m)

    @staticmethod
    def _parse_tile_id(tile_id: str) -> Tuple[int, int]:
        """'x0024_y0041' → (24, 41)"""
        parts = tile_id.split('_')
        tile_x = int(parts[0][1:])
        tile_y = int(parts[1][1:])
        return tile_x, tile_y
```

**테스트**:
```python
grid = TileGridImpl()

# 서울 (37.5665, 126.9780)
tile_id = grid.get_tile_id(126.9780, 37.5665)
# Expected: x0025_y0041 정도

bounds = grid.get_tile_bounds(tile_id)
# (126.95xx, 37.54xx, 127.02xx, 37.60xx)

pixels_10m = grid.get_pixels_for_resolution(10.0)  # 256
pixels_30m = grid.get_pixels_for_resolution(30.0)  # 85
```

##### 2. `/pixelquery/products/base.py`

```python
from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class BandInfo:
    native_name: str       # "B04" (Sentinel-2)
    standard_name: str     # "red"
    wavelength: float      # 665 nm
    resolution: float      # 10.0 m
    bandwidth: Optional[float] = None

@dataclass
class ProductProfile:
    product_id: str            # "sentinel2_l2a"
    provider: str              # "ESA"
    sensor: str                # "MSI"
    product_level: str         # "L2A"
    native_resolution: float   # 10.0

    bands: Dict[str, BandInfo]
    scale_factor: float = 1.0
    offset: float = 0.0
    nodata: int = 0

    cloud_band: Optional[str] = None
    native_crs: str = "EPSG:4326"
```

##### 3. `/pixelquery/products/profiles/sentinel2.py`

```python
from pixelquery.products.base import ProductProfile, BandInfo

SENTINEL2_L2A = ProductProfile(
    product_id="sentinel2_l2a",
    provider="ESA",
    sensor="MSI",
    product_level="L2A",
    native_resolution=10.0,

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
```

##### 4. `/pixelquery/iceberg/catalog.py`

```python
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import *

class PixelQueryCatalog:
    """Iceberg 카탈로그 래퍼"""

    def __init__(self, warehouse: str, catalog_type: str = "sql"):
        if catalog_type == "sql":
            # SQLite (로컬 개발용만!)
            self._catalog = SqlCatalog(
                "pixelquery",
                **{
                    "uri": f"sqlite:///{warehouse}/catalog.db",
                    "warehouse": f"file://{warehouse}"
                }
            )
        else:
            raise NotImplementedError(f"Catalog type {catalog_type} not yet supported")

    def create_tile_catalog_table(self) -> "Table":
        """타일 카탈로그 테이블 생성"""
        schema = Schema(
            NestedField(1, "tile_id", StringType(), required=True),
            NestedField(2, "tile_x", IntegerType(), required=True),
            NestedField(3, "tile_y", IntegerType(), required=True),
            NestedField(4, "geometry", BinaryType(), required=True),  # WKB
            NestedField(5, "acquisition_date", TimestampType(), required=True),
            NestedField(6, "year_month", StringType(), required=True),
            NestedField(7, "chunk_file_path", StringType(), required=True),
            NestedField(8, "product_id", StringType()),
            NestedField(9, "native_resolution", FloatType()),
            NestedField(10, "band_red_mean", FloatType()),
            NestedField(11, "band_nir_mean", FloatType()),
            # ... 나머지 필드
        )

        partition_spec = PartitionSpec(
            PartitionField(
                source_id=6,  # year_month
                field_id=1000,
                transform=IdentityTransform(),
                name="month_partition"
            )
        )

        return self._catalog.create_table(
            "default.tile_catalog",
            schema=schema,
            partition_spec=partition_spec
        )

    def get_table(self, name: str = "default.tile_catalog"):
        return self._catalog.load_table(name)
```

#### Week 5 마일스톤

```python
from pixelquery import PixelQuery
from pixelquery.products import SENTINEL2_L2A

pq = PixelQuery("./warehouse")

# 1. 카탈로그 초기화
pq.init_catalog()
# → warehouse/catalog.db 생성
# → warehouse/tile_catalog/metadata/ 생성

# 2. 제품 프로필 사용
profile = SENTINEL2_L2A
print(profile.bands["red"].wavelength)  # 665

# 3. 타일 그리드
from pixelquery.grid import TileGridImpl
grid = TileGridImpl()
tile_id = grid.get_tile_id(127.0, 37.5)
print(tile_id)  # x0027_y0040
```

---

### 2.4 Phase 2: Storage & Ingestion (6주) 🔴 COMPLEX

#### 목표

COG → 타일 → Arrow 청크 → GeoParquet 메타데이터 → Iceberg 커밋

#### 핵심 파일

##### 1. `/pixelquery/io/cog_reader.py`

```python
import rasterio
from rasterio.windows import from_bounds
import numpy as np

class COGReader:
    """Cloud Optimized GeoTIFF 리더"""

    def __init__(self, product_profile: ProductProfile):
        self.profile = product_profile

    def read_tile_window(
        self,
        image_path: str,
        tile_bounds: Tuple[float, float, float, float]
    ) -> dict:
        """
        타일 영역의 픽셀 데이터 읽기

        Returns:
            {
                "bands": {"red": ndarray(H, W), "nir": ...},
                "shape": (H, W),
                "resolution": float,
                "transform": Affine,
                "nodata_mask": ndarray(bool)
            }
        """
        with rasterio.open(image_path) as src:
            # CRS 변환 (WGS84 → 이미지 CRS)
            minx, miny, maxx, maxy = tile_bounds
            # ... rasterio.warp.transform_bounds ...

            # Window 읽기
            window = from_bounds(*tile_bounds, src.transform)

            # 밴드 읽기
            bands_data = {}
            for std_name, band_info in self.profile.bands.items():
                # 네이티브 밴드명 → 인덱스 매핑
                band_idx = self._get_band_index(src, band_info.native_name)

                arr = src.read(band_idx, window=window)

                # Scale + offset 적용
                arr = arr.astype(np.float32)
                arr = arr * self.profile.scale_factor + self.profile.offset

                # NoData 마스킹
                nodata = src.nodata or self.profile.nodata
                arr[arr == nodata] = np.nan

                bands_data[std_name] = arr

            return {
                "bands": bands_data,
                "shape": arr.shape,
                "resolution": self.profile.native_resolution,
                "nodata_mask": np.isnan(arr)
            }
```

##### 2. `/pixelquery/storage/arrow_chunk.py`

**🔴 MOST COMPLEX - 가변 크기 배열 관리**

```python
import pyarrow as pa
import bisect

class ArrowChunkManager:
    """월별 Arrow 청크 관리 (멀티-레졸루션)"""

    def append_observation(
        self,
        tile_id: str,
        year_month: str,
        acquisition_date: datetime,
        product_id: str,
        resolution: float,
        bands_data: Dict[str, np.ndarray],  # {"red": (256,256), "nir": ...}
        storage: StorageBackend
    ) -> str:
        """
        관측 데이터를 월별 청크에 추가

        핵심 로직:
        1. 기존 청크 읽기 (없으면 새로 생성)
        2. 날짜순 정렬된 위치 찾기 (bisect)
        3. 메타데이터 삽입
        4. Flat 배열에 픽셀 데이터 삽입 (offset 계산!)
        5. 원자적 쓰기 (.tmp → rename)
        """
        chunk_path = f"tiles/{tile_id}/{year_month}.arrow"

        # 1. 기존 청크 읽기
        if storage.exists(chunk_path):
            table = pa.ipc.open_file(chunk_path).read_all()

            dates = table["acquisition_dates"][0].to_pylist()
            product_ids = table["product_ids"][0].to_pylist()
            resolutions = table["resolutions"][0].to_pylist()
            shapes = table["pixel_shapes"][0].to_pylist()

            # 기존 밴드 데이터 (flat)
            band_red_flat = table["band_red"][0].to_numpy(zero_copy_only=False)
            band_nir_flat = table["band_nir"][0].to_numpy(zero_copy_only=False)
        else:
            # 새 청크
            dates = []
            product_ids = []
            resolutions = []
            shapes = []
            band_red_flat = np.array([], dtype=np.float32)
            band_nir_flat = np.array([], dtype=np.float32)

        # 2. 삽입 위치 찾기 (시간순 정렬 유지)
        insert_idx = bisect.bisect_left(dates, acquisition_date)

        # 3. 메타데이터 삽입
        dates.insert(insert_idx, acquisition_date)
        product_ids.insert(insert_idx, product_id)
        resolutions.insert(insert_idx, resolution)

        shape = bands_data["red"].shape
        shapes.insert(insert_idx, list(shape))

        # 4. 픽셀 데이터 삽입 (offset 계산!)
        # Offset = 이전 관측들의 총 픽셀 수
        offset = sum(s[0] * s[1] for s in shapes[:insert_idx])

        # 새 데이터 flat
        new_red_flat = bands_data["red"].flatten()
        new_nir_flat = bands_data["nir"].flatten()

        # 배열에 삽입
        band_red_flat = np.insert(band_red_flat, offset, new_red_flat)
        band_nir_flat = np.insert(band_nir_flat, offset, new_nir_flat)

        # 5. Arrow 테이블 생성
        new_table = pa.table({
            "tile_id": [tile_id],
            "year_month": [year_month],
            "acquisition_dates": [dates],
            "product_ids": [product_ids],
            "resolutions": [resolutions],
            "pixel_shapes": [shapes],
            "band_red": [band_red_flat.tolist()],
            "band_nir": [band_nir_flat.tolist()],
            "observation_count": [len(dates)],
        })

        # 6. 원자적 쓰기
        temp_path = chunk_path + ".tmp"
        with pa.ipc.new_file(temp_path, new_table.schema) as writer:
            writer.write_table(new_table)

        storage.atomic_rename(temp_path, chunk_path)

        return chunk_path

    def extract_observation(
        self,
        chunk_path: str,
        date_idx: int,
        bands: List[str],
        storage: StorageBackend
    ) -> dict:
        """
        특정 관측 추출 (offset 계산)
        """
        table = pa.ipc.open_file(chunk_path).read_all()

        shapes = table["pixel_shapes"][0].to_pylist()
        resolutions = table["resolutions"][0].to_pylist()
        product_ids = table["product_ids"][0].to_pylist()

        # Offset 계산
        offset = sum(s[0] * s[1] for s in shapes[:date_idx])
        size = shapes[date_idx][0] * shapes[date_idx][1]

        # 밴드 추출
        result = {"bands": {}}
        for band in bands:
            flat = table[f"band_{band}"][0].to_numpy()
            data_1d = flat[offset:offset+size]
            data_2d = data_1d.reshape(shapes[date_idx])
            result["bands"][band] = data_2d

        result["resolution"] = resolutions[date_idx]
        result["product_id"] = product_ids[date_idx]
        result["shape"] = shapes[date_idx]

        return result
```

**테스트 케이스** (CRITICAL):
```python
# Test 1: Single observation
chunk_mgr.append_observation(
    tile_id="x0000_y0000",
    year_month="2024-01",
    acquisition_date=datetime(2024, 1, 5),
    product_id="sentinel2_l2a",
    resolution=10.0,
    bands_data={"red": np.random.rand(256, 256), "nir": ...}
)

# Test 2: Mixed resolutions
# Sentinel-2 (10m, 256x256)
chunk_mgr.append_observation(..., resolution=10.0, bands_data={"red": (256,256)})

# Landsat-8 (30m, 85x85)
chunk_mgr.append_observation(..., resolution=30.0, bands_data={"red": (85,85)})

# Planet (3m, 853x853)
chunk_mgr.append_observation(..., resolution=3.0, bands_data={"red": (853,853)})

# Verify: 총 픽셀 수 = 256² + 85² + 853² = 800,370
```

##### 3. `/pixelquery/storage/geoparquet.py`

```python
import geopandas as gpd
from shapely.geometry import box

class GeoParquetWriter:
    """GeoParquet 메타데이터 작성"""

    def write_tile_metadata(
        self,
        tile_records: List[dict],
        partition: str,  # "2024-01"
        storage: StorageBackend
    ) -> str:
        """
        타일 메타데이터를 GeoParquet로 저장

        Records:
        [
            {
                "tile_id": "x0024_y0041",
                "tile_x": 24,
                "tile_y": 41,
                "bounds": (127.0, 37.5, 127.02, 37.54),
                "chunk_file_path": "tiles/x0024_y0041/2024-01.arrow",
                "acquisition_date": datetime(...),
                "product_id": "sentinel2_l2a",
                "resolution": 10.0,
                "band_red_mean": 0.15,
                "band_nir_mean": 0.35,
                ...
            }
        ]
        """
        # Shapely geometries 생성
        geometries = [box(*rec["bounds"]) for rec in tile_records]

        # GeoDataFrame
        gdf = gpd.GeoDataFrame(tile_records, geometry=geometries, crs="EPSG:4326")

        # Parquet 저장 (GeoParquet 형식)
        output_path = f"warehouse/data/month_partition={partition}/data.parquet"
        gdf.to_parquet(output_path, compression="snappy")

        return output_path
```

##### 4. `/pixelquery/core/pixelquery.py` - add_image 구현

```python
def add_image(
    self,
    image_path: str,
    acquisition_date: datetime,
    product_id: str,
    **metadata
) -> dict:
    """이미지 추가 (전체 플로우)"""

    # 1. 제품 프로필 로드
    profile = PRODUCT_REGISTRY[product_id]

    # 2. COG 읽기 + 타일 분할
    cog_reader = COGReader(profile)
    grid = self._grid

    tiles_data = []
    with rasterio.open(image_path) as src:
        # 이미지가 커버하는 타일 ID 찾기
        tile_ids = self._get_intersecting_tiles(src.bounds, grid)

        for tile_id in tile_ids:
            tile_bounds = grid.get_tile_bounds(tile_id)

            # 타일 영역 픽셀 읽기
            tile_data = cog_reader.read_tile_window(image_path, tile_bounds)
            tile_data["tile_id"] = tile_id
            tile_data["acquisition_date"] = acquisition_date
            tiles_data.append(tile_data)

    # 3. 트랜잭션 시작
    tx = self._tx_manager.begin()

    try:
        year_month = acquisition_date.strftime("%Y-%m")
        geoparquet_records = []

        for tile_data in tiles_data:
            # Arrow 청크에 추가
            chunk_path = tx.stage_arrow_chunk(
                tile_id=tile_data["tile_id"],
                year_month=year_month,
                data={
                    "acquisition_date": acquisition_date,
                    "product_id": product_id,
                    "resolution": profile.native_resolution,
                    "bands_data": tile_data["bands"]
                }
            )

            # GeoParquet 레코드 준비
            geoparquet_records.append({
                "tile_id": tile_data["tile_id"],
                "chunk_file_path": chunk_path,
                "acquisition_date": acquisition_date,
                "product_id": product_id,
                "resolution": profile.native_resolution,
                "band_red_mean": np.nanmean(tile_data["bands"]["red"]),
                # ...
            })

        # GeoParquet 스테이징
        tx.stage_geoparquet_metadata(geoparquet_records)

        # 4. 커밋
        result = tx.commit()

        return {
            "snapshot_id": result["snapshot_id"],
            "tiles_written": len(tiles_data),
            "chunk_paths": result["files"]
        }

    except Exception as e:
        tx.rollback()
        raise
```

#### Week 11 마일스톤

```python
pq = PixelQuery("./warehouse")

# 1. Sentinel-2 추가
result = pq.add_image(
    "sentinel2_20240105_T52SCG.tif",
    acquisition_date=datetime(2024, 1, 5),
    product_id="sentinel2_l2a"
)
print(result)
# {
#   "snapshot_id": 1,
#   "tiles_written": 42,
#   "chunk_paths": ["tiles/x0024_y0041/2024-01.arrow", ...]
# }

# 2. Landsat-8 추가 (같은 영역, 다른 해상도!)
result2 = pq.add_image(
    "landsat8_20240107_LC08_116034.tif",
    acquisition_date=datetime(2024, 1, 7),
    product_id="landsat8_l2"
)

# 3. 검증: Arrow 청크가 두 관측 포함하는지
import pyarrow as pa
chunk = pa.ipc.open_file("warehouse/tiles/x0024_y0041/2024-01.arrow").read_all()
print(chunk["observation_count"][0])  # 2
print(chunk["resolutions"][0])  # [10.0, 30.0]
print(chunk["pixel_shapes"][0])  # [[256, 256], [85, 85]]
```

---

### 2.5 Phase 3: Query Engine (5주)

#### 목표

공간+시간 쿼리 + 리샘플링 + 결과 반환

#### 핵심 파일

##### 1. `/pixelquery/query/iceberg_scan.py`

```python
import duckdb

class IcebergScanner:
    """Iceberg 기반 공간/시간 스캔"""

    def scan_tiles(
        self,
        iceberg_table,
        bounds: Tuple[float, float, float, float],
        date_range: Tuple[datetime, datetime],
        snapshot_id: Optional[int] = None
    ) -> List[dict]:
        """
        Phase 1: Iceberg 파티션 프루닝
        Phase 2: DuckDB 공간 필터

        Returns: [{"chunk_file_path": ..., "tile_id": ..., "date": ..., "resolution": ...}]
        """
        # 1. Snapshot 선택
        if snapshot_id:
            scan = iceberg_table.scan(snapshot_id=snapshot_id)
        else:
            scan = iceberg_table.scan()

        # 2. 파티션 프루닝 (month_partition)
        start_ym = date_range[0].strftime("%Y-%m")
        end_ym = date_range[1].strftime("%Y-%m")
        # ... Iceberg partition filter ...

        # 3. GeoParquet 파일 경로 수집
        parquet_files = scan.plan_files()

        # 4. DuckDB spatial 쿼리
        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")

        minx, miny, maxx, maxy = bounds
        roi_wkt = f"POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))"

        query = f"""
            SELECT
                chunk_file_path,
                tile_id,
                acquisition_date,
                product_id,
                native_resolution
            FROM read_parquet([{','.join(f"'{f}'" for f in parquet_files)}])
            WHERE ST_Intersects(
                geometry,
                ST_GeomFromText('{roi_wkt}')
            )
            AND acquisition_date BETWEEN ? AND ?
        """

        result = con.execute(query, [date_range[0], date_range[1]]).fetchall()

        return [
            {
                "chunk_file_path": row[0],
                "tile_id": row[1],
                "acquisition_date": row[2],
                "product_id": row[3],
                "resolution": row[4]
            }
            for row in result
        ]
```

##### 2. `/pixelquery/query/resampling.py`

```python
from scipy.ndimage import zoom

def resample_to_target(
    data: np.ndarray,
    src_resolution: float,
    target_resolution: float,
    method: str = "bilinear"
) -> np.ndarray:
    """
    해상도 변환

    예:
    - Landsat (30m, 85x85) → 10m (256x256): scale=3.0
    - Planet (3m, 853x853) → 10m (256x256): scale=0.3
    """
    if abs(src_resolution - target_resolution) < 0.01:
        return data  # 이미 같은 해상도

    scale_factor = src_resolution / target_resolution

    if method == "bilinear":
        order = 1
    elif method == "nearest":
        order = 0
    else:
        order = 3  # cubic

    resampled = zoom(data, scale_factor, order=order)

    return resampled
```

##### 3. `/pixelquery/query/executor.py`

```python
class QueryExecutor:
    """쿼리 실행 오케스트레이션"""

    def execute_bounds_query(
        self,
        iceberg_table,
        bounds: Tuple[float, float, float, float],
        date_range: Tuple[datetime, datetime],
        bands: List[str],
        target_resolution: float,
        storage: StorageBackend
    ) -> QueryResult:
        """전체 쿼리 플로우"""

        # 1. Iceberg + DuckDB 스캔
        scanner = IcebergScanner()
        tile_records = scanner.scan_tiles(iceberg_table, bounds, date_range)

        # 2. Arrow 청크 병렬 읽기
        chunk_mgr = ArrowChunkManager()

        results = []
        for rec in tile_records:
            # 청크에서 데이터 추출
            obs = chunk_mgr.extract_observation(
                chunk_path=rec["chunk_file_path"],
                date_idx=0,  # TODO: 날짜로 인덱스 찾기
                bands=bands,
                storage=storage
            )

            # 3. 리샘플링
            resampled_bands = {}
            for band_name, band_data in obs["bands"].items():
                resampled = resample_to_target(
                    band_data,
                    src_resolution=obs["resolution"],
                    target_resolution=target_resolution
                )
                resampled_bands[band_name] = resampled

            results.append({
                "tile_id": rec["tile_id"],
                "acquisition_date": rec["acquisition_date"],
                "product_id": rec["product_id"],
                "bands": resampled_bands,
                "resolution": target_resolution
            })

        return QueryResultImpl(results)
```

##### 4. `/pixelquery/query/result.py`

```python
import pandas as pd
import xarray as xr

class QueryResultImpl:
    """쿼리 결과"""

    def __init__(self, tiles: List[dict]):
        self.tiles = tiles

    def to_pandas(self) -> pd.DataFrame:
        """
        Flatten to DataFrame:
        | tile_id | acquisition_date | product_id | band_red | band_nir |
        """
        rows = []
        for tile in self.tiles:
            row = {
                "tile_id": tile["tile_id"],
                "acquisition_date": tile["acquisition_date"],
                "product_id": tile["product_id"],
            }
            # 밴드는 평균값 또는 전체 배열
            for band_name, band_data in tile["bands"].items():
                row[f"band_{band_name}"] = band_data.mean()  # 또는 전체 array

            rows.append(row)

        return pd.DataFrame(rows)

    def to_xarray(self) -> xr.Dataset:
        """
        시공간 큐브:
        Dimensions: (time, y, x)
        Variables: red, nir, ...
        """
        # TODO: 타일들을 공간적으로 모자이크
        pass

    def to_numpy(self) -> Dict[str, np.ndarray]:
        """Raw NumPy arrays"""
        result = {}
        for band_name in self.tiles[0]["bands"].keys():
            arrays = [t["bands"][band_name] for t in self.tiles]
            result[band_name] = np.stack(arrays)
        return result
```

#### Week 16 마일스톤

```python
pq = PixelQuery("./warehouse")

# 데이터 추가 (이미 Phase 2에서 완료)
# ...

# 🎯 쿼리 실행!
result = pq.query_by_bounds(
    bounds=(127.0, 37.5, 127.1, 37.6),
    date_range=(datetime(2024, 1, 1), datetime(2024, 1, 31)),
    bands=["red", "nir"],
    target_resolution=10.0
)

# Pandas 변환
df = result.to_pandas()
print(df)
#   tile_id  acquisition_date     product_id  band_red  band_nir
# 0 x0024_y0041  2024-01-05     sentinel2_l2a    0.145     0.352
# 1 x0024_y0041  2024-01-07     landsat8_l2      0.138     0.348  ← 리샘플링됨!

# NDVI 계산
df['ndvi'] = (df['band_nir'] - df['band_red']) / (df['band_nir'] + df['band_red'])
print(df['ndvi'])

# ✅ SUCCESS: End-to-End 쿼리 작동!
```

---

### 2.6 Phase 4: Testing & Optimization (3주)

#### Week 17: 실제 데이터 테스트

**다운로드**:
1. Sentinel-2 (Copernicus Open Access Hub)
2. Landsat-8 (USGS Earth Explorer)
3. Planet 샘플 (가능하면)

**테스트 시나리오**:
```python
# 1. 단일 제품 (Sentinel-2만)
pq.add_image("s2_seoul_20240105.tif", ...)
result = pq.query_by_bounds(...)
# → 검증: 픽셀 값 정확성, 통계

# 2. 멀티 제품 (S2 + L8)
pq.add_image("s2_seoul_20240105.tif", ...)
pq.add_image("l8_seoul_20240107.tif", ...)
result = pq.query_by_bounds(..., target_resolution=10.0)
# → 검증: 리샘플링 정확성 (RMSE 계산)

# 3. 동시 쓰기
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(3) as executor:
    futures = [
        executor.submit(pq.add_image, "s2_tile1.tif", ...),
        executor.submit(pq.add_image, "s2_tile2.tif", ...),
        executor.submit(pq.add_image, "l8_tile1.tif", ...)
    ]
# → 검증: 트랜잭션 충돌 처리, 데이터 무결성
```

#### Week 18: 성능 최적화

**프로파일링**:
```bash
python -m cProfile -o profile.stats test_query.py
python -m snakeviz profile.stats
```

**병목 예상 & 최적화**:
1. **DuckDB 스캔**: Hilbert 커브 정렬 추가
2. **Arrow 읽기**: LRU 캐시, prefetch
3. **리샘플링**: 벡터화, Numba JIT
4. **GeoParquet**: 컬럼 프루닝

**목표**:
- 쿼리 레이턴시: 300ms → 150ms
- 인제스션: 15s → 10s

#### Week 19: 문서화 & 패키징

```
/docs/
├── README.md               # Quick Start
├── ARCHITECTURE.md         # 아키텍처 상세
├── API.md                  # API 레퍼런스
├── PERFORMANCE.md          # 벤치마크
└── COMMERCIALIZATION.md    # 비즈니스 모델

/examples/
├── 01_basic_ingestion.py
├── 02_multi_resolution_query.py
├── 03_time_series_analysis.ipynb
└── 04_ndvi_calculation.ipynb

pyproject.toml              # 의존성 정리
Dockerfile                  # 컨테이너 이미지
```

---

## 3. 상용화 전략 & Go/No-Go 결정 포인트

### 3.1 단계별 검증 포인트

#### 🚦 Checkpoint 1: Week 2 (Phase 0 완료)

**질문**: 트랜잭션 PoC가 작동하는가?

✅ **GO**:
- Two-phase commit 작동
- 오버헤드 < 100ms
- 충돌 처리 가능

❌ **PIVOT**:
- 트랜잭션 너무 복잡/느림
- → "메타데이터만 ACID" 로 변경
- → 픽셀 데이터는 append-only, best-effort

#### 🚦 Checkpoint 2: Week 11 (Phase 2 완료)

**질문**: 실제 위성 데이터 인제스션이 작동하는가?

✅ **GO**:
- Sentinel-2 + Landsat-8 인제스션 성공
- Arrow 청크 정확성 검증됨
- 멀티-레졸루션 저장 확인

❌ **STOP**:
- 데이터 손상, offset 계산 버그
- → 아키텍처 재설계 필요

#### 🚦 Checkpoint 3: Week 16 (Phase 3 완료)

**질문**: 쿼리가 작동하고 성능이 허용 가능한가?

✅ **GO**:
- E2E 쿼리 작동
- 레이턴시 < 500ms (최적화 전)
- 리샘플링 정확성 검증

❌ **PIVOT**:
- 성능 > 2초 → Rust extension 고려
- 또는 아키텍처 변경 (사전 리샘플링?)

#### 🚦 Checkpoint 4: Week 19 (Phase 4 완료)

**질문**: 파일럿 고객을 확보할 수 있는가?

✅ **GO (상용화)**:
- 3-5개 파일럿 고객 확보
- 긍정적 피드백
- → Phase 5: 프로덕션화 (분산 처리, Glue 카탈로그)

❌ **PIVOT (오픈소스)**:
- 고객 확보 실패
- → 오픈소스로 전환, 커뮤니티 구축
- → 장기적 상용화 재시도

### 3.2 상용화 체크리스트

**기술적 준비**:
- [ ] 프로덕션 카탈로그 (AWS Glue)
- [ ] S3 스토리지 지원
- [ ] 에러 처리 & 로깅
- [ ] 모니터링 (Prometheus?)
- [ ] 문서화 완료

**비즈니스 준비**:
- [ ] 파일럿 고객 3-5개 확보
- [ ] 가격 모델 수립 ($99-999/월?)
- [ ] 법인 설립 (필요 시)
- [ ] 지적재산권 (Apache 2.0 라이선스)

**마케팅**:
- [ ] GitHub 오픈소스 릴리즈
- [ ] Hacker News, Reddit 게시
- [ ] 블로그 포스트 (Medium, Dev.to)
- [ ] 컨퍼런스 발표 (FOSS4G, PyCon)

### 3.3 수익 모델

**Open Core**:
```
무료 (Apache 2.0):
- 로컬 SQLite 카탈로그
- 단일 노드
- 커뮤니티 지원

Pro ($99-299/월):
- AWS Glue 카탈로그
- S3 스토리지
- 이메일 지원
- 월 1TB 데이터

Enterprise ($999-2,999/월):
- 분산 처리 (Spark 통합)
- RBAC & 감사 로그
- SLA 보장
- 전용 지원
```

**Year 1 목표**:
- 오픈소스 사용자: 100-500
- Pro 고객: 5-10
- Enterprise 고객: 1-2
- 수익: $10K-30K

**Year 2 목표**:
- 오픈소스 사용자: 1,000+
- Pro 고객: 30-50
- Enterprise 고객: 5-10
- 수익: $100K-300K

---

## 4. 최종 권고사항

### 4.1 냉정한 평가

**기술적으로**: ✅ **가능**
- 복잡하지만 구현 가능
- PyIceberg/Arrow/DuckDB 성숙도 충분
- 멀티-레졸루션은 독특한 가치

**시장적으로**: 🟡 **불확실**
- COG+STAC이 표준, 전환 비용 높음
- Havasu와 경쟁
- 하지만 AgriTech 등 니치 시장 존재

**상용화로**: 🔴 **고위험**
- Year 1 수익 $10K-30K (부족)
- 파일럿 고객 확보 불확실
- 풀타임 창업으로는 위험, Side Project로 적합

### 4.2 추천 전략

**Option A: Lean Startup (추천!)**
1. **4주**: Phase 0-1 완료 → PoC 데모
2. **고객 인터뷰**: 10-20개 AgriTech/연구소
3. **검증**: 3개 이상이 "이거 필요함"이라면 → Phase 2-3
4. **6주**: MVP 완성 → 파일럿 고객
5. **성공 시**: 전체 구현 + 창업
6. **실패 시**: 오픈소스 전환

**Option B: 오픈소스 First**
1. **전체 구현** (19주)
2. **오픈소스 릴리즈** (Apache 2.0)
3. **커뮤니티 구축** (6-12개월)
4. **사용자 확보 후** 상용 서비스 론칭
5. 장기 플레이 (Iceberg도 3년 걸림)

### 4.3 어느 쪽을 선택할 것인가?

**상용화가 목표라면**:
→ **Option A (Lean Startup)** 강력 추천
- 빠른 시장 검증
- 자원 낭비 최소화
- 피벗 옵션 유지

**학습/포트폴리오 목표였다면**:
→ **Option B (오픈소스)** 추천
- 완전한 구현 경험
- 커뮤니티 기여
- 장기적 가치

---

## 5. 구현 우선순위 (인터페이스 First)

### Week 1-2: 인터페이스 정의 + PoC

**파일 생성 순서**:
1. `pixelquery/core/interfaces.py` (모든 Protocol)
2. `pixelquery/transactions/two_phase_commit.py` (PoC)
3. `pixelquery/storage/backends.py` (StorageBackend 구현)
4. `tests/test_transactions.py` (트랜잭션 테스트)

**목표**: 인터페이스로 Mock 테스트 작성, 트랜잭션 검증

### Week 3-5: Foundation

5. `pixelquery/grid/tile_grid.py`
6. `pixelquery/products/base.py`
7. `pixelquery/products/profiles/sentinel2.py`
8. `pixelquery/products/profiles/landsat8.py`
9. `pixelquery/iceberg/catalog.py`
10. `tests/test_grid.py`, `tests/test_products.py`

### Week 6-11: Storage

11. `pixelquery/io/cog_reader.py`
12. `pixelquery/storage/arrow_chunk.py` (CRITICAL!)
13. `pixelquery/storage/geoparquet.py`
14. `pixelquery/core/pixelquery.py` (add_image)
15. `tests/test_ingestion.py` (실제 COG 데이터)

### Week 12-16: Query

16. `pixelquery/query/iceberg_scan.py`
17. `pixelquery/query/resampling.py`
18. `pixelquery/query/executor.py`
19. `pixelquery/query/result.py`
20. `pixelquery/core/pixelquery.py` (query_by_bounds)
21. `tests/test_query.py` (E2E)

### Week 17-19: Production Ready

22. 문서화 (README, API docs)
23. 예제 (Jupyter notebooks)
24. 성능 최적화
25. 패키징 (PyPI)
26. Docker 이미지

---

## 6. 핵심 리스크 완화 전략

| 리스크 | 확률 | 완화 전략 |
|--------|------|----------|
| 트랜잭션 실패 | 40% | Week 2 PoC, 실패 시 "메타데이터만 ACID" 피벗 |
| 성능 부족 | 60% | Week 18 프로파일링, Rust extension 준비 |
| 고객 확보 실패 | 50% | Week 4 고객 인터뷰, Week 11 파일럿 모집 |
| Havasu 경쟁 | 50% | "Python 생태계" 차별화, 빠른 릴리즈 |
| 개발 지연 | 70% | 단계별 검증, 불필요 기능 과감히 제거 |

---

## 7. 결론: 진행 여부

### ✅ 진행 (조건부)

**조건**:
1. **Lean Startup 방식** 채택 (4주 PoC → 시장 검증)
2. **인터페이스 우선** 개발
3. **단계별 Go/No-Go** 결정 포인트 준수
4. **파일럿 고객 확보** 실패 시 오픈소스 전환 각오

**기대 성과**:
- **기술적**: 복잡한 시스템 설계/구현 경험
- **시장적**: 지리공간 데이터 시장 이해
- **비즈니스적**: 제품 개발 → 고객 확보 경험
- **금전적**: Year 1 $10K-30K (낙관적), $0 (현실적)

**현실적 타임라인**:
- **PoC**: 4주
- **시장 검증**: 2-4주
- **MVP**: 10주 (성공 시)
- **파일럿**: 4-8주
- **전체**: ~6개월 (풀타임)

### ⚠️ 주의사항

이것은 **고위험 프로젝트**입니다:
- 기술적 복잡도 높음
- 시장 전환 비용 높음
- 경쟁자 존재 (Havasu)
- 수익 불확실

**창업 목적이라면**: Side Project로 시작 권장
**학습 목적이라면**: 훌륭한 선택

---

## 다음 단계

1. **지금 바로**: Phase 0 시작 (인터페이스 정의)
2. **Week 2**: 트랜잭션 PoC 검증
3. **Week 4**: 고객 인터뷰 (10-20개)
4. **Week 5**: GO/NO-GO 결정

**첫 파일부터 시작하시겠습니까?**
