#!/usr/bin/env python
"""
성능 최적화 전후 비교 벤치마크

Phase 1: 메타데이터 배칭
Phase 3: 병렬 처리
"""

import time
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone
import numpy as np
import rasterio
from rasterio.transform import from_bounds

from pixelquery.io.ingest import IngestionPipeline
from pixelquery.grid.tile_grid import FixedTileGrid
from pixelquery.catalog.local import LocalCatalog


def create_test_files(n_files=5):
    """테스트용 COG 파일 생성"""
    test_dir = tempfile.mkdtemp()
    cog_files = []

    print(f"Creating {n_files} test COG files...")
    for i in range(n_files):
        cog_path = Path(test_dir) / f"test_{i:03d}.tif"

        # 1024x1024 4밴드 테스트 이미지 (중간 크기, 더 많은 타일)
        width, height = 1024, 1024
        # 각 파일마다 다른 위치 (더 넓은 영역, 더 많은 타일 생성)
        bounds = (126.5 + i*0.5, 37.0, 127.0 + i*0.5, 37.5)

        transform = from_bounds(*bounds, width, height)

        # 4밴드 랜덤 데이터
        data = np.random.randint(100, 4000, (4, height, width), dtype=np.uint16)

        # COG 파일 쓰기
        with rasterio.open(
            cog_path, 'w',
            driver='GTiff',
            height=height,
            width=width,
            count=4,
            dtype=np.uint16,
            crs='EPSG:4326',
            transform=transform,
            nodata=0
        ) as dst:
            dst.write(data)

        acq_time = datetime(2024, 6, 1 + i, 10, 30, tzinfo=timezone.utc)
        cog_files.append((str(cog_path), acq_time))

    print(f"✓ {n_files} COG files created\n")
    return test_dir, cog_files


def benchmark_baseline(cog_files):
    """Baseline: auto_commit=True (Phase 1 이전)"""
    print("\n" + "="*60)
    print("BASELINE: 최적화 전 (auto_commit=True, parallel=False)")
    print("="*60)

    warehouse_dir = tempfile.mkdtemp()

    try:
        tile_grid = FixedTileGrid()
        catalog = LocalCatalog(warehouse_dir)
        pipeline = IngestionPipeline(
            warehouse_path=warehouse_dir,
            tile_grid=tile_grid,
            catalog=catalog,
            max_workers=1  # 순차 처리
        )

        start = time.time()

        for i, (cog_path, acq_time) in enumerate(cog_files, 1):
            print(f"  [{i}/{len(cog_files)}] Ingesting {Path(cog_path).name}...", end=" ")
            t0 = time.time()

            metadata = pipeline.ingest_cog(
                cog_path=cog_path,
                acquisition_time=acq_time,
                product_id="test_product",
                band_mapping={1: "blue", 2: "green", 3: "red", 4: "nir"},
                auto_commit=True,  # 즉시 메타데이터 쓰기
                parallel=False  # 순차 처리
            )

            elapsed = time.time() - t0
            print(f"{elapsed:.2f}s ({len(metadata)} chunks)")

        total_time = time.time() - start

        print(f"\n총 시간: {total_time:.2f}s")
        print(f"파일당 평균: {total_time/len(cog_files):.2f}s")

        return total_time

    finally:
        shutil.rmtree(warehouse_dir)


def benchmark_phase1(cog_files):
    """Phase 1: 메타데이터 배칭"""
    print("\n" + "="*60)
    print("PHASE 1: 메타데이터 배칭 (auto_commit=False)")
    print("="*60)

    warehouse_dir = tempfile.mkdtemp()

    try:
        tile_grid = FixedTileGrid()
        catalog = LocalCatalog(warehouse_dir)
        pipeline = IngestionPipeline(
            warehouse_path=warehouse_dir,
            tile_grid=tile_grid,
            catalog=catalog,
            max_workers=1  # 순차 처리
        )

        start = time.time()

        for i, (cog_path, acq_time) in enumerate(cog_files, 1):
            print(f"  [{i}/{len(cog_files)}] Ingesting {Path(cog_path).name}...", end=" ")
            t0 = time.time()

            metadata = pipeline.ingest_cog(
                cog_path=cog_path,
                acquisition_time=acq_time,
                product_id="test_product",
                band_mapping={1: "blue", 2: "green", 3: "red", 4: "nir"},
                auto_commit=False,  # 메타데이터 버퍼링
                parallel=False  # 순차 처리
            )

            elapsed = time.time() - t0
            print(f"{elapsed:.2f}s ({len(metadata)} chunks)")

        # 메타데이터 일괄 쓰기
        print("\n  [메타데이터 일괄 쓰기]...", end=" ")
        t0 = time.time()
        pipeline.flush_metadata()
        flush_time = time.time() - t0
        print(f"{flush_time:.2f}s")

        total_time = time.time() - start

        print(f"\n총 시간: {total_time:.2f}s")
        print(f"파일당 평균: {total_time/len(cog_files):.2f}s")

        return total_time

    finally:
        shutil.rmtree(warehouse_dir)


def benchmark_phase3(cog_files):
    """Phase 1 + 3: 메타데이터 배칭 + 병렬 처리"""
    print("\n" + "="*60)
    print("PHASE 1+3: 메타데이터 배칭 + 병렬 처리")
    print("="*60)

    warehouse_dir = tempfile.mkdtemp()

    try:
        tile_grid = FixedTileGrid()
        catalog = LocalCatalog(warehouse_dir)
        pipeline = IngestionPipeline(
            warehouse_path=warehouse_dir,
            tile_grid=tile_grid,
            catalog=catalog,
            max_workers=4  # 4개 워커
        )

        start = time.time()

        for i, (cog_path, acq_time) in enumerate(cog_files, 1):
            print(f"  [{i}/{len(cog_files)}] Ingesting {Path(cog_path).name}...", end=" ")
            t0 = time.time()

            metadata = pipeline.ingest_cog(
                cog_path=cog_path,
                acquisition_time=acq_time,
                product_id="test_product",
                band_mapping={1: "blue", 2: "green", 3: "red", 4: "nir"},
                auto_commit=False,  # 메타데이터 버퍼링
                parallel=True  # 병렬 처리
            )

            elapsed = time.time() - t0
            print(f"{elapsed:.2f}s ({len(metadata)} chunks)")

        # 메타데이터 일괄 쓰기
        print("\n  [메타데이터 일괄 쓰기]...", end=" ")
        t0 = time.time()
        pipeline.flush_metadata()
        flush_time = time.time() - t0
        print(f"{flush_time:.2f}s")

        total_time = time.time() - start

        print(f"\n총 시간: {total_time:.2f}s")
        print(f"파일당 평균: {total_time/len(cog_files):.2f}s")

        return total_time

    finally:
        shutil.rmtree(warehouse_dir)


def main():
    print("""
╔══════════════════════════════════════════════════════════════╗
║         PixelQuery 성능 최적화 벤치마크                        ║
╚══════════════════════════════════════════════════════════════╝
""")

    # 테스트 파일 생성
    test_dir, cog_files = create_test_files(n_files=5)

    try:
        # 벤치마크 실행
        baseline_time = benchmark_baseline(cog_files)
        phase1_time = benchmark_phase1(cog_files)
        phase3_time = benchmark_phase3(cog_files)

        # 결과 요약
        print("\n" + "="*60)
        print("성능 비교 요약")
        print("="*60)
        print(f"{'구성':<30} {'시간':>10} {'파일당':>10} {'개선율':>10}")
        print("-" * 60)
        print(f"{'Baseline (최적화 전)':<30} {baseline_time:>9.2f}s {baseline_time/len(cog_files):>9.2f}s {'1.0x':>10}")
        print(f"{'Phase 1 (메타데이터 배칭)':<30} {phase1_time:>9.2f}s {phase1_time/len(cog_files):>9.2f}s {baseline_time/phase1_time:>9.1f}x")
        print(f"{'Phase 1+3 (배칭+병렬)':<30} {phase3_time:>9.2f}s {phase3_time/len(cog_files):>9.2f}s {baseline_time/phase3_time:>9.1f}x")
        print("="*60)

        # 243개 파일 예상 시간
        print(f"\n243개 파일 예상 시간:")
        print(f"  Baseline:  {baseline_time/len(cog_files)*243/60:.1f}분")
        print(f"  Phase 1:   {phase1_time/len(cog_files)*243/60:.1f}분")
        print(f"  Phase 1+3: {phase3_time/len(cog_files)*243/60:.1f}분 ✨")

        print(f"\n총 절약 시간: {(baseline_time - phase3_time)/len(cog_files)*243/60:.1f}분")

    finally:
        # 정리
        shutil.rmtree(test_dir)


if __name__ == "__main__":
    main()
