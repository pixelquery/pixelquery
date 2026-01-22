# Performance Comparison: COG+STAC vs Open Table Format

**Version**: 1.0
**Date**: 2026-01-06

## Executive Summary

**Key Finding**: Open Table Format (Iceberg+Arrow) is **5-30x faster** for time-series queries (1+ year) compared to COG+STAC, but COG+STAC is better for real-time access.

**Recommendation**: **Hybrid architecture** - use both!
- COG+STAC for real-time, short-term queries
- Iceberg+Arrow for historical, long-term analytics

---

## Test Scenario

### Query Parameters
```python
bounds = (126.9, 37.4, 127.1, 37.6)  # Seoul, 30km × 30km
date_range = ("2024-01-01", "2024-12-31")  # 1 year
products = ["sentinel2-l2a", "landsat8-l2"]
bands = ["red", "nir"]
target_resolution = 10.0  # meters
```

### Data Volume
- **Sentinel-2**: 5-day cycle → 73 acquisitions/year
- **Landsat-8**: 16-day cycle → 23 acquisitions/year
- **Tiles**: 10 tiles (2.56km × 2.56km each)
- **Total images**: 960 (S2: 730, L8: 230)

---

## Architecture 1: COG+STAC

### Query Execution Flow

```python
# Stage 1: Metadata query
stac_items = search_stac(
    bbox=(126.9, 37.4, 127.1, 37.6),
    datetime="2024-01-01/2024-12-31",
    collections=["sentinel2-l2a", "landsat8-l2"]
)
# → 960 STAC items returned

# Stage 2: COG file reads (960 iterations!)
results = []
for item in stac_items:
    # Separate HTTP request per COG
    with rasterio.open(item.assets['red'].href) as src:
        red = src.read(1, window=tile_window)  # HTTP range request

    with rasterio.open(item.assets['nir'].href) as src:
        nir = src.read(1, window=tile_window)

    # Stage 3: Resampling (if needed)
    if item.product == 'landsat8':
        red = scipy.ndimage.zoom(red, 3.0)  # 30m → 10m
        nir = scipy.ndimage.zoom(nir, 3.0)

    # Stage 4: NDVI calculation
    ndvi = (nir - red) / (nir + red)
    results.append(ndvi)
```

### Performance Breakdown

#### Bottleneck 1: HTTP Request Count 🔴

| Item | Value | Calculation |
|------|-------|-------------|
| **Total COG files** | 960 | 73 S2 + 23 L8 × 10 tiles |
| **HTTP requests per band** | 1-3 | Header + range requests |
| **Total HTTP round-trips** | **2,880-5,760** | 960 × 2 bands × 1-3 |

**Time estimation**:
```
Optimistic (caching, parallel):
  - HTTP RTT: ~50ms (same region as S3)
  - Parallelism: 10 concurrent requests
  - Time: (960 × 2 × 50ms) / 10 = 9.6s

Realistic (sequential, congested):
  - HTTP RTT: ~150ms
  - Parallelism: 5
  - Time: (960 × 2 × 150ms) / 5 = 57.6s
```

**🔴 Expected: 10-60 seconds (I/O bottleneck)**

#### Bottleneck 2: COG File Size 🟡

Sentinel-2 tile:
- Tile size: 2.56km × 2.56km @ 10m = 256×256 pixels
- 2 bands (red, nir) × 4 bytes = 512KB
- **But** COG contains full scene → actually 10-50MB

**Problem**: Need only tile, but download large file
- Windowed reads help, but HTTP overhead remains

#### Bottleneck 3: Resampling (CPU) ✅

```python
# Resample 230 Landsat images (30m → 10m)
for i in range(230):
    zoom(data_85x85, scale=3.0)  # ~5-10ms each

# Total: 230 × 7ms = 1.6s
```

**✅ Expected: 1-2 seconds (CPU, acceptable)**

### COG+STAC Total Performance

```
Metadata query:     0.05-0.2s  ✅ (DuckDB fast)
COG HTTP reads:     10-60s     🔴 (main bottleneck!)
Resampling:         1-2s       ✅ (acceptable)
NDVI calculation:   0.5s       ✅ (acceptable)
─────────────────────────────────────
Total:              12-63s     🔴 Slow!
```

**Conclusion**: **I/O bottleneck is severe**

---

## Architecture 2: Open Table Format (Iceberg + Arrow)

### Data Structure

```
# Monthly Arrow chunks (organized by tile)
tiles/x0024_y0041/2024-01.arrow  # Jan observations (S2: 6, L8: 2)
tiles/x0024_y0041/2024-02.arrow
...
tiles/x0024_y0041/2024-12.arrow

# Inside each chunk:
- acquisition_dates: [datetime1, datetime2, ...]
- product_ids: ['sentinel2', 'sentinel2', 'landsat8', ...]
- resolutions: [10.0, 10.0, 30.0, ...]
- band_red: [flat array of all pixels]
- band_nir: [flat array]
- pixel_shapes: [[256,256], [256,256], [85,85], ...]
```

### Query Execution Flow

```python
# Stage 1: Metadata query (Iceberg)
chunk_paths = duckdb.execute("""
    SELECT DISTINCT chunk_file_path
    FROM tile_metadata
    WHERE ST_Intersects(geometry, ?)
      AND acquisition_date BETWEEN ? AND ?
""").fetchall()
# → 120 chunk paths (10 tiles × 12 months)

# Stage 2: Arrow chunk reads (parallel)
import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor

def read_chunk(chunk_path):
    table = pa.ipc.open_file(chunk_path).read_all()
    return table

with ThreadPoolExecutor(10) as executor:
    chunks = list(executor.map(read_chunk, chunk_paths))
# → 120 Arrow files read in parallel

# Stage 3: Resampling + NDVI (per chunk)
for chunk in chunks:
    dates = chunk['acquisition_dates'][0].to_pylist()
    shapes = chunk['pixel_shapes'][0].to_pylist()
    resolutions = chunk['resolutions'][0].to_pylist()

    # Extract each observation from flat array
    offset = 0
    for i, (shape, res) in enumerate(zip(shapes, resolutions)):
        size = shape[0] * shape[1]

        # Extract pixel data
        red_flat = chunk['band_red'][0].to_numpy()
        red = red_flat[offset:offset+size].reshape(shape)

        # Resample if needed
        if res != 10.0:
            red = scipy.ndimage.zoom(red, res/10.0)

        offset += size
```

### Performance Breakdown

#### Bottleneck 1: File I/O ✅

| Item | Value | Calculation |
|------|-------|-------------|
| **Total Arrow chunks** | 120 | 10 tiles × 12 months |
| **Avg chunk size** | ~2-5MB | 8 obs/month × 2 bands × 256² |
| **Total data** | 240-600MB | 120 × 2-5MB |

**Time estimation**:
```
S3 parallel reads (10 workers):
  - Avg file size: 3MB
  - S3 throughput: ~100MB/s (parallel)
  - Time: 600MB / 100MB/s = 6s

Local reads (SSD):
  - SSD throughput: ~500MB/s
  - Time: 600MB / 500MB/s = 1.2s
```

**✅ Expected: 1-6 seconds (I/O, 10x faster than COG!)**

**Why faster?**
- Fewer files: 120 vs 960 (8x reduction)
- Monthly aggregation → multiple observations per file
- Arrow IPC is compact and zero-copy

#### Bottleneck 2: Arrow Decoding 🟡

```python
# Decode 120 chunks
for chunk in chunks:  # 120 iterations
    table.to_numpy()  # Arrow → NumPy conversion, ~10-20ms

# Total: 120 × 15ms = 1.8s
```

**✅ Expected: 1-2 seconds (acceptable)**

#### Bottleneck 3: Offset Calculation (Variable-Length Arrays) ✅

```python
# Extract observations with offset calculation
shapes = [[256,256], [256,256], [85,85], ...]  # variable!
offset = sum(s[0]*s[1] for s in shapes[:i])  # cumulative

# 960 observations × 0.01ms = 9.6ms
```

**✅ Expected: <0.1 second (negligible)**

#### Bottleneck 4: Resampling (CPU) ✅

```python
# Same as COG (230 Landsat resamplings)
# Total: 1-2s
```

**✅ Expected: 1-2 seconds (same as COG)**

### Iceberg+Arrow Total Performance

```
Metadata query (Iceberg):  0.05-0.2s  ✅
Arrow chunk reads (S3):    1-6s       ✅ (10x faster than COG)
Arrow decoding:            1-2s       ✅
Offset calculation:        <0.1s      ✅
Resampling:                1-2s       ✅
NDVI calculation:          0.5s       ✅
──────────────────────────────────────
Total:                     4-11s      ✅ Fast!
```

**Conclusion**: **I/O bottleneck significantly reduced**

---

## Performance Comparison

### 1-Year Time-Series Query (Seoul, 960 images)

| Aspect | COG+STAC | Iceberg+Arrow | Improvement |
|--------|----------|---------------|-------------|
| **Metadata query** | 0.05-0.2s | 0.05-0.2s | Same |
| **File I/O** | 10-60s 🔴 | 1-6s ✅ | **10x faster** |
| **Decoding** | Included | 1-2s | Arrow overhead |
| **Resampling** | 1-2s | 1-2s | Same |
| **Total** | **12-63s** | **4-11s** | **5x faster** |

### Bottleneck Analysis

**COG+STAC**:
- 🔴 **File count**: 960 (each observation = 1 file)
- 🔴 **HTTP overhead**: 2,880-5,760 round-trips
- 🟡 Large file sizes (full scene included)

**Iceberg+Arrow**:
- ✅ **File count**: 120 (monthly aggregation)
- ✅ **HTTP requests**: 120 (8x reduction)
- ✅ Compact Arrow format

---

## Special Cases

### Case 1: Single Date Query (1 day)

```python
# "2024-01-05 Seoul data"
# Expected: 10 tiles (S2 only for that date)
```

**COG+STAC**:
- Files: 10 COG
- I/O: 10 × 2 bands × 50ms = 1s
- **Total: 1-2s** ✅ Fast!

**Iceberg+Arrow**:
- Files: 10 chunks (each tile's 2024-01.arrow)
- But loads entire January (8 observations)
- I/O: 10 × 3MB = 30MB → 0.3s
- **Total: 1-1.5s** ✅ Similar

**Conclusion**: Single date → **COG+STAC slightly better** (loads only needed data)

### Case 2: Multi-Year Time-Series (5 years)

```python
# "2020-2024 Seoul NDVI change"
# Images: 4,800
```

**COG+STAC**:
- I/O: 4,800 × 2 × 150ms / 5 = **288s (4.8 minutes)** 🔴
- Even with caching: ~2 minutes

**Iceberg+Arrow**:
- Files: 10 tiles × 60 months = 600 chunks
- I/O: 600 × 3MB / 100MB/s = **18s** ✅
- Optimized: **5-10s**

**Conclusion**: Multi-year time-series → **Iceberg+Arrow dominates** (30x faster)

### Case 3: Real-Time Streaming (Latest Data Only)

```python
# "Yesterday/today new data"
```

**COG+STAC**:
- New COG immediately queryable
- Ingestion: 0s (already COG)
- **Total: Immediate** ✅

**Iceberg+Arrow**:
- New COG → Arrow chunk conversion required
- Ingestion: 5-15s
- **Total: 5-15s delay** 🟡

**Conclusion**: Real-time → **COG+STAC better** (no ingestion lag)

---

## Optimization Scenarios

### COG+STAC Optimization (Best Case)

```python
# 1. HTTP/2 multiplexing + aggressive caching
# 2. Prefetching (prediction-based)
# 3. Local cache (Redis, Memcached)

# After optimization:
# - HTTP overhead: 50% reduction
# - Cache hit: 30%
# - Time: 12s → 6-8s
```

**Best performance**: 6-8 seconds

### Iceberg+Arrow Optimization

```python
# 1. Local SSD storage
# 2. Pre-resampling (all to 10m)
# 3. Parquet column pruning

# After optimization:
# - I/O: 6s → 1s (SSD)
# - Resampling: 2s → 0s (pre-processed)
# - Time: 4s → 2-3s
```

**Best performance**: 2-3 seconds

---

## Final Recommendations

### Recommended Architecture: Hybrid

Use **both** COG+STAC and Iceberg+Arrow:

```
┌────────────────────────────────────┐
│ COG+STAC (Source, Real-time)       │
│ - Immediate access to new data     │
│ - Short-term queries (fast)        │
└────────────────────────────────────┘
          ↓ Ingestion (batch, async)
┌────────────────────────────────────┐
│ Iceberg+Arrow (Optimized, Analytics)│
│ - Monthly aggregation              │
│ - Long-term time-series (very fast)│
└────────────────────────────────────┘
```

### Query Routing Strategy

```python
def query_with_routing(date_range, strategy="auto"):
    """
    Auto-routing query strategy

    - Recent 7 days: COG+STAC (real-time)
    - 1+ month: Iceberg+Arrow (time-series optimized)
    """
    days = (date_range[1] - date_range[0]).days

    if strategy == "auto":
        if days <= 7:
            return query_cog_stac(date_range)  # Fast real-time
        else:
            return query_iceberg_arrow(date_range)  # Time-series optimized
```

### Ingestion Strategy

```python
# Dagster job: COG → Arrow (daily batch)
@daily_schedule
def cog_to_arrow_etl():
    """
    Convert yesterday's COG data to Arrow chunks

    - Real-time: Query from COG
    - Next day: Convert to Arrow (background)
    - Analytics: Fast query from Arrow
    """
    yesterday_cogs = get_new_cogs(days_ago=1)

    for cog in yesterday_cogs:
        append_to_arrow_chunk(cog)  # Add to monthly chunk

    update_iceberg_metadata()  # Update catalog
```

---

## Performance Decision Matrix

| Query Type | Duration | Data Volume | Recommended | Reason |
|------------|----------|-------------|-------------|--------|
| **Real-time** | <24 hours | <100 images | COG+STAC | No ingestion lag |
| **Recent** | 1-7 days | <500 images | COG+STAC | Fast enough |
| **Monthly** | 1 month | ~1,000 images | Iceberg+Arrow | 2-3x faster |
| **Seasonal** | 3-12 months | 3K-12K images | Iceberg+Arrow | 5-10x faster |
| **Multi-year** | 1-5 years | 10K-50K images | Iceberg+Arrow | 10-30x faster |
| **Historical** | 5+ years | 50K+ images | Iceberg+Arrow | 30x+ faster |

---

## Conclusion

### Key Findings

1. **COG+STAC is bottlenecked by HTTP requests** (960 files = slow)
2. **Iceberg+Arrow dramatically reduces I/O** (120 files = 10x faster)
3. **Time-series queries benefit most** (5-30x improvement)
4. **Real-time access still needs COG+STAC** (no ingestion lag)

### The Hybrid Advantage

**Don't choose one. Use both strategically.**

```
Real-time layer:  COG+STAC (last 7 days)
Analytics layer:  Iceberg+Arrow (historical)

Best of both worlds:
- Immediate access ✅
- Fast analytics ✅
- Zero migration ✅
```

---

## Benchmarking Checklist

To validate these findings in your environment:

- [ ] Measure HTTP RTT to your S3/storage (baseline)
- [ ] Test COG windowed reads with rasterio (1, 10, 100, 1000 files)
- [ ] Test Arrow IPC reads from S3 (parallel vs sequential)
- [ ] Profile resampling CPU time (scipy vs alternatives)
- [ ] Test DuckDB spatial query performance (1M STAC items)
- [ ] Measure end-to-end query latency for your use case
- [ ] Compare local SSD vs S3 performance
- [ ] Test with real network conditions (latency, bandwidth limits)

---

## Related Documents

- Architecture comparison: `hybrid_architecture.md`
- Full implementation plan: `implementation_plan.md`
- Original design: `pixelquery_design.md`
- Plan file: `~/.claude/plans/crispy-dazzling-wombat.md`

---

**Last Updated**: 2026-01-06
**Validated**: Based on research and theoretical analysis
**Status**: Requires real-world benchmarking
