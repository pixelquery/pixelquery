# Changelog

All notable changes to PixelQuery are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While in `0.x`, breaking changes may occur in any minor release. We will document
breaking changes prominently and provide migration notes where possible.

## [Unreleased]

### Added
- `py.typed` marker for PEP 561 type-checking support.
- `CHANGELOG.md`, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`.
- Issue and pull request templates under `.github/`.
- `dependabot.yml` for automated dependency updates.
- `CODEOWNERS` for automatic review assignment.

### Changed
- `__version__` is now resolved dynamically from package metadata via
  `importlib.metadata`, eliminating manual version drift.
- Project metadata (`authors`, `maintainers`, classifiers) updated to reflect
  the active maintainer and supported Python versions (3.11–3.13).
- Removed duplicate `patch_imagecodecs()` call in `PixelQueryS3.__init__`;
  the patch is applied once at package import time.

### Deprecated
- `[legacy]` extras (Iceberg backend) — planned for removal in `0.2.0`.
  Use `[icechunk]` instead.

## [0.1.22] - 2026-03-11

### Changed
- Decoupled cloud mask profiles from the core library; profiles are now resolved
  through a registry lookup, allowing downstream projects to register custom
  cloud mask configurations.

## [0.1.21] - 2026-03-11

### Added
- Mask path support follow-up (refines the path-resolution introduced in 0.1.20).

## [0.1.20] - 2026-03-10

### Added
- Mask path support for cloud mask integration.

## [0.1.19] - 2026-03-10

### Changed
- Version bump for release infrastructure (no user-facing changes).

## [0.1.18] - 2026-03-10

### Added
- `cloud_mask` parameter support in `point_timeseries`, enabling cloud-aware
  pixel sampling at point coordinates.

## [0.1.17] - 2026-03-09

### Changed
- `LocalCatalog` is now imported lazily, removing `geopandas` from the
  hard import path. This shortens cold-start time for users who do not need
  catalog functionality and avoids importing heavy geospatial dependencies
  unnecessarily.

## [0.1.16] - 2026-03-09

### Changed
- Version bump (no user-facing changes).

## [0.1.15] - 2026-03-09

### Fixed
- Ruff and mypy errors in `point_timeseries`.

## [0.1.14] - 2026-02-25

### Changed
- Internal release-process work (versions 0.1.9 through 0.1.13 were not
  published to PyPI).

## [0.1.8] - 2026-02-19

### Changed
- `PixelQueryS3` now configures the underlying obstore S3Store via environment
  variables to avoid a `pyo3` serialization panic that occurred when passing
  configuration directly through `S3Store` arguments.

## [0.1.7] - 2026-02-19

### Fixed
- VCC (Virtual Chunk Container) credential cast error by wrapping S3
  credentials with `containers_credentials`.

## [0.1.6] - 2026-02-19

### Fixed
- `S3Store` pyo3 panic by switching to `aws_`-prefixed configuration keys
  expected by the underlying Rust object_store crate.

## [0.1.5] - 2026-02-19

### Fixed
- `S3Store` bucket configuration and VCC prefix derivation. The VCC prefix
  must include the path beyond the root `/` (e.g. `file:///Users/`).

## [0.1.4] - 2026-02-19

### Added
- S3 `ObjectStore` registration in the VirtualiZarr registry, enabling virtual
  zarr stores to resolve S3-backed COG references.

## [0.1.3] - 2026-02-19

### Added
- Thread-safe Icechunk access via a singleton manager and an internal write
  lock. Concurrent ingestion from multiple threads no longer races on the
  underlying repository.

## [0.1.2] - 2026-02-19

### Fixed
- Mypy errors for `icechunk` S3 store types.

## [0.1.1] - 2026-02-19

### Fixed
- CI configuration issues.
- Optional dependency resolution when the `[icechunk]` extras were not installed.

## [0.1.0] - 2026-02-17

### Added
- Initial public release of PixelQuery.
- `PixelQueryS3` high-level API for ingesting Cloud-Optimized GeoTIFFs from
  S3-compatible storage into Icechunk virtual zarr repositories.
- Spatiotemporal scene search via `list_scenes(time_range, bounds)`.
- BBox cropping (`crop`), GeoJSON polygon clipping (`clip`), per-scene statistics
  (`stats`), NDVI computation (`ndvi`), and PNG rendering (`render_png`).
- Polygon timeseries (`timeseries`) and multi-field comparison
  (`multi_field_stats`).
- Clip-to-COG export (`clip_to_cog`) and one-call clip-to-PNG (`clip_to_png`).
- STAC 1.0.0 Item and Collection serialization (`to_stac_item`,
  `to_stac_collection`) with no `pystac` dependency.
- Scene-level lazy caching with FIFO eviction (`open_scene` cache).
- Local-file ingestion path via `pq.ingest()` and `pq.open_xarray()`.
- Custom exception hierarchy: `PixelQueryError`, `IngestionError`,
  `QueryError`, `ValidationError`, `TransactionError`.
- Built-in colormaps (`rdylgn`, `viridis`, `inferno`).
- Imagecodecs compatibility patch for Icechunk/VirtualTIFF interop.

[Unreleased]: https://github.com/pixelquery/pixelquery/compare/v0.1.22...HEAD
[0.1.22]: https://github.com/pixelquery/pixelquery/compare/v0.1.21...v0.1.22
[0.1.21]: https://github.com/pixelquery/pixelquery/compare/v0.1.20...v0.1.21
[0.1.20]: https://github.com/pixelquery/pixelquery/compare/v0.1.19...v0.1.20
[0.1.19]: https://github.com/pixelquery/pixelquery/compare/v0.1.18...v0.1.19
[0.1.18]: https://github.com/pixelquery/pixelquery/compare/v0.1.17...v0.1.18
[0.1.17]: https://github.com/pixelquery/pixelquery/compare/v0.1.16...v0.1.17
[0.1.16]: https://github.com/pixelquery/pixelquery/compare/v0.1.15...v0.1.16
[0.1.15]: https://github.com/pixelquery/pixelquery/compare/v0.1.14...v0.1.15
[0.1.14]: https://github.com/pixelquery/pixelquery/compare/v0.1.8...v0.1.14
[0.1.8]: https://github.com/pixelquery/pixelquery/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/pixelquery/pixelquery/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/pixelquery/pixelquery/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/pixelquery/pixelquery/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/pixelquery/pixelquery/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/pixelquery/pixelquery/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/pixelquery/pixelquery/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/pixelquery/pixelquery/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/pixelquery/pixelquery/releases/tag/v0.1.0
