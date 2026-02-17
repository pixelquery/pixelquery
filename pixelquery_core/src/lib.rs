//! PixelQuery Core - High-performance Rust extensions
//!
//! This crate provides performance-critical operations for PixelQuery:
//! - Array resampling (bilinear, nearest-neighbor)
//! - Arrow chunk I/O (10x faster than Python)

use pyo3::prelude::*;

pub mod processing;
pub mod storage;

/// PixelQuery Core - Rust performance extensions for satellite imagery processing
#[pymodule]
fn pixelquery_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register resampling functions
    m.add_function(wrap_pyfunction!(processing::resample::resample_bilinear, m)?)?;
    m.add_function(wrap_pyfunction!(
        processing::resample::resample_nearest_neighbor,
        m
    )?)?;

    // Register Arrow chunk I/O functions (Phase 2.2)
    m.add_function(wrap_pyfunction!(storage::arrow_chunk::arrow_write_chunk, m)?)?;
    m.add_function(wrap_pyfunction!(
        storage::arrow_chunk::arrow_append_to_chunk,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(storage::arrow_chunk::arrow_read_chunk, m)?)?;

    // Module version
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
