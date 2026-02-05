//! PixelQuery Core - High-performance Rust extensions
//!
//! This crate provides performance-critical operations for PixelQuery:
//! - Array resampling (bilinear, nearest-neighbor)
//! - Arrow chunk I/O (future)

use pyo3::prelude::*;

pub mod processing;

/// PixelQuery Core - Rust performance extensions for satellite imagery processing
#[pymodule]
fn pixelquery_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register resampling functions
    m.add_function(wrap_pyfunction!(processing::resample::resample_bilinear, m)?)?;
    m.add_function(wrap_pyfunction!(
        processing::resample::resample_nearest_neighbor,
        m
    )?)?;

    // Module version
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
