use pyo3::prelude::*;

mod processing;
mod storage;

/// PixelQuery Core - Rust performance extensions
#[pymodule]
fn pixelquery_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Resampling functions
    m.add_function(wrap_pyfunction!(processing::resample::resample_bilinear, m)?)?;
    m.add_function(wrap_pyfunction!(processing::resample::resample_nearest_neighbor, m)?)?;

    // Arrow I/O functions
    m.add_function(wrap_pyfunction!(storage::arrow_chunk::arrow_write_chunk, m)?)?;
    m.add_function(wrap_pyfunction!(storage::arrow_chunk::arrow_append_to_chunk, m)?)?;

    Ok(())
}
