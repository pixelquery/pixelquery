//! Arrow IPC Chunk Storage - Rust implementation
//!
//! High-performance Arrow chunk I/O operations for PixelQuery.
//! Provides ~10x speedup over Python implementation for append operations.

use arrow::array::{
    Array, BooleanArray, BooleanBuilder, ListArray, ListBuilder, TimestampMillisecondArray,
    UInt16Array, UInt16Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::Arc;

/// Get the Arrow schema for pixel chunks
/// Schema: time (timestamp[ms, UTC]), pixels (list<uint16>), mask (list<bool>)
fn get_chunk_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        // Note: inner field is nullable: true to match ListBuilder default behavior
        Field::new(
            "pixels",
            DataType::List(Arc::new(Field::new("item", DataType::UInt16, true))),
            false,
        ),
        Field::new(
            "mask",
            DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
            false,
        ),
    ])
}

/// Add metadata to schema
fn schema_with_metadata(schema: &Schema, metadata: HashMap<String, String>) -> Schema {
    schema.clone().with_metadata(metadata)
}

/// Create a record batch from data
fn create_record_batch(
    schema: &Schema,
    times_ms: &[i64],
    pixels: &[Vec<u16>],
    masks: &[Vec<bool>],
) -> Result<RecordBatch, arrow::error::ArrowError> {
    // Time array
    let time_array = TimestampMillisecondArray::from(times_ms.to_vec()).with_timezone("UTC");

    // Pixels list array
    let mut pixels_builder = ListBuilder::new(UInt16Builder::new());
    for pixel_row in pixels {
        let values_builder = pixels_builder.values();
        for &val in pixel_row {
            values_builder.append_value(val);
        }
        pixels_builder.append(true);
    }
    let pixels_array = pixels_builder.finish();

    // Mask list array
    let mut mask_builder = ListBuilder::new(BooleanBuilder::new());
    for mask_row in masks {
        let values_builder = mask_builder.values();
        for &val in mask_row {
            values_builder.append_value(val);
        }
        mask_builder.append(true);
    }
    let mask_array = mask_builder.finish();

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(time_array),
            Arc::new(pixels_array),
            Arc::new(mask_array),
        ],
    )
}

/// Write Arrow chunk file
///
/// # Arguments
/// * `path` - Output file path
/// * `times_ms` - Timestamps in milliseconds since epoch (UTC)
/// * `pixels` - List of pixel arrays (each as Vec<u16>)
/// * `masks` - List of mask arrays (each as Vec<bool>)
/// * `metadata` - Metadata dict (product_id, resolution, etc.)
#[pyfunction]
#[pyo3(signature = (path, times_ms, pixels, masks, metadata))]
pub fn arrow_write_chunk(
    path: String,
    times_ms: Vec<i64>,
    pixels: Vec<Vec<u16>>,
    masks: Vec<Vec<bool>>,
    metadata: &Bound<'_, PyDict>,
) -> PyResult<()> {
    // Convert Python dict to HashMap
    let mut meta_map = HashMap::new();
    for (key, value) in metadata.iter() {
        let k: String = key.extract()?;
        let v: String = value.extract()?;
        meta_map.insert(k, v);
    }

    // Add observation count
    meta_map.insert("num_observations".to_string(), times_ms.len().to_string());

    // Add creation time
    let now: DateTime<Utc> = Utc::now();
    meta_map.insert("creation_time".to_string(), now.to_rfc3339());

    let schema = get_chunk_schema();
    let schema_with_meta = schema_with_metadata(&schema, meta_map);

    let batch = create_record_batch(&schema_with_meta, &times_ms, &pixels, &masks)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    // Create parent directories if needed
    if let Some(parent) = Path::new(&path).parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    }

    // Write to file
    let file = File::create(&path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    let writer = BufWriter::new(file);

    let mut ipc_writer = FileWriter::try_new(writer, &schema_with_meta)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    ipc_writer
        .write(&batch)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    ipc_writer
        .finish()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    Ok(())
}

/// Append to existing Arrow chunk file
///
/// This is the high-performance path. Instead of:
/// 1. Read entire file in Python (60-100ms)
/// 2. Concatenate in Python (5-10ms)
/// 3. Write entire file (30-50ms)
///
/// We do everything in Rust with efficient Arrow operations.
/// Expected: ~10ms total instead of 95-160ms (10-15x faster)
///
/// # Arguments
/// * `path` - Chunk file path
/// * `times_ms` - New timestamps in milliseconds since epoch (UTC)
/// * `pixels` - New pixel arrays
/// * `masks` - New mask arrays
/// * `metadata` - Metadata to merge
#[pyfunction]
#[pyo3(signature = (path, times_ms, pixels, masks, metadata))]
pub fn arrow_append_to_chunk(
    path: String,
    times_ms: Vec<i64>,
    pixels: Vec<Vec<u16>>,
    masks: Vec<Vec<bool>>,
    metadata: &Bound<'_, PyDict>,
) -> PyResult<()> {
    let path_obj = Path::new(&path);

    // If file doesn't exist, create new
    if !path_obj.exists() {
        return arrow_write_chunk(path, times_ms, pixels, masks, metadata);
    }

    // Read existing file
    let existing_file = File::open(&path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    let reader = BufReader::new(existing_file);

    let file_reader = FileReader::try_new(reader, None)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    // Get existing metadata
    let existing_schema = file_reader.schema();
    let existing_metadata: HashMap<String, String> = existing_schema
        .metadata()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Read all existing batches
    let mut existing_batches: Vec<RecordBatch> = Vec::new();
    for batch_result in file_reader {
        let batch = batch_result
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
        existing_batches.push(batch);
    }

    // Create new batch
    let schema = get_chunk_schema();
    let new_batch = create_record_batch(&schema, &times_ms, &pixels, &masks)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    // Merge metadata
    let mut merged_metadata = existing_metadata;

    // Convert Python dict to HashMap
    for (key, value) in metadata.iter() {
        let k: String = key.extract()?;
        let v: String = value.extract()?;
        merged_metadata.insert(k, v);
    }

    // Update observation count
    let total_rows: usize = existing_batches.iter().map(|b| b.num_rows()).sum::<usize>()
        + new_batch.num_rows();
    merged_metadata.insert("num_observations".to_string(), total_rows.to_string());

    // Update last_updated time
    let now: DateTime<Utc> = Utc::now();
    merged_metadata.insert("last_updated".to_string(), now.to_rfc3339());

    let schema_with_meta = schema_with_metadata(&schema, merged_metadata);

    // Write to temporary file
    let temp_path = format!("{}.tmp", path);
    let temp_file = File::create(&temp_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    let writer = BufWriter::new(temp_file);

    let mut ipc_writer = FileWriter::try_new(writer, &schema_with_meta)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    // Write all existing batches
    for batch in &existing_batches {
        // Need to create new batch with updated schema
        let rebatched = RecordBatch::try_new(
            Arc::new(schema_with_meta.clone()),
            batch.columns().to_vec(),
        )
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
        ipc_writer
            .write(&rebatched)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
    }

    // Write new batch
    let new_batch_with_schema =
        RecordBatch::try_new(Arc::new(schema_with_meta.clone()), new_batch.columns().to_vec())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;
    ipc_writer
        .write(&new_batch_with_schema)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    ipc_writer
        .finish()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    // Atomic rename
    std::fs::rename(&temp_path, &path).map_err(|e| {
        // Clean up temp file on error
        let _ = std::fs::remove_file(&temp_path);
        PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e))
    })?;

    Ok(())
}

/// Read Arrow chunk file and return data
///
/// Returns: (times_ms, pixels, masks, metadata)
#[pyfunction]
#[pyo3(signature = (path))]
pub fn arrow_read_chunk(
    py: Python<'_>,
    path: String,
) -> PyResult<(Vec<i64>, Vec<Vec<u16>>, Vec<Vec<bool>>, Py<PyDict>)> {
    let file = File::open(&path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("{}", e)))?;
    let reader = BufReader::new(file);

    let file_reader = FileReader::try_new(reader, None)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

    // Get metadata
    let schema = file_reader.schema();
    let metadata: HashMap<String, String> = schema
        .metadata()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Read all batches
    let mut all_times: Vec<i64> = Vec::new();
    let mut all_pixels: Vec<Vec<u16>> = Vec::new();
    let mut all_masks: Vec<Vec<bool>> = Vec::new();

    for batch_result in file_reader {
        let batch = batch_result
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{}", e)))?;

        // Extract time column
        let time_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Invalid time column")
            })?;

        for i in 0..time_col.len() {
            if !time_col.is_null(i) {
                all_times.push(time_col.value(i));
            }
        }

        // Extract pixels column
        let pixels_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Invalid pixels column")
            })?;

        for i in 0..pixels_col.len() {
            if !pixels_col.is_null(i) {
                let values = pixels_col.value(i);
                let uint16_arr = values.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Invalid pixel values")
                })?;
                let pixel_vec: Vec<u16> = uint16_arr.values().to_vec();
                all_pixels.push(pixel_vec);
            }
        }

        // Extract mask column
        let mask_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Invalid mask column")
            })?;

        for i in 0..mask_col.len() {
            if !mask_col.is_null(i) {
                let values = mask_col.value(i);
                let bool_arr = values
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Invalid mask values")
                    })?;
                let mask_vec: Vec<bool> = (0..bool_arr.len()).map(|i| bool_arr.value(i)).collect();
                all_masks.push(mask_vec);
            }
        }
    }

    // Convert metadata to Python dict
    let py_metadata = PyDict::new_bound(py);
    for (k, v) in metadata {
        py_metadata.set_item(k, v)?;
    }

    Ok((all_times, all_pixels, all_masks, py_metadata.unbind()))
}
