use arrow::array::{
    ArrayRef, BooleanArray, ListArray, TimestampMillisecondArray, UInt16Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::{reader::FileReader, writer::FileWriter};
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyIOError, PyRuntimeError};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

/// Get Arrow schema for pixel chunks
fn get_chunk_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "pixels",
            DataType::List(Arc::new(Field::new("item", DataType::UInt16, false))),
            false,
        ),
        Field::new(
            "mask",
            DataType::List(Arc::new(Field::new("item", DataType::Boolean, false))),
            false,
        ),
    ])
}

/// Add metadata to schema
fn add_metadata_to_schema(schema: Schema, metadata: HashMap<String, String>) -> Schema {
    schema.with_metadata(metadata)
}

/// Extract metadata from schema
fn extract_metadata(schema: &Arc<Schema>) -> HashMap<String, String> {
    schema.metadata().clone()
}

/// Create UInt16 list array from nested vectors
fn create_list_array_u16(data: Vec<Vec<u16>>) -> Result<ListArray, PyErr> {
    let mut values = Vec::new();
    let mut offsets = vec![0i32];

    for row in data {
        let row_len = row.len() as i32;
        values.extend(row);
        offsets.push(offsets.last().unwrap() + row_len);
    }

    let values_array = UInt16Array::from(values);
    let field = Arc::new(Field::new("item", DataType::UInt16, false));
    let offset_buffer = OffsetBuffer::new(offsets.into());

    ListArray::try_new(field, offset_buffer, Arc::new(values_array), None)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create list array: {}", e)))
}

/// Create Boolean list array from nested vectors
fn create_list_array_bool(data: Vec<Vec<bool>>) -> Result<ListArray, PyErr> {
    let mut values = Vec::new();
    let mut offsets = vec![0i32];

    for row in data {
        let row_len = row.len() as i32;
        values.extend(row);
        offsets.push(offsets.last().unwrap() + row_len);
    }

    let values_array = BooleanArray::from(values);
    let field = Arc::new(Field::new("item", DataType::Boolean, false));
    let offset_buffer = OffsetBuffer::new(offsets.into());

    ListArray::try_new(field, offset_buffer, Arc::new(values_array), None)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create list array: {}", e)))
}

/// Create Arrow RecordBatch from data
fn create_record_batch(
    schema: &Schema,
    times_ms: Vec<i64>,
    pixels: Vec<Vec<u16>>,
    masks: Vec<Vec<bool>>,
) -> Result<RecordBatch, PyErr> {
    // Time array with UTC timezone
    let time_array = TimestampMillisecondArray::from(times_ms)
        .with_timezone("UTC");

    // Pixels list array
    let pixels_array = create_list_array_u16(pixels)?;

    // Mask list array
    let masks_array = create_list_array_bool(masks)?;

    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(time_array) as ArrayRef,
            Arc::new(pixels_array) as ArrayRef,
            Arc::new(masks_array) as ArrayRef,
        ],
    )
    .map_err(|e| PyRuntimeError::new_err(format!("Failed to create batch: {}", e)))
}

/// Write Arrow chunk file
#[pyfunction]
pub fn arrow_write_chunk(
    path: String,
    times_ms: Vec<i64>,
    pixels: Vec<Vec<u16>>,
    masks: Vec<Vec<bool>>,
    metadata: HashMap<String, String>,
) -> PyResult<()> {
    let schema = get_chunk_schema();
    let schema_with_metadata = add_metadata_to_schema(schema, metadata);

    let batch = create_record_batch(&schema_with_metadata, times_ms, pixels, masks)?;

    let file = File::create(&path)
        .map_err(|e| PyIOError::new_err(format!("Failed to create file: {}", e)))?;

    let mut writer = FileWriter::try_new(file, &schema_with_metadata)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create writer: {}", e)))?;

    writer
        .write(&batch)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to write batch: {}", e)))?;

    writer
        .finish()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to finish writing: {}", e)))?;

    Ok(())
}

/// Append to existing Arrow chunk file
#[pyfunction]
pub fn arrow_append_to_chunk(
    path: String,
    times_ms: Vec<i64>,
    pixels: Vec<Vec<u16>>,
    masks: Vec<Vec<bool>>,
    metadata: HashMap<String, String>,
) -> PyResult<()> {
    let path_obj = Path::new(&path);

    // If file doesn't exist, create new
    if !path_obj.exists() {
        return arrow_write_chunk(path, times_ms, pixels, masks, metadata);
    }

    // Read existing file
    let existing_file = File::open(&path)
        .map_err(|e| PyIOError::new_err(format!("Failed to open file: {}", e)))?;

    let reader = FileReader::try_new(existing_file, None)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to read existing file: {}", e)))?;

    let schema = reader.schema();
    let existing_metadata = extract_metadata(&schema);

    // Read all existing batches
    let mut existing_batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to read batch: {}", e)))?;
        existing_batches.push(batch);
    }

    // Create new batch
    let new_batch = create_record_batch(&schema, times_ms, pixels, masks)?;

    // Merge metadata
    let mut merged_metadata = existing_metadata;
    for (k, v) in metadata {
        merged_metadata.insert(k, v);
    }

    // Update observation count
    let total_rows: usize = existing_batches.iter().map(|b| b.num_rows()).sum::<usize>()
        + new_batch.num_rows();
    merged_metadata.insert("num_observations".to_string(), total_rows.to_string());
    merged_metadata.insert(
        "last_updated".to_string(),
        chrono::Utc::now().to_rfc3339(),
    );

    let schema_with_metadata = add_metadata_to_schema(schema.as_ref().clone(), merged_metadata);

    // Write to temporary file
    let temp_path = format!("{}.tmp", path);
    let temp_file = File::create(&temp_path)
        .map_err(|e| PyIOError::new_err(format!("Failed to create temp file: {}", e)))?;

    let mut writer = FileWriter::try_new(temp_file, &schema_with_metadata)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create writer: {}", e)))?;

    // Write all existing batches
    for batch in existing_batches {
        writer
            .write(&batch)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to write existing batch: {}", e)))?;
    }

    // Write new batch
    writer
        .write(&new_batch)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to write new batch: {}", e)))?;

    writer
        .finish()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to finish writing: {}", e)))?;

    // Atomic rename
    std::fs::rename(&temp_path, &path)
        .map_err(|e| PyIOError::new_err(format!("Failed to rename temp file: {}", e)))?;

    Ok(())
}
