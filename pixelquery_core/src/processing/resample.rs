use ndarray::{Array2, ArrayView2};
use numpy::{IntoPyArray, PyArray2, PyReadonlyArray2};
use pyo3::prelude::*;

/// Bilinear interpolation resample
///
/// # Arguments
/// * `data` - Input 2D array (uint16)
/// * `target_height` - Output height
/// * `target_width` - Output width
///
/// # Returns
/// Resampled array with exact (target_height, target_width) shape
#[pyfunction]
pub fn resample_bilinear(
    py: Python,
    data: PyReadonlyArray2<u16>,
    target_height: usize,
    target_width: usize,
) -> PyResult<Py<PyArray2<u16>>> {
    let input = data.as_array();
    let output = bilinear_resample_impl(input, target_height, target_width);
    Ok(output.into_pyarray(py).unbind())
}

/// Nearest-neighbor resample for boolean masks
#[pyfunction]
pub fn resample_nearest_neighbor(
    py: Python,
    mask: PyReadonlyArray2<bool>,
    target_height: usize,
    target_width: usize,
) -> PyResult<Py<PyArray2<bool>>> {
    let input = mask.as_array();
    let output = nearest_neighbor_impl(input, target_height, target_width);
    Ok(output.into_pyarray(py).unbind())
}

// Implementation functions
fn bilinear_resample_impl(
    input: ArrayView2<u16>,
    target_h: usize,
    target_w: usize,
) -> Array2<u16> {
    let (input_h, input_w) = input.dim();
    let mut output = Array2::zeros((target_h, target_w));

    let scale_h = input_h as f32 / target_h as f32;
    let scale_w = input_w as f32 / target_w as f32;

    for i in 0..target_h {
        for j in 0..target_w {
            let src_y = i as f32 * scale_h;
            let src_x = j as f32 * scale_w;

            let y0 = src_y.floor() as usize;
            let x0 = src_x.floor() as usize;
            let y1 = (y0 + 1).min(input_h - 1);
            let x1 = (x0 + 1).min(input_w - 1);

            let dy = src_y - y0 as f32;
            let dx = src_x - x0 as f32;

            // Bilinear interpolation
            let v00 = input[[y0, x0]] as f32;
            let v01 = input[[y0, x1]] as f32;
            let v10 = input[[y1, x0]] as f32;
            let v11 = input[[y1, x1]] as f32;

            let v0 = v00 * (1.0 - dx) + v01 * dx;
            let v1 = v10 * (1.0 - dx) + v11 * dx;
            let value = v0 * (1.0 - dy) + v1 * dy;

            output[[i, j]] = value.round() as u16;
        }
    }

    output
}

fn nearest_neighbor_impl(
    input: ArrayView2<bool>,
    target_h: usize,
    target_w: usize,
) -> Array2<bool> {
    let (input_h, input_w) = input.dim();
    let mut output = Array2::default((target_h, target_w));

    let scale_h = input_h as f32 / target_h as f32;
    let scale_w = input_w as f32 / target_w as f32;

    for i in 0..target_h {
        for j in 0..target_w {
            let src_y = (i as f32 * scale_h).round() as usize;
            let src_x = (j as f32 * scale_w).round() as usize;

            let y = src_y.min(input_h - 1);
            let x = src_x.min(input_w - 1);

            output[[i, j]] = input[[y, x]];
        }
    }

    output
}
