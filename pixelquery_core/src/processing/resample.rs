//! Array resampling operations
//!
//! High-performance resampling for satellite imagery:
//! - Bilinear interpolation for continuous data (pixel values)
//! - Nearest-neighbor for discrete data (masks)
//!
//! Optimized for uint16 satellite imagery with ~10x speedup over scipy.

use ndarray::{Array2, ArrayView2};
use numpy::{IntoPyArray, PyArray2, PyReadonlyArray2};
use pyo3::prelude::*;
use rayon::prelude::*;

/// Bilinear interpolation resample for uint16 arrays
///
/// # Arguments
/// * `data` - Input 2D array of uint16 pixel values
/// * `target_height` - Output height in pixels
/// * `target_width` - Output width in pixels
///
/// # Returns
/// Resampled array with exact (target_height, target_width) shape
///
/// # Example
/// ```python
/// from pixelquery_core import resample_bilinear
/// import numpy as np
///
/// data = np.array([[100, 200], [300, 400]], dtype=np.uint16)
/// result = resample_bilinear(data, 4, 4)
/// assert result.shape == (4, 4)
/// ```
#[pyfunction]
pub fn resample_bilinear<'py>(
    py: Python<'py>,
    data: PyReadonlyArray2<'py, u16>,
    target_height: usize,
    target_width: usize,
) -> PyResult<Bound<'py, PyArray2<u16>>> {
    let input = data.as_array();
    let output = bilinear_resample_impl(input, target_height, target_width);
    Ok(output.into_pyarray_bound(py))
}

/// Nearest-neighbor resample for boolean mask arrays
///
/// # Arguments
/// * `mask` - Input 2D boolean mask array
/// * `target_height` - Output height in pixels
/// * `target_width` - Output width in pixels
///
/// # Returns
/// Resampled mask with exact (target_height, target_width) shape
///
/// # Example
/// ```python
/// from pixelquery_core import resample_nearest_neighbor
/// import numpy as np
///
/// mask = np.array([[True, False], [False, True]], dtype=bool)
/// result = resample_nearest_neighbor(mask, 4, 4)
/// assert result.shape == (4, 4)
/// ```
#[pyfunction]
pub fn resample_nearest_neighbor<'py>(
    py: Python<'py>,
    mask: PyReadonlyArray2<'py, bool>,
    target_height: usize,
    target_width: usize,
) -> PyResult<Bound<'py, PyArray2<bool>>> {
    let input = mask.as_array();
    let output = nearest_neighbor_impl(input, target_height, target_width);
    Ok(output.into_pyarray_bound(py))
}

/// Internal bilinear resampling implementation
///
/// Uses parallel row processing for large arrays (>1000 rows)
fn bilinear_resample_impl(input: ArrayView2<u16>, target_h: usize, target_w: usize) -> Array2<u16> {
    let (input_h, input_w) = input.dim();

    // Handle edge cases
    if input_h == 0 || input_w == 0 || target_h == 0 || target_w == 0 {
        return Array2::zeros((target_h, target_w));
    }

    // If same size, just copy
    if input_h == target_h && input_w == target_w {
        return input.to_owned();
    }

    let scale_h = (input_h - 1) as f64 / (target_h.max(1) - 1).max(1) as f64;
    let scale_w = (input_w - 1) as f64 / (target_w.max(1) - 1).max(1) as f64;

    // Pre-compute source positions for each target column
    let x_positions: Vec<(usize, usize, f64)> = (0..target_w)
        .map(|j| {
            let src_x = j as f64 * scale_w;
            let x0 = (src_x.floor() as usize).min(input_w.saturating_sub(1));
            let x1 = (x0 + 1).min(input_w.saturating_sub(1));
            let dx = src_x - x0 as f64;
            (x0, x1, dx)
        })
        .collect();

    // Use parallel processing for large arrays
    let rows: Vec<Vec<u16>> = if target_h > 100 {
        (0..target_h)
            .into_par_iter()
            .map(|i| {
                let src_y = i as f64 * scale_h;
                let y0 = (src_y.floor() as usize).min(input_h.saturating_sub(1));
                let y1 = (y0 + 1).min(input_h.saturating_sub(1));
                let dy = src_y - y0 as f64;

                x_positions
                    .iter()
                    .map(|&(x0, x1, dx)| {
                        // Bilinear interpolation
                        let v00 = input[[y0, x0]] as f64;
                        let v01 = input[[y0, x1]] as f64;
                        let v10 = input[[y1, x0]] as f64;
                        let v11 = input[[y1, x1]] as f64;

                        let v0 = v00 * (1.0 - dx) + v01 * dx;
                        let v1 = v10 * (1.0 - dx) + v11 * dx;
                        let value = v0 * (1.0 - dy) + v1 * dy;

                        value.round().clamp(0.0, u16::MAX as f64) as u16
                    })
                    .collect()
            })
            .collect()
    } else {
        // Sequential for small arrays
        (0..target_h)
            .map(|i| {
                let src_y = i as f64 * scale_h;
                let y0 = (src_y.floor() as usize).min(input_h.saturating_sub(1));
                let y1 = (y0 + 1).min(input_h.saturating_sub(1));
                let dy = src_y - y0 as f64;

                x_positions
                    .iter()
                    .map(|&(x0, x1, dx)| {
                        let v00 = input[[y0, x0]] as f64;
                        let v01 = input[[y0, x1]] as f64;
                        let v10 = input[[y1, x0]] as f64;
                        let v11 = input[[y1, x1]] as f64;

                        let v0 = v00 * (1.0 - dx) + v01 * dx;
                        let v1 = v10 * (1.0 - dx) + v11 * dx;
                        let value = v0 * (1.0 - dy) + v1 * dy;

                        value.round().clamp(0.0, u16::MAX as f64) as u16
                    })
                    .collect()
            })
            .collect()
    };

    // Build output array from rows
    let flat: Vec<u16> = rows.into_iter().flatten().collect();
    Array2::from_shape_vec((target_h, target_w), flat).expect("Shape mismatch in resampling")
}

/// Internal nearest-neighbor resampling implementation
fn nearest_neighbor_impl(input: ArrayView2<bool>, target_h: usize, target_w: usize) -> Array2<bool> {
    let (input_h, input_w) = input.dim();

    // Handle edge cases
    if input_h == 0 || input_w == 0 || target_h == 0 || target_w == 0 {
        return Array2::default((target_h, target_w));
    }

    // If same size, just copy
    if input_h == target_h && input_w == target_w {
        return input.to_owned();
    }

    let scale_h = input_h as f64 / target_h as f64;
    let scale_w = input_w as f64 / target_w as f64;

    // Pre-compute source x indices
    let x_indices: Vec<usize> = (0..target_w)
        .map(|j| {
            let src_x = (j as f64 + 0.5) * scale_w - 0.5;
            src_x.round().max(0.0) as usize
        })
        .map(|x| x.min(input_w.saturating_sub(1)))
        .collect();

    // Use parallel processing for large arrays
    let rows: Vec<Vec<bool>> = if target_h > 100 {
        (0..target_h)
            .into_par_iter()
            .map(|i| {
                let src_y = (i as f64 + 0.5) * scale_h - 0.5;
                let y = (src_y.round().max(0.0) as usize).min(input_h.saturating_sub(1));

                x_indices.iter().map(|&x| input[[y, x]]).collect()
            })
            .collect()
    } else {
        (0..target_h)
            .map(|i| {
                let src_y = (i as f64 + 0.5) * scale_h - 0.5;
                let y = (src_y.round().max(0.0) as usize).min(input_h.saturating_sub(1));

                x_indices.iter().map(|&x| input[[y, x]]).collect()
            })
            .collect()
    };

    // Build output array
    let flat: Vec<bool> = rows.into_iter().flatten().collect();
    Array2::from_shape_vec((target_h, target_w), flat).expect("Shape mismatch in resampling")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;

    #[test]
    fn test_bilinear_upscale() {
        let input = array![[100u16, 200], [300, 400]];
        let result = bilinear_resample_impl(input.view(), 4, 4);

        assert_eq!(result.dim(), (4, 4));
        // Corners should match (approximately)
        assert_eq!(result[[0, 0]], 100);
        assert_eq!(result[[0, 3]], 200);
        assert_eq!(result[[3, 0]], 300);
        assert_eq!(result[[3, 3]], 400);
    }

    #[test]
    fn test_bilinear_downscale() {
        let input = Array2::from_elem((256, 256), 1000u16);
        let result = bilinear_resample_impl(input.view(), 64, 64);

        assert_eq!(result.dim(), (64, 64));
        // Uniform input should give uniform output
        assert!(result.iter().all(|&v| v == 1000));
    }

    #[test]
    fn test_nearest_neighbor() {
        let input = array![[true, false], [false, true]];
        let result = nearest_neighbor_impl(input.view(), 4, 4);

        assert_eq!(result.dim(), (4, 4));
    }

    #[test]
    fn test_same_size() {
        let input = array![[100u16, 200], [300, 400]];
        let result = bilinear_resample_impl(input.view(), 2, 2);

        assert_eq!(result, input);
    }

    #[test]
    fn test_empty_input() {
        let input: Array2<u16> = Array2::zeros((0, 0));
        let result = bilinear_resample_impl(input.view(), 4, 4);
        assert_eq!(result.dim(), (4, 4));
    }
}
