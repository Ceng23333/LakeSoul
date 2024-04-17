use pyo3::prelude::*;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn lakesoul(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(register_lakesoul_table, m)?)?;

    let datafusion = PyModule::new(py, "datafusion")?;
    init_datafusion_module(datafusion)?;
    m.add_submodule(datafusion)?;
    

    Ok(())
}

use std::sync::Arc;
use datafusion_python::catalog::PyTable;
use datafusion_python::datafusion::arrow;
use datafusion_python::catalog;
use arrow::datatypes::{SchemaRef, Schema};
use arrow::record_batch::RecordBatch;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn register_lakesoul_table(a: usize, b: usize) -> PyResult<PyTable> {
    Ok(PyTable::new(Arc::new(datafusion_python::datafusion::datasource::MemTable::try_new(SchemaRef::new(Schema::empty()), vec![vec![RecordBatch::new_empty(SchemaRef::new(Schema::empty()))]]).unwrap())))
}

/// Initializes the `datafusion-python` module to match the pattern of `datafusion-python` https://docs.rs/datafusion-python/33.0.0/datafusion_python/
fn init_datafusion_module(m: &PyModule) -> PyResult<()> {
    // m.add_class::<catalog::PyCatalog>()?;
    // m.add_class::<catalog::PyDatabase>()?;
    m.add_class::<catalog::PyTable>()?;

    Ok(())
}