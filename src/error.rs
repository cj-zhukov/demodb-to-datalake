pub type Result<T> = core::result::Result<T, Error>;

use thiserror::Error;
use std::num::ParseIntError;
use std::io::Error as IoError;
use sqlx::Error as SqlxError;
use serde_json::Error as SerdeError;
use datafusion::error::DataFusionError;
use datafusion::arrow::error::ArrowError;
use parquet::errors::ParquetError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("custom error: `{0}`")]
    Custom(String),

    #[error("parse int error: `{0}`")]
    ParseIntError(#[from] ParseIntError),

    #[error("io error: `{0}`")]
    IOError(#[from] IoError),

    #[error("config parse error: `{0}`")]
    ConfigParseError(#[from] SerdeError),

    #[error("sqlx error: `{0}`")]
    SqlxError(#[from] SqlxError),

    #[error("arrow error: `{0}`")]
    ArrowError(#[from] ArrowError),

    #[error("datafusion error: `{0}`")]
    DatafusionError(#[from] DataFusionError),

    #[error("parquet error: `{0}`")]
    ParquetError(#[from] ParquetError),
}