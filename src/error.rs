use std::num::ParseIntError;

use crate::utils::QueryParserError;

use color_eyre::Report;
use datafusion::error::DataFusionError;
use datafusion::arrow::error::ArrowError;
use parquet::errors::ParquetError;
use std::io::Error as IoError;
use sqlx::Error as SqlxError;
use serde_json::Error as SerdeError;
use thiserror::Error;


#[derive(Error, Debug)]
pub enum AppError {
    #[error("ParseIntError")]
    ParseIntError(#[from] ParseIntError),

    #[error("QueryParserError")]
    QueryParserError(#[from] QueryParserError),

    #[error("IoError")]
    IOError(#[from] IoError),

    #[error("SerdeError")]
    SerdeError(#[from] SerdeError),

    #[error("SqlxError")]
    SqlxError(#[from] SqlxError),

    #[error("ArrowError")]
    ArrowError(#[from] ArrowError),

    #[error("DataFusionError")]
    DatafusionError(#[from] DataFusionError),

    #[error("ParquetError")]
    ParquetError(#[from] ParquetError),

    #[error("Unexpected error")]
    UnexpectedError(#[source] Report),
}