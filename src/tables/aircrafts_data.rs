use crate::table_worker::{TableWorkerDyn, TableWorkerStatic};
use crate::{prepare_query, AppError, AIRCRAFTS_DATA_TABLE_NAME};

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::Json;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};

#[derive(Debug, Default, FromRow, Serialize)]
pub struct AircraftsData {
    pub aircraft_code: String,
    pub model: Option<Json<Model>>,
    pub range: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Model {
    pub en: Option<String>,
    pub ru: Option<String>,
}

impl AsRef<str> for AircraftsData {
    fn as_ref(&self) -> &str {
        AIRCRAFTS_DATA_TABLE_NAME
    }
}

impl AircraftsData {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AircraftsData {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("aircraft_code", DataType::Utf8, false),
            Field::new("model", DataType::Utf8, true),
            Field::new("range", DataType::Int32, true),
        ])
    }

    fn to_record_batch(records: &[Self]) -> Result<RecordBatch, AppError> {
        let schema = Arc::new(Self::schema());
        let aircraft_codes = records
            .iter()
            .map(|r| r.aircraft_code.as_str())
            .collect::<Vec<_>>();
        let models = records
            .iter()
            .map(|r| {
                r.model
                    .as_ref()
                    .map(|val| serde_json::to_string(val))
                    .transpose()
            })
            .collect::<Result<Vec<_>, serde_json::Error>>()?;
        let ranges = records.iter().map(|r| r.range).collect::<Vec<_>>();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(aircraft_codes)),
                Arc::new(StringArray::from(models)),
                Arc::new(Int32Array::from(ranges)),
            ],
        )?)
    }

    pub fn to_df(ctx: &SessionContext, records: &[Self]) -> Result<DataFrame, AppError> {
        let batch = Self::to_record_batch(records)?;
        let df = ctx.read_batch(batch)?;
        Ok(df)
    }
}

#[async_trait]
impl TableWorkerDyn for AircraftsData {
    async fn query_table(&self, pool: &PgPool, query: &str) -> Result<(), AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);
        Ok(())
    }

    async fn query_table_to_string(
        &self,
        pool: &PgPool,
        query: &str,
    ) -> Result<Vec<String>, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query(&query);
        let data: Vec<PgRow> = query.fetch_all(pool).await?;
        let rows: Vec<String> = data
            .iter()
            .map(|row| {
                format!(
                    "aircraft_code: {}, model: {}, range: {}",
                    row.get::<String, _>("aircraft_code"),
                    row.get::<Value, _>("model"),
                    row.get::<i32, _>("range"),
                )
            })
            .collect();
        Ok(rows)
    }

    async fn query_table_to_json(&self, pool: &PgPool, query: &str) -> Result<String, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let data = query.fetch_all(pool).await?;
        let res = serde_json::to_string(&data)?;
        Ok(res)
    }

    async fn query_table_to_df(
        &self,
        pool: &PgPool,
        query: &str,
        ctx: &SessionContext,
    ) -> Result<DataFrame, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let records = query.fetch_all(pool).await?;
        let df = Self::to_df(&ctx, &records)?;
        Ok(df)
    }
}

#[async_trait]
impl TableWorkerStatic for AircraftsData {
    async fn query_table(pool: &PgPool, query: &str) -> Result<(), AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);
        Ok(())
    }

    async fn query_table_to_string(pool: &PgPool, query: &str) -> Result<Vec<String>, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query(&query);
        let data: Vec<PgRow> = query.fetch_all(pool).await?;
        let rows: Vec<String> = data
            .iter()
            .map(|row| {
                format!(
                    "aircraft_code: {}, model: {}, range: {}",
                    row.get::<String, _>("aircraft_code"),
                    row.get::<Value, _>("model"),
                    row.get::<i32, _>("range"),
                )
            })
            .collect();
        Ok(rows)
    }

    async fn query_table_to_json(pool: &PgPool, query: &str) -> Result<String, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let data = query.fetch_all(pool).await?;
        let res = serde_json::to_string(&data)?;
        Ok(res)
    }

    async fn query_table_to_df(
        pool: &PgPool,
        query: &str,
        ctx: &SessionContext,
    ) -> Result<DataFrame, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let records = query.fetch_all(pool).await?;
        let df = Self::to_df(&ctx, &records)?;
        Ok(df)
    }
}
