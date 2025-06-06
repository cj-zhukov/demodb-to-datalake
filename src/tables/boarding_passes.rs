use crate::table_worker::{TableWorkerDyn, TableWorkerStatic};
use crate::{prepare_query, AppError, BOARDING_PASSES_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use serde::Serialize;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};

#[derive(Debug, Default, FromRow, Serialize)]
pub struct BoardingPasses {
    pub ticket_no: String,
    pub flight_id: Option<i32>,
    pub boarding_no: Option<i32>,
    pub seat_no: Option<String>,
}

impl AsRef<str> for BoardingPasses {
    fn as_ref(&self) -> &str {
        BOARDING_PASSES_TABLE_NAME
    }
}

impl BoardingPasses {
    pub fn new() -> Self {
        BoardingPasses::default()
    }
}

impl BoardingPasses {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("ticket_no", DataType::Utf8, false),
            Field::new("flight_id", DataType::Int32, true),
            Field::new("boarding_no", DataType::Int32, true),
            Field::new("seat_no", DataType::Utf8, true),
        ])
    }

    fn to_record_batch(records: &[Self]) -> Result<RecordBatch, AppError> {
        let schema = Arc::new(Self::schema());
        let ticket_nos = records
            .iter()
            .map(|r| r.ticket_no.as_str())
            .collect::<Vec<_>>();
        let flight_ids = records.iter().map(|r| r.flight_id).collect::<Vec<_>>();
        let boarding_nos = records.iter().map(|r| r.boarding_no).collect::<Vec<_>>();
        let seat_nos = records
            .iter()
            .map(|r| r.seat_no.as_deref())
            .collect::<Vec<_>>();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ticket_nos)),
                Arc::new(Int32Array::from(flight_ids)),
                Arc::new(Int32Array::from(boarding_nos)),
                Arc::new(StringArray::from(seat_nos)),
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
impl TableWorkerDyn for BoardingPasses {
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
                    "ticket_no: {}, flight_id: {}, boarding_no: {}, seat_no: {}",
                    row.get::<String, _>("ticket_no"),
                    row.get::<i32, _>("flight_id"),
                    row.get::<i32, _>("boarding_no"),
                    row.get::<String, _>("seat_no"),
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
        let df = Self::to_df(ctx, &records)?;
        Ok(df)
    }
}

#[async_trait]
impl TableWorkerStatic for BoardingPasses {
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
                    "ticket_no: {}, flight_id: {}, boarding_no: {}, seat_no: {}",
                    row.get::<String, _>("ticket_no"),
                    row.get::<i32, _>("flight_id"),
                    row.get::<i32, _>("boarding_no"),
                    row.get::<String, _>("seat_no"),
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
        let df = Self::to_df(ctx, &records)?;
        Ok(df)
    }
}
