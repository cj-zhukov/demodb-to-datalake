use crate::table_worker::TableWorkerStatic;
use crate::{AppError, table_worker::TableWorker, MAX_ROWS, SEATS_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{RecordBatch, StringArray};

#[derive(Debug, Default, FromRow, Serialize)]
pub struct Seats {
    pub aircraft_code: String,
    pub seat_no: Option<String>,
    pub fare_conditions: Option<String>,
}

impl AsRef<str> for Seats {
    fn as_ref(&self) -> &str {
        SEATS_TABLE_NAME
    }
}

impl Seats {
    pub fn new() -> Self {
        Seats::default()
    }
}

impl Seats {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("aircraft_code", DataType::Utf8, false),
            Field::new("seat_no", DataType::Utf8, true),
            Field::new("fare_conditions", DataType::Utf8, true),
        ])
    }

    fn to_record_batch(records: &[Self]) -> Result<RecordBatch, AppError> {
        let schema = Arc::new(Self::schema());
        let aircraft_codes = records.iter().map(|r| r.aircraft_code.as_str()).collect::<Vec<_>>();
        let seat_nos = records.iter().map(|r| r.seat_no.as_deref()).collect::<Vec<_>>();
        let fare_conditions_all = records.iter().map(|r| r.fare_conditions.as_deref()).collect::<Vec<_>>();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(aircraft_codes)), 
                Arc::new(StringArray::from(seat_nos)),
                Arc::new(StringArray::from(fare_conditions_all)),
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
impl TableWorker for Seats {
    async fn query_table(&self, pool: &PgPool) -> Result<(), AppError> {
        let sql = format!("select * from {} limit {}", self.as_ref(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);
        Ok(())
    }
    
    async fn query_table_to_string(&self, pool: &PgPool) -> Result<Vec<String>, AppError> {
        let sql = format!("select * from {} limit {}", self.as_ref(), MAX_ROWS);
        let query = sqlx::query(&sql);
        let data: Vec<PgRow> = query.fetch_all(pool).await?;
        let rows: Vec<String> = data
            .iter()
            .map(|row| format!("aircraft_code: {}, seat_no: {}, fare_conditions: {}", 
                row.get::<String, _>("aircraft_code"), 
                row.get::<String, _>("seat_no"), 
                row.get::<String, _>("fare_conditions"),
            ))
            .collect();
        Ok(rows)
    }

    async fn query_table_to_df(&self, pool: &PgPool, query: Option<&str>, ctx: &SessionContext) -> Result<DataFrame, AppError> {
        let sql = match query {
            None => format!("select * from {} limit {}", self.as_ref(), MAX_ROWS),
            Some(sql) => sql.to_string(),
        };
        let query = sqlx::query_as::<_, Self>(&sql);
        let records = query.fetch_all(pool).await?;
        let df = Self::to_df(ctx, &records)?;
        Ok(df)
    }

    async fn query_table_to_json(&self, pool: &PgPool) -> Result<String, AppError> {
        let sql = format!("select * from {} limit {}", self.as_ref(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        let res = serde_json::to_string(&data)?;
        Ok(res)
    }
}

#[async_trait]
impl TableWorkerStatic for Seats {
    async fn query_table(pool: &PgPool) -> Result<(), AppError> {
        let sql = format!("select * from {SEATS_TABLE_NAME} limit {MAX_ROWS}");
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);
        Ok(())
    }

    async fn query_table_to_string(pool: &PgPool) -> Result<Vec<String>, AppError> {
        let sql = format!("select * from {SEATS_TABLE_NAME} limit {MAX_ROWS}");
        let query = sqlx::query(&sql);
        let data: Vec<PgRow> = query.fetch_all(pool).await?;
        let rows: Vec<String> = data
            .iter()
            .map(|row| format!("aircraft_code: {}, seat_no: {}, fare_conditions: {}", 
                row.get::<String, _>("aircraft_code"), 
                row.get::<String, _>("seat_no"), 
                row.get::<String, _>("fare_conditions"),
            ))
            .collect();
        Ok(rows)
    }

    async fn query_table_to_json(pool: &PgPool) -> Result<String, AppError> {
        let sql = format!("select * from {SEATS_TABLE_NAME} limit {MAX_ROWS}");
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        let res = serde_json::to_string(&data)?;
        Ok(res)
    }

    async fn query_table_to_df(pool: &PgPool, query: Option<&str>, ctx: &SessionContext) -> Result<DataFrame, AppError> {
        let sql = match query {
            None => format!("select * from {SEATS_TABLE_NAME} limit {MAX_ROWS}"),
            Some(sql) => sql.to_string(),
        };
        let query = sqlx::query_as::<_, Self>(&sql);
        let records = query.fetch_all(pool).await?;
        let df = Self::to_df(ctx, &records)?;
        Ok(df)
    }
}