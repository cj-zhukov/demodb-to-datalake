use crate::{AppError, table_worker::TableWorker, MAX_ROWS, BOARDING_PASSES_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use serde::Serialize;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};

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
        let ticket_nos = records.iter().map(|r| r.ticket_no.as_str()).collect::<Vec<_>>();
        let flight_ids = records.iter().map(|r| r.flight_id).collect::<Vec<_>>();
        let boarding_nos = records.iter().map(|r| r.boarding_no).collect::<Vec<_>>();
        let seat_nos = records.iter().map(|r| r.seat_no.as_deref()).collect::<Vec<_>>();
        
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
impl TableWorker for BoardingPasses {
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
            .map(|row| format!("ticket_no: {}, flight_id: {}, boarding_no: {}, seat_no: {}", 
                row.get::<String, _>("ticket_no"), 
                row.get::<i32, _>("flight_id"), 
                row.get::<i32, _>("boarding_no"),
                row.get::<String, _>("seat_no"),
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
