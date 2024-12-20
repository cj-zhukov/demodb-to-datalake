use crate::{AppError, MAX_ROWS, TableWorker, SEATS_TABLE_NAME};

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

impl Seats {
    pub fn new() -> Self {
        Seats::default()
    }

    pub fn table_name() -> String {
        SEATS_TABLE_NAME.to_string()
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

    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame, AppError> {
        let mut aircraft_codes = Vec::new();
        let mut seat_nos = Vec::new();
        let mut fare_conditionses = Vec::new();

        for record in records {
            aircraft_codes.push(record.aircraft_code.clone());
            seat_nos.push(record.seat_no.clone());
            fare_conditionses.push(record.fare_conditions.clone());
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(StringArray::from(aircraft_codes)), 
                Arc::new(StringArray::from(seat_nos)),
                Arc::new(StringArray::from(fare_conditionses)),
            ],
        )?;
        let df = ctx.read_batch(batch)?;

        Ok(df)
    }
}

#[async_trait]
impl TableWorker for Seats {
    async fn query_table(&self, pool: &PgPool) -> Result<(), AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);

        Ok(())
    }
    
    async fn query_table_to_string(&self, pool: &PgPool) -> Result<Vec<String>, AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
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

    async fn query_table_to_df(&self, pool: &PgPool) -> Result<DataFrame, AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let mut records = query.fetch_all(pool).await?;
        let ctx = SessionContext::new();
        let df = Self::to_df(ctx, &mut records)?;

        Ok(df)
    }

    async fn query_table_to_json(&self, pool: &PgPool) -> Result<String, AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        let res = serde_json::to_string(&data)?;
        
        Ok(res)
    }
}
