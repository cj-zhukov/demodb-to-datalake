use crate::{Result, MAX_ROWS, TableWorker, SEATS_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use serde_json::Value;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{RecordBatch, StringArray};

#[derive(Debug, Default, FromRow)]
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

    pub fn to_df(ctx: SessionContext, records: Vec<Self>) -> Result<DataFrame> {
        let mut aircraft_codes = Vec::new();
        let mut seat_nos = Vec::new();
        let mut fare_conditionses = Vec::new();

        for record in &records {
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
        ).map_err(|e| format!("failed creating batch for table: {} cause: {}", Self::table_name(), e))?;
    
        let df = ctx.read_batch(batch)
            .map_err(|e| format!("failed creating dataframe for table: {} cause: {}", Self::table_name(), e))?;

        Ok(df)
    }
}

#[async_trait]
impl TableWorker for Seats {
    async fn query_table(&self, pool: &PgPool) -> Result<()> {
        let sql = format!("select * from {} limit {};", Self::table_name(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);

        Ok(())
    }

    async fn query_table_to_string(&self, pool: &PgPool) -> Result<Vec<String>> {
        let sql = format!("select * from {} limit {};", Self::table_name(), MAX_ROWS);
        let query = sqlx::query(&sql);
        let data: Vec<PgRow> = query.fetch_all(pool).await?;
    
        let rows: Vec<String> = data
            .iter()
            .map(|row| format!("aircraft_code: {} model: {} range: {}", 
                row.get::<String, _>("aircraft_code"), 
                row.get::<Value, _>("model"), 
                row.get::<String, _>("range"),
            ))
            .collect();
    
        Ok(rows)
    }

    async fn query_table_to_df(&self, pool: &PgPool) -> Result<DataFrame> {
        let sql = format!("select * from {} limit {};", Self::table_name(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let records = query.fetch_all(pool).await?;
        let ctx = SessionContext::new();
        let df = Self::to_df(ctx, records)?;

        Ok(df)
    }
}