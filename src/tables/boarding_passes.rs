use crate::{Result, MAX_ROWS, TableWorker, BOARDING_PASSES_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use serde_json::Value;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};

#[derive(Debug, Default, FromRow)]
pub struct BoardingPasses {
    pub ticket_no: String,
    pub flight_id: Option<i32>,
    pub boarding_no: Option<i32>,
    pub seat_no: Option<String>,
}

impl BoardingPasses {
    pub fn new() -> Self {
        BoardingPasses::default()
    }

    pub fn table_name() -> String {
        BOARDING_PASSES_TABLE_NAME.to_string()
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

    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame> {
        let mut ticket_nos = Vec::new();
        let mut flight_ids = Vec::new();
        let mut boarding_nos = Vec::new();
        let mut seat_nos = Vec::new();

        for record in records {
            ticket_nos.push(record.ticket_no.clone());
            flight_ids.push(record.flight_id);
            boarding_nos.push(record.boarding_no);
            seat_nos.push(record.seat_no.clone());
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(StringArray::from(ticket_nos)), 
                Arc::new(Int32Array::from(flight_ids)),
                Arc::new(Int32Array::from(boarding_nos)),
                Arc::new(StringArray::from(seat_nos)), 
            ],
        ).map_err(|e| format!("failed creating batch for table: {} cause: {}", Self::table_name(), e))?;
    
        let df = ctx.read_batch(batch)
            .map_err(|e| format!("failed creating dataframe for table: {} cause: {}", Self::table_name(), e))?;

        Ok(df)
    }
}

#[async_trait]
impl TableWorker for BoardingPasses {
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
        let mut records = query.fetch_all(pool).await?;
        let ctx = SessionContext::new();
        let df = Self::to_df(ctx, &mut records)?;

        Ok(df)
    }
}
