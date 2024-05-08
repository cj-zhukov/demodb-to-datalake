use crate::{Result, MAX_ROWS, TableWorker, BOOKINGS_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::types::Decimal;
use serde_json::Value;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{RecordBatch, StringArray};

#[derive(Debug, Default, FromRow)]
pub struct Bookings {
    pub book_ref: String,
    pub book_date: Option<DateTime<Utc>>,
    pub total_amount: Option<Decimal>
}

impl Bookings {
    pub fn new() -> Self {
        Bookings::default()
    }

    pub fn table_name() -> String {
        BOOKINGS_TABLE_NAME.to_string()
    }
}

impl Bookings {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("book_ref", DataType::Utf8, false),
            Field::new("book_date", DataType::Utf8, true),
            Field::new("total_amount", DataType::Utf8, true),
        ])
    }

    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame> {
        let mut book_refs = Vec::new();
        let mut book_dates: Vec<Option<String>> = Vec::new();
        let mut total_amounts = Vec::new();

        for record in records {
            book_refs.push(record.book_ref.clone());
            let book_date = match &mut record.book_date {
                Some(val) => Some(val.to_rfc3339()),
                None => None
            };
            book_dates.push(book_date);
            let total_amount = match &mut record.total_amount {
                Some(val) => Some(val.to_string()),
                None => None
            };
            total_amounts.push(total_amount);
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(StringArray::from(book_refs)), 
                Arc::new(StringArray::from(book_dates)),
                Arc::new(StringArray::from(total_amounts)),
            ],
        ).map_err(|e| format!("failed creating batch for table: {} cause: {}", Self::table_name(), e))?;
    
        let df = ctx.read_batch(batch)
            .map_err(|e| format!("failed creating dataframe for table: {} cause: {}", Self::table_name(), e))?;

        Ok(df)
    }
}

#[async_trait]
impl TableWorker for Bookings {
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
