use crate::table_worker::TableWorkerStatic;
use crate::{AppError, table_worker::TableWorker, MAX_ROWS, BOOKINGS_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::types::Decimal;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{RecordBatch, StringArray};

#[derive(Debug, Default, FromRow)]
pub struct Bookings {
    pub book_ref: String,
    pub book_date: Option<DateTime<Utc>>,
    pub total_amount: Option<Decimal>
}

impl Serialize for Bookings {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer 
    {
        let book_date = self.book_date.map(|val| val.to_rfc3339());
        let total_amount = self.total_amount.map(|val| val.to_string());

        serde_json::json!({ "book_ref": self.book_ref, "book_date": book_date, "total_amount": total_amount})
            .serialize(serializer)
    }
}

impl AsRef<str> for Bookings {
    fn as_ref(&self) -> &str {
        BOOKINGS_TABLE_NAME
    }
}

impl Bookings {
    pub fn new() -> Self {
        Self::default()
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

    fn to_record_batch(records: &[Self]) -> Result<RecordBatch, AppError> {
        let schema = Arc::new(Self::schema());
        let book_refs = records.iter().map(|r| r.book_ref.as_str()).collect::<Vec<_>>();
        let book_dates = records
            .iter()
            .map(|r| 
                r.book_date
                .as_ref()
                .map(|val| val.to_rfc3339())
            )
            .collect::<Vec<_>>();
        let total_amounts = records
            .iter()
            .map(|r|
                r.total_amount
                .as_ref()
                .map(|val| val.to_string())
            )
            .collect::<Vec<_>>();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(book_refs)), 
                Arc::new(StringArray::from(book_dates)),
                Arc::new(StringArray::from(total_amounts)),
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
impl TableWorker for Bookings {
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
            .map(|row| format!("book_ref: {}, book_date: {}, total_amount: {}", 
                row.get::<String, _>("book_ref"), 
                row.get::<DateTime<Utc>, _>("book_date"), 
                row.get::<Decimal, _>("total_amount"), 
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
impl TableWorkerStatic for Bookings {
    async fn query_table(pool: &PgPool) -> Result<(), AppError> {
        let sql = format!("select * from {BOOKINGS_TABLE_NAME} limit {MAX_ROWS}");
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);
        Ok(())
    }

    async fn query_table_to_string(pool: &PgPool) -> Result<Vec<String>, AppError> {
        let sql = format!("select * from {BOOKINGS_TABLE_NAME} limit {MAX_ROWS}");
        let query = sqlx::query(&sql);
        let data: Vec<PgRow> = query.fetch_all(pool).await?;
        let rows: Vec<String> = data
            .iter()
            .map(|row| format!("book_ref: {}, book_date: {}, total_amount: {}", 
                row.get::<String, _>("book_ref"), 
                row.get::<DateTime<Utc>, _>("book_date"), 
                row.get::<Decimal, _>("total_amount"), 
            ))
            .collect();
        Ok(rows)
    }

    async fn query_table_to_json(pool: &PgPool) -> Result<String, AppError> {
        let sql = format!("select * from {BOOKINGS_TABLE_NAME} limit {MAX_ROWS}");
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        let res = serde_json::to_string(&data)?;
        Ok(res)
    }

    async fn query_table_to_df(pool: &PgPool, query: Option<&str>, ctx: &SessionContext) -> Result<DataFrame, AppError> {
        let sql = match query {
            None => format!("select * from {BOOKINGS_TABLE_NAME} limit {MAX_ROWS}"),
            Some(sql) => sql.to_string(),
        };
        let query = sqlx::query_as::<_, Self>(&sql);
        let records = query.fetch_all(pool).await?;
        let df = Self::to_df(ctx, &records)?;
        Ok(df)
    }
}
