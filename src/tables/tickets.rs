use crate::table_worker::{TableWorkerDyn, TableWorkerStatic};
use crate::{prepare_query, AppError, TICKETS_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use sqlx::types::Json;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{RecordBatch, StringArray};

#[derive(Debug, Default, FromRow, Serialize)]
pub struct Tickets {
    pub ticket_no: String, 
    pub book_ref: Option<String>,
    pub passenger_id: Option<String>,
    pub passenger_name: Option<String>,
    pub contact_data: Option<Json<ContactData>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContactData {
    pub email: Option<String>,
    pub phone: Option<String>,
}

impl AsRef<str> for Tickets {
    fn as_ref(&self) -> &str {
        TICKETS_TABLE_NAME 
    }
}

impl Tickets {
    pub fn new() -> Self {
        Tickets::default()
    }
}

impl Tickets {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("ticket_no", DataType::Utf8, false),
            Field::new("book_ref", DataType::Utf8, true),
            Field::new("passenger_id", DataType::Utf8, true),
            Field::new("passenger_name", DataType::Utf8, true),
            Field::new("contact_data", DataType::Utf8, true), 
        ])
    }

    fn to_record_batch(records: &[Self]) -> Result<RecordBatch, AppError> {
        let schema = Arc::new(Self::schema());
        let ticket_nos = records.iter().map(|r| r.ticket_no.as_str()).collect::<Vec<_>>();
        let book_refs = records.iter().map(|r| r.book_ref.as_deref()).collect::<Vec<_>>();
        let passenger_ids = records.iter().map(|r| r.passenger_id.as_deref()).collect::<Vec<_>>();
        let passenger_names = records.iter().map(|r| r.passenger_name.as_deref()).collect::<Vec<_>>();
        let contact_data_all = records
            .iter()
            .map(|r| 
                r.contact_data
                .as_ref()
                .map(|val| serde_json::to_string(val))
                .transpose()
            )
            .collect::<Result<Vec<_>, serde_json::Error>>()?;

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ticket_nos)),
                Arc::new(StringArray::from(book_refs)),
                Arc::new(StringArray::from(passenger_ids)), 
                Arc::new(StringArray::from(passenger_names)),
                Arc::new(StringArray::from(contact_data_all)),
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
impl TableWorkerDyn for Tickets {
    async fn query_table(&self, pool: &PgPool, query: &str) -> Result<(), AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);
        Ok(())
    }

    async fn query_table_to_string(&self, pool: &PgPool, query: &str) -> Result<Vec<String>, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query(&query);
        let data: Vec<PgRow> = query.fetch_all(pool).await?;
        let rows: Vec<String> = data
            .iter()
            .map(|row| format!("ticket_no: {}, book_ref: {}, passenger_id: {}, passenger_name: {}, contact_data: {}", 
                row.get::<String, _>("ticket_no"), 
                row.get::<String, _>("book_ref"), 
                row.get::<String, _>("passenger_id"),
                row.get::<String, _>("passenger_name"),
                row.get::<Value, _>("contact_data")),
            )
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

    async fn query_table_to_df(&self, pool: &PgPool, query: &str, ctx: &SessionContext) -> Result<DataFrame, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let records = query.fetch_all(pool).await?;
        let df = Self::to_df(ctx, &records)?;
        Ok(df)
    }
}

#[async_trait]
impl TableWorkerStatic for Tickets {
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
            .map(|row| format!("ticket_no: {}, book_ref: {}, passenger_id: {}, passenger_name: {}, contact_data: {}", 
                row.get::<String, _>("ticket_no"), 
                row.get::<String, _>("book_ref"), 
                row.get::<String, _>("passenger_id"),
                row.get::<String, _>("passenger_name"),
                row.get::<Value, _>("contact_data")),
            )
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

    async fn query_table_to_df(pool: &PgPool, query: &str, ctx: &SessionContext) -> Result<DataFrame, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let records = query.fetch_all(pool).await?;
        let df = Self::to_df(ctx, &records)?;
        Ok(df)
    }
}
