use crate::{AppError, table_worker::TableWorker, MAX_ROWS, TICKETS_TABLE_NAME};

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

    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame, AppError> {
        let mut ticket_nos = Vec::new();
        let mut book_refs = Vec::new();
        let mut passenger_ids = Vec::new();
        let mut passenger_names= Vec::new();
        let mut contact_datas = Vec::new();

        for record in records {
            ticket_nos.push(record.ticket_no.clone());
            book_refs.push(record.book_ref.clone());
            passenger_ids.push(record.passenger_id.clone());
            passenger_names.push(record.passenger_name.clone());
            let contact_data = match &mut record.contact_data {
                Some(val) => {
                    Some(serde_json::to_string(&val)?)
                },
                None => None
            };
            contact_datas.push(contact_data);
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(StringArray::from(ticket_nos)),
                Arc::new(StringArray::from(book_refs)),
                Arc::new(StringArray::from(passenger_ids)), 
                Arc::new(StringArray::from(passenger_names)),
                Arc::new(StringArray::from(contact_datas)),
            ],
        )?;
        let df = ctx.read_batch(batch)?;

        Ok(df)
    }
}

#[async_trait]
impl TableWorker for Tickets {
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

    async fn query_table_to_df(&self, pool: &PgPool, query: Option<&str>) -> Result<DataFrame, AppError> {
        let sql = match query {
            None => format!("select * from {} limit {}", self.as_ref(), MAX_ROWS),
            Some(sql) => sql.to_string(),
        };
        let query = sqlx::query_as::<_, Self>(&sql);
        let mut records = query.fetch_all(pool).await?;
        let ctx = SessionContext::new();
        let df = Self::to_df(ctx, &mut records)?;

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