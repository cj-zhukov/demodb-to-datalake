use crate::{Result, MAX_ROWS, TableWorker, TICKETS_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use sqlx::types::Json;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{RecordBatch, StringArray};

#[derive(Debug, Default, FromRow)]
pub struct Tickets {
    pub book_ref: String,
    pub passenger_id: Option<String>,
    pub passenger_name: Option<String>,
    pub contact_data: Option<Json<ContactData>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContactData {
    pub email: Option<String>,
    pub phone: Option<String>,
}

impl Tickets {
    pub fn new() -> Self {
        Tickets::default()
    }

    pub fn table_name() -> String {
        TICKETS_TABLE_NAME.to_string()
    }
}

impl Tickets {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("book_ref", DataType::Utf8, false),
            Field::new("passenger_id", DataType::Utf8, true),
            Field::new("passenger_name", DataType::Utf8, true),
            Field::new("contact_data", DataType::Utf8, true), 
        ])
    }

    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame> {
        let mut book_refs = Vec::new();
        let mut passenger_ids = Vec::new();
        let mut passenger_names= Vec::new();
        let mut contact_datas = Vec::new();

        for record in records {
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
                Arc::new(StringArray::from(book_refs)),
                Arc::new(StringArray::from(passenger_ids)), 
                Arc::new(StringArray::from(passenger_names)),
                Arc::new(StringArray::from(contact_datas)),
            ],
        ).map_err(|e| format!("failed creating batch for table: {} cause: {}", Self::table_name(), e))?;
    
        let df = ctx.read_batch(batch)
            .map_err(|e| format!("failed creating dataframe for table: {} cause: {}", Self::table_name(), e))?;

        Ok(df)
    }
}

#[async_trait]
impl TableWorker for Tickets {
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
            .map(|row| format!("book_ref: {} passenger_id: {} passenger_name: {} contact_data: {}", 
                row.get::<String, _>("book_ref"), 
                row.get::<String, _>("passenger_id"), 
                row.get::<String, _>("passenger_name"),
                row.get::<Value, _>("contact_data"))
            )
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
