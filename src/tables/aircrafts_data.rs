use crate::{Result, MAX_ROWS, TableWorker, AIRCRAFTS_DATA_TABLE_NAME};

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use sqlx::types::Json;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};

#[derive(Debug, Default, FromRow)]
pub struct AircraftData {
    pub aircraft_code: String,
    pub model: Option<Json<Model>>,
    pub range: Option<i32>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Model {
    pub en: Option<String>,
    pub ru: Option<String>,
}

impl AircraftData {
    pub fn new() -> Self {
        AircraftData::default()
    }

    pub fn table_name() -> String {
        AIRCRAFTS_DATA_TABLE_NAME.to_string()
    }
}

impl AircraftData {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("aircraft_code", DataType::Utf8, false),
            Field::new("model", DataType::Utf8, true),
            Field::new("range", DataType::Int32, true),
        ])
    }

    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame> {
        let mut aircraft_codes = Vec::new();
        let mut models = Vec::new();
        let mut ranges= Vec::new();

        for record in records {
            aircraft_codes.push(record.aircraft_code.clone());
            let model = match &mut record.model {
                Some(v) => {
                    Some(serde_json::to_string(&v)?)
                }
                None => None
            };
            models.push(model);
            ranges.push(record.range);
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(StringArray::from(aircraft_codes)), 
                Arc::new(StringArray::from(models)),
                Arc::new(Int32Array::from(ranges)),
            ],
        )?;
        let df = ctx.read_batch(batch)?;

        Ok(df)
    }
}

#[async_trait]
impl TableWorker for AircraftData {
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
            .map(|row| format!("aircraft_code: {}, model: {}, range: {}", 
                row.get::<String, _>("aircraft_code"), 
                row.get::<Value, _>("model"), 
                row.get::<i32, _>("range"),
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
