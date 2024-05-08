use crate::{Result, MAX_ROWS, TableWorker, AIRPORTS_DATA_TABLE_NAME};

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
pub struct AirportsData {
    pub airport_code: String,
    pub airport_name: Option<Json<AirportName>>,
    pub city: Option<Json<City>>,
    // pub coordinates: Option<Point>,
    pub timezone: Option<String>,
}

// #[derive(Debug, Serialize, Deserialize, FromRow)]
// pub struct Point {
//     pub x: Option<f64>,
//     pub y: Option<f64>,
// }

#[derive(Debug, Serialize, Deserialize)]
pub struct AirportName {
    pub en: Option<String>,
    pub ru: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct City {
    pub en: Option<String>,
    pub ru: Option<String>,
}

impl AirportsData {
    pub fn new() -> Self {
        AirportsData::default()
    }

    pub fn table_name() -> String {
        AIRPORTS_DATA_TABLE_NAME.to_string()
    }
}

impl AirportsData {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("airport_code", DataType::Utf8, false),
            Field::new("airport_name", DataType::Utf8, true),
            Field::new("city", DataType::Utf8, true),
            // Field::new("coordinates", DataType::Struct(vec![DataType::Float64, DataType::Float64]), true), // point data type
            // Field::new("coordinates", DataType::Utf8, true),
            Field::new("timezone", DataType::Utf8, true),
        ])
    }

    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame> {
        let mut airport_codes = Vec::new();
        let mut airport_names = Vec::new();
        let mut cities= Vec::new();
        // let mut coordinates_all = Vec::new();
        let mut timezones = Vec::new();

        for record in records {
            airport_codes.push(record.airport_code.clone());
            let airport_name = match &mut record.airport_name {
                Some(val) => {
                    Some(serde_json::to_string(&val)?)
                },
                None => None
            };
            airport_names.push(airport_name);
            let city = match &mut record.city {
                Some(val) => {
                    Some(serde_json::to_string(&val)?)
                },
                None => None
            };
            cities.push(city);
            // coordinates_all.push(None);
            // match &mut record.coordinates {
            //     Some(val) => {
            //         xs.push(Some(val.x));
            //         ys.push(Some(val.y));
            //     },
            //     None => {
            //         xs.push(None);
            //         ys.push(None);   
            //     }
            // };
            // let coordinates = match &record.coordinates {
            //     Some(val) => {
            //         Some(format!("x: {:?}, y: {:?}", val.x, val.y))
            //     },
            //     None => None
            // };
            // coordinates_all.push(coordinates);

            timezones.push(record.timezone.clone());
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(StringArray::from(airport_codes)), 
                Arc::new(StringArray::from(airport_names)),
                Arc::new(StringArray::from(cities)),
                // Arc::new(StringArray::from(coordinates_all)),
                Arc::new(StringArray::from(timezones)),
            ],
        ).map_err(|e| format!("failed creating batch for table: {} cause: {}", Self::table_name(), e))?;
    
        let df = ctx.read_batch(batch)
            .map_err(|e| format!("failed creating dataframe for table: {} cause: {}", Self::table_name(), e))?;

        Ok(df)
    }
}

#[async_trait]
impl TableWorker for AirportsData {
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
