use crate::{AppError, table_worker::TableWorker, MAX_ROWS, AIRPORTS_DATA_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use sqlx::types::Json;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{RecordBatch, StringArray};

#[derive(Debug, Default, Deserialize, Serialize, FromRow)]
pub struct AirportsData {
    pub airport_code: String,
    pub airport_name: Option<Json<AirportName>>,
    pub city: Option<Json<City>>,
    pub coordinates: Option<Json<Coordinates>>, 
    pub timezone: Option<String>,
}

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

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Coordinates {
    pub x: f64,
    pub y: f64,
}

impl AsRef<str> for AirportsData {
    fn as_ref(&self) -> &str {
        AIRPORTS_DATA_TABLE_NAME
    }
}

impl AirportsData {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AirportsData {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("airport_code", DataType::Utf8, false),
            Field::new("airport_name", DataType::Utf8, true),
            Field::new("city", DataType::Utf8, true),
            Field::new("coordinates", DataType::Utf8, true),
            Field::new("timezone", DataType::Utf8, true),
        ])
    }

    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame, AppError> {
        let mut airport_codes = Vec::new();
        let mut airport_names = Vec::new();
        let mut cities= Vec::new();
        let mut coordinates_all = Vec::new();
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

            let coordinates = match &mut record.coordinates {
                Some(val) => {
                    Some(serde_json::to_string(&val)?)
                },
                None => None
            };
            coordinates_all.push(coordinates);

            timezones.push(record.timezone.clone());
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(StringArray::from(airport_codes)), 
                Arc::new(StringArray::from(airport_names)),
                Arc::new(StringArray::from(cities)),
                Arc::new(StringArray::from(coordinates_all)),
                Arc::new(StringArray::from(timezones)),
            ],
        )?;
        let df = ctx.read_batch(batch)?;

        Ok(df)
    }
}

#[async_trait]
impl TableWorker for AirportsData {
    async fn query_table(&self, pool: &PgPool) -> Result<(), AppError> {
        let sql = format!("select 
                                        airport_code, 
                                        airport_name, 
                                        city, 
                                        json_build_object('x', coordinates[0], 'y', coordinates[1]) as coordinates, 
                                        timezone 
                                    from {} limit {}", self.as_ref(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);

        Ok(())
    }

    async fn query_table_to_string(&self, pool: &PgPool) -> Result<Vec<String>, AppError> {
        let sql = format!("select 
                                        airport_code, 
                                        airport_name, 
                                        city, 
                                        json_build_object('x', coordinates[0], 'y', coordinates[1]) as coordinates, 
                                        timezone 
                                    from {} limit {}", self.as_ref(), MAX_ROWS);
        let query = sqlx::query(&sql);
        let data: Vec<PgRow> = query.fetch_all(pool).await?;
    
        let rows: Vec<String> = data
            .iter()
            .map(|row| format!("airport_code: {}, airport_name: {}, city: {}, coordinates: {}, timezone: {}", 
                row.get::<String, _>("airport_code"), 
                row.get::<Value, _>("airport_name"), 
                row.get::<Value, _>("city"),
                row.get::<Value, _>("coordinates"),
                row.get::<String, _>("timezone"),
            ))
            .collect();
    
        Ok(rows)
    }

    async fn query_table_to_df(&self, pool: &PgPool, query: Option<&str>) -> Result<DataFrame, AppError> {
        let sql = match query {
            None => format!("select 
                            airport_code, 
                            airport_name, 
                            city, 
                            json_build_object('x', coordinates[0], 'y', coordinates[1]) as coordinates, 
                            timezone 
                        from {} limit {}", self.as_ref(), MAX_ROWS),
            Some(sql) => sql.to_string(),
        };
        let query = sqlx::query_as::<_, Self>(&sql);
        let mut records = query.fetch_all(pool).await?;
        let ctx = SessionContext::new();
        let df = Self::to_df(ctx, &mut records)?;

        Ok(df)
    }

    async fn query_table_to_json(&self, pool: &PgPool) -> Result<String, AppError> {
        let sql = format!("select 
                                        airport_code, 
                                        airport_name, 
                                        city, 
                                        json_build_object('x', coordinates[0], 'y', coordinates[1]) as coordinates, 
                                        timezone 
                                    from {} limit {}", self.as_ref(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        let res = serde_json::to_string(&data)?;
        
        Ok(res)
    }
}