use crate::table_worker::{TableWorkerDyn, TableWorkerStatic};
use crate::{prepare_query, AppError, AIRPORTS_DATA_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::postgres::PgTypeInfo;
use sqlx::prelude::Type;
use sqlx::{Decode, Postgres};
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use sqlx::types::Json;
use sqlx::postgres::types::PgPoint;
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
    pub coordinates: Option<SerPgPoint>,
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

#[derive(Debug, FromRow)]
pub struct SerPgPoint(PgPoint);

impl Serialize for SerPgPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let point = &self.0;
        let json = serde_json::json!({ "x": point.x, "y": point.y });
        json.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SerPgPoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Point {
            x: f64,
            y: f64,
        }

        let point = Point::deserialize(deserializer)?;
        Ok(SerPgPoint(PgPoint { x: point.x, y: point.y }))
    }
}

// For PgTypeInfo
impl Type<Postgres> for SerPgPoint {
    fn type_info() -> PgTypeInfo {
        PgPoint::type_info()
    }
}

// For Decode
impl<'r> Decode<'r, Postgres> for SerPgPoint {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        PgPoint::decode(value).map(SerPgPoint)
    }
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

    fn to_record_batch(records: &[Self]) -> Result<RecordBatch, AppError> {
        let schema = Arc::new(Self::schema());
        let airport_codes = records.iter().map(|r| r.airport_code.as_str()).collect::<Vec<_>>();
        let airport_names = records
            .iter()
            .map(|r| 
                r.airport_name
                .as_ref()
                .map(|val| serde_json::to_string(val))
                .transpose()
            )
            .collect::<Result<Vec<_>, serde_json::Error>>()?;
        let cities = records
            .iter()
            .map(|r| 
                r.city
                .as_ref()
                .map(|val| serde_json::to_string(val))
                .transpose()
            )
            .collect::<Result<Vec<_>, serde_json::Error>>()?;
        let coordinates_all = records
            .iter()
            .map(|r| 
                r.coordinates
                .as_ref()
                .map(|val| serde_json::to_string(val))
                .transpose()
            )
            .collect::<Result<Vec<_>, serde_json::Error>>()?;
        let timezones = records.iter().map(|r| r.timezone.as_deref()).collect::<Vec<_>>();
        
        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(airport_codes)), 
                Arc::new(StringArray::from(airport_names)),
                Arc::new(StringArray::from(cities)),
                Arc::new(StringArray::from(coordinates_all)),
                Arc::new(StringArray::from(timezones)),
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
impl TableWorkerDyn for AirportsData {
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
            .map(|row| format!("airport_code: {}, airport_name: {}, city: {}, coordinates: {:?}, timezone: {}", 
                row.get::<String, _>("airport_code"), 
                row.get::<Value, _>("airport_name"), 
                row.get::<Value, _>("city"),
                row.get::<PgPoint, _>("coordinates"),
                row.get::<String, _>("timezone"),
            ))
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
impl TableWorkerStatic for AirportsData {
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
            .map(|row| format!("airport_code: {}, airport_name: {}, city: {}, coordinates: {:?}, timezone: {}", 
                row.get::<String, _>("airport_code"), 
                row.get::<Value, _>("airport_name"), 
                row.get::<Value, _>("city"),
                row.get::<PgPoint, _>("coordinates"),
                row.get::<String, _>("timezone"),
            ))
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