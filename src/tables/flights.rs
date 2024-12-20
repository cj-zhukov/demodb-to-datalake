use crate::{AppError, MAX_ROWS, TableWorker, FLIGHTS_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use sqlx::types::chrono::{DateTime, Utc};
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};

#[derive(Debug, Default, FromRow)]
pub struct Flights {
    pub flight_id: i32,
    pub flight_no: Option<String>,
    pub scheduled_departure: Option<DateTime<Utc>>,
    pub scheduled_arrival: Option<DateTime<Utc>>,
    pub departure_airport: Option<String>,
    pub arrival_airport: Option<String>,
    pub status: Option<String>,
    pub aircraft_code: Option<String>,
    pub actual_departure: Option<DateTime<Utc>>,
    pub actual_arrival: Option<DateTime<Utc>>,
}

impl Serialize for Flights {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer 
    {
        let scheduled_departure = self.scheduled_departure.map(|val| val.to_rfc3339());
        let scheduled_arrival = self.scheduled_arrival.map(|val| val.to_rfc3339());
        let actual_departure = self.actual_departure.map(|val| val.to_rfc3339());
        let actual_arrival = self.actual_arrival.map(|val| val.to_rfc3339());

        serde_json::json!({
            "flight_id": self.flight_id, "flight_no": self.flight_no, "scheduled_departure": scheduled_departure,
            "scheduled_arrival": scheduled_arrival, "departure_airport": self.departure_airport, "arrival_airport": self.arrival_airport,
            "status": self.status, "aircraft_code": self.aircraft_code, "actual_departure": actual_departure, "actual_arrival": actual_arrival,
        }).serialize(serializer)
    }
}

impl Flights {
    pub fn new() -> Self {
        Flights::default()
    }

    pub fn table_name() -> String {
        FLIGHTS_TABLE_NAME.to_string()
    }
}

impl Flights {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("flight_id", DataType::Int32, false),
            Field::new("flight_no", DataType::Utf8, true),
            Field::new("scheduled_departure", DataType::Utf8, true),
            Field::new("scheduled_arrival", DataType::Utf8, true),
            Field::new("departure_airport", DataType::Utf8, true),
            Field::new("arrival_airport", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
            Field::new("aircraft_code", DataType::Utf8, true),
            Field::new("actual_departure", DataType::Utf8, true),
            Field::new("actual_arrival", DataType::Utf8, true),
        ])
    }

    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame, AppError> {
        let mut flight_ids = Vec::new();
        let mut flight_nos = Vec::new();
        let mut scheduled_departures = Vec::new();
        let mut scheduled_arrivals = Vec::new();
        let mut departure_airports = Vec::new();
        let mut arrival_airports = Vec::new();
        let mut statuses = Vec::new();
        let mut aircraft_codes = Vec::new();
        let mut actual_departures = Vec::new();
        let mut actual_arrivals = Vec::new();

        for record in records {
            flight_ids.push(record.flight_id);
            flight_nos.push(record.flight_no.clone());
            let scheduled_departure = record.scheduled_departure.as_mut().map(|val| val.to_rfc3339());
            scheduled_departures.push(scheduled_departure);
            let scheduled_arrival = record.scheduled_arrival.as_mut().map(|val| val.to_rfc3339());
            scheduled_arrivals.push(scheduled_arrival);
            departure_airports.push(record.departure_airport.clone());
            arrival_airports.push(record.arrival_airport.clone());
            statuses.push(record.status.clone());
            aircraft_codes.push(record.aircraft_code.clone());
            let actual_departure = record.actual_departure.as_mut().map(|val| val.to_rfc3339());
            actual_departures.push(actual_departure);
            let actual_arrival = record.actual_arrival.as_mut().map(|val| val.to_rfc3339());
            actual_arrivals.push(actual_arrival);
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(Int32Array::from(flight_ids)), 
                Arc::new(StringArray::from(flight_nos)),
                Arc::new(StringArray::from(scheduled_departures)),
                Arc::new(StringArray::from(scheduled_arrivals)),
                Arc::new(StringArray::from(departure_airports)),
                Arc::new(StringArray::from(arrival_airports)),
                Arc::new(StringArray::from(statuses)),
                Arc::new(StringArray::from(aircraft_codes)),
                Arc::new(StringArray::from(actual_departures)),
                Arc::new(StringArray::from(actual_arrivals)),
            ],
        )?;
        let df = ctx.read_batch(batch)?;

        Ok(df)
    }
}

#[async_trait]
impl TableWorker for Flights {
    async fn query_table(&self, pool: &PgPool) -> Result<(), AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);

        Ok(())
    }

    async fn query_table_to_string(&self, pool: &PgPool) -> Result<Vec<String>, AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
        let query = sqlx::query(&sql);
        let data: Vec<PgRow> = query.fetch_all(pool).await?;
    
        let rows: Vec<String> = data
            .iter()
            .map(|row| format!("flight_id: {}, flight_no: {}, scheduled_departure: {}, scheduled_arrival: {}, departure_airport: {} \
            arrival_airport: {}, status: {}, aircraft_code: {}, actual_departure: {:?}, actual_arrival: {:?}", 
                row.get::<i32, _>("flight_id"), 
                row.get::<String, _>("flight_no"), 
                row.get::<DateTime<Utc>, _>("scheduled_departure"),
                row.get::<DateTime<Utc>, _>("scheduled_arrival"),
                row.get::<String, _>("departure_airport"),
                row.get::<String, _>("arrival_airport"),
                row.get::<String, _>("status"),
                row.get::<String, _>("aircraft_code"),
                row.get::<Option<DateTime<Utc>>, _>("actual_departure"),
                row.get::<Option<DateTime<Utc>>, _>("actual_arrival"),
            ))
            .collect();
    
        Ok(rows)
    }

    async fn query_table_to_df(&self, pool: &PgPool) -> Result<DataFrame, AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let mut records = query.fetch_all(pool).await?;
        let ctx = SessionContext::new();
        let df = Self::to_df(ctx, &mut records)?;

        Ok(df)
    }

    async fn query_table_to_json(&self, pool: &PgPool) -> Result<String, AppError> {
        let sql = format!("select * from {} limit {}", Self::table_name(), MAX_ROWS);
        let query = sqlx::query_as::<_, Self>(&sql);
        let data = query.fetch_all(pool).await?;
        let res = serde_json::to_string(&data)?;
        
        Ok(res)
    }
}
