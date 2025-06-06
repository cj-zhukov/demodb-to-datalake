use crate::table_worker::{TableWorkerDyn, TableWorkerStatic};
use crate::{prepare_query, AppError, TICKET_FLIGHTS_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use serde::Serialize;
use sqlx::types::Decimal;
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};

#[derive(Debug, Default, FromRow)]
pub struct TicketFlights {
    pub ticket_no: String,
    pub flight_id: Option<i32>,
    pub fare_conditions: Option<String>,
    pub amount: Option<Decimal>,
}

impl Serialize for TicketFlights {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let amount = self.amount.map(|val| val.to_string());

        serde_json::json!({ "ticket_no": self.ticket_no, "flight_id": self.flight_id, "fare_conditions": self.fare_conditions, "amount": amount})
            .serialize(serializer)
    }
}

impl AsRef<str> for TicketFlights {
    fn as_ref(&self) -> &str {
        TICKET_FLIGHTS_TABLE_NAME
    }
}

impl TicketFlights {
    pub fn new() -> Self {
        TicketFlights::default()
    }
}

impl TicketFlights {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("ticket_no", DataType::Utf8, false),
            Field::new("flight_id", DataType::Int32, true),
            Field::new("fare_conditions", DataType::Utf8, true),
            Field::new("amount", DataType::Utf8, true),
        ])
    }

    fn to_record_batch(records: &[Self]) -> Result<RecordBatch, AppError> {
        let schema = Arc::new(Self::schema());
        let ticket_nos = records
            .iter()
            .map(|r| r.ticket_no.as_str())
            .collect::<Vec<_>>();
        let flight_ids = records.iter().map(|r| r.flight_id).collect::<Vec<_>>();
        let fare_conditions_all = records
            .iter()
            .map(|r| r.fare_conditions.as_deref())
            .collect::<Vec<_>>();
        let amounts = records
            .iter()
            .map(|r| r.amount.as_ref().map(|val| val.to_string()))
            .collect::<Vec<_>>();

        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ticket_nos)),
                Arc::new(Int32Array::from(flight_ids)),
                Arc::new(StringArray::from(fare_conditions_all)),
                Arc::new(StringArray::from(amounts)),
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
impl TableWorkerDyn for TicketFlights {
    async fn query_table(&self, pool: &PgPool, query: &str) -> Result<(), AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let data = query.fetch_all(pool).await?;
        println!("{:?}", data);
        Ok(())
    }

    async fn query_table_to_string(
        &self,
        pool: &PgPool,
        query: &str,
    ) -> Result<Vec<String>, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query(&query);
        let data: Vec<PgRow> = query.fetch_all(pool).await?;
        let rows: Vec<String> = data
            .iter()
            .map(|row| {
                format!(
                    "ticket_no: {}, flight_id: {}, fare_conditions: {}, amount: {}",
                    row.get::<String, _>("ticket_no"),
                    row.get::<i32, _>("flight_id"),
                    row.get::<String, _>("fare_conditions"),
                    row.get::<Decimal, _>("amount")
                )
            })
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

    async fn query_table_to_df(
        &self,
        pool: &PgPool,
        query: &str,
        ctx: &SessionContext,
    ) -> Result<DataFrame, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let records = query.fetch_all(pool).await?;
        let df = Self::to_df(ctx, &records)?;
        Ok(df)
    }
}

#[async_trait]
impl TableWorkerStatic for TicketFlights {
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
            .map(|row| {
                format!(
                    "ticket_no: {}, flight_id: {}, fare_conditions: {}, amount: {}",
                    row.get::<String, _>("ticket_no"),
                    row.get::<i32, _>("flight_id"),
                    row.get::<String, _>("fare_conditions"),
                    row.get::<Decimal, _>("amount")
                )
            })
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

    async fn query_table_to_df(
        pool: &PgPool,
        query: &str,
        ctx: &SessionContext,
    ) -> Result<DataFrame, AppError> {
        let query = prepare_query(query)?;
        let query = sqlx::query_as::<_, Self>(&query);
        let records = query.fetch_all(pool).await?;
        let df = Self::to_df(ctx, &records)?;
        Ok(df)
    }
}
