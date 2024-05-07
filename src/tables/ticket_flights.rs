use crate::{Result, MAX_ROWS, TableWorker, TICKET_FLIGHTS_TABLE_NAME};

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{postgres::PgRow, FromRow, Row, PgPool};
use sqlx::types::Decimal;
use datafusion::prelude::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};

#[derive(Debug, Default, FromRow)]
pub struct TicketFlights {
    pub ticket_no: String,
    pub flight_id: Option<i32>,
    pub fare_conditions: Option<String>,
    pub amount: Option<Decimal>,
}

impl TicketFlights {
    pub fn new() -> Self {
        TicketFlights::default()
    }

    pub fn table_name() -> String {
        TICKET_FLIGHTS_TABLE_NAME.to_string()
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

    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame> {
        let mut ticket_nos = Vec::new();
        let mut flight_ids = Vec::new();
        let mut fare_conditionses = Vec::new();
        let mut amounts: Vec<Option<String>> = Vec::new();

        for record in records {
            ticket_nos.push(record.ticket_no.clone());
            flight_ids.push(record.flight_id);
            fare_conditionses.push(record.fare_conditions.clone());
            amounts.push(None); 
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(StringArray::from(ticket_nos)),
                Arc::new(Int32Array::from(flight_ids)), 
                Arc::new(StringArray::from(fare_conditionses)),
                Arc::new(StringArray::from(amounts)),
            ],
        ).map_err(|e| format!("failed creating batch for table: {} cause: {}", Self::table_name(), e))?;
    
        let df = ctx.read_batch(batch)
            .map_err(|e| format!("failed creating dataframe for table: {} cause: {}", Self::table_name(), e))?;

        Ok(df)
    }
}


#[async_trait]
impl TableWorker for TicketFlights {
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
            .map(|row| format!("ticket_no: {} flight_id: {} fare_conditions: {} amount: {}", 
                row.get::<String, _>("ticket_no"), 
                row.get::<i32, _>("flight_id"), 
                row.get::<String, _>("fare_conditions"),
                row.get::<Decimal, _>("amount"))
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
