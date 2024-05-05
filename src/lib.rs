pub type Result<T> = core::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error>;

pub mod config;
pub mod tables;
use tables::{tickets, ticket_flights};

use async_trait::async_trait;
use sqlx::PgPool;
use datafusion::prelude::*;

const MAX_ROWS: u32 = 10;
pub const TICKETS_TABLE_NAME: &str = "tickets";
pub const TICKET_FLIGHTS_TABLE_NAME: &str = "ticket_flights";

pub enum Table {
    TicketsTable,
    TicketFlightsTable, 
}

impl Table {
    pub fn new(name: &str) -> Option<Self> {
        match name {
            TICKETS_TABLE_NAME => Some(Self::TicketsTable),
            TICKET_FLIGHTS_TABLE_NAME => Some(Self::TicketFlightsTable),
            _ => None, 
        }
    }

    pub fn to_worker(&self) -> Box<dyn TableWorker> {
        match *self {
            Self::TicketsTable => Box::new(tickets::Tickets::new()),
            Self::TicketFlightsTable => Box::new(ticket_flights::TicketFlights::new()),
        }
    } 
}

#[async_trait]
pub trait TableWorker {
    async fn query_table(&self, pool: &PgPool) -> Result<()>;
    async fn query_table_to_string(&self, pool: &PgPool) -> Result<Vec<String>>;
    async fn query_table_to_df(&self, pool: &PgPool) -> Result<DataFrame>;
}