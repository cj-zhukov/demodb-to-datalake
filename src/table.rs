use crate::{tables, AppError, utils::*};
use tables::*;

use async_trait::async_trait;
use datafusion::prelude::*;
use sqlx::PgPool;

pub enum Table {
    AircraftDataTable, 
    AirportsDataTable,
    BoardingPassesTable, 
    BookingsTable, 
    FlightsTable, 
    SeatsTable, 
    TicketsTable,
    TicketFlightsTable, 
}

impl AsRef<str> for Table {
    fn as_ref(&self) -> &str {
        match *self {
            Self::AircraftDataTable => AIRCRAFTS_DATA_TABLE_NAME,
            Self::AirportsDataTable => AIRPORTS_DATA_TABLE_NAME,
            Self::BoardingPassesTable => BOARDING_PASSES_TABLE_NAME,
            Self::BookingsTable => BOOKINGS_TABLE_NAME,
            Self::FlightsTable => FLIGHTS_TABLE_NAME,
            Self::SeatsTable => SEATS_TABLE_NAME,
            Self::TicketsTable => TICKETS_TABLE_NAME,
            Self::TicketFlightsTable => TICKET_FLIGHTS_TABLE_NAME,
        }
    }
}

impl Table {
    pub fn new(name: &str) -> Option<Self> {
        match name {
            AIRCRAFTS_DATA_TABLE_NAME => Some(Self::AircraftDataTable),
            AIRPORTS_DATA_TABLE_NAME => Some(Self::AirportsDataTable),
            BOARDING_PASSES_TABLE_NAME => Some(Self::BoardingPassesTable),
            BOOKINGS_TABLE_NAME => Some(Self::BookingsTable),
            FLIGHTS_TABLE_NAME => Some(Self::FlightsTable),
            SEATS_TABLE_NAME => Some(Self::SeatsTable),
            TICKETS_TABLE_NAME => Some(Self::TicketsTable),
            TICKET_FLIGHTS_TABLE_NAME => Some(Self::TicketFlightsTable),
            _ => None, 
        }
    }

    pub fn to_worker(&self) -> Box<dyn TableWorker> {
        match *self {
            Self::AircraftDataTable => Box::new(AircraftsData::new()),
            Self::AirportsDataTable => Box::new(AirportsData::new()),
            Self::BoardingPassesTable => Box::new(BoardingPasses::new()), 
            Self::BookingsTable => Box::new(Bookings::new()),
            Self::FlightsTable => Box::new(Flights::new()),
            Self::SeatsTable => Box::new(Seats::new()),
            Self::TicketsTable => Box::new(Tickets::new()),
            Self::TicketFlightsTable => Box::new(TicketFlights::new()),
        }
    } 
}

#[async_trait]
pub trait TableWorker {
    async fn query_table(&self, pool: &PgPool) -> Result<(), AppError>;
    async fn query_table_to_string(&self, pool: &PgPool) -> Result<Vec<String>, AppError>;
    async fn query_table_to_json(&self, pool: &PgPool) -> Result<String, AppError>;
    async fn query_table_to_df(&self, pool: &PgPool, query: Option<&str>) -> Result<DataFrame, AppError>;
}