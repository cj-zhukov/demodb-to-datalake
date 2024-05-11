pub type Result<T> = core::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error>;

pub mod config;
pub mod tables;
use tables::{aircrafts_data, airports_data, boarding_passes, bookings, flights, seats, ticket_flights, tickets};

use async_trait::async_trait;
use sqlx::PgPool;
use datafusion::prelude::*;

const MAX_ROWS: u32 = 10;
pub const AIRCRAFTS_DATA_TABLE_NAME: &str = "aircrafts_data";
pub const AIRPORTS_DATA_TABLE_NAME: &str = "airports_data";
pub const BOARDING_PASSES_TABLE_NAME: &str = "boarding_passes";
pub const BOOKINGS_TABLE_NAME: &str = "bookings";
pub const FLIGHTS_TABLE_NAME: &str = "flights";
pub const SEATS_TABLE_NAME: &str = "seats";
pub const TICKETS_TABLE_NAME: &str = "tickets";
pub const TICKET_FLIGHTS_TABLE_NAME: &str = "ticket_flights";

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
            Self::AircraftDataTable => Box::new(aircrafts_data::AircraftData::new()),
            Self::AirportsDataTable => Box::new(airports_data::AirportsData::new()),
            Self::BoardingPassesTable => Box::new(boarding_passes::BoardingPasses::new()), 
            Self::BookingsTable => Box::new(bookings::Bookings::new()),
            Self::FlightsTable => Box::new(flights::Flights::new()),
            Self::SeatsTable => Box::new(seats::Seats::new()),
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
