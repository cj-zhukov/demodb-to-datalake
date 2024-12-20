use crate::{tables, AppError, utils::*};
use tables::{aircrafts_data, airports_data, boarding_passes, bookings, flights, seats, ticket_flights, tickets};

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

    pub fn name(&self) -> &str {
        match *self {
            Table::AircraftDataTable => AIRCRAFTS_DATA_TABLE_NAME,
            Table::AirportsDataTable => AIRPORTS_DATA_TABLE_NAME,
            Table::BoardingPassesTable => BOARDING_PASSES_TABLE_NAME,
            Table::BookingsTable => BOOKINGS_TABLE_NAME,
            Table::FlightsTable => FLIGHTS_TABLE_NAME,
            Table::SeatsTable => SEATS_TABLE_NAME,
            Table::TicketsTable => TICKETS_TABLE_NAME,
            Table::TicketFlightsTable => TICKET_FLIGHTS_TABLE_NAME,
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
    async fn query_table(&self, pool: &PgPool) -> Result<(), AppError>;
    async fn query_table_to_string(&self, pool: &PgPool) -> Result<Vec<String>, AppError>;
    async fn query_table_to_json(&self, pool: &PgPool) -> Result<String, AppError>;
    async fn query_table_to_df(&self, pool: &PgPool) -> Result<DataFrame, AppError>;
}