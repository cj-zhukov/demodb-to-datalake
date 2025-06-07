use datafusion::prelude::{DataFrame, SessionContext};
use sqlx::PgPool;

use crate::table_worker::helpers::*;
use crate::{table_worker::TableWorkerDyn, utils::*};
use crate::{tables::*, AppError};

#[derive(Debug, PartialEq)]
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
}

// Dynamic dispatch
impl Table {
    pub fn to_worker(&self) -> Box<dyn TableWorkerDyn> {
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

// Static dispatch
impl Table {
    pub async fn run_query_table(&self, pool: &PgPool, query: &str) -> Result<(), AppError> {
        match *self {
            Self::AircraftDataTable => process_query_table::<AircraftsData>(pool, query).await,
            Self::AirportsDataTable => process_query_table::<AirportsData>(pool, query).await,
            Self::BoardingPassesTable => process_query_table::<BoardingPasses>(pool, query).await,
            Self::BookingsTable => process_query_table::<Bookings>(pool, query).await,
            Self::FlightsTable => process_query_table::<Flights>(pool, query).await,
            Self::SeatsTable => process_query_table::<Seats>(pool, query).await,
            Self::TicketsTable => process_query_table::<Tickets>(pool, query).await,
            Self::TicketFlightsTable => process_query_table::<TicketFlights>(pool, query).await,
        }
    }

    pub async fn run_query_table_to_string(
        &self,
        pool: &PgPool,
        query: &str,
    ) -> Result<Vec<String>, AppError> {
        match *self {
            Self::AircraftDataTable => process_table_to_string::<AircraftsData>(pool, query).await,
            Self::AirportsDataTable => process_table_to_string::<AirportsData>(pool, query).await,
            Self::BoardingPassesTable => {
                process_table_to_string::<BoardingPasses>(pool, query).await
            }
            Self::BookingsTable => process_table_to_string::<Bookings>(pool, query).await,
            Self::FlightsTable => process_table_to_string::<Flights>(pool, query).await,
            Self::SeatsTable => process_table_to_string::<Seats>(pool, query).await,
            Self::TicketsTable => process_table_to_string::<Tickets>(pool, query).await,
            Self::TicketFlightsTable => process_table_to_string::<TicketFlights>(pool, query).await,
        }
    }

    pub async fn run_query_table_to_json(
        &self,
        pool: &PgPool,
        query: &str,
    ) -> Result<String, AppError> {
        match *self {
            Self::AircraftDataTable => process_table_to_json::<AircraftsData>(pool, query).await,
            Self::AirportsDataTable => process_table_to_json::<AirportsData>(pool, query).await,
            Self::BoardingPassesTable => process_table_to_json::<BoardingPasses>(pool, query).await,
            Self::BookingsTable => process_table_to_json::<Bookings>(pool, query).await,
            Self::FlightsTable => process_table_to_json::<Flights>(pool, query).await,
            Self::SeatsTable => process_table_to_json::<Seats>(pool, query).await,
            Self::TicketsTable => process_table_to_json::<Tickets>(pool, query).await,
            Self::TicketFlightsTable => process_table_to_json::<TicketFlights>(pool, query).await,
        }
    }

    pub async fn run_query_table_to_df(
        &self,
        pool: &PgPool,
        query: &str,
        ctx: &SessionContext,
    ) -> Result<DataFrame, AppError> {
        match *self {
            Self::AircraftDataTable => process_table_to_df::<AircraftsData>(pool, query, ctx).await,
            Self::AirportsDataTable => process_table_to_df::<AirportsData>(pool, query, ctx).await,
            Self::BoardingPassesTable => {
                process_table_to_df::<BoardingPasses>(pool, query, ctx).await
            }
            Self::BookingsTable => process_table_to_df::<Bookings>(pool, query, ctx).await,
            Self::FlightsTable => process_table_to_df::<Flights>(pool, query, ctx).await,
            Self::SeatsTable => process_table_to_df::<Seats>(pool, query, ctx).await,
            Self::TicketsTable => process_table_to_df::<Tickets>(pool, query, ctx).await,
            Self::TicketFlightsTable => {
                process_table_to_df::<TicketFlights>(pool, query, ctx).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("aircrafts_data", Some(Table::AircraftDataTable))]
    #[case("airports_data", Some(Table::AirportsDataTable))]
    #[case("boarding_passes", Some(Table::BoardingPassesTable))]
    #[case("bookings", Some(Table::BookingsTable))]
    #[case("flights", Some(Table::FlightsTable))]
    #[case("seats", Some(Table::SeatsTable))]
    #[case("tickets", Some(Table::TicketsTable))]
    #[case("ticket_flights", Some(Table::TicketFlightsTable))]
    #[case("foo", None)]
    #[case("", None)]
    fn test_create_table(#[case] input: &str, #[case] expected: Option<Table>) {
        assert_eq!(expected, Table::new(input));
    }

    #[rstest]
    #[case(Table::AircraftDataTable, "aircrafts_data")]
    #[case(Table::AirportsDataTable, "airports_data")]
    #[case(Table::BoardingPassesTable, "boarding_passes")]
    #[case(Table::BookingsTable, "bookings")]
    #[case(Table::FlightsTable, "flights")]
    #[case(Table::SeatsTable, "seats")]
    #[case(Table::TicketsTable, "tickets")]
    #[case(Table::TicketFlightsTable, "ticket_flights")]
    fn test_table_name(#[case] input: Table, #[case] expected: &str) {
        assert_eq!(expected, input.as_ref());
    }
}
