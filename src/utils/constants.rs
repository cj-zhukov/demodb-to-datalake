use std::{env as std_env, sync::LazyLock};

use dotenvy::dotenv;
use secrecy::Secret;

pub const MAX_DB_CONS: u32 = 100;
pub const MAX_ROWS: u32 = 10;
pub const AIRCRAFTS_DATA_TABLE_NAME: &str = "aircrafts_data";
pub const AIRPORTS_DATA_TABLE_NAME: &str = "airports_data";
pub const BOARDING_PASSES_TABLE_NAME: &str = "boarding_passes";
pub const BOOKINGS_TABLE_NAME: &str = "bookings";
pub const FLIGHTS_TABLE_NAME: &str = "flights";
pub const SEATS_TABLE_NAME: &str = "seats";
pub const TICKETS_TABLE_NAME: &str = "tickets";
pub const TICKET_FLIGHTS_TABLE_NAME: &str = "ticket_flights";

pub mod env {
    pub const DATABASE_URL_ENV_VAR: &str = "DATABASE_URL";
}

pub static DATABASE_URL: LazyLock<Secret<String>> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::DATABASE_URL_ENV_VAR)
        .expect("DATABASE_URL_ENV_VAR must be set.");
    if secret.is_empty() {
        panic!("DATABASE_URL_ENV_VAR must not be empty.");
    }
    Secret::new(secret)
});