use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};
use demodb_to_datalake::{AIRCRAFTS_DATA_TABLE_NAME, AIRPORTS_DATA_TABLE_NAME, BOARDING_PASSES_TABLE_NAME, BOOKINGS_TABLE_NAME, FLIGHTS_TABLE_NAME, SEATS_TABLE_NAME, TICKETS_TABLE_NAME, TICKET_FLIGHTS_TABLE_NAME};

use color_eyre::Result;
use secrecy::ExposeSecret;

#[tokio::main]
async fn main() -> Result<()> {
    let db = PostgresDb::builder()
        .with_url(DATABASE_URL.expose_secret())
        .with_max_cons(MAX_DB_CONS)
        .build()
        .await?;

    let tables = [AIRCRAFTS_DATA_TABLE_NAME, AIRPORTS_DATA_TABLE_NAME, BOARDING_PASSES_TABLE_NAME, BOOKINGS_TABLE_NAME, FLIGHTS_TABLE_NAME, SEATS_TABLE_NAME, TICKETS_TABLE_NAME, TICKET_FLIGHTS_TABLE_NAME];
    for table in tables {
        let table = Table::new(table);
        if let Some(table) = table {
            // print results
            println!("table: {}", table.as_ref());
            table.run_query_table(db.as_ref()).await?;
        }
    }

    Ok(())
}