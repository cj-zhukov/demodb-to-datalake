use demodb_to_datalake::{write_df_to_file, PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};
use demodb_to_datalake::{AIRCRAFTS_DATA_TABLE_NAME, AIRPORTS_DATA_TABLE_NAME, BOARDING_PASSES_TABLE_NAME, BOOKINGS_TABLE_NAME, FLIGHTS_TABLE_NAME, SEATS_TABLE_NAME, TICKETS_TABLE_NAME, TICKET_FLIGHTS_TABLE_NAME};

use color_eyre::{eyre::Context, Result};
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
            let worker = table.to_worker();

            // results as dataframe
            let res = worker.query_table_to_df(db.as_ref(), None)
                .await
                .wrap_err(format!("failed when quering table: {}", table.as_ref()))?;
            res.clone().show().await?;
            println!();

            // save into file
            // let file_name = format!("{}.parquet", table.as_ref());
            // write_df_to_file(res, &file_name).await?;
        }
    }

    Ok(())
}