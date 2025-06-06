use color_eyre::eyre::Context;
use datafusion::prelude::SessionContext;
use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};
use demodb_to_datalake::tables_names::*;

use color_eyre::Result;
use secrecy::ExposeSecret;

#[tokio::main]
async fn main() -> Result<()> {
    let db = PostgresDb::builder()
        .with_url(DATABASE_URL.expose_secret())
        .with_max_cons(MAX_DB_CONS)
        .build()
        .await?;

    let tables = [
        AIRCRAFTS_DATA_TABLE_NAME, 
        AIRPORTS_DATA_TABLE_NAME, 
        BOARDING_PASSES_TABLE_NAME, 
        BOOKINGS_TABLE_NAME, 
        FLIGHTS_TABLE_NAME, 
        SEATS_TABLE_NAME, 
        TICKETS_TABLE_NAME, 
        TICKET_FLIGHTS_TABLE_NAME
    ];
    for table in tables {
        let table = Table::new(table);
        if let Some(table) = table {
            let query = format!("select * from {}", table.as_ref());

            println!("query_table table: {}", table.as_ref());
            table.run_query_table(db.as_ref(), &query)
                .await
                .wrap_err(format!("failed quering table: {}", table.as_ref()))?;

            println!();
            println!("query_table_to_string table: {}", table.as_ref());
            let res = table.run_query_table_to_string(db.as_ref(), &query)
                .await
                .wrap_err(format!("failed quering table: {}", table.as_ref()))?;
            println!("{:?}", res);

            println!();
            println!("query_table_to_json table: {}", table.as_ref());
            let res = table.run_query_table_to_json(db.as_ref(), &query)
                .await
                .wrap_err(format!("failed quering table: {}", table.as_ref()))?;
            println!("{:?}", res);

            println!();
            println!("query_table_to_df table: {}", table.as_ref());
            let ctx = SessionContext::new();
            let res = table.run_query_table_to_df(db.as_ref(), &query, &ctx)
                .await
                .wrap_err(format!("failed quering table: {}", table.as_ref()))?;
            res.show().await?;
        }
    }
    Ok(())
}