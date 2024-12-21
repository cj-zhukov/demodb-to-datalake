use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};

use color_eyre::{eyre::Context, Result};
use secrecy::ExposeSecret;

#[tokio::main]
async fn main() -> Result<()> {
    let db = PostgresDb::builder()
        .with_url(DATABASE_URL.expose_secret())
        .with_max_cons(MAX_DB_CONS)
        .build()
        .await?;

    let tables = ["aircrafts_data", "airports_data", "boarding_passes", "bookings", "flights", "seats", "tickets", "ticket_flights"];
    // let tables = ["airports_data"];
    for table in tables {
        let table = Table::new(table);
        if let Some(table) = table {
            let table_name = table.name();
            let worker = table.to_worker();

            // print results
            // worker.query_table(db.as_ref())
            //     .await
            //     .wrap_err(format!("failed when quering table: {}", table_name))?;

            // results to string
            // let res = worker.query_table_to_json(db.as_ref())
            //     .await
            //     .wrap_err(format!("failed when quering table: {}", table_name))?;
            // println!("{}", res);

            // results as dataframe
            let res = worker.query_table_to_df(db.as_ref())
                .await
                .wrap_err(format!("failed when quering table: {}", table_name))?;
            res.show().await?;
            println!();

            // save into file
            // write_df_to_file(res, "foo.parquet").await?;
        }
    }

    Ok(())
}