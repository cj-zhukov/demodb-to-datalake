use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};

use anyhow::{Context, Result};
use secrecy::ExposeSecret;

#[tokio::main]
async fn main() -> Result<()> {
    let db = PostgresDb::builder()
        .with_max_cons(MAX_DB_CONS)
        .with_url(DATABASE_URL.expose_secret())
        .build()
        .await?;

    let tables = ["aircrafts_data", "airports_data", "boarding_passes", "bookings", "flights", "seats", "tickets", "ticket_flights"];
    for table in tables {
        let table = Table::new(table);
        if let Some(table) = table {
            let table_name = table.name();
            let worker = table.to_worker();

            let res = worker.query_table_to_df(db.as_ref())
                .await
                .context(format!("failed when quering table: {}", table_name))?;
            res.show().await?;

            // let res = worker.query_table_to_json(&pool)
            //     .await
            //     .context(format!("failed when quering table: {}", table_name))?;
            // println!("{}", res);
            // println!();

            // write_df_to_file(res, "foo.parquet").await?;
        }
    }

    Ok(())
}