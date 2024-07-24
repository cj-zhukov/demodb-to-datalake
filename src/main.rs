use demodb_to_datalake::{config::Config, Table, utils::utils::write_df_to_file};

use anyhow::{Context, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::new("config.json").await?;
    let pool = config.connect().await?;

    let tables = ["aircrafts_data", "airports_data", "boarding_passes", "bookings", "flights", "seats", "tickets", "ticket_flights"];
    for table in tables {
        let table = Table::new(table);
        if let Some(table) = table {
            let table_name = table.name();
            let worker = table.to_worker();
            // worker.query_table(&pool)
            //     .await
            //     .context(format!("failed when quering table: {}", table_name))?;

            let res = worker.query_table_to_df(&pool)
                .await
                .context(format!("failed when quering table: {}", table_name))?;
            res.show().await?;
            // write_df_to_file(res, "foo.parquet").await?;

            // let res = worker.query_table_to_json(&pool)
            //     .await
            //     .context(format!("failed when quering table: {}", table_name))?;
            // println!("{}", res);
            // println!();
        }
    }

    Ok(())
}
