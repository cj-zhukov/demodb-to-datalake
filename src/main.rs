use demodb_to_datalake::{config::Config, Table};

use anyhow::{Context, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::new("config.json").await?;
    let pool = config.connect().await?;

    let tables = ["aircrafts_data", "airports_data", "boarding_passes", "bookings", "flights", "seats", "tickets", "ticket_flights"];
    for table in tables {
        let table = Table::new(table);
        if let Some(table) = table {
            let table_name = table.value();
            let worker = table.to_worker();
            let res = worker.query_table_to_df(&pool)
                .await
                .context(format!("failed when quering table: {}", table_name))?;
            res.show().await?;
            println!();
        }
    }

    Ok(())
}
