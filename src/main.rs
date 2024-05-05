use demodb_to_datalake::{Result, config::Config, Table};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::new("config.json").await?;
    println!("{}", config);
    let pool = config.connect().await?;

    let tables = ["tickets", "ticket_flights", "foo"];
    for table in tables {
        let table = Table::new(table);
        if let Some(table) = table {
            let worker = table.to_worker();
            let res = worker.query_table_to_df(&pool).await?;
            res.show().await?;
            println!();
        }
    }

    Ok(())
}
