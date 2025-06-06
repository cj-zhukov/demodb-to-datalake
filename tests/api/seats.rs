use demodb_to_datalake::{PostgresDb, Table, DATABASE_URL, MAX_DB_CONS};

use color_eyre::Result;
use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

#[tokio::test]
async fn test_flights_dyn_df() -> Result<()> {
    let db = PostgresDb::builder()
      .with_url(DATABASE_URL.expose_secret())
      .with_max_cons(MAX_DB_CONS)
      .build()
      .await?;
    let table = Table::SeatsTable;
    let worker = table.to_worker();
    let ctx = SessionContext::new();
    let query = format!("select * from {} limit 2000", table.as_ref());
    let res = worker.query_table_to_df(db.as_ref(), &query, &ctx).await?;

    assert_eq!(res.schema().fields().len(), 3); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 1339); // rows count

    let rows = res.sort(vec![col("aircraft_code").sort(true, true)]).unwrap().limit(0, Some(10)).unwrap();
    assert_batches_eq!(
        &[
            "+---------------+---------+-----------------+",
            "| aircraft_code | seat_no | fare_conditions |",
            "+---------------+---------+-----------------+",
            "| 319           | 2C      | Business        |",
            "| 319           | 2D      | Business        |",
            "| 319           | 2F      | Business        |",
            "| 319           | 3A      | Business        |",
            "| 319           | 3C      | Business        |",
            "| 319           | 3D      | Business        |",
            "| 319           | 3F      | Business        |",
            "| 319           | 4A      | Business        |",
            "| 319           | 4C      | Business        |",
            "| 319           | 2A      | Business        |",
            "+---------------+---------+-----------------+",
        ],
        &rows.collect().await.unwrap()
    );
    Ok(())
}