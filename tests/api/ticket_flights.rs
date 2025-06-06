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
    let table = Table::TicketFlightsTable;
    let worker = table.to_worker();
    let ctx = SessionContext::new();
    let query = format!("select * from {}", table.as_ref());
    let res = worker.query_table_to_df(db.as_ref(), &query, &ctx).await?;

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

    let rows = res.sort(vec![col("ticket_no").sort(true, true)]).unwrap().limit(0, Some(10)).unwrap();
    assert_batches_eq!(
        &[
            "+---------------+-----------+-----------------+-----------+",
            "| ticket_no     | flight_id | fare_conditions | amount    |",
            "+---------------+-----------+-----------------+-----------+",
            "| 0005432003235 | 89752     | Business        | 99800.00  |",
            "| 0005432003470 | 89913     | Business        | 99800.00  |",
            "| 0005432003656 | 90106     | Business        | 99800.00  |",
            "| 0005432079221 | 36094     | Business        | 99800.00  |",
            "| 0005432801137 | 9563      | Business        | 150400.00 |",
            "| 0005432949087 | 164161    | Business        | 105900.00 |",
            "| 0005433557112 | 164098    | Business        | 105900.00 |",
            "| 0005433567794 | 164215    | Business        | 105900.00 |",
            "| 0005434861552 | 65405     | Business        | 49700.00  |",
            "| 0005435834642 | 117026    | Business        | 199300.00 |",
            "+---------------+-----------+-----------------+-----------+",
        ],
        &rows.collect().await.unwrap()
    );
    Ok(())
}