use crate::helpers::TestApp;
use demodb_to_datalake::{Table, DATABASE_URL};

use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

#[tokio::test]
async fn test_flights() {
    let app = TestApp::new(DATABASE_URL.expose_secret(), Table::TicketFlightsTable).await.unwrap();
    let res = app.test_ticket_flights().await.unwrap();

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

    let rows = res.sort(vec![col("ticket_no").sort(true, true)]).unwrap().limit(0, Some(10)).unwrap();
    assert_batches_eq!(
        &[
            "+---------------+-----------+-----------------+----------+",
            "| ticket_no     | flight_id | fare_conditions | amount   |",
            "+---------------+-----------+-----------------+----------+",
            "| 0005432000284 | 187662    | Economy         | 6200.00  |",
            "| 0005432000285 | 187570    | Business        | 18500.00 |",
            "| 0005432000286 | 187570    | Economy         | 6200.00  |",
            "| 0005432000287 | 187728    | Economy         | 6200.00  |",
            "| 0005432000288 | 187728    | Business        | 18500.00 |",
            "| 0005432000289 | 187754    | Economy         | 6200.00  |",
            "| 0005432000290 | 187754    | Economy         | 6200.00  |",
            "| 0005432000291 | 187506    | Business        | 18500.00 |",
            "| 0005432000292 | 187506    | Economy         | 6200.00  |",
            "| 0005432000293 | 187625    | Economy         | 6200.00  |",
            "+---------------+-----------+-----------------+----------+",
        ],
        &rows.collect().await.unwrap()
    );
}