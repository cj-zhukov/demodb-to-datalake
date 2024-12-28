use crate::helpers::TestApp;
use demodb_to_datalake::{Table, DATABASE_URL};

use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

#[tokio::test]
async fn test_flights() {
    let app = TestApp::new(DATABASE_URL.expose_secret(), Table::SeatsTable).await.unwrap();
    let res = app.test_seats().await.unwrap();

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
}