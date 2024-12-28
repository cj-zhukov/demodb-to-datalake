use crate::helpers::TestApp;
use demodb_to_datalake::{Table, DATABASE_URL};

use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

#[tokio::test]
async fn test_flights() {
    let app = TestApp::new(DATABASE_URL.expose_secret(), Table::FlightsTable).await.unwrap();
    let res = app.test_flights().await.unwrap();

    assert_eq!(res.schema().fields().len(), 10); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

    let rows = res.sort(vec![col("flight_id").sort(true, true)]).unwrap().limit(0, Some(10)).unwrap();
    assert_batches_eq!(
        &[
            "+-----------+-----------+---------------------------+---------------------------+-------------------+-----------------+---------+---------------+---------------------------+---------------------------+",
            "| flight_id | flight_no | scheduled_departure       | scheduled_arrival         | departure_airport | arrival_airport | status  | aircraft_code | actual_departure          | actual_arrival            |",
            "+-----------+-----------+---------------------------+---------------------------+-------------------+-----------------+---------+---------------+---------------------------+---------------------------+",
            "| 1         | PG0403    | 2017-06-13T08:25:00+00:00 | 2017-06-13T09:20:00+00:00 | DME               | LED             | Arrived | 321           | 2017-06-13T08:29:00+00:00 | 2017-06-13T09:24:00+00:00 |",
            "| 2         | PG0404    | 2017-06-13T16:05:00+00:00 | 2017-06-13T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2017-06-13T16:11:00+00:00 | 2017-06-13T17:06:00+00:00 |",
            "| 3         | PG0405    | 2017-06-13T06:35:00+00:00 | 2017-06-13T07:30:00+00:00 | DME               | LED             | Arrived | 321           | 2017-06-13T06:38:00+00:00 | 2017-06-13T07:33:00+00:00 |",
            "| 4         | PG0402    | 2017-02-10T09:25:00+00:00 | 2017-02-10T10:20:00+00:00 | DME               | LED             | Arrived | 321           | 2017-02-10T09:30:00+00:00 | 2017-02-10T10:26:00+00:00 |",
            "| 5         | PG0403    | 2017-02-10T08:25:00+00:00 | 2017-02-10T09:20:00+00:00 | DME               | LED             | Arrived | 321           | 2017-02-10T08:28:00+00:00 | 2017-02-10T09:22:00+00:00 |",
            "| 6         | PG0403    | 2016-12-08T08:25:00+00:00 | 2016-12-08T09:20:00+00:00 | DME               | LED             | Arrived | 321           | 2016-12-08T08:31:00+00:00 | 2016-12-08T09:25:00+00:00 |",
            "| 7         | PG0404    | 2017-02-10T16:05:00+00:00 | 2017-02-10T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2017-02-10T16:07:00+00:00 | 2017-02-10T17:02:00+00:00 |",
            "| 8         | PG0404    | 2016-12-08T16:05:00+00:00 | 2016-12-08T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2016-12-08T16:10:00+00:00 | 2016-12-08T17:05:00+00:00 |",
            "| 9         | PG0404    | 2016-11-26T16:05:00+00:00 | 2016-11-26T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2016-11-26T16:09:00+00:00 | 2016-11-26T17:03:00+00:00 |",
            "| 10        | PG0404    | 2017-01-16T16:05:00+00:00 | 2017-01-16T17:00:00+00:00 | DME               | LED             | Arrived | 321           | 2017-01-16T16:08:00+00:00 | 2017-01-16T17:03:00+00:00 |",
            "+-----------+-----------+---------------------------+---------------------------+-------------------+-----------------+---------+---------------+---------------------------+---------------------------+",
        ],
        &rows.collect().await.unwrap()
    );
}