use crate::helpers::TestApp;
use demodb_to_datalake::{Table, DATABASE_URL};

use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

#[tokio::test]
async fn test_boarding_passes() {
    let app = TestApp::new(DATABASE_URL.expose_secret(), Table::BoardingPassesTable).await.unwrap();
    let ctx = SessionContext::new();
    let res = app.test_boarding_passes(&ctx).await.unwrap();

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 10); // rows count

    let rows = res.sort(vec![col("ticket_no").sort(true, true)]).unwrap().limit(0, Some(10)).unwrap();
    assert_batches_eq!(
        &[
            "+---------------+-----------+-------------+---------+",
            "| ticket_no     | flight_id | boarding_no | seat_no |",
            "+---------------+-----------+-------------+---------+",
            "| 0005432208788 | 198393    | 5           | 28C     |",
            "| 0005433655456 | 198393    | 7           | 31J     |",
            "| 0005435189093 | 198393    | 1           | 27G     |",
            "| 0005435189096 | 198393    | 3           | 18E     |",
            "| 0005435189100 | 198393    | 10          | 30F     |",
            "| 0005435189117 | 198393    | 4           | 31B     |",
            "| 0005435189119 | 198393    | 2           | 2D      |",
            "| 0005435189129 | 198393    | 8           | 30C     |",
            "| 0005435189151 | 198393    | 6           | 32A     |",
            "| 0005435629876 | 198393    | 9           | 30E     |",
            "+---------------+-----------+-------------+---------+",
        ],
        &rows.collect().await.unwrap()
    );
}