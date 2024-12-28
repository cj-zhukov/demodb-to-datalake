use crate::helpers::TestApp;
use demodb_to_datalake::{Table, DATABASE_URL};

use datafusion::{assert_batches_eq, prelude::*};
use secrecy::ExposeSecret;

#[tokio::test]
async fn test_aircrafts_data() {
    let app = TestApp::new(DATABASE_URL.expose_secret(), Table::AircraftDataTable).await.unwrap();
    let res = app.test_aircrafts_data().await.unwrap();

    assert_eq!(res.schema().fields().len(), 3); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 9); // rows count

    let rows = res.sort(vec![col("range").sort(true, true)]).unwrap();
    assert_batches_eq!(
        &[
              "+---------------+---------------------------------------------------------+-------+",
              "| aircraft_code | model                                                   | range |",
              "+---------------+---------------------------------------------------------+-------+",
            r#"| CN1           | {"en":"Cessna 208 Caravan","ru":"Сессна 208 Караван"}   | 1200  |"#,
            r#"| CR2           | {"en":"Bombardier CRJ-200","ru":"Бомбардье CRJ-200"}    | 2700  |"#,
            r#"| SU9           | {"en":"Sukhoi Superjet-100","ru":"Сухой Суперджет-100"} | 3000  |"#,
            r#"| 733           | {"en":"Boeing 737-300","ru":"Боинг 737-300"}            | 4200  |"#,
            r#"| 321           | {"en":"Airbus A321-200","ru":"Аэробус A321-200"}        | 5600  |"#,
            r#"| 320           | {"en":"Airbus A320-200","ru":"Аэробус A320-200"}        | 5700  |"#,
            r#"| 319           | {"en":"Airbus A319-100","ru":"Аэробус A319-100"}        | 6700  |"#,
            r#"| 763           | {"en":"Boeing 767-300","ru":"Боинг 767-300"}            | 7900  |"#,
            r#"| 773           | {"en":"Boeing 777-300","ru":"Боинг 777-300"}            | 11100 |"#,
              "+---------------+---------------------------------------------------------+-------+",
        ],
        &rows.collect().await.unwrap()
    );
}